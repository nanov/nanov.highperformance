namespace Nanov.HighPerformance.Threading.Actors;

using System;
using System.Diagnostics;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors.Internal;
using Nanov.HighPerformance.Threading.Actors.WaitPolicies;

/// <summary>
///   Key-affine message processor: a single producer dispatches <c>(key, message)</c>; workers process
///   them with <b>strict FIFO order per key</b> and exclusive per-key ownership.
///   <para>
///     Single producer is the caller's contract, as with the SPSC worker family. Ownership is arbitrated
///     by a 3-state machine on each key's cell; storage is a chunk of a shared slab used as that key's
///     bounded mailbox.
///   </para>
/// </summary>
public sealed class ActorSystem<TKey, T, THandler, TScheduler> : IDisposable
	where TKey : notnull, IEquatable<TKey>
	where THandler : struct, IActorHandler<TKey, T>
	where TScheduler : struct, IKeyScheduler {
	private readonly KeyCellMap<TKey> _map;
	private readonly ChunkSlab<T> _slab;
	private readonly int _chunkSize;
	private readonly int _chunkMask;
	private readonly int _batchBound;
	private readonly IWaitPolicy _waitPolicy;

	// The PRODUCER's own copy — the one that services OnSlotReused and OnChunkReclaimed, which run on
	// the producer thread. Each worker carries a FURTHER copy of its own in WorkerState.Handler. Total
	// handler copies in the system: workers + 1 (one per worker, plus this one). Do not "deduplicate"
	// these — a handler shared across worker threads is exactly the race per-worker copies exist to
	// avoid; at one worker a single shared handler was survivable, at N it is not.
	private THandler _handler;
	private TScheduler _scheduler; // mutable struct — calls go through the field so they devirtualize

	private long _dispatched; // producer-private; no atomics

	private readonly WorkerState<THandler>[] _workers;
	private readonly PaddedLong[] _processedPublished; // one padded slot per worker; quiescence sums these
	private int _liveWorkers; // decremented as each worker exits; only the one that hits zero resolves Completion

	// Explicit initializer, not just the implicit default: nothing else assigns this field until
	// Dispose (Task 8), and an unassigned field trips CS0649, which is a build error here.
	private volatile bool _stopped = false;

	private readonly TaskCompletionSource _completion =
		new(TaskCreationOptions.RunContinuationsAsynchronously);
	private readonly ManualResetEventSlim _spaceAvailable = new(false);
	private volatile bool _producerParked;
	private long _dispatchedFinal;
	private volatile bool _completed;
	private Exception? _completionError;
	private int _completeGate;
	private int _disposeGate;
	private int _quiescenceBroadcast;
	private int _quiescenceWakes; // test hook only — counts actual WakeAll calls, not the gate flag

	/// <param name="handler">
	///   The handler prototype. The producer keeps its own copy of it, and every worker gets a further
	///   copy of its own — see the remarks on the private <c>_handler</c> field for why.
	/// </param>
	/// <param name="keyCapacity">Max live keys.</param>
	/// <param name="chunkCount">Max concurrently-dirty keys.</param>
	/// <param name="chunkSize">Messages one key may hold before backpressure.</param>
	/// <param name="batchBound">Max messages per activation; 0 ⇒ <paramref name="chunkSize" />.</param>
	/// <param name="workers">Number of worker threads; must be at least 1.</param>
	/// <param name="scheduler">The scheduling policy — see <see cref="IKeyScheduler" />.</param>
	/// <param name="waitPolicy">
	///   The idle-worker wait ladder — see <see cref="IWaitPolicy" />. <see langword="null" /> (the
	///   default) reproduces pre-Phase-4 behaviour exactly via <see cref="LegacyWaitPolicy" />, so no
	///   existing call site needs to change.
	/// </param>
	public ActorSystem(
		THandler handler,
		int keyCapacity,
		int chunkCount,
		int chunkSize,
		int batchBound,
		int workers,
		TScheduler scheduler,
		IWaitPolicy? waitPolicy = null) {
		if (workers < 1)
			throw new ArgumentOutOfRangeException(nameof(workers), "at least one worker is required.");

		_handler = handler;
		_scheduler = scheduler;
		_waitPolicy = waitPolicy ?? new LegacyWaitPolicy();
		_slab = new ChunkSlab<T>(chunkCount, chunkSize);
		_chunkSize = _slab.ChunkSize;
		_chunkMask = _slab.ChunkMask;
		_map = new KeyCellMap<TKey>(keyCapacity);

		// A chunk-bound cell is never evictable, so if chunks are scarcer than cells an evictable cell
		// always exists — cell exhaustion becomes structurally impossible and chunk exhaustion is the
		// only blocking condition.
		if (_slab.ChunkCount >= _map.SlotCount)
			throw new ArgumentOutOfRangeException(nameof(chunkCount),
				$"chunkCount ({_slab.ChunkCount}) must be less than the map's slot count ({_map.SlotCount}); " +
				"raise keyCapacity or lower chunkCount.");

		_workers = new WorkerState<THandler>[workers];
		for (var i = 0; i < workers; i++)
			_workers[i] = new WorkerState<THandler>(i, handler);
		_processedPublished = new PaddedLong[workers];
		_liveWorkers = workers;

		// Passing Fault here hands out `this` before the constructor finishes (Start may launch a
		// background thread that calls it immediately). Safe today only because Fault touches nothing
		// but already-assigned fields (_completionError, _stopped, _scheduler) — _batchBound below is
		// NOT among them. A future edit to Fault that reads a field assigned after this line would race.
		_scheduler.Start(workers, Fault);
		_batchBound = batchBound > 0 ? batchBound : _chunkSize;
	}

	/// <summary>Max messages consumed in one activation before the key is released and rescheduled.</summary>
	public int BatchBound => _batchBound;

	/// <summary>
	///   Enqueue a message for <paramref name="key" />, <b>blocking</b> if the key's mailbox is full or
	///   the slab is exhausted. Single-producer: never call concurrently. Returns false if the system is
	///   completed or disposed.
	/// </summary>
	public bool Dispatch(TKey key, T message) => DispatchCore(key, message, blocking: true);

	/// <summary>
	///   Enqueue a message, returning <c>false</c> immediately instead of blocking when the key's mailbox
	///   is full or the slab is exhausted. A rejected message is not enqueued and is not counted.
	///   <para>
	///     Because a full key blocks the single producer for <i>all</i> keys, the difference between this
	///     and <see cref="Dispatch" /> is exactly the cost of the bounded per-key mailbox — which is what
	///     makes that cost measurable.
	///   </para>
	/// </summary>
	public bool TryDispatch(TKey key, T message) => DispatchCore(key, message, blocking: false);

	private bool DispatchCore(TKey key, T message, bool blocking) {
		if (_completed || _stopped) return false;

		ref var cell = ref _map.GetOrAddRef(key, out _, out var index);
		if (cell.ChunkBase == Chunk.None && !BindChunk(ref cell, blocking)) return false;

		// Fullness is checked against the producer-private CachedHead; the real Head — which the worker
		// writes — is re-read only on the "looks full" edge. Head only advances, so CachedHead can only
		// over-estimate fullness: conservative, never overwrites a live slot.
		if (cell.Tail - cell.CachedHead >= _chunkSize) {
			cell.CachedHead = Volatile.Read(ref cell.Head);
			while (cell.Tail - cell.CachedHead >= _chunkSize) {
				if (!blocking) return false;
				if (!WaitForMailboxSpace(ref cell)) return false;
			}
		}

		ref var slot = ref _slab.SlotRef(cell.ChunkBase, cell.Tail & _chunkMask);
		try {
			_handler.OnSlotReused(ref slot);
		}
		catch (Exception ex) {
			SafeOnError(ex);
		}

		slot = message;
		Volatile.Write(ref cell.Tail, cell.Tail + 1); // release: publishes the slot write
		_dispatched++;

		// StoreLoad: the Tail store must be ordered before the State load. Volatile does NOT give this
		// on ARM64, and the test-then-CAS below means there is no RMW here to supply it. Removing this
		// barrier strands messages — see the release handshake in ReleaseAndMaybeSchedule.
		Interlocked.MemoryBarrier();
		if (Volatile.Read(ref cell.State) == CellState.Free &&
			Interlocked.CompareExchange(ref cell.State, CellState.Scheduled, CellState.Free) == CellState.Free)
			_scheduler.Schedule(index);

		return true;
	}

	private bool BindChunk(ref KeyCell<TKey> cell, bool blocking) {
		int chunk;
		while (!_slab.TryRent(out chunk)) {
			if (Harvest()) continue;              // cold: reclaim drained keys' chunks (Task 10)
			if (!blocking) return false;
			if (!WaitForChunk()) return false; // park until a worker drains something
		}

		cell.ChunkBase = chunk;
		cell.Head = 0;
		cell.Tail = 0;
		cell.CachedHead = 0;
		return true;
	}

	/// <summary>
	///   Cold, producer-side sweep: reclaim the chunks of keys that have drained. Deliberately the same
	///   shape as <c>CoalescingKeyMap.EvictFree</c> — a state-gated scan, not a queue. v1 has no
	///   drain-hint ring; that is an optimization over this scan and would be the only multi-producer
	///   structure in the system.
	///   <para>
	///     Must not run after <see cref="TryComplete" />: §5.6's quiescence argument requires every
	///     producer schedule to be sequenced-before the completion flag, and this path can schedule.
	///   </para>
	/// </summary>
	private bool Harvest() {
		if (_completed) return false; // see the remark above — never harvest after completion

		var freed = false;
		var slots = _map.SlotCount;
		for (var slot = 0; slot < slots; slot++) {
			if (!_map.IsOccupied(slot)) continue;
			ref var cell = ref _map.RefAt(slot);
			if (cell.ChunkBase == Chunk.None) continue;
			if (Volatile.Read(ref cell.State) != CellState.Free) continue;

			// Claim the cell. A worker that is mid-release loses this CAS or wins it later; either way
			// the key cannot be processed while we hold Reclaiming.
			if (Interlocked.CompareExchange(ref cell.State, CellState.Reclaiming, CellState.Free) !=
				CellState.Free)
				continue;

			var head = Volatile.Read(ref cell.Head);
			if (Volatile.Read(ref cell.Tail) != head) {
				// It went dirty between the check and the claim. Releasing this is NOT a plain revert:
				// Reclaiming is a non-Free state, so a worker's release-recheck may have lost its CAS and
				// skipped scheduling. Restoring Free without re-testing the mailbox strands the key.
				ReleaseAndMaybeSchedule(null, slot, ref cell, head, fromWorker: false);
				continue;
			}

			// Cursors are deliberately left untouched here (BindChunk zeroes Head/Tail/CachedHead on
			// rebind). A worker mid-release reads Tail unconditionally, before it ever consults State —
			// zeroing Tail/Head here would make its stale local `head` compare unequal against a
			// zeroed Tail, spuriously reschedule a chunkless cell, and hand Activate a Chunk.None to
			// index into an unchecked SlotRef. See Activate's Chunk.None guard for the other half of
			// this fix.
			try {
				_handler.OnChunkReclaimed(_slab.ChunkSpan(cell.ChunkBase));
			}
			catch (Exception ex) {
				SafeOnError(ex);
			}

			_slab.Return(cell.ChunkBase);
			cell.ChunkBase = Chunk.None;
			Volatile.Write(ref cell.State, CellState.Free);
			freed = true;
		}

		return freed;
	}

	/// <summary>
	///   Park until the worker frees space in this key's mailbox. Arm-then-recheck: reset the event
	///   BEFORE raising the parked flag, fence, then re-test the predicate inside the armed window — a
	///   worker that advanced Head just before we armed will not Set, so the recheck is the only thing
	///   that catches it. Returns false if the system stopped or completed while parked, so a dispatch
	///   racing shutdown is rejected rather than committed into a ring whose consumer has exited.
	///   <para>
	///     <b>Both sides of the handshake are fenced — keep it that way.</b> This side:
	///     <c>_producerParked = true</c> → <c>Interlocked.MemoryBarrier()</c> → re-read <c>Head</c>.
	///     The worker has two wake sites, each ordering its <c>Head</c> publish before its
	///     <c>_producerParked</c> read with a full fence: <c>ReleaseAndMaybeSchedule</c> via the
	///     <c>Interlocked.Exchange</c> on <c>cell.State</c>, and <c>Activate</c>'s mid-batch flush via an
	///     explicit <c>Interlocked.MemoryBarrier()</c>. Drop either fence and a release-store followed by
	///     an acquire-load of a <b>different</b> location is no longer StoreLoad-ordered: the worker can
	///     read <c>_producerParked</c> as false (we had not armed yet) and skip the <c>Set</c>, while we
	///     arm, fence, and read a <i>stale</i> <c>Head</c>. The wake is lost and only the bounded
	///     20&#160;ms <c>_spaceAvailable.Wait(TimeSpan)</c> below recovers it — nothing hangs, but the
	///     backpressure path takes a 20&#160;ms tail-latency cliff.
	///   </para>
	/// </summary>
	private bool WaitForMailboxSpace(ref KeyCell<TKey> cell) {
		_spaceAvailable.Reset();
		_producerParked = true;
		try {
			Interlocked.MemoryBarrier(); // flag visible before the recheck below
			if (_stopped || _completed) return false;
			cell.CachedHead = Volatile.Read(ref cell.Head);
			if (cell.Tail - cell.CachedHead < _chunkSize) return true; // freed while we were arming
			_spaceAvailable.Wait(TimeSpan.FromMilliseconds(20));
			cell.CachedHead = Volatile.Read(ref cell.Head);
			return !_stopped && !_completed;
		}
		finally {
			_producerParked = false;
		}
	}

	/// <summary>
	///   Park until the worker drains a key, which is what makes a chunk harvestable. Arm-then-recheck,
	///   same shape as <see cref="WaitForMailboxSpace" />: the predicate that matters is not "did I just
	///   harvest" but "has some cell reached <c>Free</c> since my last scan" — which a worker can make
	///   true at any moment, including inside this method's own armed window — so that predicate is
	///   producer-visible and must be re-tested before sleeping, not assumed away.
	/// </summary>
	private bool WaitForChunk() {
		_spaceAvailable.Reset();
		_producerParked = true;
		try {
			Interlocked.MemoryBarrier();
			if (_stopped || _completed) return false;

			// Recheck INSIDE the armed window. The predicate is not "did I harvest" but "has a cell
			// reached Free since my last scan" — which a worker can make true at any moment. Without
			// this we sleep a full poll interval with a harvestable chunk already available.
			//
			// !_stopped, not a bare true: this fast path skips the sleep path's own re-check below, so a
			// Dispose landing in this exact window would otherwise let a chunk-starved Dispatch return
			// true for a message that BindChunk then writes into a cell nobody drains — dropped silently.
			if (Harvest()) return !_stopped;

			_spaceAvailable.Wait(TimeSpan.FromMilliseconds(20));
			return !_stopped && !_completed;
		}
		finally {
			_producerParked = false;
		}
	}

	/// <summary>Start one thread per worker.</summary>
	public void Start() {
		for (var i = 0; i < _workers.Length; i++) {
			var w = _workers[i];
			w.Thread = new Thread(RunLoop) {
				IsBackground = true,
				Name = $"Actor<{typeof(TKey).Name}>#{w.Id}",
			};
			w.Thread.Start(w);
		}
	}

	/// <summary>
	///   Record a fault from a background thread and stop the system so everyone can exit.
	///   <para>
	///     All the steps are required. Recording alone leaves the survivors parked; stopping alone
	///     loses the exception; and without the wakes, workers already blocked on an unbounded wait
	///     never re-check <c>_stopped</c>, and a producer parked in <see cref="WaitForMailboxSpace" />
	///     or <see cref="WaitForChunk" /> is left to the 20&#160;ms poll instead of released
	///     immediately — the same pairing <see cref="TryComplete" /> and <see cref="Dispose" /> already
	///     use for <see cref="IKeyScheduler.SignalStop" />.
	///   </para>
	/// </summary>
	private void Fault(Exception error) {
		_completionError ??= error;
		_stopped = true;
		_scheduler.SignalStop();
		_spaceAvailable.Set();
	}

	private void RunLoop(object? state) {
		WorkerState<THandler>? w = null;
		try {
			// The cast lives INSIDE the try so a malformed `state` — never reachable via Start(), which
			// always passes a worker its own WorkerState, but defensive nonetheless — is caught and
			// reported through the same fault path as any other RunLoop exception, rather than escaping
			// the thread unhandled before the try even begins.
			w = (WorkerState<THandler>)state!;
			while (!_stopped) {
				if (_scheduler.TryAcquire(w.Id, out var index)) {
					Activate(w, index);
					continue;
				}

				if (!WaitForWork(w)) break;
			}
		}
		catch (Exception ex) {
			// A background-thread escape would kill the host process with Completion unresolved.
			// Fault the system so the failure is observable AND the survivors are released — see Fault's
			// own doc for why recording the exception alone is not enough.
			Fault(ex);
			// Only reachable with a worker identity; if the cast itself failed there is no handler copy
			// to report through, and Fault above is what surfaces the failure.
			if (w is not null) SafeOnError(w, ex);
		}
		finally {
			// w can be null here if the cast (or the very first w.Id dereference, for a null state)
			// threw before a worker identity was ever established — see the comment at the cast site.
			// Everything that needs w is guarded on it; NOTHING above is dereferenced unconditionally,
			// unlike the round-1 fix, which put an unguarded PublishProcessed(w!) first in this block —
			// an NRE there escaped the thread before the decrement below ever ran, leaving Completion
			// unresolved forever. The decrement itself is NOT guarded: a worker that never got an
			// identity still counts toward _liveWorkers and must still release it, or the count never
			// reaches zero and Completion hangs regardless of what happened above.
			if (w is not null) {
				PublishProcessed(w);

				// _completed means a drain was REQUESTED, not that it finished. A Dispose racing an
				// in-flight TryComplete drops messages, so the exit is only clean if we actually reached
				// quiescence. Read _completed BEFORE the counters — same acquire ordering rule as
				// IsQuiescent, and && short-circuits in the right order. This is this worker's OWN view,
				// used only for the OnCompleted callback below — see the recompute inside the decrement
				// branch for why the SYSTEM verdict cannot reuse it.
				var drainedCleanly = _completed && QuiescentSum() >= Volatile.Read(ref _dispatchedFinal);
				var hardStopped = _stopped && !drainedCleanly;
				// Same precedence as the resolution below: a recorded fault wins over a cancellation,
				// because Fault() sets _stopped as part of stopping the system. Testing hardStopped first
				// would hand every handler a synthetic OperationCanceledException while Completion reports
				// the real fault — the two must agree.
				Exception? error = _completionError ?? (hardStopped ? new OperationCanceledException() : null);
				try {
					w.Handler.OnCompleted(error); // once per worker — the documented contract
				}
				catch (Exception ex) {
					SafeOnError(w, ex);
				}
			}

			// Only the LAST worker out resolves Completion; otherwise the first to exit would report
			// the system finished while its peers are still draining. ALWAYS runs, even for a worker
			// that never got an identity — see the comment above.
			if (Interlocked.Decrement(ref _liveWorkers) == 0) {
				// Recompute HERE, not above: this read is ordered after our own Interlocked.Decrement,
				// which is ordered after every peer's decrement, which is ordered after each peer's
				// final PublishProcessed. Computed before the decrement, the last worker has no edge
				// with its peers and can see a stale sum — reporting a cleanly-drained system as
				// cancelled.
				var allDrained = _completed && QuiescentSum() >= Volatile.Read(ref _dispatchedFinal);
				// A recorded fault wins over a cancellation: Fault() sets _stopped as part of stopping
				// the system, so testing _stopped first would report every fault as a plain cancel and
				// discard the exception. A cancel is what a fault-free hard stop looks like.
				if (_completionError is not null) _completion.TrySetException(_completionError);
				else if (_stopped && !allDrained) _completion.TrySetCanceled();
				else _completion.TrySetResult();
			}
		}
	}

	private void Activate(WorkerState<THandler> w, int index) {
		w.Activations++;
		ref var cell = ref _map.RefAt(index);

		// VALIDATE the claim — do not assume it. A blind Exchange to Owned would let two workers
		// holding two tokens for one index both "own" the key and drain the same mailbox from the same
		// Head: duplicate delivery, lost messages, and per-key order gone. Exclusive ownership is the
		// only thing providing order here (design 3.1), so acquisition must be a CAS whose failure
		// means "not ours".
		//
		// A legitimate token always finds Scheduled: the producer's dispatch CAS only fires
		// Free -> Scheduled, the worker's release CAS the same, and Harvest/EvictFree only claim
		// Free -> Reclaiming. Losing this CAS therefore means a duplicate token or a reclaimed cell —
		// in both cases there is nothing for us to do, and touching the cell would be the bug.
		if (Interlocked.CompareExchange(ref cell.State, CellState.Owned, CellState.Scheduled) !=
			CellState.Scheduled) {
			w.RejectedClaims++;
			return;
		}

		var chunk = cell.ChunkBase;
		if (chunk == Chunk.None) {
			// NOT Harvest — Harvest cannot produce this. Harvest skips any cell that is not Free, and
			// when it does claim a Free-and-drained cell (Head == Tail) it is racing exactly the same
			// mid-release worker whose own Tail recheck also finds Head == Tail, so it never mints a
			// token for a chunkless cell in the first place.
			//
			// The reachable path is eviction (KeyCellMap.EvictFree), which is a DIFFERENT claim than
			// Harvest's: a worker stalls between its own Exchange(State, Free) and its Tail recheck; the
			// producer harvests the (now-Free, chunkless) cell's slot via EvictFree, tombstones it, and
			// Place later re-keys the same slot index for an unrelated TKey; the stalled worker's stale
			// CAS(Free -> Scheduled) can still win — against that re-keyed slot, not the key it was
			// originally scheduled for. There is nothing to drain and no chunk to drain from — release
			// and move on. Without this guard the drain loop would index the slab at Chunk.None (-1),
			// and SlotRef is deliberately unchecked.
			//
			// Passing cell.Head as a plain read here is safe only because both EvictFree and the
			// subsequent Place zero Head when they tombstone/re-place the slot, so the read yields 0 and
			// ReleaseAndMaybeSchedule's write-back is a no-op — it is not writing anything meaningful
			// into the re-keyed cell.
			ReleaseAndMaybeSchedule(w, index, ref cell, cell.Head, fromWorker: true);
			return;
		}

		var head = cell.Head;
		var n = 0;
		while (n < _batchBound && head != Volatile.Read(ref cell.Tail)) {
			if (_stopped) break;
			try {
				w.Handler.Handle(cell.Key, _slab.SlotRef(chunk, head & _chunkMask));
			}
			catch (Exception ex) {
				SafeOnError(w, ex);
			}

			head++;
			n++;
			w.ProcessedLocal++;
			if ((n & 31) == 0) {
				Volatile.Write(ref cell.Head, head);
				PublishProcessed(w);
				// StoreLoad fence between publishing Head and reading _producerParked — the worker's half of
				// the Dekker pair whose other half is WaitForMailboxSpace's flag → fence → re-read Head.
				// Without it the read can be satisfied before the Head store drains (x86-64 store buffer):
				// we see the pre-arm false and skip the Set while the producer arms and reads a stale Head,
				// and the wake is lost to the 20 ms poll. One fence per 32 messages, not per message.
				Interlocked.MemoryBarrier();
				if (_producerParked) {
					w.ProducerWakes++;
					_spaceAvailable.Set();
				}
			}
		}

		ReleaseAndMaybeSchedule(w, index, ref cell, head, fromWorker: true);
	}

	/// <summary>
	///   Release ownership, and reschedule if the mailbox is not empty. The ordering here is
	///   load-bearing — see §5.3. Shared by the worker and by the producer's cold paths (Task 10), which
	///   is why it takes <paramref name="fromWorker" /> rather than being open-coded twice.
	///   <paramref name="w" /> is the calling worker's own state when <paramref name="fromWorker" /> is
	///   true, and null on the producer's cold path — the worker-only block below is guarded on
	///   <paramref name="fromWorker" /> precisely so it never dereferences a null <paramref name="w" />.
	/// </summary>
	private void ReleaseAndMaybeSchedule(WorkerState<THandler>? w, int index, ref KeyCell<TKey> cell, int head,
		bool fromWorker) {
		if (fromWorker) {
			Volatile.Write(ref cell.Head, head);
			PublishProcessed(w!);
		}

		// FULL FENCE, deliberately not Volatile.Write: the Tail load below must not be hoisted above
		// this store. Producer and worker each store to their own variable then load the other's, with a
		// full fence between (Dekker) — so they cannot both miss each other, and the State CAS (never
		// the emptiness check) is the arbiter of who reschedules.
		Interlocked.Exchange(ref cell.State, CellState.Free);

		// Read _producerParked HERE — after the Exchange above frees the cell, before the Tail check
		// below. Placement matters in both directions: reading it BEFORE the Exchange, the chunk is not
		// yet harvestable, so a producer woken here would just find nothing and re-park; reading it AFTER
		// the reschedule CAS further down, the cell may already be back to Scheduled and the wake would be
		// a no-op. This is also the fix for the chunk-exhaustion lost wakeup (spec §11 known defect (a)):
		// Interlocked.Exchange is a full fence, so reading _producerParked immediately after it also
		// StoreLoad-orders the earlier Volatile.Write(ref cell.Head, …) above against this read — the
		// missing ordering that let a worker publish Head, read a stale (pre-arm) _producerParked, and
		// skip the Set while the producer armed a moment later and read a stale Head.
		if (fromWorker && _producerParked) {
			w!.ProducerWakes++;
			_spaceAvailable.Set();
		}

		if (Volatile.Read(ref cell.Tail) == head) return;
		// else: drained. The chunk is now reclaimable, but we must NOT touch ChunkBase — it is
		// producer-owned. The producer discovers it on its next cold harvest scan (Task 10).

		if (Interlocked.CompareExchange(ref cell.State, CellState.Scheduled, CellState.Free) != CellState.Free)
			return; // the producer got there first and already scheduled it — no double-schedule

		if (fromWorker) _scheduler.ScheduleLocal(w!.Id, index);
		else _scheduler.Schedule(index);
	}

	/// <summary>
	///   Publish this worker's processed count, and — if that publish just satisfied quiescence —
	///   broadcast so parked peers can exit.
	///   <para>
	///     <b>This second wake edge is required at N &gt; 1</b> (§5.6). Without it, the worker whose
	///     publish completes the sum simply carries on to its own idle check and exits, while every
	///     other worker stays parked forever with nothing left to wake them: no further message will ever
	///     be processed, so no further publish will ever occur. At N = 1 it is unreachable — the sole
	///     publisher is also the sole predicate evaluator — which is why Phase 1 omitted it.
	///   </para>
	///   <para>
	///     Reuses <see cref="IsQuiescent" /> as-is — flag FIRST, counts second. <c>_dispatchedFinal</c> is
	///     0 until <see cref="TryComplete" /> publishes it, so a sum-first check (<c>sum &gt;= 0</c>) is
	///     unconditionally true for the entire pre-completion life of the system; a worker that then reads
	///     a freshly-set <c>_completed</c> would burn the one-shot gate on a PARTIAL sum, and the real
	///     broadcast — at genuine quiescence — could never fire again, hanging every parked peer forever.
	///     Reading the flag first is what makes "observed completed ⇒ observed the real final count" hold
	///     (<see cref="TryComplete" /> publishes <c>_dispatchedFinal</c> before <c>_completed</c>), so the
	///     gate can only ever be burned at true quiescence. This is also why the flag-first order is safe
	///     even though it looks backwards next to <see cref="TryComplete" />'s unconditional
	///     <c>SignalStop()</c>: a SUPPRESSED broadcast here is rescued three ways — <c>TryComplete</c>'s
	///     own unconditional wake, this worker's own next publish within 32 messages, and the exit-path
	///     publish in <c>RunLoop</c>'s <c>finally</c> — while a PREMATURE one is not rescuable at all,
	///     because the gate is one-shot.
	///   </para>
	///   <para>
	///     <see cref="Interlocked.Exchange(ref int, int)" /> on <see cref="_quiescenceBroadcast" /> gates
	///     the actual <see cref="IKeyScheduler.WakeAll" /> call to at most once for the system's lifetime
	///     — this runs every 32 messages on the drain loop's warm path, so a repeated broadcast would be a
	///     wake storm. A worker that parks after the broadcast still re-checks quiescence inside its own
	///     arm-then-recheck (<see cref="WaitForWork" />), so it cannot miss the exit. Flag-first also
	///     collapses the common (not-yet-quiescent) case to a single cheap bool read, instead of an
	///     O(workers) sum plus a <c>long</c> read on every publish.
	///   </para>
	/// </summary>
	private void PublishProcessed(WorkerState<THandler> w) {
		Volatile.Write(ref _processedPublished[w.Id].Value, w.ProcessedLocal);
		if (IsQuiescent() && Interlocked.Exchange(ref _quiescenceBroadcast, 1) == 0) {
			Interlocked.Increment(ref _quiescenceWakes);
			_scheduler.WakeAll();
		}
	}

	private void SafeOnError(Exception error) {
		try {
			_handler.OnError(error);
		}
		catch {
			// A throwing OnError must not kill the producer.
		}
	}

	private void SafeOnError(WorkerState<THandler> w, Exception error) {
		try {
			w.Handler.OnError(error);
		}
		catch {
			// A throwing OnError must not kill the worker.
		}
	}

	/// <summary>Resolves once every worker has drained and exited.</summary>
	public Task Completion => _completion.Task;

	/// <summary>
	///   Signal that no more messages will be dispatched: drain everything queued, then exit. A non-null
	///   <paramref name="exception" /> faults <see cref="Completion" />. Returns false if already
	///   completing. Producer thread only — and it must be the producer's LAST act (see below).
	/// </summary>
	public bool TryComplete(Exception? exception = null) {
		if (Interlocked.Exchange(ref _completeGate, 1) != 0) return false;
		// Do NOT clobber a fault a worker already recorded — a soft complete with no exception must not
		// turn a dead worker into a clean Completion.
		if (exception is not null) _completionError = exception;

		// Publish the final count BEFORE the flag: the quiescence check reads the flag first, so this
		// ordering is what makes "observed completed ⇒ observes the full dispatched count" hold.
		Volatile.Write(ref _dispatchedFinal, _dispatched);
		_completed = true;

		// Shutdown wakes UNCONDITIONALLY — never flag-gated. Without this, a system that was already
		// drained and parked when TryComplete arrives would never be woken by anything: no further
		// message will ever be processed, so no worker will ever re-evaluate the predicate.
		_scheduler.SignalStop();
		_spaceAvailable.Set();

		// A system that was constructed but never started has no worker thread, so RunLoop's finally
		// — the only other place Completion resolves — will never run. Resolve it here instead;
		// TrySet* is idempotent so this cannot race a worker that starts resolving concurrently.
		// _workers[0].Thread is null iff Start() was never called — Start() creates every worker's
		// thread in one pass, so checking the first is checking them all.
		if (_workers[0].Thread is null) {
			if (exception is null) _completion.TrySetResult();
			else _completion.TrySetException(exception);
		}

		return true;
	}

	/// <summary>
	///   Park until work arrives or the system should exit. Returns false to exit the loop.
	///   <para>
	///     Arm-then-recheck: the flag is cleared, then work and quiescence are re-tested, and only then
	///     do we block. Collapsing this into check-then-park is the classic lost wakeup, and it passes
	///     every test whose worker never actually sleeps.
	///   </para>
	/// </summary>
	private bool WaitForWork(WorkerState<THandler> w) {
		// Saturating, not wrapping. `attempt` advances once per WAKEUP-without-work, not per unit time —
		// an unbounded Wait() means a worker parked on a quiet system increments zero times — so reaching
		// int.MaxValue needs ~2^31 spurious wakeups inside ONE park. Unreachable in practice, but the
		// consequence of wrapping is not benign: int.MinValue satisfies `attempt < SpinRungs`, so the
		// ladder would hand back Spin and a parked worker would burn a core for ~2^31 rungs. Worse, the
		// Spin branch never calls IsQuiescent(), so a system that drains during that window would have
		// completion delayed until the worker climbed back to a Block rung. The pre-seam code had no
		// counter and so could not do this; saturating restores that guarantee for one token.
		for (var attempt = 0; ; attempt = attempt == int.MaxValue ? attempt : attempt + 1) {
			var step = _waitPolicy.Next(attempt);

			if (step.Kind == WaitStepKind.Spin) {
				if (_scheduler.HasWork(w.Id) || _stopped) return !_stopped;
				Thread.SpinWait(step.SpinIterations);
				continue;
			}

			if (step.Kind == WaitStepKind.Yield) {
				if (_scheduler.HasWork(w.Id) || _stopped) return !_stopped;
				Thread.Yield();
				continue;
			}

			// Arm-then-recheck, unchanged: clear the flag, fence, re-test work AND quiescence, and only
			// then block. Collapsing this into check-then-park is the classic lost wakeup, and it passes
			// every test whose worker never actually sleeps.
			_scheduler.ArmWait(w.Id);
			Interlocked.MemoryBarrier();
			if (_scheduler.HasWork(w.Id) || _stopped) return !_stopped;
			if (IsQuiescent()) return false;
			// The bool return (signalled vs. timed out) is deliberately unused: a timeout is not an error,
			// and the two rechecks below run unconditionally either way — see IKeyScheduler.Wait's doc.
			// Reordering around the return value here would be the reintroduction the seam was built to
			// avoid.
			_scheduler.Wait(w.Id, step.BlockMilliseconds);
			if (_scheduler.HasWork(w.Id) || _stopped) return !_stopped;
			if (IsQuiescent()) return false;
		}
	}

	/// <summary>Sums every worker's published processed count. Shared by <see cref="IsQuiescent" /> and RunLoop's exit, so the two cannot drift.</summary>
	private long QuiescentSum() {
		var processed = 0L;
		for (var i = 0; i < _processedPublished.Length; i++)
			processed += Volatile.Read(ref _processedPublished[i].Value);
		return processed;
	}

	/// <summary>
	///   Everything dispatched has been processed and completion was signalled.
	///   <para>
	///     <b>Read order is load-bearing.</b> <c>_completed</c> must be read BEFORE the counts, both with
	///     acquire semantics. The soundness argument is "every producer increment is sequenced-before
	///     <c>_completed = true</c>, so a worker observing the flag must observe the counts" — which only
	///     holds if the count loads are ordered after the flag load. Reversed, ARM64 will serve a stale
	///     count beside a fresh flag: false quiescence, and messages silently dropped on dispose.
	///   </para>
	///   This mirrors the family's existing soft-complete rule (re-read Tail after observing
	///   _completed) with the summed processed count as the new Tail.
	/// </summary>
	private bool IsQuiescent() {
		if (!_completed) return false; // flag FIRST — see the doc comment; do not reorder
		// >= not ==: an undercount only delays quiescence, but any overshoot with == would hang
		// forever rather than exit late.
		return QuiescentSum() >= Volatile.Read(ref _dispatchedFinal);
	}

	/// <summary>
	///   Hard stop: drop queued messages and exit. The message currently inside the handler still
	///   finishes — tearing down under a running handler is something the family never does.
	///   Workers are joined against ONE SHARED budget, not one per worker — N workers must not make
	///   disposal take N seconds. A system that was constructed but never started has no worker thread
	///   to resolve <see cref="Completion" />, so this resolves it directly as cancelled in that case.
	/// </summary>
	public void Dispose() {
		if (Interlocked.Exchange(ref _disposeGate, 1) != 0) return;
		_stopped = true;
		_scheduler.SignalStop();
		_spaceAvailable.Set();
		if (_workers[0].Thread is null)
			_completion.TrySetCanceled();
		else {
			var deadline = Stopwatch.StartNew();
			var budget = TimeSpan.FromSeconds(1);
			foreach (var w in _workers) {
				var remaining = budget - deadline.Elapsed;
				if (remaining > TimeSpan.Zero) w.Thread?.Join(remaining);
			}
		}
		// The events are deliberately NOT disposed: the producer is a caller-owned thread that may be
		// parked inside Wait() right now, and disposing under it throws ObjectDisposedException on
		// their thread. Letting the finalizer reclaim them is the correct trade here.
	}

	// ---- test hooks -------------------------------------------------------------------------------

	internal ref KeyCell<TKey> CellOf(TKey key) => ref _map.GetOrAddRef(key, out _, out _);

	/// <summary>
	///   Simulate a fault on a scheduler-owned background thread (the router, or an equivalent).
	///   <para>
	///     There is no natural trigger for this in-process: the router's own catch has no reachable
	///     throw site short of a bounds misconfiguration, and a worker-loop escape has no natural
	///     trigger at all. A hook is the honest instrument for exercising the fault channel rather than
	///     contriving a fake "natural" failure that would misrepresent what the test is guarding.
	///   </para>
	/// </summary>
	internal void InjectSchedulerFaultForTesting(Exception error) => Fault(error);

	internal long DispatchedCount => _dispatched;

	internal int Activations {
		get {
			var sum = 0;
			foreach (var w in _workers) sum += Volatile.Read(ref w.Activations);
			return sum;
		}
	}

	internal int RejectedClaims {
		get {
			var sum = 0;
			foreach (var w in _workers) sum += Volatile.Read(ref w.RejectedClaims);
			return sum;
		}
	}

	internal int WorkersThatActivated {
		get {
			var n = 0;
			foreach (var w in _workers)
				if (Volatile.Read(ref w.Activations) > 0) n++;
			return n;
		}
	}

	internal int ProducerWakes {
		get {
			var sum = 0;
			foreach (var w in _workers) sum += Volatile.Read(ref w.ProducerWakes);
			return sum;
		}
	}

	/// <summary>
	///   Number of times <see cref="IKeyScheduler.WakeAll" /> was actually called for quiescence — NOT the
	///   gate flag, which is always 0 or 1 by construction and so cannot distinguish "gated correctly"
	///   from "never checked, never broadcast."
	/// </summary>
	internal int QuiescenceWakes => Volatile.Read(ref _quiescenceWakes);
}
