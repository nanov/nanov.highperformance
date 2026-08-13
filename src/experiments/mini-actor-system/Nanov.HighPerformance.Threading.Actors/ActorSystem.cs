namespace Nanov.HighPerformance.Threading.Actors;

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors.Internal;

/// <summary>
///   Key-affine message processor: a single producer dispatches <c>(key, message)</c>; workers process
///   them with <b>strict FIFO order per key</b> and exclusive per-key ownership.
///   <para>
///     Single producer is the caller's contract, as with the SPSC worker family. Ownership is arbitrated
///     by a 3-state machine on each key's cell; storage is a chunk of a shared slab used as that key's
///     bounded mailbox.
///   </para>
/// </summary>
public sealed class ActorSystem<TKey, T, THandler> : IDisposable
	where TKey : notnull, IEquatable<TKey>
	where THandler : struct, IActorHandler<TKey, T> {
	private readonly KeyCellMap<TKey> _map;
	private readonly ChunkSlab<T> _slab;
	private readonly KeyRing _scheduled;
	private readonly int _chunkSize;
	private readonly int _chunkMask;
	private readonly int _batchBound;
	private THandler _handler; // mutable struct — calls go through the field so state persists

	private long _dispatched; // producer-private; no atomics

	private readonly int[] _local; // worker-private reschedule stack — never touched by the producer
	private int _localCount;
	private Thread? _worker;

	// Explicit initializer, not just the implicit default: nothing else assigns this field until
	// Dispose (Task 8), and an unassigned field trips CS0649, which is a build error here.
	private volatile bool _stopped = false;

	private long _processedLocal;    // worker-private running count
	private long _processedPublished; // published for the quiescence check (Task 7)
	private int _activations; // test hook: counts Activate calls, so batchBound has a mechanical guard

	private readonly TaskCompletionSource _completion =
		new(TaskCreationOptions.RunContinuationsAsynchronously);
	private readonly ManualResetEventSlim _workAvailable = new(false);
	private readonly ManualResetEventSlim _spaceAvailable = new(false);
	private volatile bool _producerParked;
	private int _producerWakes; // test hook: counts Sets actually delivered to a parked producer
	private long _dispatchedFinal;
	private volatile bool _completed;
	private Exception? _completionError;
	private int _completeGate;
	private int _disposeGate;

	/// <param name="handler">
	///   The single shared handler. At one worker there is no per-worker copy — per-worker copies
	///   arrive with the multi-worker phase.
	/// </param>
	/// <param name="keyCapacity">Max live keys.</param>
	/// <param name="chunkCount">Max concurrently-dirty keys.</param>
	/// <param name="chunkSize">Messages one key may hold before backpressure.</param>
	/// <param name="batchBound">Max messages per activation; 0 ⇒ <paramref name="chunkSize" />.</param>
	public ActorSystem(THandler handler, int keyCapacity, int chunkCount, int chunkSize, int batchBound = 0) {
		_handler = handler;
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

		_scheduled = new KeyRing(_map.SlotCount);
		_local = new int[_map.SlotCount];
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
			ScheduleFromProducer(index);

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
				ReleaseAndMaybeSchedule(slot, ref cell, head, fromWorker: false);
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
	///     <b>The recheck above does not cover every window — known debt, not silently relied upon.</b>
	///     This side of the handshake IS fenced: <c>_producerParked = true</c> →
	///     <c>Interlocked.MemoryBarrier()</c> → re-read <c>Head</c>. But the worker's wake sites
	///     (<c>Activate</c>'s periodic flush and <c>ReleaseAndMaybeSchedule</c>) publish
	///     <c>Head</c> with a plain <c>Volatile.Write</c> and then read <c>_producerParked</c> — a
	///     release-store followed by an acquire-load of a <b>different</b> location, which is not
	///     StoreLoad-ordered. So the worker can publish <c>Head</c>, read <c>_producerParked</c> as
	///     false (we had not armed yet), and skip the <c>Set</c>, while we arm, fence, and then read a
	///     <i>stale</i> <c>Head</c> — a wake can be missed entirely. It is the bounded 20&#160;ms
	///     <c>_spaceAvailable.Wait(TimeSpan)</c> below — not the recheck — that covers that case:
	///     nothing hangs, but a missed wake costs a full 20&#160;ms poll interval, a tail-latency cliff
	///     on the backpressure path. The fix (a short spin before parking, matching
	///     <c>SpRingCore.WaitForSpaceSlow</c>, or a fence before the <c>_producerParked</c> read on the
	///     worker side) is deferred pending measurement.
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
	///   Park until the worker drains a key, which is what makes a chunk harvestable. There is no
	///   producer-visible predicate to re-test here — only a harvest adds a chunk, and only this thread
	///   harvests — so this arms and waits without a recheck.
	/// </summary>
	private bool WaitForChunk() {
		_spaceAvailable.Reset();
		_producerParked = true;
		try {
			Interlocked.MemoryBarrier();
			if (_stopped || _completed) return false;
			_spaceAvailable.Wait(TimeSpan.FromMilliseconds(20));
			return !_stopped && !_completed;
		}
		finally {
			_producerParked = false;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void ScheduleFromProducer(int index) {
		if (!_scheduled.TryEnqueue(index))
			throw new InvalidOperationException(
				"scheduler ring full — it is sized to the map, so this indicates a key index was scheduled twice.");
		_workAvailable.Set();
	}

	/// <summary>Start the worker thread.</summary>
	public void Start() {
		_worker = new Thread(RunLoop) { IsBackground = true, Name = $"Actor<{typeof(TKey).Name}>" };
		_worker.Start();
	}

	private void RunLoop() {
		try {
			while (!_stopped) {
				if (TryAcquire(out var index)) {
					Activate(index);
					continue;
				}

				if (!WaitForWork()) break;
			}
		}
		catch (Exception ex) {
			// A background-thread escape would kill the host process with Completion unresolved.
			// Fault Completion instead so the failure is observable.
			_completionError ??= ex;
			SafeOnError(ex);
		}
		finally {
			PublishProcessed();

			// _completed means a drain was REQUESTED, not that it finished. A Dispose racing an
			// in-flight TryComplete drops messages, so the exit is only clean if we actually reached
			// quiescence. Read _completed BEFORE the counters — same acquire ordering rule as
			// IsQuiescent, and && short-circuits in the right order.
			var drainedCleanly = _completed &&
				Volatile.Read(ref _processedPublished) == Volatile.Read(ref _dispatchedFinal);
			var hardStopped = _stopped && !drainedCleanly;
			Exception? error = hardStopped ? new OperationCanceledException() : _completionError;
			try {
				_handler.OnCompleted(error);
			}
			catch (Exception ex) {
				SafeOnError(ex);
			}

			// Hard stop cancels — mirrors the worker family (SpRingCore.SetCompletion). A genuine
			// soft-complete-with-exception is the only case that actually faults Completion.
			if (hardStopped) _completion.TrySetCanceled();
			else if (error is not null) _completion.TrySetException(error);
			else _completion.TrySetResult();
		}
	}

	/// <summary>
	///   Take the next key. The worker-private local stack has priority over the producer's ring, which
	///   keeps a busy key pinned to this worker (affinity) and — critically — keeps the ring
	///   single-producer, since self-rescheduled keys never enter it.
	/// </summary>
	private bool TryAcquire(out int index) {
		if (_localCount > 0) {
			index = _local[--_localCount];
			return true;
		}

		return _scheduled.TryDequeue(out index);
	}

	private void Activate(int index) {
		_activations++;
		ref var cell = ref _map.RefAt(index);
		Interlocked.Exchange(ref cell.State, CellState.Owned);

		var chunk = cell.ChunkBase;
		if (chunk == Chunk.None) {
			// Spurious schedule: the producer harvested this cell's chunk while we were between our
			// State release and our Tail recheck. There is nothing to drain and no chunk to drain from
			// — release and move on. Without this the drain loop would index the slab at Chunk.None
			// (-1), and SlotRef is deliberately unchecked.
			ReleaseAndMaybeSchedule(index, ref cell, cell.Head, fromWorker: true);
			return;
		}

		var head = cell.Head;
		var n = 0;
		while (n < _batchBound && head != Volatile.Read(ref cell.Tail)) {
			if (_stopped) break;
			try {
				_handler.Handle(cell.Key, _slab.SlotRef(chunk, head & _chunkMask));
			}
			catch (Exception ex) {
				SafeOnError(ex);
			}

			head++;
			n++;
			_processedLocal++;
			if ((n & 31) == 0) {
				Volatile.Write(ref cell.Head, head);
				PublishProcessed();
				if (_producerParked) {
					_producerWakes++;
					_spaceAvailable.Set();
				}
			}
		}

		ReleaseAndMaybeSchedule(index, ref cell, head, fromWorker: true);
	}

	/// <summary>
	///   Release ownership, and reschedule if the mailbox is not empty. The ordering here is
	///   load-bearing — see §5.3. Shared by the worker and by the producer's cold paths (Task 10), which
	///   is why it takes <paramref name="fromWorker" /> rather than being open-coded twice.
	/// </summary>
	private void ReleaseAndMaybeSchedule(int index, ref KeyCell<TKey> cell, int head, bool fromWorker) {
		if (fromWorker) {
			Volatile.Write(ref cell.Head, head);
			PublishProcessed();
			if (_producerParked) {
				_producerWakes++;
				_spaceAvailable.Set();
			}
		}

		// FULL FENCE, deliberately not Volatile.Write: the Tail load below must not be hoisted above
		// this store. Producer and worker each store to their own variable then load the other's, with a
		// full fence between (Dekker) — so they cannot both miss each other, and the State CAS (never
		// the emptiness check) is the arbiter of who reschedules.
		Interlocked.Exchange(ref cell.State, CellState.Free);

		if (Volatile.Read(ref cell.Tail) == head) return;
		// else: drained. The chunk is now reclaimable, but we must NOT touch ChunkBase — it is
		// producer-owned. The producer discovers it on its next cold harvest scan (Task 10).

		if (Interlocked.CompareExchange(ref cell.State, CellState.Scheduled, CellState.Free) != CellState.Free)
			return; // the producer got there first and already scheduled it — no double-schedule

		if (fromWorker) PushLocal(index);
		else ScheduleFromProducer(index);
	}

	private void PushLocal(int index) => _local[_localCount++] = index;

	private void PublishProcessed() => Volatile.Write(ref _processedPublished, _processedLocal);

	private void SafeOnError(Exception error) {
		try {
			_handler.OnError(error);
		}
		catch {
			// A throwing OnError must not kill the worker.
		}
	}

	/// <summary>Resolves once the worker has drained and exited.</summary>
	public Task Completion => _completion.Task;

	/// <summary>
	///   Signal that no more messages will be dispatched: drain everything queued, then exit. A non-null
	///   <paramref name="exception" /> faults <see cref="Completion" />. Returns false if already
	///   completing. Producer thread only — and it must be the producer's LAST act (see below).
	/// </summary>
	public bool TryComplete(Exception? exception = null) {
		if (Interlocked.Exchange(ref _completeGate, 1) != 0) return false;
		_completionError = exception;

		// Publish the final count BEFORE the flag: the quiescence check reads the flag first, so this
		// ordering is what makes "observed completed ⇒ observes the full dispatched count" hold.
		Volatile.Write(ref _dispatchedFinal, _dispatched);
		_completed = true;

		// Shutdown wakes UNCONDITIONALLY — never flag-gated. Without this, a system that was already
		// drained and parked when TryComplete arrives would never be woken by anything: no further
		// message will ever be processed, so no worker will ever re-evaluate the predicate.
		WakeAll();
		_spaceAvailable.Set();

		// A system that was constructed but never started has no worker thread, so RunLoop's finally
		// — the only other place Completion resolves — will never run. Resolve it here instead;
		// TrySet* is idempotent so this cannot race a worker that starts resolving concurrently.
		if (_worker is null) {
			if (exception is null) _completion.TrySetResult();
			else _completion.TrySetException(exception);
		}

		return true;
	}

	private void WakeAll() => _workAvailable.Set();

	/// <summary>
	///   Park until work arrives or the system should exit. Returns false to exit the loop.
	///   <para>
	///     Arm-then-recheck: the flag is cleared, then work and quiescence are re-tested, and only then
	///     do we block. Collapsing this into check-then-park is the classic lost wakeup, and it passes
	///     every test whose worker never actually sleeps.
	///   </para>
	/// </summary>
	private bool WaitForWork() {
		for (var spin = 0; spin < 32; spin++) {
			if (HasWork() || _stopped) return !_stopped;
			Thread.SpinWait(64);
		}

		while (true) {
			_workAvailable.Reset();
			Interlocked.MemoryBarrier();
			if (HasWork() || _stopped) return !_stopped;
			if (IsQuiescent()) return false;
			_workAvailable.Wait();
			if (HasWork() || _stopped) return !_stopped;
			if (IsQuiescent()) return false;
		}
	}

	private bool HasWork() => _localCount > 0 || _scheduled.CountApprox > 0;

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
	///   _completed) with the processed count as the new Tail.
	/// </summary>
	private bool IsQuiescent() {
		if (!_completed) return false;
		var processed = Volatile.Read(ref _processedPublished);
		// >= not ==: an undercount only delays quiescence, but any overshoot with == would hang
		// forever rather than exit late.
		return processed >= Volatile.Read(ref _dispatchedFinal);
	}

	/// <summary>
	///   Hard stop: drop queued messages and exit. The message currently inside the handler still
	///   finishes — tearing down under a running handler is something the family never does.
	///   Workers are joined against one shared budget, not one per worker. A system that was
	///   constructed but never started has no worker thread to resolve <see cref="Completion" />, so
	///   this resolves it directly as cancelled in that case.
	/// </summary>
	public void Dispose() {
		if (Interlocked.Exchange(ref _disposeGate, 1) != 0) return;
		_stopped = true;
		WakeAll();
		_spaceAvailable.Set();
		if (_worker is null) _completion.TrySetCanceled();
		else _worker.Join(TimeSpan.FromSeconds(1));
		// The events are deliberately NOT disposed: the producer is a caller-owned thread that may be
		// parked inside Wait() right now, and disposing under it throws ObjectDisposedException on
		// their thread. Letting the finalizer reclaim them is the correct trade here.
	}

	// ---- test hooks -------------------------------------------------------------------------------

	internal ref KeyCell<TKey> CellOf(TKey key) => ref _map.GetOrAddRef(key, out _, out _);

	internal int ScheduledCountApprox => _scheduled.CountApprox;

	internal long DispatchedCount => _dispatched;

	internal int Activations => Volatile.Read(ref _activations);

	internal int ProducerWakes => Volatile.Read(ref _producerWakes);
}
