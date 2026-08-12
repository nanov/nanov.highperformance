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

	private int _dispatched; // producer-private; no atomics

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
	private long _dispatchedFinal;
	private volatile bool _completed;
	private Exception? _completionError;
	private int _completeGate;

	/// <param name="handler">Struct handler; copied into each worker.</param>
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
	///   Enqueue a message for <paramref name="key" />. Single-producer: never call concurrently.
	///   Returns false if the system is completed or disposed.
	/// </summary>
	public bool Dispatch(TKey key, T message) {
		if (_completed || _stopped) return false;

		ref var cell = ref _map.GetOrAddRef(key, out _, out var index);
		if (cell.ChunkBase == Chunk.None && !TryBindChunk(ref cell)) return false;

		ref var slot = ref _slab.SlotRef(cell.ChunkBase, cell.Tail & _chunkMask);
		_handler.OnSlotReused(ref slot);
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

	private bool TryBindChunk(ref KeyCell<TKey> cell) {
		if (!_slab.TryRent(out var chunk)) return false; // backpressure lands here in Task 9
		cell.ChunkBase = chunk;
		cell.Head = 0;
		cell.Tail = 0;
		cell.CachedHead = 0;
		return true;
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
		finally {
			PublishProcessed();
			Exception? error = _stopped && !_completed ? new OperationCanceledException() : _completionError;
			try {
				_handler.OnCompleted(error);
			}
			catch (Exception ex) {
				SafeOnError(ex);
			}

			if (error is null) _completion.TrySetResult();
			else _completion.TrySetException(error);
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
		Volatile.Write(ref cell.Head, head);
		PublishProcessed();

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
		return processed == Volatile.Read(ref _dispatchedFinal);
	}

	public void Dispose() {
		_stopped = true;
		WakeAll();
		_worker?.Join(TimeSpan.FromSeconds(1));
		// The events are deliberately NOT disposed: the producer is a caller-owned thread that may be
		// parked inside Wait() right now, and disposing under it throws ObjectDisposedException on
		// their thread. Letting the finalizer reclaim them is the correct trade here.
	}

	// ---- test hooks -------------------------------------------------------------------------------

	internal ref KeyCell<TKey> CellOf(TKey key) => ref _map.GetOrAddRef(key, out _, out _);

	internal int ScheduledCountApprox => _scheduled.CountApprox;

	internal int DispatchedCount => _dispatched;

	internal int Activations => Volatile.Read(ref _activations);
}
