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
		_batchBound = batchBound > 0 ? batchBound : _chunkSize;
	}

	/// <summary>Max messages consumed in one activation before the key is released and rescheduled.</summary>
	public int BatchBound => _batchBound;

	/// <summary>
	///   Enqueue a message for <paramref name="key" />. Single-producer: never call concurrently.
	///   Returns false if the system is completed or disposed.
	/// </summary>
	public bool Dispatch(TKey key, T message) {
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
	}

	public void Dispose() { }

	// ---- test hooks -------------------------------------------------------------------------------

	internal ref KeyCell<TKey> CellOf(TKey key) => ref _map.GetOrAddRef(key, out _, out _);

	internal int ScheduledCountApprox => _scheduled.CountApprox;

	internal int DispatchedCount => _dispatched;
}
