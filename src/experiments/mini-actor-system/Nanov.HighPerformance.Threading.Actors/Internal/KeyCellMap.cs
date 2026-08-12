namespace Nanov.HighPerformance.Threading.Actors.Internal;

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

/// <summary>
///   Producer-private, <b>fixed-capacity, no-relocation</b> open-addressing (linear-probe) map
///   <c>TKey → </c><see cref="KeyCell{TKey}" />. Cells live inline and are reached by a <b>stable slot
///   index</b> (which is what the scheduler carries): a placed cell never moves, so a held index stays
///   valid. Eviction is an in-place tombstone, never a relocation.
///   <para>
///     Structural operations (probe / insert / evict) are <b>producer-only</b>. Workers reach a cell
///     by index via <see cref="RefAt" /> and touch only <c>State</c> and <c>Head</c> — never the map
///     metadata — so the probe can never race a key clear.
///   </para>
///   Forked from <c>CoalescingKeyMap</c> in the Workers assembly; the probe and eviction shapes are
///   deliberately identical.
/// </summary>
internal sealed class KeyCellMap<TKey>
	where TKey : notnull, IEquatable<TKey> {
	private const byte Empty = 0;
	private const byte Occupied = 1;
	private const byte Tombstone = 2;

	private readonly KeyCell<TKey>[] _cells;
	private readonly byte[] _control;
	private readonly int _mask;
	private int _count;

	/// <param name="capacity">Max live keys. Slots are sized to ~2× so the load factor stays low.</param>
	internal KeyCellMap(int capacity) {
		var slots = (int)BitOperations.RoundUpToPowerOf2((uint)(Math.Max(capacity, 4) * 2));
		_cells = new KeyCell<TKey>[slots];
		_control = new byte[slots];
		_mask = slots - 1;
	}

	internal int Count => _count;
	internal int SlotCount => _cells.Length;

	internal bool IsOccupied(int slot) => _control[slot] == Occupied;

	/// <summary>Ref to the cell at a stable slot <paramref name="index" /> — the worker's path.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal ref KeyCell<TKey> RefAt(int index) =>
		ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_cells), (nint)(uint)index);

	/// <summary>
	///   Get the cell for <paramref name="key" /> by ref, or insert a fresh one (<c>Free</c>, no chunk,
	///   cursors zeroed). <paramref name="index" /> is the stable slot handle. Producer-only.
	/// </summary>
	internal ref KeyCell<TKey> GetOrAddRef(TKey key, out bool existing, out int index) {
		var mask = _mask;
		ref var cells = ref MemoryMarshal.GetArrayDataReference(_cells);
		ref var control = ref MemoryMarshal.GetArrayDataReference(_control);

		var slot = key.GetHashCode() & mask;
		var firstFree = -1;
		for (var n = 0; n <= mask; n++) {
			var ctl = Unsafe.Add(ref control, (nint)(uint)slot);
			if (ctl == Empty) {
				var at = firstFree >= 0 ? firstFree : slot;
				Place(at, key);
				existing = false;
				index = at;
				return ref Unsafe.Add(ref cells, (nint)(uint)at);
			}

			if (ctl == Occupied) {
				ref var cell = ref Unsafe.Add(ref cells, (nint)(uint)slot);
				if (cell.Key.Equals(key)) {
					existing = true;
					index = slot;
					return ref cell;
				}
			} else if (firstFree < 0) {
				firstFree = slot;
			}

			slot = (slot + 1) & mask;
		}

		return ref GetOrAddRefCold(key, firstFree, out existing, out index);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private ref KeyCell<TKey> GetOrAddRefCold(TKey key, int firstFree, out bool existing, out int index) {
		if (firstFree >= 0) {
			Place(firstFree, key);
			existing = false;
			index = firstFree;
			return ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_cells), (nint)(uint)firstFree);
		}

		if (!EvictFree())
			throw new InvalidOperationException(
				"KeyCellMap is full of live cells — concurrent live keys exceeded capacity.");

		return ref GetOrAddRef(key, out existing, out index);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void Place(int slot, TKey key) {
		var offset = (nint)(uint)slot;
		Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_control), offset) = Occupied;
		ref var cell = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_cells), offset);
		cell = default;
		cell.Key = key;
		cell.ChunkBase = Chunk.None;
		_count++;
	}

	/// <summary>
	///   Tombstone every cell that is <c>Free</c> <b>and</b> holds no chunk, reclaiming its slot.
	///   Both conditions matter: a cell holding storage must keep its identity until the producer has
	///   harvested the chunk (§4.4/§5.5), and a non-<c>Free</c> cell is in use by a worker. <c>State</c>
	///   is acquire-read because the worker publishes it. Cold path.
	/// </summary>
	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool EvictFree() {
		var mask = _mask;
		ref var cells = ref MemoryMarshal.GetArrayDataReference(_cells);
		ref var control = ref MemoryMarshal.GetArrayDataReference(_control);
		var freed = false;
		for (var slot = 0; slot <= mask; slot++) {
			ref var ctl = ref Unsafe.Add(ref control, (nint)(uint)slot);
			if (ctl != Occupied) continue;
			ref var cell = ref Unsafe.Add(ref cells, (nint)(uint)slot);
			if (cell.ChunkBase != Chunk.None) continue;

			// CAS, not a plain read: a chunkless-and-Free cell can now be one Harvest() just reclaimed
			// (Task 10), and a worker can still be mid-release on it — between its own State Exchange
			// to Free and its Tail recheck. A plain read-then-tombstone here would let that delayed
			// worker win its reschedule CAS against a slot the producer has already re-keyed for a
			// different TKey. Claiming via CompareExchange closes that window the same way Harvest's
			// own claim does.
			if (Interlocked.CompareExchange(ref cell.State, CellState.Reclaiming, CellState.Free) != CellState.Free)
				continue;
			ctl = Tombstone;
			cell = default;
			cell.ChunkBase = Chunk.None;
			_count--;
			freed = true;
		}

		return freed;
	}
}
