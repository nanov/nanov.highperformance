namespace Nanov.HighPerformance.Threading.Workers.Internal;

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
///   One inline cell of <see cref="AccumulatingKeyMap{TKey, TValue}" />: the key, its accumulator, and a
///   4-state lock/queue word (<c>0 Free</c> / <c>1 Available</c> / <c>2 Merging</c> / <c>3 Consuming</c>,
///   driven by the worker). Stored inline in the map's array and accessed by <c>ref</c> — never a heap
///   object.
/// </summary>
internal struct AccumulatingCell<TKey, TValue> where TValue : struct {
	public TKey Key;
	public TValue Acc;
	public int State;
}

/// <summary>
///   Producer-private, <b>fixed-capacity, no-relocation</b> open-addressing (linear-probe) map
///   <c>TKey → AccumulatingCell</c> for the accumulating worker. Cells live inline and are reached by a
///   <b>stable slot index</b> (handed to the consumer through the ring): a placed cell never moves, so a
///   held <c>ref</c>/index stays valid. Eviction is an <b>in-place tombstone</b> of a <c>Free</c>
///   (<c>State == 0</c>) cell — never a relocation. Structural ops (probe/insert/evict) are
///   producer-only; the consumer only reaches a cell by index via <see cref="RefAt" /> and touches the
///   cell's <c>State</c>/<c>Acc</c>, never the map metadata.
/// </summary>
internal sealed class AccumulatingKeyMap<TKey, TValue> : IDisposable
	where TKey : notnull, IEquatable<TKey>
	where TValue : struct {
	private const byte Empty = 0;
	private const byte Occupied = 1;
	private const byte Tombstone = 2;

	private AccumulatingCell<TKey, TValue>[] _cells;
	private byte[] _control; // producer-only slot metadata
	private readonly int _mask;
	private int _count; // occupied (non-tombstone) slots

	internal AccumulatingKeyMap(int capacity) {
		// Slots sized to ~2× the max dirty set so load factor stays low and there's room for tombstones.
		var slots = (int)BitOperations.RoundUpToPowerOf2((uint)(Math.Max(capacity, 4) * 2));
		_cells = new AccumulatingCell<TKey, TValue>[slots];
		_control = new byte[slots];
		_mask = slots - 1;
		_count = 0;
	}

	internal int Count => _count;
	internal int SlotCount => _cells.Length;

	/// <summary>Ref to the cell at a stable slot <paramref name="index" /> (the consumer's path).</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal ref AccumulatingCell<TKey, TValue> RefAt(int index) => ref _cells[index];

	/// <summary>
	///   Get the cell for <paramref name="key" /> (by <c>ref</c>) or insert a fresh one. On insert
	///   (<paramref name="existing" /> false) the cell's <c>Key</c> is set; the caller initializes
	///   <c>Acc</c>/<c>State</c>. <paramref name="index" /> is the stable slot handle. Producer-only.
	/// </summary>
	internal ref AccumulatingCell<TKey, TValue> GetOrAddRef(TKey key, out bool existing, out int index) {
		var hash = key.GetHashCode();
		while (true) {
			var slot = hash & _mask;
			var firstFree = -1;
			for (var n = 0; n <= _mask; n++) {
				var ctl = _control[slot];
				if (ctl == Empty) {
					var at = firstFree >= 0 ? firstFree : slot;
					Place(at, key);
					existing = false;
					index = at;
					return ref _cells[at];
				}

				if (ctl == Occupied && _cells[slot].Key.Equals(key)) {
					existing = true;
					index = slot;
					return ref _cells[slot];
				}

				if (ctl == Tombstone && firstFree < 0) firstFree = slot;
				slot = (slot + 1) & _mask;
			}

			// No empty slot seen in a full pass. Reuse a tombstone if we found one; else the table is full
			// of in-use cells → evict Free (drained) cells in place and retry.
			if (firstFree >= 0) {
				Place(firstFree, key);
				existing = false;
				index = firstFree;
				return ref _cells[firstFree];
			}

			if (!EvictFree())
				throw new InvalidOperationException(
					"AccumulatingKeyMap is full of dirty cells — concurrent dirty keys exceeded capacity.");
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void Place(int slot, TKey key) {
		_control[slot] = Occupied;
		_cells[slot] = default;
		_cells[slot].Key = key;
		_count++;
	}

	// Tombstone every Free (State == 0) cell, reclaiming its slot. Returns true if it freed at least one.
	// State is acquire-read because the consumer publishes State→0 under the cell lock.
	private bool EvictFree() {
		var freed = false;
		for (var slot = 0; slot <= _mask; slot++) {
			if (_control[slot] != Occupied) continue;
			if (Volatile.Read(ref _cells[slot].State) != 0) continue;
			_control[slot] = Tombstone;
			_cells[slot] = default; // drop the key/value so it isn't pinned and won't match a probe
			_count--;
			freed = true;
		}

		return freed;
	}

	public void Dispose() {
		_cells = null!;
		_control = null!;
	}
}
