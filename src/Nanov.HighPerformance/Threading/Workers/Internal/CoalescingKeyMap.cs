namespace Nanov.HighPerformance.Threading.Workers.Internal;

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

/// <summary>
///   One inline cell of <see cref="CoalescingKeyMap{TKey, TValue}" />: the key, its coalesced value, and a
///   4-state lock/queue word (<c>0 Free</c> / <c>1 Available</c> / <c>2 Merging</c> / <c>3 Consuming</c>,
///   driven by the worker). Stored inline in the map's array and accessed by <c>ref</c> — never a heap
///   object.
/// </summary>
internal struct CoalescingCell<TKey, TValue> {
	public TKey Key;
	public TValue Value;
	public int State;
}

/// <summary>
///   Producer-private, <b>fixed-capacity, no-relocation</b> open-addressing (linear-probe) map
///   <c>TKey → CoalescingCell</c> for the coalescing worker. Cells live inline and are reached by a
///   <b>stable slot index</b> (handed to the consumer through the ring): a placed cell never moves, so a
///   held <c>ref</c>/index stays valid. Eviction is an <b>in-place tombstone</b> of a <c>Free</c>
///   (<c>State == 0</c>) cell — never a relocation. Structural ops (probe/insert/evict) are
///   producer-only; the consumer only reaches a cell by index via <see cref="RefAt" /> and touches the
///   cell's <c>State</c>/<c>Value</c>, never the map metadata.
/// </summary>
internal sealed class CoalescingKeyMap<TKey, TValue> : IDisposable
	where TKey : notnull, IEquatable<TKey> {
	private const byte Empty = 0;
	private const byte Occupied = 1;
	private const byte Tombstone = 2;

	private CoalescingCell<TKey, TValue>[] _cells;
	private byte[] _control; // producer-only slot metadata
	private readonly int _mask;
	private int _count; // occupied (non-tombstone) slots

	internal CoalescingKeyMap(int capacity) {
		// Slots sized to ~2× the max dirty set so load factor stays low and there's room for tombstones.
		var slots = (int)BitOperations.RoundUpToPowerOf2((uint)(Math.Max(capacity, 4) * 2));
		_cells = new CoalescingCell<TKey, TValue>[slots];
		_control = new byte[slots];
		_mask = slots - 1;
		_count = 0;
	}

	internal int Count => _count;
	internal int SlotCount => _cells.Length;

	/// <summary>Ref to the cell at a stable slot <paramref name="index" /> (the consumer's path).</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal ref CoalescingCell<TKey, TValue> RefAt(int index) =>
		ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_cells), (nint)(uint)index);

	/// <summary>
	///   Get the cell for <paramref name="key" /> (by <c>ref</c>) or insert a fresh one. On insert
	///   (<paramref name="existing" /> false) the cell's <c>Key</c> is set; the caller initializes
	///   <c>Value</c>/<c>State</c>. <paramref name="index" /> is the stable slot handle. Producer-only.
	///   The probe reads through unchecked refs (<c>slot</c> is always masked into range), so no bounds
	///   checks; the full-pass/evict path is cold (out of line).
	/// </summary>
	internal ref CoalescingCell<TKey, TValue> GetOrAddRef(TKey key, out bool existing, out int index) {
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
				firstFree = slot; // ctl == Tombstone (Empty handled above)
			}

			slot = (slot + 1) & mask;
		}

		// No empty slot in a full pass → reuse a tombstone or evict drained cells. Rare; kept out of line.
		return ref GetOrAddRefCold(key, firstFree, out existing, out index);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private ref CoalescingCell<TKey, TValue> GetOrAddRefCold(TKey key, int firstFree, out bool existing, out int index) {
		if (firstFree >= 0) {
			Place(firstFree, key);
			existing = false;
			index = firstFree;
			return ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_cells), (nint)(uint)firstFree);
		}

		// Table is full of in-use cells → evict Free (drained) cells in place, then re-probe.
		if (!EvictFree())
			throw new InvalidOperationException(
				"CoalescingKeyMap is full of dirty cells — concurrent dirty keys exceeded capacity.");

		return ref GetOrAddRef(key, out existing, out index);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void Place(int slot, TKey key) {
		var offset = (nint)(uint)slot;
		Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_control), offset) = Occupied;
		ref var cell = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_cells), offset);
		cell = default;
		cell.Key = key;
		_count++;
	}

	// Tombstone every Free (State == 0) cell, reclaiming its slot. Returns true if it freed at least one.
	// State is acquire-read because the consumer publishes State→0 under the cell lock. Cold path.
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
			if (Volatile.Read(ref cell.State) != 0) continue;
			ctl = Tombstone;
			cell = default; // drop the key/value so it isn't pinned and won't match a probe
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
