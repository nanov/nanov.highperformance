namespace Nanov.HighPerformance.Threading.Workers.Internal;

using System;
using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

/// <summary>
///   Producer-private open-addressing (linear-probe) map <c>TKey → int seq</c> for the compacting
///   worker: it remembers, per key, the absolute ring sequence of that key's currently-queued slot.
///   <b>Single-threaded</b> — touched only by the producer thread, so it needs no synchronization.
///   Inspired by Prague's <c>ValueDictionary</c> hot-path layout (metadata slot → packed entry index)
///   but trimmed to exactly what the worker needs: lookup, insert-or-update, and a compact-and-rebuild
///   that evicts stale (already-consumed) keys. No per-key delete (so probe chains never hold
///   tombstones); eviction is the bulk <see cref="Compact{TPred}" />. ArrayPool-backed, zero
///   steady-state allocation.
/// </summary>
internal struct CompactingKeyMap<TKey> : IDisposable where TKey : notnull, IEquatable<TKey> {
	private int[] _metadata;          // probe slot → entry index; -1 = empty
	private TKey[] _keys;             // entry index → key   (packed [0, _count))
	private int[] _seqs;              // entry index → seq   (packed [0, _count))
	private int _slotMask;            // _metadata.Length - 1 (power of two)
	private int _count;               // live + stale entries packed in [0, _count)
	private readonly int _entriesCapacity;

	/// <summary>
	///   <paramref name="ringCapacity" /> is the worker's (power-of-two) ring capacity. Entry capacity is
	///   sized to 2× so stale entries can accumulate between compactions while still guaranteeing room: at
	///   most <c>ringCapacity</c> keys are ever pending, so a compaction always reclaims enough.
	/// </summary>
	internal CompactingKeyMap(int ringCapacity) {
		_entriesCapacity = Math.Max(ringCapacity * 2, 4);
		// Load factor ≈ 0.75 → slots ≥ entries * 4/3, rounded up to a power of two.
		var slots = (int)BitOperations.RoundUpToPowerOf2((uint)(_entriesCapacity * 4L / 3 + 1));
		_slotMask = slots - 1;
		_metadata = ArrayPool<int>.Shared.Rent(slots);
		Array.Fill(_metadata, -1, 0, slots);
		_keys = ArrayPool<TKey>.Shared.Rent(_entriesCapacity);
		_seqs = ArrayPool<int>.Shared.Rent(_entriesCapacity);
		_count = 0;
	}

	internal readonly int Count => _count;
	internal readonly bool IsFull => _count >= _entriesCapacity;

	/// <summary>Look up the recorded seq for <paramref name="key" />. False if absent.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal readonly bool TryGetSeq(TKey key, out int seq) {
		var mask = _slotMask;
		var slot = key.GetHashCode() & mask;
		ref var meta0 = ref MemoryMarshal.GetArrayDataReference(_metadata);
		ref var keys0 = ref MemoryMarshal.GetArrayDataReference(_keys);
		ref var seqs0 = ref MemoryMarshal.GetArrayDataReference(_seqs);
		while (true) {
			var idx = Unsafe.Add(ref meta0, slot);
			if (idx < 0) {
				seq = 0;
				return false;
			}

			if (Unsafe.Add(ref keys0, idx).Equals(key)) {
				seq = Unsafe.Add(ref seqs0, idx);
				return true;
			}

			slot = (slot + 1) & mask;
		}
	}

	/// <summary>
	///   Insert <paramref name="key" /> → <paramref name="seq" />, or update its seq if already present.
	///   The caller MUST guarantee room (see <see cref="IsFull" /> / <see cref="Compact{TPred}" />) before
	///   calling when the key may be new; an update of an existing key never grows the map.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void Set(TKey key, int seq) {
		var mask = _slotMask;
		var slot = key.GetHashCode() & mask;
		ref var meta0 = ref MemoryMarshal.GetArrayDataReference(_metadata);
		ref var keys0 = ref MemoryMarshal.GetArrayDataReference(_keys);
		ref var seqs0 = ref MemoryMarshal.GetArrayDataReference(_seqs);
		while (true) {
			var idx = Unsafe.Add(ref meta0, slot);
			if (idx < 0) break;
			if (Unsafe.Add(ref keys0, idx).Equals(key)) {
				Unsafe.Add(ref seqs0, idx) = seq;
				return;
			}

			slot = (slot + 1) & mask;
		}

		var count = _count;
		Unsafe.Add(ref keys0, count) = key;
		Unsafe.Add(ref seqs0, count) = seq;
		Unsafe.Add(ref meta0, slot) = count;
		_count = count + 1;
	}

	/// <summary>
	///   Compact-and-rebuild: keep only entries whose seq satisfies <paramref name="pred" /> (the live
	///   "pending" ones), drop the rest, then rebuild the probe metadata. O(capacity), producer-side.
	/// </summary>
	internal void Compact<TPred>(in TPred pred) where TPred : struct, ISeqKeepPredicate {
		ref var keys0 = ref MemoryMarshal.GetArrayDataReference(_keys);
		ref var seqs0 = ref MemoryMarshal.GetArrayDataReference(_seqs);
		var count = _count;
		var write = 0;
		for (var read = 0; read < count; read++) {
			var seq = Unsafe.Add(ref seqs0, read);
			if (!pred.Keep(seq)) continue;
			if (write != read) {
				Unsafe.Add(ref keys0, write) = Unsafe.Add(ref keys0, read);
				Unsafe.Add(ref seqs0, write) = seq;
			}

			write++;
		}

		_count = write;

		var slots = _slotMask + 1;
		Array.Fill(_metadata, -1, 0, slots);
		var mask = _slotMask;
		ref var meta0 = ref MemoryMarshal.GetArrayDataReference(_metadata);
		for (var i = 0; i < write; i++) {
			var slot = Unsafe.Add(ref keys0, i).GetHashCode() & mask;
			while (Unsafe.Add(ref meta0, slot) >= 0) slot = (slot + 1) & mask;
			Unsafe.Add(ref meta0, slot) = i;
		}
	}

	public void Dispose() {
		if (_metadata is null) return;
		ArrayPool<int>.Shared.Return(_metadata);
		ArrayPool<TKey>.Shared.Return(_keys, RuntimeHelpers.IsReferenceOrContainsReferences<TKey>());
		ArrayPool<int>.Shared.Return(_seqs);
		_metadata = null!;
		_keys = null!;
		_seqs = null!;
		_count = 0;
	}
}
