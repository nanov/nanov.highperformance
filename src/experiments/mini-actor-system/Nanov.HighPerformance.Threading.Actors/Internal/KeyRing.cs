namespace Nanov.HighPerformance.Threading.Actors.Internal;

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
///   Bounded single-producer / single-consumer ring of key indices.
///   <para>
///     <b>Strictly one producer and one consumer.</b> A worker's self-reschedule must NOT be enqueued
///     here — that would be a second producer, which breaks the ring silently. Self-rescheduled keys go
///     on the worker's private local stack instead (see <c>ActorSystem</c>).
///   </para>
///   Sized to the key map's slot count, so it can never fill: a key index is in at most one place at a
///   time, so the number in flight is bounded by the map (§6).
/// </summary>
internal sealed class KeyRing {
	private readonly int[] _slots;
	private readonly int _mask;
	private int _head; // consumer-written
	private int _tail; // producer-written

	internal KeyRing(int capacity) {
		var n = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(capacity, 2));
		_slots = new int[n];
		_mask = n - 1;
	}

	internal int Capacity => _slots.Length;

	/// <summary>Approximate depth (<c>Tail − Head</c>). Lock-free, racy by a few.</summary>
	internal int CountApprox => Volatile.Read(ref _tail) - Volatile.Read(ref _head);

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryEnqueue(int value) {
		var tail = _tail;
		if (tail - Volatile.Read(ref _head) >= _slots.Length) return false;
		_slots[tail & _mask] = value;
		Volatile.Write(ref _tail, tail + 1); // release: publishes the slot write
		return true;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryDequeue(out int value) {
		var head = _head;
		if (head == Volatile.Read(ref _tail)) { // acquire: pairs with the producer's release
			value = 0;
			return false;
		}

		value = _slots[head & _mask];
		Volatile.Write(ref _head, head + 1);
		return true;
	}
}
