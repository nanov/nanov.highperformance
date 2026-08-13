namespace Nanov.HighPerformance.Threading.Actors.Internal;

using System;
using System.Numerics;
using System.Threading;

/// <summary>
///   Bounded multi-producer / multi-consumer queue of key indices, with per-slot sequence stamps
///   (Vyukov). Scheduler D's global submission queue.
///   <para>
///     <b>Why MPMC and not SPMC.</b> The spec describes this edge as producer-single, but a worker
///     whose deque is full spills here as well, so there are <c>workers + 1</c> possible enqueuers.
///     The enqueue side therefore CASes too. It costs one CAS on a path that runs once per
///     ACTIVATION rather than per message, and it removes a sizing invariant that would otherwise
///     be silent to violate.
///   </para>
///   <para>
///     <b>It cannot fill in normal operation.</b> Sized to the key map's slot count; a key index is
///     in at most one place at a time, so the number in flight is bounded by the map. The
///     <see langword="false" /> return is a guard against a mis-sized construction, not backpressure.
///   </para>
///   <para>
///     <b>The sequence stamp is the whole protocol.</b> A slot is writable when its sequence equals
///     the enqueue position, readable when it is one past the dequeue position, and is handed back
///     to the next lap by stamping it <c>pos + capacity</c>. Ownership of a slot therefore never
///     depends on the cursors alone, which is what makes a CAS on the cursor sufficient to claim the
///     slot without a lock.
///   </para>
/// </summary>
internal sealed class GlobalKeyQueue {
	private struct Cell {
		internal long Sequence;
		internal int Value;
	}

	private readonly Cell[] _cells;
	private readonly int _mask;

	// Padded apart: enqueuers and dequeuers hammer these from different cores.
	private PaddedLong _enqueuePos;
	private PaddedLong _dequeuePos;

	internal GlobalKeyQueue(int capacity) {
		ArgumentOutOfRangeException.ThrowIfGreaterThan(capacity, 1 << 30);
		var n = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(capacity, 2));
		_cells = new Cell[n];
		for (var i = 0; i < n; i++)
			_cells[i].Sequence = i;
		_mask = n - 1;
	}

	internal int Capacity => _cells.Length;

	/// <summary>Approximate depth. Racy by design — read before parking, never for correctness.</summary>
	internal int CountApprox {
		get {
			var count = Volatile.Read(ref _enqueuePos.Value) - Volatile.Read(ref _dequeuePos.Value);
			return count < 0 ? 0 : (int)count;
		}
	}

	internal bool TryEnqueue(int value) {
		while (true) {
			var pos = Volatile.Read(ref _enqueuePos.Value);
			ref var cell = ref _cells[(int)(pos & _mask)];
			var difference = Volatile.Read(ref cell.Sequence) - pos;
			if (difference == 0) {
				if (Interlocked.CompareExchange(ref _enqueuePos.Value, pos + 1, pos) != pos) continue;
				cell.Value = value;
				Volatile.Write(ref cell.Sequence, pos + 1); // release: publishes Value to the consumer
				return true;
			}

			if (difference < 0) return false; // full: the slot a lap behind has not been consumed yet
			// difference > 0: another enqueuer already took this position. Re-read and retry.
		}
	}

	internal bool TryDequeue(out int value) {
		while (true) {
			var pos = Volatile.Read(ref _dequeuePos.Value);
			ref var cell = ref _cells[(int)(pos & _mask)];
			var difference = Volatile.Read(ref cell.Sequence) - (pos + 1);
			if (difference == 0) {
				if (Interlocked.CompareExchange(ref _dequeuePos.Value, pos + 1, pos) != pos) continue;
				value = cell.Value;
				Volatile.Write(ref cell.Sequence, pos + _mask + 1); // hand the slot to the next lap
				return true;
			}

			if (difference < 0) { // empty: the producer has not published this slot yet
				value = 0;
				return false;
			}

			// difference > 0: another consumer already took this position. Re-read and retry.
		}
	}
}
