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

	/// <summary>The number of slots — the constructor's <c>capacity</c> argument rounded up to a power of two (minimum 2).</summary>
	internal int Capacity => _cells.Length;

	/// <summary>
	///   Approximate depth. Racy by design — read before parking, never for correctness.
	///   <para>
	///     Reads <c>_dequeuePos</c> before <c>_enqueuePos</c> on purpose: a concurrent op landing between
	///     the two reads then biases the result toward OVER-counting (a wasted spin) rather than
	///     under-counting (a park on a queue that still has work) — "read before parking" only holds if
	///     under-reporting can't happen. The opposite of <see cref="WorkStealingDeque.CountApprox" />,
	///     where under-reporting is the safe direction (you just skip a victim) — don't make these two
	///     match.
	///   </para>
	/// </summary>
	internal int CountApprox {
		get {
			var dequeuePos = Volatile.Read(ref _dequeuePos.Value);
			var enqueuePos = Volatile.Read(ref _enqueuePos.Value);
			var count = enqueuePos - dequeuePos;
			return count < 0 ? 0 : (int)count;
		}
	}

	/// <summary>
	///   Claims the next slot and publishes <paramref name="value" /> into it. Returns
	///   <see langword="false" /> when the slot a lap behind has not been consumed yet.
	///   <para>
	///     <b><see langword="false" /> means "not right now", never "the queue is full".</b> A dequeuer
	///     that has already won its CAS on <c>_dequeuePos</c> but has not yet reached its stamp write in
	///     <see cref="TryDequeue" /> leaves that slot looking unavailable for a few instructions, even
	///     though the item it guards is already spoken for and about to be freed. A caller must retry (or
	///     fall back to another sink) rather than latch a single <see langword="false" /> onto "the queue
	///     is at capacity" — see the type docs' "cannot fill in normal operation" note for what a
	///     PERSISTENT <see langword="false" /> actually means here.
	///   </para>
	/// </summary>
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

	/// <summary>
	///   Claims the oldest published slot and returns its value. Returns <see langword="false" /> when
	///   the slot has not been published yet.
	///   <para>
	///     <b><see langword="false" /> means "not right now", never "the queue is empty".</b> An
	///     enqueuer that has already won its CAS on <c>_enqueuePos</c> but has not yet reached its stamp
	///     write in <see cref="TryEnqueue" /> leaves that slot looking unpublished for a few
	///     instructions, even though the item is already committed and about to appear. A single
	///     <see langword="false" /> is not a drain signal: if a worker claims the last visible item and
	///     is descheduled mid-window, every other worker can see <see langword="false" /> here, conclude
	///     the queue is drained, and park — while the item behind that window is still queued. Callers
	///     must retry or back off, never treat one <see langword="false" /> as "empty, nothing left to
	///     do".
	///   </para>
	/// </summary>
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
