namespace Nanov.HighPerformance.Threading.Actors.Internal;

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
///   A Chase-Lev work-stealing deque of key indices, over a FIXED buffer.
///   <para>
///     The owner pushes and pops the <b>bottom</b> — LIFO, and on the uncontended path atomic-free
///     apart from one fence. Thieves take the <b>top</b> and arbitrate with a CAS. Owner and thieves
///     only contend when exactly one item remains, which is the case
///     <see cref="TryPopBottom" />'s CAS exists to settle.
///   </para>
///   <para>
///     <b>Memory ordering follows Lê / Pop / Cohen / Zappa Nardelli (2013), not the 2005 paper.</b>
///     Two full StoreLoad fences are load-bearing and neither can be a <c>Volatile</c> write: on
///     ARM64 a release store is not ordered against a subsequent load of a DIFFERENT location, and
///     both the pop and the steal do exactly that — store or load one cursor, then load the other.
///     Drop either fence and owner and thief can both take the last item, handing one key to two
///     workers and breaking the exclusive-ownership invariant the whole system rests on.
///   </para>
///   <para>
///     <b>Fixed buffer, no growth.</b> A key index is in at most one place at a time, so the number
///     in flight is bounded by the key map; <see cref="TryPushBottom" /> returns <see langword="false" />
///     when full and the caller spills to the global queue, the same escape
///     <c>ThreadPoolWorkQueue</c> uses. That refusal is also what makes the fixed buffer SAFE: a
///     thief that has read <c>_buffer[top]</c> but not yet won its CAS cannot have that slot
///     overwritten, because overwriting it needs <c>bottom - top &gt;= capacity</c> while <c>top</c>
///     is still at the value the thief read — precisely when the push refuses.
///   </para>
/// </summary>
internal sealed class WorkStealingDeque {
	private readonly int[] _buffer;
	private readonly int _mask;

	// Padded apart: the owner writes _bottom on every push and pop while thieves CAS _top. Sharing a
	// line would put a steal attempt in the owner's write path on every single push.
	private PaddedLong _top;
	private PaddedLong _bottom;

	internal WorkStealingDeque(int capacity) {
		ArgumentOutOfRangeException.ThrowIfGreaterThan(capacity, 1 << 30);
		var n = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(capacity, 2));
		_buffer = new int[n];
		_mask = n - 1;
	}

	internal int Capacity => _buffer.Length;

	/// <summary>Approximate depth. Racy by design — used to decide whether stealing is worth a scan.</summary>
	internal int CountApprox {
		get {
			var bottom = Volatile.Read(ref _bottom.Value);
			var top = Volatile.Read(ref _top.Value);
			var count = bottom - top;
			return count < 0 ? 0 : (int)count;
		}
	}

	/// <summary>
	///   OWNER ONLY. Returns <see langword="false" /> when full so the caller can spill to the global
	///   queue.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryPushBottom(int value) {
		var bottom = _bottom.Value; // owner-private
		var top = Volatile.Read(ref _top.Value);
		if (bottom - top >= _buffer.Length) return false;
		_buffer[(int)(bottom & _mask)] = value;
		Volatile.Write(ref _bottom.Value, bottom + 1); // release: publishes the slot write
		return true;
	}

	/// <summary>OWNER ONLY. Takes the newest item — the one whose cell lines are warmest.</summary>
	internal bool TryPopBottom(out int value) {
		var bottom = _bottom.Value - 1;
		Volatile.Write(ref _bottom.Value, bottom);

		// The load-bearing fence. Without it the load of _top below can be reordered ahead of the
		// store to _bottom above, and a thief racing for the last item sees a stale bottom while we
		// see a stale top — both take it.
		Interlocked.MemoryBarrier();

		var top = Volatile.Read(ref _top.Value);
		if (top > bottom) {
			// Empty. Restore bottom, or the next push writes the wrong slot.
			Volatile.Write(ref _bottom.Value, bottom + 1);
			value = 0;
			return false;
		}

		value = _buffer[(int)(bottom & _mask)];
		if (top != bottom) return true; // more than one left: no thief can be after this slot

		// Exactly one left, and a thief may be taking it right now. The CAS settles it.
		var won = Interlocked.CompareExchange(ref _top.Value, top + 1, top) == top;
		Volatile.Write(ref _bottom.Value, bottom + 1); // the deque is empty either way
		if (won) return true;
		value = 0;
		return false;
	}

	/// <summary>ANY THIEF. Takes the oldest item; one key per steal (see the type docs on granularity).</summary>
	internal bool TrySteal(out int value) {
		var top = Volatile.Read(ref _top.Value);

		// Second load-bearing fence: _bottom must be loaded AFTER _top, and on ARM64 two acquire
		// loads of different locations do not give that on their own.
		Interlocked.MemoryBarrier();

		var bottom = Volatile.Read(ref _bottom.Value);
		if (top >= bottom) {
			value = 0;
			return false;
		}

		value = _buffer[(int)(top & _mask)];
		if (Interlocked.CompareExchange(ref _top.Value, top + 1, top) == top) return true;

		// Lost to the owner or another thief. Report empty-handed; the caller moves to the next victim.
		value = 0;
		return false;
	}
}
