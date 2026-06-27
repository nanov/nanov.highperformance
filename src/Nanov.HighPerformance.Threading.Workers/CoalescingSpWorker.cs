namespace Nanov.HighPerformance.Threading.Workers;

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Nanov.HighPerformance.Threading.Workers.Internal;

/// <summary>
///   Single-producer / single-consumer worker that <b>coalesces</b> a payload per key with
///   <b>exactly-once</b> delivery — the coalescing/combine sibling of
///   <see cref="ConflationSpWorker{TKey, TValue, THandler}" /> (which is last-wins). Each key has a stable
///   cell holding a <typeparamref name="TValue" />; the producer merges <c>incoming</c> into it
///   (<see cref="ICoalescingWorkerHandler{TKey, TValue}.Coalesce" />), and the worker thread
///   takes-and-resets it and calls <c>Handle</c> with the coalesced total.
///   <para>
///     A per-cell 4-state lock (<c>Free</c>/<c>Available</c>/<c>Merging</c>/<c>Consuming</c>) makes
///     producer-merge and consumer-take mutually exclusive, so a contribution is delivered exactly once —
///     never double-counted, never lost. Single producer is the caller's contract; the consumer is the
///     worker's own thread. <c>default(TValue)</c> is the empty value (the identity).
///     <typeparamref name="TValue" /> may be a <b>value type</b> — cells live inline in pooled arrays, the
///     dirty queue carries <c>int</c> slot indices, the take is a by-value copy, and there is <b>no
///     per-item allocation</b> — or a <b>reference type</b>, in which case the handler's <c>Coalesce</c>
///     may allocate (the worker itself still allocates nothing per item).
///   </para>
/// </summary>
public sealed class CoalescingSpWorker<TKey, TValue, THandler> : IDisposable, IWorkerQueue
	where TKey : notnull, IEquatable<TKey>
	where THandler : struct, ICoalescingWorkerHandler<TKey, TValue> {
	private const int Free = 0;
	private const int Available = 1;
	private const int Merging = 2;
	private const int Consuming = 3;

	private readonly SpRingCore<int> _ring; // dirty-cell index queue (FIFO SPSC)
	private readonly CoalescingKeyMap<TKey, TValue> _map; // producer-private; cells reached by index
	private THandler _handler; // mutable struct — Coalesce/Handle/OnError/OnCompleted go through this field
#if DEBUG
	private int _producing; // DEBUG-only producer overlap guard (whole Dispatch is the transaction)
#endif

	public CoalescingSpWorker(THandler handler, int capacity, string? threadName = null) {
		_handler = handler;
		_map = new CoalescingKeyMap<TKey, TValue>(capacity);
		_ring = new SpRingCore<int>(capacity,
			threadName ?? $"CoalescingSpWorker<{typeof(TKey).Name},{typeof(TValue).Name}>", RunLoop);
	}

	/// <summary>Completes when the worker has drained and exited (after Complete() or Dispose()).</summary>
	public Task Completion => _ring.Completion;

	/// <summary>Approximate dirty keys currently queued (<c>Tail − Head</c>). Lock-free, racy by a few.</summary>
	public int QueuedApprox => _ring.QueuedApprox;

	/// <summary>Ring capacity (power of two) = max concurrently-dirty keys.</summary>
	public int Capacity => _ring.Capacity;

	public void Start(CancellationToken ct = default) => _ring.Start(ct);

	/// <summary>
	///   Merge <paramref name="value" /> into <paramref name="key" />'s current. Returns false if the
	///   worker is completed/disposed. Conflating an already-dirty key never blocks; a new dirty key blocks
	///   if <see cref="Capacity" /> keys are already dirty (backpressure).
	/// </summary>
	public bool Dispatch(TKey key, TValue value) {
		DebugBeginProduce();
		try {
			ref var cell = ref _map.GetOrAddRef(key, out var existing, out var index);

			// HOT PATH: an existing cell still queued (Available) → coalesce the new value into the
			// accumulated one in place. One CAS; the consumer is locked out (sees Merging and spins); the
			// index is already in the ring so there is no enqueue.
			if (existing && Interlocked.CompareExchange(ref cell.State, Merging, Available) == Available) {
				cell.Value = _handler.Coalesce(ref cell.Value, ref value);
				Volatile.Write(ref cell.State, Available); // release the lock
				return true;
			}

			// Otherwise the cell's value is the identity (default): a FRESH cell (just placed, invisible to
			// the consumer — no lock needed) or a DRAINED/being-consumed existing one (acquire it first).
			// Either way `Coalesce(default, value) == value`, so we store `value` directly — no Coalesce call.
			if (existing)
				AcquireDrained(ref cell); // waits out a Consuming take, then claims Free → Merging

			cell.Value = value;
			Volatile.Write(ref cell.State, Available); // publish (release) before enqueuing the index
			if (!_ring.TryEnqueue(index)) {
				Volatile.Write(ref cell.State, Free); // worker not accepting → undo the dirty mark
				return false;
			}

			return true;
		}
		finally {
			DebugEndProduce();
		}
	}

	// Existing cell whose fast Available→Merging claim failed: it is Free (drained → re-dirty) or Consuming
	// (consumer mid-take → wait). Leaves State == Merging; the consumer has already reset Value to default.
	[MethodImpl(MethodImplOptions.NoInlining)]
	private static void AcquireDrained(ref CoalescingCell<TKey, TValue> cell) {
		var spin = new SpinWait();
		while (Interlocked.CompareExchange(ref cell.State, Merging, Free) != Free)
			spin.SpinOnce(); // Consuming → let the consumer finish (→ Free), then claim
	}

	// Consumer's fast Available→Consuming claim failed: producer mid-merge (Merging → spin then claim) or a
	// spurious Free/Consuming (nothing to take → skip). Returns true if it acquired the cell (Consuming).
	[MethodImpl(MethodImplOptions.NoInlining)]
	private static bool TakeSlow(ref CoalescingCell<TKey, TValue> cell) {
		var spin = new SpinWait();
		while (true) {
			var s = Volatile.Read(ref cell.State);
			if (s == Merging) {
				spin.SpinOnce();
				continue;
			}

			if (s != Available)
				return false; // Free/Consuming → nothing to take (spurious); skip

			if (Interlocked.CompareExchange(ref cell.State, Consuming, Available) == Available)
				return true;

			spin.SpinOnce();
		}
	}

	/// <summary>
	///   Signal that no more items will be dispatched. The worker drains the remaining dirty keys and then
	///   exits. A non-null exception faults <see cref="Completion" />. Returns false if already completing.
	/// </summary>
	public bool TryComplete(Exception? exception = null) => _ring.TryComplete(exception);

	public void Dispose() {
		_ring.Dispose();
		_map.Dispose();
	}

	private void RunLoop() {
		try {
			while (_ring.TryClaimRun(out var head, out var available)) {
				while (head != available && _ring.TryTakeAndAdvance(head++, out var index)) {
					ref var cell = ref _map.RefAt(index);

					// Acquire the cell: Available -> Consuming. Fast path is one CAS; the producer holds it
					// only briefly (Merging) or it's a spurious Free/Consuming → handle that out of line.
					if (Interlocked.CompareExchange(ref cell.State, Consuming, Available) != Available && !TakeSlow(ref cell))
						continue;

					var key = cell.Key;
					var taken = cell.Value; // by-value copy (struct, or a reference for class TValue)
					cell.Value = default!; // reset to the identity (null for a reference TValue) — releases it
					Volatile.Write(ref cell.State, Free); // release -> Free (drained, evictable)

					try {
						_handler.Handle(key, taken);
					}
					catch (Exception ex) {
						_handler.OnError(ex);
					}
				}
			}
		}
		finally {
			var ex = _ring.CompletionException();
			try {
				_handler.OnCompleted(ex);
			}
			catch (Exception onCompletedEx) {
				_handler.OnError(onCompletedEx);
			}

			_ring.SetCompletion(ex);
		}
	}

	[Conditional("DEBUG")]
	private void DebugBeginProduce() {
#if DEBUG
		if (Interlocked.Exchange(ref _producing, 1) != 0)
			throw new InvalidOperationException(
				"Concurrent producer detected — CoalescingSpWorker requires single-producer (non-overlapping) access.");
#endif
	}

	[Conditional("DEBUG")]
	private void DebugEndProduce() {
#if DEBUG
		Volatile.Write(ref _producing, 0);
#endif
	}
}
