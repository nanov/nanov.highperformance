namespace Nanov.HighPerformance.Threading.Workers;

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Nanov.HighPerformance.Threading.Workers.Internal;

/// <summary>
///   Single-producer / single-consumer worker that <b>accumulates</b> a value-type payload per key with
///   <b>exactly-once</b> delivery — the additive/combine sibling of
///   <see cref="CompactingSpWorker{TKey, TValue, THandler}" /> (which is last-wins over a reference
///   value). Each key has a stable inline cell holding a <typeparamref name="TValue" /> accumulator; the
///   producer merges <c>incoming</c> into it in place (<see cref="IAccumulatingWorkerHandler{TKey, TValue}.Accumulate" />),
///   and the worker thread takes-and-resets it and calls <c>Handle</c> with the drained total. No
///   allocation: cells live inline in pooled arrays; the dirty queue carries <c>int</c> slot indices; the
///   take is a struct copy.
///   <para>
///     A per-cell 4-state lock (<c>Free</c>/<c>Available</c>/<c>Merging</c>/<c>Consuming</c>) makes
///     producer-merge and consumer-take mutually exclusive, so a contribution is delivered exactly once —
///     never double-counted, never lost. Single producer is the caller's contract; the consumer is the
///     worker's own thread. <typeparamref name="TValue" /> is a <see langword="struct" /> and
///     <c>default(TValue)</c> is the empty accumulator.
///   </para>
/// </summary>
public sealed class AccumulatingSpWorker<TKey, TValue, THandler> : IDisposable, IWorkerQueue
	where TKey : notnull, IEquatable<TKey>
	where TValue : struct
	where THandler : struct, IAccumulatingWorkerHandler<TKey, TValue> {
	private const int Free = 0;
	private const int Available = 1;
	private const int Merging = 2;
	private const int Consuming = 3;

	private readonly SpRingCore<int> _ring; // dirty-cell index queue (FIFO SPSC)
	private readonly AccumulatingKeyMap<TKey, TValue> _map; // producer-private; cells reached by index
	private THandler _handler; // mutable struct — Accumulate/Handle/OnError/OnCompleted go through this field
#if DEBUG
	private int _producing; // DEBUG-only producer overlap guard (whole Dispatch is the transaction)
#endif

	public AccumulatingSpWorker(THandler handler, int capacity, string? threadName = null) {
		_handler = handler;
		_map = new AccumulatingKeyMap<TKey, TValue>(capacity);
		_ring = new SpRingCore<int>(capacity,
			threadName ?? $"AccumulatingSpWorker<{typeof(TKey).Name},{typeof(TValue).Name}>", RunLoop);
	}

	/// <summary>Completes when the worker has drained and exited (after Complete() or Dispose()).</summary>
	public Task Completion => _ring.Completion;

	/// <summary>Approximate dirty keys currently queued (<c>Tail − Head</c>). Lock-free, racy by a few.</summary>
	public int QueuedApprox => _ring.QueuedApprox;

	/// <summary>Ring capacity (power of two) = max concurrently-dirty keys.</summary>
	public int Capacity => _ring.Capacity;

	public void Start(CancellationToken ct = default) => _ring.Start(ct);

	/// <summary>
	///   Merge <paramref name="value" /> into <paramref name="key" />'s accumulator. Returns false if the
	///   worker is completed/disposed. Conflating an already-dirty key never blocks; a new dirty key blocks
	///   if <see cref="Capacity" /> keys are already dirty (backpressure).
	/// </summary>
	public bool Dispatch(TKey key, TValue value) {
		DebugBeginProduce();
		try {
			ref var cell = ref _map.GetOrAddRef(key, out _, out var index); // fresh cell is {Key, default, Free}

			// Acquire the cell: (Free | Available) -> Merging. Spin while the consumer holds it (Consuming).
			var spin = new SpinWait();
			int pre;
			while (true) {
				var s = Volatile.Read(ref cell.State);
				if (s == Consuming) {
					spin.SpinOnce();
					continue;
				}

				if (Interlocked.CompareExchange(ref cell.State, Merging, s) == s) {
					pre = s;
					break;
				}

				spin.SpinOnce();
			}

			_handler.Accumulate(ref cell.Acc, value); // in-place merge, exclusive of the consumer

			// Release: Merging -> Available. Enqueue the index only if the cell was Free (newly dirty);
			// an already-Available cell is already in the ring.
			Volatile.Write(ref cell.State, Available);
			if (pre == Free && !_ring.TryEnqueue(index)) {
				Volatile.Write(ref cell.State, Free); // worker not accepting → undo the dirty mark
				return false;
			}

			return true;
		}
		finally {
			DebugEndProduce();
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

					// Acquire the cell: Available -> Consuming. Spin while the producer holds it (Merging).
					var spin = new SpinWait();
					var acquired = false;
					while (true) {
						var s = Volatile.Read(ref cell.State);
						if (s == Merging) {
							spin.SpinOnce();
							continue;
						}

						if (s != Available)
							break; // Free/Consuming → nothing to take (spurious); skip

						if (Interlocked.CompareExchange(ref cell.State, Consuming, Available) == Available) {
							acquired = true;
							break;
						}

						spin.SpinOnce();
					}

					if (!acquired) continue;

					var key = cell.Key;
					var taken = cell.Acc; // struct copy
					cell.Acc = default; // reset in place
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
				"Concurrent producer detected — AccumulatingSpWorker requires single-producer (non-overlapping) access.");
#endif
	}

	[Conditional("DEBUG")]
	private void DebugEndProduce() {
#if DEBUG
		Volatile.Write(ref _producing, 0);
#endif
	}
}
