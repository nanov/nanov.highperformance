namespace Nanov.HighPerformance.Threading.Workers;

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Nanov.HighPerformance.Threading.Workers.Internal;

/// <summary>
///   Single-producer / single-consumer worker that <b>conflates by key</b>: the producer calls
///   <see cref="Dispatch" /> with a <c>(key, value)</c>; if that key is still queued, the value is
///   merged into the queued one IN PLACE (keeping its position) via the handler's
///   <see cref="IItemMerger{TValue}.Merge" />, so the consumer sees the latest value per key. Zero
///   per-message allocation; blocking backpressure when <see cref="Capacity" /> distinct keys are
///   pending. The handler is a <see langword="struct" /> constrained to
///   <see cref="ICompactingWorkerHandler{TKey, TValue}" /> — calls devirtualize and inline.
///   <para>Sync, reference-value variant. Single producer is the caller's contract.</para>
/// </summary>
public sealed class CompactingSpWorker<TKey, TValue, THandler> : IDisposable, IWorkerQueue
	where TKey : notnull, IEquatable<TKey>
	where TValue : class
	where THandler : struct, ICompactingWorkerHandler<TKey, TValue> {
	private readonly CompactingSpRingCore<TKey, TValue, THandler> _core;
	private THandler _handler; // mutable struct — Handle/OnError/OnCompleted go through this field

	public CompactingSpWorker(THandler handler, int capacity, string? threadName = null) {
		_handler = handler;
		// The core gets the handler as its merge facet (TMerger = THandler). Merge is pure, so the core's
		// copy needs no shared mutable state with this worker's _handler (which drives the consumer side).
		_core = new CompactingSpRingCore<TKey, TValue, THandler>(handler, capacity,
			threadName ?? $"CompactingSpWorker<{typeof(TKey).Name},{typeof(TValue).Name}>", RunLoop);
	}

	/// <summary>Completes when the worker has drained and exited (after Complete() or Dispose()).</summary>
	public Task Completion => _core.Completion;

	/// <summary>Approximate distinct keys currently queued (<c>Tail − Head</c>). Lock-free, racy by a few.</summary>
	public int QueuedApprox => _core.QueuedApprox;

	/// <summary>Ring capacity (power of two) = max distinct pending keys.</summary>
	public int Capacity => _core.Capacity;

	/// <summary>
	///   Start the worker thread. Optionally pass a CancellationToken — when cancelled, the worker stops
	///   immediately (drops pending items) and <see cref="Completion" /> resolves with an
	///   <see cref="OperationCanceledException" />.
	/// </summary>
	public void Start(CancellationToken ct = default) => _core.Start(ct);

	/// <summary>
	///   Enqueue <paramref name="value" /> under <paramref name="key" />, conflating with any still-queued
	///   value for the same key. Returns false if completed/disposed; blocks if <see cref="Capacity" />
	///   distinct keys are already pending.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool Dispatch(TKey key, TValue value) => _core.Dispatch(key, value);

	/// <summary>
	///   Signal that no more items will be dispatched. The worker drains remaining items and then exits.
	///   Optionally pass an exception — if non-null, <see cref="Completion" /> resolves faulted with it.
	///   Returns true on first call, false if already completed.
	/// </summary>
	public bool TryComplete(Exception? exception = null) => _core.TryComplete(exception);

	public void Dispose() => _core.Dispose();

	private void RunLoop() {
		try {
			while (_core.TryClaimRun(out var head, out var available)) {
				while (head != available && _core.TryTake(head++, out var key, out var value)) {
					try {
						_handler.Handle(key, value);
					}
					catch (Exception ex) {
						_handler.OnError(ex);
					}
				}
			}
		}
		finally {
			var ex = _core.CompletionException();
			try {
				_handler.OnCompleted(ex);
			}
			catch (Exception onCompletedEx) {
				_handler.OnError(onCompletedEx);
			}

			_core.SetCompletion(ex);
		}
	}
}
