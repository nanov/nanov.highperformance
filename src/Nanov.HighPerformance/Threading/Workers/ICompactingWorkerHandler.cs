namespace Nanov.HighPerformance.Threading.Workers;

using System;

/// <summary>
///   Synchronous handler for <c>CompactingSpWorker&lt;TKey, TValue, THandler&gt;</c>. Implement as a
///   <see langword="struct" /> — the worker stores it by value and dispatches through a constrained
///   generic call, so the JIT devirtualizes and inlines per closed worker type. Inherits the producer-
///   side <see cref="IItemMerger{TValue}" /> (conflation) and adds the consumer side. Note the thread
///   split: <see cref="IItemMerger{TValue}.Merge" /> runs on the producer thread; <see cref="Handle" />/
///   <see cref="OnError" />/<see cref="OnCompleted" /> run on the worker thread.
/// </summary>
public interface ICompactingWorkerHandler<TKey, TValue> : IItemMerger<TValue> {
	/// <summary>Process the latest value for one key. Invoked in first-seen key order on the worker thread.</summary>
	void Handle(TKey key, TValue value);

	/// <summary>Invoked synchronously when <see cref="Handle" /> throws. The worker continues with the next item.</summary>
	void OnError(Exception error);

	/// <summary>
	///   Invoked once after the worker has drained and is about to exit. <paramref name="error" /> is
	///   <see langword="null" /> for normal completion, an <see cref="OperationCanceledException" /> for
	///   cancellation, or the exception passed to <c>TryComplete</c>.
	/// </summary>
	void OnCompleted(Exception? error);
}
