namespace Nanov.HighPerformance.Threading.Workers;

using System;

/// <summary>
///   Synchronous handler for <c>ConflationSpWorker&lt;TKey, TValue, THandler&gt;</c>. Implement as a
///   <see langword="struct" /> — the worker stores it by value and dispatches through a constrained
///   generic call, so the JIT devirtualizes and inlines per closed worker type. The worker conflates
///   last-wins by plain override (no merge or drop callback), so the handler is consumer-side only:
///   <see cref="Handle" />/<see cref="OnError" />/<see cref="OnCompleted" /> all run on the worker thread.
/// </summary>
public interface IConflationWorkerHandler<TKey, TValue> {
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
