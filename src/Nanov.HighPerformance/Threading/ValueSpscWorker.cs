namespace Nanov.HighPerformance.Threading;

using System;

/// <summary>
///   Value-type twin of <see cref="SpscWorker{T, THandler}" /> in the Disruptor-net
///   <c>ValueDisruptor</c> style: producers write <b>directly into the ring slot</b>
///   via <see cref="ValueSpscWorkerBase{T}.Publish" />, so there is no struct copy at the call site.
///   <code>
/// using (var scope = worker.Publish()) {
///     if (!scope.IsOpen) return;            // worker completed/disposed
///     ref var slot = ref scope.Event();
///     slot.Field1 = ...;
///     slot.Field2 = ...;
/// }
///   </code>
///   Single-producer / single-consumer, zero per-message allocation. The dedicated worker thread
///   invokes <typeparamref name="THandler" />'s <c>Handle(ref HandleScope&lt;T&gt;)</c> per slot —
///   devirtualized (struct handler), no virtual dispatch per item. Backpressure, completion,
///   cancellation and Dispose semantics mirror <see cref="SpscWorker{T, THandler}" />.
/// </summary>
public sealed class ValueSpscWorker<T, THandler> : ValueSpscWorkerBase<T>
	where T : struct
	where THandler : struct, IValueSpscHandler<T> {
	private THandler _handler; // mutable struct — calls go through this field so state persists

	public ValueSpscWorker(THandler handler, int capacity, string? threadName = null)
		: base(capacity, threadName, $"ValueSpscWorker<{typeof(T).Name}>") {
		_handler = handler;
	}

	protected override void RunLoop() {
		try {
			while (TryDequeue(out var scope)) {
				try {
					_handler.Handle(ref scope);
				}
				catch (Exception ex) {
					_handler.OnError(ex);
				}
				finally {
					// Safety net: ensure _head advances even if the handler didn't release.
					// Idempotent — when the handler already released, this is a no-op.
					scope.Release();
				}
			}
		}
		finally {
			var ex = CompletionException();
			try {
				_handler.OnCompleted(ex);
			}
			catch (Exception onCompletedEx) {
				_handler.OnError(onCompletedEx);
			}

			SetCompletion(ex);
		}
	}
}
