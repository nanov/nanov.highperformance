namespace Nanov.HighPerformance.Threading;

using System;

/// <summary>
///   Single-producer / single-consumer buffered worker with zero per-message allocation.
///   - Producer calls <see cref="SpscWorkerBase{T}.Dispatch" /> on its own thread; blocks if the
///     buffer is full.
///   - A dedicated worker thread invokes <typeparamref name="THandler" />'s <c>Handle</c> for each
///     item in order. The handler is a <see langword="struct" /> constrained to
///     <see cref="ISpscHandler{T}" />, so its calls devirtualize and inline — no virtual dispatch
///     on the hot path.
///   - Backpressure via user-mode / kernel fallback <see cref="System.Threading.ManualResetEventSlim" />
///     waits — no heap allocations per message.
///   All the ring-buffer plumbing lives on <see cref="SpscWorkerBase{T}" />; this subclass only adds
///   the sync handler dispatch loop.
/// </summary>
public sealed class SpscWorker<T, THandler> : SpscWorkerBase<T>
	where THandler : struct, ISpscHandler<T> {
	private THandler _handler; // mutable struct — calls go through this field so state persists

	public SpscWorker(THandler handler, int capacity, string? threadName = null)
		: base(capacity, threadName) {
		_handler = handler;
	}

	protected override void RunLoop() {
		try {
			while (TryDequeue(out var item)) {
				try {
					_handler.Handle(item);
				}
				catch (Exception ex) {
					_handler.OnError(ex);
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
