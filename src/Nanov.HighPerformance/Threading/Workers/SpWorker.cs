namespace Nanov.HighPerformance.Threading.Workers;

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Nanov.HighPerformance.Threading.Workers.Internal;

/// <summary>
///   Single-producer / single-consumer buffered worker with zero per-message allocation.
///   - Producer calls <see cref="Dispatch" /> on its own thread; blocks if the buffer is full.
///   - A dedicated worker thread invokes <typeparamref name="THandler" />'s <c>Handle</c> for each
///     item in order. The handler is a <see langword="struct" /> constrained to
///     <see cref="IWorkerHandler{T}" />, so its calls devirtualize and inline — no virtual dispatch
///     on the hot path.
///   - Backpressure via user-mode / kernel fallback <see cref="ManualResetEventSlim" /> waits — no
///     heap allocations per message.
///   For zero-copy over large structs use <see cref="ValueSpWorker{T, THandler}" /> instead.
/// </summary>
public sealed class SpWorker<T, THandler> : IDisposable, IWorkerQueue
	where THandler : struct, IWorkerHandler<T> {
	private readonly SpRingCore<T> _core;
	private THandler _handler; // mutable struct — calls go through this field so state persists

	public SpWorker(THandler handler, int capacity, string? threadName = null) {
		_handler = handler;
		_core = new SpRingCore<T>(capacity, threadName ?? $"SpWorker<{typeof(T).Name}>", RunLoop);
	}

	/// <summary>Completes when the worker has drained and exited (after Complete() or Dispose()).</summary>
	public Task Completion => _core.Completion;

	/// <summary>Approximate items currently queued (<c>Tail − Head</c>). Lock-free, racy by a few.</summary>
	public int QueuedApprox => _core.QueuedApprox;

	/// <summary>Ring capacity (rounded up to a power of two).</summary>
	public int Capacity => _core.Capacity;

	/// <summary>
	///   Start the worker thread. Optionally pass a CancellationToken — when cancelled,
	///   the worker stops immediately (drops pending items) and <see cref="Completion" />
	///   resolves with an <see cref="OperationCanceledException" />.
	/// </summary>
	public void Start(CancellationToken ct = default) => _core.Start(ct);

	/// <summary>
	///   Signal that no more items will be dispatched. The worker drains remaining items and then
	///   exits. Optionally pass an exception — if non-null, <see cref="Completion" /> resolves faulted
	///   with it. Returns true on first call, false if already completed.
	/// </summary>
	public bool TryComplete(Exception? exception = null) => _core.TryComplete(exception);

	public void Dispose() => _core.Dispose();

	/// <summary>
	///   Enqueue an item. Returns false if the worker has been completed or disposed
	///   (item is not enqueued). Returns true if successfully enqueued. Blocks if the buffer is full.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool Dispatch(T item) => _core.TryEnqueue(item);

	private void RunLoop() {
		try {
			while (_core.TryDequeue(out var item)) {
				try {
					_handler.Handle(item);
				}
				catch (Exception ex) {
					_handler.OnError(ex);
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
