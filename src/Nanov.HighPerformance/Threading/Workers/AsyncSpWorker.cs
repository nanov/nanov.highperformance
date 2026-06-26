namespace Nanov.HighPerformance.Threading.Workers;

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Nanov.HighPerformance.Threading.Workers.Internal;

/// <summary>
///   Async twin of <see cref="SpWorker{T, THandler}" />: <typeparamref name="THandler" />
///   implements <see cref="IAsyncWorkerHandler{T}" /> whose <c>HandleAsync</c> returns a ValueTask.
///   Runs on a dedicated thread with synchronous waits for items (zero alloc). For sync-completing
///   HandleAsync, entirely synchronous (zero overhead). For truly async HandleAsync, the dedicated
///   thread blocks on the ValueTask via its awaiter — the handler's internal async continuations run
///   on ThreadPool as normal. No SynchronizationContext is set on the dedicated thread, so no deadlock
///   risk from blocking. Suitable for bounded worker pools (dozens of workers); not for thousands of
///   concurrent handlers — for that, use a ThreadPool-based async loop.
/// </summary>
public sealed class AsyncSpWorker<T, THandler> : IDisposable, IWorkerQueue
	where THandler : struct, IAsyncWorkerHandler<T> {
	private readonly SpRingCore<T> _core;
	private readonly ManualResetEventSlim _processDoneEvt = new(false);
	private readonly Action _onProcessComplete; // bound to _processDoneEvt.Set — one alloc per worker
	private THandler _handler; // mutable struct — calls go through this field so state persists

	public AsyncSpWorker(THandler handler, int capacity, string? threadName = null) {
		_handler = handler;
		_onProcessComplete = _processDoneEvt.Set;
		_core = new SpRingCore<T>(capacity, threadName ?? $"AsyncSpWorker<{typeof(T).Name}>", RunLoop);
	}

	/// <summary>Completes when the worker has drained and exited.</summary>
	public Task Completion => _core.Completion;

	/// <summary>Approximate items currently queued (<c>Tail − Head</c>). Lock-free, racy by a few.</summary>
	public int QueuedApprox => _core.QueuedApprox;

	/// <summary>Ring capacity (rounded up to a power of two).</summary>
	public int Capacity => _core.Capacity;

	/// <summary>
	///   Start the worker thread. Optionally pass a CancellationToken — when cancelled, the worker
	///   stops immediately (drops pending items) and <see cref="Completion" /> resolves with an
	///   <see cref="OperationCanceledException" />.
	/// </summary>
	public void Start(CancellationToken ct = default) => _core.Start(ct);

	/// <summary>
	///   Signal that no more items will be dispatched. The worker drains remaining items (processing
	///   each via HandleAsync) and then exits. Optionally pass an exception — if non-null,
	///   <see cref="Completion" /> resolves faulted with it. Returns true on first call, false if
	///   already completed.
	/// </summary>
	public bool TryComplete(Exception? exception = null) => _core.TryComplete(exception);

	/// <summary>
	///   Enqueue an item. Returns false if the worker has been completed or disposed (item is not
	///   enqueued). Returns true if successfully enqueued. Blocks if the buffer is full.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool Dispatch(T item) => _core.TryEnqueue(item);

	private void RunLoop() {
		try {
			while (_core.TryDequeue(out var item)) {
				try {
#pragma warning disable CA2012 // advanced ValueTask awaiting; all completion cases are handled
					var vt = _handler.HandleAsync(item, _core.CancellationToken);
					var awaiter = vt.GetAwaiter();
					if (!awaiter.IsCompleted) {
						// 1:1 rendezvous: exactly one continuation Set() per item, consumed by this Wait();
						// the Reset() restores the unsignaled precondition for the next item.
						awaiter.UnsafeOnCompleted(_onProcessComplete);
						_processDoneEvt.Wait();
						_processDoneEvt.Reset();
					}

					awaiter.GetResult();
#pragma warning restore CA2012
				}
				catch (Exception ex) {
					_handler.OnError(ex);
				}
			}
		}
		finally {
			var ex = _core.CompletionException();
			try {
#pragma warning disable CA2012
				var vt = _handler.OnCompletedAsync(ex);
				var aw = vt.GetAwaiter();
				if (!aw.IsCompleted) {
					aw.UnsafeOnCompleted(_onProcessComplete);
					_processDoneEvt.Wait();
					_processDoneEvt.Reset();
				}

				aw.GetResult();
#pragma warning restore CA2012
			}
			catch (Exception onCompletedEx) {
				_handler.OnError(onCompletedEx);
			}

			_core.SetCompletion(ex);
		}
	}

	public void Dispose() {
		_core.Dispose();
		_processDoneEvt.Dispose();
	}
}
