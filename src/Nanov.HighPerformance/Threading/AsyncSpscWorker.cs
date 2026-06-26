namespace Nanov.HighPerformance.Threading;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
///   Async twin of <see cref="SpscWorker{T, THandler}" />: <typeparamref name="THandler" />
///   implements <see cref="IAsyncSpscHandler{T}" /> whose <c>HandleAsync</c> returns a ValueTask.
///   Runs on a dedicated thread with synchronous waits for items (zero alloc).
///   For sync-completing HandleAsync, entirely synchronous (zero overhead).
///   For truly async HandleAsync, the dedicated thread blocks on the ValueTask via its awaiter —
///   the handler's internal async continuations run on ThreadPool as normal. No
///   SynchronizationContext is set on the dedicated thread, so no deadlock risk from blocking.
///   Suitable for bounded worker pools (dozens of workers). Not suitable for thousands of concurrent
///   handlers — for that, use a ThreadPool-based async loop. All the ring-buffer plumbing lives on
///   <see cref="SpscWorkerBase{T}" />; this subclass only adds the async handler dispatch loop and
///   the per-item ValueTask rendezvous.
/// </summary>
public sealed class AsyncSpscWorker<T, THandler> : SpscWorkerBase<T>
	where THandler : struct, IAsyncSpscHandler<T> {
	private readonly Action _onProcessComplete;
	private readonly ManualResetEventSlim _processDoneEvt = new(false);
	private THandler _handler; // mutable struct — calls go through this field so state persists

	public AsyncSpscWorker(THandler handler, int capacity, string? threadName = null)
		: base(capacity, threadName ?? $"AsyncSpscWorker<{typeof(T).Name}>") {
		_handler = handler;
		_onProcessComplete = () => _processDoneEvt.Set(); // cached delegate, one-time alloc
	}

	protected override void RunLoop() {
		try {
			while (TryDequeue(out var item)) {
				try {
#pragma warning disable CA2012 // advanced ValueTask awaiting; all completion cases are handled
					var vt = _handler.HandleAsync(item, CancellationToken);
					var awaiter = vt.GetAwaiter();
					if (!awaiter.IsCompleted) {
						// 1:1 rendezvous: exactly one continuation Set() per item, consumed by this Wait();
						// the Reset() restores the unsignaled precondition for the next item. No cross-item
						// signal leakage (the next continuation isn't registered until the next iteration).
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
			var ex = CompletionException();
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

			SetCompletion(ex);
		}
	}

	public override void Dispose() {
		base.Dispose();
		_processDoneEvt.Dispose();
	}
}
