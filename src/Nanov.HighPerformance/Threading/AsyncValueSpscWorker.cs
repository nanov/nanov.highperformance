namespace Nanov.HighPerformance.Threading;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
///   Async twin of <see cref="ValueSpscWorker{T, THandler}" />. Same scope-based publish/consume
///   APIs — <see cref="ValueSpscWorkerBase{T}.Publish" /> returns a <see cref="PublishScope{T}" />,
///   the handler's <c>HandleAsync</c> receives a <see cref="HandleScope{T}" /> by ref — but the
///   handler can return a <see cref="ValueTask" /> for in-flight async work (see
///   <see cref="IAsyncValueSpscHandler{T}" /> for the required non-<c>async</c> read-then-release shape).
///   <para>
///     Runs on a dedicated thread with synchronous waits for items (zero alloc). For sync-completing
///     ValueTasks, entirely synchronous (zero overhead). For truly async ones, the dedicated thread
///     blocks on the ValueTask via its awaiter. No <c>SynchronizationContext</c> on the dedicated
///     thread, so no deadlock risk.
///   </para>
/// </summary>
public sealed class AsyncValueSpscWorker<T, THandler> : ValueSpscWorkerBase<T>
	where T : struct
	where THandler : struct, IAsyncValueSpscHandler<T> {
	private readonly Action _onProcessComplete;
	private readonly ManualResetEventSlim _processDoneEvt = new(false);
	private THandler _handler; // mutable struct — calls go through this field so state persists

	public AsyncValueSpscWorker(THandler handler, int capacity, string? threadName = null)
		: base(capacity, threadName, $"AsyncValueSpscWorker<{typeof(T).Name}>") {
		_handler = handler;
		_onProcessComplete = () => _processDoneEvt.Set();
	}

	protected override void RunLoop() {
		try {
			while (TryDequeue(out var scope)) {
				try {
#pragma warning disable CA2012 // advanced ValueTask awaiting; all completion cases are handled
					var vt = _handler.HandleAsync(ref scope, CancellationToken);
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
				finally {
					scope.Release();
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
