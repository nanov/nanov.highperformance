namespace Nanov.HighPerformance.Threading.Workers;

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Nanov.HighPerformance.Threading.Workers.Internal;

/// <summary>
///   Async twin of <see cref="ValueSpWorker{T, THandler}" />. Same scope-based publish/consume APIs
///   — <see cref="Publish" /> returns a <see cref="PublishScope{T}" />, the handler's <c>HandleAsync</c>
///   receives a <see cref="HandleScope{T}" /> by ref — but the handler can return a
///   <see cref="ValueTask" /> for in-flight async work (see <see cref="IAsyncValueWorkerHandler{T}" /> for
///   the required non-<c>async</c> read-then-release shape).
///   <para>
///     Runs on a dedicated thread with synchronous waits for items (zero alloc). For sync-completing
///     ValueTasks, entirely synchronous (zero overhead). For truly async ones, the dedicated thread
///     blocks on the ValueTask via its awaiter. No <c>SynchronizationContext</c> on the dedicated
///     thread, so no deadlock risk.
///   </para>
/// </summary>
public sealed class AsyncValueSpWorker<T, THandler> : IDisposable, IWorkerQueue
	where T : struct
	where THandler : struct, IAsyncValueWorkerHandler<T> {
	private readonly SpRingCore<T> _core;
	private readonly ManualResetEventSlim _processDoneEvt = new(false);
	private readonly Action _onProcessComplete; // bound to _processDoneEvt.Set — one alloc per worker
	private THandler _handler; // mutable struct — calls go through this field so state persists

	public AsyncValueSpWorker(THandler handler, int capacity, string? threadName = null) {
		_handler = handler;
		_onProcessComplete = _processDoneEvt.Set;
		_core = new SpRingCore<T>(capacity, threadName ?? $"AsyncValueSpWorker<{typeof(T).Name}>", RunLoop);
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
	///   Signal that no more items will be dispatched. The worker drains remaining items and then
	///   exits. Optionally pass an exception — if non-null, <see cref="Completion" /> resolves faulted
	///   with it. Returns true on first call, false if already completed.
	/// </summary>
	public bool TryComplete(Exception? exception = null) => _core.TryComplete(exception);

	/// <summary>
	///   Claim the next slot for writing. Blocks if the ring is full. The returned
	///   <see cref="PublishScope{T}" /> exposes a <see langword="ref" /> to the slot through
	///   <see cref="PublishScope{T}.Event" />; disposing the scope publishes the slot and signals the
	///   consumer. If the worker is Completed or Disposed, the returned scope is "closed".
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public PublishScope<T> Publish() {
		if (!_core.TryReserve(out var tail)) return default;
		return new PublishScope<T>(_core, tail);
	}

	private void RunLoop() {
		try {
			while (_core.TryClaim(out var head)) {
				var scope = new HandleScope<T>(_core, head);
				try {
#pragma warning disable CA2012 // advanced ValueTask awaiting; all completion cases are handled
					var vt = _handler.HandleAsync(ref scope, _core.CancellationToken);
					var awaiter = vt.GetAwaiter();
					if (!awaiter.IsCompleted) {
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
