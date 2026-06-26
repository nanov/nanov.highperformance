namespace Nanov.HighPerformance.Threading.Workers;

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Nanov.HighPerformance.Threading.Workers.Internal;

/// <summary>
///   Zero-copy, scope-based async value worker. The consume loop runs as an <c>async Task</c> on the
///   ThreadPool, so a suspended handler's continuation resumes the loop on the pool (no dedicated
///   thread to park) — matching a <c>Channel&lt;T&gt;</c> consumer for truly-async, serial, FIFO
///   handlers, while sync-completing handlers drain in a tight synchronous loop. Producers write
///   directly into the ring slot via <see cref="Publish" />; the handler reads through a
///   <see cref="HandleScope{T}" /> and must read + <see cref="HandleScope{T}.Release" /> the slot
///   synchronously before returning its <see cref="ValueTask" /> (a ref struct can't cross an
///   <c>await</c>) — see <see cref="IAsyncValueWorkerHandler{T}" />. Single logical consumer, one item
///   in flight; the worker loop is allocation-free (a genuinely-suspending handler boxes its own
///   per-item state machine, inherent to async).
/// </summary>
public sealed class AsyncValueSpWorker<T, THandler> : IDisposable, IWorkerQueue
	where T : struct
	where THandler : struct, IAsyncValueWorkerHandler<T> {
	private readonly SpRingCore<T> _core;
	private THandler _handler; // mutable struct — calls go through this field so state persists
	private Task? _loopTask;

	public AsyncValueSpWorker(THandler handler, int capacity) {
		_handler = handler;
		_core = new SpRingCore<T>(capacity); // ThreadPool-async mode — no dedicated thread
	}

	/// <summary>Completes when the worker has drained and exited.</summary>
	public Task Completion => _core.Completion;

	/// <summary>Approximate items currently queued (<c>Tail − Head</c>). Lock-free, racy by a few.</summary>
	public int QueuedApprox => _core.QueuedApprox;

	/// <summary>Ring capacity (rounded up to a power of two).</summary>
	public int Capacity => _core.Capacity;

	/// <summary>
	///   Start the consume loop on the ThreadPool. Optionally pass a CancellationToken — when
	///   cancelled, the worker stops (drops pending items) and <see cref="Completion" /> resolves with
	///   an <see cref="OperationCanceledException" />.
	/// </summary>
	public void Start(CancellationToken ct = default) {
		_core.Start(ct);
		// Run the consume loop with ExecutionContext flow suppressed. The loop is decoupled
		// infrastructure: an AsyncLocal / Activity.Current set by the caller of Start must NOT flow
		// across the queue into the handler (per-item context belongs in the item), and a clean EC keeps
		// the loop's awaits cheap. ConfigureAwait(false) only drops the SynchronizationContext, not EC.
		var suppressed = false;
		if (!ExecutionContext.IsFlowSuppressed()) {
			ExecutionContext.SuppressFlow();
			suppressed = true;
		}

		try {
			_loopTask = Task.Run(RunLoopAsync);
		}
		finally {
			if (suppressed) ExecutionContext.RestoreFlow();
		}
	}

	/// <summary>
	///   Signal that no more items will be dispatched. The worker drains remaining items and then
	///   exits. Optionally pass an exception to fault <see cref="Completion" />. Returns false if
	///   already completing.
	/// </summary>
	public bool TryComplete(Exception? exception = null) => _core.TryComplete(exception);

	/// <summary>
	///   Claim the next slot for writing. Blocks if the ring is full. Disposing the returned
	///   <see cref="PublishScope{T}" /> publishes the slot and signals the consumer. If the worker is
	///   completed/disposed, the scope is "closed" (<see cref="PublishScope{T}.IsOpen" /> is false).
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public PublishScope<T> Publish() {
		_core.DebugBeginProduce();
		if (!_core.TryReserve(out var tail)) {
			_core.DebugEndProduce();
			return default;
		}

		return new PublishScope<T>(_core, tail);
	}

	private async Task RunLoopAsync() {
		try {
			var ct = _core.CancellationToken;
			while (true) {
				if (_core.TryClaimNow(out var head)) {
					// The ref-struct scope is consumed synchronously (read + Release) before the await —
					// it never crosses the suspension. The handler returns a ValueTask for any async tail.
					var vt = default(ValueTask);
					try {
						var scope = new HandleScope<T>(_core, head);
						try {
							vt = _handler.HandleAsync(ref scope, ct);
						}
						finally {
							scope.Release(); // safety net; idempotent (handler should release before returning)
						}
					}
					catch (Exception ex) {
						_handler.OnError(ex);
						continue;
					}

					try {
						await vt.ConfigureAwait(false);
					}
					catch (Exception ex) {
						_handler.OnError(ex);
					}
				} else if (!await _core.WaitForItemAsync().ConfigureAwait(false)) {
					break; // hard stop, or soft complete with the ring drained
				}
			}
		}
		finally {
			var ex = _core.CompletionException();
			try {
				await _handler.OnCompletedAsync(ex).ConfigureAwait(false);
			}
			catch (Exception onCompletedEx) {
				_handler.OnError(onCompletedEx);
			}

			_core.SetCompletion(ex);
		}
	}

	public void Dispose() {
		_core.SignalStop();
		try {
			_loopTask?.Wait(1000);
		}
		catch {
			/* ignore */
		}

		_core.Dispose();
	}
}
