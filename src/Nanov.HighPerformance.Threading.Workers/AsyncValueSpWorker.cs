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
		_core = new SpRingCore<T>(capacity, MakeCleaner()); // ThreadPool-async mode — no dedicated thread
	}

	// Bridge for HandleScope.Release → SpRingCore.ClearSlot to reach this worker's strongly-typed handler
	// without the (THandler-erased) core/scope naming THandler. The trampoline is a static method on this
	// closed generic, so it can name THandler; the holder (this) is a GC-tracked object, so nothing is a
	// raw pointer into the heap. _handler.Clear runs in-place on the field (state persists, devirtualized).
	private unsafe SlotCleaner<T> MakeCleaner() => new(this, &ClearTrampoline);

	private static void ClearTrampoline(object holder, ref T slot) =>
		Unsafe.As<AsyncValueSpWorker<T, THandler>>(holder)._handler.Clear(ref slot);

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
			// Claim the whole available run, drain it scope-by-scope, then re-claim — one _idx.Tail read per
			// run instead of per item (mirrors the sync ValueSpWorker). Head publishes are batched
			// (ReleaseDeferred + PublishHead per ShouldFlush); an early HandleScope.Release still publishes
			// immediately. IsStopped is rechecked per item so a hard stop drops the rest of the run.
			while (true) {
				_core.AckWake(); // clear the parked flag once per wake, before draining
				while (_core.TryClaimRunNow(out var head, out var available)) {
					var pending = 0;
					do {
						if (_core.ShouldFlush(pending)) {
							_core.PublishHead(head);
							pending = 0;
						}

						// The ref-struct scope is consumed synchronously (read + Release) before the await —
						// it never crosses the suspension. The handler returns a ValueTask for any async tail.
						var vt = default(ValueTask);
						var scope = new HandleScope<T>(_core, head);
						try {
							vt = _handler.HandleAsync(ref scope, ct);
						}
						catch (Exception ex) {
							_handler.OnError(ex);
							vt = default; // nothing to await; the finally still releases the slot
						}
						finally {
							if (scope.IsReleased) {
								pending = 0; // early release already published past this item
							} else {
								scope.ReleaseDeferred(); // clear now, publish with the batch
								pending++;
							}
						}

						try {
							await vt.ConfigureAwait(false);
						}
						catch (Exception ex) {
							_handler.OnError(ex);
						}
					}
					while (!_core.IsStopped && ++head != available);

					// On a hard-stop exit head wasn't incremented past the current item, so this under-publishes by one — intentional and safe (slot already cleared; hard stop drops the ring). Don't "fix" it to head + 1.
					if (pending > 0) _core.PublishHead(head);
				}

				if (!await _core.WaitForItemAsync().ConfigureAwait(false))
					break; // hard stop, or soft complete with the ring drained
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
