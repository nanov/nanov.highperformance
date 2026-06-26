namespace Nanov.HighPerformance.Threading.Workers;

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Nanov.HighPerformance.Threading.Workers.Internal;

/// <summary>
///   Async copy-based worker. The consume loop runs as an <c>async Task</c> on the ThreadPool, so a
///   suspended handler's continuation resumes the loop on the pool (no dedicated thread to wake) —
///   matching a <c>Channel&lt;T&gt;</c> consumer for <b>truly-async, one-at-a-time (serial, FIFO)</b>
///   handlers, while handlers that complete synchronously drain in a tight synchronous loop.
///   <para>
///     <typeparamref name="THandler" /> is a struct constrained to <see cref="IAsyncWorkerHandler{T}" />
///     — calls devirtualize and inline. One item in flight at a time (no pipelining); the worker loop
///     is allocation-free, but a handler that genuinely suspends boxes its own per-item state machine
///     (inherent to async). For purely synchronous handlers use <see cref="SpWorker{T, THandler}" />.
///   </para>
/// </summary>
public sealed class AsyncSpWorker<T, THandler> : IDisposable, IWorkerQueue
	where THandler : struct, IAsyncWorkerHandler<T> {
	private readonly SpRingCore<T> _core;
	private THandler _handler; // mutable struct — calls go through this field so state persists
	private Task? _loopTask;

	public AsyncSpWorker(THandler handler, int capacity) {
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
	///   Enqueue an item. Returns false if the worker has been completed or disposed. Blocks if the
	///   buffer is full.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool Dispatch(T item) => _core.TryEnqueue(item);

	private async Task RunLoopAsync() {
		try {
			var ct = _core.CancellationToken;
			while (true) {
				if (_core.TryDequeueNow(out var item)) {
					try {
						await _handler.HandleAsync(item, ct).ConfigureAwait(false);
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
		_core.SignalStop(); // wake the loop so it exits
		try {
			_loopTask?.Wait(1000);
		}
		catch {
			/* ignore */
		}

		_core.Dispose(); // safe now — the loop has exited
	}
}
