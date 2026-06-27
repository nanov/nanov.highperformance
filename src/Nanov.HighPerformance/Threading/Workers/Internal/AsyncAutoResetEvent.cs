namespace Nanov.HighPerformance.Threading.Workers.Internal;

using System;
using System.Threading;
using System.Threading.Tasks.Sources;

/// <summary>
///   Zero-allocation, single-waiter auto-reset async event for the ThreadPool-async worker's
///   "ring empty → wait for an item" path. <see cref="Set" /> completes a pending
///   <see cref="WaitAsync" /> or latches one signal; <see cref="WaitAsync" /> consumes a latch or
///   parks. Returns a <see cref="ValueTask{Boolean}" /> (result always <see langword="true" /> =
///   "woke") so the consumer's wait is a plain non-<c>async</c> method.
///   <para>
///     Lock-free, hand-rolled <see cref="IValueTaskSource{Boolean}" /> in the style of the BCL's
///     <c>System.Threading.Channels.AsyncOperation</c> / <c>SocketAsyncEventArgs</c>: a <b>single</b>
///     state field (<see cref="_continuation" />) IS the state machine, resolved by one
///     <see cref="Interlocked.CompareExchange{T}" />. No <c>ManualResetValueTaskSourceCore</c> +
///     external latch (that composition has a desync seam — it deadlocked the cap-2 churn test).
///     States: <see langword="null" /> = armed (a wait is outstanding, no signal yet);
///     <see cref="s_signaled" /> = <see cref="Set" /> fired; any other delegate = the awaiter's
///     registered continuation.
///   </para>
///   <para>
///     Allocation: <see cref="WaitAsync" /> returns a struct <see cref="ValueTask{Boolean}" /> over
///     this reused source (nothing), and a wake schedules the continuation by queueing <b>this</b>
///     reused object as an <see cref="IThreadPoolWorkItem" /> — so even the suspend→resume path is
///     zero-alloc (no per-wake delegate/work-item wrapper, unlike the
///     <c>ThreadPool.UnsafeQueueUserWorkItem(Action,state)</c> path).
///   </para>
///   <para>
///     Contract: <b>single serial waiter</b> (the consumer awaits each result before the next) and
///     <b>multiple concurrent setters</b> (the producer's commit edge + the stop/complete paths).
///     The continuation runs on the ThreadPool without capturing ExecutionContext or a scheduler —
///     valid here because the worker loop suppresses EC flow and awaits with <c>ConfigureAwait(false)</c>.
///   </para>
/// </summary>
internal sealed class AsyncAutoResetEvent : IValueTaskSource<bool>, IThreadPoolWorkItem {
	// Sentinel parked in _continuation to mean "Set has fired" (latched signal, or completion).
	private static readonly Action<object?> s_signaled = static _ => { };

	private Action<object?>? _continuation; // null = armed; s_signaled = set; else = registered waiter
	private object? _continuationState;
	private Action<object?>? _pending; // continuation captured for the queued Execute() (single waiter ⇒ one at a time)

	/// <summary>Signal. Safe from multiple producer/stop threads.</summary>
	public void Set() {
		var prev = Interlocked.CompareExchange(ref _continuation, s_signaled, null);
		if (prev is null || ReferenceEquals(prev, s_signaled))
			return; // latched (no waiter registered) or already signaled — idempotent

		// prev is the awaiter's registered continuation: claim it (exactly one winner) and run it.
		if (ReferenceEquals(Interlocked.CompareExchange(ref _continuation, s_signaled, prev), prev))
			Schedule(prev);
	}

	/// <summary>Wait for the next signal. Single consumer, serial. Result is always true ("woke").</summary>
	public ValueTask<bool> WaitAsync() {
		// Consume a latched signal if present; otherwise arm a pending wait (await calls OnCompleted).
		if (ReferenceEquals(Interlocked.CompareExchange(ref _continuation, null, s_signaled), s_signaled))
			return new ValueTask<bool>(true);

		return new ValueTask<bool>(this, 0);
	}

	ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token) =>
		ReferenceEquals(Volatile.Read(ref _continuation), s_signaled)
			? ValueTaskSourceStatus.Succeeded
			: ValueTaskSourceStatus.Pending;

	void IValueTaskSource<bool>.OnCompleted(Action<object?> continuation, object? state, short token,
		ValueTaskSourceOnCompletedFlags flags) {
		_continuationState = state;
		var prev = Interlocked.CompareExchange(ref _continuation, continuation, null);
		if (prev is null)
			return; // registered — a Set will claim and schedule it

		// prev == s_signaled: a Set already fired → consume it and run the continuation now.
		Volatile.Write(ref _continuation, null);
		Schedule(continuation);
	}

	bool IValueTaskSource<bool>.GetResult(short token) {
		// Reset for the next cycle — only if still signaled, so a fresh latch isn't clobbered.
		Interlocked.CompareExchange(ref _continuation, null, s_signaled);
		_continuationState = null;
		return true;
	}

	// Queue THIS reused object (no per-wake wrapper allocation). Single waiter ⇒ at most one pending
	// continuation, so stashing it in _pending is race-free with the next cycle (the consumer can't
	// re-arm until Execute resumes it).
	private void Schedule(Action<object?> continuation) {
		_pending = continuation;
		ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false);
	}

	void IThreadPoolWorkItem.Execute() {
		var continuation = _pending;
		var state = _continuationState;
		_pending = null;
		continuation?.Invoke(state);
	}
}
