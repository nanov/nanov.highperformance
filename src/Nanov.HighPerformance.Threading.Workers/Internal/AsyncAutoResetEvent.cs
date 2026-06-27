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
///     state field (<see cref="_continuation" />) IS the state machine, resolved entirely by
///     <see cref="Interlocked.CompareExchange{T}" /> (no blind writes — weak-memory-safe on ARM).
///     States: <see langword="null" /> = armed (no waiter registered, no signal latched);
///     <see cref="s_signaled" /> = <see cref="Set" /> fired with no waiter (latched signal);
///     any other delegate = the awaiter's registered continuation.
///   </para>
///   <para>
///     CORRECTNESS INVARIANT: <b>every <see cref="Set" /> is delivered as either a latch or a
///     schedule — no signal is ever dropped.</b> When <see cref="Set" /> claims a registered waiter it
///     CASes <see cref="_continuation" /> straight back to <see langword="null" /> (NOT
///     <see cref="s_signaled" />) before scheduling, so any concurrent/subsequent <see cref="Set" />
///     sees <see langword="null" /> and latches a fresh <see cref="s_signaled" /> for the next cycle
///     rather than collapsing into a signal the wake path would erase. (The earlier "leave
///     <c>s_signaled</c> until <c>GetResult</c> resets it" design dropped a concurrent setter's signal
///     in that window — the lost-wakeup race this replaces.) <c>GetResult</c> therefore must not touch
///     the state field at all — it cannot tell a leftover duplicate from a fresh concurrent latch.
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
		while (true) {
			var cur = Volatile.Read(ref _continuation);
			if (ReferenceEquals(cur, s_signaled))
				return; // already latched — signal coalesces (idempotent)

			if (cur is null) {
				// No waiter registered — latch the signal for the next WaitAsync.
				if (ReferenceEquals(Interlocked.CompareExchange(ref _continuation, s_signaled, null), null))
					return;
				continue; // lost the race; re-read and retry
			}

			// A waiter is registered: claim it and reset to null (NOT s_signaled), so a concurrent/later
			// Set latches a fresh signal instead of being dropped. Exactly one setter wins the claim.
			if (ReferenceEquals(Interlocked.CompareExchange(ref _continuation, null, cur), cur)) {
				Schedule(cur);
				return;
			}
		}
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
		while (true) {
			var cur = Volatile.Read(ref _continuation);
			if (ReferenceEquals(cur, s_signaled)) {
				// A Set already fired (latched) → consume it and run the continuation now.
				if (ReferenceEquals(Interlocked.CompareExchange(ref _continuation, null, s_signaled), s_signaled)) {
					Schedule(continuation);
					return;
				}

				continue; // lost the race; re-read and retry
			}

			if (cur is null) {
				// Register — a Set will claim and schedule it.
				if (ReferenceEquals(Interlocked.CompareExchange(ref _continuation, continuation, null), null))
					return;
				continue; // lost the race; re-read and retry
			}

			// A different continuation is already registered: violates the single-serial-waiter contract.
			throw new InvalidOperationException("AsyncAutoResetEvent supports a single serial waiter.");
		}
	}

	bool IValueTaskSource<bool>.GetResult(short token) {
		// Deliberately does NOT touch _continuation. The normal wake path already reset it to null at claim
		// time (Set), and a concurrent Set may have since latched a fresh s_signaled here — clearing it would
		// drop that distinct signal (the very lost-wakeup this design fixes). GetResult cannot tell a leftover
		// duplicate (synchronous-success path) from a fresh concurrent latch, so it leaves the field alone; a
		// leftover duplicate costs at most one spurious, self-healing recheck on the next WaitAsync.
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
