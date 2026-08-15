namespace Nanov.HighPerformance.Threading.Actors;

using System;

/// <summary>
///   The scheduling policy: which worker gets which key next. Implement as a <see langword="struct" />
///   so the calls devirtualize — the struct wraps a reference to its own shared state, the same shape
///   the test recorders use.
///   <para>
///     The scheduler carries <b>key indices</b>, never messages, and per-key ordering does NOT depend
///     on it: a key index is in exactly one place at any instant, arbitrated by the cell state machine.
///     A scheduler may therefore use any queue discipline it likes — LIFO, stealing from either end,
///     out-of-order claims — without affecting correctness.
///   </para>
/// </summary>
public interface IKeyScheduler {
	/// <summary>
	///   Allocate per-worker structures and start any scheduler-owned threads. Called once, before any
	///   worker starts.
	///   <para>
	///     <paramref name="onFault" /> is the scheduler's channel back to the system for a fault on a
	///     thread the scheduler owns. A scheduler-owned thread that dies silently is the worst failure
	///     mode available: the system's workers park on an unbounded wait, quiescence becomes
	///     unreachable, and <c>Completion</c> never resolves. Invoke it from any catch on such a thread
	///     BEFORE stopping, so the exception is recorded rather than lost. <paramref name="onFault" />
	///     itself must not throw — a caller invoking it from a catch block should wrap the call in its
	///     own <see langword="try" />/<see langword="catch" /> so a throwing implementation cannot skip
	///     that catch's own cleanup or escape the thread unhandled.
	///   </para>
	/// </summary>
	void Start(int workerCount, Action<Exception> onFault);

	/// <summary>
	///   Schedule a newly-dirty key from the PRODUCER thread. Must not be called from a worker —
	///   <see cref="ScheduleLocal" /> exists for that, and mixing them breaks single-producer queues.
	/// </summary>
	void Schedule(int cellIdx);

	/// <summary>
	///   Schedule from a WORKER (a self-reschedule after a bounded batch). Routing this separately is
	///   what keeps a producer-owned queue strictly single-producer.
	/// </summary>
	void ScheduleLocal(int workerId, int cellIdx);

	/// <summary>Take the next key for this worker. Non-blocking; may steal.</summary>
	bool TryAcquire(int workerId, out int cellIdx);

	/// <summary>Whether this worker could acquire right now. Racy by design; used before parking.</summary>
	bool HasWork(int workerId);

	/// <summary>
	///   Wake EVERY worker unconditionally. Shutdown and quiescence only — never flag-gated. A system
	///   that is already drained and parked has no other way to be told.
	/// </summary>
	void WakeAll();

	/// <summary>Stop: release every parked worker so each can observe the exit condition.</summary>
	void SignalStop();

	/// <summary>
	///   Arm this worker's wait. Half of arm-then-recheck — the CALLER does the recheck between this
	///   and <see cref="Wait" />. Collapsing the two is the classic lost wakeup.
	/// </summary>
	void ArmWait(int workerId);

	/// <summary>
	///   Block this worker until signalled or until <paramref name="millisecondsTimeout" /> elapses.
	///   <para>
	///     Returns <see langword="false" /> on timeout, <see langword="true" /> if signalled. A timeout is
	///     <b>not</b> an error — it is a rung of the wait ladder, same as a signal. Either way the caller
	///     must re-test work and quiescence before parking again, which <c>WaitForWork</c> already does
	///     unconditionally after this call, so the return value is informational rather than
	///     branch-worthy.
	///   </para>
	/// </summary>
	bool Wait(int workerId, int millisecondsTimeout);
}
