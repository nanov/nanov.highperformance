namespace Nanov.HighPerformance.Threading.Actors;

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
	/// <summary>Allocate per-worker structures. Called once, before any worker starts.</summary>
	void Start(int workerCount);

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

	/// <summary>Block this worker until signalled.</summary>
	void Wait(int workerId);
}
