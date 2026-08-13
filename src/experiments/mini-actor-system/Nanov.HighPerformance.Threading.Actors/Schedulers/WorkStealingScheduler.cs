namespace Nanov.HighPerformance.Threading.Actors.Schedulers;

using System;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors.Internal;

/// <summary>
///   Scheduler D — pull-side work stealing. The producer submits newly-dirty keys to a shared global
///   queue; each worker keeps a Chase-Lev deque for its own self-rescheduled keys and, when it runs
///   dry, drains the global queue and then steals one key from a random peer.
///   <para>
///     <b>No router thread and no key→worker map.</b> Where B decides placement on the push side, D
///     lets idle workers decide on the pull side. The cell state machine already guarantees that one
///     key is processed by one worker at a time, so a key may move between workers freely — that is
///     what makes stealing correct here and what makes it a real alternative to B rather than a
///     variation on it.
///   </para>
///   <para>
///     <b>Steal granularity is one key.</b> The steal-half variant exists because fine-grained task
///     deques make a single steal too small to amortise its CAS. That does not apply: one key is
///     already a batch of up to <c>batchBound</c> messages, so a single steal moves real work.
///     Steal-half would also pile several hot keys onto one thief, which is the imbalance stealing
///     is supposed to remove.
///   </para>
///   <para>
///     <b>This scheduler owns no threads</b>, so it has no stop flag of its own and never invokes the
///     fault callback — the callback exists for a scheduler that runs its own thread and could die on
///     it silently (B's router). <see cref="SignalStop" /> only has to release parked workers so each
///     can observe the system's own exit condition.
///   </para>
/// </summary>
public struct WorkStealingScheduler : IKeyScheduler {
	private readonly State _state;

	public WorkStealingScheduler() => _state = new State();

	private sealed class State {
		public GlobalKeyQueue? Global;
		public WorkStealingDeque[]? Deques;
		public ManualResetEventSlim[]? WorkAvailable;

		// Racy by design. A stale `true` costs at most one wasted Set — either on a worker that is
		// already awake and heading for TryAcquire (which finds the work anyway), or on a worker that
		// left WaitForWork via IsQuiescent or _stopped, whose Parked[id] then stays `true` forever with
		// nobody left to read it. Both are safe: once IsQuiescent, no cell can mint a new token, because
		// ReleaseAndMaybeSchedule only reschedules when Tail != head, which means unprocessed messages
		// remain — contradicting `sum >= _dispatchedFinal`; and under `_stopped` every worker is exiting
		// regardless, so a wasted Set on a dead worker's flag reaches nobody. A stale `false` cannot lose
		// a wakeup, because the sleeper re-tests HasWork inside its armed window before it blocks.
		public bool[]? Parked;

		public uint[]? Rng; // per-worker xorshift; a shared Random would be a contention point
		public bool Started;

		public long StealsAttempted;
		public long StealsSucceeded;
		public long GlobalDequeues;
		public long DequeOverflows;
		public long Parks;
		public long GlobalEnqueueRetries;
		public int WakeScanCursor; // rotating start for WakeOne's scan -- see WakeOne's doc
	}

	/// <summary>Global queue capacity; must be at least the key map's slot count so it can never fill.</summary>
	public int Capacity { get; set; } = 1024;

	/// <summary>
	///   Per-worker deque capacity. Overflow is not an error — a full deque spills to the global
	///   queue, which is what lets the deque stay small and cache-resident.
	/// </summary>
	public int DequeCapacity { get; set; } = 256;

	/// <summary>Steal attempts, including the ones that came up empty. Denominator of the steal rate.</summary>
	public readonly long StealsAttempted => Volatile.Read(ref _state.StealsAttempted);

	/// <summary>Steals that took a key. <b>The number that says which regime you are in</b> (§8).</summary>
	public readonly long StealsSucceeded => Volatile.Read(ref _state.StealsSucceeded);

	public readonly long GlobalDequeues => Volatile.Read(ref _state.GlobalDequeues);

	public readonly long DequeOverflows => Volatile.Read(ref _state.DequeOverflows);

	/// <summary>
	///   Failed <c>TryEnqueue</c> attempts that had to retry against the global queue's claim-window
	///   race (see <see cref="TryEnqueueGlobalWithRetry" />). Non-zero under normal load; what tells
	///   the benchmark whether that retry tail is ever actually paid.
	/// </summary>
	public readonly long GlobalEnqueueRetries => Volatile.Read(ref _state.GlobalEnqueueRetries);

	/// <summary>
	///   Count of times a worker entered <see cref="Wait" />, incremented on ENTRY before the block
	///   runs — so this measures park INTENT, not actual sleep time. A worker whose event was already
	///   <c>Set</c> when it called <see cref="Wait" /> (e.g. a wake that landed between the armed
	///   recheck and this call) still increments this and returns immediately.
	/// </summary>
	public readonly long Parks => Volatile.Read(ref _state.Parks);

	/// <summary>
	///   <paramref name="onFault" /> is deliberately unused: this scheduler starts no thread of its
	///   own, so there is no thread whose silent death it would have to report.
	/// </summary>
	public void Start(int workerCount, Action<Exception> onFault) {
		if (workerCount < 1) throw new ArgumentOutOfRangeException(nameof(workerCount));
		// A struct WorkStealingScheduler wraps a reference to shared State; handing the SAME value to a
		// second ActorSystem would put two owners on every deque — and the deque's whole cheap path
		// assumes exactly one. Fail loudly instead.
		if (_state.Started)
			throw new InvalidOperationException(
				"this scheduler instance is already driving a system; construct a new one per ActorSystem.");
		_state.Started = true;

		_state.Global = new GlobalKeyQueue(Capacity);
		_state.Deques = new WorkStealingDeque[workerCount];
		_state.WorkAvailable = new ManualResetEventSlim[workerCount];
		_state.Parked = new bool[workerCount];
		_state.Rng = new uint[workerCount];
		for (var i = 0; i < workerCount; i++) {
			_state.Deques[i] = new WorkStealingDeque(DequeCapacity);
			_state.WorkAvailable[i] = new ManualResetEventSlim(false);
			_state.Rng[i] = (uint)(i * 2654435761u + 1u); // any non-zero seed; xorshift needs one
		}
	}

	/// <summary>
	///   <b>The throw below is unrecoverable, not a reported error.</b> By the time <c>Schedule</c> runs,
	///   <c>DispatchCore</c> has already incremented <c>_dispatched</c> and won the cell's
	///   <c>Free → Scheduled</c> CAS — so a throw here leaves that cell stuck at <c>Scheduled</c> with no
	///   token in any queue: <c>Activate</c> is unreachable without a token, <c>Harvest</c> skips any cell
	///   that is not <c>Free</c>, and <c>ReleaseAndMaybeSchedule</c> never runs for it because the cell
	///   never activates. <c>QuiescentSum()</c> can then never reach <c>_dispatchedFinal</c> — every
	///   worker parks forever and <c>Completion</c> never resolves; only <see cref="IDisposable.Dispose" />
	///   on the owning <c>ActorSystem</c> breaks it.
	///   <para>
	///     The candidate remedy — having <c>DispatchCore</c> restore the cell to <c>Free</c> before the
	///     exception escapes — is only PARTIAL even if made: it stops the cell from being stuck forever,
	///     but quiescence for THIS dispatch still needs a later dispatch to the same key to re-arm and
	///     drain it. Recorded here as a known gap; not implemented — <b>do not change the logic.</b>
	///   </para>
	/// </summary>
	public readonly void Schedule(int cellIdx) {
		if (!TryEnqueueGlobalWithRetry(_state, cellIdx))
			throw new InvalidOperationException(
				"global key queue enqueue failed after 64 attempts over roughly 65ms — either Capacity is set " +
				"below the key map's slot count, or a dequeuer has been stalled (e.g. preempted) that long " +
				"holding a claim on the slot this position wrapped onto. Not necessarily a duplicate schedule.");
		WakeOne(_state);
	}

	public readonly void ScheduleLocal(int workerId, int cellIdx) {
		if (!_state.Deques![workerId].TryPushBottom(cellIdx)) {
			// Deque full: spill to the global queue rather than growing. Some other worker will pick it
			// up, which costs this key its affinity but keeps the deque bounded and cache-resident.
			Interlocked.Increment(ref _state.DequeOverflows);
			if (!TryEnqueueGlobalWithRetry(_state, cellIdx))
				throw new InvalidOperationException(
					"deque overflowed into a global queue that then failed to enqueue after 64 attempts over " +
					"roughly 65ms — either Capacity is set below the key map's slot count, or a dequeuer has " +
					"been stalled that long holding a claim on the slot this position wrapped onto.");
		}

		// A self-reschedule leaves stealable work behind, so a sleeping peer should be given the chance
		// to take it. Gated on there being a sleeper at all, so a busy system never pays for this.
		WakeOne(_state);
	}

	/// <summary>
	///   <see cref="GlobalKeyQueue.TryEnqueue" />'s <see langword="false" /> means "not right now", never
	///   durable fullness (see its own doc) — but the window here is NOT "a few instructions". With
	///   <c>CountApprox</c> reading near-empty at the moment of failure (confirmed by instrumentation
	///   during review), the slot this position wrapped onto is a FULL LAP stale: the participant that
	///   still holds it claimed position <c>pos − capacity</c> and has been stalled across roughly
	///   <c>capacity</c> subsequent queue operations — OS-preemption scale (tens to hundreds of
	///   microseconds), not a handful of retired instructions. That is the actual justification for
	///   <see cref="SpinWait" />'s escalation (busy-spin, then a yield, then <c>Thread.Sleep(1)</c>)
	///   rather than a fixed busy-spin budget: a busy-spin-only retry would not survive a stalled
	///   participant that has genuinely been preempted for a scheduling quantum.
	///   <para>
	///     <b>Wall-clock cost: up to roughly 65&#160;ms.</b> 64 attempts (1 initial + 63 retries, the last
	///     of which does not spin before giving up — see the loop below), measured on this machine at
	///     ~64.75&#160;ms end to end for 64 spins — <b>~44</b> of the 63 <c>SpinOnce</c> calls this loop
	///     actually makes land past <c>SpinWait</c>'s internal sleep threshold and are <c>Thread.Sleep(1)</c>,
	///     not a spin. That figure is a hedge, not a recount: 44 was derived for a 64-<c>SpinOnce</c> loop,
	///     the shipped loop makes 63 calls, and the removed call is the most-escalated one — so 43 is the
	///     defensible number, but this was never re-measured against the 63-call loop, hence ~44. This runs
	///     on the producer or a worker thread once per activation, not per message, but anyone budgeting
	///     dispatch tail latency for this scheduler needs that number.
	///     <see cref="GlobalEnqueueRetries" /> counts how often this path is actually paid.
	///   </para>
	/// </summary>
	private static bool TryEnqueueGlobalWithRetry(State s, int cellIdx) {
		var global = s.Global!;
		var spinner = new SpinWait();
		for (var attempt = 0; attempt < 64; attempt++) {
			if (global.TryEnqueue(cellIdx)) return true;
			Interlocked.Increment(ref s.GlobalEnqueueRetries);
			// Only spin if there is a next attempt to make — spinning after the LAST failed attempt pays
			// up to a full Thread.Sleep(1) (SpinWait has long since escalated past its busy-spin phase by
			// attempt 64) that this call can never use before giving up.
			if (attempt < 63) spinner.SpinOnce();
		}

		return false;
	}

	/// <summary>
	///   Wake exactly one sleeping worker, not the herd. The fence is the Dekker handshake against
	///   <c>ActorSystem.WaitForWork</c>: the enqueue above must be visible BEFORE the parked flags are
	///   read, or we see nobody parked and skip the wake while the sleeper's own armed recheck sees a
	///   not-yet-visible queue and blocks. Mid-run that stalls one worker; on the last dispatch before
	///   <c>TryComplete</c> the key is never picked up, quiescence never arrives, and the system hangs.
	///   <para>
	///     <b>arm64 vs. x86-64.</b> On arm64, <c>Volatile.Write</c>/<c>Volatile.Read</c> lower to
	///     <c>stlr</c>/<c>ldar</c>, and ARMv8's release/acquire is RCsc — an <c>ldar</c> cannot be
	///     reordered ahead of an earlier <c>stlr</c>. Since the publish side of both enqueue paths
	///     (<see cref="GlobalKeyQueue.TryEnqueue" />, <see cref="WorkStealingDeque.TryPushBottom" />) ends
	///     in a <c>Volatile.Write</c>, and the first flag read below is a <c>Volatile.Read</c>, this fence
	///     is REDUNDANT on arm64 — removing it there cannot produce a lost wakeup, and a mutation test that
	///     does so on Apple Silicon coming back clean does not mean the fence is dead code. It is
	///     load-bearing on x86-64, where those lower to plain <c>mov</c>s and the store buffer permits the
	///     exact StoreLoad reordering this fence exists to forbid. Keep it regardless of which
	///     architecture you are testing on.
	///   </para>
	///   <para>
	///     <b>Rotating scan start, not always index 0.</b> A burst of K schedules in quick succession
	///     would otherwise re-read <c>Parked[0]</c> every time — the first <c>Set</c> has not cleared it
	///     yet — so every wake in the burst lands on worker 0 while 1..N-1 stay parked: a real cold-start
	///     parallelism loss in the scheduler whose whole point is to be benchmarked against B. Same
	///     motivation as <see cref="TrySteal" />'s randomised victim scan — a per-worker RNG there, a
	///     shared cursor here.
	///   </para>
	///   <para>
	///     <b>Plain read/write on <c>WakeScanCursor</c>, not <see cref="Interlocked" />.</b> It is a pure
	///     fairness heuristic, not correctness-critical — an <see langword="int" /> cannot tear, and a
	///     lost update just means two concurrent <c>Schedule</c> calls start their scan from the same
	///     index, which is the pre-fix (always-0) behaviour for that one pair, never worse. Written back
	///     only on the branch that actually finds and wakes someone, so a busy system genuinely pays
	///     nothing extra here — matching <see cref="ScheduleLocal" />'s own "a busy system never pays for
	///     this" comment on the call site.
	///   </para>
	/// </summary>
	private static void WakeOne(State s) {
		Interlocked.MemoryBarrier();
		var parked = s.Parked!;
		var count = parked.Length;
		var start = s.WakeScanCursor % count; // plain read — see the doc above
		for (var i = 0; i < count; i++) {
			var idx = start + i;
			if (idx >= count) idx -= count;
			if (!Volatile.Read(ref parked[idx])) continue;
			s.WorkAvailable![idx].Set();
			s.WakeScanCursor = idx + 1; // plain write, only here — see the doc above
			return;
		}
	}

	/// <summary>Local deque, then the global queue, then a steal scan. Non-blocking throughout.</summary>
	public readonly bool TryAcquire(int workerId, out int cellIdx) {
		// Clear the parked flag on the hot path so a worker that left WaitForWork via HasWork (rather
		// than via Wait) stops attracting wakes meant for a real sleeper.
		Volatile.Write(ref _state.Parked![workerId], false);

		if (_state.Deques![workerId].TryPopBottom(out cellIdx)) return true;

		if (_state.Global!.TryDequeue(out cellIdx)) {
			Interlocked.Increment(ref _state.GlobalDequeues);
			return true;
		}

		return TrySteal(_state, workerId, out cellIdx);
	}

	private static bool TrySteal(State s, int workerId, out int cellIdx) {
		var deques = s.Deques!;
		var count = deques.Length;
		if (count > 1) {
			// Random victim, then a full round so no peer is permanently skipped. A fixed scan order
			// would make every idle worker converge on the same victim.
			var start = (int)(NextRandom(s, workerId) % (uint)count);
			for (var i = 0; i < count; i++) {
				var victim = start + i;
				if (victim >= count) victim -= count;
				if (victim == workerId) continue;

				Interlocked.Increment(ref s.StealsAttempted);
				if (!deques[victim].TrySteal(out cellIdx)) continue;
				Interlocked.Increment(ref s.StealsSucceeded);
				return true;
			}
		}

		cellIdx = 0;
		return false;
	}

	/// <summary>Per-worker xorshift32. Only this worker touches its own slot, so no atomics.</summary>
	private static uint NextRandom(State s, int workerId) {
		var x = s.Rng![workerId];
		x ^= x << 13;
		x ^= x >> 17;
		x ^= x << 5;
		s.Rng[workerId] = x;
		return x;
	}

	/// <summary>
	///   Racy by design; read before parking.
	///   <para>
	///     <b>Peer deques are part of the answer.</b> If this only reported on the worker's own deque
	///     and the global queue, a worker holding stealable keys would leave every peer asleep and D
	///     would silently degenerate into B without a router — quiescence still resolves, so no
	///     correctness test would catch it.
	///   </para>
	/// </summary>
	public readonly bool HasWork(int workerId) {
		if (_state.Deques![workerId].CountApprox > 0) return true;
		if (_state.Global!.CountApprox > 0) return true;

		var deques = _state.Deques;
		for (var i = 0; i < deques.Length; i++) {
			if (i == workerId) continue;
			if (deques[i].CountApprox > 0) return true;
		}

		return false;
	}

	public readonly void WakeAll() {
		foreach (var evt in _state.WorkAvailable!)
			evt.Set();
	}

	/// <summary>
	///   No stop flag of its own: this scheduler owns no threads, so all stopping means is releasing
	///   parked workers to observe the system's own exit condition.
	/// </summary>
	public readonly void SignalStop() => WakeAll();

	/// <summary>
	///   Reset BEFORE raising the flag. Reversed, a <c>Set</c> that lands between the two is erased by
	///   the <c>Reset</c> and that wakeup is lost.
	/// </summary>
	public readonly void ArmWait(int workerId) {
		_state.WorkAvailable![workerId].Reset();
		Volatile.Write(ref _state.Parked![workerId], true);
	}

	public readonly void Wait(int workerId) {
		Interlocked.Increment(ref _state.Parks);
		_state.WorkAvailable![workerId].Wait();
		Volatile.Write(ref _state.Parked![workerId], false);
	}
}
