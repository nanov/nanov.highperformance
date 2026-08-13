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

		// Racy by design. A stale `true` costs one wasted Set on a worker that is already awake and
		// heading for TryAcquire — which finds the work anyway. A stale `false` cannot lose a wakeup,
		// because the sleeper re-tests HasWork inside its armed window before it blocks.
		public bool[]? Parked;

		public uint[]? Rng; // per-worker xorshift; a shared Random would be a contention point
		public bool Started;

		public long StealsAttempted;
		public long StealsSucceeded;
		public long GlobalDequeues;
		public long DequeOverflows;
		public long Parks;
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

	public readonly void Schedule(int cellIdx) {
		if (!_state.Global!.TryEnqueue(cellIdx))
			throw new InvalidOperationException(
				"global key queue full — it is sized to the map, so this most likely means Capacity is set " +
				"below the key map's slot count, not that a key index was scheduled twice.");
		WakeOne(_state);
	}

	public readonly void ScheduleLocal(int workerId, int cellIdx) {
		if (!_state.Deques![workerId].TryPushBottom(cellIdx)) {
			// Deque full: spill to the global queue rather than growing. Some other worker will pick it
			// up, which costs this key its affinity but keeps the deque bounded and cache-resident.
			Interlocked.Increment(ref _state.DequeOverflows);
			if (!_state.Global!.TryEnqueue(cellIdx))
				throw new InvalidOperationException(
					"deque overflowed into a full global queue — both are sized to the map, so Capacity is " +
					"almost certainly below the key map's slot count.");
		}

		// A self-reschedule leaves stealable work behind, so a sleeping peer should be given the chance
		// to take it. Gated on there being a sleeper at all, so a busy system never pays for this.
		WakeOne(_state);
	}

	/// <summary>
	///   Wake exactly one sleeping worker, not the herd. The fence is the Dekker handshake against
	///   <c>ActorSystem.WaitForWork</c>: the enqueue above must be visible BEFORE the parked flags are
	///   read, or we see nobody parked and skip the wake while the sleeper's own armed recheck sees a
	///   not-yet-visible queue and blocks. Mid-run that stalls one worker; on the last dispatch before
	///   <c>TryComplete</c> the key is never picked up, quiescence never arrives, and the system hangs.
	/// </summary>
	private static void WakeOne(State s) {
		Interlocked.MemoryBarrier();
		var parked = s.Parked!;
		for (var i = 0; i < parked.Length; i++) {
			if (!Volatile.Read(ref parked[i])) continue;
			s.WorkAvailable![i].Set();
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
