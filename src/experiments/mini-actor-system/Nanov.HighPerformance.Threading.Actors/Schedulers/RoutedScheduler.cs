namespace Nanov.HighPerformance.Threading.Actors.Schedulers;

using System;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors.Internal;

/// <summary>How the router picks a worker for a newly-dirty key.</summary>
public enum RoutingPolicy {
	/// <summary>Fewest queued keys wins. Free — <c>KeyRing.CountApprox</c> is already exposed.</summary>
	LeastLoaded,

	/// <summary>
	///   The metric-free null baseline. If this matches <see cref="LeastLoaded" /> in the benchmark,
	///   the router's cleverness is not earning its keep — which is a real result, not a failure.
	/// </summary>
	RoundRobin,
}

/// <summary>
///   Scheduler B — push-side routing. The producer enqueues newly-dirty keys into one SPSC ring; a
///   dedicated router thread drains it and pushes each index into one worker's own SPSC ring.
///   <para>
///     <b>Every edge is single-producer / single-consumer.</b> producer → router is one ring with one
///     writer and one reader; router → worker <c>i</c> likewise. A worker's SELF-reschedule must never
///     enter its ring — that would make the router a second producer — so it goes on that worker's
///     private local stack instead (<see cref="ScheduleLocal" />).
///   </para>
///   <para>
///     Local-first draining gives affinity for free: a key that keeps receiving messages stays on the
///     worker that already has its cell lines warm, which is why B needs no key→worker map at all.
///     But unbounded local-first STARVES the ring — a continuously-refilled key is popped from local
///     forever while a ring-queued key never runs. <see cref="LocalDrainBound" /> caps consecutive
///     local acquisitions; after that the ring is checked first. This is a real B-vs-D asymmetry: under
///     B nothing can move the starved key elsewhere, whereas D's thieves can steal it.
///   </para>
/// </summary>
public struct RoutedScheduler : IKeyScheduler {
	private readonly State _state;

	public RoutedScheduler() => _state = new State();

	private sealed class State {
		public KeyRing? Inbox;              // producer → router
		public KeyRing[]? WorkerRings;      // router → worker i
		public int[][]? Local;              // worker i's private stack
		public int[]? LocalCount;
		public int[]? LocalRun;             // consecutive local acquisitions, for the starvation bound
		public ManualResetEventSlim[]? WorkAvailable;
		public readonly ManualResetEventSlim InboxNotEmpty = new(false);
		public volatile bool RouterParked;
		public Thread? Router;
		public volatile bool Stopping;
		public int NextRoundRobin;
	}

	/// <summary>Ring capacity; must be at least the key map's slot count so a ring can never fill.</summary>
	public int Capacity { get; set; } = 1024;

	public RoutingPolicy Policy { get; set; } = RoutingPolicy.LeastLoaded;

	/// <summary>
	///   Maximum consecutive acquisitions from a worker's local stack before its ring gets priority.
	///   Bounds the starvation described on the type. 16 is a guess pending measurement, not a result.
	/// </summary>
	public int LocalDrainBound { get; set; } = 16;

	public void Start(int workerCount) {
		if (workerCount < 1) throw new ArgumentOutOfRangeException(nameof(workerCount));
		_state.Inbox = new KeyRing(Capacity);
		_state.WorkerRings = new KeyRing[workerCount];
		_state.Local = new int[workerCount][];
		_state.LocalCount = new int[workerCount];
		_state.LocalRun = new int[workerCount];
		_state.WorkAvailable = new ManualResetEventSlim[workerCount];
		for (var i = 0; i < workerCount; i++) {
			_state.WorkerRings[i] = new KeyRing(Capacity);
			_state.Local[i] = new int[Capacity];
			_state.WorkAvailable[i] = new ManualResetEventSlim(false);
		}

		var s = _state;
		var policy = Policy;
		s.Router = new Thread(() => RouteLoop(s, policy)) { IsBackground = true, Name = "ActorRouter" };
		s.Router.Start();
	}

	/// <summary>
	///   Drain the producer's inbox and route each key index to a worker.
	///   <para>
	///     <b>The router parks rather than spinning.</b> A spin-forever router burns a whole core while
	///     idle — it would steal a core from the workers and distort every measurement in the B-vs-D
	///     benchmark, which is the entire point of building this. So it uses the same arm-then-recheck
	///     protocol as the worker and producer parks: reset the event BEFORE raising the parked flag,
	///     fence, re-test the inbox inside the armed window, and only then block. The producer's
	///     <see cref="Schedule" /> sets the event when it sees the flag up.
	///   </para>
	///   <para>
	///     <b><c>Stopping</c> is read AFTER the recheck, never before.</b> <c>SignalStop</c> only ever
	///     runs (from <c>TryComplete</c>/<c>Dispose</c>) once every <see cref="Schedule" /> call the
	///     single producer will ever make has already returned, so its <c>Stopping = true</c> write is
	///     always sequenced-after every prior <c>TryEnqueue</c> on the producer thread, and the CLR
	///     preserves that relative order of volatile writes across threads. That only helps a reader who
	///     checks <b>the flag first and the data second</b> — checking data (<c>TryDequeue</c>) before
	///     the flag, as an earlier draft of this loop did, reads the two in the wrong order and can
	///     observe <c>Stopping == true</c> while a genuinely-fresh <c>TryDequeue</c> would still have
	///     found an item: the router exits with that key stranded in <see cref="State.Inbox" /> forever,
	///     and quiescence — which needs every scheduled key routed and processed — is never reached.
	///     Confirmed by instrumentation: the stall reproduces with <c>Inbox.CountApprox &gt; 0</c> at the
	///     moment of exit. So <c>Stopping</c> is consulted only once more, after the armed recheck has
	///     already proven the inbox empty — never before a dequeue attempt.
	///   </para>
	/// </summary>
	private static void RouteLoop(State s, RoutingPolicy policy) {
		while (true) {
			if (s.Inbox!.TryDequeue(out var cellIdx)) {
				Route(s, policy, cellIdx);
				continue;
			}

			s.InboxNotEmpty.Reset();
			s.RouterParked = true;
			try {
				Interlocked.MemoryBarrier(); // flag visible before the recheck below

				// Recheck INSIDE the armed window: a producer that enqueued just before we armed saw
				// the flag down and did not Set, so this recheck is the only thing that catches it.
				if (s.Inbox.TryDequeue(out cellIdx)) {
					Route(s, policy, cellIdx);
					continue;
				}

				// Only now — after a fresh, empty recheck — is it safe to consult Stopping; see the
				// ordering argument on the doc comment above.
				if (s.Stopping) return;

				s.InboxNotEmpty.Wait();
			}
			finally {
				s.RouterParked = false;
			}
		}
	}

	private static void Route(State s, RoutingPolicy policy, int cellIdx) {
		var target = policy == RoutingPolicy.RoundRobin
			? s.NextRoundRobin++ % s.WorkerRings!.Length
			: LeastLoadedIndex(s);

		if (!s.WorkerRings![target].TryEnqueue(cellIdx))
			throw new InvalidOperationException(
				"worker ring full — rings are sized to the map, so this means a key index was scheduled twice.");
		s.WorkAvailable![target].Set();
	}

	private static int LeastLoadedIndex(State s) {
		// Queued KEY count, not queued messages. Message depth is the truer metric but is stale the
		// instant it is read and would cost cross-core reads per decision; key count is already exposed.
		var best = 0;
		var bestDepth = s.WorkerRings![0].CountApprox;
		for (var i = 1; i < s.WorkerRings.Length; i++) {
			var depth = s.WorkerRings[i].CountApprox;
			if (depth >= bestDepth) continue;
			best = i;
			bestDepth = depth;
		}

		return best;
	}

	public void Schedule(int cellIdx) {
		if (!_state.Inbox!.TryEnqueue(cellIdx))
			throw new InvalidOperationException(
				"router inbox full — it is sized to the map, so this means a key index was scheduled twice.");
		// StoreLoad fence between publishing the item and reading the router's parked flag. Without it
		// this read can be hoisted above the enqueue: we would see RouterParked == false and skip the
		// Set, while the router's own recheck sees a not-yet-visible ring and parks. Mid-run that
		// stalls routing until the next dispatch; on the LAST dispatch before TryComplete the key is
		// never routed at all, quiescence is never reached, and the system hangs. Schedule runs once
		// per activation, not per message, so the fence is cheap here.
		//
		// This is the same StoreLoad gap recorded as known debt on the worker's wake site — do not
		// reproduce it here.
		Interlocked.MemoryBarrier();
		if (_state.RouterParked) _state.InboxNotEmpty.Set();
	}

	public void ScheduleLocal(int workerId, int cellIdx) =>
		_state.Local![workerId][_state.LocalCount![workerId]++] = cellIdx;

	public bool TryAcquire(int workerId, out int cellIdx) {
		var localCount = _state.LocalCount![workerId];

		// Starvation bound: after LocalDrainBound consecutive local pops, look at the ring first so a
		// ring-queued key cannot be starved by a continuously-refilled local one.
		if (localCount > 0 && _state.LocalRun![workerId] < LocalDrainBound) {
			cellIdx = _state.Local![workerId][--_state.LocalCount[workerId]];
			_state.LocalRun[workerId]++;
			return true;
		}

		_state.LocalRun![workerId] = 0;
		if (_state.WorkerRings![workerId].TryDequeue(out cellIdx)) return true;

		if (localCount > 0) {
			cellIdx = _state.Local![workerId][--_state.LocalCount[workerId]];
			return true;
		}

		return false;
	}

	public bool HasWork(int workerId) =>
		_state.LocalCount![workerId] > 0 || _state.WorkerRings![workerId].CountApprox > 0;

	public void WakeAll() {
		// The router too — unconditionally. WakeAll is the shutdown/quiescence path, and a router still
		// holding an unrouted key would leave quiescence permanently unreachable. Shutdown wakes
		// everything and is never flag-gated.
		_state.InboxNotEmpty.Set();
		foreach (var evt in _state.WorkAvailable!)
			evt.Set();
	}

	public void SignalStop() {
		_state.Stopping = true;
		_state.InboxNotEmpty.Set(); // unconditional: release a parked router so it can observe Stopping
		WakeAll();
	}

	public void ArmWait(int workerId) => _state.WorkAvailable![workerId].Reset();

	public void Wait(int workerId) => _state.WorkAvailable![workerId].Wait();
}
