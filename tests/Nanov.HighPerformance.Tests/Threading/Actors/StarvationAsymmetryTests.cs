namespace Nanov.HighPerformance.Tests.Threading.Actors;

using System;
using System.Linq;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using Nanov.HighPerformance.Tests.Threading.Workers;
using NUnit.Framework;

/// <summary>
///   The one behavioural difference between schedulers B and D that the design predicts and nothing
///   in the suite had ever measured (spec §6.1).
///   <para>
///     B gives a worker a private local stack for its self-rescheduled keys and drains it first,
///     which buys affinity for free. Unbounded, that starves the worker's RING: a key refilled faster
///     than it drains is popped from local forever while a ring-queued key never runs at all — and
///     under B nothing can move that key elsewhere. <c>LocalDrainBound</c> caps the local run to fix
///     it. D needs no such bound, because a thief can take the starved key.
///   </para>
///   <para>
///     <b>Setup, per §6.1.</b> One hot key dispatched densely and consecutively with
///     <c>batchBound: 1</c>, so every activation self-reschedules onto the local stack; one victim
///     key dispatched early; <c>RoundRobin</c> routing, because <c>LeastLoaded</c> routes work away
///     from a visibly busy worker and destroys the setup. The B cases use a single worker so "the
///     victim is on the busy worker's ring" is structural rather than arranged.
///   </para>
///   <para>
///     <b>The oracle is POSITION, not time.</b> How many hot messages were handled before the victim
///     is exactly what starvation means, and it does not depend on machine speed.
///   </para>
/// </summary>
[TestFixture]
public class StarvationAsymmetryTests {
	private const int HotKey = 0;
	private const int VictimKey = 1;
	private const int HotMessages = 3_000;
	private const int VictimDispatchedAfter = 10;

	/// <summary>
	///   Runs the hot-key-plus-victim workload and returns how many messages were handled before the
	///   victim's single message.
	///   <para>
	///     <b>The hot handler is deliberately slower than the producer.</b> Starvation only occurs
	///     while the hot key's mailbox is non-empty at every release — that is what makes the worker
	///     self-reschedule onto its local stack instead of falling through to its ring. If the worker
	///     ever drains faster than the producer refills, the local stack empties, the ring gets
	///     checked, and the victim runs early: the test would then measure the race rather than the
	///     hazard. Burning a little time per hot message keeps the producer ahead until backpressure
	///     caps it, at which point the mailbox stays near-full BY CONSTRUCTION and the self-reschedule
	///     is guaranteed. The <c>Throw</c> hook is reused as a per-message callback — it runs at the
	///     top of <c>Handle</c> and returning <see langword="false" /> means "did not throw".
	///   </para>
	/// </summary>
	private static int VictimPosition<TScheduler>(TScheduler scheduler, int workers, int batchBound)
		where TScheduler : struct, IKeyScheduler {
		var state = new RecordingState {
			Throw = (key, _) => {
				if (key == HotKey) Thread.SpinWait(5000);
				return false;
			},
		};
		using var system = new ActorSystem<int, int, RecordingHandler, TScheduler>(
			new RecordingHandler(state), keyCapacity: 64, chunkCount: 16, chunkSize: 256,
			batchBound: batchBound, workers: workers, scheduler: scheduler);
		system.Start();

		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				for (var i = 0; i < HotMessages; i++) {
					if (i == VictimDispatchedAfter && !system.Dispatch(VictimKey, 0)) return;
					if (!system.Dispatch(HotKey, i)) return;
					dispatched++;
				}
			}
			catch (Exception ex) {
				producerError = ex;
			}
		}) { IsBackground = true };
		producer.Start();

		Assert.That(producer.Join(TimeSpan.FromSeconds(90)), Is.True,
			$"producer wedged after {Volatile.Read(ref dispatched)}/{HotMessages} dispatches");
		if (producerError is not null) Assert.Fail($"producer threw: {producerError}");

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, HotMessages + 1);

		var arrivals = state.Seen.ToList();
		var position = arrivals.FindIndex(x => x.Key == VictimKey);
		Assert.That(position, Is.GreaterThanOrEqualTo(0), "the victim key was never handled at all");
		TestContext.Out.WriteLine($"victim handled at position {position} of {arrivals.Count}");
		return position;
	}

	/// <summary>
	///   B with the bound disabled: the hazard, demonstrated. This is the test whose absence spec §6.1
	///   recorded — without it, <c>LocalDrainBound</c> is a fix for a problem nobody had reproduced.
	/// </summary>
	[Test]
	[CancelAfter(120_000)]
	public void Unbounded_Local_Draining_Starves_A_Ring_Queued_Key() {
		var position = VictimPosition(
			new RoutedScheduler {
				Capacity = 256, Policy = RoutingPolicy.RoundRobin, LocalDrainBound = int.MaxValue,
			}, workers: 1, batchBound: 1);

		// Threshold DERIVED, not guessed. Calibration history (see the task report): at
		// Thread.SpinWait(200) the mailbox intermittently emptied and this test failed outright in ~40%
		// of runs; at SpinWait(1000) the failure rate dropped but a low-tail outlier (position 60) still
		// surfaced once in 20 executions. At this file's final calibration — SpinWait(5000) above — 20/20
		// executions (10 runs x net9.0 + net10.0) landed at EXACTLY position 3000 (fully exhausted), zero
		// variance. The bounded case (LocalDrainBound = 16) is position 17 in every run across every
		// calibration tried, also zero variance — see Bounded_Local_Draining_Rescues_A_Ring_Queued_Key.
		// 200 sits ~15x BELOW the stable unbounded position and ~12x ABOVE the bounded position: inside
		// the measured gap, not inside either distribution, with slack against machines slower or noisier
		// than this one. The Step 3 vacuity check in the task report confirms LocalDrainBound = 16 still
		// fails this exact assertion (position 17 < 200) with zero variance.
		const int threshold = 200;
		Assert.That(position, Is.GreaterThan(threshold),
			"with local draining unbounded the ring-queued key should not be serviced until the hot " +
			"stream is nearly exhausted — if this fails, the starvation hazard is not what §6.1 describes");
	}

	/// <summary>B with the shipped default: the bound rescues the victim quickly.</summary>
	[Test]
	[CancelAfter(120_000)]
	public void Bounded_Local_Draining_Rescues_A_Ring_Queued_Key() {
		const int bound = 16;
		var position = VictimPosition(
			new RoutedScheduler {
				Capacity = 256, Policy = RoutingPolicy.RoundRobin, LocalDrainBound = bound,
			}, workers: 1, batchBound: 1);

		// The bound caps consecutive local pops, so the victim must surface within roughly
		// bound x batchBound messages of being routed. Generous slack: the claim is "promptly",
		// orders of magnitude below the unbounded case, not an exact figure.
		Assert.That(position, Is.LessThan(HotMessages / 10),
			$"LocalDrainBound = {bound} did not bound the local run");
	}

	/// <summary>
	///   D with no bound of any kind: the same workload that starves under unbounded B does not starve
	///   here. This is the asymmetry §6.1 asserts, measured.
	///   <para>
	///     <b>What rescues the victim is not necessarily a steal.</b> Under D the victim is submitted
	///     to the shared global queue, so an idle worker can simply dequeue it — no theft required.
	///     The claim this test makes is only the one §6.1 makes: D needs no drain bound to avoid the
	///     hazard, because the starved key is reachable by workers other than the one pinned to the
	///     hot key. Which of the two mechanisms got there first is not asserted, and the counters are
	///     printed rather than checked for exactly that reason.
	///   </para>
	/// </summary>
	[Test]
	[CancelAfter(120_000)]
	public void Work_Stealing_Rescues_A_Starved_Key_With_No_Drain_Bound_At_All() {
		var scheduler = new WorkStealingScheduler { Capacity = 256, DequeCapacity = 256 };
		var position = VictimPosition(scheduler, workers: 4, batchBound: 1);

		Assert.That(position, Is.LessThan(HotMessages / 10),
			"the victim key should have been picked up long before the hot stream ended — D is supposed " +
			"to be immune to this hazard without needing a drain bound");

		// Printed, not checked — see the remarks above on why which mechanism rescued the victim is not
		// this test's claim.
		TestContext.Out.WriteLine(
			$"steals {scheduler.StealsSucceeded}/{scheduler.StealsAttempted}, " +
			$"global {scheduler.GlobalDequeues}, overflows {scheduler.DequeOverflows}, " +
			$"parks {scheduler.Parks}, enqueueRetries {scheduler.GlobalEnqueueRetries}");
	}
}
