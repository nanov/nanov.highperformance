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
///     under B nothing can move that key elsewhere, at ANY worker count, because the ring is
///     per-worker. <c>LocalDrainBound</c> caps the local run to fix it. D's demonstrated immunity below
///     (see <see cref="A_Starved_Key_Is_Rescued_Under_D_With_No_Drain_Bound" />) comes from having an
///     IDLE PEER able to reach the starved key through a structure that is shared rather than
///     per-worker — not from some mechanism B structurally lacks at the same worker count. That test's
///     remarks scope the claim to N ≥ 2 and explain why N = 1 does not distinguish the schedulers at
///     all.
///   </para>
///   <para>
///     <b>Setup, per §6.1.</b> One hot key dispatched densely and consecutively with
///     <c>batchBound: 1</c>, so every activation self-reschedules onto the local stack; one victim
///     key dispatched early; <c>RoundRobin</c> routing, because <c>LeastLoaded</c> routes work away
///     from a visibly busy worker and destroys the setup. The B cases use a single worker so "the
///     victim is on the busy worker's ring" is structural rather than arranged.
///   </para>
///   <para>
///     <b><c>RoundRobin</c> is required by §6.1's construction only at N &gt; 1; at the single worker
///     the B tests below actually use it is a no-op.</b> <c>RoutedScheduler.Route</c> reduces to
///     <c>(uint)n % 1 == 0</c> and <c>LeastLoadedIndex</c> also always returns worker 0, so
///     <see cref="RoutingPolicy" /> has literally no effect on either B test's result here — nothing in
///     this file would catch a later reader changing <c>Policy</c> on the B schedulers below. It is set
///     to <c>RoundRobin</c> anyway to match §6.1's construction and because it is exactly what the next
///     paragraph is about.
///   </para>
///   <para>
///     <b>This workload does NOT generalise to N &gt; 1 by simply raising <c>workers</c>.</b> Only two
///     keys ever reach the router — the hot key's first schedule and the victim's — so at N ≥ 2 with
///     <c>RoundRobin</c> the victim would very likely land directly on an idle worker's own ring and
///     run almost immediately: no starvation, for reasons unrelated to whether §6.1's claim holds at
///     N &gt; 1. Demonstrating the B hazard at N &gt; 1 needs a different construction (e.g. enough hot
///     keys that every worker is independently saturated), which this file does not attempt.
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

	// Threshold DERIVED, not guessed, and shared by BOTH the unbounded (B) and D assertions below so
	// the two form an actual partition rather than two independently-chosen numbers that both happen
	// to leave a band of positions that would satisfy neither claim. Calibration history (task report):
	// at Thread.SpinWait(200) the mailbox intermittently emptied and the unbounded test failed outright
	// in ~40% of runs; at SpinWait(1000) the failure rate dropped but a low-tail outlier (position 60)
	// still surfaced once in 20 executions. At this file's final calibration — SpinWait(5000) below —
	// 20/20 executions (10 runs x net9.0 + net10.0) landed at EXACTLY position 3000 (fully exhausted),
	// zero variance, matching the closed form (position == HotMessages, because with one worker the
	// ring is unreachable until the producer stops). The D test's positions are 0-28 across every run
	// measured, always far below this threshold. 200 sits ~15x BELOW the stable unbounded position and
	// ~12x ABOVE the bounded position (bound + 1 = 17, also a closed form — see
	// Bounded_Local_Draining_Rescues_A_Ring_Queued_Key): inside the measured gap, not inside either
	// distribution, with slack against machines slower or noisier than this one. The Step 3 vacuity
	// check in the task report confirms LocalDrainBound = 16 still fails the unbounded assertion
	// (position 17 &lt; 200) with zero variance.
	private const int StarvationThreshold = 200;

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
		// Both Dispatch failure paths above `return` without failing the test — a run silently
		// truncated by a rejected Dispatch would otherwise complete cleanly and pass. Pin the count.
		Assert.That(Volatile.Read(ref dispatched), Is.EqualTo(HotMessages),
			"producer dispatch was silently truncated — a Dispatch call was rejected mid-run");

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

		Assert.That(position, Is.GreaterThan(StarvationThreshold),
			"with local draining unbounded the ring-queued key should not be serviced until the hot " +
			"stream is nearly exhausted — if this fails, the starvation hazard is not what §6.1 describes");
	}

	/// <summary>
	///   B with the shipped default: the bound rescues the victim at an exact, predictable position.
	///   <para>
	///     <b>The position is a closed form, not an empirical magnitude that happened to land well.</b>
	///     <c>RoutedScheduler.TryAcquire</c> increments <c>LocalRun</c> only on a local pop and resets
	///     it to 0 only on a ring pop; with <c>batchBound: 1</c> every hot activation is exactly one
	///     local pop, so after <c>LocalDrainBound</c> consecutive local pops the ring is checked next
	///     and the victim — the only thing on it — is taken. Position = <c>LocalDrainBound + 1</c>,
	///     deterministically. Every calibration measured in the task report (spin 200 through 5000)
	///     landed on exactly 17 for <c>bound = 16</c>, zero variance — confirming the closed form
	///     rather than merely being consistent with it.
	///   </para>
	/// </summary>
	[Test]
	[CancelAfter(120_000)]
	public void Bounded_Local_Draining_Rescues_A_Ring_Queued_Key() {
		const int bound = 16;
		var position = VictimPosition(
			new RoutedScheduler {
				Capacity = 256, Policy = RoutingPolicy.RoundRobin, LocalDrainBound = bound,
			}, workers: 1, batchBound: 1);

		Assert.That(position, Is.EqualTo(bound + 1),
			$"LocalDrainBound = {bound} should deterministically rescue the victim at position " +
			$"bound + 1 = {bound + 1} — TryAcquire pops exactly {bound} consecutive local activations " +
			"before the ring gets priority");
	}

	/// <summary>
	///   D with no bound of any kind: the same workload that starves under unbounded B does not starve
	///   here. This is the asymmetry §6.1 asserts, measured.
	///   <para>
	///     <b>Scoped to N ≥ 2 workers — this is NOT a property D has that B lacks at equal worker
	///     count.</b> At <c>workers: 1</c>, D exhibits the identical hazard, unbounded, with nothing to
	///     fix it: <c>WorkStealingScheduler.TryAcquire</c> pops the worker's own deque unconditionally
	///     before ever consulting the global queue, and <c>ScheduleLocal</c> pushes the self-reschedule
	///     onto that SAME deque — with one worker the global queue, where the victim sits, is
	///     unreachable until the hot key's mailbox finally drains, giving position 3000, exactly as
	///     under unbounded B. The honest form of the asymmetry §6.1 actually argues, and what this
	///     file's data supports: under B a ring-queued key is unreachable by idle peers even at
	///     N &gt; 1, because the ring is per-worker; under D the starved key sits in a structure any
	///     idle peer can reach. D's immunity comes from HAVING an idle peer able to reach a shared
	///     structure, not from some mechanism B structurally lacks. Reading "D needs no drain bound" as
	///     "N = 1 is safe under D" would hit exactly the hazard this file exists to demonstrate.
	///   </para>
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
	public void A_Starved_Key_Is_Rescued_Under_D_With_No_Drain_Bound() {
		var scheduler = new WorkStealingScheduler { Capacity = 256, DequeCapacity = 256 };
		var position = VictimPosition(scheduler, workers: 4, batchBound: 1);

		Assert.That(position, Is.LessThan(StarvationThreshold),
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
