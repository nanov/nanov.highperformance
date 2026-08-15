namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Throughput.Actors;

using System;
using System.Diagnostics;
using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using Nanov.HighPerformance.Threading.Actors.WaitPolicies;
using Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>
///   Task 2's original policy-comparison cells: one cell per policy, discovered and ordered by
///   <see cref="PerfTestTypeSelector" />.
///   <para>
///     <b>Kept, but demoted — do not read these as the primary measurement.</b> A review of Task 2 found
///     the shape itself is biased on this host: <c>PerfTestTypeSelector</c> sorts discovered tests by name
///     ascending, so a <c>--target Wait</c> sweep always runs <c>WaitBounded…</c> before
///     <c>WaitLegacy…</c> (and would have run <c>WaitBackoff…</c> before both). If host load drifts across
///     the sweep — and on this host it does — one arm systematically inherits the quieter window and the
///     other the noisier one, and nothing in the run distinguishes a real effect from that drift. Task 3
///     replaces the comparison with <see cref="WaitPolicyPairedCellBase" /> below, which interleaves both
///     policies inside a single session so drift lands on both arms and cancels in the paired ratio. These
///     four are left in place only as individual smoke checks (each policy still drains 10M messages
///     cleanly under D) and as a way to sanity-check the paired cells' own numbers against the shape this
///     file used to report.
///   </para>
///   <para>
///     Scheduler fixed to D (<see cref="WorkStealingScheduler" />) — it has the most idle churn, so a
///     ladder change should show first there. Cardinality 1024, uniform skew, held constant across all
///     four cells so only the policy and the per-message cost differ.
///   </para>
/// </summary>
public abstract class WaitPolicyCellBase : ActorThroughputTestBase {
	protected WaitPolicyCellBase(ProgramOptions options) : base(options) { }

	protected sealed override int Cardinality => 1024;

	protected sealed override Skew Skew => Skew.Uniform;

	protected abstract IWaitPolicy WaitPolicy { get; }

	protected override long RunStream(int[] keys, ActorPerfState state, int? producerCpu) =>
		Drive(new WorkStealingScheduler { Capacity = Cardinality * 4, DequeCapacity = 256 },
			Cardinality, keys, state, producerCpu, WaitPolicy);
}

/// <summary>
///   Legacy ladder, zero per-message cost — the cell where idle park/unpark churn dominates and a ladder
///   change is most likely to show.
/// </summary>
public sealed class WaitLegacySpin0ThroughputTest : WaitPolicyCellBase {
	public WaitLegacySpin0ThroughputTest(ProgramOptions options) : base(options) { }
	protected override IWaitPolicy WaitPolicy => new LegacyWaitPolicy();
	protected override int SpinPerMessage => 0;
}

/// <summary>
///   Legacy ladder at the Phase 3 calibrated per-message cost — the consumer-bound cell where the ladder
///   may be invisible.
/// </summary>
public sealed class WaitLegacySpin32ThroughputTest : WaitPolicyCellBase {
	public WaitLegacySpin32ThroughputTest(ProgramOptions options) : base(options) { }
	protected override IWaitPolicy WaitPolicy => new LegacyWaitPolicy();
	protected override int SpinPerMessage => 32;
}

/// <summary>Bounded block (default 20 ms), zero per-message cost — Legacy's Spin0 counterpart.</summary>
public sealed class WaitBoundedSpin0ThroughputTest : WaitPolicyCellBase {
	public WaitBoundedSpin0ThroughputTest(ProgramOptions options) : base(options) { }
	protected override IWaitPolicy WaitPolicy => new BoundedWaitPolicy();
	protected override int SpinPerMessage => 0;
}

/// <summary>Bounded block (default 20 ms) at the Phase 3 calibrated per-message cost.</summary>
public sealed class WaitBoundedSpin32ThroughputTest : WaitPolicyCellBase {
	public WaitBoundedSpin32ThroughputTest(ProgramOptions options) : base(options) { }
	protected override IWaitPolicy WaitPolicy => new BoundedWaitPolicy();
	protected override int SpinPerMessage => 32;
}

/// <summary>
///   Task 3's replacement for the one-cell-per-policy shape above: Legacy and Backoff interleaved inside a
///   single session, so a drifting host biases both arms equally instead of biasing whichever name sorts
///   first.
///   <para>
///     <b>Shape.</b> Each round is one full system build, a complete 10M-message drain, and a disposal —
///     for one policy. Rounds alternate Legacy, Backoff, Legacy, Backoff, … for <see cref="Rounds" /> rounds
///     per arm, and the leading arm within each round also alternates (Legacy leads on even rounds, Backoff
///     on odd) so even within-round ordering cannot bias the result. This does not reuse
///     <see cref="ActorThroughputTestBase.Drive{TScheduler}" /> — that helper times a single arm end to end
///     via <see cref="ThroughputSessionContext" />, and a paired cell needs an independent, per-round timer
///     for each arm plus its own printed table, so it drives the system directly, the same way
///     <see cref="ProducerBoundCalibrationTest" /> drives its own internal sweep.
///   </para>
///   <para>
///     <b>What gets printed vs. what gets returned.</b> <see cref="IThroughputTest.Run" /> must return an
///     operation count for the harness's own ops/sec line, so this returns the summed handled count across
///     every round of both arms — that aggregate is not the interesting number here and should not be read
///     as one. The interesting numbers are the per-round (Legacy, Backoff) pairs and the median of their
///     ratios, both written to the console from inside <see cref="Run" />, because <c>ThroughputSessionContext</c>
///     has nowhere to carry a table.
///   </para>
///   <para>
///     <b>Iteration budget.</b> Each round drains the same 10M messages the single-policy cells above use
///     per <c>Run()</c> call, so one paired <c>Run()</c> already does 2 × <see cref="Rounds" /> of that work
///     — the internal alternation is what supplies the repeat count a paired comparison needs.
///     <c>ThroughputTestSession</c> still wraps this in its own <c>--runs</c> loop (default 7); passing
///     <c>--runs 1</c> when driving these cells avoids paying for that repetition twice.
///   </para>
/// </summary>
public abstract class WaitPolicyPairedCellBase : IThroughputTest {
	private const int Workers = 8;
	private const int Cardinality = 1024;
	private const long Iterations = 10L * 1000L * 1000L;
	private const int Rounds = 8;

	private readonly int? _producerCpu;

	protected WaitPolicyPairedCellBase(ProgramOptions options) => _producerCpu = options.GetCustomCpu(0);

	protected abstract int SpinPerMessage { get; }

	public int RequiredProcessorCount => Workers + 1;

	public long Run(ThroughputSessionContext sessionContext) {
		var keys = KeySequence.Uniform(Cardinality, seed: (int)Skew.Uniform * 7919 + Cardinality);

		var legacyOpsPerSecond = new double[Rounds];
		var backoffOpsPerSecond = new double[Rounds];
		var totalHandled = 0L;

		Console.WriteLine();
		Console.WriteLine($"  Paired rounds ({GetType().Name}) — leader alternates per round:");
		Console.WriteLine("  round | leader  | legacy msgs/sec  | backoff msgs/sec | ratio (backoff/legacy)");
		Console.WriteLine("  ------+---------+------------------+------------------+------------------------");

		sessionContext.Start();
		for (var round = 0; round < Rounds; round++) {
			var legacyLeads = round % 2 == 0;
			double legacyOps;
			double backoffOps;

			if (legacyLeads) {
				legacyOps = RunArm(new LegacyWaitPolicy(), keys, ref totalHandled);
				backoffOps = RunArm(new BackoffWaitPolicy(), keys, ref totalHandled);
			}
			else {
				backoffOps = RunArm(new BackoffWaitPolicy(), keys, ref totalHandled);
				legacyOps = RunArm(new LegacyWaitPolicy(), keys, ref totalHandled);
			}

			legacyOpsPerSecond[round] = legacyOps;
			backoffOpsPerSecond[round] = backoffOps;
			Console.WriteLine(
				$"  {round,5} | {(legacyLeads ? "legacy" : "backoff"),-7} | {legacyOps,16:N0} | {backoffOps,16:N0} | {backoffOps / legacyOps,22:F3}");
		}

		sessionContext.Stop();

		var ratios = new double[Rounds];
		for (var round = 0; round < Rounds; round++)
			ratios[round] = backoffOpsPerSecond[round] / legacyOpsPerSecond[round];
		Array.Sort(ratios);
		var medianRatio = Rounds % 2 == 0
			? (ratios[Rounds / 2 - 1] + ratios[Rounds / 2]) / 2.0
			: ratios[Rounds / 2];

		Console.WriteLine();
		Console.WriteLine($"  Median of per-round ratios (backoff/legacy): {medianRatio:F3}");
		Console.WriteLine("  This is the drift-robust statistic — NOT the ratio of the two arms' medians.");

		return totalHandled;
	}

	/// <summary>Runs one arm's round: a fresh system, a fresh handler state, a full drain, a disposal.</summary>
	private double RunArm(IWaitPolicy waitPolicy, int[] keys, ref long totalHandled) {
		var state = new ActorPerfState { SpinPerMessage = SpinPerMessage };
		var stopwatch = Stopwatch.StartNew();
		var handled = Drive(waitPolicy, keys, state);
		stopwatch.Stop();

		PerfTestUtil.FailIfNot(Iterations, handled,
			$"{waitPolicy.GetType().Name}: expected {Iterations} handled, saw {handled}");
		totalHandled += handled;
		return Iterations / stopwatch.Elapsed.TotalSeconds;
	}

	private long Drive(IWaitPolicy waitPolicy, int[] keys, ActorPerfState state) {
		using var system = new ActorSystem<int, long, ActorPerfHandler, WorkStealingScheduler>(
			new ActorPerfHandler(state), keyCapacity: Cardinality * 2, chunkCount: Cardinality,
			chunkSize: 256, batchBound: 16, workers: Workers,
			scheduler: new WorkStealingScheduler { Capacity = Cardinality * 4, DequeCapacity = 256 },
			waitPolicy: waitPolicy);
		system.Start();

		using (ThreadAffinityUtil.SetThreadAffinity(_producerCpu)) {
			var mask = keys.Length - 1;
			for (var i = 0L; i < Iterations; i++)
				system.Dispatch(keys[(int)(i & mask)], i);

			system.TryComplete();
			system.Completion.GetAwaiter().GetResult();
		}

		return state.HandledCount;
	}
}

/// <summary>
///   Zero per-message cost — the interesting paired cell. This is where idle park/unpark churn dominates
///   and where a better ladder should show up in the paired ratio if it is going to show up anywhere.
/// </summary>
public sealed class WaitPairedSpin0ThroughputTest : WaitPolicyPairedCellBase {
	public WaitPairedSpin0ThroughputTest(ProgramOptions options) : base(options) { }
	protected override int SpinPerMessage => 0;
}

/// <summary>
///   The Phase 3 calibrated per-message cost — the consumer-bound paired cell where the ladder may be
///   invisible against the handler's own cost.
/// </summary>
public sealed class WaitPairedSpin32ThroughputTest : WaitPolicyPairedCellBase {
	public WaitPairedSpin32ThroughputTest(ProgramOptions options) : base(options) { }
	protected override int SpinPerMessage => 32;
}
