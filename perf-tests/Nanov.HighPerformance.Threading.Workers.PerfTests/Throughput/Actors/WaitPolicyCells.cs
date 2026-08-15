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
///   Task 3's replacement for the one-cell-per-policy shape above: Legacy, Bounded and Backoff interleaved
///   inside a single session, so a drifting host biases all three arms equally instead of biasing whichever
///   name sorts first.
///   <para>
///     <b>Three arms, not two.</b> <see cref="BackoffWaitPolicy" /> differs from
///     <see cref="LegacyWaitPolicy" /> in three things at once — exponential rungs, a yield tier, AND a
///     bounded 20&#160;ms block instead of an unbounded one. A Legacy-vs-Backoff win alone cannot say which
///     of those three earned it, and bounding the block is exactly what <see cref="BoundedWaitPolicy" />
///     already isolated in Task 2. <see cref="BoundedWaitPolicy" /> is carried here as the control arm so
///     the Bounded→Backoff ratio can answer that question on its own, independent of the Legacy→Backoff
///     headline: Legacy→Bounded isolates "bounding the block alone", Bounded→Backoff isolates "the ladder
///     and yield tier on top of an already-bounded block", and Legacy→Backoff is just their combination.
///   </para>
///   <para>
///     <b>Shape.</b> Each round is one full system build, a complete 10M-message drain, and a disposal, per
///     arm. The three arms' EXECUTION ORDER cycles round to round — round <c>r</c> leads with arm
///     <c>r % 3</c> and follows with the other two in a fixed rotation — so across any <see cref="Rounds" />
///     rounds every arm spends an equal share of rounds leading, second, and trailing rather than one arm
///     structurally always going first or last. This does not reuse
///     <see cref="ActorThroughputTestBase.Drive{TScheduler}" /> — that helper times a single arm end to end
///     via <see cref="ThroughputSessionContext" />, and a paired cell needs an independent, per-round timer
///     for each arm plus its own printed table, so it drives the system directly, the same way
///     <see cref="ProducerBoundCalibrationTest" /> drives its own internal sweep.
///   </para>
///   <para>
///     <b>A discarded warm-up round runs first,</b> driving all three arms once before the measured loop
///     starts and throwing its numbers away. Round 0 of the measured loop would otherwise silently absorb
///     the JIT of the <c>ActorSystem</c> instantiation, first-touch page faults on the 2&#160;MB chunk slab,
///     and first thread creation — all real costs, none of them about the wait policy. The warm-up also
///     gives every arm's <c>Next</c> call site at least one invocation before tiered compilation's rejit
///     decision, which matters for the next point.
///   </para>
///   <para>
///     <b>The one bias round-robin execution order cannot cancel: tiered-PGO devirtualisation.</b>
///     <c>ActorSystem._waitPolicy</c> is a plain <see cref="IWaitPolicy" />, and the <c>WaitForWork</c> call
///     site that invokes <c>Next</c> is JITted once per generic <c>ActorSystem</c> instantiation per
///     process. Whichever concrete policy type that call site sees FIRST — before tier-1 rejit — sets the
///     class profile the runtime devirtualises against; every OTHER policy's calls through that same call
///     site pay a failed type-guard check and run un-inlined for the rest of the process, no matter what
///     order later rounds execute in. Rotating the leader spreads which arm goes first WITHIN a round, but
///     the process only has one "first ever" call, and <c>PerfTestTypeSelector</c> sorting cells ascending
///     by name means <c>WaitBounded*</c> and <c>WaitLegacy*</c> (the single-policy cells above) also run
///     BEFORE <c>WaitPaired*</c> in the same process when driven with <c>--target Wait</c>, and
///     <c>WaitPairedSpin32</c> inherits whatever profile <c>WaitPairedSpin0</c> left behind. Direction is
///     conservative — it disfavours a policy other than whichever the process saw first, and the magnitude
///     is bounded by the same ns-call-vs-µs-spin argument in <see cref="IWaitPolicy" />'s own doc — but it
///     is exactly the kind of few-percent systematic this paired design exists to remove, and spin&#160;0 is
///     where the idle path is hottest and would show it most.
///   </para>
///   <para>
///     <b>The only supported invocation of these two cells is therefore: one cell per process, with tiered
///     PGO disabled</b> — <c>DOTNET_TieredPGO=0 dotnet run ... --target WaitPairedSpin0 --runs 1</c> (and a
///     separate process for <c>WaitPairedSpin32</c>). Driving both cells, or any single-policy cell above,
///     in the same process invalidates the class-profile argument above. Nothing here enforces the
///     environment variable — a process can't reliably assert how it was launched — so this is a
///     documentation constraint, not a runtime guard; the warm-up round is the partial, in-process
///     mitigation, and it helps without guaranteeing symmetry on its own.
///   </para>
///   <para>
///     <b>What gets printed vs. what gets returned.</b> <see cref="IThroughputTest.Run" /> must return an
///     operation count for the harness's own ops/sec line, so this returns the summed handled count across
///     every round of all three arms plus the warm-up — that aggregate is not the interesting number here
///     and should not be read as one. The interesting numbers are the per-round (Legacy, Bounded, Backoff)
///     triples, the three pairwise ratios, and — per <see cref="ActorThroughputTestBase" />'s own "treat
///     differences smaller than the spread as absent" rule — the min/max range alongside each ratio's
///     median, not just the median alone. All of it is written to the console from inside <see cref="Run" />
///     because <c>ThroughputSessionContext</c> has nowhere to carry a table.
///   </para>
///   <para>
///     <b>Iteration budget and <c>--runs</c>.</b> Each round drains the same 10M messages the single-policy
///     cells above use per <c>Run()</c> call, so one paired <c>Run()</c> already does 3 × <see cref="Rounds" />
///     of that work plus one warm-up round of 3 more — the internal rotation is what supplies the repeat
///     count a paired comparison needs. <c>ThroughputTestSession</c>'s own <c>--runs</c> loop (default 7)
///     would multiply that again with no aggregate to show for the repetition, so the constructor rejects
///     any <c>ProgramOptions.RunCountForThroughputTest</c> above 1 rather than merely documenting it.
///   </para>
/// </summary>
public abstract class WaitPolicyPairedCellBase : IThroughputTest {
	private const int Workers = 8;
	private const int Cardinality = 1024;
	private const long Iterations = 10L * 1000L * 1000L;

	// A multiple of ArmCount so the round-robin leader rotation completes whole cycles: every arm leads,
	// sits second, and trails exactly Rounds / ArmCount times. >= 8, per the brief's "at least 8 rounds
	// per arm" (each round below measures all three arms, so Rounds itself is the per-arm round count).
	private const int Rounds = 9;

	private const int ArmCount = 3;
	private const int LegacyArm = 0;
	private const int BoundedArm = 1;
	private const int BackoffArm = 2;

	private readonly int? _producerCpu;

	protected WaitPolicyPairedCellBase(ProgramOptions options) {
		if (options.RunCountForThroughputTest > 1)
			throw new ArgumentException(
				$"{GetType().Name} already repeats {Rounds} rounds per arm inside its own loop (see the class " +
				$"doc's PGO/process note); layering ThroughputTestSession's own --runs " +
				$"({options.RunCountForThroughputTest}) on top multiplies the cost with no aggregate to show " +
				"for it and no additional PGO isolation. Pass --runs 1.", nameof(options));

		_producerCpu = options.GetCustomCpu(0);
	}

	protected abstract int SpinPerMessage { get; }

	public int RequiredProcessorCount => Workers + 1;

	public long Run(ThroughputSessionContext sessionContext) {
		var keys = KeySequence.Uniform(Cardinality, seed: (int)Skew.Uniform * 7919 + Cardinality);
		var totalHandled = 0L;

		Console.WriteLine();
		Console.WriteLine($"  Warm-up round ({GetType().Name}) — discarded; JIT/PGO/paging exposure for all three arms:");
		for (var armIndex = 0; armIndex < ArmCount; armIndex++)
			RunArm(armIndex, keys, ref totalHandled);

		var legacyOpsPerSecond = new double[Rounds];
		var boundedOpsPerSecond = new double[Rounds];
		var backoffOpsPerSecond = new double[Rounds];

		Console.WriteLine();
		Console.WriteLine($"  Paired rounds ({GetType().Name}) — execution order rotates every round:");
		Console.WriteLine("  round | order                  | legacy msgs/sec  | bounded msgs/sec | backoff msgs/sec ");
		Console.WriteLine("  ------+------------------------+------------------+------------------+------------------");

		sessionContext.Start();
		for (var round = 0; round < Rounds; round++) {
			var leadArm = round % ArmCount;
			var opsByArm = new double[ArmCount];
			for (var slot = 0; slot < ArmCount; slot++) {
				var armIndex = (leadArm + slot) % ArmCount;
				opsByArm[armIndex] = RunArm(armIndex, keys, ref totalHandled);
			}

			legacyOpsPerSecond[round] = opsByArm[LegacyArm];
			boundedOpsPerSecond[round] = opsByArm[BoundedArm];
			backoffOpsPerSecond[round] = opsByArm[BackoffArm];

			var order = $"{ArmCode(leadArm)}→{ArmCode((leadArm + 1) % ArmCount)}→{ArmCode((leadArm + 2) % ArmCount)}";
			Console.WriteLine(
				$"  {round,5} | {order,-22} | {opsByArm[LegacyArm],16:N0} | {opsByArm[BoundedArm],16:N0} | {opsByArm[BackoffArm],16:N0}");
		}

		sessionContext.Stop();

		Console.WriteLine();
		PrintRatio("Legacy → Bounded (bounding the block alone)", legacyOpsPerSecond, boundedOpsPerSecond);
		PrintRatio("Legacy → Backoff (the full change)", legacyOpsPerSecond, backoffOpsPerSecond);
		PrintRatio("Bounded → Backoff (ladder + yield ALONE, block already bounded)", boundedOpsPerSecond,
			backoffOpsPerSecond);
		Console.WriteLine("  Each line's value is the MEDIAN of the per-round ratios (drift-robust), not the");
		Console.WriteLine("  ratio of the two arms' medians. Range is min–max across the same per-round ratios;");
		Console.WriteLine("  treat a difference smaller than the range as absent, per this file's own rule.");

		return totalHandled;
	}

	private static string ArmCode(int armIndex) => armIndex switch {
		LegacyArm => "Legacy",
		BoundedArm => "Bounded",
		BackoffArm => "Backoff",
		_ => throw new ArgumentOutOfRangeException(nameof(armIndex)),
	};

	private static IWaitPolicy CreateWaitPolicy(int armIndex) => armIndex switch {
		LegacyArm => new LegacyWaitPolicy(),
		BoundedArm => new BoundedWaitPolicy(),
		BackoffArm => new BackoffWaitPolicy(),
		_ => throw new ArgumentOutOfRangeException(nameof(armIndex)),
	};

	private static void PrintRatio(string label, double[] from, double[] to) {
		var ratios = new double[from.Length];
		for (var i = 0; i < from.Length; i++)
			ratios[i] = to[i] / from[i];
		Array.Sort(ratios);

		var median = ratios.Length % 2 == 0
			? (ratios[(ratios.Length / 2) - 1] + ratios[ratios.Length / 2]) / 2.0
			: ratios[ratios.Length / 2];

		Console.WriteLine($"  {label}: median {median:F3}, range {ratios[0]:F3}–{ratios[^1]:F3}");
	}

	/// <summary>
	///   Runs one arm's round: collects outside the timer so the previous arm's <c>ChunkSlab</c> finalizers
	///   don't run concurrently with this arm's measurement, constructs the policy (also outside the timer)
	///   and a fresh handler state, then a fresh system, a full drain, and a disposal.
	/// </summary>
	private double RunArm(int armIndex, int[] keys, ref long totalHandled) {
		GC.Collect();
		GC.WaitForPendingFinalizers();
		GC.Collect();

		var waitPolicy = CreateWaitPolicy(armIndex);
		var state = new ActorPerfState { SpinPerMessage = SpinPerMessage };
		var stopwatch = Stopwatch.StartNew();
		var handled = Drive(waitPolicy, keys, state);
		stopwatch.Stop();

		PerfTestUtil.FailIfNot(Iterations, handled,
			$"{ArmCode(armIndex)}: expected {Iterations} handled, saw {handled}");
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
///   and where a better ladder should show up in the paired ratios if it is going to show up anywhere. See
///   the base class doc for the required single-process, <c>DOTNET_TieredPGO=0</c>, <c>--runs 1</c>
///   invocation.
/// </summary>
public sealed class WaitPairedSpin0ThroughputTest : WaitPolicyPairedCellBase {
	public WaitPairedSpin0ThroughputTest(ProgramOptions options) : base(options) { }
	protected override int SpinPerMessage => 0;
}

/// <summary>
///   The Phase 3 calibrated per-message cost — the consumer-bound paired cell where the ladder may be
///   invisible against the handler's own cost. See the base class doc for the required single-process,
///   <c>DOTNET_TieredPGO=0</c>, <c>--runs 1</c> invocation — this cell must not share a process with
///   <see cref="WaitPairedSpin0ThroughputTest" />.
/// </summary>
public sealed class WaitPairedSpin32ThroughputTest : WaitPolicyPairedCellBase {
	public WaitPairedSpin32ThroughputTest(ProgramOptions options) : base(options) { }
	protected override int SpinPerMessage => 32;
}
