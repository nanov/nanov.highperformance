namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Throughput.Actors;

using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using Nanov.HighPerformance.Threading.Actors.WaitPolicies;
using Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>
///   Task 2's policy-comparison cells. <b>Not</b> part of the Phase 3 B-vs-D matrix
///   (<see cref="ActorsBTestBase" /> / <see cref="ActorsDTestBase" />) — the scheduler is fixed here and
///   the wait policy varies, the opposite of what that matrix does.
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
