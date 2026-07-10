namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Throughput.OneToOne;

using System.Runtime.CompilerServices;
using Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>
///   Keyed throughput through <see cref="CoalescingSpWorker{TKey, TValue, THandler}" /> (exactly-once,
///   additive merge). The producer dispatches over a fixed key space; a lagging consumer folds many
///   updates per key, but exactly-once delivery means the grand total still equals the sum of every
///   dispatched value — so correctness is verifiable. No Disruptor analog (Disruptor has no coalescer);
///   this measures keyed dispatch + cell-merge throughput.
/// </summary>
public sealed class OneToOneCoalescingSpWorkerThroughputTest : IThroughputTest {
	private const int KeyCount = 1024;
	private const int MapCapacity = 4096;
	private const long Iterations = 1000L * 1000L * 100L;
	private static readonly long ExpectedResult = PerfTestUtil.AccumulatedAddition(Iterations);

	private readonly int? _producerCpu;
	private readonly int? _consumerCpu;

	public OneToOneCoalescingSpWorkerThroughputTest(ProgramOptions options) {
		_producerCpu = options.GetCustomCpu(0);
		_consumerCpu = options.GetCustomCpu(1);
	}

	public int RequiredProcessorCount => 2;

	[MethodImpl(MethodImplOptions.AggressiveOptimization)]
	public long Run(ThroughputSessionContext sessionContext) {
		var accumulator = new Accumulator();
		using var worker = new CoalescingSpWorker<long, long, SummingCoalescingHandler>(
			new SummingCoalescingHandler(accumulator, _consumerCpu), MapCapacity);
		worker.Start();

		using (ThreadAffinityUtil.SetThreadAffinity(_producerCpu)) {
			sessionContext.Start();

			for (var i = 0L; i < Iterations; i++)
				worker.Dispatch(i % KeyCount, i);

			worker.TryComplete();
			worker.Completion.GetAwaiter().GetResult();

			sessionContext.Stop();
		}

		PerfTestUtil.FailIfNot(ExpectedResult, accumulator.Sum,
			$"Coalesced total should be {ExpectedResult}, but was {accumulator.Sum}");
		return Iterations;
	}
}
