namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Throughput.OneToOne;

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>
///   Baseline: 1-producer / 1-consumer unicast over a bounded <see cref="BlockingCollection{T}" /> (the
///   classic lock-based blocking queue). Analog of Disruptor's OneToOneBlockingCollectionThroughputTest.
/// </summary>
public sealed class OneToOneBlockingCollectionThroughputTest : IThroughputTest {
	private const int BufferSize = 1024 * 64;
	private const long Iterations = 1000L * 1000L * 100L;
	private static readonly long ExpectedResult = PerfTestUtil.AccumulatedAddition(Iterations);

	private readonly int? _producerCpu;
	private readonly int? _consumerCpu;

	public OneToOneBlockingCollectionThroughputTest(ProgramOptions options) {
		_producerCpu = options.GetCustomCpu(0);
		_consumerCpu = options.GetCustomCpu(1);
	}

	public int RequiredProcessorCount => 2;

	[MethodImpl(MethodImplOptions.AggressiveOptimization)]
	public long Run(ThroughputSessionContext sessionContext) {
		using var collection = new BlockingCollection<long>(BufferSize);
		var accumulator = new Accumulator();
		var consumerCpu = _consumerCpu;
		var consumer = PerfTestUtil.StartLongRunning(() => {
			if (consumerCpu is { } cpu)
				ThreadAffinityUtil.PinCurrentThread(cpu);

			foreach (var value in collection.GetConsumingEnumerable())
				accumulator.Sum += value;
		});

		using (ThreadAffinityUtil.SetThreadAffinity(_producerCpu)) {
			sessionContext.Start();

			for (var i = 0L; i < Iterations; i++)
				collection.Add(i);
			collection.CompleteAdding();

			consumer.GetAwaiter().GetResult();
			sessionContext.Stop();
		}

		PerfTestUtil.FailIfNot(ExpectedResult, accumulator.Sum,
			$"Consumer should have summed to {ExpectedResult}, but was {accumulator.Sum}");
		return Iterations;
	}
}
