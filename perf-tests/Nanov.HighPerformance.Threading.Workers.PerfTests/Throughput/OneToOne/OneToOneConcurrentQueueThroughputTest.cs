namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Throughput.OneToOne;

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>
///   Baseline: 1-producer / 1-consumer unicast over an unbounded <see cref="ConcurrentQueue{T}" /> — the
///   lock-free BCL queue with no backpressure (producer can run ahead). Analog of Disruptor's
///   OneToOneConcurrentQueueThroughputTest.
/// </summary>
public sealed class OneToOneConcurrentQueueThroughputTest : IThroughputTest {
	private const long Iterations = 1000L * 1000L * 100L;
	private static readonly long ExpectedResult = PerfTestUtil.AccumulatedAddition(Iterations);

	private readonly int? _producerCpu;
	private readonly int? _consumerCpu;

	public OneToOneConcurrentQueueThroughputTest(ProgramOptions options) {
		_producerCpu = options.GetCustomCpu(0);
		_consumerCpu = options.GetCustomCpu(1);
	}

	public int RequiredProcessorCount => 2;

	[MethodImpl(MethodImplOptions.AggressiveOptimization)]
	public long Run(ThroughputSessionContext sessionContext) {
		var queue = new ConcurrentQueue<long>();
		var accumulator = new Accumulator();
		var consumerCpu = _consumerCpu;
		var consumer = PerfTestUtil.StartLongRunning(() => {
			if (consumerCpu is { } cpu)
				ThreadAffinityUtil.PinCurrentThread(cpu);

			var spin = new SpinWait();
			var remaining = Iterations;
			while (remaining > 0) {
				if (queue.TryDequeue(out var value)) {
					accumulator.Sum += value;
					remaining--;
					spin.Reset();
				} else {
					spin.SpinOnce();
				}
			}
		});

		using (ThreadAffinityUtil.SetThreadAffinity(_producerCpu)) {
			sessionContext.Start();

			for (var i = 0L; i < Iterations; i++)
				queue.Enqueue(i);

			consumer.GetAwaiter().GetResult();
			sessionContext.Stop();
		}

		PerfTestUtil.FailIfNot(ExpectedResult, accumulator.Sum,
			$"Consumer should have summed to {ExpectedResult}, but was {accumulator.Sum}");
		return Iterations;
	}
}
