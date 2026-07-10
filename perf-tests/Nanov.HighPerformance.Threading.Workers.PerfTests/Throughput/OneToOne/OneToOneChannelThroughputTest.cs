namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Throughput.OneToOne;

using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>
///   Baseline: 1-producer / 1-consumer unicast over a bounded <see cref="Channel{T}" /> (SingleReader /
///   SingleWriter), the natural BCL comparison for the SPSC workers.
/// </summary>
public sealed class OneToOneChannelThroughputTest : IThroughputTest {
	private const int BufferSize = 1024 * 64;
	private const long Iterations = 1000L * 1000L * 100L;
	private static readonly long ExpectedResult = PerfTestUtil.AccumulatedAddition(Iterations);

	private readonly int? _producerCpu;
	private readonly int? _consumerCpu;

	public OneToOneChannelThroughputTest(ProgramOptions options) {
		_producerCpu = options.GetCustomCpu(0);
		_consumerCpu = options.GetCustomCpu(1);
	}

	public int RequiredProcessorCount => 2;

	[MethodImpl(MethodImplOptions.AggressiveOptimization)]
	public long Run(ThroughputSessionContext sessionContext) {
		var channel = Channel.CreateBounded<long>(new BoundedChannelOptions(BufferSize) {
			FullMode = BoundedChannelFullMode.Wait,
			SingleReader = true,
			SingleWriter = true
		});

		var accumulator = new Accumulator();
		var consumerCpu = _consumerCpu;
		var consumer = PerfTestUtil.StartLongRunning(() => {
			if (consumerCpu is { } cpu)
				ThreadAffinityUtil.PinCurrentThread(cpu);

			var reader = channel.Reader;
			var spin = new SpinWait();
			var remaining = Iterations;
			while (remaining > 0) {
				if (reader.TryRead(out var value)) {
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

			var writer = channel.Writer;
			var spin = new SpinWait();
			for (var i = 0L; i < Iterations; i++) {
				while (!writer.TryWrite(i))
					spin.SpinOnce();
				spin.Reset();
			}

			consumer.GetAwaiter().GetResult();
			sessionContext.Stop();
		}

		PerfTestUtil.FailIfNot(ExpectedResult, accumulator.Sum,
			$"Consumer should have summed to {ExpectedResult}, but was {accumulator.Sum}");
		return Iterations;
	}
}
