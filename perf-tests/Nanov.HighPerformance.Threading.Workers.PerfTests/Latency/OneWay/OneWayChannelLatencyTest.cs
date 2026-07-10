namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Latency.OneWay;

using System.Threading;
using System.Threading.Channels;
using Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>Baseline: one-way latency over a bounded <see cref="Channel{T}" />. Analog of Disruptor's OneWayChannelLatencyTest.</summary>
public sealed class OneWayChannelLatencyTest : ILatencyTest {
	private const int BufferSize = 1024;
	private const long Iterations = 100L * 1000L * 30L;

	private readonly int? _producerCpu;
	private readonly int? _consumerCpu;

	public OneWayChannelLatencyTest(ProgramOptions options) {
		_producerCpu = options.GetCustomCpu(0);
		_consumerCpu = options.GetCustomCpu(1);
	}

	public int RequiredProcessorCount => 2;

	public void Run(LatencySessionContext sessionContext) {
		var channel = Channel.CreateBounded<long>(new BoundedChannelOptions(BufferSize) {
			FullMode = BoundedChannelFullMode.Wait,
			SingleReader = true,
			SingleWriter = true
		});

		var histogram = sessionContext.Histogram;
		var completed = new ManualResetEventSlim(false);
		var consumerCpu = _consumerCpu;
		var consumer = PerfTestUtil.StartLongRunning(() => {
			if (consumerCpu is { } cpu)
				ThreadAffinityUtil.PinCurrentThread(cpu);

			var reader = channel.Reader;
			var spin = new SpinWait();
			while (true) {
				if (!reader.TryRead(out var sentTimestamp)) {
					spin.SpinOnce();
					continue;
				}

				spin.Reset();
				if (sentTimestamp == -1) {
					completed.Set();
					return;
				}

				var duration = StopwatchUtil.GetTimestamp() - sentTimestamp;
				histogram.RecordValue(StopwatchUtil.ToNanoseconds(duration));
			}
		});

		using (ThreadAffinityUtil.SetThreadAffinity(_producerCpu)) {
			var pause = StopwatchUtil.GetTimestampFromMicroseconds(10);
			var next = StopwatchUtil.GetTimestamp() + pause;
			var writer = channel.Writer;
			var spin = new SpinWait();

			sessionContext.Start();

			for (var i = 0L; i < Iterations; i++) {
				var now = StopwatchUtil.GetTimestamp();
				while (now < next) {
					Thread.Yield();
					now = StopwatchUtil.GetTimestamp();
				}

				while (!writer.TryWrite(now))
					spin.SpinOnce();
				spin.Reset();
				next = now + pause;
			}

			while (!writer.TryWrite(-1))
				spin.SpinOnce();

			completed.Wait();
			sessionContext.Stop();
		}

		consumer.GetAwaiter().GetResult();
	}
}
