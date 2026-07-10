namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Latency.PingPong;

using System.Threading;
using System.Threading.Channels;
using Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>Baseline: round-trip latency between two bounded <see cref="Channel{T}" />s. Analog of Disruptor's PingPongChannelLatencyTest.</summary>
public sealed class PingPongChannelLatencyTest : ILatencyTest {
	private const int BufferSize = 1024;
	private const long Iterations = 100L * 1000L * 30L;
	private const long PauseNanos = 1000L;

	private readonly int? _pingerCpu;
	private readonly int? _pongerCpu;

	public PingPongChannelLatencyTest(ProgramOptions options) {
		_pingerCpu = options.GetCustomCpu(0);
		_pongerCpu = options.GetCustomCpu(1);
	}

	public int RequiredProcessorCount => 2;

	public void Run(LatencySessionContext sessionContext) {
		var pingChannel = Channel.CreateBounded<long>(SingleOptions());
		var pongChannel = Channel.CreateBounded<long>(SingleOptions());

		var histogram = sessionContext.Histogram;
		var pauseTicks = StopwatchUtil.GetTimestampFromNanoseconds(PauseNanos);
		var signal = new ManualResetEventSlim(false);
		var stop = new ManualResetEventSlim(false);
		var hasInvalidValue = false;

		var pingerCpu = _pingerCpu;
		var pongerCpu = _pongerCpu;

		// Ponger: read the ping channel, echo into the pong channel.
		var ponger = PerfTestUtil.StartLongRunning(() => {
			if (pongerCpu is { } cpu)
				ThreadAffinityUtil.PinCurrentThread(cpu);

			var reader = pingChannel.Reader;
			var writer = pongChannel.Writer;
			var spin = new SpinWait();
			while (!stop.IsSet) {
				if (!reader.TryRead(out var value)) {
					spin.SpinOnce();
					continue;
				}

				spin.Reset();
				while (!writer.TryWrite(value))
					spin.SpinOnce();
			}
		});

		// Pinger: send a counter, await the echo, record the round-trip, send the next.
		var pinger = PerfTestUtil.StartLongRunning(() => {
			if (pingerCpu is { } cpu)
				ThreadAffinityUtil.PinCurrentThread(cpu);

			var reader = pongChannel.Reader;
			var writer = pingChannel.Writer;
			var spin = new SpinWait();
			long counter = 0, expected = 0, t0 = 0;

			void Send() {
				expected = counter;
				t0 = StopwatchUtil.GetTimestamp();
				while (!writer.TryWrite(counter))
					spin.SpinOnce();
				spin.Reset();
				counter++;
			}

			Send();
			while (true) {
				if (!reader.TryRead(out var value)) {
					spin.SpinOnce();
					continue;
				}

				spin.Reset();
				var t1 = StopwatchUtil.GetTimestamp();
				hasInvalidValue |= value != expected;
				histogram.RecordValueWithExpectedInterval(StopwatchUtil.ToNanoseconds(t1 - t0), PauseNanos);

				if (value < Iterations) {
					while (pauseTicks > StopwatchUtil.GetTimestamp() - t1)
						Thread.Yield();
					Send();
				} else {
					signal.Set();
					return;
				}
			}
		});

		sessionContext.Start();
		signal.Wait();
		sessionContext.Stop();

		stop.Set();
		pinger.GetAwaiter().GetResult();
		ponger.GetAwaiter().GetResult();

		PerfTestUtil.FailIf(hasInvalidValue, "Pinger observed an out-of-order value");
	}

	private static BoundedChannelOptions SingleOptions() => new(BufferSize) {
		FullMode = BoundedChannelFullMode.Wait,
		SingleReader = true,
		SingleWriter = true
	};
}
