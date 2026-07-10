namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Latency.OneWay;

using System;
using System.Threading;
using HdrHistogram;
using Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>
///   One-way latency through <see cref="SpWorker{T, THandler}" />: the producer paces a timestamp every
///   ~10µs into the worker; the consumer records (now − sent) into the histogram. A sentinel (-1) marks
///   the end. The SPSC analog of Disruptor's OneWaySequencedLatencyTest.
/// </summary>
public sealed class OneWaySpWorkerLatencyTest : ILatencyTest {
	private const int BufferSize = 1024;
	private const long Iterations = 100L * 1000L * 30L;

	private readonly int? _producerCpu;
	private readonly int? _consumerCpu;

	public OneWaySpWorkerLatencyTest(ProgramOptions options) {
		_producerCpu = options.GetCustomCpu(0);
		_consumerCpu = options.GetCustomCpu(1);
	}

	public int RequiredProcessorCount => 2;

	public void Run(LatencySessionContext sessionContext) {
		var completed = new ManualResetEventSlim(false);
		using var worker = new SpWorker<long, Handler>(
			new Handler(sessionContext.Histogram, completed, _consumerCpu), BufferSize);
		worker.Start();

		using (ThreadAffinityUtil.SetThreadAffinity(_producerCpu)) {
			var pause = StopwatchUtil.GetTimestampFromMicroseconds(10);
			var next = StopwatchUtil.GetTimestamp() + pause;

			sessionContext.Start();

			for (var i = 0L; i < Iterations; i++) {
				var now = StopwatchUtil.GetTimestamp();
				while (now < next) {
					Thread.Yield();
					now = StopwatchUtil.GetTimestamp();
				}

				worker.Dispatch(now);
				next = now + pause;
			}

			worker.Dispatch(-1); // sentinel
			completed.Wait();

			sessionContext.Stop();
		}

		worker.TryComplete();
		worker.Completion.GetAwaiter().GetResult();
	}

	private struct Handler : IWorkerHandler<long> {
		private readonly LongHistogram _histogram;
		private readonly ManualResetEventSlim _completed;
		private readonly int? _cpu;
		private bool _pinned;

		public Handler(LongHistogram histogram, ManualResetEventSlim completed, int? cpu) {
			_histogram = histogram;
			_completed = completed;
			_cpu = cpu;
			_pinned = false;
		}

		public void Handle(long sentTimestamp) {
			if (!_pinned) {
				if (_cpu is { } cpu)
					ThreadAffinityUtil.PinCurrentThread(cpu);
				_pinned = true;
			}

			if (sentTimestamp == -1) {
				_completed.Set();
				return;
			}

			var duration = StopwatchUtil.GetTimestamp() - sentTimestamp;
			_histogram.RecordValue(StopwatchUtil.ToNanoseconds(duration));
		}

		public void OnError(Exception error) => System.Console.Error.WriteLine(error);
		public void OnCompleted(Exception? error) { }
	}
}
