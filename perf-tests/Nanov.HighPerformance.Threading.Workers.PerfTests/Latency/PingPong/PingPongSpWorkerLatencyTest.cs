namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Latency.PingPong;

using System;
using System.Threading;
using HdrHistogram;
using Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>
///   Round-trip latency between two <see cref="SpWorker{T, THandler}" />s. The pinger sends a counter
///   into the ping worker; the ponger (its consumer) echoes it into the pong worker; the pinger (the pong
///   worker's consumer) records the round-trip and sends the next. The SPSC analog of Disruptor's
///   PingPongSequencedLatencyTest.
///
///   Each worker stays strict SPSC: the ping worker is produced only by the pinger thread (after the test
///   thread's single kick, which happens-before it) and consumed by the ponger; the pong worker is
///   produced only by the ponger thread and consumed by the pinger.
/// </summary>
public sealed class PingPongSpWorkerLatencyTest : ILatencyTest {
	private const int BufferSize = 1024;
	private const long Iterations = 100L * 1000L * 30L;
	private const long PauseNanos = 1000L;

	private readonly int? _pingerCpu;
	private readonly int? _pongerCpu;

	public PingPongSpWorkerLatencyTest(ProgramOptions options) {
		_pingerCpu = options.GetCustomCpu(0);
		_pongerCpu = options.GetCustomCpu(1);
	}

	public int RequiredProcessorCount => 2;

	public void Run(LatencySessionContext sessionContext) {
		var loop = new Loop {
			Histogram = sessionContext.Histogram,
			PauseTicks = StopwatchUtil.GetTimestampFromNanoseconds(PauseNanos),
			PingerCpu = _pingerCpu,
			PongerCpu = _pongerCpu
		};

		using var pingWorker = new SpWorker<long, Ponger>(new Ponger(loop), BufferSize);
		using var pongWorker = new SpWorker<long, Pinger>(new Pinger(loop), BufferSize);
		loop.PingWorker = pingWorker;
		loop.PongWorker = pongWorker;

		pingWorker.Start();
		pongWorker.Start();

		sessionContext.Start();
		loop.Send(); // kick off the loop (test thread); the pinger thread takes over from here
		loop.Signal.Wait();
		sessionContext.Stop();

		PerfTestUtil.FailIf(loop.HasInvalidValue, "Pinger observed an out-of-order value");
	}

	// Shared loop state. The two handler structs hold a reference to this; all mutable state lives here so
	// it is visible across the pinger / ponger threads (each transition is fenced by a worker dispatch).
	private sealed class Loop {
		public SpWorker<long, Ponger>? PingWorker;
		public SpWorker<long, Pinger>? PongWorker;
		public LongHistogram Histogram = null!;
		public readonly ManualResetEventSlim Signal = new(false);
		public long T0;
		public long Counter;
		public long ExpectedValue;
		public long PauseTicks;
		public bool HasInvalidValue;
		public int? PingerCpu;
		public int? PongerCpu;
		public bool PingerPinned;
		public bool PongerPinned;

		public void Send() {
			ExpectedValue = Counter;
			T0 = StopwatchUtil.GetTimestamp();
			PingWorker!.Dispatch(Counter);
			Counter++;
		}
	}

	// Consumer of the ping worker: echoes each value into the pong worker.
	private readonly struct Ponger : IWorkerHandler<long> {
		private readonly Loop _loop;
		public Ponger(Loop loop) => _loop = loop;

		public void Handle(long value) {
			if (!_loop.PongerPinned) {
				if (_loop.PongerCpu is { } cpu)
					ThreadAffinityUtil.PinCurrentThread(cpu);
				_loop.PongerPinned = true;
			}

			_loop.PongWorker!.Dispatch(value);
		}

		public void OnError(Exception error) => System.Console.Error.WriteLine(error);
		public void OnCompleted(Exception? error) { }
	}

	// Consumer of the pong worker: records the round-trip and sends the next ping (or signals done).
	private readonly struct Pinger : IWorkerHandler<long> {
		private readonly Loop _loop;
		public Pinger(Loop loop) => _loop = loop;

		public void Handle(long value) {
			if (!_loop.PingerPinned) {
				if (_loop.PingerCpu is { } cpu)
					ThreadAffinityUtil.PinCurrentThread(cpu);
				_loop.PingerPinned = true;
			}

			var t1 = StopwatchUtil.GetTimestamp();
			_loop.HasInvalidValue |= value != _loop.ExpectedValue;
			_loop.Histogram.RecordValueWithExpectedInterval(StopwatchUtil.ToNanoseconds(t1 - _loop.T0), PauseNanos);

			if (value < Iterations) {
				while (_loop.PauseTicks > StopwatchUtil.GetTimestamp() - t1)
					Thread.Yield();
				_loop.Send();
			} else {
				_loop.Signal.Set();
			}
		}

		public void OnError(Exception error) => System.Console.Error.WriteLine(error);
		public void OnCompleted(Exception? error) { }
	}
}
