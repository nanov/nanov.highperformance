namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Throughput.Actors;

using System;
using System.Diagnostics;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>
///   Not a comparison — a calibration. Finds the per-message cost at which this machine stops being
///   producer-bound.
///   <para>
///     The actor system is single-producer. With a cheap handler, eight workers starve on one dispatch
///     thread, every scheduler saturates at the same rate, and a B-vs-D matrix run below this point
///     returns "no difference" for a reason that has nothing to do with scheduling. Throughput here
///     RISES with spin while the producer is the bottleneck and FALLS once the consumers are — the knee
///     is the answer, and the first value on the falling side is what the cells should use.
///   </para>
///   <para>
///     Runs on D only: the question is about the producer, and the scheduler is not part of it.
///   </para>
/// </summary>
public sealed class ProducerBoundCalibrationTest : IThroughputTest {
	private const int Cardinality = 1024;

	// 2M per rung, and the ladder stops at 64. The top rung already costs ~2 us/message, which is far
	// past any plausible knee — going higher only buys a longer wait for a number nobody will use.
	private const long Iterations = 2L * 1000L * 1000L;
	private static readonly int[] Ladder = [0, 1, 2, 4, 8, 16, 32, 64];

	private readonly int? _producerCpu;

	public ProducerBoundCalibrationTest(ProgramOptions options) => _producerCpu = options.GetCustomCpu(0);

	public int RequiredProcessorCount => 9;

	public long Run(ThroughputSessionContext sessionContext) {
		var keys = KeySequence.Uniform(Cardinality, seed: 12345);

		Console.WriteLine();
		Console.WriteLine("  spin | msgs/sec    | ns/msg");
		Console.WriteLine("  -----+-------------+-------");

		sessionContext.Start();
		foreach (var spin in Ladder) {
			var state = new ActorPerfState { SpinPerMessage = spin };
			var sw = Stopwatch.StartNew();
			var handled = Drive(keys, state, spin);
			sw.Stop();

			if (handled != Iterations)
				throw new InvalidOperationException($"spin {spin}: handled {handled} of {Iterations}");

			var perSecond = Iterations / sw.Elapsed.TotalSeconds;
			Console.WriteLine($"  {spin,4} | {perSecond,11:N0} | {1e9 / perSecond,6:F1}");
		}

		sessionContext.Stop();

		Console.WriteLine();
		Console.WriteLine("  Read the knee: throughput rises while the PRODUCER is the bottleneck and falls");
		Console.WriteLine("  once the CONSUMERS are. Take the first spin value on the falling side.");
		return Iterations * Ladder.Length;
	}

	private long Drive(int[] keys, ActorPerfState state, int spin) {
		using var system = new Nanov.HighPerformance.Threading.Actors.ActorSystem<int, long, ActorPerfHandler,
			WorkStealingScheduler>(
			new ActorPerfHandler(state), keyCapacity: Cardinality * 2, chunkCount: Cardinality,
			chunkSize: 256, batchBound: 16, workers: 8,
			scheduler: new WorkStealingScheduler { Capacity = 4096, DequeCapacity = 256 });
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
