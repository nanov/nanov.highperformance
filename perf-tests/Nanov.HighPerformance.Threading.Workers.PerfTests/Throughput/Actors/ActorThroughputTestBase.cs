namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Throughput.Actors;

using System;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>Which key distribution a cell drives.</summary>
public enum Skew {
	Uniform,
	Zipf,

	/// <summary>Written but not driven by this cut's throughput matrix — reserved for the latency cut.</summary>
	HotKey,
}

/// <summary>
///   One cell of the B-vs-D matrix: a scheduler, a skew and a key cardinality, driven by a precomputed
///   key stream at a fixed worker count and a fixed per-message cost.
///   <para>
///     <b>Everything except the scheduler is held constant across a comparison pair.</b> Same stream,
///     same seed, same cardinality, same worker count, same spin. If two cells differ in anything else,
///     the difference between their numbers is not attributable to the scheduler.
///   </para>
/// </summary>
public abstract class ActorThroughputTestBase : IThroughputTest {
	/// <summary>Fixed for this cut. A worker sweep is a later cut, not this one.</summary>
	protected const int Workers = 8;

	// 10M at the calibrated per-message cost is ~0.5s of work spread over 8 workers — enough to swamp
	// startup and JIT, short enough that the whole matrix runs in about two minutes.
	protected const long Iterations = 10L * 1000L * 1000L;

	/// <summary>Set by the calibration test in Task 3. Do NOT change without re-running it.</summary>
	protected const int SpinPerMessage = 0; // ← Task 3 replaces this with the measured value

	private readonly int? _producerCpu;

	protected ActorThroughputTestBase(ProgramOptions options) => _producerCpu = options.GetCustomCpu(0);

	protected abstract int Cardinality { get; }

	protected abstract Skew Skew { get; }

	/// <summary>Runs the stream through a system built on this cell's scheduler; returns messages handled.</summary>
	protected abstract long RunStream(int[] keys, ActorPerfState state, int? producerCpu);

	public int RequiredProcessorCount => Workers + 1;

	public long Run(ThroughputSessionContext sessionContext) {
		// Seeded by (skew, cardinality) only — never by scheduler, so a B cell and a D cell at the same
		// coordinates drive byte-identical key orders.
		var seed = (int)Skew * 7919 + Cardinality;
		var keys = Skew switch {
			Skew.Uniform => KeySequence.Uniform(Cardinality, seed),
			Skew.Zipf => KeySequence.Zipf(Cardinality, seed),
			Skew.HotKey => KeySequence.HotKey(Cardinality, seed),
			_ => throw new ArgumentOutOfRangeException(nameof(Skew)),
		};

		var state = new ActorPerfState { SpinPerMessage = SpinPerMessage };

		sessionContext.Start();
		var handled = RunStream(keys, state, _producerCpu);
		sessionContext.Stop();

		PerfTestUtil.FailIfNot(Iterations, handled,
			$"handler should have seen {Iterations} messages, saw {handled}");
		return Iterations;
	}

	/// <summary>
	///   Shared drive loop. Sizing: <c>keyCapacity</c> at 2x cardinality keeps the map's load factor low
	///   so eviction does not run; <c>chunkCount</c> at cardinality means every live key can hold a chunk
	///   so harvest does not run. Both cold paths are producer-side and scheduler-independent, so leaving
	///   them out of the measurement removes noise that would land on both cells equally but unevenly.
	/// </summary>
	protected static long Drive<TScheduler>(TScheduler scheduler, int cardinality, int[] keys,
		ActorPerfState state, int? producerCpu)
		where TScheduler : struct, IKeyScheduler {
		using var system = new ActorSystem<int, long, ActorPerfHandler, TScheduler>(
			new ActorPerfHandler(state), keyCapacity: cardinality * 2, chunkCount: cardinality,
			chunkSize: 256, batchBound: 16, workers: Workers, scheduler: scheduler);
		system.Start();

		using (ThreadAffinityUtil.SetThreadAffinity(producerCpu)) {
			var mask = keys.Length - 1;
			for (var i = 0L; i < Iterations; i++)
				system.Dispatch(keys[(int)(i & mask)], i);

			system.TryComplete();
			system.Completion.GetAwaiter().GetResult();
		}

		return state.HandledCount;
	}
}
