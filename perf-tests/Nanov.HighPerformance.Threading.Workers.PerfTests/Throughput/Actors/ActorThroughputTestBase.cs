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

	/// <summary>
	///   Measured, not guessed — see <c>ProducerBoundCalibrationTest</c>. Do NOT change without re-running it.
	///   <para>
	///     <b>Measured 2026-08-14</b>, Apple M4 Pro (12 logical cores), 8 workers, cardinality 1024, two
	///     invocations of 7 repeats each. Individual repeats are NOT interpretable on this host — see the
	///     spread column — so the figures below are per-rung medians across each invocation's 7 repeats:
	///   </para>
	///   <code>
	///     spin |  pass 1  |  pass 2  | pass 2 spread
	///     -----+----------+----------+--------------
	///        0 |  2.06M/s |  1.66M/s |    1.48x
	///        4 |  3.93M/s |  1.83M/s |    5.53x
	///       16 |  5.69M/s |  2.68M/s |    3.13x   &lt;- pass 1 peak
	///       32 |  4.39M/s |  5.09M/s |    1.20x   &lt;- pass 2 peak; CHOSEN
	///       64 |  2.37M/s |  2.82M/s |    1.12x
	///   </code>
	///   <para>
	///     <b>Why 32, and what the choice does and does not rest on.</b> The knee moved between passes
	///     (16 in pass 1, 32 in pass 2), so it is not pinned to a rung. What both passes agree on is that
	///     spin 64 sits below spin 32 — the falling side begins at or after 32 — and that the run-to-run
	///     spread collapses to ~1.2x at 32 and 64 against 3-6x below. That collapse is the property the
	///     matrix actually needs: it is the point where the per-message cost dominates and the number
	///     reproduces. 32 is the first rung with it.
	///   </para>
	///   <para>
	///     <b>The rise from spin 0 is not an artefact.</b> At low spin the single producer races ahead,
	///     workers park and unpark constantly, and wake churn dominates; more handler cost keeps workers
	///     busy longer and RAISES throughput, until the spin itself dominates and it falls. That is also
	///     why the low rungs are the noisy ones — they are producer-bound and thrashing.
	///   </para>
	///   <para>
	///     <b>Host caveat that no calibration can remove.</b> <c>ThreadAffinityUtil</c> is a documented
	///     no-op on macOS — there is no per-core pinning API — so this suite cannot isolate itself from
	///     placement noise even on an idle machine. Treat differences smaller than the spread as absent.
	///   </para>
	/// </summary>
	protected virtual int SpinPerMessage => 32;

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
	/// <param name="waitPolicy">
	///   The idle-worker wait ladder to drive the system with. <see langword="null" /> (the default)
	///   reproduces the Phase 3 cells' behaviour exactly — <see cref="ActorSystem{TKey,T,THandler,TScheduler}" />
	///   falls back to its own legacy default — so existing call sites need no change.
	/// </param>
	protected static long Drive<TScheduler>(TScheduler scheduler, int cardinality, int[] keys,
		ActorPerfState state, int? producerCpu, IWaitPolicy? waitPolicy = null)
		where TScheduler : struct, IKeyScheduler {
		using var system = new ActorSystem<int, long, ActorPerfHandler, TScheduler>(
			new ActorPerfHandler(state), keyCapacity: cardinality * 2, chunkCount: cardinality,
			chunkSize: 256, batchBound: 16, workers: Workers, scheduler: scheduler, waitPolicy: waitPolicy);
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
