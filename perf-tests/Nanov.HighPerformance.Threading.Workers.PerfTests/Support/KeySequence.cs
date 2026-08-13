namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

using System;

/// <summary>
///   Precomputed key streams for the actor-system throughput cells.
///   <para>
///     <b>Precomputed on purpose.</b> A Zipf sampler costs more per draw than <c>Dispatch</c> does, so
///     generating inside the measured loop would put the sampler in the measurement — and a different
///     amount of it in each cell. Every cell builds its stream before the clock starts and then indexes
///     it modulo its length.
///   </para>
///   <para>
///     <b>Same seed, same stream.</b> B and D must see byte-identical key orders or the comparison means
///     nothing. The seed is fixed per (skew, cardinality), never per scheduler.
///   </para>
/// </summary>
public static class KeySequence {
	/// <summary>Stream length. Cycled by the cells; long enough that the cycle is not itself a pattern.</summary>
	public const int Length = 1 << 20;

	/// <summary>Every key equally likely, in shuffled order — the no-skew control.</summary>
	public static int[] Uniform(int cardinality, int seed) {
		var rng = new Random(seed);
		var keys = new int[Length];
		for (var i = 0; i < Length; i++)
			keys[i] = rng.Next(cardinality);
		return keys;
	}

	/// <summary>
	///   Zipf(s = 1) over <paramref name="cardinality" /> keys — the classic skew: key 0 takes roughly
	///   <c>1 / H(N)</c> of the traffic. Built by inverse-CDF over a precomputed cumulative table, which
	///   is O(N) memory and O(log N) per draw, both off the measured path.
	/// </summary>
	public static int[] Zipf(int cardinality, int seed) {
		var cumulative = new double[cardinality];
		var running = 0.0;
		for (var i = 0; i < cardinality; i++) {
			running += 1.0 / (i + 1);
			cumulative[i] = running;
		}

		for (var i = 0; i < cardinality; i++)
			cumulative[i] /= running;

		var rng = new Random(seed);
		var keys = new int[Length];
		for (var i = 0; i < Length; i++) {
			var u = rng.NextDouble();
			var idx = Array.BinarySearch(cumulative, u);
			if (idx < 0) idx = ~idx;
			if (idx >= cardinality) idx = cardinality - 1;
			keys[i] = idx;
		}

		return keys;
	}

	/// <summary>
	///   90% of traffic to one key, the rest spread uniformly — the pathological case B's per-worker ring
	///   cannot rebalance and D's shared structures can.
	/// </summary>
	public static int[] HotKey(int cardinality, int seed) {
		var rng = new Random(seed);
		var keys = new int[Length];
		for (var i = 0; i < Length; i++)
			keys[i] = rng.NextDouble() < 0.9 ? 0 : rng.Next(1, Math.Max(2, cardinality));
		return keys;
	}
}
