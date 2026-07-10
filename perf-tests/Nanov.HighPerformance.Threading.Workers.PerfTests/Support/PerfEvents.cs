namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>Reference-type payload (one reused instance per producer) for the copy-worker tests.</summary>
public sealed class PerfEvent {
	public long Value;
}

/// <summary>Value-type payload for the zero-copy value-worker tests.</summary>
public struct PerfValueEvent {
	public long Value;
}

/// <summary>
///   Reference-type long carrier for the conflation worker, whose <c>TValue</c> must be a class
///   (last-wins publishes a single atomic reference write). One instance is reused per key, so the
///   throughput test stays allocation-free.
/// </summary>
public sealed class LongRef {
	public long Value;
}

/// <summary>
///   Shared, reference-type consumer state so the by-value struct handlers' accumulation is observable
///   from the test thread after completion. One per run. <see cref="Pinned" /> gates the consumer's
///   one-time self-pin; <see cref="LastByKey" /> is used only by the conflation test (last-wins, where
///   a running sum is not verifiable).
/// </summary>
public sealed class Accumulator {
	public long Sum;
	public long Count;
	public bool Pinned;
	public readonly long[] LastByKey;

	public Accumulator(int keyCount = 0) =>
		LastByKey = keyCount > 0 ? new long[keyCount] : System.Array.Empty<long>();
}
