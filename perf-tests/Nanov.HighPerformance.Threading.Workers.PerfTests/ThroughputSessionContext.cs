namespace Nanov.HighPerformance.Threading.Workers.PerfTests;

using System;
using System.Diagnostics;

/// <summary>
///   Per-run state for a throughput test: the elapsed-time stopwatch plus optional batch statistics
///   (how many consumer wake-ups served how many items). Ported from Disruptor-net.
/// </summary>
public sealed class ThroughputSessionContext {
	private readonly Stopwatch _stopwatch = new();

	public double? BatchPercent { get; private set; }
	public double? AverageBatchSize { get; private set; }

	public TimeSpan ElapsedTime => _stopwatch.Elapsed;

	public void Reset() {
		_stopwatch.Reset();
		BatchPercent = null;
		AverageBatchSize = null;
	}

	public void Start() => _stopwatch.Start();
	public void Stop() => _stopwatch.Stop();

	public void SetBatchData(long batchesProcessedCount, long iterations) {
		if (batchesProcessedCount <= 0) return;
		AverageBatchSize = (double)iterations / batchesProcessedCount;
		BatchPercent = 1 - (double)batchesProcessedCount / iterations;
	}
}
