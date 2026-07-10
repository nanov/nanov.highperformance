namespace Nanov.HighPerformance.Threading.Workers.PerfTests;

using System;
using System.Globalization;
using System.Text;

/// <summary>One run's throughput result (or the exception that failed it). Ported from Disruptor-net.</summary>
internal sealed class ThroughputTestSessionResult {
	private readonly Exception? _exception;

	public long TotalOperationsInRun { get; }
	public double? BatchPercent { get; }
	public double? AverageBatchSize { get; }
	public TimeSpan Duration { get; }
	public int Gen0 { get; }
	public int Gen1 { get; }
	public int Gen2 { get; }

	public ThroughputTestSessionResult(long totalOperationsInRun, TimeSpan duration, int gen0, int gen1, int gen2,
		ThroughputSessionContext sessionContext) {
		TotalOperationsInRun = totalOperationsInRun;
		BatchPercent = sessionContext.BatchPercent;
		AverageBatchSize = sessionContext.AverageBatchSize;
		Duration = duration;
		Gen0 = gen0;
		Gen1 = gen1;
		Gen2 = gen2;
	}

	public ThroughputTestSessionResult(Exception exception) => _exception = exception;

	public double OpsPerSecond => TotalOperationsInRun / Duration.TotalSeconds;

	public void AppendDetailedHtmlReport(int runId, StringBuilder sb) {
		if (_exception != null) {
			sb.AppendLine(" <tr>");
			sb.AppendLine($"     <td>{runId}</td>");
			sb.AppendLine("     <td>FAILED</td>");
			sb.AppendLine($"     <td>{_exception.Message}</td>");
			sb.AppendLine("     <td></td><td></td><td></td>");
			sb.AppendLine(" </tr>");
			return;
		}

		sb.AppendLine(" <tr>");
		sb.AppendLine($"     <td>{runId}</td>");
		sb.AppendLine($"     <td>{OpsPerSecond.ToString("### ### ### ###", CultureInfo.InvariantCulture)}</td>");
		sb.AppendLine($"     <td>{Duration.TotalMilliseconds.ToString("N0", CultureInfo.InvariantCulture)} (ms)</td>");
		sb.AppendLine($"     <td>{Gen0} - {Gen1} - {Gen2}</td>");
		sb.AppendLine($"     <td>{BatchPercent:P}</td>");
		sb.AppendLine($"     <td>{AverageBatchSize:N0}</td>");
		sb.AppendLine(" </tr>");
	}

	public override string ToString() =>
		_exception != null
			? $"Run: FAILED: {_exception.Message}"
			: FormattableString.Invariant(
				$"Run: {OpsPerSecond / 1_000_000:N1} Mops ({OpsPerSecond:### ### ### ###} ops/s) - Duration: {Duration.TotalMilliseconds:N0} ms - GC: {Gen0}-{Gen1}-{Gen2} - Batch %: {BatchPercent:P} - Avg Batch: {AverageBatchSize:N1}");
}
