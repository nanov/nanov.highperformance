namespace Nanov.HighPerformance.Threading;

/// <summary>
///   Non-generic, read-only view of an SPSC worker's ring buffer, surfaced for telemetry. Lets a
///   gauge sample queue depth and capacity without knowing the worker's generic item type. Every
///   worker in the family implements it (via <see cref="SpscWorkerBase{T}" /> /
///   <see cref="ValueSpscWorkerBase{T}" />).
/// </summary>
public interface ISpscWorkerQueue {
	/// <summary>Approximate items currently queued (<c>Tail − Head</c>). Lock-free, racy by a few.</summary>
	int QueuedApprox { get; }

	/// <summary>Ring capacity (power of two).</summary>
	int Capacity { get; }
}
