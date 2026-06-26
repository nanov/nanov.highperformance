namespace Nanov.HighPerformance.Threading;

/// <summary>
///   Non-generic, read-only view of a worker's ring buffer, surfaced for telemetry. Lets a gauge
///   sample queue depth and capacity without knowing the worker's generic item type.
/// </summary>
public interface IWorkerQueue {
	/// <summary>Approximate items currently queued (<c>Tail − Head</c>). Lock-free, racy by a few.</summary>
	int QueuedApprox { get; }

	/// <summary>Ring capacity (power of two).</summary>
	int Capacity { get; }
}
