namespace Nanov.HighPerformance.Threading.Workers;

/// <summary>
///   Non-generic, read-only view of an SPSC worker's ring buffer, surfaced for telemetry. Lets a
///   gauge sample queue depth and capacity without knowing the worker's generic item type. Every
///   worker in the family (<see cref="SpWorker{T, THandler}" />, <see cref="AsyncSpWorker{T, THandler}" />,
///   <see cref="ValueSpWorker{T, THandler}" />, <see cref="AsyncValueSpWorker{T, THandler}" />)
///   implements it.
/// </summary>
public interface IWorkerQueue {
	/// <summary>Approximate items currently queued (<c>Tail − Head</c>). Lock-free, racy by a few.</summary>
	int QueuedApprox { get; }

	/// <summary>Ring capacity (power of two).</summary>
	int Capacity { get; }
}
