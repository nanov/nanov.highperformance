namespace Nanov.HighPerformance.Threading.Actors.Internal;

using System.Threading;

/// <summary>
///   One worker's private state. A separate heap object per worker, so the hot fields of different
///   workers do not share a cache line.
///   <para>
///     <c>Handler</c> is a per-worker COPY of the caller's handler struct. That is what makes handler
///     fields per-worker state rather than a data race: at one worker a single shared handler was
///     survivable, at N it is not. Calls go through the field (<c>worker.Handler.Handle(...)</c>) so
///     they still devirtualize.
///   </para>
/// </summary>
internal sealed class WorkerState<THandler>
	where THandler : struct {
	public readonly int Id;
	public THandler Handler;
	public long ProcessedLocal; // worker-private; published into the padded array
	public int Activations;
	public int RejectedClaims;
	public int ProducerWakes;
	public Thread? Thread;

	public WorkerState(int id, THandler handler) {
		Id = id;
		Handler = handler;
	}
}
