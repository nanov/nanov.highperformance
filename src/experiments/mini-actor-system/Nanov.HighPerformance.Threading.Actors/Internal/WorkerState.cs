namespace Nanov.HighPerformance.Threading.Actors.Internal;

using System.Threading;

/// <summary>
///   One worker's private state. A separate heap object per worker — but the CLR gives no placement
///   guarantee beyond that: the constructor loop allocates all N back-to-back, and at this object's
///   size (a handful of fields) two or more will very likely land on the same 64-byte cache line
///   regardless — this is <b>not</b> false-sharing-proofed by construction. <c>ProcessedLocal</c> in
///   particular is written once per MESSAGE (hotter than the batched-publish <see cref="PaddedLong" />
///   it feeds) and can false-share across workers at N &gt; 1 as a result. Padding this is deferred
///   pending measurement in the benchmark phase, not added speculatively here.
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
