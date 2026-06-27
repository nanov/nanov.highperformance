namespace Nanov.HighPerformance.Threading.Workers;

using System;

/// <summary>
///   Handler for <c>AccumulatingSpWorker&lt;TKey, TValue, THandler&gt;</c> — the additive/combine sibling
///   of <see cref="ICompactingWorkerHandler{TKey, TValue}" />. Where the compacting worker is last-wins
///   over a reference value, this worker <b>accumulates</b> a <see langword="struct" /> value per key
///   with <b>exactly-once</b> delivery: each key has a stable, lockable cell into which the producer
///   merges in place, and the consumer takes-and-resets it. Implement as a <see langword="struct" /> so
///   the calls devirtualize and inline.
/// </summary>
public interface IAccumulatingWorkerHandler<TKey, TValue> where TValue : struct {
	/// <summary>
	///   Merge <paramref name="incoming" /> into the per-key <paramref name="accumulator" /> <b>in place</b>
	///   (e.g. <c>accumulator.Sum += incoming.Value</c>). Runs on the PRODUCER thread while it holds the
	///   cell lock, so it is exclusive with the consumer's take — mutate freely, allocate nothing.
	///   <c>default(TValue)</c> is the empty accumulator (a fresh key starts there; a consumed cell resets
	///   there), so this must be associative with <c>default</c> as identity.
	/// </summary>
	void Accumulate(ref TValue accumulator, TValue incoming);

	/// <summary>
	///   Process one key's accumulated value. Invoked on the worker thread with a by-value copy taken from
	///   the cell (outside the lock), in first-dirtied key order. The accumulation covers everything merged
	///   since the key was last delivered.
	/// </summary>
	void Handle(TKey key, TValue accumulated);

	/// <summary>Invoked synchronously when <see cref="Handle" /> throws. The worker continues with the next item.</summary>
	void OnError(Exception error);

	/// <summary>
	///   Invoked once after the worker has drained and is about to exit. <paramref name="error" /> is
	///   <see langword="null" /> for normal completion, an <see cref="OperationCanceledException" /> for
	///   cancellation, or the exception passed to <c>TryComplete</c>.
	/// </summary>
	void OnCompleted(Exception? error);
}
