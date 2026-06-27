namespace Nanov.HighPerformance.Threading.Workers;

using System;

/// <summary>
///   Handler for <c>CoalescingSpWorker&lt;TKey, TValue, THandler&gt;</c> — the coalescing/combine sibling
///   of <see cref="IConflationWorkerHandler{TKey, TValue}" />. Where the conflation worker is last-wins,
///   this worker <b>coalesces</b> a value per key with <b>exactly-once</b> delivery: each key has a stable,
///   lockable cell into which the producer merges, and the consumer takes-and-resets it.
///   <typeparamref name="TValue" /> may be a value type (zero-allocation, the inline-cell case) or a
///   reference type (the handler may allocate). Implement as a <see langword="struct" /> so the calls
///   devirtualize and inline.
/// </summary>
public interface ICoalescingWorkerHandler<TKey, TValue> {
	/// <summary>
	///   Combine the per-key <paramref name="current" /> with <paramref name="incoming" /> and return the
	///   coalesced value (e.g. <c>new Tally { Sum = current.Sum + incoming.Sum }</c>). May fold and return
	///   <paramref name="current" /> or produce an entirely new value — up to the implementation. Runs on the
	///   PRODUCER thread while it holds the cell lock, so it is exclusive with the consumer's take.
	///   <c>default(TValue)</c> is the empty value — the identity (a fresh key starts there; a consumed cell
	///   resets there) — so this must be associative with <c>default</c> as identity. For a reference
	///   <typeparamref name="TValue" />, <c>default</c> is <see langword="null" />.
	/// </summary>
	TValue Coalesce(ref TValue current, ref TValue incoming);

	/// <summary>
	///   Process one key's coalesced value. Invoked on the worker thread with a by-value copy taken from the
	///   cell (outside the lock), in first-dirtied key order. The value covers everything merged for that key
	///   since it was last delivered.
	/// </summary>
	void Handle(TKey key, TValue value);

	/// <summary>Invoked synchronously when <see cref="Handle" /> throws. The worker continues with the next item.</summary>
	void OnError(Exception error);

	/// <summary>
	///   Invoked once after the worker has drained and is about to exit. <paramref name="error" /> is
	///   <see langword="null" /> for normal completion, an <see cref="OperationCanceledException" /> for
	///   cancellation, or the exception passed to <c>TryComplete</c>.
	/// </summary>
	void OnCompleted(Exception? error);
}
