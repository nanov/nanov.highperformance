namespace Nanov.HighPerformance.Threading.Actors;

using System;

/// <summary>
///   Per-message logic plus lifecycle hooks. Implement as a <see langword="struct" /> so the calls
///   devirtualize and inline — never store one as an interface reference.
///   <para>
///     There are <c>workers + 1</c> independent COPIES of the <c>THandler</c> struct: one per worker,
///     plus one for the producer. <see cref="OnSlotReused" /> and <see cref="OnChunkReclaimed" /> run
///     on the producer's copy; <see cref="Handle" /> and <see cref="OnCompleted" /> run on a worker's
///     own copy. A copy's value-type fields are therefore private to the thread that owns it — only
///     state reached through a shared REFERENCE (e.g. a field holding a class instance or a
///     <c>ConcurrentQueue</c>) is actually visible across copies, and that shared state must still be
///     thread-safe.
///   </para>
/// </summary>
public interface IActorHandler<in TKey, T> {
	/// <summary>
	///   Handle one message. Runs on a worker thread with <b>exclusive ownership</b> of the key, and
	///   messages of one key arrive strictly in dispatch order.
	/// </summary>
	void Handle(TKey key, T message);

	/// <summary>
	///   An error was reported through the safe-error path. Always synchronous. Most commonly
	///   <see cref="Handle" /> threw — the message is then <b>consumed</b>, not retried, and the worker
	///   continues with the next message of the same key, so a poison message cannot stall the key and
	///   per-key ordering is unaffected. But this also fires on the <b>producer</b> thread when
	///   <see cref="OnSlotReused" /> or <see cref="OnChunkReclaimed" /> throws.
	/// </summary>
	void OnError(Exception error);

	/// <summary>
	///   A worker has drained and is exiting, called on that worker's own handler copy. Fires
	///   <b>once per worker</b> — at <c>workers</c> workers, <c>workers</c> total invocations, one per
	///   copy, never on the producer's copy. <c>null</c> = normal completion.
	/// </summary>
	void OnCompleted(Exception? error);

	/// <summary>
	///   A mailbox slot is about to be overwritten, so <paramref name="slot" /> holds a message that has
	///   already been fully processed. Runs on the <b>producer</b> thread. Hook for returning the old
	///   message to a pool; leave empty to keep the reference until the next overwrite (§4.6).
	/// </summary>
	void OnSlotReused(ref T slot);

	/// <summary>
	///   A key drained and its chunk is being recycled; every slot holds an already-processed message.
	///   Runs on the <b>producer</b> thread, cold — once per key lifetime. Hook for draining the tail
	///   into a pool (§4.6).
	/// </summary>
	void OnChunkReclaimed(Span<T> chunk);
}
