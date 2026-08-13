namespace Nanov.HighPerformance.Threading.Actors;

using System;

/// <summary>
///   Per-message logic plus lifecycle hooks. Implement as a <see langword="struct" /> so the calls
///   devirtualize and inline — never store one as an interface reference.
///   <para>
///     There is currently <b>one</b> <c>THandler</c> field, shared between the producer and worker
///     threads: the producer calls <see cref="OnSlotReused" />/<see cref="OnChunkReclaimed" /> on it
///     and the worker calls <see cref="Handle" />/<see cref="OnCompleted" />, so handler state must be
///     thread-safe. Per-worker copies (so each worker gets its own, thread-local instance) arrive with
///     the multi-worker phase.
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
	///   The worker has drained and is exiting. Fires <b>once per worker</b> (so N times for N workers),
	///   on that worker's own handler copy. <c>null</c> = normal completion.
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
