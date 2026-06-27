namespace Nanov.HighPerformance.Threading.Workers;

/// <summary>
///   Narrow, producer-side conflation facet for <c>CompactingSpWorker&lt;TKey, TValue, THandler&gt;</c>.
///   Separated from the handler so the (consumer-model-agnostic) core depends only on this — it never
///   references <c>Handle</c>, which keeps the core reusable by a future async compacting worker.
///   Implement as part of a <see langword="struct" /> handler so the call devirtualizes and inlines.
/// </summary>
public interface IItemMerger<TValue> {
	/// <summary>
	///   Conflate an <paramref name="incoming" /> value into the one already queued for the same key,
	///   returning the value to publish. Runs on the PRODUCER thread (not the worker thread), so it
	///   <b>must be pure</b>: read only its two arguments, return a value, touch no mutable handler
	///   state. Must be <b>replace-style / idempotent</b> — each returned value self-contained — because
	///   the lossless override protocol may, under a race, deliver the previous value and then
	///   <paramref name="incoming" /> as two separate items. (Additive/accumulate merges are unsafe here.)
	///   "Keep latest / react on replacement" = <c>return incoming;</c>.
	/// </summary>
	TValue Merge(TValue current, TValue incoming);
}
