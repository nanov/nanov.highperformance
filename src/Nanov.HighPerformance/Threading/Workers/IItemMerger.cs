namespace Nanov.HighPerformance.Threading.Workers;

/// <summary>
///   Narrow, producer-side conflation facet for <c>CompactingSpWorker&lt;TKey, TValue, THandler&gt;</c>.
///   Separated from the handler so the (consumer-model-agnostic) core depends only on this — it never
///   references <c>Handle</c>, which keeps the core reusable by a future async compacting worker.
///   Implement as part of a <see langword="struct" /> handler so the calls devirtualize and inline.
///   Both members run on the PRODUCER thread (concurrent with the worker thread's <c>Handle</c>).
/// </summary>
public interface IItemMerger<TValue> {
	/// <summary>
	///   Conflate an <paramref name="incoming" /> value into the one already queued for the same key,
	///   returning the value to publish. "Keep latest / react on replacement" = <c>return incoming;</c>;
	///   field-combine (a fresh object) and keep-existing (<c>return current;</c>) also work.
	///   <para>Guarantees and contract:</para>
	///   <list type="bullet">
	///     <item>
	///       <b>When.</b> Called from <c>Dispatch</c> only when the key is still <i>pending</i> (its slot
	///       is queued but not yet consumed). At most <b>once per Dispatch</b>; <b>not</b> called on a
	///       key's first dispatch, nor when its prior slot was already consumed or evicted (those enqueue
	///       fresh). For a hot key re-dispatched while queued the merges <b>chain</b> —
	///       <paramref name="current" /> is the accumulated result of prior merges.
	///     </item>
	///     <item>
	///       <b>Thread.</b> Runs on the PRODUCER thread (concurrent with <c>Handle</c>). It <b>must be
	///       pure</b>: read only its two arguments, return a value, touch no mutable handler state.
	///     </item>
	///     <item>
	///       <b>Both arguments are read-only.</b> Return <paramref name="incoming" /> unchanged (last-wins),
	///       <paramref name="current" /> (keep-existing), or build a <b>fresh</b> object (combine) — do not
	///       mutate either. <paramref name="current" /> is the live queued value (never null), possibly read
	///       by the consumer or delivered as-is, and only its reference write is atomic (mutating fields
	///       risks a torn read). Mutating <paramref name="incoming" /> is also unsafe: it can <b>alias</b>
	///       <paramref name="current" /> when the caller reuses a reference, and it is itself published as-is
	///       on the rare doubt path.
	///     </item>
	///     <item>
	///       <b>Replace-style / idempotent only.</b> Each returned value must be self-contained: under a
	///       rare race the worker may deliver the previous value and then <paramref name="incoming" />
	///       separately, so additive/accumulate merges would double-count.
	///     </item>
	///   </list>
	/// </summary>
	TValue Merge(TValue current, TValue incoming);

	/// <summary>
	///   Release a value the ring is dropping so pooled / rented resources don't leak (e.g.
	///   <c>ArrayPool&lt;byte&gt;.Shared.Return(...)</c>, dispose, return to an object pool). Runs on the
	///   PRODUCER thread.
	///   <para>
	///     Called once for each input to <see cref="Merge" /> that <see cref="Merge" /> did <b>not</b>
	///     return — i.e. the value(s) the merge superseded. Concretely: last-wins (<c>return incoming</c>)
	///     ⇒ <c>Discarded(current)</c>; keep-existing (<c>return current</c>) ⇒ <c>Discarded(incoming)</c>;
	///     combine (a fresh object) ⇒ <c>Discarded(current)</c> and <c>Discarded(incoming)</c>. A returned
	///     value is never discarded (it's live in the slot until it is itself superseded or delivered).
	///   </para>
	///   <para>
	///     Accounting: in the common (uncontended) case every dispatched value is eventually passed to
	///     <b>exactly one</b> of <c>Handle</c> (it survived to the consumer) or <see cref="Discarded" />
	///     (a merge dropped it) — so pooled payloads round-trip cleanly. Two caveats: it is
	///     <b>at-most-once, not exactly-once</b> — under a rare at-head race a superseded value may be
	///     delivered instead of discarded (never both), so make cleanup tolerant (pool-return / dispose,
	///     not a hard exactly-once decrement); and a <b>hard stop</b> (cancel / <c>Dispose</c>) drops
	///     still-queued values without discarding them — drain via <c>TryComplete</c> (which delivers
	///     everything to <c>Handle</c>) for full cleanup. The argument may alias a value also seen by
	///     <c>Handle</c> only across these caveats; within one clean conflation it does not.
	///   </para>
	/// </summary>
	void Discarded(TValue value);
}
