namespace Nanov.HighPerformance.Threading.Workers.Internal;

/// <summary>
///   Struct-dispatched keep/drop predicate over a slot sequence, used by
///   <see cref="CompactingKeyMap{TKey}.Compact{TPred}" /> so the compaction loop devirtualizes and
///   inlines the test (no delegate alloc, no virtual call). Implement as a <see langword="struct" />.
/// </summary>
internal interface ISeqKeepPredicate {
	/// <summary>True to keep the entry with this <paramref name="seq" />, false to evict it.</summary>
	bool Keep(int seq);
}
