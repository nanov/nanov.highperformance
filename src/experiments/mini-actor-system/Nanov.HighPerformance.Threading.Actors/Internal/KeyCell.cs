namespace Nanov.HighPerformance.Threading.Actors.Internal;

/// <summary>
///   A key's durable record. Lives inline in <see cref="KeyCellMap{TKey}" />'s array and is reached by
///   a <b>stable slot index</b> — a placed cell never moves, so a held index stays valid.
///   <para>
///     Access discipline (do not violate — it is what keeps the map probe race-free):
///     <list type="bullet">
///       <item><c>Key</c>, <c>ChunkBase</c>, <c>CachedHead</c> — <b>producer only</b>.</item>
///       <item><c>Tail</c> — <b>producer only</b>.</item>
///       <item>
///         <c>Head</c> — normally the current owning worker during a drain, and the producer also
///         writes it in <c>BindChunk</c> (zeroing <c>Head</c>/<c>Tail</c>/<c>CachedHead</c> together
///         when binding a fresh chunk), in <c>Place</c> (initializing a fresh or re-keyed slot), and in
///         <c>EvictFree</c> (wiping a tombstoned slot after claiming it). "While no worker can own the
///         cell" is <b>not</b> a safe description of these: a worker can be stalled between its own
///         <c>Exchange(State, Free)</c> and its Tail recheck while the producer's <c>EvictFree</c> claims
///         and re-keys that very slot out from under it — see <c>ActorSystem.Activate</c>'s
///         <c>chunk == Chunk.None</c> guard for the mechanism and why the write-back stays a safe no-op.
///       </item>
///       <item>
///         <c>State</c> — producer and workers, mostly via <c>Interlocked</c> (CAS / <c>Exchange</c>);
///         <c>Harvest</c>'s and <c>EvictFree</c>'s final publish, and <c>Place</c>'s initial publish, are
///         a plain <c>Volatile.Write</c> instead — legal only because each is either preceded by an
///         <c>Interlocked</c> CAS that already claimed the cell, or (in <c>Place</c>) targets a slot
///         nothing else can observe yet.
///       </item>
///     </list>
///   </para>
///   Layout is array-of-structs with no padding on purpose: the worker touches the cell only once per
///   batch (publishing <c>Head</c>) and once per activation (<c>State</c>) — its per-message work is
///   entirely inside the chunk — so false sharing amortizes over the batch.
/// </summary>
internal struct KeyCell<TKey> {
	public TKey Key;
	public int State;
	public int ChunkBase;
	public int Head;
	public int Tail;
	public int CachedHead;
}

/// <summary>
///   The cell ownership states. <c>Free</c> MUST be 0 — <see cref="KeyCellMap{TKey}" />'s eviction
///   sweep keys off it, exactly as <c>CoalescingKeyMap</c> does.
/// </summary>
internal static class CellState {
	/// <summary>Nobody holds the key and it has no queued messages.</summary>
	internal const int Free = 0;

	/// <summary>The key's index is sitting in a scheduler queue.</summary>
	internal const int Scheduled = 1;

	/// <summary>A worker holds the key exclusively and is draining it.</summary>
	internal const int Owned = 2;

	/// <summary>Producer-only, cold: the producer is harvesting this cell's chunk (§4.4).</summary>
	internal const int Reclaiming = 3;
}
