namespace Nanov.HighPerformance.Threading.Workers;

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Nanov.HighPerformance.Threading.Workers.Internal;

/// <summary>
///   Single-producer / single-consumer worker that <b>conflates by key</b>, <b>last-wins</b>: the producer
///   calls <see cref="Dispatch" /> with a <c>(key, value)</c>; if that key is still pending, the latest value
///   <b>overrides</b> the queued one — so the consumer sees only the latest value per key, in first-seen key
///   order. There is no merge or drop callback — conflation is a plain override. Zero per-message allocation;
///   blocking backpressure when <see cref="Capacity" /> distinct keys are pending.
///   <para>
///     This is the <b>last-wins twin</b> of <see cref="CoalescingSpWorker{TKey, TValue, THandler}" /> (which is
///     exactly-once additive): it shares the coalescer's two structures — a producer-private, no-relocation
///     <see cref="CoalescingKeyMap{TKey, TValue}" /> of stable cells (<c>{Key, Value, State}</c>) plus a
///     <see cref="SpRingCore{T}" /> of dirty cell <c>int</c> indices — but replaces the 4-state merge lock with
///     a 3-state <b>presence</b> word (<c>Free</c>/<c>Available</c>/<c>Consuming</c>) and plain overwrite
///     instead of a read-modify-write. Last-wins has no merge to serialize, so the producer just writes the
///     cell's <c>Value</c> (a single atomic reference write — hence <typeparamref name="TValue" /> is a
///     <see langword="class" />) and flips the state with one <see cref="Interlocked.Exchange(ref int, int)" />.
///     <c>Value</c> has a <b>single writer, the producer</b> — the consumer never writes it (it only reads
///     the latest and flips <c>State</c>). It must not: the producer's <c>Value</c> write and its
///     <c>State</c> publish are not one atomic step, so a consumer clear can null a value the producer has
///     written but not yet published, stranding the next ring entry with a <c>null</c>. The reference is
///     released for GC when the producer overwrites it (the key's next dispatch) or the cell is evicted.
///     Eviction is the coalescer's unchanged producer-side, state-gated tombstone (a slot is reclaimed only
///     while <c>Free</c>, resetting the cell), which both frees the value and makes a handed-out index safe to reuse.
///   </para>
///   The handler is a <see langword="struct" /> constrained to <see cref="IConflationWorkerHandler{TKey, TValue}" />
///   so <c>Handle</c>/<c>OnError</c>/<c>OnCompleted</c> devirtualize and inline. Single producer is the caller's
///   contract; the consumer is the worker's own thread.
/// </summary>
public sealed class ConflationSpWorker<TKey, TValue, THandler> : IDisposable, IWorkerQueue
	where TKey : notnull, IEquatable<TKey>
	where TValue : class
	where THandler : struct, IConflationWorkerHandler<TKey, TValue> {
	// Presence states. Only Free == 0 is "evictable" (the contract CoalescingKeyMap.EvictFree keys off); the
	// other two are arbitrary non-zero values. No Merging — last-wins has no read-modify-write to serialize.
	private const int Free = 0;       // drained / never dirtied → evictable
	private const int Available = 1;  // queued: index is in the dirty ring, Value holds the latest
	private const int Consuming = 2;  // consumer has claimed the cell and is reading it; not done

	private readonly SpRingCore<int> _ring; // dirty-cell index queue (FIFO SPSC)
	private readonly CoalescingKeyMap<TKey, TValue> _map; // producer-private; cells reached by index
	private THandler _handler; // mutable struct — Handle/OnError/OnCompleted go through this field
#if DEBUG
	private int _producing; // DEBUG-only producer overlap guard (whole Dispatch is the transaction)
#endif

	public ConflationSpWorker(THandler handler, int capacity, string? threadName = null) {
		_handler = handler;
		_map = new CoalescingKeyMap<TKey, TValue>(capacity);
		_ring = new SpRingCore<int>(capacity,
			threadName ?? $"ConflationSpWorker<{typeof(TKey).Name},{typeof(TValue).Name}>", RunLoop);
	}

	/// <summary>Completes when the worker has drained and exited (after Complete() or Dispose()).</summary>
	public Task Completion => _ring.Completion;

	/// <summary>Approximate distinct keys currently queued (<c>Tail − Head</c>). Lock-free, racy by a few.</summary>
	public int QueuedApprox => _ring.QueuedApprox;

	/// <summary>Ring capacity (power of two) = max distinct pending keys.</summary>
	public int Capacity => _ring.Capacity;

	/// <summary>
	///   Start the worker thread. Optionally pass a CancellationToken — when cancelled, the worker stops
	///   immediately (drops pending items) and <see cref="Completion" /> resolves with an
	///   <see cref="OperationCanceledException" />.
	/// </summary>
	public void Start(CancellationToken ct = default) => _ring.Start(ct);

	/// <summary>
	///   Enqueue <paramref name="value" /> under <paramref name="key" />, conflating with any still-queued
	///   value for the same key (last-wins). Returns false if completed/disposed. Conflating an already-pending
	///   key never blocks; a new pending key blocks when <see cref="Capacity" /> keys are already pending.
	/// </summary>
	public bool Dispatch(TKey key, TValue value) {
		DebugBeginProduce();
		try {
			// Hard stop (Dispose/cancel) tears the map down, so bail before touching it. The soft-complete
			// path keeps the map valid and is rejected later by TryEnqueue (→ rollback → false).
			if (_ring.IsStopped)
				return false;

			ref var cell = ref _map.GetOrAddRef(key, out _, out var index);

			// Last-wins overwrite: a single atomic reference write. Value has ONE writer (this producer); the
			// consumer only ever CASes it back to null, so there is no lost update / torn read.
			Volatile.Write(ref cell.Value, value);

			// Claim the cell as queued. Exchange is a full fence, so the value write above is published before
			// any consumer can observe Available.
			var prev = Interlocked.Exchange(ref cell.State, Available);
			if (prev == Available)
				return true; // already in the dirty ring → conflated in place, keeps its position

			// prev was Free (new/drained) or Consuming (consumer dequeued already & is mid-read) → the index is
			// NOT in the ring → enqueue it once. On Consuming, the consumer's closing Consuming→Free CAS will
			// fail (state is now Available) and this fresh ring entry redelivers the latest value.
			if (_ring.TryEnqueue(index))
				return true;

			Volatile.Write(ref cell.State, Free); // worker not accepting (completed/stopped) → undo the mark
			return false;
		}
		finally {
			DebugEndProduce();
		}
	}

	/// <summary>
	///   Signal that no more items will be dispatched. The worker drains remaining items and then exits.
	///   Optionally pass an exception — if non-null, <see cref="Completion" /> resolves faulted with it.
	///   Returns true on first call, false if already completed.
	/// </summary>
	public bool TryComplete(Exception? exception = null) => _ring.TryComplete(exception);

	public void Dispose() {
		// A consumer that didn't exit may still be reading the map — leave it to the GC then.
		if (_ring.Shutdown(null)) _map.Dispose();
	}

	private void RunLoop() {
		try {
			while (_ring.TryClaimRun(out var head, out var available)) {
				while (head != available && _ring.TryTakeAndAdvance(head++, out var index)) {
					ref var cell = ref _map.RefAt(index);

					// Claim: Available -> Consuming. A spurious Free/Consuming (shouldn't normally happen) → skip.
					if (Interlocked.CompareExchange(ref cell.State, Consuming, Available) != Available)
						continue;

					var key = cell.Key;
					var taken = Volatile.Read(ref cell.Value); // latest (published before the producer's Exchange)

					// Release: Consuming -> Free. Fails iff the producer re-dirtied (set Available) during the read,
					// in which case a fresh ring entry already exists → leave the cell queued (redelivered).
					//
					// The consumer does NOT null cell.Value: the producer is Value's single writer, and its
					// Value-write and State-publish (Exchange) are not one atomic step, so any consumer clear can
					// race a value the producer has written but not yet published — nulling it strands the next
					// ring entry with a null (an NRE in the handler). A conditional clear can't distinguish that
					// window (the read may already capture the to-be-published value). The reference is released
					// for GC when the producer overwrites it (the key's next dispatch) or the cell is evicted
					// (EvictFree/Place reset the cell) — never eagerly on drain. See CLAUDE.md invariants.
					Interlocked.CompareExchange(ref cell.State, Free, Consuming);

					try {
						_handler.Handle(key, taken!);
					}
					catch (Exception ex) {
						_handler.OnError(ex);
					}
				}
			}
		}
		finally {
			var ex = _ring.CompletionException();
			try {
				_handler.OnCompleted(ex);
			}
			catch (Exception onCompletedEx) {
				_handler.OnError(onCompletedEx);
			}

			_ring.SetCompletion(ex);
		}
	}

	[Conditional("DEBUG")]
	private void DebugBeginProduce() {
#if DEBUG
		if (Interlocked.Exchange(ref _producing, 1) != 0)
			throw new InvalidOperationException(
				"Concurrent producer detected — ConflationSpWorker requires single-producer (non-overlapping) access.");
#endif
	}

	[Conditional("DEBUG")]
	private void DebugEndProduce() {
#if DEBUG
		Volatile.Write(ref _producing, 0);
#endif
	}
}
