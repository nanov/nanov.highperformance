namespace Nanov.HighPerformance.Threading.Workers.Internal;

using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
///   SPSC ring core for the key-conflating worker. A two-array ring (<c>TKey[]</c> + <c>TValue[]</c>)
///   plus a producer-private <see cref="CompactingKeyMap{TKey}" />. Unlike the strict-FIFO
///   <see cref="SpRingCore{T}" />, the producer may write a value slot inside <c>[head, tail)</c> (an
///   in-place override of an already-queued key) — so this is a deliberate SIBLING, not a modification
///   of <see cref="SpRingCore{T}" />, keeping that core's FIFO invariant pristine. Generic over the
///   merge facet <typeparamref name="TMerger" /> only (never <c>Handle</c>), so the consumer run loop
///   lives in the worker and a future async worker can reuse this core.
///   <para>
///     LOSSLESS OVERRIDE PROTOCOL (Dekker, fenced on BOTH sides). Override: write the value slot
///     (atomic reference <c>Volatile.Write</c>) → StoreLoad fence (<see cref="Interlocked.MemoryBarrier" />)
///     → read <c>_reading</c> THEN <c>Head</c>. Trust the in-place override (the consumer will
///     acquire-read the merged value) unless the consumer has CONSUMED the slot (<c>Head &gt; seq</c>) or
///     is MID-READING this exact slot (<c>Head == seq &amp;&amp; _reading</c>); in those cases enqueue a
///     FRESH slot with the un-merged incoming value (a rare, harmless duplicate). This is correct for
///     LAST-WINS only (incoming wins regardless of what the consumer took); combine/additive merges
///     cannot be made exactly-once on a ring and belong in a cell-based accumulating worker.
///     Consumer side (<see cref="TryTake" />): set <c>_reading=1</c> → StoreLoad fence → read the
///     value → advance <c>Head</c> → clear <c>_reading=0</c>. The matching fences plus the producer
///     reading <c>_reading</c> before <c>Head</c> make it impossible to both read a stale value AND have
///     the producer trust the override. The <c>_reading</c> flag is what lets the slot at <c>Head</c>
///     conflate cleanly (no spurious duplicate) when the consumer is merely parked, not mid-read. Single
///     producer (caller contract, DEBUG-guarded); single consumer (the worker's own thread).
///   </para>
/// </summary>
internal sealed class CompactingSpRingCore<TKey, TValue, TMerger> : IDisposable
	where TKey : notnull, IEquatable<TKey>
	where TValue : class
	where TMerger : struct, IItemMerger<TValue> {
	private const int SpinThreshold = 200;

	private readonly TKey[] _keys;
	private readonly TValue?[] _values;
	private readonly int _mask;
	private TMerger _merger;                  // producer-side; pure Merge only
	private CompactingKeyMap<TKey> _pending;  // PRODUCER-THREAD-ONLY: key → seq

	private readonly ManualResetEventSlim _itemEvt = new(false);
	private readonly ManualResetEventSlim _spaceEvt = new(false);
	private readonly TaskCompletionSource _completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
	private readonly Thread _thread;
	private volatile bool _completed;
	private Exception? _completeException;
	private CancellationToken _ct;
	private CancellationTokenRegistration _ctRegistration;
	private bool _disposed;
	private volatile bool _stop;
	private PaddedIndices _idx;               // cache-line-separated Head/Tail (reused from SpRingCore.cs)
	private int _cachedHead;                  // producer-private gating cache (LMAX)
	// Consumer-written / producer-read: 1 while the consumer is mid-reading the slot at Head, else 0. Lets
	// the override recheck distinguish "consumer parked at Head, slot untouched" (safe to override in place)
	// from "consumer mid-read of this exact slot" (must enqueue fresh). See the OVERRIDE PROTOCOL doc.
	private int _reading;
#if DEBUG
	private int _producing;                   // DEBUG-only producer overlap guard
#endif

	internal CompactingSpRingCore(TMerger merger, int capacity, string threadName, ThreadStart runLoop) {
		capacity = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(capacity, 1));
		_keys = new TKey[capacity];
		_values = new TValue?[capacity];
		_mask = capacity - 1;
		_merger = merger;
		_pending = new CompactingKeyMap<TKey>(capacity);
		_thread = new Thread(runLoop) {
			IsBackground = true,
			Name = threadName
		};
	}

	internal Task Completion => _completionTcs.Task;
	internal int QueuedApprox => _idx.Tail - _idx.Head;
	internal int Capacity => _keys.Length;

	internal void Start(CancellationToken ct) {
		_ct = ct;
		if (ct.CanBeCanceled)
			_ctRegistration = ct.Register(static s => ((CompactingSpRingCore<TKey, TValue, TMerger>)s!).SignalStop(), this);
		_thread.Start();
	}

	internal void SignalStop() {
		_stop = true;
		_itemEvt.Set();
		_spaceEvt.Set();
	}

	internal bool TryComplete(Exception? exception) {
		if (_completed) return false;
		_completeException = exception;
		_completed = true;
		_itemEvt.Set();
		return true;
	}

	internal Exception? CompletionException() =>
		_completeException ?? (_ct.IsCancellationRequested ? new OperationCanceledException(_ct) : null);

	internal void SetCompletion(Exception? exception) {
		if (exception is OperationCanceledException)
			_completionTcs.TrySetCanceled(_ct);
		else if (exception is not null)
			_completionTcs.TrySetException(exception);
		else
			_completionTcs.TrySetResult();
		_ctRegistration.Dispose();
	}

	public void Dispose() {
		if (_disposed) return;
		_disposed = true;
		SignalStop();
		try {
			_thread.Join(1000);
		}
		catch {
			/* ignore */
		}

		_itemEvt.Dispose();
		_spaceEvt.Dispose();
		_pending.Dispose();
		GC.SuppressFinalize(this);
	}

	// --- Producer: conflation dispatch -----------------------------------------------------------

	/// <summary>
	///   Conflating enqueue. If the key is still pending, merge in place + fenced recheck (deliver the
	///   merged value, or enqueue fresh on doubt). Otherwise enqueue a fresh slot. False if completed/stopped.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool Dispatch(TKey key, TValue value) {
		DebugBeginProduce();
		try {
			if (_completed || _stop) return false;

			if (_pending.TryGetSeq(key, out var seq) && IsPending(seq)) {
				var slot = seq & _mask;
				var current = Volatile.Read(ref _values[slot])!;
				var merged = _merger.Merge(current, value);
				Volatile.Write(ref _values[slot], merged);
				Interlocked.MemoryBarrier(); // StoreLoad: publish merged before reading _reading/Head
				// Read _reading BEFORE Head (the order that makes this lost-update-free — see protocol doc).
				// Trust the in-place override unless the consumer has CONSUMED the slot (head > seq) or is
				// MID-READING this exact slot (head == seq && reading). Otherwise (head < seq, or head == seq
				// with the consumer parked / not yet reading it) the consumer will acquire-read the merged value.
				var reading = Volatile.Read(ref _reading);
				var head = Volatile.Read(ref _idx.Head);
				if (head - seq < 0 || (head == seq && reading == 0)) {
					// Trust the in-place override. Release the input(s) the merge dropped — whichever of
					// current/incoming it did NOT return (the returned value is live in the slot). Covers
					// last-wins (frees current), keep-existing (frees incoming), and combine (frees both).
					// Guard the alias case (caller reused a reference) so a value isn't discarded twice.
					if (!ReferenceEquals(merged, current))
						_merger.Discarded(current);
					if (!ReferenceEquals(merged, value) && !ReferenceEquals(value, current))
						_merger.Discarded(value);
					return true;
				}
				// else: consumer consumed or is mid-reading the slot → enqueue the un-merged value fresh.
				// CORRECT ONLY for last-wins (incoming wins regardless of what was consumed). Combine/additive
				// merges cannot be made exactly-once here (the consumer may have taken `current`, which `merged`
				// also contains) — use the cell-based accumulating worker for those.
			}

			if (!TryReserve(out var tail)) return false;
			var freshSlot = tail & _mask;
			_keys[freshSlot] = key;
			Volatile.Write(ref _values[freshSlot], value);
			if (_pending.IsFull)
				_pending.Compact(new PendingPredicate(Volatile.Read(ref _idx.Head), tail));
			_pending.Set(key, tail);
			CommitTail(tail);
			return true;
		}
		finally {
			DebugEndProduce();
		}
	}

	// seq is in the live window [Head, Tail). Producer owns Tail (plain read); Head is acquire-read.
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private bool IsPending(int seq) {
		var head = Volatile.Read(ref _idx.Head);
		return seq - head >= 0 && _idx.Tail - seq > 0;
	}

	private readonly struct PendingPredicate : ISeqKeepPredicate {
		private readonly int _head;
		private readonly int _tail;

		internal PendingPredicate(int head, int tail) {
			_head = head;
			_tail = tail;
		}

		public bool Keep(int seq) => seq - _head >= 0 && _tail - seq > 0;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private bool TryReserve(out int tail) {
		if (_completed || _stop) {
			tail = 0;
			return false;
		}

		if (_idx.Tail - _cachedHead == _keys.Length) {
			_cachedHead = Volatile.Read(ref _idx.Head);
			if (_idx.Tail - _cachedHead == _keys.Length && !WaitForSpaceSlow()) {
				tail = 0;
				return false;
			}
		}

		tail = _idx.Tail;
		return true;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void CommitTail(int tail) {
		Volatile.Write(ref _idx.Tail, tail + 1);
		// Edge-triggered: signal only on empty→non-empty. Sync consumer's spin-recheck masks the
		// StoreLoad window (see SpRingCore SIGNALING INVARIANT) — no per-commit fence needed here.
		if (Volatile.Read(ref _idx.Head) == tail)
			_itemEvt.Set();
	}

	// --- Consumer (driven by the worker's run loop) ----------------------------------------------

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryClaimRun(out int head, out int available) {
		var tail = Volatile.Read(ref _idx.Tail);
		if (_idx.Head == tail) {
			if (!WaitForItemSlow()) {
				head = 0;
				available = 0;
				return false;
			}

			tail = Volatile.Read(ref _idx.Tail);
		}

		if (_stop) {
			head = 0;
			available = 0;
			return false;
		}

		head = _idx.Head;
		available = tail;
		return true;
	}

	/// <summary>
	///   Take the (key, value) at <paramref name="head" />, clear the slot, advance + space-signal. The
	///   value is acquire-read because a conflated override publishes it without moving <c>Tail</c>.
	///   False on hard stop (drops the item).
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryTake(int head, out TKey key, out TValue value) {
		if (_stop) {
			key = default!;
			value = default!;
			return false;
		}

		var slot = head & _mask;
		// Announce we're mid-reading this slot, then a StoreLoad fence so the announce is globally visible
		// BEFORE we read the value (Dekker: the producer's override fences symmetrically). Without this the
		// value read could hoist ahead of the announce and a racing override would be lost.
		Volatile.Write(ref _reading, 1);
		Interlocked.MemoryBarrier();
		key = _keys[slot];
		value = Volatile.Read(ref _values[slot])!;
		_keys[slot] = default!;
		Volatile.Write(ref _values[slot], null);
		AdvanceHead(head);            // publish head+1 (release) BEFORE clearing _reading
		Volatile.Write(ref _reading, 0);
		return true;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void AdvanceHead(int head) {
		Volatile.Write(ref _idx.Head, head + 1);
		if (Volatile.Read(ref _idx.Tail) - head == _keys.Length)
			_spaceEvt.Set();
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool WaitForSpaceSlow() {
		var spins = 0;
		int head;
		while (_idx.Tail - (head = Volatile.Read(ref _idx.Head)) == _keys.Length) {
			if (_stop || _completed) return false;
			if (spins++ < SpinThreshold) {
				Thread.SpinWait(10);
				continue;
			}

			_spaceEvt.Wait();
			_spaceEvt.Reset();
			spins = 0;
		}

		_cachedHead = head;
		return true;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool WaitForItemSlow() {
		var spins = 0;
		while (_idx.Head == Volatile.Read(ref _idx.Tail)) {
			if (_stop) return false;
			if (_completed) return _idx.Head != Volatile.Read(ref _idx.Tail);
			if (spins++ < SpinThreshold) {
				Thread.SpinWait(10);
				continue;
			}

			_itemEvt.Wait();
			_itemEvt.Reset();
			spins = 0;
		}

		return true;
	}

	[Conditional("DEBUG")]
	private void DebugBeginProduce() {
#if DEBUG
		if (Interlocked.Exchange(ref _producing, 1) != 0)
			throw new InvalidOperationException(
				"Concurrent producer detected — CompactingSpWorker requires single-producer (non-overlapping) access.");
#endif
	}

	[Conditional("DEBUG")]
	private void DebugEndProduce() {
#if DEBUG
		Volatile.Write(ref _producing, 0);
#endif
	}
}
