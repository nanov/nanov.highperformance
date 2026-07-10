namespace Nanov.HighPerformance.Threading.Workers.Internal;

using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

/// <summary>
///   Internal single-producer / single-consumer ring buffer core, shared (by composition) across the
///   whole SPSC worker family: the power-of-two ring, the edge-triggered wake events, the dedicated
///   worker thread, the lifecycle (<see cref="Start" />/<see cref="Dispose" />/<see cref="TryComplete" />/
///   cancellation/completion), and the producer/consumer wait + signal primitives. The public workers
///   own one of these and forward to it; the public scopes hold one to reach the slot. Not part of the
///   public API.
///   <para>
///     The producer/consumer FAST paths (the empty/full index checks, slot write/read, and the
///     edge-triggered <c>Set()</c>) are <see cref="MethodImplOptions.AggressiveInlining" /> so they
///     inline into the callers' hot loops; only the COLD parking loops
///     (<see cref="WaitForSpaceSlow" />/<see cref="WaitForItemSlow" />) are
///     <see cref="MethodImplOptions.NoInlining" />, keeping the hot frame tight.
///   </para>
///   <para>
///     SIGNALING INVARIANT (strict SPSC). Exactly one producer thread reserves+commits slots; the
///     single dedicated worker thread is the only consumer and the only writer of <c>_idx.Head</c>. The
///     wake events use edge-triggered <c>Set()</c> (only on the empty→non-empty / full→non-full
///     transition) paired with a non-atomic <c>Wait();Reset()</c>. This is correct ONLY under SPSC:
///     while the consumer is parked, <c>_idx.Head</c> does not advance, so only the first item past
///     <c>_idx.Head</c> satisfies <c>_idx.Head == tail</c> and fires the lone <c>Set</c> — no second signal a
///     <c>Reset()</c> could lose. <see cref="ManualResetEventSlim" /> is latching (order-insensitive),
///     and the <c>while (_idx.Head == Volatile.Read(ref _idx.Tail))</c> recheck acquire-reads <c>_idx.Tail</c>
///     published with release before the <c>Set</c>. Shutdown rides the separate <c>volatile _stop</c>,
///     re-checked before every park. WARNING: a second producer, or advancing <c>_idx.Head</c> off the
///     worker thread, breaks this and hangs silently.
///   </para>
/// </summary>
internal sealed class SpRingCore<T> : IDisposable {
	/// <summary>
	///   SpinWait iterations before falling back to manual reset event wait.
	///   Reduces syscall overhead for bursty traffic at the cost of CPU during idle.
	/// </summary>
	private const int SpinThreshold = 200;

	internal readonly T[] _buffer;
	internal readonly int _mask;
	// Bound by the value workers to run their handler's Clear(ref T) on a slot at release (see
	// ClearAndAdvanceHead); stays default (inert) for the copy-based workers, which never create a HandleScope.
	private SlotCleaner<T> _cleaner;
	private readonly ManualResetEventSlim _itemEvt = new(false); // set when items available (dedicated-thread consumers)
	private readonly ManualResetEventSlim _spaceEvt = new(false); // set when space freed
	private readonly AsyncAutoResetEvent? _itemReadyAsync; // async item signal (ThreadPool-async consumers); null on the dedicated-thread path
	private readonly TaskCompletionSource _completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
	private readonly Thread? _thread; // null in ThreadPool-async mode (the worker drives an async loop instead)
	private volatile bool _completed; // soft stop (Complete) — drain remaining
	private Exception? _completeException;
	private CancellationToken _ct;
	private CancellationTokenRegistration _ctRegistration;
	private bool _disposed;
	private volatile bool _stop; // hard stop (Dispose / cancel) — drop remaining
	// Consumer index (Head) and producer index (Tail) on separate cache lines: each is written every
	// item by a different core, so co-locating them ping-pongs the line. 128-byte stride covers Apple
	// Silicon's 128-byte cache lines as well as x64's 64-byte ones.
	private PaddedIndices _idx;
	// Producer-private cached lower bound on the consumer's Head (LMAX "gating sequence"). Read/written
	// ONLY by the single producer (TryReserve / WaitForSpaceSlow), so it needs no synchronization. Head
	// only advances, so _cachedHead ≤ real Head always — it can only make the producer believe the ring is
	// MORE full than it is (conservative: never overwrites a live slot). The producer re-reads the real
	// Head only when the cached value says "full"; while the ring isn't full it never touches the
	// consumer's cache line for the fullness check (mirrors the consumer's per-run Tail batching in
	// TryClaimRun). Stays 0 until the first wrap — with Tail also 0, Tail − _cachedHead = 0 ≠ capacity,
	// so the inert initial state is correct.
	private int _cachedHead;
#if DEBUG
	private int _producing; // 0/1 — debug-only re-entrancy flag for the producer overlap guard
#endif

	// Dedicated-thread mode (sync workers + the dedicated-thread async workers).
	internal SpRingCore(int capacity, string threadName, ThreadStart runLoop) : this(capacity, threadName, runLoop,
		SlotCleaner<T>.NoOp) {
	}

	// Dedicated-thread mode (sync workers + the dedicated-thread async workers).
	internal SpRingCore(int capacity, string threadName, ThreadStart runLoop, SlotCleaner<T> cleaner) {
		capacity = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(capacity, 1));
		_buffer = new T[capacity];
		_mask = capacity - 1;
		_thread = new Thread(runLoop) {
			IsBackground = true,
			Name = threadName
		};
		_cleaner = cleaner;
	}

	// ThreadPool-async mode: no dedicated thread; the worker drives an async loop via Task and waits
	// for items through the async signal.
	internal SpRingCore(int capacity) : this(capacity, SlotCleaner<T>.NoOp) { }

	// ThreadPool-async mode: no dedicated thread; the worker drives an async loop via Task and waits
	// for items through the async signal.
	internal SpRingCore(int capacity, SlotCleaner<T> cleaner) {
		capacity = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(capacity, 1));
		_buffer = new T[capacity];
		_mask = capacity - 1;
		_thread = null;
		_itemReadyAsync = new AsyncAutoResetEvent();
		_cleaner = cleaner;
	}

	internal Task Completion => _completionTcs.Task;
	internal int QueuedApprox => _idx.Tail - _idx.Head;
	internal int Capacity => _buffer.Length;
	internal CancellationToken CancellationToken => _ct;

	internal bool IsStopped => _stop;

	internal void Start(CancellationToken ct) {
		_ct = ct;
		if (ct.CanBeCanceled)
			_ctRegistration = ct.Register(static s => ((SpRingCore<T>)s!).OnCancel(), this);
		_thread?.Start(); // ThreadPool-async workers launch their own loop after Start
	}

	private void OnCancel() => SignalStop();

	/// <summary>Set the hard-stop flag and wake every waiter (blocked producer, sync consumer, async consumer).</summary>
	internal void SignalStop() {
		_stop = true;
		_itemEvt.Set();
		_spaceEvt.Set();
		_itemReadyAsync?.Set();
	}

	internal bool TryComplete(Exception? exception) {
		if (_completed) return false;
		_completeException = exception;
		_completed = true;
		_itemEvt.Set();
		_itemReadyAsync?.Set();
		return true;
	}

	/// <summary>The exception to complete with: the explicit one, else cancellation, else null.</summary>
	internal Exception? CompletionException() =>
		_completeException ?? (_ct.IsCancellationRequested ? new OperationCanceledException(_ct) : null);

	/// <summary>Resolve the completion task and tear down the cancellation registration. Call once, on exit.</summary>
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
		SignalStop(); // wake both sides so they observe _stop and exit
		if (_thread is not null) {
			try {
				_thread.Join(1000);
			}
			catch {
				/* ignore */
			}
		}
		// ThreadPool-async workers await their loop Task before calling Dispose, so the events are
		// no longer in use by the consumer here.

		_itemEvt.Dispose();
		_spaceEvt.Dispose();
		GC.SuppressFinalize(this);
	}

	/// <summary>
	///   Producer reservation. Fast-path inline fullness check; parks on the cold path. Returns false
	///   if the worker is no longer accepting. On true, <paramref name="tail" /> is the index to write
	///   (<c>_buffer[tail &amp; _mask]</c>); the caller writes it then calls <see cref="CommitTail" />.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryReserve(out int tail) {
		if (_completed || _stop) {
			tail = 0;
			return false;
		}

		// Fullness check against the cached Head first — no cross-core read while the ring isn't full. Only
		// when the cache says "full" do we refresh from the real Head (one acquire-read) and re-check; if
		// still full, park in WaitForSpaceSlow (which also refreshes _cachedHead on exit).
		if (_idx.Tail - _cachedHead == _buffer.Length) {
			_cachedHead = Volatile.Read(ref _idx.Head);
			if (_idx.Tail - _cachedHead == _buffer.Length && !WaitForSpaceSlow()) {
				tail = 0;
				return false;
			}
		}

		tail = _idx.Tail;
		return true;
	}

	/// <summary>Publish the slot at <paramref name="tail" /> and edge-signal the consumer.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void CommitTail(int tail) {
		Volatile.Write(ref _idx.Tail, tail + 1);
		// Edge-triggered: signal only on the empty→non-empty transition. The edge test reads _idx.Head
		// AFTER publishing _idx.Tail — a store then a load of a DIFFERENT location, which a weak memory model
		// (ARM64) may reorder, letting the producer read a STALE _idx.Head and skip the wake for the item it
		// just committed (a StoreLoad / Dekker lost-wakeup). The consumer's post-Reset / arm-then-recheck
		// re-reads _idx.Tail published with release before the Set. See SIGNALING INVARIANT.
		if (_itemReadyAsync is null) {
			// Sync consumer: WaitForItemSlow spin-rechecks _idx.Tail (≈200×) before it parks, which drains the
			// store buffer and masks the StoreLoad window — no per-commit fence needed.
			if (Volatile.Read(ref _idx.Head) == tail)
				_itemEvt.Set();
		} else {
			// Async consumer parks immediately (no spin), so the window is exposed. A full fence makes the
			// Tail publish globally visible before the Head read, so the edge decision can't skip on a stale
			// Head; paired with the arm-then-recheck in WaitForItemAsync this closes the lost-wakeup.
			Interlocked.MemoryBarrier();
			if (Volatile.Read(ref _idx.Head) == tail)
				_itemReadyAsync.Set();
		}
	}

	/// <summary>Reserve + write + commit in one shot (copy-based producers). False if not accepting.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryEnqueue(T item) {
		DebugBeginProduce();
		if (!TryReserve(out var tail)) {
			DebugEndProduce();
			return false;
		}

		SlotRef(tail) = item;
		CommitTail(tail);
		DebugEndProduce();
		return true;
	}

	/// <summary>
	///   DEBUG-only producer overlap guard. SPSC requires the producer side to be non-concurrent —
	///   different threads may produce serially (with a happens-before edge between them), but two
	///   producers must never overlap. <see cref="DebugBeginProduce" /> marks a producer transaction
	///   in progress (the whole <see cref="TryEnqueue" />, or a <c>Publish()</c>→scope-<c>Dispose()</c>
	///   span) and throws if a second one starts before the first ends. Compiled out entirely in
	///   Release.
	/// </summary>
	[Conditional("DEBUG")]
	internal void DebugBeginProduce() {
#if DEBUG
		if (Interlocked.Exchange(ref _producing, 1) != 0)
			throw new InvalidOperationException(
				"Concurrent producer detected — SpWorker requires single-producer (non-overlapping) access.");
#endif
	}

	[Conditional("DEBUG")]
	internal void DebugEndProduce() {
#if DEBUG
		Volatile.Write(ref _producing, 0);
#endif
	}

	/// <summary>
	///   Ring slot ref without the array bounds check. The JIT can't prove
	///   <c>(index &amp; _mask) &lt; _buffer.Length</c> across fields, but it holds by construction:
	///   <c>_mask = Length − 1</c> with Length a power of two, so the masked index is always in range.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ref T SlotRef(int index) =>
		ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_buffer), (nint)(uint)(index & _mask));

	/// <summary>Writable ref to slot <paramref name="head" /> — the scope reads/writes the slot through this.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal ref T GetSlotRef(int head) => ref SlotRef(head);

	/// <summary>
	///   Release-time fast path for the value scope: run the handler's <c>Clear(ref T)</c> on slot
	///   <paramref name="head" />, then <see cref="AdvanceHead" /> (advance + edge-signal a blocked
	///   producer) — one call from <see cref="HandleScope{T}.Release" />. The handler nulls the slot's
	///   reference fields / returns pooled buffers, so the ring doesn't pin them and the zero-copy slot is
	///   never overwritten with a full <c>default(T)</c>. <c>Clear</c> is a no-op when no cleaner is bound
	///   (copy workers don't use this path).
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void ClearAndAdvanceHead(int head) {
		_cleaner.Clear(ref SlotRef(head));
		AdvanceHead(head);
	}

	/// <summary>Release the slot at <paramref name="head" /> and edge-signal a blocked producer.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void AdvanceHead(int head) {
		Volatile.Write(ref _idx.Head, head + 1);
		// Edge-triggered space signal, mirror of CommitTail: wake a blocked producer only on the
		// full→non-full transition. Safe under SPSC (single producer). See SIGNALING INVARIANT.
		if (Volatile.Read(ref _idx.Tail) - head == _buffer.Length)
			_spaceEvt.Set();
	}

	/// <summary>
	///   Per-item run drain for the run-claim consumer: hard-stop check (drops the item on cancel/dispose),
	///   then <see cref="TakeAndAdvance" />. False = the run is cut short by a hard stop; the loop exits.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryTakeAndAdvance(int head, out T item) {
		if (_stop) {
			item = default!;
			return false;
		}

		item = TakeAndAdvance(head);
		return true;
	}

	/// <summary>
	///   Copy the item out of slot <paramref name="head" />, clear the slot, and advance + space-signal
	///   the ring (<see cref="AdvanceHead" />) — i.e. free the slot eagerly, before the handler runs, so
	///   a slow handler can never stall the producer (only one item is ever in-flight in the handler).
	///   The clear is skipped for blittable <typeparamref name="T" />: <see cref="RuntimeHelpers.IsReferenceOrContainsReferences{T}" />
	///   is a JIT intrinsic folded to a constant per closed generic, so the clear (and its write) vanishes
	///   for e.g. <see cref="int" /> or a struct of primitives; reference (or ref-containing) <typeparamref name="T" />
	///   is still cleared so the ring doesn't pin it.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private T TakeAndAdvance(int head) {
		ref var slot = ref SlotRef(head);
		var item = slot;
		if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
			slot = default!;
		AdvanceHead(head);
		return item;
	}

	/// <summary>
	///   Consumer claim of the whole currently-available run <c>[head, available)</c>. One
	///   <c>_idx.Tail</c> read on the hot (non-empty) path — the caller then drains the run with
	///   <see cref="TakeAndAdvance" /> per item (head still advances per item, so backpressure stays
	///   tight and the space-signal edge stays correct), and only re-claims (re-parking when truly
	///   empty) once the run is exhausted. This drops the per-item emptiness <c>_idx.Tail</c> read that a
	///   per-item claim would pay. Returns false to exit — hard stop (drops queued items) or soft
	///   complete with the ring drained. Single consumer only.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryClaimRun(out int head, out int available) {
		var tail = Volatile.Read(ref _idx.Tail);
		if (_idx.Head == tail) {
			if (!WaitForItemSlow()) {
				head = 0;
				available = 0;
				return false;
			}

			tail = Volatile.Read(ref _idx.Tail); // re-read the now-advanced tail after waking
		}

		// Hard stop drops items still in the ring; soft complete drains them (WaitForItemSlow exits
		// above only once the ring is empty).
		if (_stop) {
			head = 0;
			available = 0;
			return false;
		}

		head = _idx.Head;
		available = tail;
		return true;
	}

	// --- ThreadPool-async consumer path -----------------------------------------------------------

	/// <summary>
	///   Non-blocking consumer run-claim for the async loop — the no-park twin of
	///   <see cref="TryClaimRun" />. Returns false if the ring is empty or hard-stopped (the caller then
	///   calls <see cref="WaitForItemAsync" /> to distinguish wait from exit). On true, the caller drains
	///   <c>[head, available)</c> with <see cref="TryTakeAndAdvance" /> (copy) / <see cref="HandleScope{T}" />
	///   (value) per item, then re-claims — one <c>_idx.Tail</c> read per run instead of per item, matching
	///   the sync consumer. Single consumer only.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryClaimRunNow(out int head, out int available) {
		var tail = Volatile.Read(ref _idx.Tail);
		if (_idx.Head == tail || _stop) {
			head = 0;
			available = 0;
			return false;
		}

		head = _idx.Head;
		available = tail;
		return true;
	}

	/// <summary>
	///   Item wait for the ThreadPool-async loop when the ring is empty. Returns a
	///   <see cref="ValueTask{Boolean}" />: completed-<see langword="false" /> on hard stop or
	///   soft-complete-drained (caller exits); completed-<see langword="true" /> when an item is
	///   already available; otherwise the <see cref="AsyncAutoResetEvent" />'s pending wait (result
	///   <see langword="true" /> = "woke" — the caller re-checks via <see cref="TryClaimRunNow" /> on the
	///   next loop turn). Deliberately NOT an <c>async</c> method, so
	///   an empty wait allocates nothing — the suspension reuses the caller's single (one-time)
	///   RunLoopAsync state machine. Hard stop is checked first, so queued items are dropped on stop.
	///   Single logical consumer only.
	/// </summary>
	internal ValueTask<bool> WaitForItemAsync() {
		if (_stop) return new ValueTask<bool>(false);
		if (_idx.Head != Volatile.Read(ref _idx.Tail)) return new ValueTask<bool>(true);
		if (_completed) return DrainedOnComplete();

		// Ring looks empty and we're about to park. Arm FIRST — the AsyncAutoResetEvent's Interlocked CAS is
		// a full StoreLoad fence — THEN recheck. Paired with CommitTail's producer-side fence, a freshly
		// committed item is globally visible here (Dekker), so we self-signal and let the loop drain it
		// instead of parking on an edge the producer may have skipped. Also catches a late stop/complete.
		var vt = _itemReadyAsync!.WaitAsync();
		if (vt.IsCompleted) return vt; // a Set was latched between the empty check above and the arm
		if (_stop || _completed || _idx.Head != Volatile.Read(ref _idx.Tail))
			_itemReadyAsync.Set(); // self-signal: complete the armed wait so the loop re-evaluates
		return vt;
	}

	// Soft-complete must drain everything already committed. _completed is published (release) AFTER the
	// producer's final commit, so re-reading the ring after observing _completed (acquire) sees the final
	// _idx.Tail — without this re-read a (possibly stale) empty check could abandon items committed right
	// before completion. Returns true (an item is available → drain it) or false (drained → exit).
	private ValueTask<bool> DrainedOnComplete() =>
		_idx.Head != Volatile.Read(ref _idx.Tail) ? new ValueTask<bool>(true) : new ValueTask<bool>(false);

	// Cold path: producer parks until space frees, or the worker stops/completes.
	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool WaitForSpaceSlow() {
		var spins = 0;
		int head;
		while (_idx.Tail - (head = Volatile.Read(ref _idx.Head)) == _buffer.Length) {
			if (_stop || _completed) return false;
			if (spins++ < SpinThreshold) {
				Thread.SpinWait(10);
				continue;
			}

			_spaceEvt.Wait();
			_spaceEvt.Reset();
			spins = 0;
		}

		_cachedHead = head; // refresh the gating cache with the Head that proved there is space
		return true;
	}

	// Cold path: consumer parks until an item arrives, or the worker stops/completes-drained.
	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool WaitForItemSlow() {
		var spins = 0;
		while (_idx.Head == Volatile.Read(ref _idx.Tail)) {
			if (_stop) return false;
			// Soft-complete drains: re-read _idx.Tail after observing _completed (acquire) so an item
			// committed right before completion — not yet visible to the empty check above — is still drained.
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
}

/// <summary>
///   Cache-line-separated SPSC ring indices. Non-generic on purpose — generic types can't have
///   explicit layout. The consumer-written <see cref="Head" /> and producer-written <see cref="Tail" />
///   sit 128 bytes apart (one full cache line on Apple Silicon, two on x64) so the two cores don't
///   ping-pong a shared line every item.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = 384)]
internal struct PaddedIndices {
	[FieldOffset(128)] public int Head;
	[FieldOffset(256)] public int Tail;
}
