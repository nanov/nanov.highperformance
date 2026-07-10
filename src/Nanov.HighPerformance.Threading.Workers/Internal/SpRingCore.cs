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
///   whole SPSC worker family: the power-of-two ring, the parked-flag wake signaling, the dedicated
///   worker thread, the lifecycle (<see cref="Start" />/<see cref="Dispose" />/<see cref="TryComplete" />/
///   cancellation/completion), and the producer/consumer wait + signal primitives. The public workers
///   own one of these and forward to it; the public scopes hold one to reach the slot. Not part of the
///   public API.
///   <para>
///     The producer/consumer FAST paths (the empty/full index checks, slot write/read, and the
///     parked-flag-gated <c>Set()</c>) are <see cref="MethodImplOptions.AggressiveInlining" /> so they
///     inline into the callers' hot loops; only the COLD parking loops
///     (<see cref="WaitForSpaceSlow" />/<see cref="WaitForItemSlow" />) are
///     <see cref="MethodImplOptions.NoInlining" />, keeping the hot frame tight.
///   </para>
///   <para>
///     SIGNALING INVARIANT (strict SPSC, parked-flag protocol). Exactly one producer thread
///     reserves+commits slots; the single consumer is the only writer of <c>_idx.Head</c>. Wakeups
///     are gated by parked-waiter flags, not by re-deriving the empty/full edge from the other
///     side's index: a waiter raises its flag (<c>_idx.ConsumerParked</c> / <c>_idx.ProducerParked</c>)
///     when it ENTERS its slow path and clears it on exit; the opposite side signals only when the
///     flag is up. A signal may be SPURIOUS, never LOST: the flag goes up before the waiter's final
///     recheck of the other index, and that recheck is ordered after the flag write — by a full
///     fence before every kernel park (sync, cold) or by the <c>WaitAsync</c> CAS in
///     arm-then-recheck (async) — so either the committer sees the flag, or the waiter sees the
///     fresh index and never parks. On the sync side, where <c>CommitTail</c> pays no per-commit
///     fence, the consumer's ~200-iteration spin re-reading the index is the load-bearing mask for
///     the store-buffer window (not merely additional insurance); the async consumer has no spin phase, so
///     the async branch of <c>CommitTail</c> keeps a per-commit <c>Interlocked.MemoryBarrier()</c>
///     (StoreLoad — <c>Volatile</c> release/acquire does not order a store before a subsequent load
///     of a different location on ARM64). Shutdown rides <c>volatile _stop</c> / <c>_completed</c>;
///     <c>SignalStop</c>/<c>TryComplete</c> Set every event UNCONDITIONALLY (never flag-gated) and
///     both slow paths recheck them before every park. WARNING: a second concurrent producer, or
///     advancing <c>_idx.Head</c> off the worker thread, breaks this and hangs silently.
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
	// Batched-release flush threshold for the FIFO drain loops: publish Head every this-many deferred
	// releases (also always at run boundaries, and immediately when the producer is parked). max(1, …)
	// disables batching for tiny rings; capacity/4 keeps at least ~3/4 of a small ring usable while a
	// batch is pending; 32 caps the staleness for big rings.
	private readonly int _flushThreshold;
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
		_flushThreshold = Math.Max(1, Math.Min(32, capacity >> 2));
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
		_flushThreshold = Math.Max(1, Math.Min(32, capacity >> 2));
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
		_spaceEvt.Set(); // wake a producer parked on a full ring so it observes _completed and stops
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
	///   (via <see cref="GetSlotRef" /> / <see cref="SlotRef" />); the caller writes it then calls <see cref="CommitTail" />.
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

	/// <summary>Publish the slot at <paramref name="tail" /> and wake the consumer if it is parked.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void CommitTail(int tail) {
		Volatile.Write(ref _idx.Tail, tail + 1);
		// Parked-flag signaling: wake the consumer only when its parked flag is up, instead of
		// re-deriving the empty→non-empty edge from a per-commit cross-core read of _idx.Head. The
		// flag's line changes only on slow-path entry/exit, so this read stays warm while the consumer
		// is running. A signal may be SPURIOUS (flag up = consumer still in its spin phase — harmless,
		// the events latch), never LOST: the consumer raises the flag BEFORE its spin-guarded (sync) /
		// fenced (async) final recheck of _idx.Tail. See SIGNALING INVARIANT.
		if (_itemReadyAsync is null) {
			// Sync consumer: no per-commit fence. The flag goes up before WaitForItemSlow's whole
			// ~200-iteration spin phase (which re-reads _idx.Tail), so a commit whose — possibly
			// store-buffer-hoisted — flag read missed the flag has its Tail store visible long before
			// the consumer's fenced pre-park recheck.
			if (Volatile.Read(ref _idx.ConsumerParked) != 0)
				_itemEvt.Set();
		} else {
			// Async consumer parks without a spin phase, so the Dekker window is real: the full fence
			// makes the Tail publish globally visible before the flag read, paired with the
			// arm-then-recheck in WaitForItemAsync (whose WaitAsync CAS is the consumer-side fence).
			Interlocked.MemoryBarrier();
			if (Volatile.Read(ref _idx.ConsumerParked) != 0)
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
	///   <paramref name="head" />, then <see cref="AdvanceHead" /> (advance + wake a parked producer)
	///   — one call from <see cref="HandleScope{T}.Release" />. The handler nulls the slot's
	///   reference fields / returns pooled buffers, so the ring doesn't pin them and the zero-copy slot is
	///   never overwritten with a full <c>default(T)</c>. <c>Clear</c> is a no-op when no cleaner is bound
	///   (copy workers don't use this path).
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void ClearAndAdvanceHead(int head) {
		_cleaner.Clear(ref SlotRef(head));
		AdvanceHead(head);
	}

	/// <summary>
	///   Batched-release twin of <see cref="TryTakeAndAdvance" />: hard-stop check + copy-out + slot
	///   clear, WITHOUT publishing <c>Head</c>. The FIFO drain loops batch the publishes through
	///   <see cref="PublishHead" /> per <see cref="ShouldFlush" /> — and ALWAYS flush at run
	///   boundaries, i.e. before every re-claim/park: an empty ring implies everything is published
	///   (the no-deadlock rule; see BATCHED HEAD PUBLISH in CLAUDE.md). The keyed workers keep the
	///   per-item <see cref="TryTakeAndAdvance" /> path.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryTake(int head, out T item) {
		if (_stop) {
			item = default!;
			return false;
		}

		ref var slot = ref SlotRef(head);
		item = slot;
		if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
			slot = default!;
		return true;
	}

	/// <summary>
	///   Run the bound cleaner on slot <paramref name="head" /> without publishing — the deferred
	///   (auto-release) half of the value path; <see cref="HandleScope{T}.ReleaseDeferred" /> calls
	///   this, the loop publishes later per its flush policy.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void ClearSlot(int head) => _cleaner.Clear(ref SlotRef(head));

	/// <summary>
	///   Publish the consumer's head cursor (ABSOLUTE value, unlike <see cref="AdvanceHead" />'s
	///   item index) and wake the producer if it is parked. Every slot below
	///   <paramref name="nextHead" /> must already be cleared/copied out.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void PublishHead(int nextHead) {
		Volatile.Write(ref _idx.Head, nextHead);
		if (Volatile.Read(ref _idx.ProducerParked) != 0)
			_spaceEvt.Set();
	}

	/// <summary>
	///   Flush policy for the batched release: publish every <c>_flushThreshold</c> deferred items, or
	///   immediately when the producer is parked (warm-line read, checked per item) — so a blocked
	///   producer never waits on the batch, only on the one handler currently running.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool ShouldFlush(int pending) =>
		pending >= _flushThreshold || Volatile.Read(ref _idx.ProducerParked) != 0;

	/// <summary>Release the slot at <paramref name="head" /> and wake the producer if it is parked.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void AdvanceHead(int head) => PublishHead(head + 1);

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
	///   <c>_idx.Tail</c> read on the hot (non-empty) path — the caller then drains the run — keyed
	///   workers per item via <see cref="TryTakeAndAdvance" /> (head advances per item); the FIFO
	///   workers via <see cref="TryTake" /> with batched <see cref="PublishHead" /> flushes (always
	///   flushed at the run boundary) — and only re-claims (re-parking when truly empty) once the run
	///   is exhausted. This drops the per-item emptiness <c>_idx.Tail</c> read that a
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
	///   <c>[head, available)</c> with <see cref="TryTake" /> (copy) / <see cref="HandleScope{T}" /> + deferred
	///   release (value) under the batched head publish, then re-claims — one <c>_idx.Tail</c> read per run instead of per item, matching
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
	///   <see langword="true" /> = "woke" — the caller re-checks via <see cref="TryClaimRunNow" /> on
	///   the next loop turn, after <see cref="AckWake" />). Deliberately NOT an <c>async</c> method, so
	///   an empty wait allocates nothing — the suspension reuses the caller's single (one-time)
	///   RunLoopAsync state machine. Hard stop is checked first, so queued items are dropped on stop.
	///   Single logical consumer only.
	/// </summary>
	internal ValueTask<bool> WaitForItemAsync() {
		if (_stop) return new ValueTask<bool>(false);
		if (_idx.Head != Volatile.Read(ref _idx.Tail)) return new ValueTask<bool>(true);
		if (_completed) return DrainedOnComplete();

		// Raise the parked flag BEFORE arming, then recheck. CommitTail's fenced flag read pairs with
		// this: either the producer sees the flag (and Sets), or this recheck — ordered after the flag
		// write by WaitAsync's Interlocked CAS (a full fence) — sees the fresh Tail and self-signals so
		// the loop drains instead of parking on a skipped edge. Also catches a late stop/complete. The
		// flag is cleared by the loop via AckWake once the wait completes, so it stays up while parked.
		Volatile.Write(ref _idx.ConsumerParked, 1);
		var vt = _itemReadyAsync!.WaitAsync();
		if (vt.IsCompleted) return vt; // a Set was latched between the empty check above and the arm
		if (_stop || _completed || _idx.Head != Volatile.Read(ref _idx.Tail))
			_itemReadyAsync.Set(); // self-signal: complete the armed wait so the loop re-evaluates
		return vt;
	}

	/// <summary>
	///   Clear the async consumer's parked flag — called by the async loops once per wake, before
	///   draining, so producers stop paying the (cheap, latching) spurious Set per commit while the
	///   consumer is running. Clearing an already-0 flag is a harmless consumer-owned-line write.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void AckWake() => Volatile.Write(ref _idx.ConsumerParked, 0);

	// Soft-complete must drain everything already committed. _completed is published (release) AFTER the
	// producer's final commit, so re-reading the ring after observing _completed (acquire) sees the final
	// _idx.Tail — without this re-read a (possibly stale) empty check could abandon items committed right
	// before completion. Returns true (an item is available → drain it) or false (drained → exit).
	private ValueTask<bool> DrainedOnComplete() =>
		_idx.Head != Volatile.Read(ref _idx.Tail) ? new ValueTask<bool>(true) : new ValueTask<bool>(false);

	// Cold path: producer parks until space frees, or the worker stops/completes.
	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool WaitForSpaceSlow() {
		// Parked flag mirrors WaitForItemSlow: raised for the whole slow path, read by AdvanceHead
		// instead of a per-item _idx.Tail read. Spurious Sets while still spinning are harmless.
		Volatile.Write(ref _idx.ProducerParked, 1);
		try {
			var spins = 0;
			int head;
			while (_idx.Tail - (head = Volatile.Read(ref _idx.Head)) == _buffer.Length) {
				if (_stop || _completed) return false;
				if (spins++ < SpinThreshold) {
					Thread.SpinWait(10);
					continue;
				}

				// About to kernel-park: full fence + final recheck (Dekker pairing with AdvanceHead's
				// Head-write→flag-read), then park. See WaitForItemSlow.
				Interlocked.MemoryBarrier();
				if (_idx.Tail - Volatile.Read(ref _idx.Head) != _buffer.Length) continue;
				_spaceEvt.Wait();
				_spaceEvt.Reset();
				spins = 0;
			}

			// Space freed — but re-check lifecycle even when a slot is now available: a producer woken by
			// TryComplete/SignalStop (or one that raced them to here) must refuse the reserve so a dispatch
			// racing soft-complete is rejected, never committed into a ring whose consumer has exited.
			if (_stop || _completed) return false;
			_cachedHead = head; // refresh the gating cache with the Head that proved there is space
			return true;
		}
		finally {
			Volatile.Write(ref _idx.ProducerParked, 0);
		}
	}

	// Cold path: consumer parks until an item arrives, or the worker stops/completes-drained.
	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool WaitForItemSlow() {
		// Raise the parked flag for the WHOLE slow path (spin phase included): CommitTail may then fire
		// a spurious Set while we're still spinning — harmless (latching event) — but a commit can never
		// find the flag down while we end up parked. Cleared on every exit.
		Volatile.Write(ref _idx.ConsumerParked, 1);
		try {
			var spins = 0;
			while (_idx.Head == Volatile.Read(ref _idx.Tail)) {
				if (_stop) return false;
				// Soft-complete drains: re-read _idx.Tail after observing _completed (acquire) so an item
				// committed right before completion — not yet visible to the empty check above — is still
				// drained.
				if (_completed) return _idx.Head != Volatile.Read(ref _idx.Tail);
				if (spins++ < SpinThreshold) {
					Thread.SpinWait(10);
					continue;
				}

				// About to kernel-park. One full fence per park (cold): guarantees the flag write above is
				// globally visible before this final emptiness recheck — the Dekker pairing with
				// CommitTail's Tail-write→flag-read. If an item slipped in, drain it instead of parking.
				Interlocked.MemoryBarrier();
				if (_idx.Head != Volatile.Read(ref _idx.Tail)) return true;
				_itemEvt.Wait();
				_itemEvt.Reset();
				spins = 0;
			}

			return true;
		}
		finally {
			Volatile.Write(ref _idx.ConsumerParked, 0);
		}
	}
}

/// <summary>
///   Cache-line-separated SPSC ring indices + parked-waiter flags. Non-generic on purpose — generic
///   types can't have explicit layout. Each field sits on its own 128-byte stride (one full cache
///   line on Apple Silicon, two on x64) so no two of them — nor any neighboring class field — share
///   a line: <see cref="Head" /> is written per item by the consumer, <see cref="Tail" /> per item
///   by the producer, and the parked flags are written only on slow-path entry/exit but read per
///   item by the opposite side (see SIGNALING INVARIANT).
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = 640)]
internal struct PaddedIndices {
	[FieldOffset(128)] public int Head;
	[FieldOffset(256)] public int Tail;
	[FieldOffset(384)] public int ConsumerParked; // 1 while the consumer is in WaitForItemSlow / armed via WaitForItemAsync
	[FieldOffset(512)] public int ProducerParked; // 1 while the producer is in WaitForSpaceSlow
}
