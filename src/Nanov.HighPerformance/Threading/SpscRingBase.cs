namespace Nanov.HighPerformance.Threading;

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
///   Core single-producer / single-consumer ring buffer shared by the whole SPSC worker family: the
///   power-of-two ring, the edge-triggered wake events, the dedicated worker thread, the lifecycle
///   (<see cref="Start" />/<see cref="Dispose" />/<see cref="TryComplete" />/cancellation/completion),
///   and the producer/consumer wait + signal primitives. It is deliberately access-agnostic — the
///   copy-based family (<see cref="SpscWorkerBase{T}" />: <c>Dispatch</c> / <c>TryDequeue(out T)</c>)
///   and the zero-copy scope-based family (<see cref="ValueSpscWorkerBase{T}" />: <c>Publish</c> /
///   <c>TryDequeue(out HandleScope&lt;T&gt;)</c>) layer their own slot-access API on top.
///   <para>
///     The producer/consumer FAST paths (the empty/full index checks, slot write/read, and the
///     edge-triggered <c>Set()</c>) stay inlined in the leaf access methods via
///     <see cref="TryReserve" /> / <see cref="TryClaim" /> / <see cref="CommitTail" /> /
///     <see cref="AdvanceHead" />; only the COLD parking loops (<c>WaitForSpaceSlow</c> /
///     <c>WaitForItemSlow</c>) are out-of-line (<see cref="MethodImplOptions.NoInlining" />), keeping
///     the hot frame tight.
///   </para>
///   <para>
///     SIGNALING INVARIANT (strict SPSC). Exactly one producer thread reserves+commits slots; the
///     single dedicated worker thread is the only consumer and the only writer of <c>_head</c>. The
///     wake events use edge-triggered <c>Set()</c> (only on the empty→non-empty / full→non-full
///     transition) paired with a non-atomic <c>Wait();Reset()</c>. This is correct ONLY under SPSC:
///     while the consumer is parked, <c>_head</c> does not advance, so only the first item past
///     <c>_head</c> satisfies <c>_head == tail</c> and fires the lone <c>Set</c> — no second signal a
///     <c>Reset()</c> could lose. <see cref="ManualResetEventSlim" /> is latching (order-insensitive),
///     and the <c>while (_head == Volatile.Read(ref _tail))</c> recheck acquire-reads <c>_tail</c>
///     published with release before the <c>Set</c>. Shutdown rides the separate <c>volatile _stop</c>,
///     re-checked before every park. WARNING: a second producer, or advancing <c>_head</c> off the
///     worker thread, breaks this and hangs silently.
///   </para>
/// </summary>
public abstract class SpscRingBase<T> : IDisposable, ISpscWorkerQueue {
	/// <summary>
	///   SpinWait iterations before falling back to manual reset event wait.
	///   Reduces syscall overhead for bursty traffic at the cost of CPU during idle.
	/// </summary>
	private const int SpinThreshold = 200;

	internal readonly T[] _buffer;
	internal readonly int _mask;
	private readonly ManualResetEventSlim _itemEvt = new(false); // set when items available
	private readonly ManualResetEventSlim _spaceEvt = new(false); // set when space freed
	private readonly TaskCompletionSource _completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
	private readonly Thread _thread;
	private volatile bool _completed; // soft stop (Complete) — drain remaining
	private Exception? _completeException;
	private CancellationToken _ct;
	private CancellationTokenRegistration _ctRegistration;
	internal int _head; // consumer index (worker thread)
	private volatile bool _stop; // hard stop (Dispose / cancel) — drop remaining
	internal int _tail; // producer index (dispatching thread)

	private protected SpscRingBase(int capacity, string? threadName, string defaultThreadName) {
		capacity = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(capacity, 1));
		_buffer = new T[capacity];
		_mask = capacity - 1;
		_thread = new Thread(RunLoop) {
			IsBackground = true,
			Name = threadName ?? defaultThreadName
		};
	}

	/// <summary>Completes when the worker has drained and exited (after Complete() or Dispose()).</summary>
	public Task Completion => _completionTcs.Task;

	/// <summary>Approximate items currently queued (<c>Tail − Head</c>). Lock-free, racy by a few.</summary>
	public int QueuedApprox => _tail - _head;

	/// <summary>Ring capacity (rounded up to a power of two).</summary>
	public int Capacity => _buffer.Length;

	/// <summary>
	///   Token captured at <see cref="Start" /> time. Subclasses pass it to work that accepts
	///   cancellation so hard-stop requests abort in-progress work.
	/// </summary>
	protected CancellationToken CancellationToken => _ct;

	public virtual void Dispose() {
		if (_stop) return;
		_stop = true;
		// Wake up both sides so they observe _stop and exit.
		_itemEvt.Set();
		_spaceEvt.Set();
		try {
			_thread.Join(1000);
		}
		catch {
			/* ignore */
		}

		_itemEvt.Dispose();
		_spaceEvt.Dispose();
		GC.SuppressFinalize(this);
	}

	/// <summary>
	///   Start the worker thread. Optionally pass a CancellationToken — when cancelled,
	///   the worker stops immediately (drops pending items) and <see cref="Completion" />
	///   resolves with an <see cref="OperationCanceledException" />.
	/// </summary>
	public void Start(CancellationToken ct = default) {
		_ct = ct;
		if (ct.CanBeCanceled)
			_ctRegistration = ct.Register(static s => ((SpscRingBase<T>)s!).OnCancel(), this);
		_thread.Start();
	}

	private void OnCancel() {
		_stop = true;
		_itemEvt.Set();
		_spaceEvt.Set();
	}

	/// <summary>
	///   Signal that no more items will be dispatched. The worker drains remaining items
	///   and then exits. Optionally pass an exception — if non-null, <see cref="Completion" />
	///   resolves faulted with it; otherwise it resolves normally.
	///   Returns true on first call, false if already completed.
	/// </summary>
	public bool TryComplete(Exception? exception = null) {
		if (_completed) return false;
		_completeException = exception;
		_completed = true;
		_itemEvt.Set();
		return true;
	}

	/// <summary>
	///   The dedicated worker thread's entry point. Each leaf drains the ring here (claiming via
	///   <see cref="TryClaim" />, dispatching its struct handler per item), and on exit calls the
	///   handler's completion hook and resolves <see cref="Completion" /> via <see cref="SetCompletion" />.
	/// </summary>
	protected abstract void RunLoop();

	/// <summary>The exception to complete with: the explicit one, else cancellation, else null.</summary>
	protected Exception? CompletionException() =>
		_completeException ?? (_ct.IsCancellationRequested ? new OperationCanceledException(_ct) : null);

	/// <summary>Resolve <see cref="Completion" /> and tear down the cancellation registration. Call once, on exit.</summary>
	protected void SetCompletion(Exception? exception) {
		if (exception is OperationCanceledException)
			_completionTcs.TrySetCanceled(_ct);
		else if (exception is not null)
			_completionTcs.TrySetException(exception);
		else
			_completionTcs.TrySetResult();
		_ctRegistration.Dispose();
	}

	/// <summary>
	///   Producer reservation. Fast-path inline fullness check; parks on the cold path. Returns false
	///   if the worker is no longer accepting (completed/disposed). On true, the next slot to write is
	///   <c>_buffer[_tail &amp; _mask]</c>; the caller writes it then calls <see cref="CommitTail" />.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private protected bool TryReserve() {
		if (_completed || _stop) return false;
		if (_tail - Volatile.Read(ref _head) == _buffer.Length)
			return WaitForSpaceSlow();
		return true;
	}

	/// <summary>
	///   Consumer claim. Fast-path inline emptiness check; parks on the cold path. Returns false to
	///   exit the loop — hard stop (drops queued items) or soft complete with the ring drained. On
	///   true, the next item is at <c>_buffer[_head &amp; _mask]</c>.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private protected bool TryClaim() {
		if (_head == Volatile.Read(ref _tail) && !WaitForItemSlow())
			return false;
		// Hard stop drops items still in the ring; soft complete drains them (WaitForItemSlow exits
		// above only once the ring is empty).
		return !_stop;
	}

	/// <summary>Publish the slot at <paramref name="tail" /> and edge-signal the consumer.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void CommitTail(int tail) {
		Volatile.Write(ref _tail, tail + 1);
		// Edge-triggered: signal only on the empty→non-empty transition (latching event; the consumer's
		// post-Reset recheck re-reads _tail, so this lone Set cannot be lost). See SIGNALING INVARIANT.
		if (Volatile.Read(ref _head) == tail)
			_itemEvt.Set();
	}

	/// <summary>Release the slot at <paramref name="head" /> and edge-signal a blocked producer.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void AdvanceHead(int head) {
		Volatile.Write(ref _head, head + 1);
		// Edge-triggered space signal, mirror of CommitTail: wake a blocked producer only on the
		// full→non-full transition. Safe under SPSC (single producer). See SIGNALING INVARIANT.
		if (Volatile.Read(ref _tail) - head == _buffer.Length)
			_spaceEvt.Set();
	}

	// Cold path: producer parks until space frees, or the worker stops/completes.
	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool WaitForSpaceSlow() {
		var spins = 0;
		while (_tail - Volatile.Read(ref _head) == _buffer.Length) {
			if (_stop || _completed) return false;
			if (spins++ < SpinThreshold) {
				Thread.SpinWait(10);
				continue;
			}

			_spaceEvt.Wait();
			_spaceEvt.Reset();
			spins = 0;
		}

		return true;
	}

	// Cold path: consumer parks until an item arrives, or the worker stops/completes-drained.
	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool WaitForItemSlow() {
		var spins = 0;
		while (_head == Volatile.Read(ref _tail)) {
			if (_stop || _completed) return false;
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
