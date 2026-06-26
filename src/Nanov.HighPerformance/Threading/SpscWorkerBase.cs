namespace Nanov.HighPerformance.Threading;

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
///   Shared machinery for <see cref="SpscWorker{T, THandler}" /> (sync) and
///   <see cref="AsyncSpscWorker{T, THandler}" /> (async): the ring buffer, the sync primitives, the
///   dedicated worker thread, the copy-based <see cref="Dispatch" />, and the lifecycle plumbing.
///   The per-item dispatch loop lives in each subclass's <see cref="RunLoop" /> (which calls its
///   <see langword="struct" /> handler directly, devirtualized — no virtual dispatch per item); the
///   base only exposes the non-virtual <see cref="TryDequeue" /> / completion helpers it uses. The
///   value-type twin is <see cref="ValueSpscWorkerBase{T}" />.
///   <para>
///     SIGNALING INVARIANT (strict SPSC). Exactly one producer thread calls <see cref="Dispatch" />;
///     the single dedicated worker thread is the only consumer and the only writer of <c>_head</c>.
///     The wake events use edge-triggered <c>Set()</c> (only on the empty→non-empty / full→non-full
///     transition) paired with a non-atomic <c>Wait();Reset()</c>. This is correct ONLY under SPSC:
///     while the consumer is parked, <c>_head</c> does not advance, so only the first item past
///     <c>_head</c> satisfies <c>_head == currentTail</c> and fires the lone <c>Set</c> — there is no
///     second signal a <c>Reset()</c> could lose. <see cref="ManualResetEventSlim" /> is latching, so
///     <c>Set</c>/<c>Wait</c> ordering is irrelevant, and the enclosing
///     <c>while (_head == Volatile.Read(ref _tail))</c> recheck acquire-reads <c>_tail</c> (published
///     with release before the <c>Set</c>), so any signalled item is observed before re-parking.
///     Shutdown rides the separate <c>volatile _stop</c>, re-checked before every park. WARNING: a
///     second producer, or advancing <c>_head</c> off the worker thread, breaks this and hangs silently.
///   </para>
/// </summary>
public abstract class SpscWorkerBase<T> : IDisposable, ISpscWorkerQueue {
	/// <summary>
	///   SpinWait iterations before falling back to manual reset event wait.
	///   Reduces syscall overhead for bursty traffic at the cost of CPU during idle.
	/// </summary>
	private protected const int SpinThreshold = 200;

	private readonly T[] _buffer;
	private readonly TaskCompletionSource _completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
	private readonly ManualResetEventSlim _itemEvt = new(false); // set when items available
	private readonly int _mask;
	private readonly ManualResetEventSlim _spaceEvt = new(false); // set when space freed
	private readonly Thread _thread;
	private volatile bool _completed; // soft stop (Complete) — drain remaining
	private Exception? _completeException;
	private CancellationToken _ct;
	private CancellationTokenRegistration _ctRegistration;
	private int _head; // consumer index (worker thread)
	private volatile bool _stop; // hard stop (Dispose / cancel) — drop remaining
	private int _tail; // producer index (dispatching thread)

	protected SpscWorkerBase(int capacity, string? threadName) {
		capacity = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(capacity, 1));
		_buffer = new T[capacity];
		_mask = capacity - 1;
		_thread = new Thread(RunLoop) {
			IsBackground = true,
			Name = threadName ?? $"SpscWorker<{typeof(T).Name}>"
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
			_ctRegistration = ct.Register(static s => ((SpscWorkerBase<T>)s!).OnCancel(), this);
		_thread.Start();
	}

	private void OnCancel() {
		_stop = true;
		_itemEvt.Set();
		_spaceEvt.Set();
	}

	/// <summary>
	///   Enqueue an item. Returns false if the worker has been completed or disposed
	///   (item is not enqueued). Returns true if successfully enqueued.
	///   Blocks if the buffer is full.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool Dispatch(T item) {
		if (_completed || _stop) return false;
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

		var currentTail = _tail;
		_buffer[currentTail & _mask] = item;
		Volatile.Write(ref _tail, currentTail + 1);

		// Edge-triggered: signal only on the empty→non-empty transition. Safe under SPSC — see the
		// class-level SIGNALING INVARIANT. This lone Set cannot be lost (latching event + the consumer's
		// post-Reset recheck re-reads _tail).
		if (Volatile.Read(ref _head) == currentTail)
			_itemEvt.Set();
		return true;
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
	///   The dedicated worker thread's entry point. Each subclass drains the ring here, invoking its
	///   struct handler per item, and on exit calls the handler's completion hook and resolves
	///   <see cref="Completion" /> via <see cref="SetCompletion" />.
	/// </summary>
	protected abstract void RunLoop();

	/// <summary>
	///   Wait for and dequeue the next item (copy it out, advance <c>_head</c>, wake a blocked
	///   producer). Returns false when the worker should exit — hard stop, or soft complete with the
	///   ring drained. Non-virtual — called from the subclass loop, no per-item virtual dispatch.
	/// </summary>
	protected bool TryDequeue(out T item) {
		var spins = 0;
		while (_head == Volatile.Read(ref _tail)) {
			if (_stop || _completed) {
				item = default!;
				return false;
			}

			if (spins++ < SpinThreshold) {
				Thread.SpinWait(10);
				continue;
			}

			_itemEvt.Wait();
			_itemEvt.Reset();
			spins = 0;
		}

		// Hard stop drops items still in the ring (mirrors the original outer `while (!_stop)` guard).
		// Soft complete (_completed) is NOT checked here — it drains what's queued, exiting above once empty.
		if (_stop) {
			item = default!;
			return false;
		}

		ref var slot = ref _buffer[_head & _mask];
		item = slot;
		slot = default!;

		var currentHead = _head;
		Volatile.Write(ref _head, currentHead + 1);

		// Edge-triggered space signal, mirror of Dispatch's item signal: wake a blocked producer
		// only on the full→non-full transition. Safe under SPSC (single producer) — see the
		// class-level SIGNALING INVARIANT.
		if (Volatile.Read(ref _tail) - currentHead == _buffer.Length)
			_spaceEvt.Set();
		return true;
	}

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
}
