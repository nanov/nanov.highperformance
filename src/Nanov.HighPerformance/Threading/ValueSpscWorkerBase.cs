namespace Nanov.HighPerformance.Threading;

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
///   Shared machinery for <see cref="ValueSpscWorker{T, THandler}" /> (sync) and
///   <see cref="AsyncValueSpscWorker{T, THandler}" /> (async): the ring buffer, the sync
///   primitives, the dedicated worker thread, and the scope-based <see cref="Publish" />.
///   The per-item dispatch loop lives in each subclass's <see cref="RunLoop" /> (which calls its
///   <see langword="struct" /> handler directly, devirtualized — no virtual dispatch per item);
///   the base only exposes the non-virtual <see cref="TryDequeue" /> / completion helpers it uses.
///   The base is intentionally not generic in the handler type so the public
///   <see cref="PublishScope{T}" /> / <see cref="ConsumeScope{T}" /> stay free of the handler
///   parameter.
///   <para>
///     SIGNALING INVARIANT (strict SPSC). Exactly one producer thread calls <see cref="Publish" />
///     (and disposes the returned <see cref="PublishScope{T}" />); the single dedicated worker thread is
///     the only consumer and the only writer of <c>_head</c> — note <see cref="ConsumeScope{T}.Release" />
///     (which advances <c>_head</c>) is always called on the worker thread, synchronously, before the
///     async subclass returns its <c>ValueTask</c>, NOT on a ThreadPool continuation. The wake events use
///     edge-triggered <c>Set()</c> (only on the empty→non-empty / full→non-full transition) paired with a
///     non-atomic <c>Wait();Reset()</c>. This is correct ONLY under SPSC: while the consumer is parked,
///     <c>_head</c> does not advance, so only the first item past <c>_head</c> satisfies
///     <c>_head == _tail</c> and fires the lone <c>Set</c> — no second signal a <c>Reset()</c> could lose.
///     <see cref="ManualResetEventSlim" /> is latching (order-insensitive), and the
///     <c>while (_head == Volatile.Read(ref _tail))</c> recheck acquire-reads <c>_tail</c> published with
///     release before the <c>Set</c>. Shutdown rides the separate <c>volatile _stop</c>, re-checked before
///     every park. WARNING: a second producer, or advancing <c>_head</c> off the worker thread (e.g. a
///     handler releasing from a ThreadPool continuation), breaks this and hangs silently.
///   </para>
/// </summary>
public abstract class ValueSpscWorkerBase<T> : IDisposable, IWorkerQueue where T : struct {
	private protected const int SpinThreshold = 200;

	internal readonly T[] _buffer;
	private readonly TaskCompletionSource _completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
	internal readonly ManualResetEventSlim _itemEvt = new(false);
	internal readonly int _mask;
	internal readonly ManualResetEventSlim _spaceEvt = new(false);
	private readonly Thread _thread;
	private volatile bool _completed; // soft stop (Complete) — drain remaining
	private Exception? _completeException;
	private CancellationToken _ct;
	private CancellationTokenRegistration _ctRegistration;
	internal int _head; // consumer index (worker thread)
	private volatile bool _stop; // hard stop (Dispose / cancel) — drop remaining
	internal int _tail; // producer index (dispatching thread)

	protected ValueSpscWorkerBase(int capacity, string? threadName) {
		capacity = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(capacity, 1));
		_buffer = new T[capacity];
		_mask = capacity - 1;
		_thread = new Thread(RunLoop) {
			IsBackground = true,
			Name = threadName ?? $"ValueSpscWorker<{typeof(T).Name}>"
		};
	}

	public Task Completion => _completionTcs.Task;

	/// <summary>
	///   Approximate number of items currently queued (Tail − Head). Plain reads — int loads are
	///   atomic on .NET and the gauge already accepts approximation; freshness within a few items
	///   doesn't matter. Reusing the ring's existing indices means zero hot-path cost.
	/// </summary>
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

	public void Start(CancellationToken ct = default) {
		_ct = ct;
		if (ct.CanBeCanceled)
			_ctRegistration = ct.Register(static s => ((ValueSpscWorkerBase<T>)s!).OnCancel(), this);
		_thread.Start();
	}

	private void OnCancel() {
		_stop = true;
		_itemEvt.Set();
		_spaceEvt.Set();
	}

	/// <summary>
	///   Claim the next slot for writing. Blocks if the ring is full. The returned
	///   <see cref="PublishScope{T}" /> exposes a <see langword="ref" /> to the slot through
	///   <see cref="PublishScope{T}.Event" />; disposing the scope publishes the slot and
	///   signals the consumer.
	///   <para>
	///     If the worker is Completed or Disposed (either before the call or while waiting
	///     for space), the returned scope is "closed" — <see cref="PublishScope{T}.IsOpen" />
	///     is false, <see cref="PublishScope{T}.Event" /> throws, and Dispose is a no-op.
	///   </para>
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public PublishScope<T> Publish() {
		if (_completed || _stop) return default;
		var spins = 0;
		while (_tail - Volatile.Read(ref _head) == _buffer.Length) {
			if (_stop || _completed) return default;
			if (spins++ < SpinThreshold) {
				Thread.SpinWait(10);
				continue;
			}

			_spaceEvt.Wait();
			_spaceEvt.Reset();
			spins = 0;
		}

		return new PublishScope<T>(this, _tail);
	}

	public bool TryComplete(Exception? exception = null) {
		if (_completed) return false;
		_completeException = exception;
		_completed = true;
		_itemEvt.Set();
		return true;
	}

	/// <summary>
	///   The dedicated worker thread's entry point. Each subclass drains the ring here, invoking its
	///   struct handler per slot, and on exit calls the handler's completion hook and resolves
	///   <see cref="Completion" /> via <see cref="SetCompletion" />.
	/// </summary>
	protected abstract void RunLoop();

	/// <summary>
	///   Wait for and claim the next slot. Returns false (and a closed-ish default scope) when the
	///   worker should exit — hard stop, or soft complete with the ring drained. Otherwise blocks
	///   (spin then park) until an item is available and returns its <see cref="ConsumeScope{T}" />.
	///   Non-virtual — called from the subclass loop, no per-item virtual dispatch.
	/// </summary>
	protected bool TryDequeue(out ConsumeScope<T> scope) {
		var spins = 0;
		while (_head == Volatile.Read(ref _tail)) {
			if (_stop || _completed) {
				scope = default;
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

		scope = new ConsumeScope<T>(this, _head);
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

/// <summary>
///   In-flight publish handle for a <see cref="ValueSpscWorkerBase{T}" />. Non-<see cref="IDisposable" />
///   on purpose — the C# pattern-based <c>using</c> picks up the public <see cref="Dispose" />
///   by name. Don't stash, don't return, don't escape — the scope must fall out of its
///   <c>using</c> block in the same stack frame it was claimed in, or the slot will never
///   publish.
///   <para>
///     <c>default(PublishScope&lt;T&gt;)</c> represents the "closed" scope returned from
///     <see cref="ValueSpscWorkerBase{T}.Publish" /> when the worker is no longer accepting.
///   </para>
/// </summary>
public readonly ref struct PublishScope<T> where T : struct {
	private readonly ValueSpscWorkerBase<T>? _worker;
	private readonly int _tail;

	internal PublishScope(ValueSpscWorkerBase<T> worker, int tail) {
		_worker = worker;
		_tail = tail;
	}

	/// <summary>False for the "closed" scope returned when the worker rejected the claim.</summary>
	public bool IsOpen => _worker is not null;

	/// <summary>Writable ref to the claimed slot. Throws if the scope is closed.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ref T Event() {
		if (_worker is null)
			throw new InvalidOperationException("Publish scope is closed; the worker rejected the claim.");
		return ref _worker._buffer[_tail & _worker._mask];
	}

	/// <summary>Publish the slot and signal the consumer. No-op on a closed scope.</summary>
	public void Dispose() {
		if (_worker is null) return;
		Volatile.Write(ref _worker._tail, _tail + 1);

		// Edge-triggered: signal only on the empty→non-empty transition. Safe under SPSC — see the
		// SIGNALING INVARIANT on ValueSpscWorkerBase. The lone Set cannot be lost (latching event +
		// the consumer's post-Reset recheck re-reads _tail).
		if (Volatile.Read(ref _worker._head) == _tail)
			_worker._itemEvt.Set();
	}
}

/// <summary>
///   In-flight consume handle passed to the handler (sync or async). Gives ref access to the slot
///   via <see cref="Event" /> and lets the handler release the slot early via <see cref="Release" />
///   so the producer can reuse it while the handler is still running. If the handler doesn't release,
///   the worker loop's <c>finally</c> does as a safety net. <see cref="Release" /> is idempotent.
/// </summary>
public ref struct ConsumeScope<T> where T : struct {
	private readonly ValueSpscWorkerBase<T> _worker;
	private readonly int _head;

	internal ConsumeScope(ValueSpscWorkerBase<T> worker, int head) {
		_worker = worker;
		_head = head;
		IsReleased = false;
	}

	/// <summary>True once the slot has been released (by the handler or the loop).</summary>
	public bool IsReleased { get; private set; }

	/// <summary>
	///   Ref to the slot. Call before <see cref="Release" />; after Release, the slot may
	///   be overwritten by the producer at any time.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public readonly ref T Event() {
		if (IsReleased)
			throw new InvalidOperationException("ConsumeScope already released; the slot may have been reused.");
		return ref _worker._buffer[_head & _worker._mask];
	}

	/// <summary>
	///   Release the slot — advance <c>_head</c> and wake a producer waiting for space.
	///   After this call the slot's contents are not safe to read. Idempotent.
	/// </summary>
	public void Release() {
		if (IsReleased) return;
		IsReleased = true;
		ref var slot = ref _worker._buffer[_head & _worker._mask];
		slot = default;
		Volatile.Write(ref _worker._head, _head + 1);

		// Edge-triggered space signal, mirror of PublishScope.Dispose's item signal: wake a blocked
		// producer only on the full→non-full transition. Safe under SPSC (single producer; this runs on
		// the worker thread) — see the SIGNALING INVARIANT on ValueSpscWorkerBase.
		if (Volatile.Read(ref _worker._tail) - _head == _worker._buffer.Length)
			_worker._spaceEvt.Set();
	}
}
