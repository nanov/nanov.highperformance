namespace Nanov.HighPerformance.Threading;

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
///   Async variant of <see cref="SpscWorker{T, THandler}" />: <typeparamref name="THandler" />
///   implements <see cref="IAsyncSpscHandler{T}" /> whose <c>HandleAsync</c> returns a ValueTask.
///   Runs on a dedicated thread with synchronous waits for items (zero alloc).
///   For sync-completing HandleAsync, entirely synchronous (zero overhead).
///   For truly async HandleAsync, the dedicated thread blocks on the ValueTask via
///   the awaiter — handler's internal async continuations run on ThreadPool as normal.
///   No SynchronizationContext is set on the dedicated thread, so no deadlock risk from blocking.
///   Suitable for bounded worker pools (dozens of workers). Not suitable for thousands
///   of concurrent handlers — for that, use a ThreadPool-based async loop.
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
///     WARNING: a second producer, or advancing <c>_head</c> off the worker thread, breaks this and
///     hangs silently.
///   </para>
/// </summary>
public sealed class AsyncSpscWorker<T, THandler> : IDisposable, IWorkerQueue
	where THandler : struct, IAsyncSpscHandler<T> {
	private const int SpinThreshold = 200;

	private readonly T[] _buffer;
	private readonly TaskCompletionSource _completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
	private readonly ManualResetEventSlim _itemEvt = new(false);
	private readonly int _mask;
	private readonly Action _onProcessComplete;
	private readonly ManualResetEventSlim _processDoneEvt = new(false);
	private readonly ManualResetEventSlim _spaceEvt = new(false);
	private readonly Thread _thread;
	private THandler _handler; // mutable struct — calls go through this field so state persists
	private volatile bool _completed;
	private Exception? _completeException;
	private CancellationToken _ct;
	private CancellationTokenRegistration _ctRegistration;
	private int _head;
	private volatile bool _stop;
	private int _tail;

	public AsyncSpscWorker(THandler handler, int capacity, string? threadName = null) {
		_handler = handler;
		capacity = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(capacity, 1));
		_buffer = new T[capacity];
		_mask = capacity - 1;
		_onProcessComplete = () => _processDoneEvt.Set(); // cached delegate, one-time alloc
		_thread = new Thread(Loop) {
			IsBackground = true,
			Name = threadName ?? $"AsyncSpscWorker<{typeof(T).Name}>"
		};
	}

	/// <summary>Completes when the worker has drained and exited.</summary>
	public Task Completion => _completionTcs.Task;

	/// <summary>Approximate items currently queued (<c>Tail − Head</c>). Lock-free, racy by a few.</summary>
	public int QueuedApprox => _tail - _head;

	/// <summary>Ring capacity (rounded up to a power of two).</summary>
	public int Capacity => _buffer.Length;

	/// <summary>
	///   Start the worker thread. Optionally pass a CancellationToken — when cancelled,
	///   the worker stops immediately (drops pending items) and <see cref="Completion" />
	///   resolves with an <see cref="OperationCanceledException" />.
	/// </summary>
	public void Start(CancellationToken ct = default) {
		_ct = ct;
		if (ct.CanBeCanceled)
			_ctRegistration = ct.Register(static s => ((AsyncSpscWorker<T, THandler>)s!).OnCancel(), this);
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
		// class-level SIGNALING INVARIANT. The acquire read of _head pairs with the consumer's release
		// write in Loop, and the consumer's post-Reset recheck re-reads _tail, so this lone Set cannot
		// be lost.
		if (Volatile.Read(ref _head) == currentTail)
			_itemEvt.Set();
		return true;
	}

	/// <summary>
	///   Signal that no more items will be dispatched. The worker drains remaining items
	///   (processing each via HandleAsync) and then exits. Optionally pass an exception —
	///   if non-null, <see cref="Completion" /> resolves faulted with it.
	///   Returns true on first call, false if already completed.
	/// </summary>
	public bool TryComplete(Exception? exception = null) {
		if (_completed) return false;
		_completeException = exception;
		_completed = true;
		_itemEvt.Set();
		return true;
	}

	private void Loop() {
		try {
			while (!_stop) {
				var spins = 0;
				// Evaluate Volatile.Read(ref _tail) directly to see concurrent writes during spin-waits
				while (_head == Volatile.Read(ref _tail)) {
					if (_stop) return;
					if (_completed) return; // drained everything, producer done
					if (spins++ < SpinThreshold) {
						Thread.SpinWait(10);
						continue;
					}

					_itemEvt.Wait();
					_itemEvt.Reset();
					spins = 0;
				}

				ref var slot = ref _buffer[_head & _mask];
				var item = slot;
				slot = default!;

				var currentHead = _head;
				Volatile.Write(ref _head, currentHead + 1);

				// Edge-triggered space signal, mirror of Dispatch's item signal: wake a blocked producer
				// only on the full→non-full transition. Safe under SPSC (single producer) — see the
				// class-level SIGNALING INVARIANT.
				if (Volatile.Read(ref _tail) - currentHead == _buffer.Length)
					_spaceEvt.Set();

				try {
#pragma warning disable CA2012 // advanced ValueTask awaiting; all completion cases are handled
					var vt = _handler.HandleAsync(item, _ct);
					var awaiter = vt.GetAwaiter();
					if (awaiter.IsCompleted) {
						awaiter.GetResult();
					}
					else {
						// 1:1 rendezvous: exactly one continuation Set() per item, consumed by this Wait();
						// the Reset() restores the unsignaled precondition for the next item. No cross-item
						// signal leakage (the next continuation isn't registered until the next iteration).
						awaiter.UnsafeOnCompleted(_onProcessComplete);
						_processDoneEvt.Wait();
						_processDoneEvt.Reset();
						awaiter.GetResult();
#pragma warning restore CA2012
					}
				}
				catch (Exception ex) {
					_handler.OnError(ex);
				}
			}
		}
		finally {
			var ex = _completeException
			         ?? (_ct.IsCancellationRequested ? new OperationCanceledException(_ct) : null);
			try {
#pragma warning disable CA2012
				var vt = _handler.OnCompletedAsync(ex);
				var aw = vt.GetAwaiter();
				if (aw.IsCompleted) {
					aw.GetResult();
				}
				else {
					aw.UnsafeOnCompleted(_onProcessComplete);
					_processDoneEvt.Wait();
					_processDoneEvt.Reset();
					aw.GetResult();
				}
#pragma warning restore CA2012
			}
			catch (Exception onCompletedEx) {
				_handler.OnError(onCompletedEx);
			}

			if (ex is OperationCanceledException)
				_completionTcs.TrySetCanceled(_ct);
			else if (ex is not null)
				_completionTcs.TrySetException(ex);
			else
				_completionTcs.TrySetResult();
			_ctRegistration.Dispose();
		}
	}

	public void Dispose() {
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
		_processDoneEvt.Dispose();
		GC.SuppressFinalize(this);
	}
}
