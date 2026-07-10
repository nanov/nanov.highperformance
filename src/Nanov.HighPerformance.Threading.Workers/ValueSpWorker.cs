namespace Nanov.HighPerformance.Threading.Workers;

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Nanov.HighPerformance.Threading.Workers.Internal;

/// <summary>
///   Value-type twin of <see cref="SpWorker{T, THandler}" /> in the Disruptor-net
///   <c>ValueDisruptor</c> style: producers write <b>directly into the ring slot</b> via
///   <see cref="Publish" /> (no struct copy at the call site).
///   <code>
/// using (var scope = worker.Publish()) {
///     if (!scope.IsOpen) return;            // worker completed/disposed
///     ref var slot = ref scope.Event();
///     slot.Field1 = ...;
///     slot.Field2 = ...;
/// }
///   </code>
///   Single-producer / single-consumer, zero per-message allocation. The dedicated worker thread
///   invokes <typeparamref name="THandler" />'s <c>Handle(ref HandleScope&lt;T&gt;)</c> per slot —
///   devirtualized (struct handler), no virtual dispatch per item.
/// </summary>
public sealed class ValueSpWorker<T, THandler> : IDisposable, IWorkerQueue
	where T : struct
	where THandler : struct, IValueWorkerHandler<T> {
	private readonly SpRingCore<T> _core;
	private THandler _handler; // mutable struct — calls go through this field so state persists

	public ValueSpWorker(THandler handler, int capacity, string? threadName = null) {
		_handler = handler;
		_core = new SpRingCore<T>(capacity, threadName ?? $"ValueSpWorker<{typeof(T).Name}>", RunLoop, MakeCleaner());
	}

	// Bridge for HandleScope.Release → SpRingCore.ClearSlot to reach this worker's strongly-typed handler
	// without the (THandler-erased) core/scope naming THandler. The trampoline is a static method on this
	// closed generic, so it can name THandler; the holder (this) is a GC-tracked object, so nothing is a
	// raw pointer into the heap. _handler.Clear runs in-place on the field (state persists, devirtualized).
	private unsafe SlotCleaner<T> MakeCleaner() => new(this, &ClearTrampoline);

	private static void ClearTrampoline(object holder, ref T slot) =>
		Unsafe.As<ValueSpWorker<T, THandler>>(holder)._handler.Clear(ref slot);

	/// <summary>Completes when the worker has drained and exited (after Complete() or Dispose()).</summary>
	public Task Completion => _core.Completion;

	/// <summary>Approximate items currently queued (<c>Tail − Head</c>). Lock-free, racy by a few.</summary>
	public int QueuedApprox => _core.QueuedApprox;

	/// <summary>Ring capacity (rounded up to a power of two).</summary>
	public int Capacity => _core.Capacity;


	/// <summary>
	///   Start the worker thread. Optionally pass a CancellationToken — when cancelled, the worker
	///   stops immediately (drops pending items) and <see cref="Completion" /> resolves with an
	///   <see cref="OperationCanceledException" />.
	/// </summary>
	public void Start(CancellationToken ct = default) => _core.Start(ct);

	/// <summary>
	///   Signal that no more items will be dispatched. The worker drains remaining items and then
	///   exits. Optionally pass an exception — if non-null, <see cref="Completion" /> resolves faulted
	///   with it. Returns true on first call, false if already completed.
	/// </summary>
	public bool TryComplete(Exception? exception = null) => _core.TryComplete(exception);

	public void Dispose() => _core.Dispose();

	/// <summary>
	///   Claim the next slot for writing. Blocks if the ring is full. The returned
	///   <see cref="PublishScope{T}" /> exposes a <see langword="ref" /> to the slot through
	///   <see cref="PublishScope{T}.Event" />; disposing the scope publishes the slot and signals the
	///   consumer. If the worker is Completed or Disposed, the returned scope is "closed"
	///   (<see cref="PublishScope{T}.IsOpen" /> is false).
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public PublishScope<T> Publish() {
		_core.DebugBeginProduce();
		if (!_core.TryReserve(out var tail)) {
			_core.DebugEndProduce();
			return default;
		}

		// The producer transaction spans until the returned scope is disposed (which commits and ends it).
		return new PublishScope<T>(_core, tail);
	}

	private void RunLoop() {
		try {
			// Claim the whole available run, drain it scope-by-scope with batched Head publishes. The
			// pre-handler ShouldFlush check publishes PRIOR items only (head = this item's index — its
			// slot is still owned by the scope), so a producer parked during the previous handler never
			// also waits out this one. A handler that Releases early publishes immediately through the
			// scope (Head is absolute, covering any deferred predecessors) — the batch restarts empty.
			// Auto-releases defer via ReleaseDeferred (clear now, publish with the batch). Run-boundary
			// flush before every re-claim/park: empty ring ⇒ all published — no deadlock.
			while (_core.TryClaimRun(out var head, out var available)) {
				var pending = 0;
				do {
					if (_core.ShouldFlush(pending)) {
						_core.PublishHead(head);
						pending = 0;
					}

					var scope = new HandleScope<T>(_core, head);
					try {
						_handler.Handle(ref scope);
					}
					catch (Exception ex) {
						_handler.OnError(ex);
					}
					finally {
						if (scope.IsReleased) {
							pending = 0; // early release already published past this item
						} else {
							scope.ReleaseDeferred();
							pending++;
						}
					}
				}
				while (!_core.IsStopped && ++head != available);

				if (pending > 0) _core.PublishHead(head);
			}
		}
		finally {
			var ex = _core.CompletionException();
			try {
				_handler.OnCompleted(ex);
			}
			catch (Exception onCompletedEx) {
				_handler.OnError(onCompletedEx);
			}

			_core.SetCompletion(ex);
		}
	}
}
