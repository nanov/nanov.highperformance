namespace Nanov.HighPerformance.Threading.Workers;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
///   Synchronous, copy-based handler for <see cref="SpWorker{T, THandler}" />. Implement as a
///   <see langword="struct" /> — the worker stores it by value and dispatches through a constrained
///   generic call, so the JIT devirtualizes and inlines <see cref="Handle" /> per closed worker type
///   (no virtual dispatch on the hot path). A stateless handler is an empty <c>readonly struct</c>;
///   a stateful one keeps its mutable state in fields (mutations persist — calls go through the
///   worker's handler field) or behind a reference it captures.
/// </summary>
public interface IWorkerHandler<in T> {
	/// <summary>Process one item. Invoked in FIFO order on the worker thread.</summary>
	void Handle(T item);

	/// <summary>
	///   Invoked when <see cref="Handle" /> throws. Always synchronous. The worker swallows the
	///   exception after this returns and continues with the next item.
	/// </summary>
	void OnError(Exception error);

	/// <summary>
	///   Invoked once after the worker has drained and is about to exit. <paramref name="error" />
	///   is <see langword="null" /> for normal completion, an <see cref="OperationCanceledException" />
	///   for cancellation, or the exception passed to <c>TryComplete</c>. Use it to propagate
	///   completion to a downstream stage in a pipeline.
	/// </summary>
	void OnCompleted(Exception? error);
}

/// <summary>
///   Asynchronous, copy-based handler for <see cref="AsyncSpWorker{T, THandler}" />. Same
///   devirtualized struct-handler contract as <see cref="IWorkerHandler{T}" />, but
///   <see cref="HandleAsync" /> returns a <see cref="ValueTask" /> for in-flight async work.
///   <see cref="OnError" /> stays synchronous.
/// </summary>
public interface IAsyncWorkerHandler<in T> {
	/// <summary>
	///   Process one item. Invoked in FIFO order on the worker thread. The
	///   <paramref name="cancellationToken" /> is the one passed to <c>Start</c> — forward it to
	///   async IO so a hard stop aborts in-flight work. Sync-completing <see cref="ValueTask" />s
	///   incur no overhead; truly async ones block the dedicated thread during their internal awaits.
	/// </summary>
	ValueTask HandleAsync(T item, CancellationToken cancellationToken);

	/// <summary>Invoked synchronously when <see cref="HandleAsync" /> faults. See <see cref="IWorkerHandler{T}.OnError" />.</summary>
	void OnError(Exception error);

	/// <summary>Drain/shutdown hook. See <see cref="IWorkerHandler{T}.OnCompleted" />.</summary>
	ValueTask OnCompletedAsync(Exception? error);
}

/// <summary>
///   Synchronous, zero-copy scope-based handler for <see cref="ValueSpWorker{T, THandler}" />.
///   Receives a <see cref="HandleScope{T}" /> by ref: read the slot via
///   <see cref="HandleScope{T}.Event" />, optionally call <see cref="HandleScope{T}.Release" /> to
///   free it for the producer early, then do the rest of the work. If you don't release, the worker
///   loop auto-releases when <see cref="Handle" /> returns. Implement as a <see langword="struct" />
///   for the same devirtualization benefit as <see cref="IWorkerHandler{T}" />.
/// </summary>
public interface IValueWorkerHandler<T> where T : struct {
	/// <summary>Process one slot. Invoked in FIFO order on the worker thread.</summary>
	void Handle(ref HandleScope<T> scope);

	/// <summary>
	///   Reset a slot when it is released — invoked at the moment of release, whether the handler
	///   released early via <see cref="HandleScope{T}.Release" /> (e.g. to free the slot for the
	///   producer while a long task runs) or the worker loop auto-released it. Null out the slot's
	///   reference fields (so the ring doesn't pin them) or return pooled buffers, touching only what
	///   matters — the zero-copy ring never writes a full <c>default(T)</c> back. Always runs for the
	///   value worker; keep it allocation-free. A no-op body is fine for blittable <typeparamref name="T" />.
	/// </summary>
	void Clear(ref T value);

	/// <summary>Invoked synchronously when <see cref="Handle" /> throws. See <see cref="IWorkerHandler{T}.OnError" />.</summary>
	void OnError(Exception error);

	/// <summary>Drain/shutdown hook. See <see cref="IWorkerHandler{T}.OnCompleted" />.</summary>
	void OnCompleted(Exception? error);
}

/// <summary>
///   Asynchronous, zero-copy scope-based handler for <see cref="AsyncValueSpWorker{T, THandler}" />.
///   <para>
///     <see cref="HandleAsync" /> must NOT be declared <c>async</c> — a ref struct scope can't cross
///     an <c>await</c>. Do the sync slot read + <see cref="HandleScope{T}.Release" /> in the method
///     body, then return a <see cref="ValueTask" /> from a nested <c>async</c> helper:
///   </para>
///   <code>
/// public ValueTask HandleAsync(ref HandleScope&lt;T&gt; scope, CancellationToken ct) {
///     ref var slot = ref scope.Event();
///     var a = slot.A; var b = slot.B;
///     scope.Release();                 // producer can reuse the slot now
///     return DoWorkAsync(a, b, ct);    // actual async work lives here
/// }
///   </code>
/// </summary>
public interface IAsyncValueWorkerHandler<T> where T : struct {
	/// <summary>Process one slot. Invoked in FIFO order on the worker thread. Must not be <c>async</c> (see remarks).</summary>
	ValueTask HandleAsync(ref HandleScope<T> scope, CancellationToken cancellationToken);

	/// <summary>
	///   Reset a slot when it is released. See <see cref="IValueWorkerHandler{T}.Clear" /> — same
	///   contract: runs synchronously at the moment of release (typically the early
	///   <see cref="HandleScope{T}.Release" /> the handler does before returning its
	///   <see cref="ValueTask" />), nulls reference fields / returns pooled buffers, allocation-free.
	/// </summary>
	void Clear(ref T value);

	/// <summary>Invoked synchronously when <see cref="HandleAsync" /> faults. See <see cref="IWorkerHandler{T}.OnError" />.</summary>
	void OnError(Exception error);

	/// <summary>Drain/shutdown hook. See <see cref="IWorkerHandler{T}.OnCompleted" />.</summary>
	ValueTask OnCompletedAsync(Exception? error);
}
