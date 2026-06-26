namespace Nanov.HighPerformance.Threading;

using System;
using System.Runtime.CompilerServices;

/// <summary>
///   Zero-copy, scope-based access layer over <see cref="SpscRingBase{T}" />, shared by
///   <see cref="ValueSpscWorker{T, THandler}" /> (sync) and
///   <see cref="AsyncValueSpscWorker{T, THandler}" /> (async). In the Disruptor-net
///   <c>ValueDisruptor</c> style, producers write <b>directly into the ring slot</b> via
///   <see cref="Publish" /> (no struct copy at the call site) and the handler consumes through a
///   <see cref="HandleScope{T}" /> by ref. The copy-based twin is <see cref="SpscWorkerBase{T}" />.
///   <para>
///     <see cref="HandleScope{T}.Release" /> (which advances the ring) is always called on the worker
///     thread, synchronously, before an async handler returns its <c>ValueTask</c> — NOT on a
///     ThreadPool continuation. Advancing the head off the worker thread would break the SPSC
///     SIGNALING INVARIANT (see <see cref="SpscRingBase{T}" />) and hang silently.
///   </para>
/// </summary>
public abstract class ValueSpscWorkerBase<T> : SpscRingBase<T> where T : struct {
	private protected ValueSpscWorkerBase(int capacity, string? threadName, string defaultThreadName)
		: base(capacity, threadName, defaultThreadName) {
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
		if (!TryReserve()) return default;
		return new PublishScope<T>(this, _tail);
	}

	/// <summary>
	///   Wait for and claim the next slot. Returns false when the worker should exit — hard stop, or
	///   soft complete with the ring drained. Otherwise returns the slot's <see cref="HandleScope{T}" />.
	///   Non-virtual — called from the leaf loop, no per-item virtual dispatch.
	/// </summary>
	protected bool TryDequeue(out HandleScope<T> scope) {
		if (!TryClaim()) {
			scope = default;
			return false;
		}

		scope = new HandleScope<T>(this, _head);
		return true;
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
		_worker.CommitTail(_tail);
	}
}

/// <summary>
///   In-flight consume handle passed to the handler (sync or async). Gives ref access to the slot
///   via <see cref="Event" /> and lets the handler release the slot early via <see cref="Release" />
///   so the producer can reuse it while the handler is still running. If the handler doesn't release,
///   the worker loop's <c>finally</c> does as a safety net. <see cref="Release" /> is idempotent.
/// </summary>
public ref struct HandleScope<T> where T : struct {
	private readonly ValueSpscWorkerBase<T> _worker;
	private readonly int _head;

	internal HandleScope(ValueSpscWorkerBase<T> worker, int head) {
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
			throw new InvalidOperationException("HandleScope already released; the slot may have been reused.");
		return ref _worker._buffer[_head & _worker._mask];
	}

	/// <summary>
	///   Release the slot — clear it, advance the ring, and wake a producer waiting for space.
	///   After this call the slot's contents are not safe to read. Idempotent.
	/// </summary>
	public void Release() {
		if (IsReleased) return;
		IsReleased = true;
		_worker._buffer[_head & _worker._mask] = default;
		_worker.AdvanceHead(_head);
	}
}
