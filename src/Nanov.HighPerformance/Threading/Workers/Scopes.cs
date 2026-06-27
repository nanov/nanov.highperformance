namespace Nanov.HighPerformance.Threading.Workers;

using System;
using System.Runtime.CompilerServices;
using Nanov.HighPerformance.Threading.Workers.Internal;

/// <summary>
///   In-flight publish handle for a value-type SPSC worker (returned by <c>Publish()</c> on
///   <see cref="ValueSpWorker{T, THandler}" /> / <see cref="AsyncValueSpWorker{T, THandler}" />).
///   Non-<see cref="IDisposable" /> on purpose — the C# pattern-based <c>using</c> picks up the public
///   <see cref="Dispose" /> by name. Don't stash, don't return, don't escape — the scope must fall out
///   of its <c>using</c> block in the same stack frame it was claimed in, or the slot will never
///   publish.
///   <para>
///     <c>default(PublishScope&lt;T&gt;)</c> represents the "closed" scope returned when the worker is
///     no longer accepting.
///   </para>
/// </summary>
public readonly ref struct PublishScope<T> where T : struct {
	private readonly SpRingCore<T>? _core;
	private readonly int _tail;

	internal PublishScope(SpRingCore<T> core, int tail) {
		_core = core;
		_tail = tail;
	}

	/// <summary>False for the "closed" scope returned when the worker rejected the claim.</summary>
	public bool IsOpen => _core is not null;

	/// <summary>Writable ref to the claimed slot. Throws if the scope is closed.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ref T Event() {
		if (_core is null)
			throw new InvalidOperationException("Publish scope is closed; the worker rejected the claim.");
		return ref _core.GetSlotRef(_tail);
	}

	/// <summary>Publish the slot and signal the consumer. No-op on a closed scope.</summary>
	public void Dispose() {
		if (_core is null) return;
		_core.CommitTail(_tail);
		_core.DebugEndProduce(); // ends the producer transaction opened by Publish() (DEBUG-only no-op otherwise)
	}
}

/// <summary>
///   In-flight consume handle passed to a value-type handler (sync or async). Gives ref access to the
///   slot via <see cref="Event" /> and lets the handler release the slot early via <see cref="Release" />
///   so the producer can reuse it while the handler is still running. If the handler doesn't release,
///   the worker loop's <c>finally</c> does as a safety net. <see cref="Release" /> is idempotent.
/// </summary>
public ref struct HandleScope<T> where T : struct {
	private readonly SpRingCore<T> _core;
	private readonly int _head;

	internal HandleScope(SpRingCore<T> core, int head) {
		_core = core;
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
		return ref _core.GetSlotRef(_head);
	}

	/// <summary>
	///   Release the slot — run the handler's <c>Clear</c> on it, advance the ring, and wake a producer
	///   waiting for space. After this call the slot's contents are not safe to read. Idempotent.
	/// </summary>
	public void Release() {
		if (IsReleased) return;
		IsReleased = true;
		// Run the value handler's Clear(ref T) on the slot then advance the ring, in one core call —
		// whether this is an early release (the handler freeing the slot mid-task) or the loop's
		// auto-release. The handler nulls just its reference fields / returns pooled buffers; the zero-copy
		// ring never writes a full default(T) back. Clear is a no-op when no cleaner is bound.
		_core.ClearAndAdvanceHead(_head);
	}
}
