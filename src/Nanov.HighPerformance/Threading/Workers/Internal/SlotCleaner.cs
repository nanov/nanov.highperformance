namespace Nanov.HighPerformance.Threading.Workers.Internal;

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices.ComTypes;

/// <summary>
///   The bridge that lets <see cref="SpRingCore{T}" /> run a value worker's strongly-typed
///   <c>Clear(ref T)</c> on a slot at release time, without the (THandler-erased) core or
///   <see cref="HandleScope{T}" /> naming the handler type. A two-field value: a GC-tracked
///   <c>object</c> holder (the worker) and a managed function pointer to a static trampoline the
///   worker supplies (which recovers the typed worker and forwards to its handler's <c>Clear</c>).
///   <para>
///     Zero-allocation and unsafe-free at the call site — only this struct touches the function
///     pointer. The holder is passed to the trampoline as a normal managed argument and the pointer
///     targets static code, so nothing here is a raw pointer into the GC heap (it survives compaction).
///     <c>default(SlotCleaner&lt;T&gt;)</c> is the inert "no cleaner" used by the copy-based workers,
///     which never bind one; <see cref="Clear" /> is then a no-op.
///   </para>
/// </summary>
internal readonly unsafe struct SlotCleaner<T> {
	private readonly object? _holder; // the worker, GC-tracked
	private readonly delegate*<object, ref T, void> _clear; // → static trampoline (stable code address)

	internal static SlotCleaner<T> NoOp => new SlotCleaner<T>(null!, &NoOpTrampoline);

	internal SlotCleaner(object holder, delegate*<object, ref T, void> clear) {
		_holder = holder;
		_clear = clear;
	}

	/// <summary>Run the handler's <c>Clear</c> on the slot. No-op when no cleaner is bound.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void Clear(ref T slot)
		=> _clear(_holder!, ref slot);

	private static void NoOpTrampoline(object holder, ref T slot) { }
}
