namespace Nanov.HighPerformance.Threading.Actors.Internal;

using System.Runtime.InteropServices;

/// <summary>
///   A <see langword="long" /> alone on its own 128-byte cache line, for per-worker counters that are
///   written by their owner and read by everyone at quiescence. Without the padding, N workers'
///   counters share lines and every publish invalidates its neighbours.
///   <para>
///     <b>Non-generic on purpose.</b> A generic type cannot carry
///     <see cref="StructLayoutAttribute" /> with <see cref="LayoutKind.Explicit" /> — the same reason
///     <c>SpRingCore</c>'s <c>PaddedIndices</c> is a standalone type.
///   </para>
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = 128)]
internal struct PaddedLong {
	[FieldOffset(0)] public long Value;
}
