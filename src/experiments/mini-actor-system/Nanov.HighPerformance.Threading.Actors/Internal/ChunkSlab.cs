namespace Nanov.HighPerformance.Threading.Actors.Internal;

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

/// <summary>
///   The message store: one pre-allocated <typeparamref name="T" /> array carved into fixed-size
///   chunks. A chunk is lent to one key for as long as that key has messages, and is used as that
///   key's bounded mailbox.
///   <para>
///     The free list is a <b>producer-private intrusive stack</b> of chunk indices — a plain head
///     plus a <c>next</c> array, with <b>no interlocked operations</b>. Both rent and return happen
///     on the producer thread (the worker never touches <c>ChunkBase</c>), so there is exactly one
///     reader and one writer and they are the same thread. Do not "make this thread-safe": if a
///     second thread ever needs to return a chunk, the fix is to route it through the producer, not
///     to add a CAS.
///   </para>
/// </summary>
internal sealed class ChunkSlab<T> {
	private readonly T[] _slots;
	private readonly int[] _nextFree; // intrusive free-list links, indexed by chunk
	private readonly int _chunkSize;
	private readonly int _chunkMask;
	private readonly int _chunkShift;
	private readonly int _chunkCount;
	private int _freeHead;
	private int _freeCount;

	/// <param name="chunkCount">Max concurrently-dirty keys. Rounded up to a power of two.</param>
	/// <param name="chunkSize">Messages a single key may hold. Rounded up to a power of two.</param>
	internal ChunkSlab(int chunkCount, int chunkSize) {
		_chunkCount = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(chunkCount, 1));
		_chunkSize = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(chunkSize, 1));
		_chunkMask = _chunkSize - 1;
		_chunkShift = BitOperations.TrailingZeroCount(_chunkSize);

		var total = (long)_chunkCount * _chunkSize;
		if (total > int.MaxValue)
			throw new ArgumentOutOfRangeException(nameof(chunkCount), "chunkCount * chunkSize overflows int");

		_slots = new T[(int)total];
		_nextFree = new int[_chunkCount];

		// Seed the free list with every chunk, ascending so the first rent hands out chunk 0.
		for (var i = 0; i < _chunkCount; i++)
			_nextFree[i] = i - 1;
		_freeHead = _chunkCount - 1;
		_freeCount = _chunkCount;
	}

	internal int ChunkSize => _chunkSize;

	/// <summary>Mask for turning an absolute mailbox cursor into an in-chunk slot index.</summary>
	internal int ChunkMask => _chunkMask;

	internal int ChunkCount => _chunkCount;

	/// <summary>Chunks currently on the free list. Producer-thread view; not thread-safe.</summary>
	internal int FreeCount => _freeCount;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryRent(out int chunk) {
		var head = _freeHead;
		if (head < 0) {
			chunk = Chunk.None;
			return false;
		}

		_freeHead = _nextFree[head];
		_freeCount--;
		chunk = head;
		return true;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void Return(int chunk) {
		_nextFree[chunk] = _freeHead;
		_freeHead = chunk;
		_freeCount++;
	}

	/// <summary>
	///   Ref to a slot. <paramref name="indexInChunk" /> must already be masked with
	///   <see cref="ChunkMask" />; the access is unchecked (safe by construction — power-of-two sizes).
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal ref T SlotRef(int chunk, int indexInChunk) =>
		ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_slots),
			(nint)(uint)((chunk << _chunkShift) + indexInChunk));

	/// <summary>The whole chunk, for the cold reclaim walk (§4.6).</summary>
	internal Span<T> ChunkSpan(int chunk) =>
		_slots.AsSpan(chunk * _chunkSize, _chunkSize);
}
