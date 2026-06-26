namespace Nanov.HighPerformance.Threading;

using System.Runtime.CompilerServices;

/// <summary>
///   Copy-based access layer over <see cref="SpscRingBase{T}" />, shared by
///   <see cref="SpscWorker{T, THandler}" /> (sync) and <see cref="AsyncSpscWorker{T, THandler}" />
///   (async). Producers <see cref="Dispatch" /> an item (copied into the ring); the worker loop
///   dequeues a copy via <see cref="TryDequeue" /> and hands it to the handler by value. For zero-copy
///   over large structs use <see cref="ValueSpscWorkerBase{T}" /> instead.
/// </summary>
public abstract class SpscWorkerBase<T> : SpscRingBase<T> {
	private protected SpscWorkerBase(int capacity, string? threadName, string defaultThreadName)
		: base(capacity, threadName, defaultThreadName) {
	}

	/// <summary>
	///   Enqueue an item. Returns false if the worker has been completed or disposed
	///   (item is not enqueued). Returns true if successfully enqueued. Blocks if the buffer is full.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool Dispatch(T item) {
		if (!TryReserve()) return false;
		var tail = _tail;
		_buffer[tail & _mask] = item;
		CommitTail(tail);
		return true;
	}

	/// <summary>
	///   Wait for and dequeue the next item (copy it out, clear the slot, advance the ring). Returns
	///   false when the worker should exit — hard stop, or soft complete with the ring drained.
	///   Non-virtual — called from the leaf loop, no per-item virtual dispatch.
	/// </summary>
	protected bool TryDequeue(out T item) {
		if (!TryClaim()) {
			item = default!;
			return false;
		}

		var head = _head;
		ref var slot = ref _buffer[head & _mask];
		item = slot;
		slot = default!;
		AdvanceHead(head);
		return true;
	}
}
