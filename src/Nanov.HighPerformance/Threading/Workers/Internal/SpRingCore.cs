namespace Nanov.HighPerformance.Threading.Workers.Internal;

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
///   Internal single-producer / single-consumer ring buffer core, shared (by composition) across the
///   whole SPSC worker family: the power-of-two ring, the edge-triggered wake events, the dedicated
///   worker thread, the lifecycle (<see cref="Start" />/<see cref="Dispose" />/<see cref="TryComplete" />/
///   cancellation/completion), and the producer/consumer wait + signal primitives. The public workers
///   own one of these and forward to it; the public scopes hold one to reach the slot. Not part of the
///   public API.
///   <para>
///     The producer/consumer FAST paths (the empty/full index checks, slot write/read, and the
///     edge-triggered <c>Set()</c>) are <see cref="MethodImplOptions.AggressiveInlining" /> so they
///     inline into the callers' hot loops; only the COLD parking loops
///     (<see cref="WaitForSpaceSlow" />/<see cref="WaitForItemSlow" />) are
///     <see cref="MethodImplOptions.NoInlining" />, keeping the hot frame tight.
///   </para>
///   <para>
///     SIGNALING INVARIANT (strict SPSC). Exactly one producer thread reserves+commits slots; the
///     single dedicated worker thread is the only consumer and the only writer of <c>_head</c>. The
///     wake events use edge-triggered <c>Set()</c> (only on the empty→non-empty / full→non-full
///     transition) paired with a non-atomic <c>Wait();Reset()</c>. This is correct ONLY under SPSC:
///     while the consumer is parked, <c>_head</c> does not advance, so only the first item past
///     <c>_head</c> satisfies <c>_head == tail</c> and fires the lone <c>Set</c> — no second signal a
///     <c>Reset()</c> could lose. <see cref="ManualResetEventSlim" /> is latching (order-insensitive),
///     and the <c>while (_head == Volatile.Read(ref _tail))</c> recheck acquire-reads <c>_tail</c>
///     published with release before the <c>Set</c>. Shutdown rides the separate <c>volatile _stop</c>,
///     re-checked before every park. WARNING: a second producer, or advancing <c>_head</c> off the
///     worker thread, breaks this and hangs silently.
///   </para>
/// </summary>
internal sealed class SpRingCore<T> : IDisposable {
	/// <summary>
	///   SpinWait iterations before falling back to manual reset event wait.
	///   Reduces syscall overhead for bursty traffic at the cost of CPU during idle.
	/// </summary>
	private const int SpinThreshold = 200;

	internal readonly T[] _buffer;
	internal readonly int _mask;
	private readonly ManualResetEventSlim _itemEvt = new(false); // set when items available
	private readonly ManualResetEventSlim _spaceEvt = new(false); // set when space freed
	private readonly TaskCompletionSource _completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
	private readonly Thread _thread;
	private volatile bool _completed; // soft stop (Complete) — drain remaining
	private Exception? _completeException;
	private CancellationToken _ct;
	private CancellationTokenRegistration _ctRegistration;
	private int _head; // consumer index (worker thread)
	private volatile bool _stop; // hard stop (Dispose / cancel) — drop remaining
	private int _tail; // producer index (dispatching thread)

	internal SpRingCore(int capacity, string threadName, ThreadStart runLoop) {
		capacity = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(capacity, 1));
		_buffer = new T[capacity];
		_mask = capacity - 1;
		_thread = new Thread(runLoop) {
			IsBackground = true,
			Name = threadName
		};
	}

	internal Task Completion => _completionTcs.Task;
	internal int QueuedApprox => _tail - _head;
	internal int Capacity => _buffer.Length;
	internal CancellationToken CancellationToken => _ct;

	internal void Start(CancellationToken ct) {
		_ct = ct;
		if (ct.CanBeCanceled)
			_ctRegistration = ct.Register(static s => ((SpRingCore<T>)s!).OnCancel(), this);
		_thread.Start();
	}

	private void OnCancel() {
		_stop = true;
		_itemEvt.Set();
		_spaceEvt.Set();
	}

	internal bool TryComplete(Exception? exception) {
		if (_completed) return false;
		_completeException = exception;
		_completed = true;
		_itemEvt.Set();
		return true;
	}

	/// <summary>The exception to complete with: the explicit one, else cancellation, else null.</summary>
	internal Exception? CompletionException() =>
		_completeException ?? (_ct.IsCancellationRequested ? new OperationCanceledException(_ct) : null);

	/// <summary>Resolve the completion task and tear down the cancellation registration. Call once, on exit.</summary>
	internal void SetCompletion(Exception? exception) {
		if (exception is OperationCanceledException)
			_completionTcs.TrySetCanceled(_ct);
		else if (exception is not null)
			_completionTcs.TrySetException(exception);
		else
			_completionTcs.TrySetResult();
		_ctRegistration.Dispose();
	}

	public void Dispose() {
		if (_stop) return;
		_stop = true;
		// Wake up both sides so they observe _stop and exit.
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

	/// <summary>
	///   Producer reservation. Fast-path inline fullness check; parks on the cold path. Returns false
	///   if the worker is no longer accepting. On true, <paramref name="tail" /> is the index to write
	///   (<c>_buffer[tail &amp; _mask]</c>); the caller writes it then calls <see cref="CommitTail" />.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryReserve(out int tail) {
		if (_completed || _stop) {
			tail = 0;
			return false;
		}

		if (_tail - Volatile.Read(ref _head) == _buffer.Length && !WaitForSpaceSlow()) {
			tail = 0;
			return false;
		}

		tail = _tail;
		return true;
	}

	/// <summary>Publish the slot at <paramref name="tail" /> and edge-signal the consumer.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void CommitTail(int tail) {
		Volatile.Write(ref _tail, tail + 1);
		// Edge-triggered: signal only on the empty→non-empty transition (latching event; the consumer's
		// post-Reset recheck re-reads _tail, so this lone Set cannot be lost). See SIGNALING INVARIANT.
		if (Volatile.Read(ref _head) == tail)
			_itemEvt.Set();
	}

	/// <summary>Reserve + write + commit in one shot (copy-based producers). False if not accepting.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryEnqueue(T item) {
		if (!TryReserve(out var tail)) return false;
		_buffer[tail & _mask] = item;
		CommitTail(tail);
		return true;
	}

	/// <summary>
	///   Consumer claim. Fast-path inline emptiness check; parks on the cold path. Returns false to
	///   exit the loop — hard stop (drops queued items) or soft complete with the ring drained. On
	///   true, <paramref name="head" /> is the next item's index (<c>_buffer[head &amp; _mask]</c>).
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryClaim(out int head) {
		if (_head == Volatile.Read(ref _tail) && !WaitForItemSlow()) {
			head = 0;
			return false;
		}

		// Hard stop drops items still in the ring; soft complete drains them (WaitForItemSlow exits
		// above only once the ring is empty).
		if (_stop) {
			head = 0;
			return false;
		}

		head = _head;
		return true;
	}

	/// <summary>Release the slot at <paramref name="head" /> and edge-signal a blocked producer.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void AdvanceHead(int head) {
		Volatile.Write(ref _head, head + 1);
		// Edge-triggered space signal, mirror of CommitTail: wake a blocked producer only on the
		// full→non-full transition. Safe under SPSC (single producer). See SIGNALING INVARIANT.
		if (Volatile.Read(ref _tail) - head == _buffer.Length)
			_spaceEvt.Set();
	}

	/// <summary>Claim + copy out + clear + advance in one shot (copy-based consumers). False to exit.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryDequeue(out T item) {
		if (!TryClaim(out var head)) {
			item = default!;
			return false;
		}

		ref var slot = ref _buffer[head & _mask];
		item = slot;
		slot = default!;
		AdvanceHead(head);
		return true;
	}

	// Cold path: producer parks until space frees, or the worker stops/completes.
	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool WaitForSpaceSlow() {
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

		return true;
	}

	// Cold path: consumer parks until an item arrives, or the worker stops/completes-drained.
	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool WaitForItemSlow() {
		var spins = 0;
		while (_head == Volatile.Read(ref _tail)) {
			if (_stop || _completed) return false;
			if (spins++ < SpinThreshold) {
				Thread.SpinWait(10);
				continue;
			}

			_itemEvt.Wait();
			_itemEvt.Reset();
			spins = 0;
		}

		return true;
	}
}
