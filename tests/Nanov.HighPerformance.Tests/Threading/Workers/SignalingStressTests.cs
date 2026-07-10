namespace Nanov.HighPerformance.Tests.Threading.Workers;

using Nanov.HighPerformance.Threading.Workers;
using NUnit.Framework;

/// <summary>
///   Lost-wakeup stress guards for SpRingCore's wake signaling (see SIGNALING INVARIANT). Tiny
///   capacities maximize empty↔non-empty / full↔non-full boundary churn — every crossing is a
///   park/signal opportunity — and high item counts (0.2–1M) make a lost wakeup a near-certain plateau, which the
///   stall watchdog turns into a fast failure instead of a hung run.
/// </summary>
[TestFixture]
public class SignalingStressTests {
	// Shared observable state; the struct handlers write through this reference (house Rec pattern).
	// Expected/OutOfOrder are consumer-thread-only; Processed is read cross-thread via Volatile.
	private sealed class CountState {
		public int Expected;
		public int OutOfOrder;
		public int Processed;
	}

	private readonly struct CountingHandler : IWorkerHandler<int> {
		private readonly CountState _state;
		private readonly int _sleepEvery;

		public CountingHandler(CountState state, int sleepEvery = 0) {
			_state = state;
			_sleepEvery = sleepEvery;
		}

		public void Handle(int item) {
			if (item != _state.Expected++) _state.OutOfOrder++;
			if (_sleepEvery > 0 && item % _sleepEvery == 0) Thread.Sleep(1);
			Volatile.Write(ref _state.Processed, _state.Expected);
		}

		public void OnError(Exception error) { }
		public void OnCompleted(Exception? error) { }
	}

	private readonly struct AsyncCountingHandler : IAsyncWorkerHandler<int> {
		private readonly CountState _state;

		public AsyncCountingHandler(CountState state) => _state = state;

		// Completes synchronously — keeps the loop in its tight drain; the signaling churn under test
		// happens at the ring boundaries, not inside the handler.
		public ValueTask HandleAsync(int item, CancellationToken cancellationToken) {
			if (item != _state.Expected++) _state.OutOfOrder++;
			Volatile.Write(ref _state.Processed, _state.Expected);
			return default;
		}

		public void OnError(Exception error) { }
		public ValueTask OnCompletedAsync(Exception? error) => default;
	}

	[TestCase(1)]
	[TestCase(2)]
	public void Sync_Churn_1M_Items_No_Lost_Wakeup(int capacity) {
		var state = new CountState();
		using var w = new SpWorker<int, CountingHandler>(new CountingHandler(state), capacity);
		w.Start();

		const int n = 1_000_000;
		var dispatched = 0;
		var producer = Task.Run(() => {
			for (var i = 0; i < n; i++) {
				w.Dispatch(i);
				Volatile.Write(ref dispatched, i + 1);
			}
		});
		// Producer-side stall watchdog: Dispatch blocks on a full ring, so a lost space wakeup would
		// hang the producer forever — a plateauing `dispatched` fails fast here instead.
		WorkerTestUtil.AwaitCompletionOrStall(producer, () => Volatile.Read(ref dispatched), n);
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => Volatile.Read(ref state.Processed), n);

		Assert.That(Volatile.Read(ref state.Processed), Is.EqualTo(n));
		Assert.That(state.OutOfOrder, Is.Zero);
	}

	// An intermittent 1ms handler stall at tiny capacity keeps the ring full, so most dispatches cross
	// the full→non-full boundary — hammering the producer park + space-signal path specifically.
	[TestCase(1)]
	[TestCase(2)]
	public void Sync_Blocked_Producer_Churn_No_Lost_Wakeup(int capacity) {
		var state = new CountState();
		using var w = new SpWorker<int, CountingHandler>(new CountingHandler(state, sleepEvery: 1024), capacity);
		w.Start();

		const int n = 200_000;
		var dispatched = 0;
		var producer = Task.Run(() => {
			for (var i = 0; i < n; i++) {
				w.Dispatch(i);
				Volatile.Write(ref dispatched, i + 1);
			}
		});
		// Producer-side stall watchdog: Dispatch blocks on a full ring, so a lost space wakeup would
		// hang the producer forever — a plateauing `dispatched` fails fast here instead.
		WorkerTestUtil.AwaitCompletionOrStall(producer, () => Volatile.Read(ref dispatched), n);
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => Volatile.Read(ref state.Processed), n);

		Assert.That(Volatile.Read(ref state.Processed), Is.EqualTo(n));
		Assert.That(state.OutOfOrder, Is.Zero);
	}

	[TestCase(1)]
	[TestCase(2)]
	public void Async_Churn_1M_Items_No_Lost_Wakeup(int capacity) {
		var state = new CountState();
		using var w = new AsyncSpWorker<int, AsyncCountingHandler>(new AsyncCountingHandler(state), capacity);
		w.Start();

		const int n = 1_000_000;
		var dispatched = 0;
		var producer = Task.Run(() => {
			for (var i = 0; i < n; i++) {
				w.Dispatch(i);
				Volatile.Write(ref dispatched, i + 1);
			}
		});
		// Producer-side stall watchdog: Dispatch blocks on a full ring, so a lost space wakeup would
		// hang the producer forever — a plateauing `dispatched` fails fast here instead.
		WorkerTestUtil.AwaitCompletionOrStall(producer, () => Volatile.Read(ref dispatched), n);
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => Volatile.Read(ref state.Processed), n);

		Assert.That(Volatile.Read(ref state.Processed), Is.EqualTo(n));
		Assert.That(state.OutOfOrder, Is.Zero);
	}

	// Deterministic async idle-park wake: parks the ThreadPool loop on the armed AsyncAutoResetEvent
	// (no spin phase on the async side), then a lone Dispatch must complete — a skipped Set on the
	// armed waiter fails AwaitProgress instead of hanging.
	[Test]
	public void Async_Dispatch_Wakes_A_Parked_Loop() {
		var state = new CountState();
		using var w = new AsyncSpWorker<int, AsyncCountingHandler>(new AsyncCountingHandler(state), 4);
		w.Start();

		Thread.Sleep(150); // the loop finds the ring empty and arms the async wait
		Assert.That(w.Dispatch(0), Is.True);
		WorkerTestUtil.AwaitProgress(() => Volatile.Read(ref state.Processed), 1);

		Thread.Sleep(150); // loop re-arms (ring drained, empty again)
		Assert.That(w.Dispatch(1), Is.True);
		WorkerTestUtil.AwaitProgress(() => Volatile.Read(ref state.Processed), 2);

		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => Volatile.Read(ref state.Processed), 2);
	}
}
