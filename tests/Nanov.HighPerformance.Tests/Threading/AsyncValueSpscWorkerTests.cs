namespace Nanov.HighPerformance.Tests.Threading;

using System.Linq;
using Nanov.HighPerformance.Threading;
using NUnit.Framework;

[TestFixture]
public class AsyncValueSpscWorkerTests {
	// --- Smoke --------------------------------------------------------------------

	[Test]
	public void Publish_Process_Sync_Completing_RoundTrip() {
		using var w = new Rec(4);
		w.Start();

		using (var s = w.Publish()) s.Event() = 7;

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Processed, Is.EqualTo(new[] { 7 }));
	}

	[Test]
	public void Sync_Completing_ValueTask_Preserves_Order_At_Scale() {
		using var w = new Rec(16);
		w.Start();

		for (var i = 0; i < 100; i++) {
			using var s = w.Publish();
			s.Event() = i;
		}

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Processed, Is.EqualTo(Enumerable.Range(0, 100)));
	}

	[Test]
	public void Truly_Async_ValueTask_Is_Awaited_Before_Next_Iteration() {
		// AsyncTail returns a Task.Delay — the loop must block on it via the
		// _processDoneEvt rendezvous before processing the next slot.
		using var w = new Rec(4, asyncTail: _ => Task.Delay(25));
		w.Start();

		for (var i = 0; i < 5; i++) {
			using var s = w.Publish();
			s.Event() = i;
		}

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Processed, Is.EqualTo(Enumerable.Range(0, 5)));
	}

	// --- Release semantics --------------------------------------------------------

	[Test]
	public async Task Early_Release_Unblocks_Producer_While_Async_Tail_Is_Running() {
		// Capacity 2. Sequence (mirrors sync variant):
		//   publish(1) → Loop picks, Release early, AsyncTail blocks → head advanced by 1.
		//   publish(2) → slot 1 filled.
		//   publish(3) → would block WITHOUT early Release; succeeds WITH it.
		// Synchronize on asyncTailBlocked before publish(3) to eliminate the race.
		var asyncTailBlocked = new ManualResetEventSlim(false);
		var asyncTailProceed = new ManualResetEventSlim(false);
		using var w = new Rec(2, asyncTail: _ => Task.Run(() => {
			asyncTailBlocked.Set();
			asyncTailProceed.Wait();
		}));
		w.Start();

		using (var s = w.Publish()) s.Event() = 1;

		using (var s = w.Publish()) s.Event() = 2;

		Assert.That(asyncTailBlocked.Wait(1000), Is.True,
			"handler should have released item 1 and launched its async tail");

		var publish3 = Task.Run(() => {
			using var s = w.Publish();
			s.Event() = 3;
		});
		var first = await Task.WhenAny(publish3, Task.Delay(1000));
		Assert.That(first, Is.SameAs(publish3),
			"early Release should free a slot even while the async tail is in flight");

		asyncTailProceed.Set();
		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
	}

	[Test]
	public void Safety_Net_Release_When_Handler_Doesnt() {
		using var w = new Rec(2, releaseEarly: false);
		w.Start();

		for (var i = 0; i < 10; i++) {
			using var s = w.Publish();
			s.Event() = i;
		}

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Processed, Is.EqualTo(Enumerable.Range(0, 10)));
	}

	[Test]
	public void Handler_Throw_Is_Reported_To_OnError_And_Worker_Continues() {
		// Faults are no longer swallowed — each sync throw inside HandleAsync goes to OnError and the
		// worker continues to the next slot. The slot is still released (safety net), and nothing lands
		// in Processed because the throw precedes the add.
		using var w = new Rec(4, throwOnProcess: true);
		w.Start();

		for (var i = 0; i < 5; i++) {
			using var s = w.Publish();
			s.Event() = i;
		}

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Processed, Is.Empty);
		Assert.That(w.Errors, Has.Count.EqualTo(5));
		Assert.That(w.Errors[0], Is.InstanceOf<InvalidOperationException>());
	}

	[Test]
	public void Async_Tail_Fault_Is_Reported_To_OnError_And_Worker_Continues() {
		// A fault INSIDE the awaited ValueTask — surfaced by the Loop's awaiter.GetResult(). The tail is
		// a faulted Task, so the outer ValueTask represents a truly async failure rather than a sync throw.
		// Each fault is reported to OnError (await unwraps to the InvalidOperationException) and the worker
		// drains; nothing reaches Processed because the tail faulted before the add ran.
		using var w = new Rec(4, asyncTail: _ => Task.FromException(new InvalidOperationException("tail bang")));
		w.Start();

		for (var i = 0; i < 5; i++) {
			using var s = w.Publish();
			s.Event() = i;
		}

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True, "worker must drain despite every async tail faulting");
		Assert.That(w.Processed, Is.Empty, "tail faulted before Processed.Add ran");
		Assert.That(w.Errors, Has.Count.EqualTo(5));
		Assert.That(w.Errors[0], Is.InstanceOf<InvalidOperationException>());
	}

	// --- Completion / cancellation ------------------------------------------------

	[Test]
	public void Completion_Resolves_Normally_After_TryComplete() {
		using var w = new Rec(8);
		w.Start();

		for (var i = 0; i < 8; i++) {
			using var s = w.Publish();
			s.Event() = i;
		}

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Completion.IsCompletedSuccessfully, Is.True);
	}

	[Test]
	public async Task Cancellation_Stops_The_Worker() {
		using var w = new Rec(4);
		using var cts = new CancellationTokenSource();
		w.Start(cts.Token);

		cts.Cancel();
		var completed = await Task.WhenAny(w.Completion, Task.Delay(2000));
		Assert.That(completed, Is.SameAs(w.Completion));
		Assert.That(w.Completion.IsCanceled, Is.True);
	}

	[Test]
	public void Publish_After_Complete_Returns_Closed_Scope() {
		using var w = new Rec(4);
		w.Start();
		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);

		using var scope = w.Publish();
		Assert.That(scope.IsOpen, Is.False);
	}

	// --- Edge-triggered signaling regression guards -------------------------------

	// Regression guard for the edge-triggered ring signaling (see ValueSpscWorkerBase SIGNALING
	// INVARIANT). Tiny capacity maximizes empty↔non-empty / full↔non-full transitions; sync-completing
	// handler. A dropped wakeup surfaces as a timeout (stall watchdog), not a wrong value.
	[TestCase(1)]
	[TestCase(2)]
	public async Task Edge_Triggered_Signaling_Survives_Boundary_Churn(int capacity) {
		using var w = new Rec(capacity);
		w.Start();

		const int n = 50_000;
		await Task.Run(() => {
			for (var i = 0; i < n; i++) {
				using var scope = w.Publish();
				scope.Event() = i;
			}
		});
		w.TryComplete();
		// Stall watchdog: a dropped wakeup plateaus Processed and fails fast, never hangs the run.
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), n);

		Assert.That(w.Processed, Is.EqualTo(Enumerable.Range(0, n)));
	}

	// Same churn, but every item goes through a truly-async tail so each crosses the _processDoneEvt
	// Wait();Reset() rendezvous on top of the ring signaling.
	[Test]
	public async Task Truly_Async_Boundary_Churn_Survives_The_ProcessDone_Rendezvous() {
		using var w = new Rec(2, asyncTail: _ => Task.Run(() => { }));
		w.Start();

		const int n = 20_000;
		await Task.Run(() => {
			for (var i = 0; i < n; i++) {
				using var scope = w.Publish();
				scope.Event() = i;
			}
		});
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), n);

		Assert.That(w.Processed.OrderBy(x => x), Is.EqualTo(Enumerable.Range(0, n)));
	}

	// Deterministic lost-wakeup guard for the item signal — reliably parks the consumer on _itemEvt first
	// (unlike the churn test), so a dropped empty→non-empty Set() in PublishScope.Dispose is always caught.
	[Test]
	public void Publish_Wakes_An_Idle_Parked_Worker() {
		using var w = new Rec(4);
		w.Start();

		Thread.Sleep(150); // let the consumer exhaust its spin budget and park on _itemEvt
		using (var s = w.Publish()) s.Event() = 1;
		WorkerTestUtil.AwaitProgress(() => CountOf(w), 1);

		Thread.Sleep(150); // worker re-parks (queue drained, empty again)
		using (var s = w.Publish()) s.Event() = 2;
		WorkerTestUtil.AwaitProgress(() => CountOf(w), 2);

		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 2);
		Assert.That(w.Processed, Is.EqualTo(new[] { 1, 2 }));
	}

	// Forces the producer to block on the space event repeatedly. ReleaseEarly=false so head advances only
	// after the (occasionally delayed) tail, keeping the tiny buffer full and the producer parking on space.
	[Test]
	public async Task Blocking_Producer_Is_Always_Re_Signaled_When_Space_Frees() {
		using var w = new Rec(2, releaseEarly: false,
			asyncTail: v => v % 20 == 0 ? Task.Delay(1) : Task.CompletedTask);
		w.Start();

		const int n = 2_000;
		await Task.Run(() => {
			for (var i = 0; i < n; i++) {
				using var scope = w.Publish();
				scope.Event() = i;
			}
		});
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), n);

		Assert.That(w.Processed, Is.EqualTo(Enumerable.Range(0, n)));
	}

	// "Queue cleaned up on shutdown": Complete called while the buffer is FULL (consumer gated in its async
	// tail) must still drain every committed item and resolve Completion within a bound — no item stranded.
	[Test]
	public void Complete_Drains_A_Full_Buffer() {
		var entered = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		using var w = new Rec(2, releaseEarly: false,
			asyncTail: v => Task.Run(() => {
				if (v == 0) {
					entered.Set();
					gate.Wait();
				}
			}));
		w.Start();

		using (var s = w.Publish()) s.Event() = 0;
		Assert.That(entered.Wait(2000), Is.True,
			"consumer must enter the tail for item 0 before we fill the ring");
		// head still 0 (released only after the tail completes); cap=2 ring now holds 0 and 1 → full.
		using (var s = w.Publish()) s.Event() = 1;

		Assert.That(w.TryComplete(), Is.True);
		gate.Set();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 2);

		Assert.That(w.Processed, Is.EqualTo(new[] { 0, 1 }));
	}

	private static int CountOf(Rec w) {
		lock (w.Processed) return w.Processed.Count;
	}

	// Shared, observable state the struct handler writes through (the struct is copied into the worker;
	// the test reads results via this reference).
	private sealed class RecordingState {
		public readonly List<Exception?> CompletedWith = new();
		public readonly List<Exception> Errors = new();
		public readonly List<int> Processed = new();
	}

	// Read the slot + Release synchronously, then return a ValueTask from the nested async helper — a ref
	// struct scope can't cross an await, so HandleAsync itself must NOT be async.
	private readonly struct RecordingHandler : IAsyncValueSpscHandler<int> {
		private readonly RecordingState _state;
		private readonly Func<int, Task>? _asyncTail; // invoked AFTER Release, returned as ValueTask
		private readonly bool _releaseEarly;
		private readonly bool _throwOnProcess;

		public RecordingHandler(RecordingState state, Func<int, Task>? asyncTail, bool releaseEarly,
			bool throwOnProcess) {
			_state = state;
			_asyncTail = asyncTail;
			_releaseEarly = releaseEarly;
			_throwOnProcess = throwOnProcess;
		}

		public ValueTask HandleAsync(ref HandleScope<int> scope, CancellationToken cancellationToken) {
			ref var slot = ref scope.Event();
			var value = slot;
			if (_releaseEarly) scope.Release();
			if (_throwOnProcess) throw new InvalidOperationException("bang");
			if (_asyncTail is not null) return DoAsync(value);
			lock (_state.Processed) _state.Processed.Add(value);
			return default;
		}

		public void OnError(Exception error) {
			lock (_state.Errors) _state.Errors.Add(error);
		}

		public ValueTask OnCompletedAsync(Exception? error) {
			lock (_state.CompletedWith) _state.CompletedWith.Add(error);
			return default;
		}

		private async ValueTask DoAsync(int value) {
			await _asyncTail!(value).ConfigureAwait(false);
			lock (_state.Processed) _state.Processed.Add(value);
		}
	}

	// Thin harness mirroring the old Recording surface so the test bodies stay close to the original.
	private sealed class Rec : IDisposable {
		private readonly AsyncValueSpscWorker<int, RecordingHandler> _worker;
		private readonly RecordingState _state = new();

		public Rec(int capacity, Func<int, Task>? asyncTail = null, bool releaseEarly = true,
			bool throwOnProcess = false) =>
			_worker = new AsyncValueSpscWorker<int, RecordingHandler>(
				new RecordingHandler(_state, asyncTail, releaseEarly, throwOnProcess), capacity);

		public List<int> Processed => _state.Processed;
		public List<Exception?> CompletedWith => _state.CompletedWith;
		public List<Exception> Errors => _state.Errors;
		public Task Completion => _worker.Completion;

		public void Start(CancellationToken ct = default) => _worker.Start(ct);
		public PublishScope<int> Publish() => _worker.Publish();
		public bool TryComplete(Exception? exception = null) => _worker.TryComplete(exception);
		public void Dispose() => _worker.Dispose();
	}
}
