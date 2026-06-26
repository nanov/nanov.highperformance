namespace Nanov.HighPerformance.Tests.Threading.Workers;

using System.Linq;
using Nanov.HighPerformance.Threading.Workers;
using NUnit.Framework;

[TestFixture]
public class AsyncSpWorkerTests {
	[Test]
	public void SyncCompleting_ProcessAsync_Processes_In_Order() {
		using var w = new Rec(16);
		w.Start();
		for (var i = 0; i < 100; i++) w.Dispatch(i);
		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Processed, Is.EqualTo(Enumerable.Range(0, 100)));
	}

	[Test]
	public void TrulyAsync_ProcessAsync_Blocks_Worker_Until_Complete() {
		// Async delay -> worker uses GetAwaiter-callback pattern (not sync path)
		using var w = new Rec(8, asyncDelay: TimeSpan.FromMilliseconds(30));
		w.Start();
		for (var i = 0; i < 10; i++) w.Dispatch(i);
		w.TryComplete();
		Assert.That(w.Completion.Wait(5000), Is.True);
		Assert.That(w.Processed, Is.EqualTo(Enumerable.Range(0, 10)));
	}

	[Test]
	public void TryComplete_Second_Call_Returns_False() {
		using var w = new Rec(8);
		w.Start();
		Assert.That(w.TryComplete(), Is.True);
		Assert.That(w.TryComplete(), Is.False);
		Assert.That(w.Completion.Wait(2000), Is.True);
	}

	[Test]
	public void Dispatch_After_Complete_Returns_False() {
		using var w = new Rec(8);
		w.Start();
		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Dispatch(42), Is.False);
	}

	[Test]
	public void TryComplete_With_Exception_Faults_Completion() {
		using var w = new Rec(8);
		w.Start();
		w.Dispatch(1);
		var err = new InvalidOperationException("boom");
		w.TryComplete(err);

		var agg = Assert.Throws<AggregateException>(() => w.Completion.Wait(2000));
		Assert.That(agg!.InnerException, Is.InstanceOf<InvalidOperationException>());

		Assert.That(w.CompletedWith, Has.Count.EqualTo(1));
		Assert.That(w.CompletedWith[0], Is.SameAs(err));
	}

	[Test]
	public void Cancellation_Cancels_Completion() {
		using var w = new Rec(16, TimeSpan.FromMilliseconds(30));
		using var cts = new CancellationTokenSource();
		w.Start(cts.Token);
		for (var i = 0; i < 100; i++) w.Dispatch(i);

		Thread.Sleep(50);
		cts.Cancel();

		var agg = Assert.Throws<AggregateException>(() => w.Completion.Wait(2000));
		Assert.That(agg!.InnerException, Is.InstanceOf<OperationCanceledException>());

		Assert.That(w.CompletedWith, Has.Count.EqualTo(1));
		Assert.That(w.CompletedWith[0], Is.InstanceOf<OperationCanceledException>());
	}

	// Renamed from ProcessAsync_Exception_Is_Swallowed: the new worker no longer silently swallows a
	// faulting HandleAsync — it reports the exception to the handler's OnError (synchronously) and
	// continues to the next item. Assert both: the worker drained the other items AND OnError recorded it.
	[Test]
	public void Handler_Exception_Is_Reported_To_OnError_And_Worker_Continues() {
		using var w = new Rec(8, hook: i => {
			if (i == 2) throw new InvalidOperationException("boom");
		});
		w.Start();
		for (var i = 0; i < 5; i++) w.Dispatch(i);
		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Processed, Is.SupersetOf(new[] { 0, 1, 3, 4 }));
		Assert.That(w.Errors, Has.Count.EqualTo(1));
		Assert.That(w.Errors[0], Is.InstanceOf<InvalidOperationException>());
	}

	[Test]
	public async Task Dispatch_Blocks_When_Full() {
		using var w = new Rec(2, TimeSpan.FromMilliseconds(100));
		w.Start();
		w.Dispatch(1);
		w.Dispatch(2);
		w.Dispatch(3);

		var dispatchTask = Task.Run(() => w.Dispatch(4));
		Assert.That(dispatchTask.Wait(20), Is.False, "dispatch should block until space");
		Assert.That(await dispatchTask, Is.True);

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
	}

	[Test]
	public async Task Concurrent_Dispatch_Preserves_Order() {
		using var w = new Rec(16);
		w.Start();

		const int n = 10_000;
		await Task.Run(() => {
			for (var i = 0; i < n; i++) w.Dispatch(i);
		});
		w.TryComplete();
		// Stall watchdog: a dropped wakeup plateaus Processed and fails fast, never hangs the run.
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), n);

		Assert.That(w.Processed, Is.EqualTo(Enumerable.Range(0, n)));
	}

	// Regression guard for the edge-triggered ring signaling (see AsyncSpWorker SIGNALING INVARIANT).
	// Tiny capacity maximizes empty↔non-empty / full↔non-full transitions over many iterations; a dropped
	// wakeup surfaces as a stall, not a wrong value.
	[TestCase(1)]
	[TestCase(2)]
	public async Task Edge_Triggered_Signaling_Survives_Boundary_Churn(int capacity) {
		using var w = new Rec(capacity);
		w.Start();

		const int n = 50_000;
		await Task.Run(() => {
			for (var i = 0; i < n; i++) w.Dispatch(i);
		});
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), n);

		Assert.That(w.Processed, Is.EqualTo(Enumerable.Range(0, n)));
	}

	// Same churn, but the handler completes asynchronously (Task.Yield) so every item goes through the
	// _processDoneEvt Wait();Reset() rendezvous — the per-item park the async worker adds on top of the ring.
	[Test]
	public async Task Truly_Async_Boundary_Churn_Survives_The_ProcessDone_Rendezvous() {
		using var w = new Rec(2, yield: true);
		w.Start();

		const int n = 20_000;
		await Task.Run(() => {
			for (var i = 0; i < n; i++) w.Dispatch(i);
		});
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), n);

		Assert.That(w.Processed, Is.EqualTo(Enumerable.Range(0, n)));
	}

	// Deterministic lost-wakeup guard for the item signal — reliably parks the consumer first (unlike the
	// churn test), so a dropped empty→non-empty Set() is always caught.
	[Test]
	public void Dispatch_Wakes_An_Idle_Parked_Worker() {
		using var w = new Rec(4);
		w.Start();

		Thread.Sleep(150); // let the consumer exhaust its spin budget and park on _itemEvt
		Assert.That(w.Dispatch(1), Is.True);
		WorkerTestUtil.AwaitProgress(() => CountOf(w), 1);

		Thread.Sleep(150); // worker re-parks (queue drained, empty again)
		Assert.That(w.Dispatch(2), Is.True);
		WorkerTestUtil.AwaitProgress(() => CountOf(w), 2);

		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 2);
		Assert.That(w.Processed, Is.EqualTo(new[] { 1, 2 }));
	}

	// Forces the producer to block on the space event repeatedly (slow consumer, tiny buffer), exercising
	// the full→non-full conditional Set(). Every blocked Dispatch must be re-signaled; no hang.
	[Test]
	public async Task Blocking_Producer_Is_Always_Re_Signaled_When_Space_Frees() {
		using var w = new Rec(2, hook: i => { if (i % 10 == 0) Thread.Sleep(1); });
		w.Start();

		const int n = 2_000;
		await Task.Run(() => {
			for (var i = 0; i < n; i++) w.Dispatch(i);
		});
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), n);

		Assert.That(w.Processed, Is.EqualTo(Enumerable.Range(0, n)));
	}

	// "Queue cleaned up on shutdown": Complete called while the buffer is FULL (consumer gated mid-process)
	// must still drain every committed item and resolve Completion within a bound — no item stranded.
	[Test]
	public void Complete_Drains_A_Full_Buffer() {
		var entered = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		using var w = new Rec(2, hook: i => { if (i == 0) { entered.Set(); gate.Wait(); } });
		w.Start();

		Assert.That(w.Dispatch(0), Is.True);
		Assert.That(entered.Wait(2000), Is.True,
			"consumer must pick up item 0 (head advances) before we fill the ring");
		Assert.That(w.Dispatch(1), Is.True);
		Assert.That(w.Dispatch(2), Is.True);

		Assert.That(w.TryComplete(), Is.True);
		gate.Set();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 3);

		Assert.That(w.Processed, Is.EqualTo(new[] { 0, 1, 2 }));
	}

	private static int CountOf(Rec w) {
		lock (w.Processed) return w.Processed.Count;
	}

	[Test]
	public void Dispose_Is_Idempotent() {
		var w = new Rec(8);
		w.Start();
		w.Dispose();
		Assert.DoesNotThrow(() => w.Dispose());
	}

	[Test]
	public void Pipeline_Propagates_Completion() {
		// Simulate: stageA.OnCompleted -> stageB.TryComplete
		using var stageB = new Rec(8);
		stageB.Start();

		using var stageA = new PipelineStage(8, stageB);
		stageA.Start();

		for (var i = 0; i < 20; i++) stageA.Dispatch(i);
		stageA.TryComplete();

		Assert.That(stageA.Completion.Wait(2000), Is.True);
		Assert.That(stageB.Completion.Wait(2000), Is.True);
		Assert.That(stageB.Processed, Has.Count.EqualTo(20));
	}

	// Shared, observable state the struct handler writes through (the struct is copied into the worker;
	// the test reads results via this reference).
	private sealed class RecordingState {
		public readonly List<Exception?> CompletedWith = new();
		public readonly List<Exception> Errors = new();
		public readonly List<int> Processed = new();
	}

	private readonly struct RecordingHandler : IAsyncWorkerHandler<int> {
		private readonly RecordingState _state;
		private readonly TimeSpan _syncDelay;
		private readonly TimeSpan _asyncDelay;
		private readonly Action<int>? _hook;
		private readonly bool _yield;

		public RecordingHandler(RecordingState state, TimeSpan syncDelay, TimeSpan asyncDelay,
			Action<int>? hook, bool yield) {
			_state = state;
			_syncDelay = syncDelay;
			_asyncDelay = asyncDelay;
			_hook = hook;
			_yield = yield;
		}

		public async ValueTask HandleAsync(int item, CancellationToken cancellationToken) {
			_hook?.Invoke(item);
			if (_syncDelay > TimeSpan.Zero) Thread.Sleep(_syncDelay);
			// Task.Yield forces the truly-async path (the _processDoneEvt rendezvous) with no timer cost,
			// so a boundary-churn test can hammer it across many iterations.
			if (_yield) await Task.Yield();
			if (_asyncDelay > TimeSpan.Zero) await Task.Delay(_asyncDelay, cancellationToken);
			lock (_state.Processed) _state.Processed.Add(item);
		}

		public void OnError(Exception error) {
			lock (_state.Errors) _state.Errors.Add(error);
		}

		public ValueTask OnCompletedAsync(Exception? error) {
			lock (_state.CompletedWith) _state.CompletedWith.Add(error);
			return default;
		}
	}

	// Thin harness mirroring the old Recording surface so the test bodies stay close to the original.
	private sealed class Rec : IDisposable {
		private readonly AsyncSpWorker<int, RecordingHandler> _worker;
		private readonly RecordingState _state = new();

		public Rec(int capacity,
			TimeSpan syncDelay = default,
			TimeSpan asyncDelay = default,
			Action<int>? hook = null,
			bool yield = false) =>
			_worker = new AsyncSpWorker<int, RecordingHandler>(
				new RecordingHandler(_state, syncDelay, asyncDelay, hook, yield), capacity);

		public List<int> Processed => _state.Processed;
		public List<Exception?> CompletedWith => _state.CompletedWith;
		public List<Exception> Errors => _state.Errors;
		public Task Completion => _worker.Completion;

		public void Start(CancellationToken ct = default) => _worker.Start(ct);
		public bool Dispatch(int item) => _worker.Dispatch(item);
		public bool TryComplete(Exception? exception = null) => _worker.TryComplete(exception);
		public void Dispose() => _worker.Dispose();
	}

	// Async pipeline stage: forwards each item downstream and propagates completion, mirroring the
	// source's PipelineStage worker subclass.
	private readonly struct PipelineHandler : IAsyncWorkerHandler<int> {
		private readonly Rec _downstream;

		public PipelineHandler(Rec downstream) => _downstream = downstream;

		public ValueTask HandleAsync(int item, CancellationToken cancellationToken) {
			_downstream.Dispatch(item);
			return default;
		}

		public void OnError(Exception error) { }

		public ValueTask OnCompletedAsync(Exception? error) {
			_downstream.TryComplete(error);
			return default;
		}
	}

	private sealed class PipelineStage : IDisposable {
		private readonly AsyncSpWorker<int, PipelineHandler> _worker;

		public PipelineStage(int capacity, Rec downstream) =>
			_worker = new AsyncSpWorker<int, PipelineHandler>(new PipelineHandler(downstream), capacity);

		public Task Completion => _worker.Completion;

		public void Start(CancellationToken ct = default) => _worker.Start(ct);
		public bool Dispatch(int item) => _worker.Dispatch(item);
		public bool TryComplete(Exception? exception = null) => _worker.TryComplete(exception);
		public void Dispose() => _worker.Dispose();
	}
}
