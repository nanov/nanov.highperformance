namespace Nanov.HighPerformance.Tests.Threading.Workers;

using System.Linq;
using System.Runtime.CompilerServices;
using Nanov.HighPerformance.Threading.Workers;
using NUnit.Framework;

[TestFixture]
public class SpWorkerTests {
	[Test]
	public void Capacity_Rounds_Up_To_Power_Of_2() {
		using var w = new Rec(5);
		w.Start();
		// 5 -> 8; dispatch 8 items, 9th would block. Just verify 8 work.
		for (var i = 0; i < 8; i++) Assert.That(w.Dispatch(i), Is.True);
		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Processed, Has.Count.EqualTo(8));
	}

	[Test]
	public void Dispatch_Process_InOrder() {
		using var w = new Rec(16);
		w.Start();
		for (var i = 0; i < 100; i++) w.Dispatch(i);
		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Processed, Is.EqualTo(Enumerable.Range(0, 100)));
	}

	[Test]
	public async Task Dispatch_Blocks_When_Full_Until_Space() {
		using var w = new Rec(2, TimeSpan.FromMilliseconds(100));
		w.Start();

		// Fill enough to actually block: buffer cap=2 + one being processed = 3 in flight
		w.Dispatch(1);
		w.Dispatch(2);
		w.Dispatch(3);

		// This 4th dispatch must wait for Process to free space.
		var dispatchTask = Task.Run(() => w.Dispatch(4));
		var completedImmediately = dispatchTask.Wait(20);
		Assert.That(completedImmediately, Is.False, "dispatch should block until space");

		Assert.That(await dispatchTask, Is.True);

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Processed, Is.EqualTo(new[] { 1, 2, 3, 4 }));
	}

	[Test]
	public void TryComplete_Drains_Remaining_And_Completes_Normally() {
		using var w = new Rec(16);
		w.Start();
		for (var i = 0; i < 50; i++) w.Dispatch(i);
		Assert.That(w.TryComplete(), Is.True);
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Processed, Has.Count.EqualTo(50));
		Assert.That(w.CompletedWith, Has.Count.EqualTo(1));
		Assert.That(w.CompletedWith[0], Is.Null);
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
	public void Dispatch_After_Dispose_Returns_False() {
		var w = new Rec(8);
		w.Start();
		w.Dispose();
		Assert.That(w.Dispatch(42), Is.False);
	}

	[Test]
	public void TryComplete_With_Exception_Faults_Completion() {
		using var w = new Rec(8);
		w.Start();
		w.Dispatch(1);
		var err = new InvalidOperationException("boom");
		Assert.That(w.TryComplete(err), Is.True);

		var agg = Assert.Throws<AggregateException>(() => w.Completion.Wait(2000));
		Assert.That(agg!.InnerException, Is.InstanceOf<InvalidOperationException>());

		Assert.That(w.CompletedWith, Has.Count.EqualTo(1));
		Assert.That(w.CompletedWith[0], Is.SameAs(err));
	}

	[Test]
	public void Cancellation_Stops_Worker_And_Completion_Is_Cancelled() {
		using var w = new Rec(16, TimeSpan.FromMilliseconds(50));
		using var cts = new CancellationTokenSource();
		w.Start(cts.Token);
		for (var i = 0; i < 100; i++) w.Dispatch(i);

		Thread.Sleep(80); // let a couple finish
		cts.Cancel();

		var agg = Assert.Throws<AggregateException>(() => w.Completion.Wait(2000));
		Assert.That(agg!.InnerException, Is.InstanceOf<OperationCanceledException>());

		Assert.That(w.Processed, Has.Count.LessThan(100));
		Assert.That(w.CompletedWith, Has.Count.EqualTo(1));
		Assert.That(w.CompletedWith[0], Is.InstanceOf<OperationCanceledException>());
	}

	[Test]
	public void Handler_Exception_Is_Reported_To_OnError_And_Worker_Continues() {
		using var w = new Rec(8, hook: i => {
			if (i == 2) throw new InvalidOperationException("handler failed");
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
	public async Task Concurrent_Dispatch_And_Process_Preserves_Order() {
		using var w = new Rec(16);
		w.Start();

		const int n = 10_000;
		// Single writer thread per SPSC contract
		await Task.Run(() => {
			for (var i = 0; i < n; i++) w.Dispatch(i);
		});
		w.TryComplete();
		// Stall watchdog: a dropped wakeup plateaus Processed and fails fast, never hangs the run.
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), n);

		Assert.That(w.Processed, Is.EqualTo(Enumerable.Range(0, n)));
	}

	// Regression guard for the edge-triggered signaling (see SpWorker SIGNALING INVARIANT). Tiny
	// capacity maximizes empty↔non-empty / full↔non-full transitions — the exact boundary the conditional
	// Set() gates — over many iterations. A dropped wakeup surfaces as a timeout here, not a wrong value.
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
		// Stall watchdog: fails in ~5s if a wakeup is lost (Processed plateaus), unbounded for a slow-but-live run.
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), n);

		Assert.That(w.Processed, Is.EqualTo(Enumerable.Range(0, n)));
	}

	// Deterministic lost-wakeup guard for the item signal. Unlike the churn stress test (where the consumer
	// rarely parks under load), this reliably parks the consumer on _itemEvt first, so a dropped
	// empty→non-empty Set() is ALWAYS caught: the lone Dispatch never gets processed → AwaitProgress fails.
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
		// 1ms delay on every 10th item keeps the buffer full and the producer parking on _spaceEvt.
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
		// head==1 now; cap=2 ring holds items 1 and 2 → buffer full while the consumer is parked in the hook.
		Assert.That(w.Dispatch(1), Is.True);
		Assert.That(w.Dispatch(2), Is.True);

		Assert.That(w.TryComplete(), Is.True);
		gate.Set();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 3);

		Assert.That(w.Processed, Is.EqualTo(new[] { 0, 1, 2 }));
	}

	// Gate the consumer inside item 0's hook while the ring fills to capacity, then release: the
	// consumer must drain the full backlog (items 1..15) in order with no lost wakeup.
	[Test]
	public void Drains_A_Full_Ring_Burst_In_Order() {
		var entered = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		using var w = new Rec(16, hook: i => {
			if (i == 0) {
				entered.Set();
				gate.Wait();
			}
		});
		w.Start();

		Assert.That(w.Dispatch(0), Is.True);
		Assert.That(entered.Wait(2000), Is.True, "consumer must enter item 0's hook (head still 0)");
		// Fill the rest of the cap-16 ring while the consumer is gated → items 1..15 sit committed.
		for (var i = 1; i < 16; i++) Assert.That(w.Dispatch(i), Is.True);

		gate.Set(); // release: consumer drains 1..15 in a single batch claim
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 16);

		Assert.That(w.Processed, Is.EqualTo(Enumerable.Range(0, 16)));
	}

	// Reference-type T must still have its ring slot cleared after the item is dequeued (TryDequeue
	// clears when IsReferenceOrContainsReferences<T>), so a drained item is collectable and the ring
	// doesn't pin it. The int-based tests only cover the skip-clear (blittable) side; this covers the
	// clear side — the guard for the blittable-skip optimization.
	[Test]
	public void Ref_Type_Slots_Are_Cleared_After_Drain() {
		var w = new SpWorker<object, NoopObjHandler>(new NoopObjHandler(), 8);
		w.Start();

		var weak = DispatchTransient(w);
		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);

		GC.Collect();
		GC.WaitForPendingFinalizers();
		GC.Collect();

		Assert.That(weak.TryGetTarget(out _), Is.False,
			"drained ring slot must be cleared so the item is collectable");
		GC.KeepAlive(w); // keep the buffer alive: proves the SLOT was cleared, not just the buffer freed
		w.Dispose();
	}

	// Object created and dispatched in a non-inlined frame so the only strong reference is the ring slot.
	[MethodImpl(MethodImplOptions.NoInlining)]
	private static WeakReference<object> DispatchTransient(SpWorker<object, NoopObjHandler> w) {
		var obj = new object();
		var weak = new WeakReference<object>(obj);
		Assert.That(w.Dispatch(obj), Is.True);
		return weak;
	}

	private readonly struct NoopObjHandler : IWorkerHandler<object> {
		public void Handle(object item) { }
		public void OnError(Exception error) { }
		public void OnCompleted(Exception? error) { }
	}

	[Test]
	public void Dispose_Is_Idempotent() {
		var w = new Rec(8);
		w.Start();
		w.Dispose();
		Assert.DoesNotThrow(() => w.Dispose());
	}

	[Test]
	public void Dispose_Never_Started_Resolves_Completion() {
		var w = new Rec(8);
		w.Dispose();
		Assert.That(w.Completion.Wait(2000), Is.True, "a never-started worker must not leave Completion pending");
		Assert.That(w.Completion.IsCompletedSuccessfully, Is.True);
	}

	[Test]
	public void Dispose_With_Stuck_Handler_Faults_Completion_With_Timeout() {
		using var gate = new ManualResetEventSlim(false);
		var w = new Rec(8, hook: _ => gate.Wait());
		w.Start();
		w.Dispatch(1);
		w.Dispatch(2);
		Assert.That(SpinWait.SpinUntil(() => w.Entered, 2000), Is.True, "handler never started");

		w.Dispose(); // the handler is parked on the gate — the join times out

		Assert.That(w.Completion.IsFaulted, Is.True, "a consumer that won't exit must not report success");
		var ex = w.Completion.Exception!.InnerException;
		Assert.That(ex, Is.InstanceOf<TimeoutException>());
		Assert.That(ex!.Message, Does.Contain("did not exit").And.Contain("queued"));

		gate.Set(); // unstick: the consumer exits on _stop without touching disposed handles
		Assert.That(SpinWait.SpinUntil(() => w.CompletedWith.Count == 1, 2000), Is.True);
		Assert.That(w.Errors, Is.Empty);
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

	private readonly struct RecordingHandler : IWorkerHandler<int> {
		private readonly RecordingState _state;
		private readonly TimeSpan _processDelay;
		private readonly Action<int>? _hook;

		public RecordingHandler(RecordingState state, TimeSpan processDelay, Action<int>? hook) {
			_state = state;
			_processDelay = processDelay;
			_hook = hook;
		}

		public void Handle(int item) {
			_hook?.Invoke(item);
			if (_processDelay > TimeSpan.Zero) Thread.Sleep(_processDelay);
			lock (_state.Processed) _state.Processed.Add(item);
		}

		public void OnError(Exception error) {
			lock (_state.Errors) _state.Errors.Add(error);
		}

		public void OnCompleted(Exception? error) {
			lock (_state.CompletedWith) _state.CompletedWith.Add(error);
		}
	}

	// Thin harness mirroring the old Recording surface so the test bodies stay close to the original.
	private sealed class Rec : IDisposable {
		private readonly SpWorker<int, RecordingHandler> _worker;
		private readonly RecordingState _state = new();

		public Rec(int capacity, TimeSpan processDelay = default, Action<int>? hook = null) =>
			_worker = new SpWorker<int, RecordingHandler>(new RecordingHandler(_state, processDelay, hook is null
				? null
				: i => {
					Volatile.Write(ref _entered, true);
					hook(i);
				}), capacity);

		private bool _entered;
		public bool Entered => Volatile.Read(ref _entered);

		public List<int> Processed => _state.Processed;
		public List<Exception?> CompletedWith => _state.CompletedWith;
		public List<Exception> Errors => _state.Errors;
		public Task Completion => _worker.Completion;

		public void Start(CancellationToken ct = default) => _worker.Start(ct);
		public bool Dispatch(int item) => _worker.Dispatch(item);
		public bool TryComplete(Exception? exception = null) => _worker.TryComplete(exception);
		public void Dispose() => _worker.Dispose();
	}
}
