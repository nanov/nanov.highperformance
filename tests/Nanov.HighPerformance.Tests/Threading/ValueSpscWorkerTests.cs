namespace Nanov.HighPerformance.Tests.Threading;

using System.Linq;
using Nanov.HighPerformance.Threading;
using NUnit.Framework;

[TestFixture]
public class ValueSpscWorkerTests {
	// --- Smoke tests --------------------------------------------------------------

	[Test]
	public void Publish_Process_RoundTrip() {
		using var w = new Rec(4);
		w.Start();

		using (var scope = w.Publish()) {
			Assert.That(scope.IsOpen, Is.True);
			scope.Event() = 42;
		}

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Processed, Is.EqualTo(new[] { 42 }));
	}

	[Test]
	public void Publish_Process_InOrder() {
		using var w = new Rec(16);
		w.Start();

		for (var i = 0; i < 100; i++) {
			using var scope = w.Publish();
			scope.Event() = i;
		}

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Processed, Is.EqualTo(Enumerable.Range(0, 100)));
	}

	[Test]
	public void Capacity_Rounds_Up_To_Power_Of_Two() {
		using var w = new Rec(5); // → 8
		w.Start();
		// Publish 8 items, then complete. The ring absorbs all 8 even though
		// we asked for 5.
		for (var i = 0; i < 8; i++) {
			using var scope = w.Publish();
			scope.Event() = i;
		}

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Processed, Has.Count.EqualTo(8));
	}

	// --- Publish lifecycle --------------------------------------------------------

	[Test]
	public void Publish_After_Complete_Returns_Closed_Scope() {
		using var w = new Rec(4);
		w.Start();
		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);

		using var scope = w.Publish();
		Assert.That(scope.IsOpen, Is.False);
		// A ref struct scope can't be captured in a lambda, so we can't use
		// Assert.Throws here — catch manually.
		var threw = false;
		try {
			_ = scope.Event();
		}
		catch (InvalidOperationException) {
			threw = true;
		}

		Assert.That(threw, Is.True);
	}

	[Test]
	public void Publish_On_Default_Scope_Is_A_No_Op_Dispose() {
		// Default scope has IsOpen=false and Dispose must not NRE.
		var scope = default(PublishScope<int>);
		Assert.That(scope.IsOpen, Is.False);
		scope.Dispose(); // no-op
	}

	[Test]
	public async Task Publish_Blocks_When_Full_Until_Consumer_Drains() {
		using var w = new Rec(2); // cap=2, no early release
		var allowProcess = new ManualResetEventSlim(false);
		w.Hook = _ => allowProcess.Wait();
		w.Start();

		// Without early Release, the loop's safety-net Release only fires after Hook
		// returns. Once Hook blocks on `allowProcess`, the loop is stuck in Handle for
		// slot 0 → _head never advances → publish(3) must block.
		// Cap=2 ⇒ 2 publishes succeed (one being processed from slot 0, one buffered in
		// slot 1), the 3rd has to wait for space.
		using (var s = w.Publish()) {
			s.Event() = 1;
		}

		using (var s = w.Publish()) {
			s.Event() = 2;
		}

		var published3 = false;
		var t = Task.Run(() => {
			using var scope = w.Publish();
			scope.Event() = 3;
			published3 = true;
		});

		// The 3rd publish should be blocked in Publish.
		await Task.Delay(100);
		Assert.That(published3, Is.False);

		// Let the handler drain. After Hook returns, loop releases slot 0 → publish(3) unblocks.
		allowProcess.Set();
		await t.WaitAsync(TimeSpan.FromSeconds(2));
		Assert.That(published3, Is.True);

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Processed, Is.EqualTo(new[] { 1, 2, 3 }));
	}

	// --- Release semantics --------------------------------------------------------

	[Test]
	public void Safety_Net_Release_When_Handler_Doesnt() {
		// Handler never calls Release. The loop's finally must release so _head
		// advances and we can process further items.
		using var w = new Rec(2) { ReleaseEarly = false };
		w.Start();

		for (var i = 0; i < 10; i++) {
			using var scope = w.Publish();
			scope.Event() = i;
		}

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Processed, Is.EqualTo(Enumerable.Range(0, 10)));
	}

	[Test]
	public async Task Early_Release_Unblocks_Producer_Before_Handler_Returns() {
		// Capacity 2. Sequence:
		//   publish(1) → slot 0. Loop picks it up, Release early → head advances, Hook blocks.
		//   publish(2) → slot 1.
		//   publish(3) → would block WITHOUT early Release (buffer full, handler stuck),
		//                but succeeds WITH early Release (head already advanced by 1).
		// We synchronize on handlerReachedHook before publish(3) so the race is removed
		// and the test is deterministic.
		using var w = new Rec(2) { ReleaseEarly = true };
		var allowProcessFinish = new ManualResetEventSlim(false);
		var handlerReachedHook = new ManualResetEventSlim(false);
		w.Hook = _ => {
			handlerReachedHook.Set();
			allowProcessFinish.Wait();
		};
		w.Start();

		using (var s = w.Publish()) {
			s.Event() = 1;
		}

		using (var s = w.Publish()) {
			s.Event() = 2;
		}

		Assert.That(handlerReachedHook.Wait(1000), Is.True,
			"handler should have released item 1 and entered the hook");

		// Release of slot 0 freed one unit of head-space; publish(3) should go through
		// without waiting. Off-thread so the assertion uses WhenAny as a timeout guard.
		var publish3Task = Task.Run(() => {
			using var scope = w.Publish();
			scope.Event() = 3;
		});
		var completed = await Task.WhenAny(publish3Task, Task.Delay(1000));
		Assert.That(completed, Is.EqualTo(publish3Task),
			"early Release should unblock the producer while the handler is still running");

		allowProcessFinish.Set();
		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
	}

	[Test]
	public void Handler_Throw_Reported_To_OnError_And_Worker_Continues() {
		// If the handler throws before calling Release, the loop's finally must release so
		// subsequent items flow. The exception is no longer swallowed — it goes to OnError
		// (synchronously) and the worker continues draining.
		using var w = new Rec(4) { ThrowOnProcess = true };
		w.Start();

		for (var i = 0; i < 5; i++) {
			using var scope = w.Publish();
			scope.Event() = i;
		}

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		// Handler threw on all of them → nothing recorded, but every throw is reported to
		// OnError and the worker must have drained (not deadlocked) for Completion to resolve.
		Assert.That(w.Processed, Is.Empty);
		Assert.That(w.Errors, Has.Count.EqualTo(5));
		Assert.That(w.Errors[0], Is.InstanceOf<InvalidOperationException>());
	}

	[Test]
	public void Handler_Release_Then_Throw_Does_Not_Double_Advance_Head() {
		// ReleaseEarly + ThrowOnProcess: handler releases the slot, then throws.
		// The loop's finally calls scope.Release() again — the idempotent IsReleased flag
		// makes it a no-op. A double-advance would desynchronize head/tail and either consume
		// an unpublished slot or hang. We verify by pumping 10 items and confirming the
		// worker drains to completion (no hang) even though every handler throws.
		using var w = new Rec(4) { ReleaseEarly = true, ThrowOnProcess = true };
		w.Start();

		for (var i = 0; i < 10; i++) {
			using var scope = w.Publish();
			scope.Event() = i;
		}

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True,
			"worker must drain cleanly even with release-then-throw pattern");
		Assert.That(w.Processed, Is.Empty);
		Assert.That(w.Errors, Has.Count.EqualTo(10));
	}

	// --- Completion / cancellation ------------------------------------------------

	[Test]
	public void Completion_Drains_Pending_And_Invokes_OnCompleted() {
		using var w = new Rec(16);
		w.Start();

		for (var i = 0; i < 20; i++) {
			using var scope = w.Publish();
			scope.Event() = i;
		}

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);

		Assert.That(w.Processed, Is.EqualTo(Enumerable.Range(0, 20)));
		Assert.That(w.CompletedWith, Has.Count.EqualTo(1));
		Assert.That(w.CompletedWith[0], Is.Null);
	}

	[Test]
	public void TryComplete_Returns_True_Only_Once() {
		using var w = new Rec(4);
		w.Start();
		Assert.That(w.TryComplete(), Is.True);
		Assert.That(w.TryComplete(), Is.False);
		Assert.That(w.Completion.Wait(2000), Is.True);
	}

	[Test]
	public async Task Cancellation_Stops_The_Worker() {
		using var w = new Rec(4);
		var cts = new CancellationTokenSource();
		w.Start(cts.Token);

		cts.Cancel();
		// Completion should resolve as cancelled.
		var completed = await Task.WhenAny(w.Completion, Task.Delay(2000));
		Assert.That(completed, Is.EqualTo(w.Completion));
		Assert.That(w.Completion.IsCanceled, Is.True);
	}

	[Test]
	public void OnCompleted_Throw_Still_Resolves_Completion() {
		// The loop wraps the OnCompleted call so a throwing OnCompleted must not leave
		// Completion unresolved (the throw is routed to OnError).
		using var w = new Rec(4) { ThrowOnCompleted = true };
		w.Start();
		using (var s = w.Publish()) {
			s.Event() = 1;
		}

		w.TryComplete();

		Assert.That(w.Completion.Wait(2000), Is.True, "Completion must resolve even when OnCompleted throws");
		Assert.That(w.Processed, Is.EqualTo(new[] { 1 }));
		Assert.That(w.CompletedWith, Has.Count.EqualTo(1));
		Assert.That(w.CompletedWith[0], Is.Null);
	}

	[Test]
	public async Task Concurrent_Publish_And_TryComplete_Is_Clean() {
		// Producer publishes in a tight loop from one thread; another calls TryComplete
		// mid-stream. Every Publish must either succeed or return a closed scope — no
		// hang, no crash. Final published values must all be processed.
		using var w = new Rec(16);
		w.Start();

		var cts = new CancellationTokenSource();
		var published = 0;
		var publisher = Task.Run(() => {
			var i = 0;
			while (!cts.IsCancellationRequested) {
				using var scope = w.Publish();
				if (!scope.IsOpen) return;
				scope.Event() = i++;
				Interlocked.Increment(ref published);
			}
		});

		await Task.Delay(50); // let some publishes land
		w.TryComplete(); // race — fires while publisher is still looping
		cts.CancelAfter(500); // belt and suspenders: exit publisher even if race converges weirdly

		await publisher.WaitAsync(TimeSpan.FromSeconds(2));
		Assert.That(w.Completion.Wait(2000), Is.True);

		// `published` is incremented after the scope is written but before its using-Dispose commits
		// the slot (advances Tail). Publish() reserves a slot without advancing Tail, and soft-Complete
		// drains until Head == Tail then exits — unaware of a reserved-but-uncommitted slot. So the one
		// publish that is mid reserve→commit exactly when TryComplete fires can be dropped: counted in
		// `published`, but its commit lands after the worker has exited. With a single producer thread
		// at most one publish is ever in flight, so the gap is at most one. (This cross-thread race
		// can't happen in production: dispatch Publish() and OnPartitionsRevoked's TryComplete() both
		// run on the consume thread, serialized by Confluent's synchronous rebalance callbacks.)
		var finalPublished = Volatile.Read(ref published);
		Assert.That(w.Processed, Has.Count.InRange(finalPublished - 1, finalPublished),
			"every committed publish is processed except, at most, the single one in flight when "
			+ "TryComplete raced in");
	}

	// --- Edge-triggered signaling regression guards -------------------------------

	// Regression guard for the edge-triggered ring signaling (see ValueSpscWorkerBase SIGNALING
	// INVARIANT). Tiny capacity maximizes empty↔non-empty / full↔non-full transitions over many
	// iterations; a dropped wakeup surfaces as a timeout (stall watchdog fires), not a wrong value.
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

	// Forces the producer to block on the space event repeatedly (slow consumer, tiny buffer), exercising
	// the full→non-full conditional Set(). Every blocked Publish must be re-signaled; no hang.
	[Test]
	public async Task Blocking_Producer_Is_Always_Re_Signaled_When_Space_Frees() {
		using var w = new Rec(2);
		w.Hook = arg => { if (arg.Value % 10 == 0) Thread.Sleep(1); };
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

	// "Queue cleaned up on shutdown": Complete called while the buffer is FULL (consumer gated mid-process)
	// must still drain every committed item and resolve Completion within a bound — no item stranded.
	[Test]
	public void Complete_Drains_A_Full_Buffer() {
		var entered = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		using var w = new Rec(2);
		w.Hook = arg => { if (arg.Value == 0) { entered.Set(); gate.Wait(); } };
		w.Start();

		using (var s = w.Publish()) s.Event() = 0;
		Assert.That(entered.Wait(2000), Is.True, "consumer must enter Handle(0) before we fill the ring");
		// head still 0 (released only after Handle); cap=2 ring now holds 0 and 1 → full.
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
	// the test reads results and toggles config via this reference).
	private sealed class RecordingState {
		public readonly List<Exception?> CompletedWith = new();
		public readonly List<Exception> Errors = new();
		public readonly List<int> Processed = new();
		public Action<HookArg>? Hook;
		public bool ReleaseEarly;
		public bool ThrowOnCompleted;
		public bool ThrowOnProcess;
	}

	// Struct handler that records every processed slot. Reads its config from the shared reference-type
	// state so tests can toggle flags / set the hook after construction (the struct is copied by value).
	private readonly struct RecordingHandler : IValueSpscHandler<int> {
		private readonly RecordingState _state;

		public RecordingHandler(RecordingState state) => _state = state;

		public void Handle(ref HandleScope<int> scope) {
			ref var slot = ref scope.Event();
			var value = slot;
			if (_state.ReleaseEarly) scope.Release();
			_state.Hook?.Invoke(new HookArg(value,
				_state.ReleaseEarly ? HandleScopeState.Released : HandleScopeState.Held));
			if (_state.ThrowOnProcess) throw new InvalidOperationException("bang");
			lock (_state.Processed) _state.Processed.Add(value);
		}

		public void OnError(Exception error) {
			lock (_state.Errors) _state.Errors.Add(error);
		}

		public void OnCompleted(Exception? error) {
			lock (_state.CompletedWith) _state.CompletedWith.Add(error);
			if (_state.ThrowOnCompleted) throw new InvalidOperationException("bye");
		}
	}

	// Thin harness mirroring the old Recording surface so the test bodies stay close to the original.
	private sealed class Rec : IDisposable {
		private readonly ValueSpscWorker<int, RecordingHandler> _worker;
		private readonly RecordingState _state = new();

		public Rec(int capacity) =>
			_worker = new ValueSpscWorker<int, RecordingHandler>(new RecordingHandler(_state), capacity);

		public List<int> Processed => _state.Processed;
		public List<Exception?> CompletedWith => _state.CompletedWith;
		public List<Exception> Errors => _state.Errors;

		public Action<HookArg>? Hook {
			get => _state.Hook;
			set => _state.Hook = value;
		}

		public bool ReleaseEarly {
			get => _state.ReleaseEarly;
			set => _state.ReleaseEarly = value;
		}

		public bool ThrowOnProcess {
			get => _state.ThrowOnProcess;
			set => _state.ThrowOnProcess = value;
		}

		public bool ThrowOnCompleted {
			get => _state.ThrowOnCompleted;
			set => _state.ThrowOnCompleted = value;
		}

		public Task Completion => _worker.Completion;

		public PublishScope<int> Publish() => _worker.Publish();
		public void Start(CancellationToken ct = default) => _worker.Start(ct);
		public bool TryComplete(Exception? exception = null) => _worker.TryComplete(exception);
		public void Dispose() => _worker.Dispose();
	}

	private enum HandleScopeState {
		Held,
		Released
	}

	private sealed record HookArg(int Value, HandleScopeState State);
}
