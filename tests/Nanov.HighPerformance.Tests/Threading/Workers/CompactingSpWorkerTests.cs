namespace Nanov.HighPerformance.Tests.Threading.Workers;

using System.Linq;
using System.Runtime.CompilerServices;
using Nanov.HighPerformance.Threading.Workers;
using NUnit.Framework;

[TestFixture]
public class CompactingSpWorkerTests {
	// Shared observable state the struct handler writes through (struct is copied into the worker;
	// the test reads results via this reference). Handle runs on the worker thread; Merge runs on the
	// producer thread and must be pure (it does NOT touch this state).
	private sealed class RecordingState {
		public readonly List<(int Key, int Value)> Processed = new();
		public readonly List<int> Discarded = new();
		public readonly List<Exception> Errors = new();
		public readonly List<Exception?> CompletedWith = new();
	}

	// Reference payload (TValue : class) carrying an int.
	private sealed class Boxed {
		public readonly int Value;
		public Boxed(int value) => Value = value;
	}

	// Replace-style merge: keep the latest value (return incoming).
	private readonly struct LatestHandler : ICompactingWorkerHandler<int, Boxed> {
		private readonly RecordingState _state;
		private readonly TimeSpan _delay;
		private readonly Action<int>? _hook;

		public LatestHandler(RecordingState state, TimeSpan delay, Action<int>? hook) {
			_state = state;
			_delay = delay;
			_hook = hook;
		}

		public Boxed Merge(Boxed current, Boxed incoming) => incoming; // latest-wins, pure

		public void Discarded(Boxed value) {
			lock (_state.Discarded) _state.Discarded.Add(value.Value);
		}

		public void Handle(int key, Boxed value) {
			_hook?.Invoke(key);
			if (_delay > TimeSpan.Zero) Thread.Sleep(_delay);
			lock (_state.Processed) _state.Processed.Add((key, value.Value));
		}

		public void OnError(Exception error) {
			lock (_state.Errors) _state.Errors.Add(error);
		}

		public void OnCompleted(Exception? error) {
			lock (_state.CompletedWith) _state.CompletedWith.Add(error);
		}
	}

	private sealed class Rec : IDisposable {
		private readonly CompactingSpWorker<int, Boxed, LatestHandler> _worker;
		private readonly RecordingState _state = new();

		public Rec(int capacity, TimeSpan delay = default, Action<int>? hook = null) =>
			_worker = new CompactingSpWorker<int, Boxed, LatestHandler>(new LatestHandler(_state, delay, hook), capacity);

		public List<(int Key, int Value)> Processed => _state.Processed;
		public List<int> Discarded => _state.Discarded;
		public List<Exception> Errors => _state.Errors;
		public List<Exception?> CompletedWith => _state.CompletedWith;
		public Task Completion => _worker.Completion;

		public void Start(CancellationToken ct = default) => _worker.Start(ct);
		public bool Dispatch(int key, int value) => _worker.Dispatch(key, new Boxed(value));
		public bool TryComplete(Exception? ex = null) => _worker.TryComplete(ex);
		public void Dispose() => _worker.Dispose();
	}

	private static int CountOf(Rec w) {
		lock (w.Processed) return w.Processed.Count;
	}

	[Test]
	public void Distinct_Keys_Process_In_First_Seen_Order() {
		using var w = new Rec(16);
		w.Start();
		for (var i = 0; i < 50; i++) Assert.That(w.Dispatch(i, i * 10), Is.True);
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 50);
		Assert.That(w.Processed.Select(p => p.Key), Is.EqualTo(Enumerable.Range(0, 50)));
		Assert.That(w.Processed.All(p => p.Value == p.Key * 10), Is.True);
	}

	[Test]
	public void Capacity_Rounds_Up_To_Power_Of_2() {
		using var w = new Rec(5);
		w.Start();
		for (var i = 0; i < 8; i++) Assert.That(w.Dispatch(i, i), Is.True);
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 8);
		Assert.That(w.Processed, Has.Count.EqualTo(8));
	}

	// The defining behavior: re-dispatching a queued key overrides its value in place, keeps its
	// position, and the consumer sees the LATEST value. Gate the consumer so all updates land while
	// the key is still queued.
	[Test]
	public void Same_Key_Conflates_To_Latest_Keeping_Position() {
		var entered = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		// First item (key 0) blocks in the hook so keys 1 and 2 sit queued while we re-dispatch key 1.
		using var w = new Rec(16, hook: k => {
			if (k == 0) {
				entered.Set();
				gate.Wait();
			}
		});
		w.Start();

		Assert.That(w.Dispatch(0, 0), Is.True);
		Assert.That(entered.Wait(2000), Is.True);
		// Queue (after key 0 picked up): key 1, key 2 — then conflate key 1 three times.
		Assert.That(w.Dispatch(1, 100), Is.True);
		Assert.That(w.Dispatch(2, 200), Is.True);
		Assert.That(w.Dispatch(1, 101), Is.True); // override in place
		Assert.That(w.Dispatch(1, 102), Is.True); // override again

		gate.Set();
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 3);

		// key 1 kept its position (before key 2) and carries the LATEST value 102.
		Assert.That(w.Processed, Is.EqualTo(new[] { (0, 0), (1, 102), (2, 200) }));
	}

	[Test]
	public void Merge_Hook_Combines_Values() {
		// A summing merge to prove Merge's result is what Handle sees (still replace-style: each
		// returned Boxed is self-contained — it is the running total, not a delta).
		var entered = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		var state = new RecordingState();
		var worker = new CompactingSpWorker<int, Boxed, SumHandler>(new SumHandler(state, entered, gate), 16);
		worker.Start();

		Assert.That(worker.Dispatch(0, new Boxed(0)), Is.True); // blocks consumer in hook
		Assert.That(entered.Wait(2000), Is.True);
		Assert.That(worker.Dispatch(7, new Boxed(10)), Is.True);
		Assert.That(worker.Dispatch(7, new Boxed(5)), Is.True);  // merged: 10 + 5
		Assert.That(worker.Dispatch(7, new Boxed(1)), Is.True);  // merged: 15 + 1

		gate.Set();
		worker.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(worker.Completion,
			() => { lock (state.Processed) return state.Processed.Count; }, 2);

		Assert.That(state.Processed, Does.Contain((7, 16)));
		worker.Dispose();
	}

	// Summing replace-style handler: Merge returns the running total as a fresh self-contained value.
	private readonly struct SumHandler : ICompactingWorkerHandler<int, Boxed> {
		private readonly RecordingState _state;
		private readonly ManualResetEventSlim _entered;
		private readonly ManualResetEventSlim _gate;

		public SumHandler(RecordingState state, ManualResetEventSlim entered, ManualResetEventSlim gate) {
			_state = state;
			_entered = entered;
			_gate = gate;
		}

		public Boxed Merge(Boxed current, Boxed incoming) => new(current.Value + incoming.Value);

		public void Discarded(Boxed value) {
			lock (_state.Discarded) _state.Discarded.Add(value.Value);
		}

		public void Handle(int key, Boxed value) {
			if (key == 0) {
				_entered.Set();
				_gate.Wait();
			}

			lock (_state.Processed) _state.Processed.Add((key, value.Value));
		}

		public void OnError(Exception error) {
			lock (_state.Errors) _state.Errors.Add(error);
		}

		public void OnCompleted(Exception? error) {
			lock (_state.CompletedWith) _state.CompletedWith.Add(error);
		}
	}

	[Test]
	public void TryComplete_Drains_And_Completes_Normally() {
		using var w = new Rec(16);
		w.Start();
		for (var i = 0; i < 40; i++) w.Dispatch(i, i);
		Assert.That(w.TryComplete(), Is.True);
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 40);
		Assert.That(w.Processed, Has.Count.EqualTo(40));
		Assert.That(w.CompletedWith, Has.Count.EqualTo(1));
		Assert.That(w.CompletedWith[0], Is.Null);
	}

	[Test]
	public void Dispatch_After_Complete_Returns_False() {
		using var w = new Rec(8);
		w.Start();
		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Dispatch(1, 1), Is.False);
	}

	[Test]
	public void Handler_Exception_Is_Reported_To_OnError_And_Worker_Continues() {
		using var w = new Rec(8, hook: k => {
			if (k == 2) throw new InvalidOperationException("handler failed");
		});
		w.Start();
		for (var i = 0; i < 5; i++) w.Dispatch(i, i);
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 4);
		Assert.That(w.Processed.Select(p => p.Key), Is.SupersetOf(new[] { 0, 1, 3, 4 }));
		Assert.That(w.Errors, Has.Count.EqualTo(1));
		Assert.That(w.Errors[0], Is.InstanceOf<InvalidOperationException>());
	}

	[Test]
	public void Cancellation_Stops_And_Completion_Is_Cancelled() {
		using var w = new Rec(16, TimeSpan.FromMilliseconds(50));
		using var cts = new CancellationTokenSource();
		w.Start(cts.Token);
		for (var i = 0; i < 100; i++) w.Dispatch(i, i);
		Thread.Sleep(80);
		cts.Cancel();
		var agg = Assert.Throws<AggregateException>(() => w.Completion.Wait(2000));
		Assert.That(agg!.InnerException, Is.InstanceOf<OperationCanceledException>());
		Assert.That(w.CompletedWith[0], Is.InstanceOf<OperationCanceledException>());
	}

	// Superseded values are reported to Discarded (last-wins → the old value is dropped). Consumer is
	// parked on key 0, so every key-1 conflation takes the clean trust path → deterministic accounting.
	[Test]
	public void Superseded_Values_Are_Discarded_Latest_Wins() {
		var entered = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		using var w = new Rec(16, hook: k => {
			if (k == 0) {
				entered.Set();
				gate.Wait();
			}
		});
		w.Start();

		Assert.That(w.Dispatch(0, 0), Is.True);
		Assert.That(entered.Wait(2000), Is.True);
		Assert.That(w.Dispatch(1, 100), Is.True);
		Assert.That(w.Dispatch(1, 101), Is.True); // supersedes 100
		Assert.That(w.Dispatch(1, 102), Is.True); // supersedes 101

		gate.Set();
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 2);

		Assert.That(w.Discarded, Is.EqualTo(new[] { 100, 101 }), "dropped values must be reported to Discarded");
		Assert.That(w.Processed, Is.EqualTo(new[] { (0, 0), (1, 102) }), "only survivors reach Handle");
		// Accounting: {0,102} Handled ∪ {100,101} Discarded covers every dispatched value, disjointly.
	}

	// The exact pooled-resource scenario: every rented value round-trips back to the pool exactly once —
	// via Handle (survivors) or Discarded (conflated-away) — with no double-free and no leak.
	[Test]
	public void Pooled_Values_Are_Released_Exactly_Once() {
		var pool = new TestPool();
		var entered = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		using var w = new CompactingSpWorker<int, Pooled, PoolHandler>(new PoolHandler(pool, entered, gate), 16);
		w.Start();

		Assert.That(w.Dispatch(0, pool.Rent()), Is.True); // key 0 blocks the consumer
		Assert.That(entered.Wait(2000), Is.True);

		// 50 updates across 5 keys while the consumer is parked → heavy conflation, all trust-path.
		for (var i = 0; i < 50; i++)
			Assert.That(w.Dispatch(1 + (i % 5), pool.Rent()), Is.True);

		gate.Set();
		w.TryComplete();
		Assert.That(w.Completion.Wait(5000), Is.True);

		Assert.That(pool.Rented, Is.EqualTo(51));
		Assert.That(pool.DoubleFrees, Is.Zero, "no value released twice (no double-free / use-after-free)");
		Assert.That(pool.Outstanding, Is.Zero, "every rented value released exactly once (no leak)");
	}

	private sealed class Pooled {
		public bool Freed;
	}

	// Tracks rent/free with a per-object guard so a double-release (or a leak) fails the test.
	private sealed class TestPool {
		private int _outstanding;
		public int Rented { get; private set; }
		public int DoubleFrees { get; private set; }
		public int Outstanding => Volatile.Read(ref _outstanding);

		public Pooled Rent() {
			Rented++; // producer thread only (single producer)
			Interlocked.Increment(ref _outstanding);
			return new Pooled();
		}

		public void Free(Pooled p) {
			// Handle runs on the worker thread, Discarded on the producer thread → guard the flag.
			lock (this) {
				if (p.Freed) {
					DoubleFrees++;
					return;
				}

				p.Freed = true;
			}

			Interlocked.Decrement(ref _outstanding);
		}
	}

	private readonly struct PoolHandler : ICompactingWorkerHandler<int, Pooled> {
		private readonly TestPool _pool;
		private readonly ManualResetEventSlim _entered;
		private readonly ManualResetEventSlim _gate;

		public PoolHandler(TestPool pool, ManualResetEventSlim entered, ManualResetEventSlim gate) {
			_pool = pool;
			_entered = entered;
			_gate = gate;
		}

		public Pooled Merge(Pooled current, Pooled incoming) => incoming; // last-wins
		public void Discarded(Pooled value) => _pool.Free(value);

		public void Handle(int key, Pooled value) {
			if (key == 0) {
				_entered.Set();
				_gate.Wait();
			}

			_pool.Free(value);
		}

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

	// Distinct-key flood with a stall watchdog — a dropped wakeup plateaus progress and fails fast.
	[Test]
	public async Task Distinct_Key_Flood_Preserves_Order() {
		using var w = new Rec(16);
		w.Start();
		const int n = 10_000;
		await Task.Run(() => {
			for (var i = 0; i < n; i++) w.Dispatch(i, i);
		});
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), n);
		Assert.That(w.Processed.Select(p => p.Key), Is.EqualTo(Enumerable.Range(0, n)));
		Assert.That(w.Processed.All(p => p.Value == p.Key), Is.True);
	}

	// Lost-update / lost-wakeup stress: a small key space heavily re-dispatched against a slightly slow
	// consumer maximizes the in-place-override race. Guarantee: for every key, the LAST value dispatched
	// is delivered at some point (latest-wins merge). Watchdog catches a hang from a dropped wakeup.
	[TestCase(1)]
	[TestCase(2)]
	[TestCase(8)]
	public async Task Conflation_Stress_Delivers_Latest_Per_Key(int capacity) {
		using var w = new Rec(capacity, hook: _ => Thread.SpinWait(50));
		w.Start();

		const int keys = 4;
		const int rounds = 30_000;
		var lastSent = new int[keys];
		await Task.Run(() => {
			for (var r = 0; r < rounds; r++) {
				var k = r % keys;
				var v = r;
				lastSent[k] = v;
				while (!w.Dispatch(k, v)) { } // single producer
			}
		});
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), keys); // at least one per key

		// For each key, the final delivered value must equal the last value we sent for that key.
		var lastSeen = new Dictionary<int, int>();
		lock (w.Processed)
			foreach (var (k, v) in w.Processed)
				lastSeen[k] = v;
		for (var k = 0; k < keys; k++) {
			Assert.That(lastSeen.ContainsKey(k), Is.True, $"key {k} was never delivered");
			Assert.That(lastSeen[k], Is.EqualTo(lastSent[k]),
				$"key {k}: last delivered {lastSeen[k]} != last sent {lastSent[k]} (lost update)");
		}
	}

	// Backpressure: with capacity distinct keys pending and the consumer gated, a NEW distinct key blocks
	// until space frees; conflating an EXISTING key never blocks.
	[Test]
	public async Task New_Key_Blocks_When_Full_But_Conflation_Does_Not() {
		var entered = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		using var w = new Rec(2, hook: k => {
			if (k == 0) {
				entered.Set();
				gate.Wait();
			}
		});
		w.Start();

		Assert.That(w.Dispatch(0, 0), Is.True);
		Assert.That(entered.Wait(2000), Is.True); // head advanced to 1; ring (cap 2) can hold 2 more
		Assert.That(w.Dispatch(1, 1), Is.True);
		Assert.That(w.Dispatch(2, 2), Is.True); // ring now full (keys 1,2 pending)

		// Conflating an already-pending key must NOT block.
		var conflate = Task.Run(() => w.Dispatch(1, 100));
		Assert.That(conflate.Wait(200), Is.True, "conflation of a pending key must not block");
		Assert.That(await conflate, Is.True);

		// A NEW distinct key must block until the consumer frees a slot.
		var blocked = Task.Run(() => w.Dispatch(3, 3));
		Assert.That(blocked.Wait(50), Is.False, "new key must block when full");

		gate.Set();
		Assert.That(await blocked, Is.True);
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 4);

		// key 1 delivered its latest (100); key 3 eventually delivered.
		var lastSeen = new Dictionary<int, int>();
		lock (w.Processed)
			foreach (var (k, v) in w.Processed)
				lastSeen[k] = v;
		Assert.That(lastSeen[1], Is.EqualTo(100));
		Assert.That(lastSeen.ContainsKey(3), Is.True);
	}

	// Deterministic wake of a parked consumer (mirrors SpWorkerTests).
	[Test]
	public void Dispatch_Wakes_An_Idle_Parked_Worker() {
		using var w = new Rec(4);
		w.Start();
		Thread.Sleep(150); // exhaust spin budget, park on _itemEvt
		Assert.That(w.Dispatch(1, 10), Is.True);
		WorkerTestUtil.AwaitProgress(() => CountOf(w), 1);
		Thread.Sleep(150);
		Assert.That(w.Dispatch(2, 20), Is.True);
		WorkerTestUtil.AwaitProgress(() => CountOf(w), 2);
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 2);
		Assert.That(w.Processed, Is.EqualTo(new[] { (1, 10), (2, 20) }));
	}

	// Map eviction: a long flood of one-shot keys must not grow unbounded — the producer compacts. We
	// can't read the map directly (it's producer-private), so assert the run completes and stays correct
	// with far more distinct keys than capacity (forces repeated compaction).
	[Test]
	public async Task Many_One_Shot_Keys_Force_Compaction_And_Stay_Correct() {
		using var w = new Rec(8);
		w.Start();
		const int n = 100_000;
		await Task.Run(() => {
			for (var i = 0; i < n; i++) w.Dispatch(i, i);
		});
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), n);
		Assert.That(w.Processed, Has.Count.EqualTo(n));
		Assert.That(w.Processed.Select(p => p.Key), Is.EqualTo(Enumerable.Range(0, n)));
	}

	// Gap 1: a combine merge (fresh result) must Discard BOTH inputs — current and incoming — since
	// neither is the returned value. Consumer parked → clean trust path → deterministic.
	[Test]
	public void Combine_Merge_Discards_Both_Inputs() {
		var entered = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		var state = new RecordingState();
		using var w = new CompactingSpWorker<int, Boxed, SumHandler>(new SumHandler(state, entered, gate), 16);
		w.Start();

		Assert.That(w.Dispatch(0, new Boxed(0)), Is.True); // blocks consumer
		Assert.That(entered.Wait(2000), Is.True);
		Assert.That(w.Dispatch(7, new Boxed(10)), Is.True); // fresh
		Assert.That(w.Dispatch(7, new Boxed(5)), Is.True);  // merge(10,5)=15 → discard 10 and 5
		Assert.That(w.Dispatch(7, new Boxed(1)), Is.True);  // merge(15,1)=16 → discard 15 and 1

		gate.Set();
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => { lock (state.Processed) return state.Processed.Count; }, 2);

		// Every input + intermediate result is discarded except the final survivor (16, delivered).
		Assert.That(state.Discarded, Is.EquivalentTo(new[] { 10, 5, 15, 1 }));
		Assert.That(state.Processed, Does.Contain((7, 16)));
	}

	// Gap 2: keep-existing merge (return current) must Discard the incoming (the dropped input).
	[Test]
	public void KeepExisting_Merge_Discards_Incoming() {
		var entered = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		var state = new RecordingState();
		using var w = new CompactingSpWorker<int, Boxed, KeepFirstHandler>(new KeepFirstHandler(state, entered, gate), 16);
		w.Start();

		Assert.That(w.Dispatch(0, new Boxed(0)), Is.True); // blocks consumer
		Assert.That(entered.Wait(2000), Is.True);
		Assert.That(w.Dispatch(7, new Boxed(100)), Is.True); // fresh (the kept value)
		Assert.That(w.Dispatch(7, new Boxed(5)), Is.True);   // return current → discard incoming 5
		Assert.That(w.Dispatch(7, new Boxed(1)), Is.True);   // return current → discard incoming 1

		gate.Set();
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => { lock (state.Processed) return state.Processed.Count; }, 2);

		Assert.That(state.Discarded, Is.EquivalentTo(new[] { 5, 1 }), "incoming values are dropped, current is kept");
		Assert.That(state.Processed, Does.Contain((7, 100)), "the first (kept) value is delivered");
	}

	// keep-existing handler: Merge returns current (ignore incoming).
	private readonly struct KeepFirstHandler : ICompactingWorkerHandler<int, Boxed> {
		private readonly RecordingState _state;
		private readonly ManualResetEventSlim _entered;
		private readonly ManualResetEventSlim _gate;

		public KeepFirstHandler(RecordingState state, ManualResetEventSlim entered, ManualResetEventSlim gate) {
			_state = state;
			_entered = entered;
			_gate = gate;
		}

		public Boxed Merge(Boxed current, Boxed incoming) => current;

		public void Discarded(Boxed value) {
			lock (_state.Discarded) _state.Discarded.Add(value.Value);
		}

		public void Handle(int key, Boxed value) {
			if (key == 0) {
				_entered.Set();
				_gate.Wait();
			}

			lock (_state.Processed) _state.Processed.Add((key, value.Value));
		}

		public void OnError(Exception error) {
			lock (_state.Errors) _state.Errors.Add(error);
		}

		public void OnCompleted(Exception? error) {
			lock (_state.CompletedWith) _state.CompletedWith.Add(error);
		}
	}

	// Gap 3: a drained reference value must be cleared from its ring slot so it's collectable (no pin).
	[Test]
	public void Ref_Value_Slots_Are_Cleared_After_Drain() {
		var w = new CompactingSpWorker<int, object, NoopObjHandler>(new NoopObjHandler(), 8);
		w.Start();

		var weak = DispatchTransient(w);
		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);

		GC.Collect();
		GC.WaitForPendingFinalizers();
		GC.Collect();

		Assert.That(weak.TryGetTarget(out _), Is.False, "drained value slot must be cleared so the item is collectable");
		GC.KeepAlive(w); // keep the ring alive: proves the SLOT was cleared, not just the worker freed
		w.Dispose();
	}

	// Object created + dispatched in a non-inlined frame so the only strong reference is the ring slot.
	[MethodImpl(MethodImplOptions.NoInlining)]
	private static WeakReference<object> DispatchTransient(CompactingSpWorker<int, object, NoopObjHandler> w) {
		var obj = new object();
		var weak = new WeakReference<object>(obj);
		Assert.That(w.Dispatch(1, obj), Is.True);
		return weak;
	}

	private readonly struct NoopObjHandler : ICompactingWorkerHandler<int, object> {
		public object Merge(object current, object incoming) => incoming;
		public void Discarded(object value) { }
		public void Handle(int key, object value) { }
		public void OnError(Exception error) { }
		public void OnCompleted(Exception? error) { }
	}

	// Gap 4: Dispatch after Dispose returns false.
	[Test]
	public void Dispatch_After_Dispose_Returns_False() {
		var w = new Rec(8);
		w.Start();
		w.Dispose();
		Assert.That(w.Dispatch(42, 1), Is.False);
	}

	// Gap 5: TryComplete(exception) faults Completion and OnCompleted receives the exception.
	[Test]
	public void TryComplete_With_Exception_Faults_Completion() {
		using var w = new Rec(8);
		w.Start();
		w.Dispatch(1, 1);
		var err = new InvalidOperationException("boom");
		Assert.That(w.TryComplete(err), Is.True);

		var agg = Assert.Throws<AggregateException>(() => w.Completion.Wait(2000));
		Assert.That(agg!.InnerException, Is.InstanceOf<InvalidOperationException>());
		Assert.That(w.CompletedWith, Has.Count.EqualTo(1));
		Assert.That(w.CompletedWith[0], Is.SameAs(err));
	}

	// Gap 6: reference-type keys conflate by value-equality (distinct string instance, equal value).
	[Test]
	public void Reference_Type_Keys_Conflate_By_Value_Equality() {
		var entered = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		var state = new StrState();
		using var w = new CompactingSpWorker<string, Boxed, StrHandler>(new StrHandler(state, entered, gate), 16);
		w.Start();

		Assert.That(w.Dispatch("k0", new Boxed(0)), Is.True); // blocks consumer
		Assert.That(entered.Wait(2000), Is.True);
		Assert.That(w.Dispatch("a", new Boxed(1)), Is.True);
		Assert.That(w.Dispatch("b", new Boxed(2)), Is.True);
		// Distinct string instance, equal value — must conflate with "a" via Equals, not reference identity.
		Assert.That(w.Dispatch(new string('a', 1), new Boxed(3)), Is.True);

		gate.Set();
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => { lock (state.Processed) return state.Processed.Count; }, 3);

		Assert.That(state.Processed, Is.EqualTo(new[] { ("k0", 0), ("a", 3), ("b", 2) }));
	}

	private sealed class StrState {
		public readonly List<(string Key, int Value)> Processed = new();
	}

	private readonly struct StrHandler : ICompactingWorkerHandler<string, Boxed> {
		private readonly StrState _state;
		private readonly ManualResetEventSlim _entered;
		private readonly ManualResetEventSlim _gate;

		public StrHandler(StrState state, ManualResetEventSlim entered, ManualResetEventSlim gate) {
			_state = state;
			_entered = entered;
			_gate = gate;
		}

		public Boxed Merge(Boxed current, Boxed incoming) => incoming;
		public void Discarded(Boxed value) { }

		public void Handle(string key, Boxed value) {
			if (key == "k0") {
				_entered.Set();
				_gate.Wait();
			}

			lock (_state.Processed) _state.Processed.Add((key, value.Value));
		}

		public void OnError(Exception error) { }
		public void OnCompleted(Exception? error) { }
	}
}
