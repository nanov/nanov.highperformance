namespace Nanov.HighPerformance.Tests.Threading.Workers;

using System.Linq;
using Nanov.HighPerformance.Threading.Workers;
using NUnit.Framework;

[TestFixture]
public class CoalescingSpWorkerTests {
	// Value-type current: a delta on dispatch, a running total in the cell.
	private struct Tally {
		public long Sum;
	}

	private sealed class AccState {
		public readonly Dictionary<int, long> Totals = new();   // key -> sum of all delivered Tally.Sum
		public readonly List<Exception> Errors = new();
		public readonly List<Exception?> CompletedWith = new();
		public int Handles;
	}

	private readonly struct SumHandler : ICoalescingWorkerHandler<int, Tally> {
		private readonly AccState _state;
		private readonly Action<int>? _hook;

		public SumHandler(AccState state, Action<int>? hook) {
			_state = state;
			_hook = hook;
		}

		public Tally Coalesce(ref Tally current, ref Tally incoming) {
			current.Sum += incoming.Sum;
			return current;
		}

		public void Handle(int key, Tally value) {
			_hook?.Invoke(key);
			lock (_state.Totals) {
				_state.Totals.TryGetValue(key, out var cur);
				_state.Totals[key] = cur + value.Sum;
				_state.Handles++;
			}
		}

		public void OnError(Exception error) {
			lock (_state.Errors) _state.Errors.Add(error);
		}

		public void OnCompleted(Exception? error) {
			lock (_state.CompletedWith) _state.CompletedWith.Add(error);
		}
	}

	private sealed class Rec : IDisposable {
		private readonly CoalescingSpWorker<int, Tally, SumHandler> _worker;
		private readonly AccState _state = new();

		public Rec(int capacity, Action<int>? hook = null) =>
			_worker = new CoalescingSpWorker<int, Tally, SumHandler>(new SumHandler(_state, hook), capacity);

		public Dictionary<int, long> Totals => _state.Totals;
		public List<Exception> Errors => _state.Errors;
		public List<Exception?> CompletedWith => _state.CompletedWith;
		public int Handles {
			get { lock (_state.Totals) return _state.Handles; }
		}
		public Task Completion => _worker.Completion;

		public void Start(CancellationToken ct = default) => _worker.Start(ct);
		public bool Dispatch(int key, long delta) => _worker.Dispatch(key, new Tally { Sum = delta });
		public bool TryComplete(Exception? ex = null) => _worker.TryComplete(ex);
		public void Dispose() => _worker.Dispose();
	}

	private static int HandlesOf(Rec w) => w.Handles;

	[Test]
	public void Single_Key_Coalesces_Sum_Exactly_Once() {
		using var w = new Rec(16);
		w.Start();
		long expected = 0;
		for (var i = 1; i <= 100; i++) {
			w.Dispatch(7, i);
			expected += i;
		}

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Totals[7], Is.EqualTo(expected), "sum of deltas must be delivered exactly once (no loss, no double)");
	}

	[Test]
	public void Distinct_Keys_Coalesce_Independently() {
		using var w = new Rec(16);
		w.Start();
		for (var i = 0; i < 50; i++) {
			w.Dispatch(i % 5, i); // 5 keys
		}

		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		for (var k = 0; k < 5; k++) {
			long expected = 0;
			for (var i = 0; i < 50; i++)
				if (i % 5 == k)
					expected += i;
			Assert.That(w.Totals[k], Is.EqualTo(expected), $"key {k}");
		}
	}

	[Test]
	public void Conflation_Collapses_Many_Into_Few_Deliveries() {
		var entered = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		// First dispatch (key 0) blocks the consumer so key 7's deltas all coalesce into one cell.
		using var w = new Rec(16, hook: k => {
			if (k == 0) {
				entered.Set();
				gate.Wait();
			}
		});
		w.Start();

		Assert.That(w.Dispatch(0, 0), Is.True);
		Assert.That(entered.Wait(2000), Is.True);
		for (var i = 1; i <= 1000; i++) w.Dispatch(7, i); // all conflate into key 7's cell

		gate.Set();
		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);

		Assert.That(w.Totals[7], Is.EqualTo(1000L * 1001 / 2)); // 1..1000 summed, exactly once
		// key 7 delivered in far fewer Handle calls than 1000 (conflated while parked → ideally 1)
		Assert.That(w.Handles, Is.LessThan(1000));
	}

	// The property ConflationSpWorker can't provide: coalescing exactly-once under heavy contention.
	[TestCase(1)]
	[TestCase(2)]
	[TestCase(8)]
	public async Task Exactly_Once_Under_Contention(int capacity) {
		using var w = new Rec(capacity, hook: _ => Thread.SpinWait(40)); // slow-ish consumer → race the cell lock
		w.Start();

		const int keys = 4;
		const int rounds = 50_000;
		var sent = new long[keys];
		await Task.Run(() => {
			for (var r = 0; r < rounds; r++) {
				var k = r % keys;
				long delta = (r & 7) + 1; // 1..8
				sent[k] += delta;
				while (!w.Dispatch(k, delta)) { } // single producer
			}
		});

		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => HandlesOf(w), keys);

		for (var k = 0; k < keys; k++)
			Assert.That(w.Totals[k], Is.EqualTo(sent[k]),
				$"key {k}: delivered {w.Totals.GetValueOrDefault(k)} != sent {sent[k]} (lost or double-counted)");
	}

	[Test]
	public void TryComplete_Drains_And_Completes_Normally() {
		using var w = new Rec(16);
		w.Start();
		for (var i = 0; i < 40; i++) w.Dispatch(i, 1);
		Assert.That(w.TryComplete(), Is.True);
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Totals.Values.Sum(), Is.EqualTo(40));
		Assert.That(w.CompletedWith, Has.Count.EqualTo(1));
		Assert.That(w.CompletedWith[0], Is.Null);
	}

	[Test]
	public void Dispatch_After_Complete_Returns_False() {
		using var w = new Rec(8);
		w.Start();
		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Dispatch(123, 1), Is.False);
	}

	[Test]
	public void Handler_Exception_Is_Reported_To_OnError_And_Worker_Continues() {
		using var w = new Rec(8, hook: k => {
			if (k == 2) throw new InvalidOperationException("boom");
		});
		w.Start();
		for (var i = 0; i < 5; i++) w.Dispatch(i, 1);
		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Errors, Has.Count.EqualTo(1));
		Assert.That(w.Errors[0], Is.InstanceOf<InvalidOperationException>());
		Assert.That(w.Totals.ContainsKey(0) && w.Totals.ContainsKey(4), Is.True);
	}

	[Test]
	public void Cancellation_Stops_And_Completion_Is_Cancelled() {
		using var w = new Rec(16, hook: _ => Thread.Sleep(5));
		using var cts = new CancellationTokenSource();
		w.Start(cts.Token);
		for (var i = 0; i < 200; i++) w.Dispatch(i % 4, 1);
		Thread.Sleep(50);
		cts.Cancel();
		var agg = Assert.Throws<AggregateException>(() => w.Completion.Wait(2000));
		Assert.That(agg!.InnerException, Is.InstanceOf<OperationCanceledException>());
	}

	[Test]
	public void Dispose_Is_Idempotent() {
		var w = new Rec(8);
		w.Start();
		w.Dispose();
		Assert.DoesNotThrow(() => w.Dispose());
	}

	// TValue is a REFERENCE type — proves the dropped `where TValue : struct` constraint. default(TValue)
	// is null (the identity); Coalesce returns a fresh object; the cell lock still gives exactly-once.
	private sealed class Bag {
		public long Total;
	}

	private readonly struct BagHandler : ICoalescingWorkerHandler<int, Bag> {
		private readonly Dictionary<int, long> _totals;
		public BagHandler(Dictionary<int, long> totals) => _totals = totals;

		// current is null for a key new to a generation (default(Bag)); produce a fresh combined Bag.
		public Bag Coalesce(ref Bag current, ref Bag incoming) =>
			new() { Total = (current?.Total ?? 0) + incoming.Total };

		public void Handle(int key, Bag value) {
			lock (_totals) {
				_totals.TryGetValue(key, out var cur);
				_totals[key] = cur + value.Total;
			}
		}

		public void OnError(Exception error) { }
		public void OnCompleted(Exception? error) { }
	}

	[Test]
	public async Task Reference_TValue_Coalesces_Exactly_Once() {
		var totals = new Dictionary<int, long>();
		using var w = new CoalescingSpWorker<int, Bag, BagHandler>(new BagHandler(totals), 8);
		w.Start();

		const int keys = 16;
		const int rounds = 100_000;
		var sent = new long[keys];
		await Task.Run(() => {
			for (var r = 0; r < rounds; r++) {
				var k = r % keys;
				long delta = (r & 7) + 1;
				sent[k] += delta;
				while (!w.Dispatch(k, new Bag { Total = delta })) { }
			}
		});

		w.TryComplete();
		Assert.That(w.Completion.Wait(5000), Is.True);
		for (var k = 0; k < keys; k++)
			lock (totals) Assert.That(totals.GetValueOrDefault(k), Is.EqualTo(sent[k]), $"key {k}");
	}
}
