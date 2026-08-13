namespace Nanov.HighPerformance.Tests.Threading.Actors;

using System.Linq;
using Nanov.HighPerformance.Tests.Threading.Workers;
using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using NUnit.Framework;

[TestFixture]
public class RoutedSchedulerTests {
	private static ActorSystem<int, int, RecordingHandler, RoutedScheduler> New(
		RecordingState state, int workers, RoutingPolicy policy = RoutingPolicy.LeastLoaded,
		int keyCapacity = 128, int chunkCount = 32, int chunkSize = 64, int batchBound = 0) =>
		new(new RecordingHandler(state), keyCapacity, chunkCount, chunkSize, batchBound, workers,
			new RoutedScheduler { Capacity = keyCapacity * 4, Policy = policy });

	[Test]
	[CancelAfter(60_000)]
	public void Four_Workers_Preserve_Order_Within_Every_Key() {
		const int keys = 32;
		const int perKey = 500;
		const int total = keys * perKey;
		var state = new RecordingState();
		using var system = New(state, workers: 4);
		system.Start();

		// Dispatch runs on its own thread so the test thread can bound the wait with a real Join — see
		// ActorSystemStressTests' class doc comment for why CancelAfter alone cannot catch a wedged
		// Dispatch. No Assert inside the producer thread: a failure there throws off-thread and can
		// crash the process instead of failing the test, so a plain progress counter carries the result
		// back instead.
		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				var n = 0;
				for (var i = 0; i < perKey; i++)
					for (var k = 0; k < keys; k++) {
						if (!system.Dispatch(k, i)) return;
						Volatile.Write(ref dispatched, ++n);
					}
			}
			catch (Exception ex) {
				producerError = ex;
			}
		}) { IsBackground = true };
		producer.Start();

		Assert.That(producer.Join(TimeSpan.FromSeconds(45)), Is.True,
			$"producer wedged after {Volatile.Read(ref dispatched)}/{total} dispatches");
		if (producerError is not null)
			Assert.Fail($"producer threw: {producerError}");
		Assert.That(Volatile.Read(ref dispatched), Is.EqualTo(total), "a Dispatch was rejected mid-run");

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, total);

		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");
	}

	/// <summary>
	///   64 distinct keys against a 32-chunk slab — this is deliberate, and the only N&gt;1 coverage of
	///   the harvest path (chunk exhaustion forces <c>Dispatch</c> through <c>WaitForChunk</c>/<c>Harvest</c>
	///   under real multi-worker churn). Keep this shape; do not raise <c>chunkCount</c> to make it stop
	///   blocking.
	/// </summary>
	[Test]
	[CancelAfter(60_000)]
	public void Four_Workers_All_Do_Work() {
		const int total = 4_000;
		var state = new RecordingState();
		using var system = New(state, workers: 4);
		system.Start();

		// Dispatch runs on its own thread so the test thread can bound the wait with a real Join. This
		// test in particular dispatches 64 distinct keys against chunkCount: 32, which is guaranteed to
		// exhaust the slab and park the producer in WaitForChunk — the one place a wedge here would hang
		// the run instead of failing it if it ran on the test thread. See
		// ActorSystemStressTests' class doc comment for the full rationale.
		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				for (var i = 0; i < total; i++) {
					if (!system.Dispatch(i % 64, i)) return;
					Volatile.Write(ref dispatched, i + 1);
				}
			}
			catch (Exception ex) {
				producerError = ex;
			}
		}) { IsBackground = true };
		producer.Start();

		Assert.That(producer.Join(TimeSpan.FromSeconds(45)), Is.True,
			$"producer wedged after {Volatile.Read(ref dispatched)}/{total} dispatches");
		if (producerError is not null)
			Assert.Fail($"producer threw: {producerError}");
		Assert.That(Volatile.Read(ref dispatched), Is.EqualTo(total), "a Dispatch was rejected mid-run");

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, total);

		// AwaitCompletionOrStall catches a lost message (progress stalls) and a faulted completion, but
		// not a DUPLICATED one — that would inflate HandledCount and pass silently otherwise.
		Assert.That(state.HandledCount, Is.EqualTo(total));
		Assert.That(system.WorkersThatActivated, Is.GreaterThan(1),
			"only one worker ever activated — routing is not distributing");
	}

	[Test]
	[CancelAfter(60_000)]
	public void Completion_Resolves_With_Every_Worker_Parked() {
		var state = new RecordingState();
		using var system = New(state, workers: 4);
		system.Start();
		system.Dispatch(1, 1);
		WorkerTestUtil.AwaitProgress(() => state.HandledCount, 1);
		Thread.Sleep(150); // let every worker genuinely park

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 1);
		Assert.That(Volatile.Read(ref state.CompletedCalls), Is.EqualTo(4),
			"OnCompleted fires once per worker");
	}
}
