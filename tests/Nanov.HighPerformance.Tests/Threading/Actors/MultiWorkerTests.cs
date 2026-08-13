namespace Nanov.HighPerformance.Tests.Threading.Actors;

using System;
using System.Linq;
using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using Nanov.HighPerformance.Tests.Threading.Workers;
using NUnit.Framework;

[TestFixture]
public class MultiWorkerTests {
	[Test]
	public void Quiescence_Broadcast_Fires_At_Most_Once() {
		var state = new RecordingState();
		using var system = new ActorSystem<int, int, RecordingHandler, SingleWorkerScheduler>(
			new RecordingHandler(state), keyCapacity: 64, chunkCount: 16, chunkSize: 64,
			batchBound: 0, workers: 1, scheduler: new SingleWorkerScheduler { Capacity = 256 });
		system.Start();

		for (var i = 0; i < 500; i++)
			system.Dispatch(i % 8, i);

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 500);

		Assert.Multiple(() => {
			Assert.That(state.HandledCount, Is.EqualTo(500));
			Assert.That(system.QuiescenceWakes, Is.LessThanOrEqualTo(1),
				"the quiescence broadcast must be idempotent — it is on the per-32-message publish path");
		});
	}

	[Test]
	[CancelAfter(120_000)]
	public void Eight_Workers_Sustained_Load_Preserves_Order_And_Loses_Nothing() {
		const int keys = 64;
		const int perKey = 2_000;
		const int total = keys * perKey;
		var state = new RecordingState();
		using var system = new ActorSystem<int, int, RecordingHandler, RoutedScheduler>(
			new RecordingHandler(state), keyCapacity: 256, chunkCount: 64, chunkSize: 16,
			batchBound: 4, workers: 8, scheduler: new RoutedScheduler { Capacity = 1024 });
		system.Start();

		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				for (var i = 0; i < perKey; i++)
					for (var k = 0; k < keys; k++) {
						if (!system.Dispatch(k, i)) return;
						Volatile.Write(ref dispatched, Volatile.Read(ref dispatched) + 1);
					}
			}
			catch (Exception ex) {
				producerError = ex;
			}
		}) { IsBackground = true };
		producer.Start();

		Assert.That(producer.Join(TimeSpan.FromSeconds(90)), Is.True,
			$"producer wedged after {Volatile.Read(ref dispatched)}/{total} dispatches");
		if (producerError is not null) Assert.Fail($"producer threw: {producerError}");
		Assert.That(Volatile.Read(ref dispatched), Is.EqualTo(total));

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, total);

		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");
	}

	[Test]
	[CancelAfter(60_000)]
	public void No_Worker_Ever_Rejects_A_Claim_Under_Multi_Worker_Churn() {
		const int keys = 16;
		const int perKey = 3_000;
		var state = new RecordingState();
		using var system = new ActorSystem<int, int, RecordingHandler, RoutedScheduler>(
			new RecordingHandler(state), keyCapacity: 128, chunkCount: 32, chunkSize: 8,
			batchBound: 1, workers: 8, scheduler: new RoutedScheduler { Capacity = 512 });
		system.Start();

		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				for (var i = 0; i < perKey; i++)
					for (var k = 0; k < keys; k++) {
						if (!system.Dispatch(k, i)) return;
						Volatile.Write(ref dispatched, Volatile.Read(ref dispatched) + 1);
					}
			}
			catch (Exception ex) {
				producerError = ex;
			}
		}) { IsBackground = true };
		producer.Start();

		Assert.That(producer.Join(TimeSpan.FromSeconds(45)), Is.True, "producer wedged");
		if (producerError is not null) Assert.Fail($"producer threw: {producerError}");

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, keys * perKey);

		// A rejected claim means two tokens existed for one index — the "never two" invariant broke.
		// batchBound 1 maximises release churn, which is when a duplicate would appear.
		Assert.That(system.RejectedClaims, Is.EqualTo(0),
			"a worker lost the Scheduled -> Owned CAS: two tokens existed for one key index");
	}

	[Test]
	[CancelAfter(60_000)]
	public void Every_Worker_Parks_And_Still_Completes() {
		var state = new RecordingState();
		using var system = new ActorSystem<int, int, RecordingHandler, RoutedScheduler>(
			new RecordingHandler(state), keyCapacity: 64, chunkCount: 16, chunkSize: 64,
			batchBound: 0, workers: 8, scheduler: new RoutedScheduler { Capacity = 256 });
		system.Start();

		system.Dispatch(1, 1);
		WorkerTestUtil.AwaitProgress(() => state.HandledCount, 1);
		Thread.Sleep(200); // every worker parks

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 1);
		Assert.That(Volatile.Read(ref state.CompletedCalls), Is.EqualTo(8));
	}
}
