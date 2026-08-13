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
	public void Four_Workers_Preserve_Order_Within_Every_Key() {
		const int keys = 32;
		const int perKey = 500;
		var state = new RecordingState();
		using var system = New(state, workers: 4);
		system.Start();

		for (var i = 0; i < perKey; i++)
			for (var k = 0; k < keys; k++)
				Assert.That(system.Dispatch(k, i), Is.True);

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, keys * perKey);

		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");
	}

	[Test]
	public void Four_Workers_All_Do_Work() {
		var state = new RecordingState();
		using var system = New(state, workers: 4);
		system.Start();

		for (var i = 0; i < 4_000; i++)
			system.Dispatch(i % 64, i);

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 4_000);

		Assert.That(system.WorkersThatActivated, Is.GreaterThan(1),
			"only one worker ever activated — routing is not distributing");
	}

	[Test]
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
