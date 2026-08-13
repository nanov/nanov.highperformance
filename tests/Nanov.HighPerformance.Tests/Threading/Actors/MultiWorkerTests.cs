namespace Nanov.HighPerformance.Tests.Threading.Actors;

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
}
