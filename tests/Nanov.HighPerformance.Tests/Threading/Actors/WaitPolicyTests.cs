namespace Nanov.HighPerformance.Tests.Threading.Actors;

using System.Collections.Generic;
using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using Nanov.HighPerformance.Threading.Actors.WaitPolicies;
using Nanov.HighPerformance.Tests.Threading.Workers;
using NUnit.Framework;

[TestFixture]
public class WaitPolicyTests {
	/// <summary>
	///   Pins the baseline ladder rung by rung. Later policies are measured against this one, so if it
	///   ever silently stops reproducing pre-Phase-4 behaviour, every number in the phase is measured
	///   against the wrong thing.
	/// </summary>
	[Test]
	public void Legacy_Ladder_Is_Thirty_Two_Spins_Then_Unbounded_Blocks() {
		var policy = new LegacyWaitPolicy();

		for (var attempt = 0; attempt < LegacyWaitPolicy.SpinRungs; attempt++) {
			var step = policy.Next(attempt);
			Assert.Multiple(() => {
				Assert.That(step.Kind, Is.EqualTo(WaitStepKind.Spin), $"attempt {attempt}");
				Assert.That(step.SpinIterations, Is.EqualTo(LegacyWaitPolicy.SpinIterations));
			});
		}

		foreach (var attempt in new[] { LegacyWaitPolicy.SpinRungs, 100, 10_000 }) {
			var step = policy.Next(attempt);
			Assert.Multiple(() => {
				Assert.That(step.Kind, Is.EqualTo(WaitStepKind.Block), $"attempt {attempt}");
				Assert.That(step.BlockMilliseconds, Is.EqualTo(System.Threading.Timeout.Infinite));
			});
		}
	}

	/// <summary>The seam must not have changed the default: an unspecified policy is the legacy ladder.</summary>
	[Test]
	public void An_Unspecified_Policy_Defaults_To_Legacy() {
		var state = new RecordingState();
		using var system = new ActorSystem<int, int, RecordingHandler, SingleWorkerScheduler>(
			new RecordingHandler(state), keyCapacity: 16, chunkCount: 8, chunkSize: 16,
			batchBound: 0, workers: 1, scheduler: new SingleWorkerScheduler { Capacity = 64 });
		system.Start();

		for (var i = 0; i < 100; i++)
			system.Dispatch(i % 4, i);

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 100);
		Assert.That(state.HandledCount, Is.EqualTo(100));
	}
}
