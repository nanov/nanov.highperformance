namespace Nanov.HighPerformance.Tests.Threading.Actors;

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

		// Pin the LITERALS, not just the relationship. Every assertion below is expressed in terms of
		// these constants, so without this a later edit of SpinRungs to 8 would leave the whole fixture
		// green while the method name still claims thirty-two — and every Phase 4 measurement would be
		// against a baseline that is no longer the pre-Phase-4 ladder. That is precisely the drift this
		// test exists to prevent.
		Assert.Multiple(() => {
			Assert.That(LegacyWaitPolicy.SpinRungs, Is.EqualTo(32), "the baseline ladder's rung count changed");
			Assert.That(LegacyWaitPolicy.SpinIterations, Is.EqualTo(64), "the baseline ladder's spin size changed");
		});

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

	/// <summary>
	///   The optional policy parameter did not break construction or the drain path.
	///   <para>
	///     <b>This does NOT observe which policy was chosen</b> — it would pass under any policy that
	///     eventually blocks. Verifying the default really is the legacy ladder needs an injected spy,
	///     which nothing yet requires. Named for what it checks, not for what it was first called.
	///   </para>
	/// </summary>
	[Test]
	public void An_Unspecified_Policy_Constructs_And_Drains() {
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
