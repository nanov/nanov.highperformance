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
	///   Pins the shape of the default ladder rung by rung: doubling from 1, capped at 64, then exactly
	///   one yield, then unconditional blocks. A later edit that "simplifies" the doubling or drops the
	///   yield tier would still compile and still drain — this is the test that would catch it.
	/// </summary>
	[Test]
	public void Backoff_Ladder_Doubles_Then_Caps_Then_Yields_Once_Then_Blocks() {
		var policy = new BackoffWaitPolicy();

		var expectedRungs = new[] { 1, 2, 4, 8, 16, 32, 64, 64, 64, 64 };
		for (var attempt = 0; attempt < expectedRungs.Length; attempt++) {
			var step = policy.Next(attempt);
			var expected = expectedRungs[attempt];
			Assert.Multiple(() => {
				Assert.That(step.Kind, Is.EqualTo(WaitStepKind.Spin), $"attempt {attempt}");
				Assert.That(step.SpinIterations, Is.EqualTo(expected), $"attempt {attempt}");
			});
		}

		var cumulativeSpin = 0;
		for (var attempt = 0; attempt < expectedRungs.Length; attempt++)
			cumulativeSpin += policy.Next(attempt).SpinIterations;
		Assert.That(cumulativeSpin, Is.LessThanOrEqualTo(policy.SpinBudget + policy.MaxSpinIterations),
			"cumulative spin overshot the budget by more than one rung");

		var yieldStep = policy.Next(expectedRungs.Length);
		Assert.Multiple(() => {
			Assert.That(yieldStep.Kind, Is.EqualTo(WaitStepKind.Yield));
			Assert.That(policy.Next(expectedRungs.Length - 1).Kind, Is.Not.EqualTo(WaitStepKind.Yield),
				"the rung before the yield must still be a spin");
			Assert.That(policy.Next(expectedRungs.Length + 1).Kind, Is.Not.EqualTo(WaitStepKind.Yield),
				"exactly one yield rung, not a plateau");
		});

		foreach (var attempt in new[] { expectedRungs.Length + 1, expectedRungs.Length + 2, 100, 10_000 }) {
			var step = policy.Next(attempt);
			Assert.Multiple(() => {
				Assert.That(step.Kind, Is.EqualTo(WaitStepKind.Block), $"attempt {attempt}");
				Assert.That(step.BlockMilliseconds, Is.EqualTo(policy.BlockMilliseconds));
			});
		}
	}

	/// <summary>
	///   The trap from Task 1's review: <c>WaitForWork</c> treats anything that is not <c>Spin</c> or
	///   <c>Yield</c> as <c>Block</c>, and <c>default(WaitStep)</c> is <c>Kind = Spin, SpinIterations = 0</c>
	///   — a tight, uninterruptible busy loop. Attempt values the ladder should never see in practice
	///   (negative, <see cref="int.MaxValue" />) must still never produce that default.
	/// </summary>
	[Test]
	public void Backoff_Never_Returns_A_Default_Step_For_Any_Attempt() {
		var policy = new BackoffWaitPolicy();

		foreach (var attempt in new[] { int.MinValue, -1000, -1, 0, int.MaxValue }) {
			var step = policy.Next(attempt);
			var isDefaultSpinZero = step.Kind == WaitStepKind.Spin && step.SpinIterations == 0;
			Assert.That(isDefaultSpinZero, Is.False, $"attempt {attempt} produced a default Spin(0) step");
		}
	}

	[Test]
	public void Backoff_Constructor_Rejects_NonPositive_Arguments() {
		Assert.Multiple(() => {
			Assert.That(() => new BackoffWaitPolicy(spinBudget: 0), Throws.TypeOf<ArgumentOutOfRangeException>());
			Assert.That(() => new BackoffWaitPolicy(spinBudget: -1), Throws.TypeOf<ArgumentOutOfRangeException>());
			Assert.That(() => new BackoffWaitPolicy(maxSpinIterations: 0), Throws.TypeOf<ArgumentOutOfRangeException>());
			Assert.That(() => new BackoffWaitPolicy(maxSpinIterations: -1), Throws.TypeOf<ArgumentOutOfRangeException>());
			Assert.That(() => new BackoffWaitPolicy(blockMilliseconds: 0), Throws.TypeOf<ArgumentOutOfRangeException>());
			Assert.That(() => new BackoffWaitPolicy(blockMilliseconds: -1), Throws.TypeOf<ArgumentOutOfRangeException>());
		});
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
