namespace Nanov.HighPerformance.Threading.Actors;

/// <summary>What a worker should do next while waiting for work.</summary>
public enum WaitStepKind {
	/// <summary>Burn <see cref="WaitStep.SpinIterations" /> spin iterations, then re-test for work.</summary>
	Spin,

	/// <summary>Yield the remainder of this time slice to another runnable thread, then re-test.</summary>
	Yield,

	/// <summary>Arm and block, for at most <see cref="WaitStep.BlockMilliseconds" />.</summary>
	Block,
}

/// <summary>One rung of the wait ladder.</summary>
public readonly struct WaitStep {
	public WaitStep(WaitStepKind kind, int spinIterations, int blockMilliseconds) {
		Kind = kind;
		SpinIterations = spinIterations;
		BlockMilliseconds = blockMilliseconds;
	}

	public WaitStepKind Kind { get; }

	public int SpinIterations { get; }

	/// <summary>Block timeout; <see cref="System.Threading.Timeout.Infinite" /> for an unbounded wait.</summary>
	public int BlockMilliseconds { get; }

	public static WaitStep Spin(int iterations) => new(WaitStepKind.Spin, iterations, 0);

	public static WaitStep Yield() => new(WaitStepKind.Yield, 0, 0);

	public static WaitStep Block(int milliseconds) => new(WaitStepKind.Block, 0, milliseconds);
}

/// <summary>
///   The shape of the idle-worker wait ladder: how long to spin, whether to yield, and when to block.
///   <para>
///     <b>Policy only — never mechanism.</b> The policy decides the ladder's shape; <c>ActorSystem</c>
///     walks it and performs every work and quiescence recheck, and the scheduler owns the blocking
///     primitive. A policy that could observe or influence <i>who</i> gets woken would be able to break
///     the arm-then-recheck protocol; keeping it to shape means it cannot.
///   </para>
///   <para>
///     <b>Why this is an interface parameter and not a devirtualised struct type parameter</b>, unlike
///     every other seam here: it is consulted on the IDLE path only — a few dozen times per park, and a
///     park happens at most once per activation — so an interface call of a couple of nanoseconds sits
///     against a <c>SpinWait(64)</c> of a couple of microseconds. Making it a fifth type parameter on
///     <c>ActorSystem</c> would churn a hundred-plus call sites for no measured gain. Convert it if a
///     measurement ever shows otherwise.
///   </para>
/// </summary>
public interface IWaitPolicy {
	/// <summary>
	///   The rung for this attempt. <paramref name="attempt" /> starts at 0 and increments for every
	///   rung walked within a single park; it resets when the worker finds work. A policy that returns
	///   <see cref="WaitStepKind.Block" /> forever is legal — that is what the unbounded legacy ladder
	///   does once its spin budget is gone.
	/// </summary>
	WaitStep Next(int attempt);
}
