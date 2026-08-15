namespace Nanov.HighPerformance.Threading.Actors.WaitPolicies;

using System.Threading;

/// <summary>
///   The ladder as it was before Phase 4, reproduced EXACTLY so later policies are measured against a
///   real baseline rather than a remembered one: 32 rungs of <c>SpinWait(64)</c>, then an unbounded
///   block, forever.
///   <para>
///     Its shortcomings are the point of the phase and are documented rather than fixed here: no
///     exponential backoff, no yield tier before the kernel block, and an unbounded wait that turns a
///     lost wakeup into a permanent hang rather than a latency hiccup.
///   </para>
/// </summary>
public sealed class LegacyWaitPolicy : IWaitPolicy {
	internal const int SpinRungs = 32;
	internal const int SpinIterations = 64;

	public WaitStep Next(int attempt) =>
		attempt < SpinRungs ? WaitStep.Spin(SpinIterations) : WaitStep.Block(Timeout.Infinite);
}
