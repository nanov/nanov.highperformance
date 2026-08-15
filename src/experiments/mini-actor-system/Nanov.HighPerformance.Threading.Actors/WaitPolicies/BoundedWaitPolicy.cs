namespace Nanov.HighPerformance.Threading.Actors.WaitPolicies;

/// <summary>
///   The legacy ladder with a bounded block: 32 rungs of <c>SpinWait(64)</c>, then repeated blocks of
///   at most <see cref="BlockMilliseconds" />.
///   <para>
///     <b>This is a safety change before it is a performance change.</b> An unbounded block turns a lost
///     wakeup into a permanent hang; a bounded one turns it into a latency hiccup that the next recheck
///     clears. The system has two documented, undefended lost-wakeup races (spec §11) — this does not
///     fix them, it stops them being fatal.
///   </para>
///   <para>
///     The cost is a periodic wake per idle worker. At the default 20 ms and 8 workers that is 400
///     wakeups/sec across a fully idle system, which is why the phase measures it rather than assuming
///     it is free.
///   </para>
/// </summary>
public sealed class BoundedWaitPolicy : IWaitPolicy {
	public BoundedWaitPolicy(int blockMilliseconds = 20) => BlockMilliseconds = blockMilliseconds;

	public int BlockMilliseconds { get; }

	public WaitStep Next(int attempt) =>
		attempt < LegacyWaitPolicy.SpinRungs
			? WaitStep.Spin(LegacyWaitPolicy.SpinIterations)
			: WaitStep.Block(BlockMilliseconds);
}
