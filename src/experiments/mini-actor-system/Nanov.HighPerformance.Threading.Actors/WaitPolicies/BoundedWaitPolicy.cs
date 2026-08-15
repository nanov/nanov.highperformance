namespace Nanov.HighPerformance.Threading.Actors.WaitPolicies;

using System;

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
///     wakeups/sec across a fully idle system.
///   </para>
///   <para>
///     <b>That idle cost is NOT what Phase 4's Task 2 measured, and must not be read as free.</b> Every
///     cell in that comparison ran a saturated producer, where a parked worker is signalled in
///     microseconds and this timeout therefore essentially never fires — so what was measured is the
///     difference between <c>Wait(-1)</c> and <c>Wait(20)</c> on a path that always gets signalled
///     first, which ought to be near zero by construction. The supported claim is only that under a
///     saturated producer the cost is bounded above by a few percent. Measuring the idle regime needs a
///     hold-N-workers-idle-and-watch-CPU test that does not exist.
///   </para>
/// </summary>
public sealed class BoundedWaitPolicy : IWaitPolicy {
	/// <param name="blockMilliseconds">
	///   Must be positive. Zero would make <c>ManualResetEventSlim.Wait(0)</c> return immediately, so the
	///   ladder would spin the block rung forever and every idle worker would become a 100% CPU busy-wait
	///   — silently, because the system still makes progress and still completes, so no test would catch
	///   it. Negative values other than <c>Timeout.Infinite</c> throw from inside a worker thread, which
	///   surfaces mid-run via the fault path rather than at construction. Reject both here.
	/// </param>
	public BoundedWaitPolicy(int blockMilliseconds = 20) {
		ArgumentOutOfRangeException.ThrowIfNegativeOrZero(blockMilliseconds);
		BlockMilliseconds = blockMilliseconds;
	}

	public int BlockMilliseconds { get; }

	public WaitStep Next(int attempt) =>
		attempt < LegacyWaitPolicy.SpinRungs
			? WaitStep.Spin(LegacyWaitPolicy.SpinIterations)
			: WaitStep.Block(BlockMilliseconds);
}
