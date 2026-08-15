namespace Nanov.HighPerformance.Threading.Actors.WaitPolicies;

using System;
using System.Collections.Generic;

/// <summary>
///   Exponential-backoff spin, then a yield tier, then a bounded block — the shape Helios
///   <c>DedicatedThreadPool</c>'s <c>UnfairSemaphore</c> and the CLR thread pool both use.
///   <para>
///     <b>Why backoff.</b> A flat spin pays its full cost every time even when work was one iteration
///     away, and pays the same cost when it was never coming. Doubling makes the cheap case cheap and
///     reaches the block in a logarithmic number of rungs.
///   </para>
///   <para>
///     <b>Why a yield tier.</b> Eight workers plus a producer plus a router on twelve logical cores is
///     oversubscribed. Going straight from spinning to a kernel wait skips the cheapest useful action
///     available — handing the rest of the slice to a runnable peer, which on this workload is usually
///     the very producer or worker whose progress would end this wait.
///   </para>
///   <para>
///     <b>What this does NOT do.</b> The reference design's most valuable property is that a release
///     prefers a hot spinner over a parked waiter. That cannot be expressed here: this seam controls
///     only the ladder's shape, never who gets woken. See the plan's Phase 4b note.
///   </para>
/// </summary>
public sealed class BackoffWaitPolicy : IWaitPolicy {
	private readonly int[] _ladder;

	/// <param name="spinBudget">
	///   Total spin iterations to spend climbing the ladder before the yield rung. Must be positive — zero
	///   or negative would leave the precomputed ladder empty, and attempt 0 would land straight on the
	///   yield rung with attempt 1 going straight to a block, silently collapsing the whole exponential
	///   tier.
	/// </param>
	/// <param name="maxSpinIterations">
	///   Cap on a single rung. Must be positive — zero or negative would make every rung <c>Spin(0)</c>,
	///   which <c>WaitForWork</c> cannot distinguish from a lost default <see cref="WaitStep" /> and treats
	///   as a tight, uninterruptible busy loop.
	/// </param>
	/// <param name="blockMilliseconds">
	///   Block timeout past the yield rung. Must be positive for the same reason <c>BoundedWaitPolicy</c>
	///   requires it — zero turns the block rung into a silent 100% CPU busy-wait.
	/// </param>
	public BackoffWaitPolicy(int spinBudget = 256, int maxSpinIterations = 64, int blockMilliseconds = 20) {
		ArgumentOutOfRangeException.ThrowIfNegativeOrZero(spinBudget);
		ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxSpinIterations);
		ArgumentOutOfRangeException.ThrowIfNegativeOrZero(blockMilliseconds);

		SpinBudget = spinBudget;
		MaxSpinIterations = maxSpinIterations;
		BlockMilliseconds = blockMilliseconds;

		// Precomputed so Next() is a bounds-checked array read on the idle path rather than arithmetic.
		// The loop always runs at least once (next starts at 1, spinBudget is guarded positive above), so
		// the ladder can never come out empty.
		var rungs = new List<int>();
		var spent = 0;
		var next = 1;
		while (spent < spinBudget) {
			rungs.Add(next);
			spent += next;
			next = Math.Min(next << 1, maxSpinIterations);
		}

		_ladder = rungs.ToArray();
	}

	public int SpinBudget { get; }

	public int MaxSpinIterations { get; }

	public int BlockMilliseconds { get; }

	/// <summary>
	///   Rungs of exponential spin, then exactly one yield, then repeated bounded blocks.
	///   <para>
	///     <paramref name="attempt" /> is clamped below at the first rung. <c>WaitForWork</c> only ever
	///     hands this attempts starting at 0 and increasing by one, so a negative attempt should not occur
	///     in practice — but this must never answer one with a default <see cref="WaitStep" />, which
	///     <c>WaitForWork</c> would read as <c>Spin(0)</c>, a tight busy loop.
	///   </para>
	/// </summary>
	public WaitStep Next(int attempt) {
		if (attempt < 0) return WaitStep.Spin(_ladder[0]);
		if (attempt < _ladder.Length) return WaitStep.Spin(_ladder[attempt]);
		if (attempt == _ladder.Length) return WaitStep.Yield();
		return WaitStep.Block(BlockMilliseconds);
	}
}
