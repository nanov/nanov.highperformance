namespace Nanov.HighPerformance.Tests.Threading;

using System.Diagnostics;

/// <summary>
///   Helpers for the SPSC-worker concurrency tests. The signaling regression guards must fail FAST on a
///   lost wakeup (a hung worker) without imposing a fixed upper bound that turns into flakiness on slow CI.
/// </summary>
internal static class WorkerTestUtil {
	/// <summary>
	///   Wait for <paramref name="completion" />, failing as soon as forward progress STALLS rather than
	///   after a fixed timeout. <paramref name="progress" /> is a monotonic counter (e.g. processed-item
	///   count); a lost wakeup freezes the worker, so the counter plateaus and we throw within
	///   <paramref name="stallLimit" /> (default 5s). A legitimately slow run keeps advancing the counter and
	///   is allowed unbounded time. Surfaces a faulted/cancelled completion by re-throwing it.
	/// </summary>
	public static void AwaitCompletionOrStall(Task completion, Func<int> progress, int expected,
		TimeSpan? stallLimit = null) {
		var limit = stallLimit ?? TimeSpan.FromSeconds(5);
		var last = -1;
		var sinceProgress = Stopwatch.StartNew();
		while (!completion.Wait(50)) {
			var cur = progress();
			if (cur != last) {
				last = cur;
				sinceProgress.Restart();
			} else if (sinceProgress.Elapsed > limit) {
				throw new TimeoutException(
					$"worker stalled at {cur}/{expected} for {limit.TotalSeconds:0.#}s — likely a lost wakeup");
			}
		}

		completion.GetAwaiter().GetResult(); // re-throw if the worker faulted/cancelled
	}

	/// <summary>
	///   Block until <paramref name="progress" /> reaches <paramref name="target" />, throwing if it doesn't
	///   within <paramref name="timeout" />. Used by the deterministic "wake a parked worker" guards: a lost
	///   wakeup leaves progress stuck below the target.
	/// </summary>
	public static void AwaitProgress(Func<int> progress, int target, TimeSpan? timeout = null) {
		var limit = timeout ?? TimeSpan.FromSeconds(5);
		var sw = Stopwatch.StartNew();
		while (progress() < target) {
			if (sw.Elapsed > limit)
				throw new TimeoutException(
					$"progress stuck at {progress()}/{target} after {limit.TotalSeconds:0.#}s — likely a lost wakeup");
			Thread.Sleep(10);
		}
	}
}
