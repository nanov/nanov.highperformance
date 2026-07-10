namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

using System;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Threading;

/// <summary>
///   Best-effort thread pinning for stable perf numbers — the lever behind Disruptor-net's
///   thread-affinity throughput test. Pins the CURRENT thread to a single logical CPU and bumps its
///   priority. Windows and Linux pin via the OS scheduler; macOS has no per-core pinning API, so there
///   we only raise priority (the pin is a no-op). All failures are swallowed — affinity is an
///   optimization, never a correctness requirement.
/// </summary>
public static class ThreadAffinityUtil {
	/// <summary>
	///   Pin the current thread to <paramref name="cpu" /> (if non-null) and set <paramref name="priority" />,
	///   returning a scope that restores the previous priority / clears the pin on dispose. Pass a null
	///   <paramref name="cpu" /> to set priority only.
	/// </summary>
	public static Scope SetThreadAffinity(int? cpu, ThreadPriority priority = ThreadPriority.Highest) {
		var previousPriority = Thread.CurrentThread.Priority;
		Thread.CurrentThread.Priority = priority;

		if (cpu is not { } cpuIndex)
			return new Scope(previousPriority, false);

		var pinned = TryPin(cpuIndex);
		return new Scope(previousPriority, pinned);
	}

	/// <summary>
	///   Pin the current thread to <paramref name="cpu" /> and bump priority, WITHOUT a restore scope —
	///   for a dedicated worker thread that is pinned for its whole life (the pin dies with the thread).
	///   Call it from the worker handler on its first invocation.
	/// </summary>
	public static void PinCurrentThread(int cpu, ThreadPriority priority = ThreadPriority.Highest) {
		Thread.CurrentThread.Priority = priority;
		TryPin(cpu);
	}

	/// <summary>
	///   Prepare a consumer thread for a perf run the way Disruptor does: always raise priority to
	///   <see cref="ThreadPriority.Highest" /> (steers it to a performance core on heterogeneous CPUs), and
	///   pin to <paramref name="cpu" /> when a custom CPU set was supplied. No restore scope — the worker
	///   thread is primed for its whole life.
	/// </summary>
	public static void PrimeConsumerThread(int? cpu, ThreadPriority priority = ThreadPriority.Highest) {
		Thread.CurrentThread.Priority = priority;
		if (cpu is { } cpuIndex)
			TryPin(cpuIndex);
	}

	private static bool TryPin(int cpu) {
		try {
			if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
				return PinWindows(cpu);
			if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
				return PinLinux(cpu);
		}
		catch {
			// Affinity is best-effort; ignore platform/permission failures.
		}

		return false; // macOS and everything else: no per-core pin available.
	}

	[SupportedOSPlatform("windows")]
	private static bool PinWindows(int cpu) {
		// BeginThreadAffinity stops the CLR migrating this managed thread across OS threads while pinned.
		Thread.BeginThreadAffinity();
		var handle = GetCurrentThread();
		var mask = (nuint)1 << cpu;
		var previous = SetThreadAffinityMask(handle, mask);
		if (previous == nuint.Zero) {
			Thread.EndThreadAffinity();
			return false;
		}

		return true;
	}

	[SupportedOSPlatform("linux")]
	private static bool PinLinux(int cpu) {
		// cpu_set_t as a single 64-bit word (supports CPUs 0..63 — enough for these tests).
		var mask = 1UL << cpu;
		return sched_setaffinity(0, new IntPtr(sizeof(ulong)), ref mask) == 0;
	}

	[DllImport("kernel32.dll")]
	[SupportedOSPlatform("windows")]
	private static extern IntPtr GetCurrentThread();

	[DllImport("kernel32.dll")]
	[SupportedOSPlatform("windows")]
	private static extern nuint SetThreadAffinityMask(IntPtr hThread, nuint dwThreadAffinityMask);

	[DllImport("libc", SetLastError = true)]
	[SupportedOSPlatform("linux")]
	private static extern int sched_setaffinity(int pid, IntPtr cpusetsize, ref ulong mask);

	/// <summary>Restores thread priority (and ends the Windows affinity scope) on dispose.</summary>
	public readonly struct Scope : IDisposable {
		private readonly ThreadPriority _previousPriority;
		private readonly bool _pinnedOnWindows;

		internal Scope(ThreadPriority previousPriority, bool pinned) {
			_previousPriority = previousPriority;
			_pinnedOnWindows = pinned && RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
		}

		public void Dispose() {
			Thread.CurrentThread.Priority = _previousPriority;
			if (_pinnedOnWindows)
				Thread.EndThreadAffinity();
		}
	}
}
