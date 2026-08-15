namespace Nanov.HighPerformance.Threading.Actors.Schedulers;

using System;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors.Internal;

/// <summary>
///   The Phase 1 arrangement behind the seam: ONE producer-fed SPSC ring plus a worker-private local
///   stack, drained local-first for affinity.
///   <para>
///     <b>Single worker only.</b> The ring is strictly SPSC; a second consumer corrupts it silently.
///     <see cref="Start" /> enforces this rather than trusting the caller.
///   </para>
/// </summary>
public struct SingleWorkerScheduler : IKeyScheduler {
	private readonly State _state;

	public SingleWorkerScheduler() => _state = new State();

	private sealed class State {
		public readonly ManualResetEventSlim WorkAvailable = new(false);
		public KeyRing? Ring;
		public int[]? Local;
		public int LocalCount;
		public bool Started;
	}

	/// <summary>Ring capacity; must be at least the key map's slot count so it can never fill.</summary>
	public int Capacity { get; set; } = 1024;

	// onFault is accepted for interface conformance only. This scheduler owns no background thread —
	// its only thread is the single worker itself, which already reports through ActorSystem.RunLoop's
	// own catch — so there is nothing here that could die silently and need this channel.
	public void Start(int workerCount, Action<Exception> onFault) {
		if (workerCount != 1)
			throw new ArgumentOutOfRangeException(nameof(workerCount),
				"SingleWorkerScheduler drives exactly one worker — its ring is strictly SPSC. " +
				"Use RoutedScheduler for more than one.");
		// A struct SingleWorkerScheduler wraps a reference to shared State; handing the SAME value to a
		// second ActorSystem would replace the ring out from under a live worker — two consumers on one
		// KeyRing, which corrupts silently. One instance per system, only — same guard as
		// RoutedScheduler.Start.
		if (_state.Started)
			throw new InvalidOperationException(
				"this scheduler instance is already driving a system; construct a new one per ActorSystem.");
		_state.Started = true;
		_state.Ring = new KeyRing(Capacity);
		_state.Local = new int[Capacity];
	}

	public void Schedule(int cellIdx) {
		if (!_state.Ring!.TryEnqueue(cellIdx))
			throw new InvalidOperationException(
				"scheduler ring full — it is sized to the map, so this indicates a key index was scheduled twice.");
		_state.WorkAvailable.Set();
	}

	public void ScheduleLocal(int workerId, int cellIdx) => _state.Local![_state.LocalCount++] = cellIdx;

	public bool TryAcquire(int workerId, out int cellIdx) {
		if (_state.LocalCount > 0) {
			cellIdx = _state.Local![--_state.LocalCount];
			return true;
		}

		return _state.Ring!.TryDequeue(out cellIdx);
	}

	public bool HasWork(int workerId) => _state.LocalCount > 0 || _state.Ring!.CountApprox > 0;

	public void WakeAll() => _state.WorkAvailable.Set();

	public void SignalStop() => _state.WorkAvailable.Set();

	public void ArmWait(int workerId) => _state.WorkAvailable.Reset();

	public bool Wait(int workerId, int millisecondsTimeout) => _state.WorkAvailable.Wait(millisecondsTimeout);
}
