namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

using System;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors;

/// <summary>Shared, mutable state observed through a reference — the struct handler is copied per worker.</summary>
public sealed class ActorPerfState {
	public long Handled;

	/// <summary>Spin iterations burned per message. See the calibration test for why this is not zero.</summary>
	public int SpinPerMessage;

	public long HandledCount => Volatile.Read(ref Handled);
}

/// <summary>
///   Constant-cost handler for the throughput cells.
///   <para>
///     <b>The cost is the point, not overhead to be minimised.</b> This system is single-producer: with a
///     cheap handler, eight workers starve on one dispatch thread, every scheduler saturates at the same
///     rate, and B and D measure identical — a null result manufactured by the harness. The spin makes
///     the system consumer-bound so the scheduler is what is being compared.
///   </para>
/// </summary>
public readonly struct ActorPerfHandler : IActorHandler<int, long> {
	private readonly ActorPerfState _state;

	public ActorPerfHandler(ActorPerfState state) => _state = state;

	public void Handle(int key, long message) {
		Thread.SpinWait(_state.SpinPerMessage);
		Interlocked.Increment(ref _state.Handled);
	}

	public void OnError(Exception error) => Interlocked.Increment(ref _state.Handled);

	public void OnCompleted(Exception? error) { }

	public void OnSlotReused(ref long slot) { }

	public void OnChunkReclaimed(Span<long> chunk) { }
}
