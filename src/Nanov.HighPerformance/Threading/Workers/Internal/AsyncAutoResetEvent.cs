namespace Nanov.HighPerformance.Threading.Workers.Internal;

using System;
using System.Threading.Tasks.Sources;

/// <summary>
///   Zero-allocation, single-waiter auto-reset async event for the ThreadPool-async worker's
///   "ring empty → wait for an item" path. <see cref="Set" /> completes a pending
///   <see cref="WaitAsync" /> or latches one signal (so a <c>Set</c> that arrives just before the
///   next <c>WaitAsync</c> isn't lost — mirrors the edge-triggered <c>ManualResetEventSlim</c> usage
///   on the dedicated-thread path). There is at most one waiter (the single logical consumer) and
///   one setter (the single producer) at a time; a small lock guards the rare transition (the wait
///   is off the hot path — the ring is rarely empty under load). Reused across waits, so no per-item
///   allocation.
/// </summary>
internal sealed class AsyncAutoResetEvent : IValueTaskSource {
	private readonly object _gate = new();
	private ManualResetValueTaskSourceCore<bool> _core; // mutable struct — must NOT be readonly
	private bool _signaled;
	private bool _waiting;

	public AsyncAutoResetEvent() => _core.RunContinuationsAsynchronously = true;

	public void Set() {
		lock (_gate) {
			if (_waiting) {
				_waiting = false;
				_core.SetResult(true); // RunContinuationsAsynchronously => continuation is queued, not run under the lock
			} else {
				_signaled = true;
			}
		}
	}

	public ValueTask WaitAsync() {
		lock (_gate) {
			if (_signaled) {
				_signaled = false;
				return ValueTask.CompletedTask;
			}

			_core.Reset();
			_waiting = true;
			return new ValueTask(this, _core.Version);
		}
	}

	ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _core.GetStatus(token);

	void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token,
		ValueTaskSourceOnCompletedFlags flags) =>
		_core.OnCompleted(continuation, state, token, flags);

	void IValueTaskSource.GetResult(short token) => _core.GetResult(token);
}
