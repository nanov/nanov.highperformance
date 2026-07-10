namespace Nanov.HighPerformance.Tests.Threading.Workers;

using Nanov.HighPerformance.Threading.Workers;
using NUnit.Framework;

/// <summary>
///   Guards for the batched-head-publish hazards (see BATCHED HEAD PUBLISH in the worker CLAUDE.md):
///   a parked producer must be flushed within at most one handler execution; HandleScope.Release must
///   publish immediately (never join the batch); soft-complete must drain everything even when
///   releases are sitting unpublished in the batch at complete time. All three also hold trivially on
///   the pre-batching (per-item advance) code — they land first as green regression guards.
/// </summary>
[TestFixture]
public class BatchedReleaseTests {
	private sealed class HookState {
		public Action<int>? Hook;
		public int Processed;
	}

	private readonly struct HookHandler : IWorkerHandler<int> {
		private readonly HookState _state;

		public HookHandler(HookState state) => _state = state;

		public void Handle(int item) {
			_state.Hook?.Invoke(item);
			Volatile.Write(ref _state.Processed, _state.Processed + 1);
		}

		public void OnError(Exception error) { }
		public void OnCompleted(Exception? error) { }
	}

	// Consumer gated inside item 0's handler while the producer fills the ring and parks. Ungating
	// the handler must unblock the producer promptly (run-boundary / parked-flag flush) — a batch
	// that only flushes on a count threshold would leave the producer parked forever here.
	// Fill math is behavior-agnostic on purpose (this must be green BEFORE batching lands too):
	// after item 0 is claimed, Head is 1 under per-item advance but 0 under a pending batch, so the
	// foreground fill stops at 15 extra items (safe either way) and the background task dispatches
	// TWO more — it blocks on the first or the second, under either behavior, while the handler is
	// gated.
	[Test]
	public void Parked_Producer_Unblocks_After_At_Most_One_Handler() {
		var entered = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		var state = new HookState {
			Hook = i => {
				if (i != 0) return;
				entered.Set();
				gate.Wait();
			}
		};
		using var w = new SpWorker<int, HookHandler>(new HookHandler(state), 16); // flush threshold 4
		w.Start();

		Assert.That(w.Dispatch(0), Is.True);
		Assert.That(entered.Wait(2000), Is.True, "consumer must be gated inside item 0's handler");
		for (var i = 1; i <= 15; i++) Assert.That(w.Dispatch(i), Is.True); // never blocks, either behavior

		var blocked = Task.Run(() => w.Dispatch(16) && w.Dispatch(17));
		Assert.That(blocked.Wait(50), Is.False, "producer should be parked on the full ring");

		gate.Set(); // finish item 0's handler — flush must free space within this handler boundary
		Assert.That(blocked.Wait(5000), Is.True, "flush must free space within one handler boundary");
		Assert.That(blocked.Result, Is.True);

		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => Volatile.Read(ref state.Processed), 18);
		Assert.That(Volatile.Read(ref state.Processed), Is.EqualTo(18));
	}

	// Soft-complete fired while the consumer is mid-run (intermittent stalls keep releases batched)
	// must still deliver every committed item — the drain path always flushes at run/park boundaries.
	[Test]
	public void Soft_Complete_Drains_With_Batched_Releases_Pending() {
		var state = new HookState { Hook = i => { if ((i & 255) == 0) Thread.Sleep(1); } };
		using var w = new SpWorker<int, HookHandler>(new HookHandler(state), 64); // flush threshold 16
		w.Start();

		const int n = 10_000;
		var dispatched = 0;
		var producer = Task.Run(() => {
			for (var i = 0; i < n; i++) {
				w.Dispatch(i);
				Volatile.Write(ref dispatched, i + 1);
			}
		});
		// Producer-side stall watchdog: Dispatch blocks on a full ring, so a regression here would
		// hang the producer forever — a plateauing `dispatched` fails fast instead.
		WorkerTestUtil.AwaitCompletionOrStall(producer, () => Volatile.Read(ref dispatched), n);
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => Volatile.Read(ref state.Processed), n);

		Assert.That(Volatile.Read(ref state.Processed), Is.EqualTo(n));
	}

	private struct Slot {
		public int Value;
	}

	private sealed class EarlyReleaseState {
		public readonly ManualResetEventSlim Entered = new(false);
		public readonly ManualResetEventSlim CanRelease = new(false);
		public readonly ManualResetEventSlim Released = new(false);
		public readonly ManualResetEventSlim Finish = new(false);
		public int Processed;
	}

	private readonly struct EarlyReleaseHandler : IValueWorkerHandler<Slot> {
		private readonly EarlyReleaseState _state;

		public EarlyReleaseHandler(EarlyReleaseState state) => _state = state;

		public void Handle(ref HandleScope<Slot> scope) {
			var value = scope.Event().Value;
			if (value == 0) {
				_state.Entered.Set();
				_state.CanRelease.Wait();
				scope.Release(); // must publish IMMEDIATELY — the parked producer unblocks mid-handler
				_state.Released.Set();
				_state.Finish.Wait();
			}

			Volatile.Write(ref _state.Processed, _state.Processed + 1);
		}

		public void Clear(ref Slot value) { }
		public void OnError(Exception error) { }
		public void OnCompleted(Exception? error) { }
	}

	// The early-release contract survives batching: a producer parked on the full ring resumes the
	// moment the handler calls Release — while the handler is still running — because Release
	// bypasses the batch and publishes immediately.
	[Test]
	public void HandleScope_Release_Unblocks_A_Parked_Producer_Mid_Handler() {
		var state = new EarlyReleaseState();
		using var w = new ValueSpWorker<Slot, EarlyReleaseHandler>(new EarlyReleaseHandler(state), 4);
		w.Start();

		using (var s = w.Publish()) s.Event().Value = 0;
		Assert.That(state.Entered.Wait(2000), Is.True, "consumer must be inside item 0's handler");
		// Slot 0 is still held by the scope (not released) → Head = 0. Three more publishes make the
		// cap-4 ring full; the next one blocks.
		for (var i = 1; i <= 3; i++)
			using (var s = w.Publish())
				s.Event().Value = i;

		var blocked = Task.Run(() => {
			using var s = w.Publish();
			if (!s.IsOpen) return false;
			s.Event().Value = 4;
			return true;
		});
		Assert.That(blocked.Wait(50), Is.False, "producer should be blocked on the full ring");

		state.CanRelease.Set();
		Assert.That(state.Released.Wait(2000), Is.True);
		Assert.That(blocked.Wait(2000), Is.True, "Release must publish immediately and unblock the producer");
		Assert.That(blocked.Result, Is.True);
		Assert.That(Volatile.Read(ref state.Processed), Is.Zero, "item 0's handler must still be running");

		state.Finish.Set();
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => Volatile.Read(ref state.Processed), 5);
		Assert.That(Volatile.Read(ref state.Processed), Is.EqualTo(5));
	}
}
