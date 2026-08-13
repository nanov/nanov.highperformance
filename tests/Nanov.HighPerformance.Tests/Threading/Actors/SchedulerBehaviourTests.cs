namespace Nanov.HighPerformance.Tests.Threading.Actors;

using System;
using System.Linq;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using Nanov.HighPerformance.Tests.Threading.Workers;
using NUnit.Framework;

// No `out` variance annotation: a variant type parameter cannot be instantiated with a value type,
// and every scheduler here is a struct by design.
/// <summary>Builds a scheduler of the fixture's type — the schedulers have different knobs.</summary>
public interface ISchedulerFactory<TScheduler> where TScheduler : struct, IKeyScheduler {
	TScheduler Create(int capacity);
}

internal readonly struct RoutedFactory : ISchedulerFactory<RoutedScheduler> {
	public RoutedScheduler Create(int capacity) => new() { Capacity = capacity };
}

internal readonly struct WorkStealingFactory : ISchedulerFactory<WorkStealingScheduler> {
	public WorkStealingScheduler Create(int capacity) =>
		new() { Capacity = capacity, DequeCapacity = capacity };
}

/// <summary>
///   The behaviours that must hold no matter which scheduler is installed. Per-key order comes from
///   exclusive cell ownership, not from queue discipline (§3.1), so a scheduler that reorders,
///   steals, or hands keys out LIFO must still pass every one of these unchanged.
/// </summary>
[TestFixture(typeof(RoutedScheduler), typeof(RoutedFactory))]
[TestFixture(typeof(WorkStealingScheduler), typeof(WorkStealingFactory))]
public class SchedulerBehaviourTests<TScheduler, TFactory>
	where TScheduler : struct, IKeyScheduler
	where TFactory : struct, ISchedulerFactory<TScheduler> {
	private static ActorSystem<int, int, RecordingHandler, TScheduler> NewSystem(
		RecordingState state, int keyCapacity, int chunkCount, int chunkSize, int batchBound,
		int workers, int schedulerCapacity) =>
		new(new RecordingHandler(state), keyCapacity, chunkCount, chunkSize, batchBound, workers,
			default(TFactory).Create(schedulerCapacity));

	[Test]
	[CancelAfter(120_000)]
	public void Eight_Workers_Preserve_Per_Key_Order_And_Lose_Nothing() {
		const int keys = 64;
		const int perKey = 1_000;
		const int total = keys * perKey;
		var state = new RecordingState();
		using var system = NewSystem(state, keyCapacity: 256, chunkCount: 64, chunkSize: 16,
			batchBound: 4, workers: 8, schedulerCapacity: 1024);
		system.Start();

		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				for (var i = 0; i < perKey; i++)
					for (var k = 0; k < keys; k++) {
						if (!system.Dispatch(k, i)) return;
						dispatched++;
					}
			}
			catch (Exception ex) {
				producerError = ex;
			}
		}) { IsBackground = true };
		producer.Start();

		Assert.That(producer.Join(TimeSpan.FromSeconds(90)), Is.True,
			$"producer wedged after {Volatile.Read(ref dispatched)}/{total} dispatches");
		if (producerError is not null) Assert.Fail($"producer threw: {producerError}");
		Assert.That(Volatile.Read(ref dispatched), Is.EqualTo(total), "a Dispatch was rejected mid-run");

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, total);

		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");

		// Unambiguous here, unlike Eviction_And_Harvest_Under_Eight_Workers_Lose_Nothing below: with 64
		// keys against chunkCount: 64 (64 of the map's 512 slots), every key keeps its own chunk for the
		// run's whole lifetime, so neither Harvest nor EvictFree ever runs and no cell is ever tombstoned
		// or re-keyed. A stale token therefore cannot legitimately win a CAS against a re-keyed slot (the
		// caveat ActorSystem.Activate's chunk == Chunk.None guard documents, and that
		// Eviction_And_Harvest_Under_Eight_Workers_Lose_Nothing is NOT free of) — a nonzero count here can
		// only mean a genuine duplicate token for one key index. That is exactly the failure mode a
		// scheduler that lets a key move between workers (D's stealing, D's ScheduleLocal spill into the
		// shared global queue) would expose first, and until this assertion the invariant had never been
		// checked under any such scheduler — only under SingleWorkerScheduler and RoutedScheduler, neither
		// of which can move a key once it is scheduled.
		Assert.That(system.RejectedClaims, Is.EqualTo(0),
			"a worker lost the Scheduled -> Owned CAS: two tokens existed for one key index");
	}

	/// <summary>
	///   N=8 lifecycle smoke test — NOT a guard for the quiescence broadcast, and not for a lost wakeup on
	///   the schedule path either. Trace it: 4 messages, <c>batchBound: 0</c>, all handled and published
	///   long before the 500&#160;ms sleep ends, so by the time <c>TryComplete</c> sets <c>_completed</c>
	///   no <c>PublishProcessed</c> call is still pending to observe <c>IsQuiescent()</c> true — it reads
	///   <c>_completed</c> first, and every publish already happened while that flag was false. The
	///   quiescence broadcast in <c>PublishProcessed</c> therefore never fires; every parked worker is
	///   released by <c>TryComplete</c>'s own unconditional <c>SignalStop()</c> instead. This test passes
	///   with that entire broadcast block deleted. It also does not detect a lost wakeup on dispatch: a
	///   dropped <c>Set</c> during <c>Schedule</c>/<c>ScheduleLocal</c> just leaves the message sitting
	///   until <c>SignalStop</c>'s own <c>WakeAll</c> releases every worker, which then finds
	///   <c>HasWork == true</c> and drains it anyway.
	///   <para>
	///     What it genuinely guards: that <c>TryComplete</c>'s wake is <b>not</b> flag-gated. If it were,
	///     all 8 workers would sleep forever and <see cref="WorkerTestUtil.AwaitCompletionOrStall" />
	///     would throw within its 5&#160;s stall budget — a fast red, not a hang.
	///   </para>
	///   <para>
	///     The "every worker genuinely parked before <c>TryComplete</c> ran" premise in this test's name
	///     is <b>not</b> verified by anything below and can't be from inside this generic fixture: the
	///     <c>Parks</c> counter lives only on the concrete <c>WorkStealingScheduler</c>, not on
	///     <see cref="IKeyScheduler" />, so nothing here — or in CI — holds it. Checked once by hand
	///     outside this fixture, against this exact shape under D: <c>Parks == 12</c> before
	///     <c>TryComplete</c> ran, with all 4 messages already handled — genuinely asleep, not merely
	///     fast — but that check is not repeatable from here and nothing enforces it going forward.
	///   </para>
	/// </summary>
	[Test]
	[CancelAfter(60_000)]
	public void A_Fully_Parked_System_Still_Completes() {
		var state = new RecordingState();
		using var system = NewSystem(state, keyCapacity: 64, chunkCount: 16, chunkSize: 16,
			batchBound: 0, workers: 8, schedulerCapacity: 256);
		system.Start();

		for (var i = 0; i < 4; i++)
			system.Dispatch(i, i);

		Thread.Sleep(500); // everyone drains, then parks

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 4);

		Assert.Multiple(() => {
			Assert.That(state.HandledCount, Is.EqualTo(4));
			Assert.That(state.CompletedCalls, Is.EqualTo(8), "OnCompleted fires once per worker");
		});
	}

	/// <summary>
	///   Test 15 — the F2 interleaving. TryComplete races a final in-flight message; Completion must
	///   not resolve before that message is handled.
	/// </summary>
	[Test]
	[CancelAfter(60_000)]
	public void Completion_Never_Resolves_Before_A_Racing_Final_Message_Is_Handled() {
		for (var round = 0; round < 200; round++) {
			var state = new RecordingState();
			using var system = NewSystem(state, keyCapacity: 64, chunkCount: 16, chunkSize: 16,
				batchBound: 0, workers: 4, schedulerCapacity: 256);
			system.Start();

			for (var i = 0; i < 32; i++)
				system.Dispatch(i % 8, i);

			system.TryComplete();
			WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 32);
			Assert.That(state.HandledCount, Is.EqualTo(32), $"round {round}: completed with work outstanding");
		}
	}

	/// <summary>
	///   Both producer cold paths — chunk harvest and cell eviction — against eight concurrently
	///   releasing workers. Tiny keyCapacity and chunkCount force both paths to run constantly.
	/// </summary>
	[Test]
	[CancelAfter(120_000)]
	public void Eviction_And_Harvest_Under_Eight_Workers_Lose_Nothing() {
		const int keys = 400;
		const int perKey = 3;
		const int total = keys * perKey;
		var state = new RecordingState();
		using var system = NewSystem(state, keyCapacity: 8, chunkCount: 4, chunkSize: 4,
			batchBound: 1, workers: 8, schedulerCapacity: 256);
		system.Start();

		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				for (var k = 0; k < keys; k++)
					for (var i = 0; i < perKey; i++) {
						if (!system.Dispatch(k, i)) return;
						dispatched++;
					}
			}
			catch (Exception ex) {
				producerError = ex;
			}
		}) { IsBackground = true };
		producer.Start();

		Assert.That(producer.Join(TimeSpan.FromSeconds(90)), Is.True,
			$"producer wedged after {Volatile.Read(ref dispatched)}/{total} dispatches");
		if (producerError is not null) Assert.Fail($"producer threw: {producerError}");
		Assert.That(Volatile.Read(ref dispatched), Is.EqualTo(total), "a Dispatch was rejected mid-run");

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, total);

		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");

		// The parameters force both cold paths to run today (drift risk, not a present hole): confirm at
		// least one chunk actually got reclaimed rather than relying on that being incidentally true.
		Assert.That(state.ChunkReclaimCalls, Is.GreaterThan(0), "chunk harvest/eviction never ran");
	}

	/// <summary>Test 11 — a throwing handler reports through OnError and does not break the key's order.</summary>
	[Test]
	[CancelAfter(60_000)]
	public void A_Throwing_Handler_Reports_And_The_Key_Keeps_Its_Order() {
		const int perKey = 200;
		const int total = perKey * 4;
		var state = new RecordingState { Throw = (key, value) => key == 1 && value % 10 == 0 };
		using var system = NewSystem(state, keyCapacity: 64, chunkCount: 16, chunkSize: 64,
			batchBound: 4, workers: 4, schedulerCapacity: 256);
		system.Start();

		// perKey: 200 against chunkSize: 64 means the mailbox WILL fill and Dispatch WILL block in
		// WaitForMailboxSpace — the expected path here, not an edge case. A test-thread Dispatch loop
		// cannot be interrupted by CancelAfter (ActorSystemStressTests documents this empirically), so if
		// a D-only defect wedges a worker mid-drain this would hang with no red test instead of failing
		// fast on the bounded join below.
		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				for (var i = 0; i < perKey; i++)
					for (var k = 0; k < 4; k++) {
						if (!system.Dispatch(k, i)) return;
						dispatched++;
					}
			}
			catch (Exception ex) {
				producerError = ex;
			}
		}) { IsBackground = true };
		producer.Start();

		Assert.That(producer.Join(TimeSpan.FromSeconds(45)), Is.True,
			$"producer wedged after {Volatile.Read(ref dispatched)}/{total} dispatches");
		if (producerError is not null) Assert.Fail($"producer threw: {producerError}");
		Assert.That(Volatile.Read(ref dispatched), Is.EqualTo(total), "a Dispatch was rejected mid-run");

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, total);

		var byKey = state.ByKey();
		Assert.Multiple(() => {
			Assert.That(state.Errors, Has.Count.EqualTo(perKey / 10), "every poison message must reach OnError");
			Assert.That(byKey[0], Is.EqualTo(Enumerable.Range(0, perKey).ToList()));
			Assert.That(byKey[1], Is.EqualTo(Enumerable.Range(0, perKey).Where(v => v % 10 != 0).ToList()),
				"the surviving messages of the poisoned key must still be in order");
		});
	}
}
