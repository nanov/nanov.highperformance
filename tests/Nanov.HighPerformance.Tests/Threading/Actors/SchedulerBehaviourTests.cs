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

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, total);

		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");
	}

	/// <summary>
	///   Test 14. Every worker must genuinely PARK, and the system must still complete. A lost-wakeup
	///   bug passes every test whose workers never sleep, so the sleep here is the point: the dispatch
	///   is tiny and the pause is long enough that eight workers are certainly blocked before
	///   TryComplete runs.
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

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, total);

		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");
	}

	/// <summary>Test 11 — a throwing handler reports through OnError and does not break the key's order.</summary>
	[Test]
	[CancelAfter(60_000)]
	public void A_Throwing_Handler_Reports_And_The_Key_Keeps_Its_Order() {
		const int perKey = 200;
		var state = new RecordingState { Throw = (key, value) => key == 1 && value % 10 == 0 };
		using var system = NewSystem(state, keyCapacity: 64, chunkCount: 16, chunkSize: 64,
			batchBound: 4, workers: 4, schedulerCapacity: 256);
		system.Start();

		for (var i = 0; i < perKey; i++)
			for (var k = 0; k < 4; k++)
				system.Dispatch(k, i);

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, perKey * 4);

		var byKey = state.ByKey();
		Assert.Multiple(() => {
			Assert.That(state.Errors, Has.Count.EqualTo(perKey / 10), "every poison message must reach OnError");
			Assert.That(byKey[0], Is.EqualTo(Enumerable.Range(0, perKey).ToList()));
			Assert.That(byKey[1], Is.EqualTo(Enumerable.Range(0, perKey).Where(v => v % 10 != 0).ToList()),
				"the surviving messages of the poisoned key must still be in order");
		});
	}
}
