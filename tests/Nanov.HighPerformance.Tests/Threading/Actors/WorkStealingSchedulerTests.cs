namespace Nanov.HighPerformance.Tests.Threading.Actors;

using System;
using System.Linq;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using Nanov.HighPerformance.Tests.Threading.Workers;
using NUnit.Framework;

[TestFixture]
public class WorkStealingSchedulerTests {
	[Test]
	public void Rejects_A_Second_System() {
		var scheduler = new WorkStealingScheduler();
		scheduler.Start(2, _ => { });
		Assert.Throws<InvalidOperationException>(() => scheduler.Start(2, _ => { }),
			"one scheduler instance drives one system — sharing it would put two owners on a deque");
	}

	[Test]
	public void A_Worker_Takes_Its_Own_Local_Schedule_Before_The_Global_Queue() {
		var scheduler = new WorkStealingScheduler { Capacity = 64, DequeCapacity = 64 };
		scheduler.Start(1, _ => { });
		scheduler.Schedule(11);      // global
		scheduler.ScheduleLocal(0, 22); // this worker's deque

		Assert.That(scheduler.TryAcquire(0, out var first), Is.True);
		Assert.That(first, Is.EqualTo(22), "local first — that is where the affinity comes from");
		Assert.That(scheduler.TryAcquire(0, out var second), Is.True);
		Assert.That(second, Is.EqualTo(11));
		Assert.That(scheduler.TryAcquire(0, out _), Is.False);
	}

	[Test]
	public void An_Idle_Worker_Steals_From_A_Busy_Peer() {
		var scheduler = new WorkStealingScheduler { Capacity = 64, DequeCapacity = 64 };
		scheduler.Start(2, _ => { });
		scheduler.ScheduleLocal(0, 7);

		Assert.That(scheduler.HasWork(1), Is.True,
			"worker 1 must SEE a peer's stealable key, or it parks and D degenerates into B");
		Assert.That(scheduler.TryAcquire(1, out var stolen), Is.True);
		Assert.Multiple(() => {
			Assert.That(stolen, Is.EqualTo(7));
			Assert.That(scheduler.StealsSucceeded, Is.GreaterThan(0));
		});
	}

	[Test]
	public void A_Full_Deque_Spills_To_The_Global_Queue() {
		var scheduler = new WorkStealingScheduler { Capacity = 256, DequeCapacity = 4 };
		scheduler.Start(1, _ => { });

		for (var i = 0; i < 16; i++)
			scheduler.ScheduleLocal(0, i);

		Assert.That(scheduler.DequeOverflows, Is.GreaterThan(0), "the overflow path never ran");

		var taken = new System.Collections.Generic.List<int>();
		while (scheduler.TryAcquire(0, out var value))
			taken.Add(value);

		Assert.That(taken.OrderBy(x => x), Is.EqualTo(Enumerable.Range(0, 16)),
			"spilled keys must still be acquirable — order is irrelevant, loss is not");
	}

	/// <summary>
	///   The wake edge. A parked worker must be released by a schedule from another thread. This is
	///   the Dekker handshake between the enqueue and the parked-flag read; get the fence wrong and
	///   this test hangs rather than fails, which is why it is bounded by a join.
	/// </summary>
	[Test]
	public void A_Parked_Worker_Is_Woken_By_A_Schedule() {
		var scheduler = new WorkStealingScheduler { Capacity = 64, DequeCapacity = 64 };
		scheduler.Start(2, _ => { });

		var acquired = -1;
		var parked = new ManualResetEventSlim(false);
		var worker = new Thread(() => {
			scheduler.ArmWait(0);
			Interlocked.MemoryBarrier();
			parked.Set();
			scheduler.Wait(0);
			if (scheduler.TryAcquire(0, out var value)) Volatile.Write(ref acquired, value);
		}) { IsBackground = true };
		worker.Start();

		Assert.That(parked.Wait(TimeSpan.FromSeconds(5)), Is.True);
		Thread.Sleep(50); // let it actually reach Wait
		scheduler.Schedule(42);

		Assert.That(worker.Join(TimeSpan.FromSeconds(10)), Is.True,
			"the parked worker was never woken — lost wakeup on the schedule path");
		Assert.That(Volatile.Read(ref acquired), Is.EqualTo(42));
	}

	[Test]
	public void Signal_Stop_Releases_Every_Parked_Worker() {
		var scheduler = new WorkStealingScheduler { Capacity = 64, DequeCapacity = 64 };
		scheduler.Start(3, _ => { });

		var released = 0;
		var threads = new Thread[3];
		for (var i = 0; i < threads.Length; i++) {
			var id = i;
			threads[id] = new Thread(() => {
				scheduler.ArmWait(id);
				scheduler.Wait(id);
				Interlocked.Increment(ref released);
			}) { IsBackground = true };
			threads[id].Start();
		}

		Thread.Sleep(100);
		scheduler.SignalStop();
		foreach (var thread in threads)
			Assert.That(thread.Join(TimeSpan.FromSeconds(10)), Is.True, "a worker was left parked");

		Assert.That(Volatile.Read(ref released), Is.EqualTo(3));
	}

	[Test]
	[CancelAfter(60_000)]
	public void Eight_Workers_Preserve_Per_Key_Order_And_Lose_Nothing() {
		const int keys = 32;
		const int perKey = 500;
		const int total = keys * perKey;
		var state = new RecordingState();
		using var system = new ActorSystem<int, int, RecordingHandler, WorkStealingScheduler>(
			new RecordingHandler(state), keyCapacity: 128, chunkCount: 32, chunkSize: 32,
			batchBound: 4, workers: 8,
			scheduler: new WorkStealingScheduler { Capacity = 512, DequeCapacity = 256 });
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

		Assert.That(producer.Join(TimeSpan.FromSeconds(45)), Is.True,
			$"producer wedged after {Volatile.Read(ref dispatched)}/{total} dispatches");
		if (producerError is not null) Assert.Fail($"producer threw: {producerError}");

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, total);

		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");
	}
}
