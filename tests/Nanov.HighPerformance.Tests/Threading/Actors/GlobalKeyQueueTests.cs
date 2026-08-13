namespace Nanov.HighPerformance.Tests.Threading.Actors;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors.Internal;
using NUnit.Framework;

[TestFixture]
public class GlobalKeyQueueTests {
	/// <summary>
	///   <c>TryEnqueue</c>/<c>TryDequeue</c> are the first methods in this tree with unbounded retry
	///   loops — a corrupted sequence stamp (e.g. an off-by-one in the recycle stamp) makes the
	///   <c>difference</c> check never settle, spinning forever. <c>[CancelAfter]</c> cannot interrupt
	///   that: it only arms a token nothing here observes. Running the body on its own thread and joining
	///   it with a real bounded wait means a regression here fails red instead of hanging the whole run,
	///   the same pattern <c>ActorSystemStressTests</c> and <c>WorkStealingDequeTests</c> use. The body
	///   never asserts off-thread — a failure is captured and re-thrown on the test thread after the join
	///   so it reports as a normal red test rather than a crash.
	/// </summary>
	private static void RunBounded(Action body, TimeSpan timeout) {
		ExceptionDispatchInfo? error = null;
		var thread = new Thread(() => {
			try {
				body();
			}
			catch (Exception ex) {
				error = ExceptionDispatchInfo.Capture(ex);
			}
		}) { IsBackground = true };
		thread.Start();
		Assert.That(thread.Join(timeout), Is.True, "test body wedged");
		error?.Throw();
	}

	[Test]
	public void Dequeues_In_Fifo_Order() {
		RunBounded(() => {
			var queue = new GlobalKeyQueue(16);
			for (var i = 0; i < 8; i++)
				Assert.That(queue.TryEnqueue(i), Is.True);

			var seen = new List<int>();
			while (queue.TryDequeue(out var value))
				seen.Add(value);

			Assert.That(seen, Is.EqualTo(Enumerable.Range(0, 8).ToList()));
		}, TimeSpan.FromSeconds(10));
	}

	[Test]
	public void Refuses_To_Enqueue_When_Full() {
		RunBounded(() => {
			var queue = new GlobalKeyQueue(4);
			for (var i = 0; i < queue.Capacity; i++)
				Assert.That(queue.TryEnqueue(i), Is.True);

			Assert.That(queue.TryEnqueue(99), Is.False);
		}, TimeSpan.FromSeconds(10));
	}

	[Test]
	public void Empty_Queue_Yields_Nothing() {
		var queue = new GlobalKeyQueue(8);
		Assert.Multiple(() => {
			Assert.That(queue.TryDequeue(out _), Is.False);
			Assert.That(queue.CountApprox, Is.EqualTo(0));
		});
	}

	[Test]
	public void Slots_Are_Reusable_After_A_Full_Wrap() {
		RunBounded(() => {
			var queue = new GlobalKeyQueue(4);
			for (var round = 0; round < 100; round++) {
				for (var i = 0; i < queue.Capacity; i++)
					Assert.That(queue.TryEnqueue(round * 10 + i), Is.True, $"enqueue failed on round {round}");
				for (var i = 0; i < queue.Capacity; i++) {
					Assert.That(queue.TryDequeue(out var value), Is.True, $"dequeue failed on round {round}");
					Assert.That(value, Is.EqualTo(round * 10 + i));
				}
			}
		}, TimeSpan.FromSeconds(10));
	}

	/// <summary>
	///   Many enqueuers, many dequeuers — the real shape, since a worker with a full deque spills
	///   here alongside the producer. Oracle is set equality: nothing lost, nothing handed out twice.
	/// </summary>
	[Test]
	public void Concurrent_Producers_And_Consumers_Lose_And_Duplicate_Nothing() {
		const int producers = 3;
		const int consumers = 4;
		const int perProducer = 50_000;
		const int total = producers * perProducer;
		var queue = new GlobalKeyQueue(1024);
		var taken = new List<int>[consumers];
		var producersDone = 0;
		// Set in a finally once we know the test has failed (a Join timed out), so a wedge on one thread
		// doesn't leave the others hot-spinning through every later test in the run.
		var abort = false;

		var consumerThreads = new Thread[consumers];
		for (var c = 0; c < consumers; c++) {
			var id = c;
			taken[id] = new List<int>();
			consumerThreads[id] = new Thread(() => {
				while (Volatile.Read(ref producersDone) < producers && !Volatile.Read(ref abort))
					if (queue.TryDequeue(out var value)) taken[id].Add(value);
				while (queue.TryDequeue(out var value)) taken[id].Add(value);
			}) { IsBackground = true };
			consumerThreads[id].Start();
		}

		var producerThreads = new Thread[producers];
		for (var p = 0; p < producers; p++) {
			var id = p;
			producerThreads[id] = new Thread(() => {
				for (var i = 0; i < perProducer; i++) {
					var value = id * perProducer + i;
					while (!queue.TryEnqueue(value)) { // full: consumers will drain
						if (Volatile.Read(ref abort)) return;
						Thread.SpinWait(16);
					}
				}

				Interlocked.Increment(ref producersDone);
			}) { IsBackground = true };
			producerThreads[id].Start();
		}

		try {
			foreach (var thread in producerThreads)
				Assert.That(thread.Join(TimeSpan.FromSeconds(60)), Is.True, "producer wedged");
			foreach (var thread in consumerThreads)
				Assert.That(thread.Join(TimeSpan.FromSeconds(60)), Is.True, "consumer wedged");
		}
		finally {
			Volatile.Write(ref abort, true);
		}

		var all = taken.SelectMany(x => x).ToList();
		Assert.Multiple(() => {
			Assert.That(all, Has.Count.EqualTo(total), "items lost or duplicated");
			Assert.That(all.Distinct().Count(), Is.EqualTo(total), "an item was handed out twice");
		});
	}
}
