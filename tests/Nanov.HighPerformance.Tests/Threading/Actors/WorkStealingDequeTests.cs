namespace Nanov.HighPerformance.Tests.Threading.Actors;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors.Internal;
using NUnit.Framework;

[TestFixture]
public class WorkStealingDequeTests {
	[Test]
	public void Owner_Pops_Its_Own_Pushes_In_Lifo_Order() {
		var deque = new WorkStealingDeque(16);
		for (var i = 0; i < 5; i++)
			Assert.That(deque.TryPushBottom(i), Is.True);
		Assert.That(deque.CountApprox, Is.EqualTo(5));

		var popped = new List<int>();
		while (deque.TryPopBottom(out var value))
			popped.Add(value);

		Assert.That(popped, Is.EqualTo(new List<int> { 4, 3, 2, 1, 0 }));
	}

	[Test]
	public void Thieves_Take_From_The_Oldest_End() {
		var deque = new WorkStealingDeque(16);
		for (var i = 0; i < 5; i++)
			deque.TryPushBottom(i);

		var stolen = new List<int>();
		while (deque.TrySteal(out var value))
			stolen.Add(value);

		Assert.That(stolen, Is.EqualTo(new List<int> { 0, 1, 2, 3, 4 }),
			"the owner takes the newest end so its cache stays warm; thieves take the oldest");
	}

	[Test]
	public void Push_Fails_When_Full_So_The_Caller_Can_Spill_To_The_Global_Queue() {
		var deque = new WorkStealingDeque(4);
		for (var i = 0; i < deque.Capacity; i++)
			Assert.That(deque.TryPushBottom(i), Is.True);

		Assert.That(deque.TryPushBottom(99), Is.False);
	}

	[Test]
	public void Empty_Deque_Yields_Nothing_To_Either_End() {
		var deque = new WorkStealingDeque(8);
		Assert.Multiple(() => {
			Assert.That(deque.TryPopBottom(out _), Is.False);
			Assert.That(deque.TrySteal(out _), Is.False);
			Assert.That(deque.CountApprox, Is.EqualTo(0));
		});

		// A failed pop must leave `bottom` where it found it, or the next push writes the wrong slot.
		deque.TryPushBottom(7);
		Assert.That(deque.TryPopBottom(out var value), Is.True);
		Assert.That(value, Is.EqualTo(7));
	}

	/// <summary>
	///   The single-element race: owner and thief both go for the last item. Exactly one may win.
	///   Run enough rounds that the interleaving is actually sampled — this is the case the CAS in
	///   <c>TryPopBottom</c> exists for, and a deque that gets it wrong hands the SAME key to two
	///   workers, which breaks the one-owner invariant the whole system rests on.
	///   <para>
	///     A fresh <see cref="Thread" /> per round does not sample this race: thread creation and
	///     first-schedule (~10-100 µs) dwarfs <c>TryPopBottom</c>/<c>TrySteal</c> (~10 ns) by three to
	///     four orders of magnitude, so the owner is done before the thief's delegate runs its first
	///     instruction. A single long-lived thief thread rendezvousing with the owner through a
	///     two-party <see cref="Barrier" /> every round gets both sides releasing within nanoseconds of
	///     each other, so the CAS in <c>TryPopBottom</c> is actually exercised.
	///   </para>
	/// </summary>
	[Test]
	public void Owner_And_Thief_Never_Both_Take_The_Last_Item() {
		const int rounds = 20_000;
		var deque = new WorkStealingDeque(4);
		var doubleTaken = 0;
		var lost = 0;
		var thiefWins = 0;
		var thiefGot = false;

		using var barrier = new Barrier(2);

		var thief = new Thread(() => {
			for (var round = 0; round < rounds; round++) {
				barrier.SignalAndWait();
				thiefGot = deque.TrySteal(out _);
				barrier.SignalAndWait();
			}
		}) { IsBackground = true };
		thief.Start();

		for (var round = 0; round < rounds; round++) {
			deque.TryPushBottom(round);
			barrier.SignalAndWait(); // release owner and thief together, right before the race
			var ownerGot = deque.TryPopBottom(out _);
			barrier.SignalAndWait(); // rendezvous again: thiefGot is now safe to read

			if (ownerGot && thiefGot) doubleTaken++;
			if (!ownerGot && !thiefGot) lost++;
			if (!ownerGot && thiefGot) thiefWins++;
			while (deque.TryPopBottom(out _)) { }
		}

		Assert.That(thief.Join(TimeSpan.FromSeconds(10)), Is.True, "thief wedged");

		Assert.Multiple(() => {
			Assert.That(doubleTaken, Is.EqualTo(0), "the same key was handed to two workers");
			Assert.That(lost, Is.EqualTo(0), "an item vanished — neither end got it");
			Assert.That(thiefWins, Is.GreaterThan(0), "the race was never sampled");
		});
	}

	/// <summary>
	///   The empty poll must not disturb the cursors. This is the path <c>TryAcquire</c> takes on every
	///   failed acquire — the overwhelming majority of calls, since a worker's deque never exceeds depth
	///   1 — so an early-out that got the cursor arithmetic wrong would corrupt the next push rather than
	///   fail visibly.
	/// </summary>
	[Test]
	public void An_Empty_Poll_Leaves_The_Cursors_Untouched_And_The_Deque_Usable() {
		var deque = new WorkStealingDeque(8);

		for (var i = 0; i < 1_000; i++)
			Assert.That(deque.TryPopBottom(out _), Is.False, $"poll {i} claimed an item from an empty deque");

		Assert.That(deque.CountApprox, Is.EqualTo(0), "an empty poll moved a cursor");

		// The real damage an off-by-one here would do is silent: the next push writes the wrong slot.
		for (var i = 0; i < 8; i++)
			Assert.That(deque.TryPushBottom(i), Is.True, $"push {i} failed after 1000 empty polls");

		var popped = new List<int>();
		while (deque.TryPopBottom(out var value))
			popped.Add(value);

		Assert.That(popped, Is.EqualTo(new List<int> { 7, 6, 5, 4, 3, 2, 1, 0 }));
	}

	/// <summary>
	///   Sustained concurrent stress: one owner pushing and popping while three thieves steal.
	///   The oracle is set equality — every pushed value taken exactly once, by exactly one taker.
	/// </summary>
	[Test]
	public void Concurrent_Owner_And_Thieves_Lose_And_Duplicate_Nothing() {
		const int items = 200_000;
		const int thieves = 3;
		var deque = new WorkStealingDeque(256);
		var ownerTaken = new List<int>();
		var thiefTaken = new List<int>[thieves];
		var done = false;

		var workers = new Thread[thieves];
		for (var t = 0; t < thieves; t++) {
			var id = t;
			thiefTaken[id] = new List<int>();
			workers[id] = new Thread(() => {
				while (!Volatile.Read(ref done))
					if (deque.TrySteal(out var value)) thiefTaken[id].Add(value);
				while (deque.TrySteal(out var value)) thiefTaken[id].Add(value);
			}) { IsBackground = true };
			workers[id].Start();
		}

		var pushed = 0;
		while (pushed < items) {
			if (deque.TryPushBottom(pushed)) {
				pushed++;
				continue;
			}

			// Full — drain one from our own end so the producer side makes progress.
			if (deque.TryPopBottom(out var mine)) ownerTaken.Add(mine);
		}

		while (deque.TryPopBottom(out var mine)) ownerTaken.Add(mine);
		Volatile.Write(ref done, true);
		foreach (var worker in workers)
			Assert.That(worker.Join(TimeSpan.FromSeconds(30)), Is.True, "thief wedged");

		while (deque.TryPopBottom(out var mine)) ownerTaken.Add(mine);

		var all = ownerTaken.Concat(thiefTaken.SelectMany(x => x)).ToList();
		Assert.Multiple(() => {
			Assert.That(all, Has.Count.EqualTo(items), "items lost or duplicated");
			Assert.That(all.Distinct().Count(), Is.EqualTo(items), "an item was taken twice");
		});
	}
}
