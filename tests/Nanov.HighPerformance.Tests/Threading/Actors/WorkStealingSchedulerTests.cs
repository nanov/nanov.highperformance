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
	///   The wake edge. A parked worker must be released by a schedule from another thread — the Dekker
	///   handshake between <c>Schedule</c>'s enqueue-then-flag-read (<c>WakeOne</c>'s fence) and
	///   <c>ArmWait</c>'s Reset-before-flag ordering.
	///   <para>
	///     <b>Why 200 independent trials, not a single Set/Wait handshake.</b> An earlier version of this
	///     test had the worker signal a separate <see cref="ManualResetEventSlim" /> AFTER its own
	///     <c>ArmWait</c> returned, then padded with a 50&#160;ms sleep before calling <c>Schedule</c>.
	///     That handshake is itself a full memory fence — the CLR guarantees whatever the worker wrote
	///     before signalling it is visible once the main thread's wait on it returns, with or without
	///     <c>WakeOne</c>'s own fence — and it necessarily runs strictly after <c>ArmWait</c>'s two
	///     statements have both already executed. So neither the fence nor the statement order could ever
	///     be exercised, sleep or no sleep: the SIGNAL, not the sleep, was what closed the race.
	///   </para>
	///   <para>
	///     <b>Why a fresh scheduler and a single arm per trial, not one worker looping its own arm/wait
	///     cycle 200 times.</b> A first rewrite tried exactly that — one worker thread looping
	///     <c>ArmWait</c>/<c>Wait</c>/drain — and it turned out to be racy against a CORRECT scheduler,
	///     independent of any mutation: between draining a batch (which clears <c>Parked</c> as a side
	///     effect of <c>TryAcquire</c>) and the loop's next <c>ArmWait</c>, there is a window with no
	///     recheck at all, by design — the raw scheduler API contracts that gap to the CALLER
	///     (<c>ActorSystem.WaitForWork</c> closes it with its own armed recheck; this test deliberately
	///     does not, to avoid masking the wake path under test — see below). A <c>Schedule</c> landing in
	///     that window with nothing left to rescue it is a real hang that has nothing to do with
	///     <c>WakeOne</c> or <c>ArmWait</c>. Giving each trial its OWN scheduler and exactly one arm cycle
	///     removes that unrelated race entirely, at the cost of 200 short-lived scheduler instances rather
	///     than one long-lived one.
	///   </para>
	///   <para>
	///     <b>Why no recheck between <c>ArmWait</c> and <c>Wait</c> in the worker.</b> A recheck (e.g.
	///     <c>TryAcquire</c>) would find the key via <see cref="GlobalKeyQueue" />'s own properly-fenced
	///     state directly, independent of whether <c>WakeOne</c>'s wake path works at all — masking
	///     exactly the property under test.
	///   </para>
	///   <para>
	///     <b>Why <c>started</c> does not reintroduce the fence problem.</b> It fires as the FIRST
	///     statement in the worker's lambda, BEFORE <c>ArmWait</c> — so, unlike the removed handshake, it
	///     establishes no ordering against <c>ArmWait</c>'s write or against <c>Wait</c>. It exists only
	///     to absorb OS thread-creation latency (routinely slower than this test's whole race window)
	///     so <c>Schedule</c> does not trivially always win by running before the worker even starts.
	///   </para>
	///   <para>
	///     <b>The head start after <c>started</c> is a nanosecond-scale <c>Thread.SpinWait</c>, never a
	///     sleep.</b> An earlier version used 40 <c>SpinWait.SpinOnce()</c> calls, believing that was a
	///     "handful of spin cycles" — it measures ~30&#160;ms on this machine, because
	///     <c>SpinOnce</c> escalates to <c>Thread.Sleep(1)</c> after its 20th call. That reintroduced the
	///     ORIGINAL sleep under a different name: <c>Schedule</c> landed ~30&#160;ms after the worker was
	///     already blocked in <c>Wait</c> in essentially every trial, so the test only ever proved <c>Set</c>
	///     wakes an already-blocked waiter, exactly the thing the rewrite was meant to stop doing.
	///     <c>Thread.SpinWait</c> below never sleeps and sweeps a genuine nanosecond-scale range across
	///     trials (scaled to <c>iterations</c>, not a fixed modulus — a fixed <c>iter % 2048</c> against
	///     only 200 trials only ever covers the bottom tenth of that range, biasing every trial toward a
	///     near-zero head start and measurably raising the false-failure rate), so some trials really do
	///     land inside the arm window instead of always racing well clear of it.
	///   </para>
	/// </summary>
	[Test]
	[CancelAfter(60_000)]
	public void A_Parked_Worker_Is_Woken_By_A_Schedule() {
		const int iterations = 200;
		for (var iter = 0; iter < iterations; iter++) {
			var scheduler = new WorkStealingScheduler { Capacity = 64, DequeCapacity = 64 };
			scheduler.Start(2, _ => { });

			var acquired = -1;
			using var started = new ManualResetEventSlim(false);
			var worker = new Thread(() => {
				started.Set(); // absorb thread-creation latency only — see the doc comment above
				scheduler.ArmWait(0);
				scheduler.Wait(0);
				if (scheduler.TryAcquire(0, out var value)) Volatile.Write(ref acquired, value);
			}) { IsBackground = true };
			worker.Start();
			Assert.That(started.Wait(TimeSpan.FromSeconds(5)), Is.True,
				$"iteration {iter}: worker thread never started");
			// Nanosecond-scale, never sleeps (unlike SpinWait.SpinOnce past its ~20-call escalation
			// threshold) — scaled to `iterations` so the full 0..2047-cycle sweep is covered regardless of
			// how many trials this run does. See the doc comment above.
			Thread.SpinWait(iter * 2048 / iterations);

			var key = 1000 + iter;
			scheduler.Schedule(key); // deliberately racing the worker's arm cycle — no further handshake

			Assert.That(worker.Join(TimeSpan.FromSeconds(10)), Is.True,
				$"iteration {iter}: the parked worker was never woken — lost wakeup on the schedule path");
			Assert.That(Volatile.Read(ref acquired), Is.EqualTo(key), $"iteration {iter}: wrong/missing key");
		}
	}

	/// <summary>
	///   A fixed sleep before <c>SignalStop</c> is a race against ordinary OS scheduling latency, not a
	///   guarantee: if a thread's startup is delayed past the sleep, <c>WakeAll</c>'s <c>Set</c> lands
	///   BEFORE that thread's own <c>ArmWait</c>, whose <c>Reset</c> then erases it — a failure against a
	///   correct scheduler, on a loaded box, not a real defect. A <see cref="CountdownEvent" /> signalled
	///   by each thread AFTER its own <c>ArmWait</c> returns (the same pattern
	///   <see cref="A_Parked_Worker_Is_Woken_By_A_Schedule" /> uses) removes that race: <c>SignalStop</c>
	///   cannot run until every <c>ArmWait</c> — and therefore every <c>Reset</c> — has already happened.
	/// </summary>
	[Test]
	public void Signal_Stop_Releases_Every_Parked_Worker() {
		var scheduler = new WorkStealingScheduler { Capacity = 64, DequeCapacity = 64 };
		scheduler.Start(3, _ => { });

		var released = 0;
		using var armed = new CountdownEvent(3);
		var threads = new Thread[3];
		for (var i = 0; i < threads.Length; i++) {
			var id = i;
			threads[id] = new Thread(() => {
				scheduler.ArmWait(id);
				armed.Signal();
				scheduler.Wait(id);
				Interlocked.Increment(ref released);
			}) { IsBackground = true };
			threads[id].Start();
		}

		Assert.That(armed.Wait(TimeSpan.FromSeconds(5)), Is.True, "not every worker armed in time");
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
