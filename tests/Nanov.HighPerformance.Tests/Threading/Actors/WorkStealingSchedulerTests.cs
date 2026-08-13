namespace Nanov.HighPerformance.Tests.Threading.Actors;

using System;
using System.Collections.Generic;
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

		var taken = new List<int>();
		while (scheduler.TryAcquire(0, out var value))
			taken.Add(value);

		Assert.That(taken.OrderBy(x => x), Is.EqualTo(Enumerable.Range(0, 16)),
			"spilled keys must still be acquirable — order is irrelevant, loss is not");
	}

	/// <summary>
	///   The wake edge — a parked worker must be released by a <c>Schedule</c> call from another thread.
	///   A wake-path SMOKE TEST (see the head-start paragraph below for exactly what that does and does
	///   not mean it catches) built around <c>Schedule</c>'s enqueue-then-flag-read (<c>WakeOne</c>'s
	///   fence) and <c>ArmWait</c>'s Reset-before-flag ordering.
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
	///     <b>The head start after <c>started</c> no longer sleeps — but it has NO MEASURED POWER against
	///     the <c>ArmWait</c> reversal, and the doc comment must not claim otherwise.</b> An earlier
	///     version used 40 <c>SpinWait.SpinOnce()</c> calls, believing that was a "handful of spin
	///     cycles" — it measures ~30&#160;ms on this machine, because <c>SpinOnce</c> escalates to
	///     <c>Thread.Sleep(1)</c> after its 20th call, silently reintroducing the ORIGINAL sleep under a
	///     different name. The version below, <c>Thread.SpinWait</c> scaled to <c>iterations</c>, never
	///     sleeps and genuinely varies arrival — but only across a MICROSECOND-scale range (up to roughly
	///     100&#160;µs at the top of the sweep on x86-64), not nanoseconds. And that sweep is added ON
	///     TOP of the real arrival floor, which is <c>started.Wait()</c>'s own return latency: the main
	///     thread blocks on a KERNEL event before the worker thread even exists, and typically resumes
	///     tens of microseconds after <c>started.Set()</c> fires — by which point the worker, which does
	///     not wait on anything after <c>Set()</c>, has already executed <c>ArmWait(0); Wait(0)</c>, a
	///     handful of nanoseconds of work. Essentially every trial arrives after the worker is already
	///     parked. A dedicated 10,000-trial measurement confirms this directly: 3/80,000 failures with
	///     <c>ArmWait</c>'s two statements reversed vs. 2/80,000 on the correct baseline — statistically
	///     indistinguishable, and consistent with NO trial ever landing inside the arm window at all.
	///   </para>
	///   <para>
	///     <b>So treat this as a wake-path smoke test, not an ordering-race detector.</b> What it DOES
	///     catch reliably (~200/200 trials, by construction — not measured, since these are gross
	///     breakages rather than narrow races): <c>WakeOne</c>'s fence removed, on x86-64 (NOT on arm64,
	///     where that fence is architecturally redundant — see <c>WakeOne</c>'s own doc); <c>WakeOne</c>
	///     never called at all; <c>Set</c> landing on the wrong worker's event; a scan bug that skips an
	///     index; <c>ArmWait</c> failing to reset the event before parking. What it does NOT catch:
	///     <c>ArmWait</c>'s Reset-before-flag statement order — that window is narrower than a
	///     wall-clock/thread-scheduling race can resolve from outside. The design that WOULD catch it
	///     deterministically, named here rather than implemented: a test-only <c>internal Action?
	///     ArmWaitProbe</c> hook on <c>State</c>, invoked inside <c>ArmWait</c> between <c>Reset()</c> and
	///     the <c>Parked</c> write. With the statements correctly ordered the probe is a no-op window; with
	///     them reversed, the probe instead sits between the <c>Parked</c> write and <c>Reset()</c>, and a
	///     probe body that spins ~1&#160;µs there gives a concurrent <c>Schedule</c> time to observe
	///     <c>Parked == true</c>, call <c>Set</c>, and have that <c>Set</c> erased by the <c>Reset</c> that
	///     follows — a deterministic hang (detection ≈1) instead of this design's near-zero power (a
	///     10,000-trial measurement excludes a mutation-induced rate above ~1.2×10⁻⁴/trial and says
	///     nothing below that). Cost: one null-check per park, off the per-message path — but it is a real
	///     production-code change made solely to make a unit test's power tractable, and nobody has asked
	///     for that trade yet.
	///   </para>
	/// </summary>
	[Test]
	[CancelAfter(60_000)]
	public void A_Parked_Worker_Is_Woken_By_A_Schedule() {
		const int iterations = 200;
		for (var iter = 0; iter < iterations; iter++) {
			if (RunTrial(iter)) continue;
			// Measured baseline false-positive ~2.5e-5/trial: a Schedule whose WakeOne beats the worker's
			// ArmWait sees Parked=false, skips the Set, and this test deliberately omits the armed recheck
			// that would rescue it. A real lost wakeup is deterministic, so it fails the retry too.
			Assert.That(RunTrial(iter), Is.True, $"iteration {iter}: lost wakeup on the schedule path");
		}

		bool RunTrial(int iter) {
			var scheduler = new WorkStealingScheduler { Capacity = 64, DequeCapacity = 64 };
			scheduler.Start(2, _ => { });

			var acquired = -1;
			var started = new ManualResetEventSlim(false);
			var worker = new Thread(() => {
				started.Set(); // absorb thread-creation latency only — see the doc comment above
				scheduler.ArmWait(0);
				scheduler.Wait(0);
				if (scheduler.TryAcquire(0, out var value)) Volatile.Write(ref acquired, value);
			}) { IsBackground = true };
			worker.Start();
			Assert.That(started.Wait(TimeSpan.FromSeconds(5)), Is.True,
				$"iteration {iter}: worker thread never started");
			// Microsecond-scale, never sleeps — see the doc comment above for exactly what this does and
			// does not give this test power against.
			Thread.SpinWait(iter * 2048 / iterations);

			var key = 1000 + iter;
			scheduler.Schedule(key); // deliberately racing the worker's arm cycle — no further handshake

			// A real wake lands in microseconds; 2s is generous headroom, not a tolerance for slowness.
			if (!worker.Join(TimeSpan.FromSeconds(2))) return false;
			// Dispose only on the success path. Not because the worker could still reach
			// `started.Set()` after this returns — `Set()` is the lambda's FIRST statement, and the
			// assert above already confirmed `started.Wait()` returned, so it has provably already run
			// by this point. The guard is simply harmless to keep: CountdownEvent-style primitives throw
			// ObjectDisposedException unconditionally from a Set/Signal on a disposed instance, which
			// would crash the test host from a background thread instead of leaving a readable
			// assertion failure, so skipping dispose on the timeout path costs nothing.
			started.Dispose();
			return Volatile.Read(ref acquired) == key;
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
		var armed = new CountdownEvent(3);
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
		// Dispose only once every thread has already called Signal() — guaranteed by the true above,
		// since a CountdownEvent only reaches zero after all 3 have signalled. Disposing any earlier
		// (e.g. via `using`, torn down on an assertion failure above) risks a thread reaching Signal()
		// afterwards; CountdownEvent.Signal throws ObjectDisposedException unconditionally, which would
		// crash the test host from a background thread instead of leaving a readable assertion failure.
		armed.Dispose();
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

	/// <summary>
	///   Spec §9 test 8 — steal correctness under churn: many workers, few keys, so most workers are
	///   starved of local work and must steal to make progress.
	///   <para>
	///     <b>The counters are the point.</b> Order and loss assertions would pass against a scheduler
	///     whose steal path never ran even once; asserting <c>StealsSucceeded &gt; 0</c> is what makes
	///     this a test of STEALING rather than another test of the cell machine. It is a floor, not a
	///     target: the exact rate is a benchmark question, not a correctness one.
	///   </para>
	/// </summary>
	[Test]
	[CancelAfter(120_000)]
	public void Steals_Happen_Under_Churn_And_Preserve_Per_Key_Order() {
		const int keys = 4;
		const int perKey = 20_000;
		const int total = keys * perKey;
		var state = new RecordingState();
		var scheduler = new WorkStealingScheduler { Capacity = 256, DequeCapacity = 64 };
		using var system = new ActorSystem<int, int, RecordingHandler, WorkStealingScheduler>(
			new RecordingHandler(state), keyCapacity: 64, chunkCount: 16, chunkSize: 64,
			batchBound: 1, workers: 8, scheduler: scheduler);
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
		Assert.Multiple(() => {
			Assert.That(byKey, Has.Count.EqualTo(keys));
			Assert.That(scheduler.StealsAttempted, Is.GreaterThan(0), "the steal scan never ran");
			Assert.That(scheduler.StealsSucceeded, Is.GreaterThan(0),
				"no steal ever succeeded — D is behaving as a global-queue scheduler, not a stealing one");
		});

		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()),
				$"key {key} out of order — a stolen key was processed concurrently with its owner");

		TestContext.Out.WriteLine(
			$"steals {scheduler.StealsSucceeded}/{scheduler.StealsAttempted}, " +
			$"global {scheduler.GlobalDequeues}, overflows {scheduler.DequeOverflows}, parks {scheduler.Parks}, " +
			$"enqueueRetries {scheduler.GlobalEnqueueRetries}");
	}
}
