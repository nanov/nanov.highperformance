namespace Nanov.HighPerformance.Tests.Threading.Workers;

using System.Threading.Tasks.Sources;
using Nanov.HighPerformance.Threading.Workers.Internal;
using NUnit.Framework;

// AsyncAutoResetEvent is internal — visible here via InternalsVisibleTo("Nanov.HighPerformance.Tests").
// Contract: SINGLE waiter (the consumer calls WaitAsync serially, awaiting each result before the
// next) and MULTIPLE setters (the producer's commit edge + the stop/complete paths) may Set
// concurrently. Set latches one signal when no waiter is parked; WaitAsync consumes a latch or parks.
[TestFixture]
public class AsyncAutoResetEventTests {
	[Test]
	public async Task Set_Then_WaitAsync_Completes_From_The_Latch() {
		var ev = new AsyncAutoResetEvent();
		ev.Set();

		var vt = ev.WaitAsync();
		Assert.That(vt.IsCompletedSuccessfully, Is.True, "a prior Set should latch and complete the next wait synchronously");
		Assert.That(await vt, Is.True);
	}

	[Test]
	public async Task WaitAsync_Then_Set_Completes_The_Pending_Wait() {
		var ev = new AsyncAutoResetEvent();

		var vt = ev.WaitAsync();
		Assert.That(vt.IsCompleted, Is.False, "no signal yet → the wait is pending");

		ev.Set();
		Assert.That(await vt, Is.True);
	}

	[Test]
	public async Task Auto_Reset_One_Wait_Per_Set() {
		var ev = new AsyncAutoResetEvent();
		ev.Set();

		var vt1 = ev.WaitAsync();
		Assert.That(await vt1, Is.True, "first wait consumes the latch");

		var vt2 = ev.WaitAsync();
		Assert.That(vt2.IsCompleted, Is.False, "auto-reset: the single latch was consumed, second wait parks");

		ev.Set();
		Assert.That(await vt2, Is.True);
	}

	[Test]
	public async Task Multiple_Sets_With_No_Waiter_Latch_At_Most_Once() {
		var ev = new AsyncAutoResetEvent();
		ev.Set();
		ev.Set();
		ev.Set();

		var vt1 = ev.WaitAsync();
		Assert.That(await vt1, Is.True, "the latch completes one wait");

		var vt2 = ev.WaitAsync();
		Assert.That(vt2.IsCompleted, Is.False, "extra Sets collapse to a single latch (auto-reset)");

		ev.Set();
		Assert.That(await vt2, Is.True);
	}

	[Test]
	public async Task Reusable_Across_Many_Sequential_Cycles() {
		var ev = new AsyncAutoResetEvent();
		for (var i = 0; i < 10_000; i++) {
			var vt = ev.WaitAsync();
			ev.Set();
			Assert.That(await vt, Is.True);
		}
	}

	// Drop-on-claim regression. A waiter is registered, then TWO Sets fire: the first CLAIMS the waiter
	// (schedules its resume), the second must NOT be dropped — it has to survive as a latch so the next
	// WaitAsync completes from it. The old "leave s_signaled on claim, reset in GetResult" design dropped
	// the second Set (it saw s_signaled → idempotent return, then GetResult erased the latch); the
	// claim-reset-to-null design latches it. Drives the IValueTaskSource directly like the await machinery.
	[Test]
	public async Task Second_Set_While_Claiming_A_Waiter_Survives_As_A_Latch() {
		var ev = new AsyncAutoResetEvent();
		var src = (IValueTaskSource<bool>)ev;
		var resumed = new int[1];
		Action<object?> cont = static s => Volatile.Write(ref ((int[])s!)[0], 1);

		_ = ev.WaitAsync(); // arm (no latch present)
		src.OnCompleted(cont, resumed, 0, ValueTaskSourceOnCompletedFlags.None); // register the waiter

		ev.Set(); // claims the waiter, schedules the resume, resets state to null
		ev.Set(); // must latch a fresh signal (not collapse into the one being delivered)

		var sw = new SpinWait();
		while (Volatile.Read(ref resumed[0]) == 0) // wait for the scheduled continuation to run
			sw.SpinOnce();
		Assert.That(src.GetResult(0), Is.True);

		var vt = ev.WaitAsync();
		Assert.That(vt.IsCompletedSuccessfully, Is.True,
			"the second Set must have latched — the next wait completes from it, not park (a dropped Set parks here)");
		Assert.That(await vt, Is.True);
	}

	// The lost-wakeup guard. One serial consumer; several setters hammer Set concurrently. Models the
	// worker: `items` is the producer counter (≈ Tail), the consumer drains it (≈ Head) and parks on
	// the event when caught up. A lost wakeup leaves the consumer parked with items > consumed → the
	// test hits its timeout instead of completing. Run big so the empty→non-empty race window is hit
	// many times.
	[Test]
	public async Task Stress_SingleConsumer_MultipleSetters_NoLostWakeup() {
		var ev = new AsyncAutoResetEvent();
		const int n = 500_000;
		long items = 0;   // produced (setters Interlocked++); read by the consumer
		long claimed = 0; // work-claim counter so the setters together issue exactly n Sets

		Task Setter() => Task.Run(() => {
			while (Interlocked.Increment(ref claimed) <= n) {
				Interlocked.Increment(ref items);
				ev.Set();
			}
		});

		var consumer = Task.Run(async () => {
			long c = 0;
			while (c < n) {
				if (Volatile.Read(ref items) > c)
					c++;
				else
					await ev.WaitAsync().ConfigureAwait(false);
			}

			return c;
		});

		var setters = new[] { Setter(), Setter(), Setter() };
		await Task.WhenAll(setters);

		// 60s is a *permanent-hang* budget, not a perf bound — this completes in <1s; the margin only
		// guards against transient ThreadPool starvation under a loaded full-suite run flagging a false
		// lost-wakeup. A real lost wakeup parks the consumer forever and still trips it.
		var finished = await Task.WhenAny(consumer, Task.Delay(TimeSpan.FromSeconds(60)));
		Assert.That(finished, Is.SameAs(consumer), "consumer never drained — lost wakeup (it is parked with items > consumed)");
		Assert.That(await consumer, Is.EqualTo(n));
	}

	// THE decisive guard — models the worker's signaling exactly: a single producer that Sets ONLY on
	// the empty→non-empty edge (like CommitTail), so there are no redundant Sets to mask a lost wakeup.
	// One lost edge = permanent hang (the producer won't Set again until the consumer drains, but it's
	// parked). Periodic yields force the consumer to catch up and park, hitting the arm-vs-Set window
	// over and over. A lock-free event with a lost-wakeup bug hangs here; the lock-based one passes.
	//
	// The harness mirrors the FIXED SpRingCore signaling, including the two StoreLoad fences that close
	// the Dekker lost-wakeup on weak memory (ARM64): the producer fences between publishing `tail` and
	// reading `head` (else its `Read(head)` can be reordered ahead of the `Write(tail)`, read a stale head,
	// and skip the edge `Set`), and the consumer arms then RECHECKS `tail` (the arm's CAS is a full fence),
	// self-signaling if an item appeared so it never parks on an edge the producer skipped. Without these,
	// the harness itself loses wakeups under contention — exactly the bug the SpRingCore fix addresses.
	[Test]
	public async Task Stress_EdgeGated_SingleProducer_NoLostWakeup() {
		var ev = new AsyncAutoResetEvent();
		const int n = 1_000_000;
		long tail = 0; // producer index (written by producer, read by consumer)
		long head = 0; // consumer index (written by consumer, read by producer for the edge test)

		var producer = Task.Run(() => {
			for (long i = 0; i < n; i++) {
				var t = tail;
				Volatile.Write(ref tail, t + 1); // publish
				Interlocked.MemoryBarrier(); // StoreLoad fence: publish tail before reading head (mirrors CommitTail)
				if (Volatile.Read(ref head) == t) // ring was empty → edge → signal (mirrors CommitTail)
					ev.Set();
				if ((i & 255) == 0) Thread.Yield(); // let the consumer drain & park
			}
		});

		var consumer = Task.Run(async () => {
			long h = 0;
			while (h < n) {
				if (h != Volatile.Read(ref tail)) {
					Volatile.Write(ref head, h + 1);
					h++;
				} else {
					var vt = ev.WaitAsync(); // arm — the event's CAS is a full StoreLoad fence
					if (h != Volatile.Read(ref tail)) // recheck after arming: an item committed in the gap
						ev.Set(); // self-signal so the armed wait completes instead of parking on a skipped edge
					if (!await vt.ConfigureAwait(false))
						break;
				}
			}

			return h;
		});

		await producer;
		// Permanent-hang budget (see the other stress test): completes in <1s, 60s only trips on a real
		// (forever) lost wakeup, not transient load.
		var finished = await Task.WhenAny(consumer, Task.Delay(TimeSpan.FromSeconds(60)));
		Assert.That(finished, Is.SameAs(consumer), "lost wakeup: consumer parked with an item published");
		Assert.That(await consumer, Is.EqualTo(n));
	}

	// Proves the suspend→wake path is zero-alloc (and guards the IThreadPoolWorkItem optimization).
	// Drives the IValueTaskSource directly the way the await machinery does — arm, register a reusable
	// continuation, Set (which schedules the resume on the ThreadPool), spin for it, GetResult — and
	// measures bytes/cycle across all threads. A per-wake delegate/work-item wrapper would show ~24-72
	// B/cycle; queueing the reused event itself shows ~0.
	[Test]
	public void SuspendWake_Path_Allocates_Nothing_Per_Cycle() {
		var ev = new AsyncAutoResetEvent();
		var src = (IValueTaskSource<bool>)ev;
		var flag = new int[1];
		Action<object?> cont = static s => Volatile.Write(ref ((int[])s!)[0], 1);

		void Cycle() {
			_ = ev.WaitAsync(); // arm a pending wait (no latch)
			Volatile.Write(ref flag[0], 0);
			src.OnCompleted(cont, flag, 0, ValueTaskSourceOnCompletedFlags.None);
			ev.Set(); // schedule the resume on the ThreadPool
			var sw = new SpinWait();
			while (Volatile.Read(ref flag[0]) == 0)
				sw.SpinOnce();
			src.GetResult(0); // consume + reset for reuse
		}

		for (var i = 0; i < 2000; i++) // warmup: JIT + ThreadPool spin-up
			Cycle();

		GC.Collect();
		var before = GC.GetTotalAllocatedBytes(precise: true);
		const int n = 20_000;
		for (var i = 0; i < n; i++)
			Cycle();
		var perCycle = (GC.GetTotalAllocatedBytes(precise: true) - before) / (double)n;

		Assert.That(perCycle, Is.LessThan(16.0), $"suspend→wake should be ~0 B/cycle but was {perCycle:F1} B");
	}
}
