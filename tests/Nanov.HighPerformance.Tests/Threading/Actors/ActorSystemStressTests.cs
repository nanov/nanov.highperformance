namespace Nanov.HighPerformance.Tests.Threading.Actors;

using Nanov.HighPerformance.Tests.Threading.Workers;
using Nanov.HighPerformance.Threading.Actors;
using NUnit.Framework;

/// <summary>
///   Race guards. Each targets a named interleaving from the design spec; a regression here is a real
///   concurrency bug, not a flake.
///   <para>
///     <b><see cref="CancelAfterAttribute" /> is NOT the safety net here — read this before trusting it.</b>
///     This repo's NUnit (4.3.2) marks the older <c>TimeoutAttribute</c> <c>[Obsolete]</c> (and
///     <c>TreatWarningsAsErrors</c> turns that into a build failure), so <c>CancelAfter</c> is the only
///     attribute available. But <c>CancelAfterCommand.Execute</c> runs the test body <b>synchronously, on
///     the calling thread</b>, and merely arms a <see cref="System.Threading.CancellationTokenSource" /> —
///     NUnit's own docs say the test has to observe that token itself. Nothing in
///     <see cref="ActorSystem{TKey,T,THandler}" /> does: <c>Dispatch</c>, <c>WaitForMailboxSpace</c>,
///     <c>WaitForChunk</c> and <c>TryComplete</c> all block on a plain
///     <see cref="System.Threading.ManualResetEventSlim" /><c>.Wait(TimeSpan)</c> poll with no token in
///     sight. So a wedged <c>Dispatch</c> on the TEST thread would sail straight through its
///     <c>CancelAfter</c> budget and hang the run forever — confirmed empirically (NUnit 4.3.2,
///     <c>[CancelAfter(2000)]</c> around a tight polling loop, process still alive 27+ seconds later).
///   </para>
///   <para>
///     The actual protection: every test that dispatches enough volume to risk blocking runs its
///     <c>Dispatch</c> calls on a dedicated producer <see cref="Thread" /> and joins it with an explicit
///     bounded <see cref="Thread.Join(TimeSpan)" /> on the TEST thread — a genuine bounded wait, unlike
///     <c>CancelAfter</c>. Nothing runs on that background thread unguarded: a progress counter
///     (<c>Volatile.Write</c>/<c>Volatile.Read</c>) carries how far it got, and a <c>try</c>/<c>catch</c>
///     captures any exception into a local for the test thread to re-throw AFTER the join — never an
///     in-thread <c>Assert</c>, and never an uncaught exception left to hit the process-wide
///     unhandled-exception path. Both matter: an assertion failing off-thread can crash the process
///     instead of producing a red test, and so can an exception that nothing on that thread catches —
///     which one of these tests (<c>Release_Race_Never_Strands_Or_Double_Schedules</c>) can legitimately
///     throw on purpose, since <c>ScheduleFromProducer</c>'s ring-overflow exception IS the
///     double-schedule symptom that test exists to catch. Post-join order is always: bounded join, then
///     re-surface any captured exception, then assert progress — checking progress first would report a
///     misleading "rejected mid-run" when the real cause was an exception. After that, <c>TryComplete</c>
///     + <see cref="WorkerTestUtil.AwaitCompletionOrStall" /> — the stall watchdog — covers the drain
///     phase the same way it always did. <c>CancelAfter</c> is left in place anyway: it is harmless, and
///     it does bound whichever parts of a test run on the test thread itself.
///   </para>
/// </summary>
[TestFixture]
public class ActorSystemStressTests {
	private static ActorSystem<int, int, RecordingHandler> New(RecordingState state,
		int keyCapacity = 128, int chunkCount = 32, int chunkSize = 8, int batchBound = 0) =>
		new(new RecordingHandler(state), keyCapacity, chunkCount, chunkSize, batchBound);

	/// <summary>
	///   §5.3 — the release race. One key, the producer appending continuously while the worker keeps
	///   releasing. A stranded message shows up as a stall; a double-schedule as a duplicate or a
	///   scheduler-ring overflow.
	/// </summary>
	[Test]
	[CancelAfter(120_000)]
	public void Release_Race_Never_Strands_Or_Double_Schedules() {
		const int count = 300_000;
		var state = new RecordingState();
		using var system = New(state, chunkSize: 8, batchBound: 1); // batchBound 1 ⇒ maximum release churn
		system.Start();

		// Dispatch runs on its own thread so the test thread can bound the wait with a real Join — see the
		// class doc comment for why CancelAfter alone cannot catch a wedged Dispatch. No Assert inside the
		// producer thread: a failure there throws off-thread and can crash the process instead of failing
		// the test, so a plain progress counter carries the result back instead. An exception IS still
		// possible here on purpose — ScheduleFromProducer throws on scheduler-ring overflow, which is this
		// very test's own double-schedule symptom — so it is caught and re-thrown on the test thread below
		// rather than left to hit the process-wide unhandled-exception path.
		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				for (var i = 0; i < count; i++) {
					if (!system.Dispatch(1, i)) return;
					Volatile.Write(ref dispatched, i + 1);
				}
			}
			catch (Exception ex) {
				producerError = ex; // surfaced on the test thread below; Join gives the happens-before edge
			}
		}) { IsBackground = true };
		producer.Start();

		Assert.That(producer.Join(TimeSpan.FromSeconds(90)), Is.True,
			$"producer wedged after {Volatile.Read(ref dispatched)}/{count} dispatches — likely a stranded key");
		if (producerError is not null)
			Assert.Fail($"producer threw: {producerError}");
		Assert.That(Volatile.Read(ref dispatched), Is.EqualTo(count), "a Dispatch was rejected mid-run");

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, count);

		Assert.That(state.HandledCount, Is.EqualTo(count));
		var values = state.ByKey()[1];
		Assert.That(values, Has.Count.EqualTo(count));
		for (var i = 0; i < count; i++)
			if (values[i] != i)
				Assert.Fail($"order broke at {i}: got {values[i]}");
	}

	/// <summary>
	///   §5.6 — quiescence must never be observed while a message is still queued. The final dispatch is
	///   raced against TryComplete repeatedly; a false-quiescence bug drops that message.
	/// </summary>
	[Test]
	[CancelAfter(60_000)]
	public void TryComplete_Racing_A_Final_Message_Never_Loses_It() {
		for (var round = 0; round < 200; round++) {
			var state = new RecordingState();
			using var system = New(state, chunkSize: 8);
			system.Start();

			const int count = 64;
			// Bounded producer thread, not the test thread — see the class doc comment.
			var dispatched = 0;
			Exception? producerError = null;
			var producer = new Thread(() => {
				try {
					for (var i = 0; i < count; i++) {
						if (!system.Dispatch(i % 4, i)) return;
						Volatile.Write(ref dispatched, i + 1);
					}
				}
				catch (Exception ex) {
					producerError = ex;
				}
			}) { IsBackground = true };
			producer.Start();

			Assert.That(producer.Join(TimeSpan.FromSeconds(5)), Is.True,
				$"round {round}: producer wedged after {Volatile.Read(ref dispatched)}/{count} dispatches");
			if (producerError is not null)
				Assert.Fail($"round {round}: producer threw: {producerError}");
			Assert.That(Volatile.Read(ref dispatched), Is.EqualTo(count), $"round {round}: a Dispatch was rejected");

			system.TryComplete();
			WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, count);
			Assert.That(state.HandledCount, Is.EqualTo(count), $"round {round} lost a message");
		}
	}

	/// <summary>§4.4 — the producer's harvest running against continuous worker release churn.</summary>
	[Test]
	[CancelAfter(120_000)]
	public void Harvest_Racing_Release_Never_Strands() {
		const int keys = 64;
		const int perKey = 2_000;
		const int total = keys * perKey;
		var state = new RecordingState();
		using var system = New(state, keyCapacity: 256, chunkCount: 8, chunkSize: 4, batchBound: 2);
		system.Start();

		// Bounded producer thread, not the test thread — see the class doc comment.
		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				var n = 0;
				for (var i = 0; i < perKey; i++) {
					for (var k = 0; k < keys; k++) {
						if (!system.Dispatch(k, i)) return;
						Volatile.Write(ref dispatched, ++n);
					}
				}
			}
			catch (Exception ex) {
				producerError = ex;
			}
		}) { IsBackground = true };
		producer.Start();

		Assert.That(producer.Join(TimeSpan.FromSeconds(90)), Is.True,
			$"producer wedged after {Volatile.Read(ref dispatched)}/{total} dispatches — likely a stranded key");
		if (producerError is not null)
			Assert.Fail($"producer threw: {producerError}");
		Assert.That(Volatile.Read(ref dispatched), Is.EqualTo(total), "a Dispatch was rejected mid-run");

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, total);

		Assert.That(state.HandledCount, Is.EqualTo(total));
		foreach (var (key, values) in state.ByKey())
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");
	}

	/// <summary>Sustained mixed load — order and exactly-once across many keys under churn.</summary>
	[Test]
	[CancelAfter(120_000)]
	public void Sustained_Mixed_Load_Preserves_Order_And_Loses_Nothing() {
		const int keys = 32;
		const int perKey = 5_000;
		const int total = keys * perKey;
		var state = new RecordingState();
		using var system = New(state, keyCapacity: 256, chunkCount: 16, chunkSize: 16, batchBound: 3);
		system.Start();

		// Bounded producer thread, not the test thread — see the class doc comment.
		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				var n = 0;
				for (var i = 0; i < perKey; i++) {
					for (var k = 0; k < keys; k++) {
						if (!system.Dispatch(k, i)) return;
						Volatile.Write(ref dispatched, ++n);
					}
				}
			}
			catch (Exception ex) {
				producerError = ex;
			}
		}) { IsBackground = true };
		producer.Start();

		Assert.That(producer.Join(TimeSpan.FromSeconds(90)), Is.True,
			$"producer wedged after {Volatile.Read(ref dispatched)}/{total} dispatches — likely a stranded key");
		if (producerError is not null)
			Assert.Fail($"producer threw: {producerError}");
		Assert.That(Volatile.Read(ref dispatched), Is.EqualTo(total), "a Dispatch was rejected mid-run");

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, total);

		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key}");
	}

	/// <summary>
	///   Steady state must not allocate. The ring, slab and cells are allocated once at construction;
	///   dispatch and drain must add nothing per message.
	/// </summary>
	[Test]
	[CancelAfter(60_000)]
	public void Steady_State_Dispatch_Does_Not_Allocate() {
		var state = new RecordingState { Throw = null };
		using var system = New(state, keyCapacity: 64, chunkCount: 16, chunkSize: 64);
		system.Start();

		// Warm up: bind chunks, JIT the paths.
		for (var i = 0; i < 2_000; i++)
			system.Dispatch(i % 8, i);
		WorkerTestUtil.AwaitProgress(() => state.HandledCount, 2_000);

		// RecordingHandler allocates (ConcurrentQueue nodes) and the worker is very much still running —
		// concurrently draining and calling Handle() while the loop below executes, not idle. That's fine:
		// GC.GetAllocatedBytesForCurrentThread() is inherently per-thread, so the worker's allocations were
		// never going to be counted on the test thread regardless. This isolates Dispatch's own allocation
		// by construction, not because anything else is quiesced.
		var before = GC.GetAllocatedBytesForCurrentThread();
		for (var i = 0; i < 10_000; i++)
			system.TryDispatch(1, i);
		var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

		Assert.That(allocated, Is.EqualTo(0), $"Dispatch allocated {allocated} bytes over 10k calls");
	}

	/// <summary>
	///   §4.4 — a closer (but still sampled, not forced) guard for the reclaim-revert stranding bug that
	///   <see cref="ActorSystemTests.Harvest_Under_Churn_Loses_No_Messages" /> was originally written to
	///   catch and, as that test's comment explains, mostly doesn't: a strand there self-heals because
	///   every round redispatches to all 6 keys, so only a strand on a key's absolute final message would
	///   ever surface.
	///   <para>
	///     This test manufactures exactly that shape instead of hoping for it: each round drains a burst
	///     on key 0, then sends key 0 <b>one more, final message that nothing ever follows</b> — no later
	///     dispatch to key 0 exists to reschedule it if it gets stranded. <c>chunkCount: 1</c> means the
	///     very next thing the producer does (dispatch to key 1, which needs the only chunk) can only
	///     proceed by running <c>Harvest()</c> against key 0's cell, so the harvest scan lands somewhere
	///     near the worker's release of that final message.
	///   </para>
	///   <para>
	///     Still honest disclosure: there is no hook to pin the producer's harvest CAS against the
	///     worker's release-recheck at the exact instant §5.3/§4.4 describe, so this samples scheduler
	///     jitter across many rounds rather than forcing the interleaving. A stranded message here does
	///     not report a wrong count — it reports a HANG. Unlike the four tests above, the risky call here
	///     (<c>Dispatch(1, 0)</c>, below) is exactly the one this test exists to force into the race
	///     window, so it gets the same bounded-thread treatment described in the class doc comment rather
	///     than running on the test thread — a real strand here would otherwise wedge the test thread
	///     forever inside <c>WaitForChunk</c>, which <c>CancelAfter</c> cannot interrupt.
	///   </para>
	/// </summary>
	[Test]
	[CancelAfter(120_000)]
	public void Harvest_Race_On_A_Keys_Final_Message_Is_Never_Stranded() {
		for (var round = 0; round < 500; round++) {
			var state = new RecordingState();
			using var system = New(state, keyCapacity: 4, chunkCount: 1, chunkSize: 4, batchBound: 1);
			system.Start();

			const int burst = 3;
			// Provably non-blocking: nothing else holds this system's one chunk yet, and burst (3) never
			// reaches chunkSize (4), so these three calls never touch backpressure or Harvest — safe on
			// the test thread.
			for (var i = 0; i < burst; i++)
				Assert.That(system.Dispatch(0, i), Is.True, $"round {round}: burst message {i}");
			WorkerTestUtil.AwaitProgress(() => state.HandledCount, burst, TimeSpan.FromSeconds(5));

			// Key 0's absolute final message this round — nothing dispatches to key 0 again after this.
			// Still provably non-blocking for the same reason as the burst above (Tail only reaches 4, the
			// chunk limit, never exceeds it).
			Assert.That(system.Dispatch(0, burst), Is.True, $"round {round}: final message");

			// Only one chunk exists in this system, and it's held by key 0: this can only succeed once key
			// 0's chunk is harvestable, which forces Harvest() to scan key 0's cell right around the final
			// message's release — THIS is the one call in the test that can genuinely block forever if the
			// reclaim-revert path strands key 0, so it runs on a bounded thread rather than the test thread.
			var forcedHarvest = 0;
			Exception? racerError = null;
			var racer = new Thread(() => {
				try {
					if (system.Dispatch(1, 0)) Volatile.Write(ref forcedHarvest, 1);
				}
				catch (Exception ex) {
					racerError = ex;
				}
			}) { IsBackground = true };
			racer.Start();
			Assert.That(racer.Join(TimeSpan.FromSeconds(5)), Is.True,
				$"round {round}: Dispatch(1, 0) wedged — key 0's final message was likely stranded");
			if (racerError is not null)
				Assert.Fail($"round {round}: Dispatch(1, 0) threw: {racerError}");
			Assert.That(Volatile.Read(ref forcedHarvest), Is.EqualTo(1), $"round {round}: Dispatch(1, 0) was rejected");

			system.TryComplete();
			WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, burst + 2,
				TimeSpan.FromSeconds(5));
			Assert.That(state.HandledCount, Is.EqualTo(burst + 2), $"round {round}: a message was stranded");
		}
	}

	/// <summary>
	///   The "no double-schedule" half of the ownership protocol. At one worker a double-schedule can
	///   never be observed as concurrent ownership — there is nothing else to own it concurrently WITH —
	///   so it can only ever show up as a harmless-looking symptom: the doubly-scheduled key index gets
	///   dequeued a second time, finds <c>Tail == Head</c> already (nothing left to drain), and releases
	///   immediately. That inflates <see cref="ActorSystem{TKey,T,THandler}.Activations" /> without
	///   changing the handled count.
	///   <para>
	///     Every message is dispatched here <b>before</b> <c>Start()</c> is called, so nothing races the
	///     worker while the messages queue up — each key's whole burst is sitting in its mailbox before
	///     the first activation ever runs. That makes the per-key activation count deterministic: exactly
	///     <c>ceil(perKey / batchBound)</c>, not a race-dependent approximation, so this is an exact
	///     equality rather than the "not wildly above" fudge the brief allows for — the deterministic
	///     setup earns the tighter check.
	///   </para>
	///   <para>
	///     A genuine <b>double-schedule guard</b> — one that could catch concurrent ownership rather than
	///     this spurious-activation symptom — needs more than one worker, and is owed to phase 2 (design
	///     spec §9, test 8; see the "deliberately not covered" table at the end of the phase-1 plan).
	///   </para>
	/// </summary>
	[Test]
	[CancelAfter(60_000)]
	public void No_Double_Schedule_Symptom_Activations_Match_The_Deterministic_Minimum() {
		const int keys = 8;
		const int perKey = 500;
		const int batchBound = 8;
		var state = new RecordingState();
		// chunkSize (512) comfortably holds one key's whole burst (500), so nothing here ever blocks on
		// backpressure or needs a harvest — the only thing this test wants to observe is Activations.
		using var system = New(state, keyCapacity: 64, chunkCount: 16, chunkSize: 512, batchBound: batchBound);

		for (var k = 0; k < keys; k++)
			for (var i = 0; i < perKey; i++)
				Assert.That(system.Dispatch(k, i), Is.True);

		system.Start();
		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, keys * perKey);

		Assert.That(state.HandledCount, Is.EqualTo(keys * perKey));

		var expected = keys * (int)Math.Ceiling(perKey / (double)batchBound);
		Assert.That(system.Activations, Is.EqualTo(expected),
			$"Activations ({system.Activations}) != the deterministic minimum ({expected}) — too high points at " +
			"a double-schedule (a spurious extra activation); too low is contradicted by HandledCount above");
	}

	/// <summary>
	///   §4.4/§5.5 — cell eviction (<c>KeyCellMap.EvictFree</c>) under a live worker.
	///   <b>Before this test, <c>KeyCellMap.EvictFree</c> never executed inside a running
	///   <see cref="ActorSystem{TKey,T,THandler}" /> anywhere in this suite</b> — every other actor test
	///   over-sizes the map relative to its key count, so the whole eviction sweep was exercised only by
	///   <c>KeyCellMapTests</c>' direct, single-threaded calls, never against a live producer/worker pair.
	///   <para>
	///     <c>keyCapacity: 8</c> rounds up to 16 map slots (§4.2's ~2× load factor). Several hundred
	///     distinct keys, each dispatched a handful of messages and never revisited, guarantee the map
	///     fills with no <c>Empty</c> slot left, so once no tombstone is lying around either,
	///     <c>GetOrAddRefCold</c> is forced into <c>EvictFree</c> for every further new key. <c>chunkCount</c>
	///     well below both the key count and the slot count forces the producer's cold <c>Harvest</c> path
	///     too — a cell is only evictable once its chunk has been harvested (§4.4), so this test cannot
	///     reach <c>EvictFree</c> without first exercising <c>Harvest</c>.
	///   </para>
	/// </summary>
	[Test]
	[CancelAfter(120_000)]
	public void Eviction_Under_Live_Worker_Preserves_Order_And_Loses_Nothing() {
		const int keys = 400;
		const int perKey = 3;
		const int total = keys * perKey;
		var state = new RecordingState();
		using var system = New(state, keyCapacity: 8, chunkCount: 4, chunkSize: 4, batchBound: 2);
		system.Start();

		// Bounded producer thread, not the test thread — see the class doc comment. Each key gets its
		// small burst and is never revisited, so cells and chunks for earlier keys go idle and become
		// harvestable/evictable while the producer works its way through the remaining keys.
		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				var n = 0;
				for (var k = 0; k < keys; k++)
					for (var i = 0; i < perKey; i++) {
						if (!system.Dispatch(k, i)) return;
						Volatile.Write(ref dispatched, ++n);
					}
			}
			catch (Exception ex) {
				producerError = ex;
			}
		}) { IsBackground = true };
		producer.Start();

		Assert.That(producer.Join(TimeSpan.FromSeconds(90)), Is.True,
			$"producer wedged after {Volatile.Read(ref dispatched)}/{total} dispatches — likely a stranded key");
		if (producerError is not null)
			Assert.Fail($"producer threw: {producerError}");
		Assert.That(Volatile.Read(ref dispatched), Is.EqualTo(total), "a Dispatch was rejected mid-run");

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, total);

		Assert.That(state.HandledCount, Is.EqualTo(total));
		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");
	}
}
