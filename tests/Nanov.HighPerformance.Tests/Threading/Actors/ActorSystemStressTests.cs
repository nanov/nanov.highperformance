namespace Nanov.HighPerformance.Tests.Threading.Actors;

using Nanov.HighPerformance.Tests.Threading.Workers;
using Nanov.HighPerformance.Threading.Actors;
using NUnit.Framework;

/// <summary>
///   Race guards. Each targets a named interleaving from the design spec; a regression here is a real
///   concurrency bug, not a flake. All run under the stall watchdogs so a lost wakeup fails fast.
///   <para>
///     Every test carries an explicit <see cref="CancelAfterAttribute" />. Without one, a wedged
///     <c>Dispatch</c> parks the TEST THREAD itself — before <see cref="WorkerTestUtil" />'s stall
///     watchdog is ever reached — so a lost wakeup hangs the whole suite instead of failing it. There is
///     no <c>.runsettings</c> global timeout anywhere in this repo, so this is the only backstop.
///     <c>CancelAfter</c>, not the older <c>Timeout</c>: this repo's NUnit (4.3.2) marks
///     <c>TimeoutAttribute</c> <c>[Obsolete]</c> — <c>TreatWarningsAsErrors</c> turns that into a build
///     failure — because .NET no longer supports safely aborting a blocked thread. Practically the two
///     behave the same way here: neither forcibly kills a parked producer/worker thread; both let the test
///     runner mark the test failed once the budget elapses instead of waiting on it forever, which is the
///     property this file actually needs.
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

		for (var i = 0; i < count; i++)
			Assert.That(system.Dispatch(1, i), Is.True, $"message {i}");

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
			for (var i = 0; i < count; i++)
				system.Dispatch(i % 4, i);

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
		var state = new RecordingState();
		using var system = New(state, keyCapacity: 256, chunkCount: 8, chunkSize: 4, batchBound: 2);
		system.Start();

		for (var i = 0; i < perKey; i++)
			for (var k = 0; k < keys; k++)
				Assert.That(system.Dispatch(k, i), Is.True);

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, keys * perKey);

		Assert.That(state.HandledCount, Is.EqualTo(keys * perKey));
		foreach (var (key, values) in state.ByKey())
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");
	}

	/// <summary>Sustained mixed load — order and exactly-once across many keys under churn.</summary>
	[Test]
	[CancelAfter(120_000)]
	public void Sustained_Mixed_Load_Preserves_Order_And_Loses_Nothing() {
		const int keys = 32;
		const int perKey = 5_000;
		var state = new RecordingState();
		using var system = New(state, keyCapacity: 256, chunkCount: 16, chunkSize: 16, batchBound: 3);
		system.Start();

		for (var i = 0; i < perKey; i++)
			for (var k = 0; k < keys; k++)
				Assert.That(system.Dispatch(k, i), Is.True);

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, keys * perKey);

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

		// RecordingHandler allocates (ConcurrentQueue nodes), so measure the PRODUCER only, with the
		// worker idle: this isolates Dispatch's own allocation.
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
	///     not report a wrong count — it reports a HANG, because <c>IsQuiescent</c> never sees
	///     processed == dispatched, which is exactly what the stall watchdog and the <see cref="CancelAfterAttribute" />
	///     backstop above exist to turn into a failure instead of a silent gap in coverage.
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
			for (var i = 0; i < burst; i++)
				Assert.That(system.Dispatch(0, i), Is.True, $"round {round}: burst message {i}");
			WorkerTestUtil.AwaitProgress(() => state.HandledCount, burst, TimeSpan.FromSeconds(5));

			// Key 0's absolute final message this round — nothing dispatches to key 0 again after this.
			Assert.That(system.Dispatch(0, burst), Is.True, $"round {round}: final message");
			// Only one chunk exists in this system, and it's held by key 0: this can only succeed once
			// key 0's chunk is harvestable, which forces Harvest() to scan key 0's cell right around the
			// final message's release.
			Assert.That(system.Dispatch(1, 0), Is.True, $"round {round}: forces Harvest against key 0");

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
}
