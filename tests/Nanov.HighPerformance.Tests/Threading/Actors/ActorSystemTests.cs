namespace Nanov.HighPerformance.Tests.Threading.Actors;

using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Internal;
using Nanov.HighPerformance.Tests.Threading.Workers;
using NUnit.Framework;

[TestFixture]
public class ActorSystemTests {
	private static ActorSystem<int, int, RecordingHandler> New(RecordingState state,
		int keyCapacity = 64, int chunkCount = 16, int chunkSize = 8, int batchBound = 0) =>
		new(new RecordingHandler(state), keyCapacity, chunkCount, chunkSize, batchBound);

	[Test]
	public void First_Dispatch_Binds_A_Chunk_And_Schedules() {
		var state = new RecordingState();
		using var system = New(state);

		Assert.That(system.Dispatch(1, 100), Is.True);

		// Copy out before asserting: a ref local cannot be captured in Assert.Multiple's lambda (CS8175).
		ref var cell = ref system.CellOf(1);
		var chunkBase = cell.ChunkBase;
		var tail = cell.Tail;
		var head = cell.Head;
		var cellState = cell.State;

		Assert.Multiple(() => {
			Assert.That(chunkBase, Is.Not.EqualTo(Chunk.None));
			Assert.That(tail, Is.EqualTo(1));
			Assert.That(head, Is.EqualTo(0));
			Assert.That(cellState, Is.EqualTo(CellState.Scheduled));
			Assert.That(system.ScheduledCountApprox, Is.EqualTo(1));
		});
	}

	[Test]
	public void Second_Dispatch_To_A_Scheduled_Key_Does_Not_Reschedule() {
		var state = new RecordingState();
		using var system = New(state);

		system.Dispatch(1, 100);
		system.Dispatch(1, 101);

		Assert.Multiple(() => {
			Assert.That(system.CellOf(1).Tail, Is.EqualTo(2));
			Assert.That(system.ScheduledCountApprox, Is.EqualTo(1), "first-dirty gating failed");
		});
	}

	[Test]
	public void Distinct_Keys_Each_Schedule_Once() {
		var state = new RecordingState();
		using var system = New(state);

		for (var k = 0; k < 5; k++) {
			system.Dispatch(k, k * 10);
			system.Dispatch(k, k * 10 + 1);
		}

		Assert.That(system.ScheduledCountApprox, Is.EqualTo(5));
	}

	[Test]
	public void Distinct_Keys_Get_Distinct_Chunks() {
		var state = new RecordingState();
		using var system = New(state);

		var chunks = new HashSet<int>();
		for (var k = 0; k < 5; k++) {
			system.Dispatch(k, k);
			Assert.That(chunks.Add(system.CellOf(k).ChunkBase), Is.True, $"key {k} shares a chunk");
		}
	}

	[Test]
	public void BatchBound_Defaults_To_ChunkSize() {
		var state = new RecordingState();
		using var system = New(state, chunkSize: 16, batchBound: 0);
		Assert.That(system.BatchBound, Is.EqualTo(16));
	}

	[Test]
	public void Processes_All_Messages_Of_One_Key_In_Order() {
		var state = new RecordingState();
		using var system = New(state, chunkSize: 512, batchBound: 8); // must hold all 500 — no backpressure yet
		system.Start();

		const int count = 500;
		for (var i = 0; i < count; i++)
			Assert.That(system.Dispatch(1, i), Is.True);

		WorkerTestUtil.AwaitProgress(() => state.HandledCount, count);
		Assert.That(state.ByKey()[1], Is.EqualTo(Enumerable.Range(0, count).ToList()));
	}

	[Test]
	public void Preserves_Order_Within_Every_Key_Across_Many_Keys() {
		var state = new RecordingState();
		// chunkSize must hold all 200 messages of one key — no backpressure until Task 9.
		using var system = New(state, keyCapacity: 128, chunkCount: 32, chunkSize: 256, batchBound: 8);
		system.Start();

		const int keys = 16;
		const int perKey = 200;
		for (var i = 0; i < perKey; i++)
			for (var k = 0; k < keys; k++)
				Assert.That(system.Dispatch(k, i), Is.True);

		WorkerTestUtil.AwaitProgress(() => state.HandledCount, keys * perKey);

		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");
	}

	[Test]
	public void Reschedules_A_Key_That_Exceeds_BatchBound() {
		var state = new RecordingState();
		// chunkSize holds all 100; batchBound 4 is what forces the repeated reschedule under test.
		using var system = New(state, chunkSize: 128, batchBound: 4);
		system.Start();

		const int count = 100;
		for (var i = 0; i < count; i++)
			system.Dispatch(1, i);

		WorkerTestUtil.AwaitProgress(() => state.HandledCount, count);
		Assert.That(state.ByKey()[1], Is.EqualTo(Enumerable.Range(0, count).ToList()));
		Assert.That(system.Activations, Is.GreaterThan(1),
			"batchBound was not honoured — 100 messages drained in a single activation");
	}

	[Test]
	public void Handler_Exception_Is_Reported_And_Processing_Continues() {
		var state = new RecordingState { Throw = (_, value) => value == 2 };
		using var system = New(state);
		system.Start();

		for (var i = 0; i < 5; i++)
			system.Dispatch(1, i);

		WorkerTestUtil.AwaitProgress(() => state.HandledCount, 5);
		Assert.Multiple(() => {
			Assert.That(state.Errors, Has.Count.EqualTo(1));
			Assert.That(state.ByKey()[1], Is.EqualTo(new List<int> { 0, 1, 3, 4 }), "order broke around the throw");
		});
	}

	[Test]
	public void TryComplete_Drains_Everything_Then_Completes() {
		var state = new RecordingState();
		// 2000 messages over 8 keys = 250 per key; chunkSize must hold that (no backpressure until Task 9).
		using var system = New(state, chunkSize: 256);
		system.Start();

		const int count = 2_000;
		for (var i = 0; i < count; i++)
			system.Dispatch(i % 8, i);

		Assert.That(system.TryComplete(), Is.True);
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, count);
		Assert.That(state.HandledCount, Is.EqualTo(count));
	}

	[Test]
	public void TryComplete_On_An_Already_Drained_Parked_System_Completes() {
		var state = new RecordingState();
		using var system = New(state);
		system.Start();

		system.Dispatch(1, 1);
		WorkerTestUtil.AwaitProgress(() => state.HandledCount, 1);
		Thread.Sleep(100); // let the worker genuinely park

		Assert.That(system.TryComplete(), Is.True);
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 1);
	}

	[Test]
	public void TryComplete_On_A_System_That_Never_Received_Anything_Completes() {
		var state = new RecordingState();
		using var system = New(state);
		system.Start();
		Thread.Sleep(100);

		Assert.That(system.TryComplete(), Is.True);
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 0);
	}

	[Test]
	public void TryComplete_Is_Idempotent() {
		var state = new RecordingState();
		using var system = New(state);
		system.Start();

		Assert.Multiple(() => {
			Assert.That(system.TryComplete(), Is.True);
			Assert.That(system.TryComplete(), Is.False);
		});
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 0);
	}

	[Test]
	public void Dispatch_After_TryComplete_Is_Rejected() {
		var state = new RecordingState();
		using var system = New(state);
		system.Start();
		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 0);

		Assert.That(system.Dispatch(1, 1), Is.False);
	}

	[Test]
	public void OnCompleted_Fires_Once_Per_Worker() {
		var state = new RecordingState();
		using var system = New(state);
		system.Start();
		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 0);

		Assert.That(Volatile.Read(ref state.CompletedCalls), Is.EqualTo(1));
	}

	[Test]
	public void TryComplete_With_An_Exception_Faults_Completion() {
		var state = new RecordingState();
		using var system = New(state);
		system.Start();
		system.TryComplete(new InvalidOperationException("boom"));

		// Task.Wait(int) always surfaces a fault as AggregateException — it never unwraps, even from
		// inside a polling loop like AwaitCompletionOrStall — so this matches the sibling
		// TryComplete_With_Exception_Faults_Completion tests in the SPSC worker suite (SpWorkerTests,
		// AsyncSpWorkerTests, ConflationSpWorkerTests), which all assert this way rather than expecting
		// the inner exception type directly.
		var agg = Assert.Throws<AggregateException>(() => system.Completion.Wait(2000));
		Assert.That(agg!.InnerException, Is.InstanceOf<InvalidOperationException>());
		Assert.That(agg.InnerException!.Message, Is.EqualTo("boom"));
	}

	[Test]
	public void Dispose_Drops_Queued_Messages() {
		var state = new RecordingState();
		var started = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		state.Throw = (_, value) => {
			if (value == 0) {
				// Signal entry BEFORE blocking: HandledCount only increments once Handle() returns from
				// this callback, i.e. AFTER gate.Wait unblocks — waiting on that instead of "started"
				// races Dispose() against the un-gated messages 1..49, which are fast enough to all drain
				// before Dispose() ever sets _stopped. "started" pins the synchronization point to the
				// instant the worker is actually parked inside message 0.
				started.Set();
				gate.Wait(TimeSpan.FromSeconds(5));
			}

			return false;
		};

		var system = New(state, chunkSize: 64);
		system.Start();
		for (var i = 0; i < 50; i++)
			system.Dispatch(1, i);

		Assert.That(started.Wait(TimeSpan.FromSeconds(5)), Is.True, "worker never reached message 0");
		system.Dispose();
		gate.Set();

		// Bounded settle before asserting: if hard stop did NOT honour _stopped, the worker needs only
		// microseconds to drain the remaining 49 messages once gate.Set() unblocks it — reading
		// HandledCount immediately here would race that drain and pass regardless of whether _stopped
		// actually worked. Waiting (bounded) on Completion gives the worker a real chance to either exit
		// promptly (the honoured case) or finish draining everything (the broken case), so the assertion
		// below actually distinguishes the two instead of passing either way.
		try {
			system.Completion.Wait(TimeSpan.FromMilliseconds(500));
		}
		catch {
			// Completion settling as Cancelled/Faulted is expected here — only used to let the worker
			// settle before the assertion below, not asserted on directly.
		}

		Assert.That(state.HandledCount, Is.LessThan(50), "hard stop should have dropped queued messages");
	}

	[Test]
	public void Dispose_Faults_Completion_As_Cancelled() {
		var state = new RecordingState();
		var system = New(state);
		system.Start();
		system.Dispatch(1, 1);
		WorkerTestUtil.AwaitProgress(() => state.HandledCount, 1);

		system.Dispose();

		// NOT Assert.ThrowsAsync<OperationCanceledException> — that constraint is an EXACT-type match
		// (Throws.TypeOf<T>()), and awaiting a cancelled Task surfaces System.Threading.Tasks.
		// TaskCanceledException, a subclass. Assert.CatchAsync<T> is the InstanceOf<T> form and is what
		// actually exercises "Completion is cancelled, not faulted" the way the brief intends.
		Assert.CatchAsync<OperationCanceledException>(async () => await system.Completion);
	}

	[Test]
	public void Dispose_Is_Idempotent() {
		var state = new RecordingState();
		var system = New(state);
		system.Start();
		system.Dispose();
		Assert.DoesNotThrow(() => system.Dispose());
	}

	[Test]
	public void Dispatch_After_Dispose_Is_Rejected() {
		var state = new RecordingState();
		var system = New(state);
		system.Start();
		system.Dispose();

		Assert.That(system.Dispatch(1, 1), Is.False);
	}

	[Test]
	public void Dispose_After_TryComplete_With_Work_Outstanding_Cancels_Completion() {
		var state = new RecordingState();
		var started = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		state.Throw = (_, value) => {
			if (value == 0) {
				started.Set();
				gate.Wait(TimeSpan.FromSeconds(5));
			}

			return false;
		};

		var system = New(state, chunkSize: 64);
		system.Start();
		for (var i = 0; i < 50; i++)
			system.Dispatch(1, i);

		Assert.That(started.Wait(TimeSpan.FromSeconds(5)), Is.True, "worker never reached message 0");
		system.TryComplete(); // graceful shutdown requested...
		system.Dispose();     // ...then forced before the drain could finish
		gate.Set();

		Assert.CatchAsync<OperationCanceledException>(async () => await system.Completion);
		Assert.That(state.HandledCount, Is.LessThan(50), "messages should have been dropped");
	}

	// Regression guards for a carried-over defect: a system that was constructed but Start() was never
	// called has no worker thread, so RunLoop's finally — the only other place Completion resolves —
	// never runs. Without the _worker-is-null branch in TryComplete/Dispose, await system.Completion
	// would hang forever here.

	[Test]
	public void TryComplete_On_A_System_That_Was_Never_Started_Resolves_Completion() {
		var state = new RecordingState();
		using var system = New(state);

		Assert.That(system.TryComplete(), Is.True);
		// Bounded, not a bare `await`: a regression here (the _worker-is-null branch missing) means
		// Completion never resolves, and an unbounded wait would hang the suite instead of failing it.
		Assert.That(system.Completion.Wait(TimeSpan.FromSeconds(5)), Is.True,
			"Completion did not resolve for a never-started system");
	}

	[Test]
	public void Dispose_On_A_System_That_Was_Never_Started_Resolves_Completion_As_Cancelled() {
		var state = new RecordingState();
		using var system = New(state);

		system.Dispose();

		// Bounded, not a bare `await`, for the same reason as above. Task.Wait throws (rather than
		// returning true/false) once the task is in a non-success terminal state, so — unlike the
		// TryComplete case above — the bound here is expressed as Assert.Throws<AggregateException>
		// around Wait(TimeSpan), matching the existing TryComplete_With_An_Exception_Faults_Completion
		// idiom: it throws promptly if Completion is already Canceled, or times out (no exception, test
		// fails) after 5s if a regression left Completion unresolved — never hangs.
		var agg = Assert.Throws<AggregateException>(() => system.Completion.Wait(TimeSpan.FromSeconds(5)));
		Assert.That(agg!.InnerException, Is.InstanceOf<OperationCanceledException>());
	}

	[Test]
	public void TryDispatch_Returns_False_When_The_Key_Mailbox_Is_Full() {
		var state = new RecordingState();
		using var system = New(state, chunkSize: 4); // never started — nothing drains

		for (var i = 0; i < 4; i++)
			Assert.That(system.TryDispatch(1, i), Is.True, $"message {i}");

		Assert.Multiple(() => {
			Assert.That(system.TryDispatch(1, 99), Is.False);
			Assert.That(system.DispatchedCount, Is.EqualTo(4), "a rejected TryDispatch must not be counted");
		});
	}

	[Test]
	public void TryDispatch_Returns_False_When_The_Slab_Is_Exhausted() {
		var state = new RecordingState();
		using var system = New(state, keyCapacity: 64, chunkCount: 4, chunkSize: 4);

		for (var k = 0; k < 4; k++)
			Assert.That(system.TryDispatch(k, k), Is.True, $"key {k}");

		Assert.That(system.TryDispatch(99, 99), Is.False, "5th distinct key should find no chunk");
	}

	[Test]
	public void Dispatch_Blocks_Until_The_Worker_Drains_Then_Proceeds() {
		var state = new RecordingState();
		using var system = New(state, chunkSize: 4);
		const int count = 200;

		var producer = new Thread(() => {
			for (var i = 0; i < count; i++)
				system.Dispatch(1, i);
		}) { IsBackground = true };
		producer.Start();

		Thread.Sleep(50);
		system.Start(); // producer is blocked on a full mailbox until now

		Assert.That(producer.Join(TimeSpan.FromSeconds(10)), Is.True, "producer never unblocked");
		WorkerTestUtil.AwaitProgress(() => state.HandledCount, count);
		Assert.That(state.ByKey()[1], Is.EqualTo(Enumerable.Range(0, count).ToList()));
		Assert.That(system.ProducerWakes, Is.GreaterThan(0),
			"the worker never woke the parked producer — backpressure is running on the 20ms poll alone");
	}

	[Test]
	public void Dispatch_Blocked_On_A_Full_Mailbox_Is_Released_By_Dispose() {
		var state = new RecordingState();
		var system = New(state, chunkSize: 4);
		var finished = 0;

		var producer = new Thread(() => {
			for (var i = 0; i < 1_000; i++)
				if (!system.Dispatch(1, i)) break;
			Volatile.Write(ref finished, 1);
		}) { IsBackground = true };
		producer.Start();

		Thread.Sleep(100);
		Assert.That(Volatile.Read(ref finished), Is.EqualTo(0), "producer was never actually blocked");
		system.Dispose();

		Assert.That(producer.Join(TimeSpan.FromSeconds(5)), Is.True, "producer stayed parked after Dispose");
		Assert.That(Volatile.Read(ref finished), Is.EqualTo(1));
	}

	[Test]
	public void TryComplete_Unblocks_A_Producer_Parked_On_A_Full_Mailbox() {
		var state = new RecordingState();
		using var system = New(state, chunkSize: 4);
		var finished = 0;

		var producer = new Thread(() => {
			for (var i = 0; i < 1_000; i++)
				if (!system.Dispatch(1, i)) break;
			Volatile.Write(ref finished, 1);
		}) { IsBackground = true };
		producer.Start();

		Thread.Sleep(100);
		Assert.That(Volatile.Read(ref finished), Is.EqualTo(0), "producer was never actually blocked");

		system.TryComplete();

		Assert.That(producer.Join(TimeSpan.FromSeconds(5)), Is.True, "producer stayed parked after TryComplete");
		Assert.That(Volatile.Read(ref finished), Is.EqualTo(1));
	}

	[Test]
	public void Harvest_Recycles_Chunks_Of_Drained_Keys() {
		var state = new RecordingState();
		using var system = New(state, keyCapacity: 64, chunkCount: 4, chunkSize: 4);
		system.Start();

		// Far more distinct keys than chunks: only recycling makes this possible.
		const int keys = 40;
		for (var k = 0; k < keys; k++) {
			Assert.That(system.Dispatch(k, k), Is.True, $"key {k}");
			WorkerTestUtil.AwaitProgress(() => state.HandledCount, k + 1);
		}

		Assert.That(state.HandledCount, Is.EqualTo(keys));
	}

	[Test]
	public void Harvest_Invokes_OnChunkReclaimed() {
		var state = new RecordingState();
		using var system = New(state, keyCapacity: 64, chunkCount: 4, chunkSize: 4);
		system.Start();

		for (var k = 0; k < 20; k++) {
			system.Dispatch(k, k);
			WorkerTestUtil.AwaitProgress(() => state.HandledCount, k + 1);
		}

		Assert.That(Volatile.Read(ref state.ChunkReclaimCalls), Is.GreaterThan(0));
	}

	[Test]
	public void OnSlotReused_Fires_When_A_Slot_Is_Overwritten() {
		var state = new RecordingState();
		using var system = New(state, chunkSize: 4);
		system.Start();

		const int count = 40; // wraps the 4-slot mailbox many times
		for (var i = 0; i < count; i++)
			system.Dispatch(1, i);

		WorkerTestUtil.AwaitProgress(() => state.HandledCount, count);
		Assert.That(Volatile.Read(ref state.SlotReuseCalls), Is.EqualTo(count));
	}

	// Originally named Harvest_Leaves_A_Redirtied_Key_Scheduled and written to catch the reclaim-revert
	// stranding bug on Harvest's "went dirty between check and claim" path (§4.4). It doesn't actually
	// guard that: a strand SELF-HEALS here, because every round below redispatches to all 6 keys, so the
	// very next dispatch to a stranded key sees State == Free and reschedules it through DispatchCore's
	// own test-then-CAS — only a strand on a key's absolute LAST message would ever survive to fail the
	// assertion, and nothing here guarantees landing that interleaving. Renamed to describe what it
	// actually is: a probabilistic liveness smoke test for harvest-under-churn, not a targeted race guard.
	// A targeted guard that forces the exact interleaving (rather than hoping for it) lives in
	// ActorSystemStressTests.Harvest_Race_On_A_Keys_Final_Message_Is_Never_Stranded.
	[Test]
	public void Harvest_Under_Churn_Loses_No_Messages() {
		var state = new RecordingState();
		using var system = New(state, keyCapacity: 64, chunkCount: 4, chunkSize: 4);
		system.Start();

		for (var round = 0; round < 200; round++) {
			for (var k = 0; k < 6; k++)
				system.Dispatch(k, round);
		}

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 200 * 6);
		Assert.That(state.HandledCount, Is.EqualTo(200 * 6));
	}
}
