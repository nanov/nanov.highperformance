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
}
