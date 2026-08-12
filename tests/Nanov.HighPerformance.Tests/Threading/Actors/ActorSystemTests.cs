namespace Nanov.HighPerformance.Tests.Threading.Actors;

using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Internal;
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
}
