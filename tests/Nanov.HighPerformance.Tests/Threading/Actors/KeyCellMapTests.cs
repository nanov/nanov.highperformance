namespace Nanov.HighPerformance.Tests.Threading.Actors;

using Nanov.HighPerformance.Threading.Actors.Internal;
using NUnit.Framework;

[TestFixture]
public class KeyCellMapTests {
	[Test]
	public void Inserts_Then_Finds_The_Same_Cell() {
		var map = new KeyCellMap<int>(16);

		ref var first = ref map.GetOrAddRef(42, out var existing, out var index);
		Assert.That(existing, Is.False);
		first.Tail = 7;

		ref var again = ref map.GetOrAddRef(42, out var existingAgain, out var indexAgain);
		var tail = again.Tail;

		Assert.Multiple(() => {
			Assert.That(existingAgain, Is.True);
			Assert.That(indexAgain, Is.EqualTo(index));
			Assert.That(tail, Is.EqualTo(7));
		});
	}

	[Test]
	public void Fresh_Cell_Starts_Free_With_No_Chunk() {
		var map = new KeyCellMap<int>(16);
		ref var cell = ref map.GetOrAddRef(1, out _, out _);

		var state = cell.State;
		var chunkBase = cell.ChunkBase;
		var head = cell.Head;
		var tail = cell.Tail;
		var cachedHead = cell.CachedHead;

		Assert.Multiple(() => {
			Assert.That(state, Is.EqualTo(CellState.Free));
			Assert.That(chunkBase, Is.EqualTo(Chunk.None));
			Assert.That(head, Is.EqualTo(0));
			Assert.That(tail, Is.EqualTo(0));
			Assert.That(cachedHead, Is.EqualTo(0));
		});
	}

	[Test]
	public void RefAt_Returns_The_Live_Cell() {
		var map = new KeyCellMap<int>(16);
		map.GetOrAddRef(5, out _, out var index).Tail = 3;

		Assert.That(map.RefAt(index).Tail, Is.EqualTo(3));
		map.RefAt(index).Tail = 9;
		Assert.That(map.GetOrAddRef(5, out _, out _).Tail, Is.EqualTo(9));
	}

	[Test]
	public void Distinct_Keys_Get_Distinct_Indices() {
		var map = new KeyCellMap<int>(64);
		var seen = new HashSet<int>();
		for (var k = 0; k < 32; k++) {
			map.GetOrAddRef(k, out var existing, out var index);
			Assert.That(existing, Is.False, $"key {k}");
			Assert.That(seen.Add(index), Is.True, $"key {k} reused index {index}");
		}

		Assert.That(map.Count, Is.EqualTo(32));
	}

	[Test]
	public void Evicts_Free_Unbound_Cells_When_Table_Fills() {
		var map = new KeyCellMap<int>(4); // 8 slots

		// Fill with drained cells (Free + no chunk) — all evictable.
		for (var k = 0; k < 8; k++)
			map.GetOrAddRef(k, out _, out _);

		// A ninth distinct key forces the cold path; it must evict rather than throw.
		map.GetOrAddRef(999, out var existing, out _);
		Assert.That(existing, Is.False);
	}

	[Test]
	public void Never_Evicts_A_Cell_That_Still_Holds_A_Chunk() {
		var map = new KeyCellMap<int>(4); // 8 slots

		for (var k = 0; k < 8; k++) {
			ref var cell = ref map.GetOrAddRef(k, out _, out _);
			cell.ChunkBase = k; // Free, but storage still bound → not evictable
		}

		Assert.Throws<InvalidOperationException>(() => map.GetOrAddRef(999, out _, out _));
	}

	[Test]
	public void Never_Evicts_A_Non_Free_Cell() {
		var map = new KeyCellMap<int>(4); // 8 slots

		for (var k = 0; k < 8; k++) {
			ref var cell = ref map.GetOrAddRef(k, out _, out _);
			cell.State = CellState.Owned;
		}

		Assert.Throws<InvalidOperationException>(() => map.GetOrAddRef(999, out _, out _));
	}
}
