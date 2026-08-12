namespace Nanov.HighPerformance.Tests.Threading.Actors;

using Nanov.HighPerformance.Threading.Actors.Internal;
using NUnit.Framework;

[TestFixture]
public class ChunkSlabTests {
	[Test]
	public void Chunk_None_Is_Negative_One() =>
		Assert.That(Chunk.None, Is.EqualTo(-1));

	[Test]
	public void Rounds_Capacities_Up_To_Powers_Of_Two() {
		var slab = new ChunkSlab<int>(chunkCount: 5, chunkSize: 3);
		Assert.Multiple(() => {
			Assert.That(slab.ChunkCount, Is.EqualTo(8));
			Assert.That(slab.ChunkSize, Is.EqualTo(4));
			Assert.That(slab.ChunkMask, Is.EqualTo(3));
		});
	}

	[Test]
	public void Rents_Every_Chunk_Then_Fails() {
		var slab = new ChunkSlab<int>(chunkCount: 4, chunkSize: 4);
		Assert.That(slab.FreeCount, Is.EqualTo(4));

		for (var i = 0; i < 4; i++)
			Assert.That(slab.TryRent(out _), Is.True, $"rent {i}");

		Assert.Multiple(() => {
			Assert.That(slab.FreeCount, Is.EqualTo(0));
			Assert.That(slab.TryRent(out _), Is.False);
		});
	}

	[Test]
	public void Rents_Distinct_Chunks() {
		var slab = new ChunkSlab<int>(chunkCount: 4, chunkSize: 4);
		var seen = new HashSet<int>();
		while (slab.TryRent(out var chunk))
			Assert.That(seen.Add(chunk), Is.True, $"chunk {chunk} handed out twice");

		Assert.That(seen, Has.Count.EqualTo(4));
	}

	[Test]
	public void Returned_Chunk_Is_Rentable_Again() {
		var slab = new ChunkSlab<int>(chunkCount: 2, chunkSize: 4);
		Assert.That(slab.TryRent(out var a), Is.True);
		Assert.That(slab.TryRent(out _), Is.True);
		Assert.That(slab.TryRent(out _), Is.False);

		slab.Return(a);
		Assert.Multiple(() => {
			Assert.That(slab.FreeCount, Is.EqualTo(1));
			Assert.That(slab.TryRent(out var again), Is.True);
			Assert.That(again, Is.EqualTo(a));
		});
	}

	[Test]
	public void Slots_Of_Different_Chunks_Do_Not_Alias() {
		var slab = new ChunkSlab<int>(chunkCount: 4, chunkSize: 4);
		Assert.That(slab.TryRent(out var a), Is.True);
		Assert.That(slab.TryRent(out var b), Is.True);

		for (var i = 0; i < slab.ChunkSize; i++) {
			slab.SlotRef(a, i) = 100 + i;
			slab.SlotRef(b, i) = 200 + i;
		}

		for (var i = 0; i < slab.ChunkSize; i++)
			Assert.Multiple(() => {
				Assert.That(slab.SlotRef(a, i), Is.EqualTo(100 + i));
				Assert.That(slab.SlotRef(b, i), Is.EqualTo(200 + i));
			});
	}

	[Test]
	public void ChunkSpan_Covers_Exactly_That_Chunk() {
		var slab = new ChunkSlab<int>(chunkCount: 4, chunkSize: 4);
		Assert.That(slab.TryRent(out var a), Is.True);
		for (var i = 0; i < slab.ChunkSize; i++)
			slab.SlotRef(a, i) = 7;

		var span = slab.ChunkSpan(a);
		Assert.That(span.Length, Is.EqualTo(slab.ChunkSize));
		foreach (var v in span)
			Assert.That(v, Is.EqualTo(7));

		span.Clear();
		Assert.That(slab.SlotRef(a, 0), Is.EqualTo(0));
	}
}
