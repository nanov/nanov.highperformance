namespace Nanov.HighPerformance.Tests.Threading.Workers;

using Nanov.HighPerformance.Threading.Workers.Internal;
using NUnit.Framework;

[TestFixture]
public class CompactingKeyMapTests {
	// Keep-all predicate (no eviction) and a head/tail "pending" predicate for compaction tests.
	private readonly struct KeepAll : ISeqKeepPredicate {
		public bool Keep(int seq) => true;
	}

	private readonly struct Pending : ISeqKeepPredicate {
		private readonly int _head;
		private readonly int _tail;
		public Pending(int head, int tail) { _head = head; _tail = tail; }
		public bool Keep(int seq) => seq - _head >= 0 && _tail - seq > 0;
	}

	[Test]
	public void Missing_Key_Returns_False() {
		using var map = new CompactingKeyMap<int>(8);
		Assert.That(map.TryGetSeq(42, out _), Is.False);
		Assert.That(map.Count, Is.Zero);
	}

	[Test]
	public void Set_Then_Get_Returns_Seq() {
		using var map = new CompactingKeyMap<int>(8);
		map.Set(42, 1000);
		Assert.That(map.TryGetSeq(42, out var seq), Is.True);
		Assert.That(seq, Is.EqualTo(1000));
		Assert.That(map.Count, Is.EqualTo(1));
	}

	[Test]
	public void Set_Existing_Key_Updates_In_Place_No_Growth() {
		using var map = new CompactingKeyMap<int>(8);
		map.Set(42, 1000);
		map.Set(42, 2000);
		Assert.That(map.TryGetSeq(42, out var seq), Is.True);
		Assert.That(seq, Is.EqualTo(2000));
		Assert.That(map.Count, Is.EqualTo(1), "updating an existing key must not add an entry");
	}

	[Test]
	public void Many_Keys_With_Probe_Collisions_All_Resolve() {
		using var map = new CompactingKeyMap<int>(64);
		for (var i = 0; i < 48; i++) map.Set(i, i * 10);
		for (var i = 0; i < 48; i++) {
			Assert.That(map.TryGetSeq(i, out var seq), Is.True, $"key {i} should be present");
			Assert.That(seq, Is.EqualTo(i * 10));
		}
	}

	[Test]
	public void Compact_Drops_Entries_Where_Predicate_False() {
		using var map = new CompactingKeyMap<int>(64);
		// seqs 0..31; "pending" window [16, 32) keeps 16..31, drops 0..15.
		for (var i = 0; i < 32; i++) map.Set(i, i);
		map.Compact(new Pending(16, 32));
		Assert.That(map.Count, Is.EqualTo(16));
		for (var i = 0; i < 16; i++) Assert.That(map.TryGetSeq(i, out _), Is.False, $"key {i} should be evicted");
		for (var i = 16; i < 32; i++) {
			Assert.That(map.TryGetSeq(i, out var seq), Is.True, $"key {i} should survive");
			Assert.That(seq, Is.EqualTo(i));
		}
	}

	[Test]
	public void Compact_Then_Set_Reuses_Freed_Capacity() {
		using var map = new CompactingKeyMap<int>(8); // entriesCapacity = 16
		for (var i = 0; i < 16; i++) map.Set(i, i);
		Assert.That(map.IsFull, Is.True);
		map.Compact(new Pending(8, 16)); // keep 8..15, drop 0..7
		Assert.That(map.IsFull, Is.False);
		map.Set(100, 999); // re-add into reclaimed room
		Assert.That(map.TryGetSeq(100, out var seq), Is.True);
		Assert.That(seq, Is.EqualTo(999));
	}

	[Test]
	public void KeepAll_Compact_Is_Identity() {
		using var map = new CompactingKeyMap<int>(8);
		map.Set(1, 10);
		map.Set(2, 20);
		map.Compact(new KeepAll());
		Assert.That(map.Count, Is.EqualTo(2));
		Assert.That(map.TryGetSeq(1, out var a), Is.True);
		Assert.That(a, Is.EqualTo(10));
		Assert.That(map.TryGetSeq(2, out var b), Is.True);
		Assert.That(b, Is.EqualTo(20));
	}
}
