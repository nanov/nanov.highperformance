namespace Nanov.HighPerformance.Tests.Threading.Workers;

using Nanov.HighPerformance.Threading.Workers.Internal;
using NUnit.Framework;

[TestFixture]
public class AccumulatingKeyMapTests {
	[Test]
	public void GetOrAddRef_New_Key_Reports_Not_Existing() {
		using var map = new AccumulatingKeyMap<int, long>(8);
		ref var cell = ref map.GetOrAddRef(42, out var existing, out var index);
		Assert.That(existing, Is.False);
		Assert.That(index, Is.GreaterThanOrEqualTo(0));
		// caller initializes the fresh cell
		cell.Key = 42;
		cell.Acc = 0;
		cell.State = 0;
		Assert.That(map.Count, Is.EqualTo(1));
	}

	[Test]
	public void GetOrAddRef_Existing_Returns_Same_Cell_And_Index() {
		using var map = new AccumulatingKeyMap<int, long>(8);
		ref var a = ref map.GetOrAddRef(42, out var existA, out var idxA);
		a.Key = 42;
		a.Acc = 100;

		ref var b = ref map.GetOrAddRef(42, out var existB, out var idxB);
		Assert.That(existA, Is.False);
		Assert.That(existB, Is.True);
		Assert.That(idxB, Is.EqualTo(idxA));
		Assert.That(b.Acc, Is.EqualTo(100), "must be the same cell (accumulated state preserved)");
		Assert.That(map.Count, Is.EqualTo(1));
	}

	[Test]
	public void RefAt_Returns_The_Same_Cell_As_Its_Index() {
		using var map = new AccumulatingKeyMap<int, long>(8);
		ref var a = ref map.GetOrAddRef(7, out _, out var idx);
		a.Key = 7;
		a.Acc = 55;

		ref var viaIndex = ref map.RefAt(idx);
		Assert.That(viaIndex.Key, Is.EqualTo(7));
		Assert.That(viaIndex.Acc, Is.EqualTo(55));
	}

	[Test]
	public void Many_Keys_With_Probe_Collisions_All_Resolve() {
		using var map = new AccumulatingKeyMap<int, long>(64);
		for (var i = 0; i < 40; i++) {
			ref var c = ref map.GetOrAddRef(i, out var existing, out _);
			Assert.That(existing, Is.False);
			c.Key = i;
			c.Acc = i * 10;
		}

		for (var i = 0; i < 40; i++) {
			ref var c = ref map.GetOrAddRef(i, out var existing, out _);
			Assert.That(existing, Is.True, $"key {i} should be present");
			Assert.That(c.Acc, Is.EqualTo(i * 10));
		}
	}

	[Test]
	public void Drained_Cells_Are_Evicted_To_Reuse_Slots() {
		using var map = new AccumulatingKeyMap<int, long>(8); // small → forces eviction
		// Add many distinct one-shot keys, each immediately "drained" (State == 0). The map must reuse
		// their slots for later keys rather than overflowing.
		for (var i = 0; i < 200; i++) {
			ref var c = ref map.GetOrAddRef(i, out var existing, out _);
			Assert.That(existing, Is.False, $"key {i} is new each round");
			c.Key = i;
			c.Acc = i;
			c.State = 0; // drained → evictable
		}

		// Count never exceeds the slot table; old drained keys were evicted.
		Assert.That(map.Count, Is.LessThanOrEqualTo(map.SlotCount));
		Assert.That(map.GetOrAddRef(0, out var existed, out _) is var _ && existed, Is.False,
			"an old drained key was evicted, so re-adding it is 'new'");
	}

	[Test]
	public void Dirty_Cells_Are_Not_Evicted() {
		using var map = new AccumulatingKeyMap<int, long>(8);
		// Mark a handful of keys dirty (State != 0) — they must survive while many one-shot keys churn.
		for (var k = 0; k < 4; k++) {
			ref var c = ref map.GetOrAddRef(k, out _, out _);
			c.Key = k;
			c.Acc = k * 1000;
			c.State = 2; // QUEUED-ish (non-zero) → not evictable
		}

		for (var i = 100; i < 300; i++) {
			ref var c = ref map.GetOrAddRef(i, out _, out _);
			c.Key = i;
			c.Acc = i;
			c.State = 0; // drained
		}

		for (var k = 0; k < 4; k++) {
			ref var c = ref map.GetOrAddRef(k, out var existing, out _);
			Assert.That(existing, Is.True, $"dirty key {k} must survive eviction churn");
			Assert.That(c.Acc, Is.EqualTo(k * 1000));
		}
	}
}
