namespace Nanov.HighPerformance.Tests.Threading.Actors;

using Nanov.HighPerformance.Threading.Actors.Internal;
using NUnit.Framework;

[TestFixture]
public class KeyRingTests {
	[Test]
	public void Rounds_Capacity_Up_To_Power_Of_Two() =>
		Assert.That(new KeyRing(5).Capacity, Is.EqualTo(8));

	[Test]
	public void Dequeue_On_Empty_Returns_False() {
		var ring = new KeyRing(4);
		Assert.Multiple(() => {
			Assert.That(ring.TryDequeue(out _), Is.False);
			Assert.That(ring.CountApprox, Is.EqualTo(0));
		});
	}

	[Test]
	public void Preserves_Fifo_Order() {
		var ring = new KeyRing(8);
		for (var i = 0; i < 8; i++)
			Assert.That(ring.TryEnqueue(i), Is.True);

		for (var i = 0; i < 8; i++) {
			Assert.That(ring.TryDequeue(out var v), Is.True);
			Assert.That(v, Is.EqualTo(i));
		}
	}

	[Test]
	public void Rejects_When_Full() {
		var ring = new KeyRing(4);
		for (var i = 0; i < 4; i++)
			Assert.That(ring.TryEnqueue(i), Is.True);

		Assert.That(ring.TryEnqueue(99), Is.False);
	}

	[Test]
	public void Wraps_Around_Correctly() {
		var ring = new KeyRing(4);
		for (var round = 0; round < 10; round++) {
			Assert.That(ring.TryEnqueue(round), Is.True);
			Assert.That(ring.TryDequeue(out var v), Is.True);
			Assert.That(v, Is.EqualTo(round));
		}
	}

	[Test]
	public void Spsc_Transfers_Every_Item_In_Order() {
		const int count = 200_000;
		var ring = new KeyRing(1024);
		var received = 0;

		var consumer = new Thread(() => {
			var expected = 0;
			while (expected < count)
				if (ring.TryDequeue(out var v)) {
					Assert.That(v, Is.EqualTo(expected));
					expected++;
					Volatile.Write(ref received, expected);
				}
		}) { IsBackground = true };
		consumer.Start();

		for (var i = 0; i < count; i++)
			while (!ring.TryEnqueue(i)) Thread.SpinWait(1);

		Assert.That(consumer.Join(TimeSpan.FromSeconds(30)), Is.True, "consumer stalled");
		Assert.That(Volatile.Read(ref received), Is.EqualTo(count));
	}
}
