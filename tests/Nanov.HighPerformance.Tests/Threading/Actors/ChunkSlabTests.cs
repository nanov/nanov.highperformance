namespace Nanov.HighPerformance.Tests.Threading.Actors;

using NUnit.Framework;
using Nanov.HighPerformance.Threading.Actors.Internal;

[TestFixture]
public class ChunkSlabTests {
	[Test]
	public void Chunk_None_Is_Negative_One() =>
		Assert.That(Chunk.None, Is.EqualTo(-1));
}
