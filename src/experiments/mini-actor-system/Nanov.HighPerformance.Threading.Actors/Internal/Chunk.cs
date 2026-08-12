namespace Nanov.HighPerformance.Threading.Actors.Internal;

/// <summary>
///   Non-generic home for the "no chunk bound" sentinel, so the chunk slab (generic over the message
///   type) and the key cell (generic over the key type) can share it without coupling.
/// </summary>
internal static class Chunk {
	/// <summary>No chunk is bound to this cell.</summary>
	internal const int None = -1;
}
