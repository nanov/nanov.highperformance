namespace Nanov.HighPerformance.Tests.Threading.Actors;

using System.Collections.Concurrent;
using Nanov.HighPerformance.Threading.Actors;

/// <summary>Shared, mutable state observed through a reference — the struct handler is copied.</summary>
internal sealed class RecordingState {
	public readonly ConcurrentQueue<(int Key, int Value)> Seen = new();
	public readonly ConcurrentQueue<Exception> Errors = new();
	public readonly ConcurrentQueue<Exception?> CompletedWith = new();
	public int Handled;
	public int CompletedCalls;
	public int SlotReuseCalls;
	public int ChunkReclaimCalls;
	public Func<int, int, bool>? Throw { get; set; }

	public int HandledCount => Volatile.Read(ref Handled);

	/// <summary>Per-key values in arrival order — the ordering assertion's raw material.</summary>
	public Dictionary<int, List<int>> ByKey() {
		var result = new Dictionary<int, List<int>>();
		foreach (var (key, value) in Seen) {
			if (!result.TryGetValue(key, out var list)) {
				list = new List<int>();
				result[key] = list;
			}

			list.Add(value);
		}

		return result;
	}
}

// NOTE: an explicit constructor, not a primary constructor — .editorconfig sets
// csharp_style_prefer_primary_constructors = false and EnforceCodeStyleInBuild is on.
internal readonly struct RecordingHandler : IActorHandler<int, int> {
	private readonly RecordingState _state;

	public RecordingHandler(RecordingState state) => _state = state;

	public void Handle(int key, int message) {
		if (_state.Throw?.Invoke(key, message) == true)
			throw new InvalidOperationException($"poison {key}/{message}");
		_state.Seen.Enqueue((key, message));
		Interlocked.Increment(ref _state.Handled);
	}

	public void OnError(Exception error) {
		_state.Errors.Enqueue(error);
		Interlocked.Increment(ref _state.Handled); // consumed, even though it threw
	}

	public void OnCompleted(Exception? error) {
		_state.CompletedWith.Enqueue(error);
		Interlocked.Increment(ref _state.CompletedCalls);
	}

	public void OnSlotReused(ref int slot) => Interlocked.Increment(ref _state.SlotReuseCalls);

	public void OnChunkReclaimed(Span<int> chunk) => Interlocked.Increment(ref _state.ChunkReclaimCalls);
}
