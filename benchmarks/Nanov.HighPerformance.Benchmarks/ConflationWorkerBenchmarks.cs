namespace Nanov.HighPerformance.Benchmarks;

using System;
using System.Collections.Concurrent;
using System.Threading.Channels;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Nanov.HighPerformance.Threading.Workers;

// ================================================================================================
//  ConflationSpWorker (key-conflating SPSC worker) vs the idiomatic "latest-per-key" baseline
//  (ConcurrentDictionary holding the latest value + a bounded Channel<int> of dirty keys). Pushes
//  Messages updates across KeySpace distinct keys through a single consumer that keeps only the
//  latest per key. The channel row is the baseline.
// ================================================================================================
[MemoryDiagnoser]
public class ConflationWorkerBenchmarks {
	private const int Capacity = 64;

	[Params(100_000)]
	public int Messages;

	[Params(64, 1024)]
	public int KeySpace;

	private sealed class Tick {
		public long Value;
	}

	private readonly struct DropHandler : IConflationWorkerHandler<int, Tick> {
		public void Handle(int key, Tick value) { _ = value.Value; }
		public void OnError(Exception error) { }
		public void OnCompleted(Exception? error) { }
	}

	[Benchmark(Baseline = true)]
	public async Task ConcurrentDictionary_Plus_Channel() {
		var latest = new ConcurrentDictionary<int, Tick>();
		var ch = Channel.CreateBounded<int>(new BoundedChannelOptions(Capacity) {
			SingleReader = true,
			SingleWriter = true
		});
		var consumer = Task.Run(async () => {
			await foreach (var key in ch.Reader.ReadAllAsync().ConfigureAwait(false))
				if (latest.TryRemove(key, out var t)) _ = t.Value;
		});

		for (var i = 0; i < Messages; i++) {
			var key = i % KeySpace;
			latest[key] = new Tick { Value = i };
			await ch.Writer.WriteAsync(key).ConfigureAwait(false);
		}

		ch.Writer.Complete();
		await consumer.ConfigureAwait(false);
	}

	[Benchmark]
	public void ConflationSpWorker() {
		using var w = new ConflationSpWorker<int, Tick, DropHandler>(new DropHandler(), Capacity);
		w.Start();
		for (var i = 0; i < Messages; i++)
			w.Dispatch(i % KeySpace, new Tick { Value = i });
		w.TryComplete();
		w.Completion.Wait();
	}
}
