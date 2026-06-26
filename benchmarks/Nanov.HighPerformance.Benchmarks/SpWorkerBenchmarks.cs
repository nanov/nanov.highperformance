namespace Nanov.HighPerformance.Benchmarks;

using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Nanov.HighPerformance.Threading.Workers;

/// <summary>
///   Throughput / allocation comparison: pushing <see cref="ItemCount" /> items through a single
///   dedicated consumer. Baseline is a bounded <see cref="Channel{T}" /> with one reader task;
///   the comparison is <see cref="SpWorker{T, THandler}" /> (devirtualized struct handler, zero
///   per-item allocation after construction).
/// </summary>
[MemoryDiagnoser]
public class SpWorkerBenchmarks {
	private const int Capacity = 24;

	[Params(1_000_000)]
	public int ItemCount;

	// Empty-bodied handler: isolates the dispatch/consume machinery, not the work.
	private readonly struct CountingHandler : IWorkerHandler<int> {
		private readonly StrongBox _box;

		public CountingHandler(StrongBox box) => _box = box;

		public void Handle(int item) => _box.Sum += item;
		public void OnError(System.Exception error) { }
		public void OnCompleted(System.Exception? error) { }
	}

	// Mutable reference state so the (by-value) struct handler's accumulation is observable.
	private sealed class StrongBox {
		public long Sum;
	}

	[Benchmark(Baseline = true)]
	public async Task<long> BoundedChannel() {
		var channel = Channel.CreateBounded<int>(new BoundedChannelOptions(Capacity) {
			SingleReader = true,
			SingleWriter = true
		});

		long sum = 0;
		var consumer = Task.Run(async () => {
			var reader = channel.Reader;
			while (await reader.WaitToReadAsync().ConfigureAwait(false))
				while (reader.TryRead(out var item))
					sum += item;
		});

		var writer = channel.Writer;
		for (var i = 0; i < ItemCount; i++)
			await writer.WriteAsync(i).ConfigureAwait(false);
		writer.Complete();

		await consumer.ConfigureAwait(false);
		return sum;
	}

	[Benchmark]
	public long SpWorkerDispatch() {
		var box = new StrongBox();
		using var worker = new SpWorker<int, CountingHandler>(new CountingHandler(box), Capacity);
		worker.Start();

		for (var i = 0; i < ItemCount; i++)
			worker.Dispatch(i);

		worker.TryComplete();
		worker.Completion.GetAwaiter().GetResult();
		return box.Sum;
	}
}
