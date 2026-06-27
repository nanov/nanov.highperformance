namespace Nanov.HighPerformance.Benchmarks;

using System;
using System.Threading;
using BenchmarkDotNet.Attributes;
using Nanov.HighPerformance.Threading.Workers;

// ================================================================================================
//  The two key-conflating workers on the same workload — Messages updates across KeySpace distinct keys
//  through one consumer, ConsumerWork modelling a lagging consumer (where conflation pays off):
//    • CoalescingSpWorker — combine, exactly-once; struct value (`Tally`), zero per-message allocation.
//    • ConflationSpWorker — last-wins override; reference value (`Box`), one heap alloc per dispatch
//      (inherent to TValue : class — what last-wins-over-references costs vs the struct coalescer).
//  They do different things (sum-per-key vs latest-per-key); the comparison is "which conflation strategy
//  is cheaper per message," not an apples-to-apples merge.
// ================================================================================================
[MemoryDiagnoser]
public class CoalescingWorkerBenchmarks {
	private const int Capacity = 32;

	[Params(100_000)]
	public int Messages;

	[Params(32, 64)]
	public int KeySpace;

	// Per-Handle consumer work (busy-spin iterations). 0 = overhead-bound (consumer keeps up); a non-zero
	// value models a lagging consumer where each delivered key pays a fixed cost. SpinWait keeps it
	// deterministic and fast.
	[Params(0, 200)]
	public int ConsumerWork;

	private struct Tally {
		public long Sum;
	}

	private sealed class Box {
		public long Value;
	}

	private readonly struct SumHandler : ICoalescingWorkerHandler<int, Tally> {
		private readonly int _work;
		public SumHandler(int work) => _work = work;
		public Tally Coalesce(ref Tally current, ref Tally incoming) {
			current.Sum += incoming.Sum;
			return current;
		}
		public void Handle(int key, Tally value) {
			if (_work != 0) Thread.SpinWait(_work);
			_ = value.Sum;
		}
		public void OnError(Exception error) { }
		public void OnCompleted(Exception? error) { }
	}

	private readonly struct LastHandler : IConflationWorkerHandler<int, Box> {
		private readonly int _work;
		public LastHandler(int work) => _work = work;
		public void Handle(int key, Box value) {
			if (_work != 0) Thread.SpinWait(_work);
			_ = value.Value;
		}
		public void OnError(Exception error) { }
		public void OnCompleted(Exception? error) { }
	}

	[Benchmark(Baseline = true)]
	public void CoalescingSpWorker() {
		using var w = new CoalescingSpWorker<int, Tally, SumHandler>(new SumHandler(ConsumerWork), Capacity);
		w.Start();
		for (var i = 0; i < Messages; i++)
			w.Dispatch(i % KeySpace, new Tally { Sum = i });
		w.TryComplete();
		w.Completion.Wait();
	}

	[Benchmark]
	public void ConflationSpWorker() {
		using var w = new ConflationSpWorker<int, Box, LastHandler>(new LastHandler(ConsumerWork), Capacity);
		w.Start();
		for (var i = 0; i < Messages; i++)
			w.Dispatch(i % KeySpace, new Box { Value = i });
		w.TryComplete();
		w.Completion.Wait();
	}
}
