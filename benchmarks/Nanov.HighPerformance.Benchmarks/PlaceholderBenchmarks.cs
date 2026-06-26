namespace Nanov.HighPerformance.Benchmarks;

using BenchmarkDotNet.Attributes;

// Placeholder so the harness compiles and runs. Replace with real hot-path
// benchmarks ([MemoryDiagnoser] + [Benchmark(Baseline = true)] on the managed
// baseline) as components land.
[MemoryDiagnoser]
public class PlaceholderBenchmarks {
  [Benchmark]
  public int Noop() => 0;
}
