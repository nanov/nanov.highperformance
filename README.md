# Nanov.HighPerformance

High-performance, low-allocation concurrency building blocks for .NET. The current focus is the
**SPSC worker family** under `Nanov.HighPerformance.Threading` — bounded-mailbox actors / pipeline
stages backed by a power-of-two ring buffer, with **zero per-message allocation**, blocking
backpressure, drain-on-complete, and cancellation.

Targets **net9.0** and **net10.0**. AOT-compatible, `unsafe`-enabled, zero-allocation on hot paths.

## SpWorker — single-producer ring-buffer workers

An `SpWorker` is, in effect, *a bounded `Channel<T>` whose single consumer runs your handler* in
FIFO order. A producer hands items to the worker; the worker pulls them and invokes your handler.
Four variants cover sync/async × copy/zero-copy:

| Type | Handler | Producer API | Use when |
| --- | --- | --- | --- |
| `SpWorker<T, THandler>` | `IWorkerHandler<T>` | `Dispatch(T)` | sync handler, copy-based |
| `AsyncSpWorker<T, THandler>` | `IAsyncWorkerHandler<T>` | `Dispatch(T)` | async handler, copy-based |
| `ValueSpWorker<T, THandler>` | `IValueWorkerHandler<T>` | `Publish()` → `ref` | `T : struct`, zero-copy |
| `AsyncValueSpWorker<T, THandler>` | `IAsyncValueWorkerHandler<T>` | `Publish()` → `ref` | `T : struct`, zero-copy async |

`THandler` is a **struct** constrained to the handler interface, so per-item `Handle` / `OnError` /
`OnCompleted` calls devirtualize and inline — no virtual dispatch on the hot path.

```csharp
readonly struct PrintHandler : IWorkerHandler<int> {
  public void Handle(int item) => Console.WriteLine(item);
  public void OnError(Exception e) { /* log */ }
  public void OnCompleted(Exception? e) { }
}

using var worker = new SpWorker<int, PrintHandler>(new PrintHandler(), capacity: 1024);
worker.Start();

for (var i = 0; i < 1000; i++)
  worker.Dispatch(i);          // blocks if the ring is full (backpressure)

worker.TryComplete();          // no more items; drain then exit
await worker.Completion;       // resolves when drained (or faults / cancels)
```

The contract is **single-producer** (your responsibility — `Dispatch`/`Publish` must not run on two
threads concurrently; serial cross-thread is fine) and **single-consumer** (structural — the worker
owns its consumer). A `DEBUG`-only guard throws on overlapping producers; it compiles away in
`Release`.

See the [Threading concern README](src/Nanov.HighPerformance/Threading/README.md) for the full API:
zero-copy scope usage, async-handler rules, lifecycle (`Start` / `TryComplete` / `Dispose` /
`Completion`), error semantics, telemetry (`IWorkerQueue`), and performance notes.

## Layout

| Path | Purpose |
| --- | --- |
| `src/Nanov.HighPerformance` | The library (`Nanov.HighPerformance.Threading`). |
| `tests/Nanov.HighPerformance.Tests` | NUnit tests (run on both target frameworks). |
| `benchmarks/Nanov.HighPerformance.Benchmarks` | BenchmarkDotNet harness. |

## Build, test, benchmark

```bash
# Build for both target frameworks
dotnet build -c Release

# Run the test suite (net9.0 + net10.0)
dotnet test -c Release

# Run benchmarks (Release only)
dotnet run -c Release --project benchmarks/Nanov.HighPerformance.Benchmarks -- --filter '*'
```

## License

[MIT](LICENSE) © Dimitar Nanov
