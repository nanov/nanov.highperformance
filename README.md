# Nanov.HighPerformance

A collection of high-performance, low-allocation building blocks for .NET — pooled writers,
ref-struct collections, lock-free primitives, span/binary helpers, and friends. The pieces were
scattered across projects; this library consolidates them into one place with consistent
conventions, benchmarks, and tests.

Targets **net9.0** and **net10.0**. AOT-compatible, `unsafe`-enabled, zero-allocation on hot paths.

## Layout

| Path | Purpose |
| --- | --- |
| `src/Nanov.HighPerformance` | The library. |
| `tests/Nanov.HighPerformance.Tests` | NUnit tests (run on both target frameworks). |
| `benchmarks/Nanov.HighPerformance.Benchmarks` | BenchmarkDotNet harness. |

### Concerns (namespaces)

Each folder under `src/Nanov.HighPerformance` maps to one sub-namespace:

| Namespace | What lives here |
| --- | --- |
| `Nanov.HighPerformance.Buffers` | Pooled `IBufferWriter<byte>` writers, scratch buffers. |
| `Nanov.HighPerformance.Collections` | Inline-array lists, ref-struct collections. |
| `Nanov.HighPerformance.Memory` | `ArrayPool` helpers, native/unmanaged pools, span utilities. |
| `Nanov.HighPerformance.Text` | `Ascii` comparison helpers, UTF-8 encoding/interning. |
| `Nanov.HighPerformance.Threading` | Lock-free structures, async events, `ThreadStatic` caches. |
| `Nanov.HighPerformance.Binary` | `BinaryPrimitives` codecs, enum reinterpret helpers. |

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
