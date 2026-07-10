# Nanov.HighPerformance — repo notes

High-performance, low-allocation concurrency building blocks for .NET. Zero per-message
allocation, lock-free where it counts, AOT-compatible. Multi-targets **net9.0** and **net10.0**.

## Layout

| Path | Purpose |
| --- | --- |
| `src/Nanov.HighPerformance.Threading.Workers` | SPSC worker family (the only shipped library). **Has its own `CLAUDE.md` with the hot-path invariants — read it before touching anything in that tree.** |
| `tests/Nanov.HighPerformance.Tests` | NUnit suite, runs on both TFMs. |
| `benchmarks/Nanov.HighPerformance.Benchmarks` | BenchmarkDotNet harness. |
| `perf-tests/…Workers.PerfTests` | Throughput / latency (HdrHistogram) harness. |

## Build / test / bench

```bash
dotnet build -c Release                 # both TFMs
dotnet test  -c Release                  # net9.0 + net10.0
dotnet run   -c Release --project benchmarks/Nanov.HighPerformance.Benchmarks -- --filter '*'
```

## Conventions (don't fight the build)

- **Central package management** — add/upgrade deps in `Directory.Packages.props` (`<PackageVersion>`), never inline `Version=` in a `.csproj`.
- **Central version** — bump `VersionPrefix` in `Directory.Build.props`; don't set versions per-project.
- `TreatWarningsAsErrors=true`, `Nullable=enable`, `ImplicitUsings=enable`, `AllowUnsafeBlocks=true`, `EnforceCodeStyleInBuild=true`. A warning fails the build; style is enforced from `.editorconfig`.
- Tests are **NUnit**.

## History

Decision & learnings log: **`./memory.md`** — read it before revisiting throughput, architecture,
or concurrency choices so we don't re-litigate settled ground. (Not auto-loaded; read on demand.)

## Writing code

- C# style (tabs width 2, file-scoped namespaces, K&R, `var`, …) → follow the **code-style** skill.
- Any hot-path / zero-alloc / lock-free work → follow the **high-performance-net** skill, and the worker-tree `CLAUDE.md` invariants take precedence in that subtree.
