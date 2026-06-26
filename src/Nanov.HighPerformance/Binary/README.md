# Nanov.HighPerformance.Binary

Fixed-format binary reading and writing over spans.

- `BinaryPrimitives`-based big/little-endian codecs (no `BinaryReader`/`BinaryWriter`).
- Enum reinterpret helpers (`Unsafe.As` so the JIT folds the size switch per generic instantiation).
- Static-abstract encoder/decoder strategies, devirtualized per closed generic.

Namespace: `Nanov.HighPerformance.Binary`
