# Nanov.HighPerformance.Buffers

Output sinks and scratch buffers.

- Pooled `IBufferWriter<byte>` implementations (grow-by-doubling over a managed `byte[]`, native-pool-first hybrid writers).
- Per-type `[ThreadStatic]` writer caches with size-hint learning.
- Reusable scratch buffers for fixed-width primitive encoding.

Namespace: `Nanov.HighPerformance.Buffers`
