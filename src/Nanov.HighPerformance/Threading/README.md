# Nanov.HighPerformance.Threading

Concurrency primitives. Today this concern contains the **SPSC worker family** under
[`Workers/`](Workers) — `Nanov.HighPerformance.Threading.Workers`.

## SpWorker — single-producer, dedicated-thread ring-buffer workers

An `SpWorker` is, in effect, *a bounded `Channel<T>` whose single consumer is a dedicated thread
running your handler* — a bounded-mailbox actor / pipeline stage. A producer hands items to the
worker; one dedicated background thread pulls them in FIFO order and invokes your handler. Backed
by a power-of-two ring buffer with edge-triggered `ManualResetEventSlim` signaling: **zero
per-message allocation**, blocking backpressure, drain-on-complete, and cancellation.

### The contract

- **Single consumer — enforced by design.** The worker owns the one consumer thread; nothing else
  can advance the read side. You never manage the consumer.
- **Single producer — your responsibility.** `Dispatch`/`Publish` must not be called by two threads
  *concurrently*. Different threads producing *serially* (with a happens-before edge between them —
  a lock, a queue, an `await`) is fine. In `DEBUG` builds an overlap guard throws if you violate
  this; in `Release` it compiles away.
- **Capacity** is rounded up to the next power of two.

### The four variants

| Type | Handler | Producer API | Use when |
| --- | --- | --- | --- |
| `SpWorker<T, THandler>` | `IWorkerHandler<T>` | `Dispatch(T)` | sync handler, copy-based |
| `AsyncSpWorker<T, THandler>` | `IAsyncWorkerHandler<T>` | `Dispatch(T)` | async handler, copy-based |
| `ValueSpWorker<T, THandler>` | `IValueWorkerHandler<T>` | `Publish()` → `ref` | `T : struct`, zero-copy |
| `AsyncValueSpWorker<T, THandler>` | `IAsyncValueWorkerHandler<T>` | `Publish()` → `ref` | `T : struct`, zero-copy async |

`THandler` is a **struct** constrained to the handler interface, so the per-item `Handle` /
`OnError` / `OnCompleted` calls devirtualize and inline — no virtual dispatch on the hot path. Put
mutable handler state in struct fields, or behind a reference the struct captures.

### Sync vs async workers

- **Sync** (`SpWorker` / `ValueSpWorker`): a dedicated background thread runs the handler. Pick these
  whenever the handler is synchronous.
- **Async** (`AsyncSpWorker` / `AsyncValueSpWorker`): the consume loop runs as an `async Task` on the
  ThreadPool, so a suspended handler's continuation resumes the loop *on the pool* — no thread parked
  per `await`. This matches (or beats) a `Channel<T>` consumer for **truly-async, one-at-a-time
  (serial, FIFO)** handlers, and handlers that complete synchronously still drain in a tight loop. The
  loop is allocation-free per item and per empty-wait; a handler that *genuinely* suspends boxes its
  own per-item state machine (inherent to async — a `Channel<T>` consumer pays the same).

All four are strictly single-consumer / FIFO with one item in flight.

The async loop runs with `ExecutionContext` flow **suppressed** — an `AsyncLocal` / `Activity.Current`
set on the thread that calls `Start` does **not** flow into the handler. That's deliberate: items are
decoupled from the start-time context, so per-item context (trace ids, etc.) belongs *in the item*,
not in ambient state.

**Hot async handlers:** the per-item box a genuinely-suspending handler allocates can be pooled to ~0
with `[AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]` on the suspending method —
e.g. `AsyncSpWorker_Ref` measures 103 KB/op vs 1.5 KB/op for the pooled variant in the benchmark.

### Handler interface

Every handler has three members; `OnError` is **always synchronous** (even on the async workers):

```csharp
public interface IWorkerHandler<in T> {
  void Handle(T item);              // FIFO, on the worker thread
  void OnError(Exception error);    // Handle threw — sync; worker continues to the next item
  void OnCompleted(Exception? error); // drained & exiting; null = normal, else cancel/fault
}
```

The async variants use `ValueTask HandleAsync(T, CancellationToken)` and
`ValueTask OnCompletedAsync(Exception?)`; the value variants use `Handle(ref HandleScope<T>)`.

### Usage — copy-based (sync)

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

### Usage — zero-copy scope (value)

Producers write **directly into the ring slot**; the handler reads through a `ref`:

```csharp
readonly struct SumHandler : IValueWorkerHandler<long> {
  public void Handle(ref HandleScope<long> scope) {
    ref var slot = ref scope.Event();
    // ... use slot ...
    scope.Release();           // optional: free the slot for the producer early
  }
  // Runs on every slot release (early Release() above OR the loop's auto-release): null the slot's
  // reference fields / return pooled buffers so the ring doesn't pin them. Empty for blittable T.
  public void Clear(ref long value) { }
  public void OnError(Exception e) { }
  public void OnCompleted(Exception? e) { }
}

using var worker = new ValueSpWorker<long, SumHandler>(new SumHandler(), 1024);
worker.Start();

using (var scope = worker.Publish()) {   // claims a slot
  if (scope.IsOpen)                       // false if the worker is completed/disposed
    scope.Event() = 42;                   // write in place — no struct copy
}                                         // Dispose publishes the slot + signals the consumer
```

For `AsyncValueSpWorker`, the handler must **not** be `async` (a `ref struct` can't cross an
`await`): read the slot + `Release()` synchronously, then return a `ValueTask` from a nested
`async` helper. See the XML docs on `IAsyncValueWorkerHandler<T>`.

### Lifecycle

- `Start(CancellationToken = default)` — starts the consumer thread. Cancelling the token hard-stops
  (drops pending items); `Completion` resolves as `OperationCanceledException`.
- `TryComplete(Exception? = null)` — soft stop: drain remaining items, then exit. A non-null
  exception faults `Completion`. Returns false if already completing.
- `Dispose()` — hard stop: drop pending items, join the thread (1s), release events.
- `Completion` (`Task`) — resolves once the worker has drained and exited.
- **Hard stop drops** queued items; **soft complete drains** them.

### Errors

A throwing `Handle`/`HandleAsync` is reported to the handler's synchronous `OnError` and the worker
**continues** with the next item. `OnCompleted`/`OnCompletedAsync` runs once on exit (use it to
propagate completion to a downstream stage in a pipeline).

### Telemetry

All workers implement `IWorkerQueue` — `QueuedApprox` (≈ `Tail − Head`, lock-free/racy) and
`Capacity` — so a gauge can sample queue depth without knowing `T`.

### Performance notes

- Zero per-message allocation after construction (the ring is allocated once).
- Devirtualized struct handlers; the producer/consumer fast paths are inlined, the cold parking
  loops are not (fast/slow split).
- Composition over an internal `SpRingCore<T>` — the bases are not part of the public API; only the
  four workers, the four handler interfaces, `PublishScope<T>`/`HandleScope<T>`, and `IWorkerQueue`
  are public.
