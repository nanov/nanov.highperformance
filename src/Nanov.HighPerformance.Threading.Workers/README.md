# Nanov.HighPerformance.Threading.Workers

The **SPSC worker family** — bounded-mailbox actors / pipeline stages backed by a power-of-two ring
buffer, with **zero per-message allocation**, blocking backpressure, drain-on-complete, and
cancellation. Six workers in one assembly:

| Worker | Shape | One-liner |
| --- | --- | --- |
| `SpWorker` | FIFO, sync, copy | dedicated-thread mailbox for a sync handler |
| `AsyncSpWorker` | FIFO, async, copy | ThreadPool loop for a truly-async serial handler |
| `ValueSpWorker` | FIFO, sync, zero-copy | `T : struct` written in place via a scope |
| `AsyncValueSpWorker` | FIFO, async, zero-copy | zero-copy struct mailbox, async handler |
| `ConflationSpWorker` | keyed, **last-wins** | latest value per key (skip stale) |
| `CoalescingSpWorker` | keyed, **exactly-once** | fold/sum per key (no loss, no double) |

Targets **net9.0** and **net10.0**. AOT-compatible, `unsafe`-enabled, zero-allocation on hot paths.

---

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
- Composition over an internal `SpRingCore<T>` — the cores are not part of the public API; only the
  six workers, their handler interfaces, `PublishScope<T>`/`HandleScope<T>`, and `IWorkerQueue`
  are public.

---

## ConflationSpWorker — key-conflating worker (last-wins)

A `ConflationSpWorker<TKey, TValue, THandler>` is an SPSC worker that **conflates by key, last-wins**:
you `Dispatch(key, value)`, and if a value with the same key is still queued, the new value **overrides**
the queued one **in place, keeping its original position** — a plain override, no merge or drop
callback. The consumer sees, per key, the **latest** value dispatched before it was processed — never a
stranded newer value. This is a conflation queue (LMAX/Aeron territory): exactly what you want for
"latest price per instrument", "newest state per entity", or any feed where a slow consumer should skip
stale updates rather than fall behind. (For *combining* per key — sum/merge with exactly-once — use
`CoalescingSpWorker`.)

It is the **last-wins twin of `CoalescingSpWorker`** and shares its two structures: a producer-private
map of stable per-key cells plus a `SpRingCore<int>` ring carrying dirty cell indices. The producer
overwrites a key's cell value (a single atomic reference write) and flips a 3-state presence flag; the
consumer drains the dirty ring, reading the latest value per cell. No bespoke core, no value-carrying
override ring.

### Contract & constraints

- Same single-producer (your responsibility, `DEBUG`-guarded) / single-consumer (structural) contract
  as the rest of the family; capacity rounds up to a power of two.
- `where TKey : notnull, IEquatable<TKey>` — keyed like a normal dictionary (its own `GetHashCode` /
  `Equals`); the handler supplies **no** hash. Distinct keys that share a hash bucket are *not*
  conflated.
- `where TValue : class` — values are references. A single reference write is atomic, so the cell
  overwrite is publish-safe with no torn read (this is what keeps the design lock-free and copy-free).
- **Capacity = max distinct *pending* keys.** Conflating an already-queued key never blocks; a *new*
  distinct key blocks when `Capacity` keys are already pending (backpressure).
- **Guaranteed-latest delivery.** The newest value per key is always delivered; the only artifact is a
  rare duplicate (older then newer) when a value is overwritten while its cell is mid-read — harmless
  because it's last-wins.
  (Sync, reference-value, last-wins only — async / value-struct variants are not in this version.)

### The handler

There is no merge or drop callback — conflation is an automatic override — so the handler is
consumer-side only:

```csharp
public interface IConflationWorkerHandler<TKey, TValue> {
  void Handle(TKey key, TValue value);   // worker thread, first-seen key order
  void OnError(Exception error);          // Handle threw — sync; worker continues
  void OnCompleted(Exception? error);     // drained & exiting
}
```

`Handle` receives, per key, the latest value queued for it (in first-seen key order). If you need to
release a *superseded* value (pooled / rented payloads), note that this worker does **not** surface the
dropped value — it overwrites the slot. For pooled payloads where every value must round-trip, either
keep the payload GC-managed, or use a design that delivers each value (e.g. a FIFO `SpWorker`).

### Usage

```csharp
sealed class Quote { public decimal Price; }

readonly struct LatestQuote : IConflationWorkerHandler<int, Quote> {
  public void Handle(int instrumentId, Quote q) => Publish(instrumentId, q.Price);
  public void OnError(Exception e) { /* log */ }
  public void OnCompleted(Exception? e) { }
}

using var w = new ConflationSpWorker<int, Quote, LatestQuote>(new LatestQuote(), capacity: 1024);
w.Start();

foreach (var tick in feed)
  w.Dispatch(tick.InstrumentId, new Quote { Price = tick.Price }); // same id still queued → overridden

w.TryComplete();
await w.Completion;
```

Lifecycle, errors, and telemetry (`IWorkerQueue`) are identical to the rest of the family.
Benchmarked against the idiomatic `ConcurrentDictionary`-latest + `Channel<key>` baseline it runs
~2–5× faster at ~0.4× the allocation, and at/below its coalescing twin on pure throughput.

---

## CoalescingSpWorker — key-coalescing worker (exactly-once)

A `CoalescingSpWorker<TKey, TValue, THandler>` is the **combine/additive sibling** of
`ConflationSpWorker`. Instead of last-wins override, it **folds** values per key with **exactly-once**
delivery: each key has a stable cell; the producer merges `incoming` into the cell via the handler's
`Coalesce`; the worker thread takes-and-resets the cell and calls `Handle` with the coalesced total.
A contribution is never double-counted and never lost — use it for "sum of deltas per account",
"merge of partial updates per entity", or any running fold where every input must be accounted for
exactly once.

### Contract & constraints

- Same single-producer / single-consumer contract; capacity rounds up to a power of two and equals
  the max distinct *pending* keys (a new dirty key blocks when full; folding an already-dirty key
  never blocks).
- `where TKey : notnull, IEquatable<TKey>` — keyed by its own `GetHashCode` / `Equals`.
- `TValue` is **unconstrained**. A per-cell 4-state lock (not any constraint on `TValue`) provides
  exactly-once, so `TValue` may be a **value type** (zero per-item allocation — cells live inline in
  pooled arrays, the dirty queue carries `int` indices) **or** a reference type (the handler's
  `Coalesce` may allocate; the worker itself still doesn't).
- `default(TValue)` is the **identity** (zero / null) — a freshly-taken cell starts from it.

### The handler

The handler adds one member over the conflation handler — `Coalesce`, the fold, which runs on the
**producer** thread under the cell lock:

```csharp
public interface ICoalescingWorkerHandler<TKey, TValue> {
  // Fold `incoming` into `current` and return the new accumulated value. May mutate `current` in
  // place and return it, or return a fresh value. Runs on the producer thread, under the cell lock.
  TValue Coalesce(ref TValue current, ref TValue incoming);
  void Handle(TKey key, TValue value);   // worker thread; the coalesced total for the key
  void OnError(Exception error);
  void OnCompleted(Exception? error);
}
```

### Usage

```csharp
struct Tally { public long Sum; }

readonly struct SumHandler : ICoalescingWorkerHandler<int, Tally> {
  public Tally Coalesce(ref Tally current, ref Tally incoming) {
    current.Sum += incoming.Sum;        // fold in place
    return current;
  }
  public void Handle(int key, Tally total) { /* total.Sum = exactly-once sum of deltas */ }
  public void OnError(Exception e) { }
  public void OnCompleted(Exception? e) { }
}

using var w = new CoalescingSpWorker<int, Tally, SumHandler>(new SumHandler(), capacity: 1024);
w.Start();

foreach (var d in deltas)
  w.Dispatch(d.AccountId, new Tally { Sum = d.Amount }); // folds into the account's cell

w.TryComplete();
await w.Completion;
```

### Conflation vs coalescing — which one?

| | `ConflationSpWorker` | `CoalescingSpWorker` |
| --- | --- | --- |
| Semantics | last-wins (latest per key) | combine/additive (fold per key) |
| Delivery | guaranteed-latest (rare harmless dup) | **exactly-once** |
| Handler | `Handle` only | `Handle` + `Coalesce` |
| `TValue` | `class` (atomic ref overwrite) | unconstrained (value type ⇒ 0 alloc) |
| Cell protocol | 3-state presence, no lock | 4-state lock |
| Use for | latest price/state, skip stale | running sums/merges, no loss |

Both share the same map + dirty-ring infrastructure; conflation is the slimmer last-wins
specialization. Lifecycle, errors, and telemetry are identical across the whole family.
