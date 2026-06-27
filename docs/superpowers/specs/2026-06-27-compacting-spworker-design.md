# CompactingSpWorker — design

Date: 2026-06-27
Status: approved design, pre-implementation
Scope: **sync, reference-value variant only** (`CompactingSpWorker`). Async / value-struct variants
are explicitly out of scope for v1.

## Summary

A new member of the SPSC worker family that **conflates by key**: the producer dispatches
`(key, value)` pairs; if a value with the same key is still pending in the queue, the new value is
combined into the queued one **in place, keeping its original queue position**, via a handler
`Merge(current, incoming)` hook (default usage = last-wins replace). The consumer sees, per key, the
**latest merged** value computed before it was processed — never a stranded newer value.

This is a *conflation / coalescing queue* (LMAX/Aeron territory), not a variant of the FIFO ring. It
reuses the worker shell (dedicated thread, lifecycle, wake/backpressure primitives, devirtualized
struct handler) but needs its own core, because the producer writes a slot inside `[head, tail)` —
something the strict-FIFO `SpRingCore` invariant forbids.

## Goals / non-goals

Goals:
- `Dispatch(TKey key, TValue value)`; same-key, still-pending → in-place value override, original
  position preserved.
- **Guaranteed-latest delivery**: the newest value for a key is always delivered. The only tolerated
  artifact is an occasional **duplicate** (older value delivered, then the newer one) — never a lost
  newest value.
- Zero per-dispatch allocation in steady state; O(1) dispatch.
- Reuse the existing worker shell and conventions; do not destabilize the four FIFO workers.

Non-goals (v1):
- Async (`AsyncCompactingSpWorker`) and zero-copy value-struct variants.
- Struct (`TValue : struct`) payloads — see "Why `TValue : class`".
- Multi-producer. `Sp` = single-producer contract, same as the rest of the family.

## Public surface

```csharp
public interface ICompactingWorkerHandler<TKey, TValue> {
  // Conflate an incoming value into the one already queued for this key. Returns the value to
  // publish. Runs on the PRODUCER thread (see "Merge"). Must be pure / replace-style (see semantics).
  TValue Merge(TValue current, TValue incoming);

  void Handle(TKey key, TValue value);   // FIFO over first-seen key order, on the worker thread
  void OnError(Exception error);          // always synchronous; worker continues
  void OnCompleted(Exception? error);     // drain/shutdown hook (null / OCE / TryComplete exception)
}

public sealed class CompactingSpWorker<TKey, TValue, THandler> : IDisposable, IWorkerQueue
  where TKey : notnull, IEquatable<TKey>
  where TValue : class
  where THandler : struct, ICompactingWorkerHandler<TKey, TValue> {

  public CompactingSpWorker(THandler handler, int capacity, string? threadName = null);

  public Task Completion { get; }
  public int QueuedApprox { get; }   // distinct pending keys (Tail − Head)
  public int Capacity { get; }       // power-of-two, = max distinct pending keys

  public void Start(CancellationToken ct = default);
  public bool Dispatch(TKey key, TValue value);   // false if completed/disposed; blocks if full
  public bool TryComplete(Exception? exception = null);
  public void Dispose();
}
```

Mirrors `SpWorker` exactly except `Dispatch` takes `(key, value)`, the handler gains the key, and the
handler gains a `Merge` conflation hook.

### Merge (the conflation function)

When a dispatch finds a still-pending value for the same key, the new value is combined with the
queued one via `handler.Merge(current, incoming)` and the result is published in place — instead of a
blind last-wins overwrite. "React on replacement / keep latest" = `return incoming;` (plus any side
effect). "Combine fields / keep max / keep higher version" = compute and return the merged value.

Three hard contracts:

1. **Functional signature, not in-place mutation.** The override is published with a single atomic
   reference `Volatile.Write`, so the consumer never reads a torn value. `Merge` therefore *returns*
   the value to publish; it must not mutate the live `current` object's fields in place (the consumer
   may be reading it). Mutating-and-returning `incoming` (a value the consumer can't yet see) is fine
   and is the pooling-friendly path.
2. **Runs on the PRODUCER thread.** Unlike `Handle`/`OnError`/`OnCompleted` (consumer/worker thread),
   `Merge` runs on the producer thread — so the handler struct is touched by both threads. `Merge`
   **must be pure**: read only its two arguments, return a value, touch no mutable handler state.
3. **Replace-style / idempotent only (v1).** Each value `Merge` returns must be self-contained
   (authoritative on its own). The lossless protocol's doubtful-race fallback delivers `current` then
   `incoming` as two separate items; with replace-style values that's a harmless duplicate. **Additive
   / accumulate** merges (sum, append, count) are NOT safe here — a duplicate would double-count — and
   are deferred to a separate cell-based, take-and-reset-on-consume variant (see deferred items).

### Key identity & hashing

`TKey` is treated like a normal dictionary key: bucketed by `TKey.GetHashCode()`, tie-broken by
`IEquatable<TKey>.Equals`. **The handler does not supply a hash.** Two dispatches conflate iff their
keys are `Equals` — distinct keys that share a hash bucket are correctly *not* conflated (probe +
equality), so conflation is exact, never lossy.

Performance footnote: for **reference-type** keys, `GetHashCode()` is a virtual call per dispatch.
If profiling ever flags it, the escape hatch is an optional `where TKeyComparer : struct,
IKeyComparer<TKey>` type parameter (devirtualized hash/equals, the `ValueDictionary` pattern). Out of
scope for v1 — natural hashing keeps the API ceremony-free.

### Why `TValue : class`

In-place override means the producer writes `values[seq & mask]` while the consumer may concurrently
read that same slot. A single **reference** write is atomic, so the consumer reads either the old or
the new reference, never a torn value. A large struct would tear → restricted to reference values for
v1. (`TKey` has no such constraint: a slot's key is written once at fresh-enqueue and never
overwritten — conflation keeps the same key, only the value changes.)

## Architecture

```
CompactingSpWorker<TKey,TValue,THandler>   (public, sealed; owns the core + the struct handler)
        │ forwards Dispatch/Start/Complete/Dispose
        ▼
CompactingSpRingCore<TKey,TValue>          (internal, sibling to SpRingCore — NOT a modification of it)
        ├── TKey[]   _keys      ┐ two parallel ring arrays (power-of-two), so the consumer can
        ├── TValue[] _values    ┘ deliver (key,value) and override touches only _values[seq & mask]
        ├── PaddedIndices _idx  (reused: cache-line-separated Head/Tail, monotonic ints)
        ├── wake events / dedicated thread / lifecycle  (reused shape from SpRingCore)
        └── CompactingKeyMap<TKey>  _pending   (PRODUCER-THREAD-ONLY: TKey → seq)
```

Decision — **sibling core, not an extension of `SpRingCore`.** `SpRingCore`'s documented SPSC
invariant is "producer only writes `[tail, …)`, consumer only writes `[head, tail)`." The compacting
producer deliberately writes inside `[head, tail)`. To keep the four FIFO workers' invariant pristine
(per `Threading/CLAUDE.md`), the compacting behaviour lives in a separate core. Shared *shapes*
(`PaddedIndices`, edge-triggered `ManualResetEventSlim` wake/space events, the dedicated-thread
lifecycle, `WaitForItemSlow`/`WaitForSpaceSlow`, cancel/complete) are copied/adapted, not entangled.

### `CompactingKeyMap<TKey>` (producer-private)

A **purpose-built** open-addressing, linear-probe map `TKey → int seq`, ArrayPool-backed —
**inspired by** Prague's `ValueDictionary` but not a port (decided: a lean tailored map, not a copy of
the general-purpose collection).

- Layout: `TKey[] _keys` + `int[] _seqs` + `int[] _metadata` (probe slot → entry index), power-of-two
  mask, `Unsafe.Add` probe loops — i.e. it **mirrors `ValueDictionary`'s proven hot-path structure**
  (metadata-as-index open addressing, compact-and-rebuild) so we inherit the battle-tested algorithm.
- Members: `ref int GetSeqRefOrAdd(TKey key, out bool exists)` (get-or-add, writable ref to the seq);
  `Compact(predicate)` (`Filter`-style O(capacity) compact-and-rebuild evicting entries whose
  predicate is false → drops stale, already-consumed keys). Nothing else — none of `ValueDictionary`'s
  `Intersect`/`Crop`/`Sort`/`ExtractValues`/window-slicing surface.
- Deliberately **not** ported because: we need ~3 of its ~15 members; it uses `Memory<T>` struct
  fields (house-rule smell) where we use plain arrays + local spans; it requires an `IKeyComparer<TKey>`
  where we use natural hashing (`IEquatable` + `GetHashCode`); and our value is always `int`. A
  cross-repo copy would drift anyway, so we own a small tailored map (~120 lines vs ~400).
- Re-authoring the probe/compact logic means it gets **standalone unit tests** (add/lookup, probe
  collisions, compaction correctness, zero steady-state alloc).
- Sized to comfortably exceed ring capacity (at most `capacity` keys are ever pending; stale entries
  are evicted on fill). Touched **only by the producer thread** → no synchronization, single-writer.

`seq` is the **absolute monotonic** `Tail` value at the slot's enqueue (the ring already keeps `Head`
/ `Tail` as monotonic ints, wrapping only at `& _mask`). All seq/head/tail comparisons use wrap-safe
`int` subtraction, exactly like the existing core's `Tail − Head` idiom.

### Reaching `Merge` from the core

The map + conflation logic live in the (THandler-erased) core, so it needs to call back into the
THandler-typed `Merge`. **Decided: trampoline** (mirrors the existing `SlotCleaner<T>` pattern) — a
`MergeInvoker<TValue>` struct holding the worker as `object` + a `delegate*` to a static trampoline
that `Unsafe.As`-es it back to the closed worker and calls `_handler.Merge`. One indirect call, but
only on a conflation *hit* (not on fresh enqueues). Revisit only if benchmarks show it on the
conflation-hit path; the worker-hosted fully-devirtualized alternative stays in reserve.

## Dispatch protocol (producer) — the crux

`pending(seq)` ≜ `(seq − head) >= 0 && (tail − seq) > 0` (wrap-safe).

```
bool Dispatch(TKey key, TValue value):
    if (_completed || _stop) return false
    ref int slotSeq = _pending.GetSeqRefOrAdd(key, out exists)

    if (exists):
        int seq = slotSeq
        int head = Volatile.Read(ref _idx.Head)
        if (pending(seq)):
            // conflate: merge incoming into the queued value, publish the result atomically (release)
            TValue current = Volatile.Read(ref _values[seq & _mask])
            TValue merged  = _handler.Merge(current, value)   // PRODUCER-thread, pure, replace-style
            Volatile.Write(ref _values[seq & _mask], merged)
            Interlocked.MemoryBarrier()                       // StoreLoad fence
            if ((Volatile.Read(ref _idx.Head) - seq) < 0)     // consumer strictly behind → it WILL read merged
                return true
            // else consumer may be at/past the slot → fall through and enqueue `value` fresh.
            // Fresh-enqueue uses the un-merged `value` (not `merged`): if the consumer already took
            // `current`, delivering `value` standalone is correct for replace-style; `merged` could
            // re-deliver `current`'s content. (This is exactly why additive merge is unsafe here.)

    // fresh enqueue: new key, stale entry, or doubtful override
    if (!TryReserve(out int tail)) return false               // blocks if full; false if completed/stopped
    _keys[tail & _mask]   = key
    _values[tail & _mask] = value
    slotSeq = tail                                            // record / refresh map entry for this key
    CommitTail(tail)                                          // publish Tail + edge-signal consumer
    return true
```

### Why this is lossless (the ordering argument)

Shared locations: the value slot and `_idx.Head`.

- **Producer override:** store(slot) → `MemoryBarrier` → load(head). Enqueues a fresh slot whenever
  the recheck shows `head − seq >= 0` (consumer has reached/passed the slot).
- **Consumer:** acquire-read(slot) → advance(head) (read-then-advance; the ring contract is
  respected — the consumer reads the slot it owns at `head`, then frees it).

Two cases, both safe:
1. Recheck sees `head − seq < 0` (consumer strictly behind): the producer's release store of the
   **merged** value happens-before the consumer's later acquire read of that slot → consumer reads the
   merged value. (Value slot uses `Volatile.Write`/`Volatile.Read` so this holds without relying on
   the `Tail` barrier, which a conflated override does **not** move.)
2. Recheck sees `head − seq >= 0` (consumer at/past the slot — may already have taken `current`):
   the producer **enqueues a fresh slot** with the un-merged `incoming` value → newest delivered
   (possibly after a duplicate older delivery). Safe because merge is replace-style.

So the newest value is always delivered via case 1 or case 2; the only artifact is a duplicate
(older then newer) under the exact at-`head` race. This is the same Dekker store/load-fence pattern
already documented in `SpRingCore` (`CommitTail` async branch) and `AsyncAutoResetEvent`.

> The earlier-considered "advance head, then read slot" consumer ordering is **rejected**: it would
> win Dekker on paper but break the ring contract (reading a slot after freeing it) and open a
> slot-reuse aliasing window. Read-then-advance + producer-enqueue-fresh-on-doubt is the chosen,
> contract-preserving form.

## Consumer

Same dedicated-thread run-claim drain as `SpWorker.RunLoop` (`TryClaimRun` per run +
per-item advance), with one change: the value is **acquire-read** (`Volatile.Read(ref
_values[head & _mask])`) before delivery, because a conflated override publishes a new value
**without** advancing `Tail`, so the normal `Tail`-acquire in `TryClaimRun` doesn't cover it.

Per item: acquire-read key+value → advance `Head` (+ edge-signal a blocked producer, as today) →
`handler.Handle(key, value)` (wrapped in the existing try/catch → `OnError`). Slot clearing: null the
consumed `_keys`/`_values` slot (reference types — don't pin) after the handler runs. The map is
**never** touched by the consumer.

## Capacity, backpressure & eviction

- Capacity (power-of-two) = **max distinct pending keys**. Each pending key occupies exactly one slot,
  so "ring full" (`tail − head == capacity`) means `capacity` distinct keys are pending. Producer
  blocks on full via the existing `WaitForSpaceSlow` — identical backpressure to `SpWorker`.
- The map accumulates entries for keys that were consumed and not re-dispatched. When it approaches
  its load-factor limit, the producer runs `Compact`, keeping only `pending(seq)` entries. Amortized
  O(capacity), entirely producer-side. (A re-dispatch of an existing key reuses its map slot via the
  `GetSeqRefOrAdd` ref — only genuinely new keys consume map slots.)

## Lifecycle

Unchanged from `SpWorker`: `Start(ct)`, `TryComplete(ex)` (soft stop → drain), `Dispose`/cancel
(hard stop → drop), `Completion` task, `OnCompleted` drain hook. Hard stop drops pending items; soft
complete drains them — same `_stop` / `_completed` semantics as the core today. DEBUG producer-overlap
guard applies to `Dispatch` (single-producer contract).

## Testing

- **Conflation correctness:** dispatch `(k, v1)…(k, vN)` faster than the consumer drains; assert the
  handler sees exactly the conflated latest per key (or latest + bounded duplicates), never a stranded
  newer value.
- **Merge hook:** `Merge` is invoked with `(queued, incoming)` on the producer thread and its result
  is what `Handle` delivers; a replace-style merge yields latest-wins, a field-combine merge yields
  the combined value; assert duplicates (race fallback) are still self-consistent (deliver `current`
  then `incoming`).
- **Ordering:** first-seen key order is preserved across conflation.
- **Lost-update stress:** high-churn single-producer flood with a deliberately slow consumer, using
  the `WorkerTestUtil.AwaitCompletionOrStall` / `AwaitProgress` watchdogs so a lost wakeup / lost
  update fails fast instead of hanging. Mirror the `AsyncAutoResetEvent` edge-gated stress style.
- **Backpressure:** producer blocks when `capacity` distinct keys are pending; unblocks as the
  consumer drains.
- **Map eviction:** dispatch many one-shot keys; assert the map compacts and steady-state allocation
  is zero.
- **Lifecycle:** `TryComplete` drains; `Dispose`/cancel drops; `OnCompleted` fires once with the right
  exception. Recorder pattern = struct handler wrapping a shared reference state object (mirror
  `SpWorkerTests`).
- **Build/verify:** `dotnet build -c Release` (net9.0 + net10.0, 0 warnings); `dotnet test -c Release`
  and `-c Debug` (overlap guard); benchmark vs a `ConcurrentDictionary`+`Channel` conflation baseline.

## Effort

Medium, comparable to one existing per-variant spec. Concentrated in two places:
1. The in-place override **race protocol** + its stress tests (med–high; the care goes here).
2. `CompactingKeyMap<TKey>` (med; port/trim of `ValueDictionary` to `TKey → int` + `Compact`).

Worker shell, handler interface, and consumer drain are low (reuse/mirror). The sibling core is
medium (copy/adapt shared shapes from `SpRingCore` without entangling its FIFO invariant).

## Open items deferred (not v1)

- `AsyncCompactingSpWorker` and zero-copy value-struct variants.
- Optional struct `TKeyComparer` for devirtualized hashing of reference keys.
- **Additive / accumulate merge** (sum, append, count): a separate variant with a per-key value cell,
  an identity element, and atomic take-and-reset on consume (so a re-delivery can't double-count).
  Materially different core from v1's in-place override.
