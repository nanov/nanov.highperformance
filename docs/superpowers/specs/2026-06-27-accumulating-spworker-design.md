# AccumulatingSpWorker — design

Date: 2026-06-27
Status: approved design, pre-implementation
Scope: sync, **value-type accumulator** (`TValue : struct`), additive/combine conflation with
**exactly-once** delivery. Sibling to `CompactingSpWorker` (which is last-wins, `TValue : class`).

## Why this exists

`CompactingSpWorker` (in-place ring override) is correct only for **last-wins**: under the doubt race
it falls back to delivering the raw incoming, so any *combining* merge (additive, keep-max,
field-merge) can double-process or drop a contribution. `AccumulatingSpWorker` solves combining with
**exactly-once** by giving each key a **stable, lockable cell** that the consumer **takes-and-resets**:
the consumed contribution is removed atomically, so a producer accumulate can never re-include it.

## Core shape (zero allocation)

- **Cell** = `struct { TKey Key; TValue Acc; int State }`, stored **inline** in the map's pooled arrays
  and accessed by `ref`. No heap `Cell` objects, ever.
- **`AccumulatingKeyMap<TKey, TValue>`** — producer-private, **fixed-capacity, no-relocation**
  open-addressing (linear probe). A cell, once placed in a slot, **never moves**; eviction is an
  **in-place tombstone** (the slot is marked reusable). The **slot index is a stable handle.**
- **Dirty queue** = the existing **`SpRingCore<int>`** (FIFO SPSC ring) carrying **slot indices**.
- `TValue : struct` ⇒ the accumulator lives inline; the producer mutates it in place under the cell
  lock; the consumer's take is a **struct copy** off the stack. **No heap allocation anywhere** (cells
  inline in ArrayPool-backed arrays; the index ring is `int`).

Why a stable, no-relocation map: a `ref` to a struct cell (and a slot index) is only valid while the
backing array never moves the cell. A growable/compacting map would invalidate a live `ref`/index held
by a concurrent consumer. So: fixed capacity, in-place tombstone eviction, slot index as the handle.
This bounds *concurrently-dirty* keys to capacity (total distinct keys over time is unbounded —
tombstoned slots are reused). Dirty keys ≤ ring capacity anyway.

## Cell state machine

`State` (int) packs two bits, mutated only via `Interlocked`:
- `LOCK` (`0x1`) — held by whichever side is touching `Acc`.
- `QUEUED` (`0x2`) — this cell's index is currently in the ring.

Slot occupancy (empty / occupied / tombstone) is tracked in the map's **producer-only** metadata,
separate from `State`. The consumer never touches map metadata — it only reaches a cell by the slot
index it dequeued, and only touches `State`/`Acc`/`Key`.

## Handler

```csharp
public interface IAccumulatingWorkerHandler<TKey, TValue> where TValue : struct {
  void Accumulate(ref TValue accumulator, TValue incoming); // in-place, under the cell lock; producer thread
  void Handle(TKey key, TValue accumulated);                // the drained accumulation (struct copy); worker thread
  void OnError(Exception error);
  void OnCompleted(Exception? error);
}
```
`Accumulate` mutates `accumulator` in place (no return, no alloc). `default(TValue)` is the empty/
identity accumulator (no `CreateEmpty`). `Handle` receives a by-value copy and runs **outside** the
lock.

## Producer — `Dispatch(key, incoming)`

```
ref cell = map.GetOrAddRef(key, out var existing, out var index)   // producer-only structural op
if (!existing) { cell.Key = key; cell.Acc = default; cell.State = 0 }   // init fresh cell

// acquire the cell lock (spin CAS the LOCK bit; only contends with a concurrent consumer take)
var sw = default(SpinWait)
while ((Interlocked.CompareExchange(ref cell.State, locked, unlocked-preserving-QUEUED) ...) fails) sw.SpinOnce()

handler.Accumulate(ref cell.Acc, incoming)            // in-place merge
var needEnqueue = (cell.State & QUEUED) == 0
cell.State = (cell.State | QUEUED) & ~LOCK            // set QUEUED, release LOCK (single store; we own the lock)

if (needEnqueue && !ring.TryEnqueue(index)) {         // worker completed/stopped → roll back QUEUED
  // re-acquire lock, clear QUEUED, release
  return false
}
return true
```
Single producer ⇒ the only contender for the lock is the consumer's take (rare). For an existing key
already `QUEUED`, no enqueue (conflation never blocks); a new dirty key enqueues its index (blocks if
the ring is full = backpressure on distinct dirty keys).

## Consumer — run loop over the index ring

```
while (TryClaimRun(out head, out avail)):
  while (head != avail && TryTake(head++, out var index)):     // SpRingCore drains indices
    ref cell = map.RefAt(index)
    // acquire lock (spin CAS), then take-and-reset
    acquire(cell.State LOCK)
    var key = cell.Key
    var taken = cell.Acc                 // struct copy
    cell.Acc = default                   // reset in place
    cell.State = cell.State & ~(QUEUED | LOCK)   // clear QUEUED, release LOCK
    handler.Handle(key, taken)           // OUTSIDE the lock
```

## Why exactly-once

The cell lock makes producer `Accumulate` and consumer take **mutually exclusive**, so:
- the multi-field struct copy/merge is never torn;
- every contribution sits in `cell.Acc` until **one** take copies it out and resets to `default` — so
  it is delivered **exactly once**, never double-counted, never lost.

The `QUEUED` flag + the FIFO ring ensure a dirty cell is enqueued exactly once per dirty cycle: a
producer accumulate after a take finds `QUEUED == 0` and re-enqueues, so a contribution that arrives
just after a take is not stranded. A consume that finds `Acc == default` (a spurious re-dequeue) skips
`Handle`.

## Eviction

When `GetOrAddRef` needs a slot and the map is full, the producer tombstones a **drained** cell
(`State == 0`: unlocked and unqueued ⇒ the consumer is not touching it and its index is not in the
ring) and reuses its slot. Queued/locked cells are never evicted. Producer-only; no relocation.

## Lifecycle / errors / telemetry

Same as the family (reuses `SpRingCore`): `Start(ct)`, `TryComplete` (drains — delivers all dirty
cells through `Handle`), `Dispose`/cancel (hard stop — drops), `Completion`, `OnError` (continue),
`OnCompleted`, `IWorkerQueue` (`QueuedApprox` = dirty cells, `Capacity`).

## Testing

- Map: get-or-add ref, init-on-new, probe collisions, tombstone evict + slot reuse, stable index.
- Worker: additive accumulation correctness (sum of all dispatched == sum delivered, **exactly-once**)
  under a slow consumer / heavy conflation (the case `CompactingSpWorker` can't do); ordering of
  first-dirtied; backpressure on distinct dirty keys; drain-on-complete; cancel-drops; parked wake;
  `OnError`. Stress: high-churn small key space, assert per-key delivered-sum == sent-sum (no double,
  no loss), watchdog for hangs.
- Build/verify: `dotnet build -c Release` (net9.0 + net10.0, 0 warnings); `dotnet test -c Release` + `-c Debug`.

## Out of scope (v1)

Async variant; `TValue : class` accumulators (would need double-buffer/swap for an out-of-lock
`Handle`); pooling the index ring beyond what `SpRingCore` already does.
