# SpWorker consumer optimization — design + findings

**Date:** 2026-06-27
**Scope:** `SpWorker<T, THandler>` (copy-based sync worker) only. `ValueSpWorker`, the async
workers, and `TryClaim`/`AdvanceHead` are untouched.

## Motivation

Carried over from the async-worker optimization pass. The one structurally-transferable lesson
(cache-line padding of the ring indices) the sync path already inherited via the shared
`SpRingCore<T>`. We set out to apply the LMAX Disruptor consumer trick — **batched drain** — to the
sync consumer, expecting to amortize the per-item volatile traffic.

Per item today, `RunLoop → TryDequeue` pays in `TryClaim` + `AdvanceHead`:

- `TryClaim`: `Volatile.Read(_idx.Tail)` — emptiness check.
- `AdvanceHead`: `Volatile.Write(_idx.Head, head+1)` + `Volatile.Read(_idx.Tail)` (space edge).

## What we tried, and why head-batching was abandoned

**Attempt 1 — naive head-batching** (publish `head` once at the end of a drained batch): **deadlocks.**
A handler that blocks mid-batch holds `head` back, so the ring stays full and the producer parks
forever. `SpWorker` frees each slot *before* running its handler (copy-out semantics); batching head
publication breaks that.

**Attempt 2 — copy-out then publish then handle** (copy the whole run into a side buffer, publish
`head` once, then run handlers): fixes the deadlock but **doubles in-flight capacity.** The consumer
drains the entire available run into the side buffer at once, so the producer can get up to
`~2×capacity` items ahead (ring + drain buffer) instead of the tight `capacity + 1`. This is a real
weakening of the bounded-backpressure contract — `Dispatch_Blocks_When_Full_Until_Space` caught it.

**Conclusion.** Head-batching fundamentally conflicts with `SpWorker`'s defining feature: *tight*
per-item bounded backpressure with eager slot-free. The head write **must** stay per item (each
write frees exactly one slot, keeping in-flight at `capacity + 1`); and with per-item head advance
the space-signal **must** also stay per item (the producer can park on any intermediate index, so a
batched signal risks a lost wakeup → hang). The async worker could batch because it makes no such
tight-bound promise. `SpWorker` is already well-matched to its contract.

## What we shipped

Two safe, contract-preserving changes:

### 1. Run-claim: drop the per-item emptiness `_tail` read

Snapshot the available run once per wake, then drain it item-by-item (head still advances per item
via `TakeAndAdvance`, so backpressure and the space edge stay exactly as before), re-claiming — and
only then re-parking when truly empty — once the run is exhausted.

```csharp
// SpRingCore<T>
internal bool TryClaimRun(out int head, out int available) {
    var tail = Volatile.Read(ref _idx.Tail);
    if (_idx.Head == tail) {
        if (!WaitForItemSlow()) { head = available = 0; return false; }
        tail = Volatile.Read(ref _idx.Tail);          // re-read after waking
    }
    if (_stop) { head = available = 0; return false; } // hard stop drops queued items
    head = _idx.Head;
    available = tail;
    return true;
}

internal T TakeAndAdvance(int head) {                  // copy out + (blittable-skip) clear + AdvanceHead
    ref var slot = ref _buffer[head & _mask];
    var item = slot;
    if (RuntimeHelpers.IsReferenceOrContainsReferences<T>()) slot = default!;
    AdvanceHead(head);                                  // per-item: eager free + space-signal edge
    return item;
}

internal bool TryTakeAndAdvance(int head, out T item) { // per-item: hard-stop check folded into the take
    if (_stop) { item = default!; return false; }
    item = TakeAndAdvance(head);
    return true;
}
```

```csharp
// SpWorker.RunLoop — head != available short-circuits before the take, so head++ never runs past the
// run bound; TryTakeAndAdvance returning false (hard stop) exits with the item untaken (the stray
// head++ is discarded — TryClaimRun re-reads the real _idx.Head next outer turn).
while (_core.TryClaimRun(out var head, out var available)) {
    while (head != available && _core.TryTakeAndAdvance(head++, out var item)) {
        try { _handler.Handle(item); }
        catch (Exception ex) { _handler.OnError(ex); }
    }
}
```

Net per item: **one fewer `_idx.Tail` read** (the emptiness check, now paid once per run). `_tail` is
the producer-written line, so the consumer's reads of it are coherence misses — halving them *may*
help under load. It also amortizes the park itself across a run, which helps bursty traffic. The
head write and the space-signal read/Set stay per item, so backpressure stays tight (`capacity + 1`
in flight) and no wakeup can be lost.

### 2. Skip the slot-clear for blittable `T`

`TakeAndAdvance` clears the drained slot only when `RuntimeHelpers.IsReferenceOrContainsReferences<T>()`.
The check is a JIT intrinsic folded to a constant per closed generic, so the clear (and its write)
vanishes for blittable `T` (e.g. `int`, a struct of primitives); reference / ref-containing `T` is
still cleared so the ring never pins it.

## Test coverage

All existing `SpWorkerTests` are the behavioral spec and pass through the new path — notably the
lost-wakeup guards (`Edge_Triggered_Signaling_Survives_Boundary_Churn` cap 1 & 2 ×50k,
`Blocking_Producer_Is_Always_Re_Signaled_When_Space_Frees`, `Dispatch_Wakes_An_Idle_Parked_Worker`,
`Complete_Drains_A_Full_Buffer`) and the backpressure-tightness guard
(`Dispatch_Blocks_When_Full_Until_Space`, which is exactly what rejected attempt 2). Two added:

- `Drains_A_Full_Ring_Burst_In_Order` — gate the consumer, fill the ring, release; full backlog
  drained in order with no lost wakeup.
- `Ref_Type_Slots_Are_Cleared_After_Drain` — `WeakReference` proof that reference `T` slots are still
  cleared (the guard for change #2; the `int` tests only cover the skip side).

Full suite: 81 Release / 82 Debug green (the one intermittent Debug failure is the pre-existing
`AsyncAutoResetEvent` edge-gated stress flake, unrelated to the sync path; passes 3/3 isolated).

## Result

`SpWorker_Value` (1M items): **42.43 ms** at cap 24, **40.10 ms** at cap 1024 — but with StdDev
**±3.7–5.6 ms** (~10–14%). A one-`_tail`-read-per-item change is sub-percent and sits far below this
laptop's noise floor, so a before/after comparison can't resolve it (same situation as the
index-padding change, where error > mean). Per-item allocation is 0; the reported KB/op is the
per-invocation ring buffer (one `T[capacity]` allocated per benchmark op), which scales with
capacity as expected.

**Disposition:** kept as a safe, semantics-preserving simplification that strictly reduces per-item
work (one fewer contended-line read; park amortized across a run) and preserves the tight
backpressure contract — not a demonstrable speedup on this hardware. The head-batching idea is
documented above as rejected, so it isn't retried later.
