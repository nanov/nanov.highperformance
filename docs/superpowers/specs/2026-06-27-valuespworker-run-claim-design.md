# ValueSpWorker run-claim + blittable clear-skip — design

**Date:** 2026-06-27
**Scope:** `ValueSpWorker<T, THandler>` (zero-copy scope-based sync worker) and the shared
`HandleScope<T>.Release`. Follows the `SpWorker` run-claim change
(`2026-06-27-spworker-batched-drain-design.md`).

## Motivation

After `SpWorker` got the run-claim treatment (snapshot the available run once, drop the per-item
emptiness `_tail` read), apply the same to `ValueSpWorker`. Per item today, `RunLoop → TryClaim`
reads `_idx.Tail` for the emptiness check; `HandleScope.Release → AdvanceHead` reads it again for the
space-signal edge.

Crucially this is **not** the head-batching that was rejected for `SpWorker`: run-claim keeps
per-item head advance, so it does **not** conflict with `ValueSpWorker`'s early-release contract
(`HandleScope.Release` frees the slot to the producer while the handler is still running). That
conflict only applies to publishing head once per batch — which we are not doing.

## Changes

### 1. Run-claim in `RunLoop`

```csharp
while (_core.TryClaimRun(out var head, out var available)) {
    do {
        var scope = new HandleScope<T>(_core, head);
        try { _handler.Handle(ref scope); }
        catch (Exception ex) { _handler.OnError(ex); }
        finally { scope.Release(); }   // idempotent safety net; advances head per item
    }
    while (!_core.IsStopped && ++head != available);
}
```

`TryClaimRun` snapshots `available = _idx.Tail` once per run (parking when truly empty); the `do`
drains `[head, available)` scope-by-scope. `HandleScope.Release` still advances head per item (via
`AdvanceHead`), so the early-release contract and tight backpressure (`capacity + 1` in flight) are
unchanged. `IsStopped` is rechecked per item so a hard stop drops the rest of the run. Net per item:
one fewer `_idx.Tail` read (the emptiness check, now paid once per run); parking is also amortized
across a run for bursty traffic.

`do/while` vs `while`: `TryClaimRun` only returns true for a non-empty, not-stopped run, so the
first item is always valid to process; the `IsStopped` recheck sits at the bottom. A cancel racing
in mid-run processes at most one more item before exiting — harmless (cancel drop is best-effort).

### 2. Blittable clear-skip in `HandleScope.Release`

```csharp
public void Release() {
    if (IsReleased) return;
    IsReleased = true;
    if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
        _core._buffer[_head & _core._mask] = default;   // skip for a struct of primitives
    _core.AdvanceHead(_head);
}
```

`IsReferenceOrContainsReferences<T>()` is a JIT intrinsic folded to a constant per closed generic, so
the clear (and its write) vanishes for a blittable struct `T` (e.g. an all-`long` event) and is kept
for a struct that contains a reference, so the ring never pins it. This also benefits
`AsyncValueSpWorker`, which shares `HandleScope`.

## Dead-code prune

`SpWorker`/`ValueSpWorker` no longer use the old single-item consumer primitives. Removed
`SpRingCore.TryClaim` and `SpRingCore.TryDequeue` (both orphaned; `TryDequeue` had no callers,
`TryClaim` was called only by `TryDequeue`). Updated the three `Threading/CLAUDE.md` references
(fast-path list, the `_stop`-recheck example, the blocking-consumer line) to `TryClaimRun` /
`TryTakeAndAdvance`. `TakeAndAdvance` is now `private` (only `TryTakeAndAdvance` calls it).

## Test coverage

The existing `ValueSpWorkerTests` are the spec and all run through the new path — notably the
contract guards:

- `Early_Release_Unblocks_Producer_Before_Handler_Returns` — the early-release contract (head
  advances per item, producer unblocked while the handler runs). The reason run-claim is safe here.
- `Publish_Blocks_When_Full_Until_Consumer_Drains` — tight backpressure (`capacity + 1` in flight).
- `Edge_Triggered_Signaling_Survives_Boundary_Churn` (cap 1 & 2 ×50k),
  `Blocking_Producer_Is_Always_Re_Signaled_When_Space_Frees`, `Publish_Wakes_An_Idle_Parked_Worker`,
  `Complete_Drains_A_Full_Buffer` — lost-wakeup guards.
- Cancellation, completion, handler-throw, release-then-throw (idempotent Release), overlap guard.

Added `Ref_Containing_Struct_Slots_Are_Cleared_After_Drain` — a `WeakReference` proof that a
ref-containing struct `T` still has its slot cleared on `Release` (the guard for change #2; the
`int`-based tests only cover the blittable skip side).

Full suite: Release 82/82, Debug 83/83.

## Result

Same disposition as the `SpWorker` change: a safe, semantics-preserving simplification that strictly
reduces per-item work (one fewer contended-line read; park amortized across a run) while preserving
the early-release contract and tight backpressure. The delta is below this laptop's benchmark noise
floor; it stands on correctness + reduced work, not a demonstrable speedup.
