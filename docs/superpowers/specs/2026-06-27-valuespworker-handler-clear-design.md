# ValueSpWorker handler-driven slot Clear — design

**Date:** 2026-06-27
**Scope:** `IValueWorkerHandler<T>` / `IAsyncValueWorkerHandler<T>`, the value workers
(`ValueSpWorker<T, THandler>` / `AsyncValueSpWorker<T, THandler>`), `HandleScope<T>.Release`, and the
shared `SpRingCore<T>`. **Supersedes change #2** of `2026-06-27-valuespworker-run-claim-design.md`
(the `IsReferenceOrContainsReferences<T>` blittable `= default` clear-skip).

## Motivation

On release, a ref-containing slot was cleared with a whole-struct `slot = default` write
(`HandleScope.Release`, guarded by `IsReferenceOrContainsReferences<T>`). That **defeats the point of
the zero-copy ring**: to drop one reference field it writes a full `default(T)` back into the slot — a
wide write for a large event struct, and it gives the consumer no say over *what* gets cleared (e.g.
returning a rented buffer to a pool instead of just nulling it).

Let the handler own the cleanup via a `void Clear(ref T value)` it implements. It must run **at the
moment of release** — including the **early release** path, which is a first-class feature: a
long-running handler calls `scope.Release()` to free the slot for the producer while it keeps working.
So the clear cannot be deferred to the worker loop, and it cannot be the handler's separate
responsibility — it has to fire from inside `Release`.

## The bridge problem

`HandleScope<T>` is generic over `T` only, and `SpRingCore<T>` is `THandler`-erased (it's shared with
the copy workers, where `T` may even be a reference type). Neither can name `THandler` to call its
`Clear`. Making `HandleScope` generic over `THandler` would push a self-referential generic into every
public `Handle(ref HandleScope<T, THandler>)` signature — the "generic noise" we explicitly reject.
And `Release` is reached from the handler's *own* early `scope.Release()` call, so the clear can't be
hoisted into the worker loop where `THandler` is known.

## Design

A managed function pointer + object holder, sealed inside a two-field `readonly struct SlotCleaner<T>`
(`Internal/SlotCleaner.cs`) so it's the **only** type that touches `unsafe`:

```csharp
internal readonly unsafe struct SlotCleaner<T> {
    private readonly object? _holder;                          // the worker, GC-tracked
    private readonly delegate*<object, ref T, void> _clear;    // → static trampoline (stable code addr)

    internal SlotCleaner(object holder, delegate*<object, ref T, void> clear) { _holder = holder; _clear = clear; }
    internal static SlotCleaner<T> NoOp => new(null!, &NoOpTrampoline);   // copy workers bind this

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Clear(ref T slot) { if (_clear != null) _clear(_holder!, ref slot); }

    private static void NoOpTrampoline(object holder, ref T slot) { }
}
```

The worker supplies the holder + a **static** trampoline that recovers the typed worker and forwards
to its handler — devirtualized, in-place on the `_handler` field:

```csharp
// ValueSpWorker<T, THandler> (mirror in AsyncValueSpWorker):
_core = new SpRingCore<T>(capacity, threadName, RunLoop, MakeCleaner());
private unsafe SlotCleaner<T> MakeCleaner() => new(this, &ClearTrampoline);
private static void ClearTrampoline(object holder, ref T slot) =>
    Unsafe.As<ValueSpWorker<T, THandler>>(holder)._handler.Clear(ref slot);
```

`SpRingCore` holds one `internal SlotCleaner<T> _cleaner` (taken in the ctor; copy-worker ctor
overloads default it to `SlotCleaner<T>.NoOp`) and exposes a single fused release call so
`HandleScope.Release` makes **one** core call:

```csharp
[MethodImpl(MethodImplOptions.AggressiveInlining)]
internal void ClearAndAdvanceHead(int head) {
    _cleaner.Clear(ref _buffer[head & _mask]);   // handler's Clear, or NoOp
    AdvanceHead(head);                            // advance + edge-signal a blocked producer
}

// HandleScope<T>.Release():
public void Release() {
    if (IsReleased) return;
    IsReleased = true;
    _core.ClearAndAdvanceHead(_head);
}
```

`Clear` runs on **every** release — the handler's early `scope.Release()` *or* the loop's idempotent
auto-release safety net (whichever fires first; the second is a no-op via `IsReleased`). `Event()`
slot access is routed through `SpRingCore.GetSlotRef(head)` (inlines to the same `ref _buffer[head &
_mask]`); the buffer/mask/indices and `AdvanceHead`/`CommitTail`/signaling are otherwise unchanged.

### Why this is safe and zero-overhead

- **GC-safe, no raw heap pointer.** `_holder` is an ordinary tracked `object`; `_clear` targets static
  code (never moves); the call passes the holder as a managed arg and `ref slot` as a managed byref.
  `Unsafe.As` only reinterprets the already-tracked reference. The unsafe-and-wrong alternative —
  `Unsafe.AsPointer(ref _handler)` on the movable heap field — is explicitly avoided.
- **State persists.** All four worker callbacks (`Handle`/`HandleAsync`, `Clear`, `OnError`,
  `OnCompleted[Async]`) dispatch through the non-readonly `_handler` field in place — never a copy — so
  a mutable struct handler can keep value-type state in its own fields. (The trampoline likewise calls
  `worker._handler.Clear(...)` in place.)
- **No `unsafe` leakage.** Holding a `SlotCleaner<T>` field needs no `unsafe` at the use site, so
  `SpRingCore` and `Scopes` stay unsafe-free; the only `unsafe` is `SlotCleaner` + each worker's
  one-line `MakeCleaner`. (`<AllowUnsafeBlocks>` is already on in `Directory.Build.props`.)
- **Cost.** `Release` trades the inlined conditional `= default` for one inlined, guarded indirect
  `calli` per release (the inner `_handler.Clear` inlines in the trampoline) — ~1ns over an interface
  call, immeasurable for the long-running early-release scenario this exists for; `Event()` is
  unchanged. Copy workers bind `NoOp` (and never create a `HandleScope` anyway).

## API change

`Clear(ref T value)` is a **required** member on `IValueWorkerHandler<T>` and
`IAsyncValueWorkerHandler<T>` (no default — a default interface method would box per call through the
struct generic constraint). Existing value handlers add it; a blittable `T` uses an empty body.

## Test coverage (TDD)

The spec is the `*ValueSpWorkerTests` suite. New tests, written for both the sync and async workers and
each verified RED by temporarily disabling `_cleaner.Clear` in `ClearAndAdvanceHead` (all failed for the
right reason — `ClearCount`/clear-count `0 ≠ n`, weak ref still alive — then GREEN once restored):

- `Clear_Runs_On_Early_Release` / `Clear_Runs_On_Auto_Release` (sync + async) — `Clear` runs exactly
  once per slot on both release paths, and sees a **live** reference (so it, not a library default-write,
  does the clearing).
- `Ref_Containing_Struct_Slots_Are_Cleared_After_Drain` (sync, updated to be handler-driven; async added)
  — `WeakReference` proof that a released ref-containing slot is collectable (ring doesn't pin it).
- `Handler_Struct_Value_State_Persists_Across_Calls` (sync + async) — a mutable struct handler's
  value-type counters accumulate across `Handle`/`Clear` and are read back in `OnCompleted[Async]` on the
  same instance; proves `_handler` is never copied per call. (Its handle-count assertion stayed green
  under the RED mutation, correctly isolating the no-copy property from the cleaner.)

Full suite: Release 89/89 per TFM; Debug 90/90 per TFM (the extra is the `#if DEBUG` producer-overlap guard).

> Note: the per-TFM test processes are serialized (`<TestTfmsInParallel>false</TestTfmsInParallel>` in the
> test csproj). Running net9.0 and net10.0 concurrently under CLI `dotnet test` could starve one host on a
> loaded box and trip a hard stall-watchdog timeout — a process-contention flake, not shared state (the
> workers have no shared mutable static state; every pipeline owns its `SpRingCore<T>`).

## Result

The zero-copy ring no longer writes a full `default(T)` on release; the consumer clears exactly what it
owns (null a field, return a pooled buffer), and it runs at the correct instant on both the early- and
auto-release paths — without leaking `THandler` generics into the public scope API or `unsafe` into the
core. Net per-release cost is one inlined indirect call, below this laptop's benchmark noise floor.
