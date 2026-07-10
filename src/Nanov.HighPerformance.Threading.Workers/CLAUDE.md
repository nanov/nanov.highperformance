# Threading.Workers — agent notes

Scope: the `Nanov.HighPerformance.Threading.Workers` library (its own project/assembly/package). The
public workers live at the project root; the internal cores are in [`Internal/`](Internal)
(`…Threading.Workers.Internal`). Read [`README.md`](README.md) for the user-facing API. This file is
the stuff to not get wrong when editing.

## The one rule: SPSC SIGNALING INVARIANT (parked-flag protocol)

The whole design is correct **only** under strict single-producer / single-consumer. Wakeups are
gated by **parked-waiter flags**, not by re-deriving the empty/full edge from the other side's
index: each side raises its flag (`_idx.ConsumerParked` / `_idx.ProducerParked`, each on its own
padded cache line) when it ENTERS its slow path — before the spin phase (`WaitForItemSlow` /
`WaitForSpaceSlow`) or before arming the async wait (`WaitForItemAsync`) — and clears it on exit
(sync: `finally`; async: `AckWake`, called by the loop once per wake). The opposite side signals
(`_itemEvt` / `_spaceEvt` / `_itemReadyAsync`) only when it sees the flag up — a warm-line read,
replacing the per-item cross-core read of the opposite index.

**A signal may be spurious, never lost.** The flag goes up *before* the waiter's final recheck of
the other index, and that recheck is ordered after the flag write — by an `Interlocked.MemoryBarrier()`
before every kernel park (cold, sync side) or by the `WaitAsync` CAS in arm-then-recheck (async
side). So either the committer sees the flag (and Sets — possibly spuriously while the waiter is
still spinning: harmless, the events latch), or the waiter sees the fresh index and never parks.
On the sync side (no per-commit fence in `CommitTail`), the consumer's ~200-iteration spin
re-reading the index is the load-bearing mask for the store-buffer window; the async consumer has no spin phase, so `CommitTail`'s async branch keeps its
per-commit `Interlocked.MemoryBarrier()` (StoreLoad — `Volatile` release/acquire does NOT order a
store before a subsequent load of a different location on ARM64/Apple Silicon).

Things that must stay exactly as they are:

- exactly one producer writes `_tail`; exactly one consumer writes `_head`; each flag is written
  only by its own side.
- `SignalStop` / `TryComplete` Set everything **unconditionally** — shutdown is never flag-gated.
- **Arm-then-recheck in `WaitForItemAsync`**: raise flag → `WaitAsync()` (CAS = full fence) →
  recheck `_tail`/`_stop`/`_completed` → self-`Set()` if anything appeared. Don't collapse it to a
  single check-then-wait.
- **Soft-complete drain** (`WaitForItemAsync` + `WaitForItemSlow`): when the ring looks empty,
  re-read `_tail` **after** observing `_completed` (published with release after the producer's
  last commit) — without the re-read a stale empty check silently drops the tail items. `_stop`
  (hard stop) still exits without draining — that's the drop-on-cancel contract.
- The pre-park fence + final recheck in both sync slow paths.

**A second concurrent producer, or advancing `_head` off the worker thread, breaks this and hangs
silently.** If you touch `SpRingCore` producer/consumer/signaling code, preserve this exactly. The
full write-up lives in the `SpRingCore<T>` XML doc. The regression net is
`SignalingStressTests` + `AsyncAutoResetEventTests` — run them after any change here.

- **Single consumer is structural** (the worker owns its one thread; `_head` only moves on it —
  including `HandleScope.Release`, which is a `ref struct` confined to the worker thread, called
  *before* an async handler returns its `ValueTask`).
- **Single producer is the caller's contract** — not enforceable in Release. The DEBUG-only overlap
  guard (below) catches violations in dev/test.

### Batched head publish (FIFO drain loops)

The four FIFO workers release slots in batches: the drain loop keeps a LOCAL head cursor and
publishes (`PublishHead`, absolute value) every `_flushThreshold` items
(`max(1, min(32, capacity/4))`), at every run boundary, and IMMEDIATELY when `ProducerParked` is
up (checked per item — warm line). Slots are still copied out / cleared per item (`TryTake` /
`ReleaseDeferred`). Rules that keep this safe — don't break them:

- **Flush at every run boundary, before every re-claim/park.** An empty ring must imply everything
  is published, or producer and consumer deadlock on stale state. `TryClaimRun` /
  `WaitForItemAsync` are only ever reached with the batch flushed.
- **`HandleScope.Release` publishes immediately** (it's the handler-facing "free this slot NOW"
  API); only the loops' automatic releases defer via the internal `ReleaseDeferred`. Worst-case
  producer wait on a full ring is one handler duration (pre-handler flag check), not one batch —
  the documented coarsening of the old per-item bound.
- **`PublishHead(n)` requires every slot below `n` to be cleared/copied out already.** The loops
  guarantee it by construction (clear-at-take / clear-at-deferral, in order).
- The keyed workers still use the per-item `TryTakeAndAdvance`/`AdvanceHead` path — don't move
  them to the batched API without revisiting their cell-state protocol.

Regression net: `BatchedReleaseTests` (+ `SignalingStressTests` for the park churn).

## Architecture (don't regress these)

- **Composition, not inheritance.** All machinery lives in internal sealed cores; the public workers
  *own* one and forward to it. The four FIFO workers own a `SpRingCore<T>`; `ConflationSpWorker` and
  `CoalescingSpWorker` are twins that each own a `SpRingCore<int>` dirty-index ring + a producer-private
  `CoalescingKeyMap` (conflation = last-wins overwrite + 3-state presence; coalescing = additive merge +
  4-state lock). Do **not** reintroduce public base classes — a public worker
  can't derive from an internal base (CS0060), and we deliberately keep the cores internal. Public
  surface: the six workers (the four FIFO + `ConflationSpWorker` + `CoalescingSpWorker`), their handler
  interfaces (the four `IWorkerHandler` + `IConflationWorkerHandler` + `ICoalescingWorkerHandler`),
  `PublishScope<T>`, `HandleScope<T>`, `IWorkerQueue`. Keep it tight.
- **Devirtualized struct handlers.** Workers are `…<T, THandler> where THandler : struct,
  I…Handler<T>`. Per-item `Handle`/`OnError`/`OnCompleted` go through the struct field so the JIT
  inlines them. Never store the handler as an interface reference or call it virtually.
- **Fast/slow split.** Core producer/consumer fast paths (`TryReserve`/`TryClaimRun`/`CommitTail`/
  `AdvanceHead`/`TryEnqueue`/`TryTakeAndAdvance`/`TryTake`/`PublishHead`/`ShouldFlush`) are
  `[AggressiveInlining]`; the parking loops
  (`WaitForSpaceSlow`/`WaitForItemSlow`) are `[NoInlining]`. Keep the hot path inlined and the cold
  park out of line.
- **Cache-line-padded indices.** `_head` (consumer) and `_tail` (producer) live in a
  `PaddedIndices` struct (`_idx.Head` / `_idx.Tail`) 128 bytes apart, so the two cores don't
  false-share a line every item. `PaddedIndices` is a **non-generic** top-level struct on purpose —
  generic types can't have `[StructLayout(LayoutKind.Explicit)]`. Don't fold the indices back into
  plain adjacent fields. (Local laptop benchmarks were too noisy — error > mean — to quantify the
  win; it's the standard Disruptor technique, correct and harmless, kept on that basis.)
- **Producer-cached gating sequence.** `TryReserve` checks fullness against `_cachedHead` (a
  producer-private lower bound on the consumer's `Head`) and only re-reads the real `_idx.Head` — one
  cross-core acquire — when the cache says "full"; `WaitForSpaceSlow` refreshes `_cachedHead` on exit.
  `Head` only advances, so `_cachedHead ≤ Head` always — it can only over-estimate fullness
  (conservative, never overwrites a live slot). Producer-only field ⇒ no synchronization. This is the
  LMAX gating-sequence technique and the producer-side mirror of the consumer's per-run `Tail` batch
  in `TryClaimRun`; it removes the per-enqueue read of the consumer's line while the ring isn't full.
  Don't reintroduce a direct `Volatile.Read(_idx.Head)` on the `TryReserve` fast path. (Same
  laptop-noise caveat as the padding above — kept on the standard-technique basis.)
- **Hard stop drops, soft complete drains.** `TryClaimRun` rechecks `_stop` *after* the wait loop
  even when the ring is non-empty (and `TryTakeAndAdvance` rechecks it per item mid-run) — that's
  what makes cancel/dispose drop queued items. Don't remove it (regression we already fixed once).
- **OnError is always synchronous**, on every variant including async ones. `OnCompleted` is the
  drain hook (sync for sync workers, `…Async` for async). Handler exceptions are reported to
  `OnError` and the worker continues.
- **Value-worker slot clearing is handler-driven, not `= default`.** The value handlers
  (`IValueWorkerHandler` / `IAsyncValueWorkerHandler`) carry a `void Clear(ref T value)` that runs on
  **every** slot release — early (`HandleScope.Release` called mid-`Handle`, e.g. to free the slot for a
  long-running task) *or* the loop's auto-release — via `SpRingCore.ClearAndAdvanceHead` (one call from
  `Release`: clear then `AdvanceHead`). It replaced the old whole-struct `slot = default` (a wide write
  that defeated the zero-copy point); the handler nulls just its reference fields / returns pooled
  buffers. Reaching the (THandler-typed) `Clear` from the THandler-erased core/scope is the job of the
  tiny `SlotCleaner<T>` struct (`Internal/SlotCleaner.cs`): a GC-tracked `object` holder (the worker) +
  a `delegate*` to a **static** trampoline the worker supplies (`&ClearTrampoline`, which `Unsafe.As`-es
  the holder back to the closed worker type and calls `_handler.Clear`). Nothing is a raw pointer into
  the GC heap, so it survives compaction; the only `unsafe` lives in `SlotCleaner` + each worker's
  one-line `MakeCleaner`. Copy workers never bind a cleaner (`Clear` no-ops) and never create a scope.
  Don't reintroduce the `IsReferenceOrContainsReferences<T>` `= default` clear.
- **Sync vs async use two different core paths — don't cross them.**
  - *Sync* (`SpWorker` / `ValueSpWorker`): dedicated-thread core ctor
    (`new SpRingCore<T>(capacity, threadName, runLoop)`), blocking run-claim consumer (`TryClaimRun`
    per run + `TryTakeAndAdvance` (copy) / `HandleScope.Release` (value) per item, `WaitForItemSlow`
    parking on `_itemEvt`).
  - *Async* (`AsyncSpWorker` / `AsyncValueSpWorker`): **ThreadPool** loop, no dedicated thread.
    `Start` launches `RunLoopAsync` (one `async Task`) via `Task.Run`, using the core's **no-thread**
    ctor (`new SpRingCore<T>(capacity)`), `TryClaimRunNow` (non-blocking run-claim — the no-park twin
    of `TryClaimRun`; drain `[head, available)` with `TryTake` (copy) / `HandleScope`+`ReleaseDeferred`
    (value) per item under the batched head publish, one `_idx.Tail` read per run, then re-claim), and
    `WaitForItemAsync`. (This replaced the
    old per-item `TryClaimNow`/`TryDequeueNow`, which read `_idx.Tail` every item; the run-claim mirrors
    the sync consumer's batching for the sync-completing-handler case.) The continuation resumes on the
    pool — no thread parked per `await` — so it
    matches/beats `Channel<T>` for truly-async serial handlers. The scope worker reads + `Release()`s
    the `HandleScope` **synchronously before any await** (ref struct can't cross await); `Dispose`
    `SignalStop()`s, `_loopTask?.Wait()`s, then `_core.Dispose()`. (We removed the old dedicated-thread
    async workers and their `_processDoneEvt` rendezvous — don't reintroduce them.)
  - *EC suppression:* `Start` wraps the `Task.Run` in `ExecutionContext.SuppressFlow()` so the loop
    runs with a clean EC — the start-caller's `AsyncLocal`/`Activity.Current` is captured once at
    `Start` and would otherwise wrongly wrap *every* handler invocation. Items are decoupled; per-item
    context belongs in the item, not ambient state. Keep the suppression.
- **Handler pooling (the real per-item async alloc).** A genuinely-suspending handler boxes its own
  per-item state machine — the dominant per-item allocation on the truly-async path. It's the
  consumer's code, so it's guidance, not a library change: handlers should put
  `[AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]` on the suspending method
  (benchmark: 103 KB/op → 1.5 KB/op). The benchmark's `PooledAsyncRefHandler` demonstrates it.
- **The empty-wait must stay allocation-free.** `WaitForItemAsync` is **non-`async`** on purpose: it
  returns a struct `ValueTask<bool>` (completed-true item available / completed-false stop-or-drained
  / the `AsyncAutoResetEvent`'s pending wait, result `true` = "woke" → the loop re-checks). The
  `AsyncAutoResetEvent` is a reused, **lock-free** `IValueTaskSource<bool>` (single state machine over
  one `_continuation` field + a sentinel; set on the empty→non-empty edge by `CommitTail` when
  `_itemReadyAsync != null`, and by `SignalStop`/`TryComplete`). Making `WaitForItemAsync` `async` again would box a state machine per
  empty wait — don't. The only per-loop allocation left is `RunLoopAsync`'s one-time box; per-item
  handler state-machine boxing (when the handler genuinely suspends) is inherent to async and the
  channel pays it too.
  - `AsyncAutoResetEvent` is **lock-free**: a single state machine over one `_continuation` field
    (sentinel `s_signaled`), resolved by `Interlocked.CompareExchange`, modeled on the BCL's
    `AsyncOperation` / `SocketAsyncEventArgs`. **Do not** go back to the earlier `MRVTSC` + external
    `_signaled`/`_waiting` latch: those are two state machines with a desync **seam** that lost a
    wakeup under cap-2 churn (consistent hang). The single-field design has no seam. It also
    implements `IThreadPoolWorkItem` and queues **itself** (reused) to resume the waiter, so the
    suspend→wake path allocates nothing (no per-wake delegate/work-item wrapper). Contract: **single
    serial waiter** + **multiple concurrent setters**; the resume runs on the ThreadPool without
    EC/scheduler capture (fine — the loop suppresses EC and uses `ConfigureAwait(false)`). Covered by
    `AsyncAutoResetEventTests` (unit + edge-gated 1M lost-wakeup stress + a per-cycle alloc guard);
    don't regress it.
    - **Claim-reset-to-null invariant:** every `Set` is delivered as either a **latch** or a
      **schedule** — never dropped. When `Set` claims a registered waiter it CASes `_continuation` back
      to **`null`** (not `s_signaled`) before scheduling, so a concurrent/subsequent `Set` sees `null`
      and latches a *fresh* `s_signaled` for the next cycle instead of collapsing into the one being
      delivered (the earlier "leave `s_signaled` until `GetResult` resets it" form dropped that
      concurrent setter's signal — a real lost-wakeup; `Second_Set_While_Claiming_A_Waiter_Survives_As_A_Latch`
      guards it). Consequently `GetResult` must **not** touch `_continuation` (it can't tell a leftover
      duplicate from a fresh concurrent latch). All transitions are `Interlocked.CompareExchange` —
      **no blind `Volatile.Write`** (weak-memory-safe). This fixes signal-*drop*; it does **not** fix a
      signal that's never *sent* — that's the StoreLoad fence work above, in `SpRingCore`.

## DEBUG-only producer overlap guard

`SpRingCore.DebugBeginProduce`/`DebugEndProduce` (`[Conditional("DEBUG")]` + bodies/field under
`#if DEBUG`) detect *overlapping* producers via an `Interlocked` flag — copy path wraps
`TryEnqueue`, scope path spans `Publish()`→`PublishScope.Dispose()`. It detects concurrency/overlap
(incl. two open scopes / a forgotten `using`), NOT thread identity (serial cross-thread is legal).
Compiles to nothing in Release. If you add producer entry points, guard them the same way.

## ConflationSpWorker (key-conflating worker) — invariants

`ConflationSpWorker<TKey, TValue, THandler>` conflates by key, **last-wins**: `Dispatch(key, value)`
overrides an already-pending value for the same key — a plain override, **no merge or drop callback** —
so the consumer sees only the latest value per key, in first-seen key order. It is the **last-wins twin
of `CoalescingSpWorker`** and reuses the coalescer's two structures verbatim. Things to not get wrong:

- **Twin on the coalescing infra — no bespoke core.** It owns a `SpRingCore<int>` (dirty-cell index
  ring) + a producer-private `CoalescingKeyMap<TKey, TValue>` (stable cells `{Key, Value, State}` reached
  by index), exactly like `CoalescingSpWorker`. The old `ConflationSpRingCore`/`ConflationKeyMap` (a
  two-array ring with an in-place `[head, tail)` value override + a Dekker `_reading` protocol) were
  **deleted** — they benchmarked 1.7–2.2× slower than the coalescer. Do **not** reintroduce a value-
  carrying override ring.
- **3-state presence, not a 4-state lock.** `State` ∈ `Free(0)`/`Available(1)`/`Consuming(2)`. There is
  **no `Merging`** — last-wins has no read-modify-write to serialize, so the producer never locks. Only
  `Free == 0` is "evictable" (the contract `CoalescingKeyMap.EvictFree` keys off); the other two are
  arbitrary non-zero. Don't add a `Merging` state or a `Coalesce` call.
- **`Value` is single-writer (the producer); the consumer only CASes it back to null.** Producer:
  `Volatile.Write(ref cell.Value, value)` (atomic ref overwrite) → `Interlocked.Exchange(ref cell.State,
  Available)`. If the exchange returns `Available` the index is already in the ring → conflated in place,
  done. Otherwise (`Free` or `Consuming`) the index is not in the ring → enqueue once. Consumer: CAS
  `Available→Consuming`, read `Value` (acquire — published before the producer's `Exchange`),
  **conditionally** `Interlocked.CompareExchange(ref cell.Value, null, taken)` to release the ref for GC
  iff the producer hasn't overwritten it since, then CAS `Consuming→Free`. The closing CAS **fails iff**
  the producer re-dirtied during the read (state is now `Available`), in which case a fresh ring entry
  already exists → leave the cell queued (latest redelivered). The `Interlocked` ops on `State` give a
  total order on both sides: losslessness (latest always delivered) with at most a harmless duplicate of
  the latest value — correct precisely **because** it's last-wins. Don't make the value clear
  unconditional (it would clobber a concurrent override → lost update).
- **Eviction is the coalescer's unchanged state-gated `EvictFree`** — producer-side, cold, tombstones
  only `Free` cells. While an index is in the ring (`Available`) or being read (`Consuming`), the cell is
  non-`Free`, so a handed-out index is never reused under the consumer (the slot-reuse race that killed a
  2-state dirty-flag attempt cannot occur — 1 bit can't distinguish "claimed" from "done"). The consumer
  touches only `State`/`Value`/`Key` by index via `RefAt`, never the map metadata.
- **Hard-stop guard before the map.** `Dispatch` bails on `_ring.IsStopped` *before* touching the map
  (Dispose/cancel tears the map down). The soft-complete path keeps the map valid and is rejected later
  by `TryEnqueue` (→ rollback `State→Free` → false).
- **`TValue : class`** is load-bearing: the overwrite and the null-clear are single atomic reference
  writes. A struct value would tear — don't relax the constraint without a new publish mechanism.

## CoalescingSpWorker (key-coalescing worker, exactly-once) — invariants

`CoalescingSpWorker<TKey, TValue, THandler>` folds values per key with **exactly-once** delivery — the
combine/additive sibling of `ConflationSpWorker` (last-wins). `Dispatch(key, value)` merges into the
key's stable cell via the handler's `Coalesce`; the worker thread takes-and-resets the cell and calls
`Handle` with the coalesced total. Things to not get wrong:

- **Two structures: cell map + dirty-index ring.** `CoalescingKeyMap<TKey, TValue>` (producer-private,
  fixed-capacity, no-relocation open-addressing; cell = `{Key, Value, State}` inline, reached by a
  **stable slot index**) holds the accumulators; a separate `SpRingCore<int>` carries the dirty cell
  indices (FIFO). Don't fuse them into a value-carrying ring — the in-place merge inside `[head, tail)`
  would reintroduce the producer/consumer override race (that path was tried as a fused ring and
  removed; it conflates too weakly anyway — per-batch, not global).
- **Per-cell 4-state lock is the exactly-once mechanism.** `State` ∈ `Free`/`Available`/`Merging`/
  `Consuming`, CAS'd. Producer-merge (`Merging`) and consumer-take (`Consuming`) are mutually exclusive,
  so a contribution sits in the cell until **one** take copies it out and resets to `default` — never
  double-counted, never lost. This lock — not any constraint on `TValue` — is what prevents torn reads,
  so `TValue` is **unconstrained** (value type ⇒ zero per-item alloc; reference type ⇒ the handler's
  `Coalesce` may allocate, the worker still doesn't).
- **`Coalesce(ref TValue current, ref TValue incoming) → TValue` runs on the PRODUCER thread under the
  cell lock.** Return-based: may fold `current` in place and return it, or return a fresh value. The
  worker stores the return into the cell. `default(TValue)` is the identity (null for a reference type).
- **Keys are producer-write-only.** The consumer touches a cell's `State`/`Value` only (by index via
  `RefAt`), never the map metadata, and **never clears the key** — keys are written by the producer
  (`Place`) and cleared producer-side (`EvictFree`), so the probe never races a consumer key-clear (the
  bug that bit the fused ring doesn't exist here). State-gated eviction (`EvictFree` only tombstones
  `Free` cells) means the producer never clears a cell the consumer holds.
- **`QUEUED`/first-dirty gating.** A cell's index is enqueued to the dirty ring only on the
  `Free → Available` (first-dirty) transition; re-dispatching an already-dirty key folds in place without
  re-enqueuing. Backpressure: a new dirty key blocks when `Capacity` keys are already dirty; conflating
  an already-dirty key never blocks. Hard stop drops queued cells; `TryComplete` drains them.

(We evaluated and **removed** a consumer-side-conflation `BatchAccumulatingSpWorker`: under a lagging
consumer it re-delivers each key once per batch — ~76–106× slower than this cell-map worker, which
conflates globally. Don't reintroduce it.)

## Naming

`Sp` = single-producer (the part the caller must uphold); single-consumer is structural, so it's
left out. Don't reintroduce the `Spsc` prefix.

## Tests & conventions

- NUnit, tabs (width 2), house style. Tests live in `tests/…/Threading/Workers/`.
- Recorders are **struct handlers wrapping a shared reference-type state object** (the struct is
  copied into the worker; observe results through the reference). See `SpWorkerTests` for the
  `Rec`/`RecordingState`/`RecordingHandler` pattern; mirror it.
- `WorkerTestUtil.AwaitCompletionOrStall` / `AwaitProgress` are stall watchdogs — use them for
  signaling/churn tests so a lost wakeup fails fast instead of hanging.
- The producer-guard test is `#if DEBUG` (it only throws in Debug). CI runs `-c Release`, so it's
  absent there; run `dotnet test -c Debug` to exercise it.

## Verify

- `dotnet build -c Release` → net9.0 + net10.0, 0 warnings (warnings-as-errors).
- `dotnet test -c Release` → full suite (guard compiled out).
- `dotnet test -c Debug` → includes the overlap-guard test.
- `dotnet run -c Release --project benchmarks/Nanov.HighPerformance.Benchmarks -- --filter '*SpWorker*'`.
