# SpRingCore throughput squeeze — design

**Date:** 2026-07-10
**Status:** approved design, pre-implementation
**Scope:** `SpRingCore<T>` + the four FIFO workers (`SpWorker`, `ValueSpWorker`, `AsyncSpWorker`, `AsyncValueSpWorker`). The keyed workers inherit core wins via their `SpRingCore<int>`; no keyed-worker-specific work.

## Goal and constraints

Squeeze the existing lock-step (latency-oriented) path for throughput. This is explicitly **not**
the consumer-lag throughput mode memory.md points at — the architectural ceiling identified there
(2026-06-28 entry) is accepted. Ground rules agreed with the owner:

- No public API changes; no new worker types or constructor options.
- Bounded **timing coarsening is allowed**: backpressure granularity and slot-release timing may
  coarsen within a bounded window. Correctness, FIFO ordering, the drain-on-complete /
  drop-on-stop contracts, the zero-alloc guarantees, and the SPSC signaling invariant are
  untouchable.
- Validation is **local best-effort** (macOS, no pinning): interleaved A/B runs of the perf-tests
  throughput harness + BenchmarkDotNet, relative deltas only. Small wins that can't be
  quantified locally may be kept on the standard-technique basis (as the padding and gating-cache
  work was), or dropped on complexity grounds — either way the outcome is recorded in memory.md.

## The remaining hot-path costs (what this attacks)

Per item today, even in the ideal streaming case:

| Site | Cost |
| --- | --- |
| `CommitTail` (producer) | volatile `Tail` write **+ cross-core acquire read of `Head`** for the empty-edge test; async branch adds a full fence |
| `AdvanceHead` (consumer) | volatile `Head` write **+ cross-core acquire read of `Tail`** for the full-edge test |
| every `_buffer[i & _mask]` | array bounds check (JIT can't prove `mask < length` across fields) |

The two cross-core reads are the producer↔consumer cache-line ping-pong feedback loop the
2026-06-28 memory.md entry identified as suppressing producer throughput.

## Design: three layers, three commits

Each layer lands as its own commit on a feature branch and is independently gated (build + full
test suite + stress harness + before/after perf numbers) before the next goes on. A layer that
destabilizes reverts cleanly.

### Layer 1 — codegen pass (semantics-preserving)

- **Bounds-check elimination** on ring slot access: replace `_buffer[i & _mask]` with
  `Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_buffer), (nint)(uint)(i & _mask))` in the
  inlined fast paths (`TryEnqueue`, `GetSlotRef`, `TakeAndAdvance`, `ClearAndAdvanceHead`).
  Safe by construction: `_mask = length − 1`, length a power of two, so the masked index is
  always in range.
- **Single stop-state read in `TryReserve`**: fold the `_completed || _stop` volatile-bool pair
  into one volatile `int _lifecycle` bitfield read on the producer fast path — **only if it falls
  out cleanly**; if it complicates lifecycle code, drop it (the win is one load).
- Explicitly out of scope: spin constants (`SpinThreshold`, `SpinWait(10)`) — unquantifiable
  locally, and blocking-vs-spin is settled ground (memory.md 2026-06-28).

### Layer 2 — waiter-flag signaling (removes both per-item cross-core reads)

Replace "read the other side's index to detect the wake edge" with **parked flags**: a side sets
its flag only when it enters its slow path; the opposite side signals only when the flag is up.
The flag lines change only on slow-path entry/exit, so the per-item read hits warm cache.

**Layout.** `PaddedIndices` grows to four fields, each on its own 128-byte stride:
`Head`, `Tail`, `ConsumerParked`, `ProducerParked` (explicit layout, size 640). Same shielding
rationale as today's padding.

**Producer (`CommitTail`)** — the `Head` read disappears:
- Sync branch: `Volatile.Write(Tail)` → read `ConsumerParked` → if set, `_itemEvt.Set()`.
  Still **no per-commit fence** (see safety argument).
- Async branch: `Volatile.Write(Tail)` → `Interlocked.MemoryBarrier()` (kept, unchanged
  rationale — the async consumer parks without spinning) → read `ConsumerParked` → if set,
  `_itemReadyAsync.Set()`.

**Sync consumer (`WaitForItemSlow`)**: set `ConsumerParked = 1` **before the spin phase**, clear
on every exit path. Spin / recheck / park loop otherwise unchanged.

**Async consumer (`WaitForItemAsync`)**: set `ConsumerParked = 1` before arming; the existing
arm-then-recheck stays verbatim (the `WaitAsync` CAS is the full fence ordering the flag write
before the final `Tail` recheck). `RunLoopAsync` clears the flag after the await returns.

**Producer mirror (`WaitForSpaceSlow` / `AdvanceHead`)**: `WaitForSpaceSlow` sets
`ProducerParked = 1` before its spin phase, clears on exit; `AdvanceHead` becomes
`Volatile.Write(Head)` → read `ProducerParked` → if set, `_spaceEvt.Set()`. The per-item `Tail`
read disappears.

**Safety invariant (replaces the edge-test wording): a signal may be spurious, never lost.**
Each side raises its flag *before* its spin-guarded (sync) or fenced (async) final recheck of the
other index. The fence-free sync argument transfers from today's design and strengthens: the
hazard (a flag/index read hoisted ahead of the other side's store draining from the store buffer)
lasts nanoseconds; the flag is up for the whole microseconds-long spin phase before any kernel
park. Spurious `Set`s (producer committing several items while the consumer is still spinning)
are harmless — `ManualResetEventSlim.Set` is latching and cheap when nobody is kernel-blocked,
and the `AsyncAutoResetEvent` `Set` is a CAS. The conservative direction is always safe: a flag
up too long costs spurious signals; a flag down while parked would lose a wakeup and is
structurally excluded by raise-before-final-recheck.

**Unchanged:** `SignalStop` / `TryComplete` still `Set` everything unconditionally (flags never
gate shutdown); soft-complete's re-read-after-`_completed` rules; arm-then-recheck;
`AsyncAutoResetEvent` internals (not touched at all).

### Layer 3 — batched head publish (uses the timing allowance; depends on layer 2)

While draining a claimed run, the consumer keeps a private head cursor instead of publishing per
item. `AdvanceHead` splits into a deferred `ReleaseSlot` (auto-release path) and `PublishHead`
(flush: volatile `Head` write + `ProducerParked` check/signal). Flush triggers:

1. **Run end** — always, before re-claiming or parking.
2. **Every K items** — K = `min(32, capacity / 4)`, tuned once with the perf harness.
3. **`ProducerParked` observed up** — publish immediately (warm-cache check, layer-2 synergy:
   batch when nobody waits, eager when someone does).

Slots are still **cleared per item** at release (GC / zero-copy contract unchanged; the producer
can't reach a slot until `Head` is published, so deferred publish never exposes an uncleared slot).

**Hazard rules:**

- *Deadlock (both sides parked on stale state):* the consumer **always flushes its pending head
  before any park and at every run boundary** (`WaitForItemSlow`, `WaitForItemAsync`, loop
  exit). An empty ring therefore implies everything is published — the deadlock is structurally
  impossible.
- *Slow-handler producer stall:* eager per-item release today bounds a blocked producer's wait to
  one slot-free. With batching, the worst case becomes **one handler duration** (producer parks
  while a slow handler holds up to K−1 unpublished slots). Mitigations:
  - the drain loops check `ProducerParked` **before each handler invocation** and flush if up;
  - `HandleScope<T>.Release` — the explicit "free this slot now" API — **always publishes
    immediately**, bypassing the batch. Only automatic loop releases batch.
  The coarsened bound is documented (XML doc + worker-tree CLAUDE.md).

Applies identically to the sync and async FIFO drain loops. `QueuedApprox` gets slightly staler
(already documented as approximate).

## Testing

Every layer, before the next starts:

- Existing suite green: `dotnet test -c Release` **and** `-c Debug` (producer overlap guard),
  both TFMs; the `AsyncAutoResetEvent` unit/stress/alloc-guard tests untouched and green.
- **New stress tests** (NUnit, `WorkerTestUtil.AwaitCompletionOrStall` watchdogs so hangs fail
  fast; house recorder pattern):
  - *Flag-protocol lost-wakeup stress:* ≥1M-item churn at capacity 1–2 (maximum park/unpark
    pressure), sync and async paths — a broken flag protocol hangs within seconds on Apple
    Silicon's weak memory model.
  - *Blocked-producer stress (layer 3):* tiny ring, full-speed producer, intermittently slow
    handlers; asserts progress (no deadlock) and that a mid-handler `HandleScope.Release`
    unblocks the producer promptly.
  - *Drain/drop contract with batched state:* soft-complete drains everything and hard stop
    drops, exercised with unpublished head state at the stop/complete instant.
- Zero-alloc guards unchanged: the async empty-wait stays allocation-free (existing per-cycle
  alloc guard covers it).

## Performance validation

- Per layer: perf-tests throughput harness + BenchmarkDotNet `*SpWorker*` filter, multiple
  interleaved A/B runs, **relative deltas only** — macOS numbers give direction, not headlines.
- Layers 2–3 attack the cross-core traffic directly and should show a visible throughput delta.
  A layer with no measurable win is kept or dropped on the standard-technique vs. complexity
  balance; the decision is recorded either way.

## Documentation deliverables

- Rewrite the SIGNALING INVARIANT write-up (`SpRingCore` XML doc) and the worker-tree
  `CLAUDE.md` for the flag protocol and batch-publish rules: "spurious never lost",
  publish-before-park, the one-handler-duration stall bound.
- One memory.md entry per layer: what was measured, what was decided, what not to re-litigate.

## Out of scope

- Consumer-lag / batch-accumulation throughput mode (separate future design per memory.md).
- Keyed workers (`ConflationSpWorker` / `CoalescingSpWorker`) beyond what they inherit from the core.
- Spin-constant tuning, `aggressiveSpin` changes, public API surface of any kind.
