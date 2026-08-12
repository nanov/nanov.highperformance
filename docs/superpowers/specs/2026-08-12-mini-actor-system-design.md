# Mini actor system — design

**Status:** approved design, not yet implemented
**Date:** 2026-08-12
**Branch:** `experiment/mini-actor-system`

---

## 1. Goal

A key-affine, multi-worker message processor built on the `Nanov.HighPerformance.Threading.Workers`
mechanics. A single producer dispatches `(key, message)` pairs; N pre-spawned workers process them
across all available cores while preserving **strict FIFO order per key**.

The experiment's question is a comparative one:

> Given identical storage machinery, does **push-side scheduling** (a router assigning keys to
> workers) or **pull-side scheduling** (workers taking and stealing keys) balance load better?

Both schedulers are built. They differ by one type argument.

### Why not static hash sharding

Static sharding across N `SpWorker`s is free, contention-less, and order-safe — it is the baseline
every design here must beat. It loses on three counts the experiment targets: **key skew** (hot keys
permanently pin one worker), **variable per-item cost** (even key distribution, uneven work), and
**high key churn** (any static assignment is arbitrary). Dynamic ownership is what we are paying for.

## 2. Non-goals (v1)

- **Per-key state.** A key is an ordering token, not an actor with durable state. The cell persists
  across activations, so state plugs in later as a cell field touched only by the exclusive owner —
  no protocol change required.
- **Multiple producers.** Single producer, same contract as the rest of the family
  (`DEBUG`-guarded, not enforced in Release).
- **The shared-SPMC-queue design ("design C").** Parked as a bonus round; see §11.
- **Packaging / versioning / public API freeze.** Deferred until B-vs-D is measured.
- **Chained chunks ("v2").** v1 is a bounded single chunk per key; see §11.

## 3. Architecture

```
 producer ──▶ ┌──────────────────────────────────────────────┐
              │ KeyCellMap        (producer-private probe)    │
              │  cell = { Key, State, ChunkBase, Head, Tail } │
              └──────────────────┬───────────────────────────┘
                                 │ ChunkBase ──▶ contiguous slots
              ┌──────────────────▼───────────────────────────┐
              │ MessageSlab   T[] carved into fixed chunks    │
              │ + free stack of chunk indices                 │
              └──────────────────────────────────────────────┘

              scheduler carries KEY INDICES (int), never messages

              ┌── B: router ──▶ per-worker SPSC rings
              └── D: global submission queue + Chase-Lev deques + stealing
```

Three layers, cleanly separated:

| Layer | Responsibility |
| --- | --- |
| **Storage** | where a key's messages live (slab + chunks) |
| **Ownership** | which worker may process a key, and when (cell state machine) |
| **Scheduling** | which worker gets which key next (the pluggable seam) |

The ownership layer is what makes the scheduling layer free to be anything.

### 3.1 The load-bearing invariant

> A key is in exactly one of three places at any instant: nowhere (`Free`), one scheduler queue
> (`Scheduled`), or one worker (`Owned`). Never two. Its messages live in one ordered ring.

**Therefore the scheduler's own ordering discipline is irrelevant to correctness.** LIFO local
deques, stealing from either end, out-of-order global claims — all legal. This is the single
property that makes work stealing tractable here, and it is why the rejected design C is not:
there, execution order depended on *claim* order, which is not the same as take order.

## 4. Data structures

### 4.1 Message slab

One pre-allocated `T[]`, carved into fixed-size chunks (`chunkSize`, power of two). A chunk is
identified by an `int` index; slot `i` of chunk `c` is `_slab[c * chunkSize + i]`.

Free chunks live on a **Treiber stack** of chunk indices, ABA-safe via a packed
`{ index:32, tag:32 }` in a `long` resolved with `Interlocked.CompareExchange`.

- Producer rents a chunk when a key first goes dirty; pops the **whole stack** in one CAS into a
  private free list and allocates from it with zero atomics thereafter.
- Chunks are **returned by the producer**, not the worker — see §4.4. Cost is one stack push per
  key lifetime, not per message.

There is no second pool. "Renting a buffer" is claiming slots of the one slab; the free stack is
bookkeeping, not an allocator.

### 4.2 Key cell map

Open-addressing, fixed-capacity, **no relocation**, stable `int` indices — the `CoalescingKeyMap`
contract, with the accumulator replaced by mailbox cursors.

```csharp
struct KeyCell<TKey> {
  public TKey Key;      // producer-write-only
  public int State;     // Free | Scheduled | Owned | Reclaiming | Tombstone  — Interlocked
  public int ChunkBase; // producer-assigned; -1 = no storage bound
  public int Head;      // consumer cursor, absolute
  public int Tail;      // producer cursor, absolute
}
```

Access discipline — this preserves the existing family invariant:

| Field | Written by |
| --- | --- |
| `Key`, `ChunkBase` | producer only (probe / insert / evict) |
| `State` | producer + workers, always `Interlocked` |
| `Tail` | producer only |
| `Head` | the current owner only |

Workers never touch map metadata, so the probe path never races a key clear. Capacity should be
sized to *live* keys, not pending ones — see §4.4.

`Reclaiming` and `Tombstone` are **producer-only, cold-path** states (chunk harvest, LRU eviction).
Both are entered by CAS from `Free`, so a concurrent dispatch or worker reschedule simply loses the
CAS and the cold path skips that cell. Only `Free`, `Scheduled` and `Owned` appear on the hot path,
and the §3.1 invariant is stated over those three.

### 4.3 Per-key mailbox

A bounded SPSC ring over the key's chunk. `Head`/`Tail` are absolute and masked into the chunk;
occupancy is `Tail - Head`, full at `chunkSize`.

Deliberately **stripped of signaling** — no `ManualResetEventSlim`, no thread, no completion state.
Thousands of `SpRingCore` instances would be far too heavy. What is reused is the *mechanics*:
padded cursors, producer-cached gating sequence, `SlotRef` bounds-check elimination, batched head
publish. This is a new leaner type, not `SpRingCore<T>` verbatim.

**Consumer identity is not fixed.** Successive activations of the same key may run on different
workers. This is legal — serial-cross-thread with a happens-before edge, the same legality class as
the family's documented "different threads producing serially is fine". The edge is:
owner A's `Interlocked.Exchange(State, Free)` → scheduler queue transit (release/acquire) →
owner B's acquire. It is a **new** invariant for this machinery and gets an explicit code comment
plus a targeted test (§9).

### 4.4 Lifetime split — the rule

> **Storage becomes reclaimable as soon as a key drains; cells are evicted lazily.**

A cell is ~32 bytes; a chunk is `chunkSize × sizeof(T)`. Decouple their lifetimes:

- Key drains ⇒ `State = Free`, and the key's chunk is now **reclaimable**.
- Cell **stays in the map**, so the producer re-finds the key with no re-insert cost.
- Cell eviction is a cold LRU sweep, off the hot path.

**Who returns the chunk — the producer, via a hint queue.** `ChunkBase` is producer-write-only
(§4.2), so a worker must not null it. Instead the worker, on draining a key to empty, pushes the
cell index onto a **drain-hint ring** (MPSC, one push per key lifetime — negligible). The producer
drains that ring on its cold path (when its private free list runs dry, or during the LRU sweep),
and for each hinted cell re-validates under a `CAS(State, Reclaiming, Free)` that it is still
`Free` and still empty (`Tail == Head`) before nulling `ChunkBase` and pushing the chunk. A cell
that went dirty again fails the CAS and is simply skipped — the hint is advisory, never
authoritative.

This keeps the §4.2 access discipline intact (`ChunkBase` producer-only), and it removes the
eviction-vs-drain race entirely: eviction only ever touches cells that already hold no storage.
It is also what makes per-key state a later drop-in rather than a redesign.

A dropped hint costs nothing but a delayed reclaim, so the hint ring may be lossy — if it is full,
the worker drops the hint and the LRU sweep harvests that chunk later.

> **The reclaim revert must re-run the release check.** `Reclaiming` is a non-`Free` state, so a
> worker whose release-recheck lands in that window loses its `Free → Scheduled` CAS and does *not*
> schedule. If the producer then finds `Tail != Head`, reverts to `Free` and simply moves on, that
> key is **stranded** — messages queued, nobody scheduled. So the failed-validation path is not a
> plain revert: it must run the *same* recheck the worker runs (§5.3) — restore `Free`, and if
> `Tail != Head`, `CAS(State, Scheduled, Free)` and `Schedule(cellIdx)`. Same rule applies to a
> `Tombstone` attempt that loses validation. Factor it into one shared `ReleaseAndMaybeSchedule`
> helper used by both the worker release path and the producer cold paths; do not open-code it
> twice.

## 5. Protocols

### 5.1 Dispatch (producer)

```
cell = map.GetOrAdd(key)                        // producer-private probe
if (cell.ChunkBase < 0) cell.ChunkBase = RentChunk()      // may block: slab exhausted
while (cell.Tail - Volatile.Read(cell.Head) >= chunkSize) // may block: this key's ring full
    WaitForSpace(cell)
slab[cell.ChunkBase + (cell.Tail & mask)] = message
Volatile.Write(ref cell.Tail, cell.Tail + 1)              // release: publishes the slot
if (Interlocked.CompareExchange(ref cell.State, Scheduled, Free) == Free)
    scheduler.Schedule(cellIdx)                           // first-dirty gating
```

`Schedule` fires only on the `Free → Scheduled` edge — roughly **once per activation, not once per
message**. Under load a key accumulates many messages per activation, so scheduler traffic is a
fraction of message traffic. This is what makes B's single router thread viable.

### 5.2 Activation (worker)

```
cellIdx = scheduler.TryAcquire(workerId)
Interlocked.Exchange(ref cell.State, Owned)     // producer's Free→Scheduled CAS now fails harmlessly

var head = cell.Head;
var n = 0;
while (head != Volatile.Read(ref cell.Tail) && n < batchBound) {
    try { handler.Handle(cell.Key, slab[cell.ChunkBase + (head & mask)]); }
    catch (Exception ex) { handler.OnError(ex); }
    slab[...] = default;                        // or handler-driven Clear, per the family's pattern
    head++; n++;
    if (ShouldFlush(n)) Volatile.Write(ref cell.Head, head);   // batched publish + producer wake
}
```

**`batchBound` is required.** In-place drain is not self-limiting: under a firehose the producer
refills as fast as the worker drains, `Tail` never equals `Head`, and the worker is pinned forever.
(The rejected detach-a-snapshot design was self-limiting by construction; that property is lost and
the knob is the price.) This is exactly why Akka has a `throughput` setting.

### 5.3 Release — the crux

```
Volatile.Write(ref cell.Head, head);            // publish what we consumed
Interlocked.Exchange(ref cell.State, Free);     // ← FULL FENCE, deliberately not Volatile.Write
if (Volatile.Read(ref cell.Tail) != head) {     // producer appended while we were Owned,
    if (Interlocked.CompareExchange(              // or we stopped early on batchBound
            ref cell.State, Scheduled, Free) == Free)
        scheduler.Schedule(cellIdx);
}
else
    drainHints.TryPush(cellIdx);                // advisory: chunk is reclaimable (§4.4). Lossy by
                                               // design — the worker never touches ChunkBase.
```

#### Why it is correct

This is **Dekker's pattern**. Each side stores to its own variable, then loads the other's, and each
store is followed by a full fence:

| | store | fence | load |
| --- | --- | --- | --- |
| producer | `Tail` | the `CompareExchange` on `State` | `State` |
| worker | `State` | the `Exchange` itself | `Tail` |

So both sides cannot miss each other. The three outcomes:

| Producer appends… | Result |
| --- | --- |
| while `Owned` | producer's CAS fails (`State ≠ Free`), does not schedule. Worker's recheck sees the new `Tail`, wins its CAS, reschedules. ✓ |
| after `State = Free`, before the worker's recheck | producer's CAS succeeds and schedules. Worker's CAS then fails — no double-schedule. ✓ |
| exactly concurrent | both attempt `Free → Scheduled`; exactly one CAS wins. ✓ |

Never zero schedules (no stranded message), never two (no double-schedule).

**The emptiness check is not the arbiter — the `State` CAS is.** Emptiness only decides whether a
side *attempts* the transition. Getting this backwards is the classic ownership-release bug.

> **Weak-memory note.** `Volatile` release/acquire does **not** order a store before a subsequent
> load of a *different* location on ARM64 / Apple Silicon — this is already documented in
> `Threading.Workers/CLAUDE.md` and cost a real bug once. `Interlocked.Exchange` is used at the
> release site precisely to buy the StoreLoad fence. Do not "simplify" it to `Volatile.Write`.

### 5.4 Backpressure

Two independent sources, both blocking the producer:

1. **Slab exhausted** — no free chunk for a newly-dirty key.
2. **A key's ring full** — `Tail - Head == chunkSize`.

Both wake from the worker side. Reuse the family's **parked-flag** approach rather than signalling
unconditionally: the producer raises a flag before parking; the worker checks the flag (warm line)
when publishing `Head` or returning a chunk, and signals only if it is up.

Source (2) is the sharp one: it blocks the producer on *one* key, stalling all others —
head-of-line blocking across the whole system. See §11.

### 5.5 Eviction

Cold LRU sweep, producer-side. Only cells with `State == Free` **and** `ChunkBase < 0` (storage
already harvested) are evictable — so eviction never contends with an active drain. Guarded by
`CAS(State, Tombstone, Free)`, then tombstone the key. A lost CAS, or a validation failure after
winning it, goes through the shared `ReleaseAndMaybeSchedule` path (§4.4) — never a plain revert.

## 6. Scheduler seam

```csharp
interface IKeyScheduler {
  void Schedule(int cellIdx);                       // producer first-dirty, or worker self-reschedule
  bool TryAcquire(int workerId, out int cellIdx);   // non-blocking; may steal
  bool WaitForWork(int workerId);                   // park; false ⇒ shutting down
  void Start(int workerCount);
  void SignalStop();
}
```

Devirtualized the same way handlers are: `where TScheduler : struct, IKeyScheduler`, the struct
wrapping a reference to shared state (the `Rec` / `RecordingState` pattern from `SpWorkerTests`).

**Queues can never overflow.** A key index is in at most one place at a time (§3.1), so total
indices in flight is bounded by key-map capacity. Size the scheduler queues to that and there is no
backpressure logic in the scheduler at all.

### 6.1 B — Routed (push-side)

```
producer ──SPSC(int)──▶ router ──SPSC(int)──▶ worker 0  [+ private local list]
                          │     ──SPSC(int)──▶ worker 1  [+ private local list]
                          └─ picks argmin(QueuedApprox)
```

Every edge is SPSC ⇒ `SpRingCore<int>` verbatim on all of them.

The one wrinkle: a worker's **self-reschedule cannot go into its own SPSC ring** — that would be a
second producer, which the family's `CLAUDE.md` states hangs silently. Each worker therefore has a
**private, atomic-free local list** for self-rescheduled keys, drained with priority over its ring.

That is a feature, not a workaround: a key that keeps receiving messages stays pinned to the same
worker ⇒ **affinity for free** (warm cache, warm cell line). Consequently **B needs no key→worker
map at all** — the cell state machine already prevents concurrent processing; the router only
answers "which worker gets this newly-dirty key."

### 6.2 D — Work stealing (pull-side)

```
producer ──▶ [ global submission queue ]  (SPMC, key indices)
                        │
   worker i:  local Chase-Lev deque ──▶ global ──▶ steal scan ──▶ spin ──▶ park
```

- **Global queue** — producer-single, worker-many. Vyukov-style per-slot sequence stamps, sized to
  key capacity so it never fills.
- **Local deque** — Chase-Lev. Owner pushes/pops the bottom (LIFO, essentially atomic-free);
  thieves CAS the top. Self-reschedules push to the local bottom ⇒ same affinity property as B.
- **Overflow** — fixed-size deque; if full, push to the global queue instead. This is what .NET's
  `ThreadPoolWorkQueue` does; it avoids implementing Chase-Lev's growable array in v1.
- **Idle** — bounded spin → random-victim steal scan → park. Track sleeping-worker count so
  `Schedule` wakes exactly one, not the herd.

> Use **Lê / Pop / Cohen / Zappa Nardelli (2013)** for the deque memory orderings, **not** the 2005
> paper. Apple Silicon will find the difference.

D's global queue is a contained mini-C — a shared SPMC pull. It is safe here for the reason C is
not: it carries *key indices whose ownership is arbitrated by the cell machine*, not messages whose
order depends on claim order.

### 6.3 Expected outcome

B should win on low skew (fewer atomics, one predictable hop). D should win on skew and variable
cost (idle workers self-serve instead of waiting for a router judging by a stale `QueuedApprox`).
**Steal rate is the number that says which regime you are in** — hence the counters in §8.

## 7. Public API

```csharp
readonly struct OrderHandler : IActorHandler<long, Order> {
  public void Handle(long key, Order msg) { /* exclusive per key, ordered */ }
  public void OnError(Exception error) { }
  public void OnCompleted(Exception? error) { }
}

using var system = new ActorSystem<long, Order, OrderHandler, WorkStealingScheduler>(
  handler: new OrderHandler(),
  workers: 8,
  keyCapacity: 1 << 12,     // live keys
  chunkSize: 64,            // messages per key, v1 bound
  batchBound: 64);          // max messages per activation

system.Start();
system.Dispatch(orderId, order);   // single producer; blocks on backpressure
system.TryComplete();
await system.Completion;
```

Deliberately mirrors the existing family: struct handler (devirtualized), `Start` / `TryComplete` /
`Dispose` / `Completion`, hard-stop-drops vs soft-complete-drains.

**Handler copies.** Each worker gets its own copy of the handler struct, so per-worker handler state
is thread-local by construction. Shared state goes behind a reference the struct captures — same
rule as today, but now it genuinely matters.

## 8. Observability

- `IWorkerQueue` per worker (`QueuedApprox`, `Capacity`) — B's router needs it, and it makes depth
  samplable for free.
- Per-worker counters behind a compile-time switch: activations, messages, steals attempted, steals
  succeeded, parks, reschedules.

A work-stealing scheduler whose steal rate you cannot see is unbenchmarkable.

## 9. Test plan

NUnit, `tests/…/Threading/Actors/`, house conventions (struct recorder handlers wrapping a shared
reference-state object; `WorkerTestUtil.AwaitCompletionOrStall` / `AwaitProgress` as stall
watchdogs so a lost wakeup fails fast instead of hanging).

**Every behavioural test runs against both schedulers** — same body, parameterised on `TScheduler`.

| # | Test | Guards |
| --- | --- | --- |
| 1 | Per-key order: per-key sequence numbers arrive strictly monotonic | the whole point |
| 2 | No loss / no duplication: messages in == messages out, per key | cell machine |
| 3 | **Release race**: 1 key, producer appending continuously while the worker releases; assert never stranded and never double-scheduled | §5.3 — *the* test |
| 4 | Serial consumer handoff: force each activation onto a different worker; assert `Head` continuity | §4.3 |
| 5 | Ring-full backpressure: producer blocks and resumes | §5.4 (2) |
| 6 | Slab-exhaustion backpressure | §5.4 (1) |
| 7 | `batchBound` fairness: one firehose key must not starve others | §5.2 |
| 8 | Steal correctness under churn: many workers, few keys, high steal rate | D |
| 9 | Eviction: cell reclaimed only when `Free` + unbound; key re-dispatch after eviction works | §5.5 |
| 9b | **Reclaim race**: producer harvesting a chunk concurrently with a worker's release-recheck; assert never stranded (this is the `ReleaseAndMaybeSchedule` path) | §4.4 |
| 10 | Hard stop drops / soft complete drains | family contract |
| 11 | Handler throws ⇒ `OnError`, processing continues | family contract |
| 12 | Zero per-message allocation after warmup | family contract |

Tests 3, 4 and 8 are the ones that will find real bugs; budget accordingly. All three are stress
tests and must run under the stall watchdog, on both TFMs, in Debug and Release.

## 10. Project shape

- `src/experiments/mini-actor-system/` — folder location, but **real assembly and namespace from
  day one**: `Nanov.HighPerformance.Threading.Actors`. Moving a directory later is trivial;
  renaming a namespace across tests and benchmarks is not. Promotion becomes a `git mv`.
- Multi-targets **net9.0 + net10.0** from the start — free now, painful to retrofit once there is a
  test matrix.
- House rules on from the start: `TreatWarningsAsErrors`, `Nullable`, central package management
  (`Directory.Packages.props`), central version (`Directory.Build.props`), `.editorconfig` style.
- In the solution. NUnit tests from day one.
- Scrappy throughput harness first; a BenchmarkDotNet entry in the existing benchmarks project once
  the shape settles.

### Benchmark plan

Baselines: **N statically hash-sharded `SpWorker`s** (the design this must beat) and a
`Channel<T>` + `ConcurrentDictionary` idiomatic equivalent.

Axes: worker count; key cardinality; **key skew** (uniform → Zipf → single hot key); per-message
handler cost (constant → high variance). Report throughput, p99 latency, and steal rate.

The honest expectation: this substrate is **more expensive per message than a single `SpWorker`** —
it has to be, dynamic ownership is what is being bought. The question is not whether it beats one
worker, but whether N-way dynamic balancing beats N statically sharded workers under skew.

## 11. Known limitations and upgrade path

| Limitation | v1 stance | Upgrade |
| --- | --- | --- |
| **Per-key ring is bounded ⇒ a full key blocks the single producer entirely** (head-of-line across all keys) | accept, measure | bigger chunks → v2 chaining → non-blocking `TryDispatch` |
| `batchBound` is a tuning knob | accept | none; inherent to in-place drain |
| Rigid memory (`liveKeys × chunkSize`) | accept | v2 chaining makes chunks fungible |
| Single producer | accept | out of scope |

### v2 — chained chunks

A chunked mailbox **is** a per-key ring plus a "link when full" escape hatch: same cell shape, same
drain loop, same state machine. v2 adds a `Next` chunk link and a frozen-index protocol so the
consumer can distinguish "chunk finished" from "producer has not filled it yet"
(`ConcurrentQueue<T>`'s linked array segments are the readable reference). v1 is a strict subset —
not a throwaway.

### Bonus — design C

Shared SPMC message queue, workers pull directly and offload conflicting keys to the current owner's
inbox. **Rejected for now**: take order and *claim* order differ, so two workers can invert two
messages of the same key; plus an ownership-release race on the offload path. Both are solvable
(sequence numbers + per-key reorder buffer) but the per-item cost is a contended CAS *and* a shared
map lookup. Revisit only after B and D are measured.

### Rejected — intrusive linked list over a flat slab

The original storage choice: global slab of `{ Payload, Next }`, per-key intrusive list, detach the
whole list per activation. Elastic memory and self-limiting batches, but **one CAS per message** on
append and a pointer-chasing reverse walk on drain. Chunking gets the elasticity (at v2) without
either cost. Superseded.

## 12. References

**Closest prior art, readable, in C#**
- .NET `ThreadPoolWorkQueue` / `WorkStealingQueue` (`dotnet/runtime`) — global + local queues +
  stealing, production-hardened. Read first.
- `ConcurrentQueue<T>` — linked array segments; the v2 chunk-boundary reference.
- Orleans `WorkItemGroup` — per-grain scheduling onto a shared pool.
- Akka / Akka.NET dispatcher + mailbox, incl. the `throughput` setting — the direct analogue of
  `batchBound`. `Mailbox.setAsScheduled` is the same first-dirty CAS gate as §5.1.

**The deque**
- Chase & Lev, *Dynamic Circular Work-Stealing Deque* (SPAA 2005) — the algorithm.
- Lê, Pop, Cohen, Zappa Nardelli, *Correct and Efficient Work-Stealing for Weak Memory Models*
  (PPoPP 2013) — **use these orderings**, not the 2005 paper's.
- Java `ForkJoinPool` (Doug Lea) — most battle-tested variant.

**Other**
- Vyukov bounded MPMC queue — per-slot sequence stamps; D's global submission queue.
- LMAX Disruptor `WorkerPool` — shared `workSequence` CAS claim; the transport half of design C,
  with *no* ordering guarantee. Also `Util.getMinimumSequence` for N-consumer slot reclamation.
- Seastar — the anti-reference: shard-per-core, deliberately no stealing. Read their justification.
- Erlang/BEAM scheduler run queues + migration — reference for "steal vs. sleep" policy.

## 13. Implementation order

1. Slab + chunk free stack, with tests.
2. Key cell map (fork of `CoalescingKeyMap` with mailbox cursors), with tests.
3. Cell state machine + dispatch/activation/release, single worker, no scheduler
   (tests 1, 2, 3, 5, 6, 11, 12).
4. Scheduler seam + **B**, N workers (tests 4, 7, 10; re-run 1–3 under B).
5. **D**: global queue, Chase-Lev deque, steal scan, parking (test 8; re-run all under D).
6. Harness + baselines; B-vs-D measurement.

Steps 1–3 carry the correctness risk; 4–5 carry the performance question. Do not start 5 before 3's
stress tests are green on both TFMs, Debug and Release.
