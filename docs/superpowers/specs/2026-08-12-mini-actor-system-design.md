# Mini actor system — design

**Status:** Phase 1 (single worker), Phase 2a (N workers, scheduler seam, scheduler B) and Phase 2b
(scheduler D — global queue, Chase-Lev deques, stealing, parking) implemented; the B-vs-D benchmark
outstanding (§11, §13 step 6). Unmerged.
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

The seam both schedulers plug into exists and is proven: `ActorSystem<TKey, T, THandler, TScheduler>`
takes the scheduling policy as a `where TScheduler : struct, IKeyScheduler` type argument (§6, §7), so
B and D are designed to differ by exactly that one type argument and nothing else in the call site.
**Both B and D are now built** (§6.1, §6.2) and every behavioural test runs against both. The
head-to-head benchmark (§10) is still outstanding — see §11 — so **the comparative question this
section opens with is still not answerable.** Phase 2b makes D exist and proves it correct under the
same suite as B; it does not measure D against B. The one place the two schedulers have been compared
so far is the starvation workload in §6.1, and that comparison varies worker count as well as
scheduler — read its caveats before quoting it.

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

Free chunks live on a **producer-private intrusive stack** of chunk indices — a plain `int` head
plus a `int[] next` array. **No atomics, no Treiber stack, no ABA problem.**

That falls out of §4.4: chunks are rented *and* returned by the producer, so the free list has
exactly one reader and one writer, both on the same thread. An earlier draft specified a Treiber
stack because the worker returned chunks directly; once `ChunkBase` became producer-write-only that
requirement disappeared. Do not reintroduce interlocked operations here.

There is no second pool. "Renting a buffer" is claiming slots of the one slab; the free stack is
bookkeeping, not an allocator.

### 4.2 Key cell map

Open-addressing, fixed-capacity, **no relocation**, stable `int` indices — the `CoalescingKeyMap`
contract, with the accumulator replaced by mailbox cursors.

```csharp
struct KeyCell<TKey> {
  public TKey Key;       // producer-write-only
  public int State;      // Free | Scheduled | Owned | Reclaiming | Tombstone  — Interlocked
  public int ChunkBase;  // producer-assigned; -1 = no storage bound
  public int Head;       // consumer cursor, absolute
  public int Tail;       // producer cursor, absolute
  public int CachedHead; // producer-private lower bound on Head — see §4.5
}
```

Access discipline — this preserves the existing family invariant:

| Field | Written by |
| --- | --- |
| `Key`, `ChunkBase`, `CachedHead` | producer only (probe / insert / evict) |
| `State` | producer + workers, always `Interlocked` |
| `Tail` | producer only |
| `Head` | the current owner only |

**Layout: array-of-structs, no per-cell padding.** The producer touches the cell every message, but
the worker touches it only once per batch (publishing `Head`) and once per activation (`State`) —
its per-message work is entirely inside the *chunk*. So producer/consumer false sharing on the cell
line is amortized across a whole batch and does not warrant splitting into parallel arrays, which
would cost more cache lines per dispatch than it saves.

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

**Who returns the chunk — the producer, by a cold scan.** `ChunkBase` is producer-write-only
(§4.2), so a worker must not null it. Instead the producer harvests on its **cold path** — when its
private free list runs dry — by scanning the map for cells that are `Free` with a chunk still bound.
For each candidate it CASes `Free → Reclaiming`, re-validates that the mailbox is still empty
(`Tail == Head`), runs `OnChunkReclaimed` over the chunk (§4.6), nulls `ChunkBase`, and pushes the
chunk back on the free list. A cell that went dirty again fails the CAS or the validation and is
skipped.

This is deliberately the same shape as `CoalescingKeyMap.EvictFree`: a cold, producer-side,
state-gated sweep. **v1 has no drain-hint queue.** An MPSC hint ring (workers noting "this key looks
empty" so the producer can harvest without scanning) is a pure optimization over the scan and is
deferred until a benchmark shows the scan costs anything — it would be the *only* multi-producer
structure in the system, which is a poor trade for an unmeasured win.

> **The producer runs no cold paths after `TryComplete`.** Chunk harvest and LRU eviction must not
> run once soft completion has been signalled. §5.6's quiescence argument depends on every producer
> `Schedule` being sequenced-before `completed = true` on the single producer thread, and a cold
> path can `Schedule` via `ReleaseAndMaybeSchedule`. This is structurally true today — `TryComplete`
> is the producer's last act — but it is a silent assumption, so assert it at the reclaim and
> eviction entry points. The drain phase needs no harvesting anyway: no new messages arrive, so no
> new chunks are wanted.

This keeps the §4.2 access discipline intact (`ChunkBase` producer-only), and it removes the
eviction-vs-drain race entirely: eviction only ever touches cells that already hold no storage.
It is also what makes per-key state a later drop-in rather than a redesign.

Harvesting is never *required* for correctness — it only recycles storage. If the scan frees
nothing, the producer is genuinely out of chunks and backpressures (§5.4).

> **The reclaim revert must re-run the release check.** `Reclaiming` is a non-`Free` state, so a
> worker whose release-recheck lands in that window loses its `Free → Scheduled` CAS and does *not*
> schedule. If the producer then finds `Tail != Head`, reverts to `Free` and simply moves on, that
> key is **stranded** — messages queued, nobody scheduled. So the failed-validation path is not a
> plain revert: it must run the *same* recheck the worker runs (§5.3) — restore `Free`, and if
> `Tail != Head`, `CAS(State, Scheduled, Free)` and `Schedule(cellIdx)`. Same rule applies to a
> `Tombstone` attempt that loses validation. Factor it into one shared `ReleaseAndMaybeSchedule`
> helper used by both the worker release path and the producer cold paths; do not open-code it
> twice.

### 4.5 Cost model — where the per-message cost actually is

Per message, versus a plain `SpWorker` dispatch:

| | `SpWorker` | here |
| --- | --- | --- |
| fullness check | `_cachedHead`, producer-private, L1 | must not read `cell.Head` per message — see below |
| slot write | sequential in one ring, perfect prefetch | into the key's chunk — **random** |
| publish `Tail` | `Volatile.Write` | same |
| schedule gate | — | `State` transition |
| lookup | — | map probe — **random** |

The map is **not** the expensive part. `ConflationSpWorker` pays the identical probe per dispatch
and still benchmarks 2–5× faster at ~0.4× the allocation against the
`ConcurrentDictionary` + `Channel<key>` baseline.

Two costs are removable, both with techniques already in `SpRingCore`:

- **Producer-cached gating sequence, per cell.** Never read `cell.Head` on the dispatch fast path.
  Check fullness against `cell.CachedHead` (producer-private lower bound; `Head` only advances, so
  it can only over-estimate fullness — conservative, never overwrites a live slot) and re-read the
  real `Head` only when the cache says full. This is the LMAX gating-sequence technique already
  used by `TryReserve`, applied per key.
- **Test-then-CAS on `State`.** The CAS only needs to succeed on the `Free → Scheduled` edge, and
  under load a hot key is almost always already `Scheduled` or `Owned`. Fence, read `State`, and
  CAS only if it reads `Free`. **The fence is not optional** — a plain read breaks the Dekker
  argument in §5.3 and strands messages. Expected saving is the RMW instruction, not the cache-line
  ownership (the producer already holds the line exclusively for its `Tail` write), so treat this
  as a measured optimization rather than an assumption.

What remains is irreducible: **two random accesses per message** (the cell, and the key's chunk)
against `SpWorker`'s single sequential stream. That is the price of key affinity itself — any
design that routes by key pays it — not of the map implementation.

### 4.6 Slot reuse — no per-message cleaning, two producer-side hooks

Slots are **never cleared on the drain path**. A drained reference stays in its slot until it is
overwritten or harvested. This is the same trade-off `memory.md` records for `ConflationSpWorker`
("a drained reference is released when the producer overwrites it, or when the cell is evicted"),
accepted here for the same reason: the alternative is per-message work on the hottest loop.

The consequence to handle is retention: a chunk sitting on the free stack can pin up to `chunkSize`
dead messages. Both release points are **on the producer thread**, which is what makes a pooling
hook possible at all — `ChunkBase` is producer-owned and reclaim is producer-driven (§4.4), so the
producer can hand recovered objects somewhere useful instead of merely nulling them.

| Hook | Site | Cadence | Covers |
| --- | --- | --- | --- |
| `OnSlotReused(ref T slot)` | §5.1, immediately before overwriting `slot[Tail & mask]` | per message | keys that never go idle |
| `OnChunkReclaimed(Span<T> chunk)` | §4.4, during the producer's harvest walk | per key lifetime | the tail left in a chunk when its key *does* go idle |

The overwrite hook is the load-bearing one. Reclaim alone is insufficient: a chunk is only harvested
when its key goes fully idle, so a long-lived hot key would never return anything and a pool would
starve under exactly the load it exists for.

**The overwrite hook needs no cross-thread synchronization.** The producer is reading back its own
earlier write — the worker only ever *reads* slots. Slot `Tail & mask` was last written at
`Tail − chunkSize`, which is below `Head`, or the §5.1 fullness check would have blocked. The ring's
existing `Head` gate already proves the worker is finished with it.

**v1 ships the hooks, not a pool.** Both default to a no-op (or a plain null-out for reference `T`).
That keeps the cold path shaped to host pooling without committing to a pool API before a benchmark
says it pays.

> This is deliberately **not** the `IsReferenceOrContainsReferences<T>` `= default` clear that
> `Threading.Workers/CLAUDE.md` forbids. That rule targets `ValueSpWorker`'s per-item, per-slot
> whole-struct clear, which defeated zero-copy and was replaced by handler-driven `Clear`. These are
> producer-side hooks at overwrite and recycle time; there is no per-item clear on the drain loop.

## 5. Protocols

### 5.1 Dispatch (producer)

```
cell = map.GetOrAdd(key)                        // producer-private probe
if (cell.ChunkBase < 0) cell.ChunkBase = RentChunk()      // may block: slab exhausted

if (cell.Tail - cell.CachedHead >= chunkSize) {           // producer-private — no cross-core read
    cell.CachedHead = Volatile.Read(ref cell.Head);       // only on the "looks full" edge
    while (cell.Tail - cell.CachedHead >= chunkSize)      // may block: this key's ring full
        WaitForSpace(cell);                               // refreshes CachedHead on exit
}

ref var slot = ref slab[cell.ChunkBase + (cell.Tail & mask)]
handler.OnSlotReused(ref slot)                            // pool-return hook (§4.6); default no-op
slot = message
Volatile.Write(ref cell.Tail, cell.Tail + 1)              // release: publishes the slot
_dispatched++                                             // producer-private, no atomics (§5.6)

Interlocked.MemoryBarrier()                               // StoreLoad — required by §5.3's Dekker
if (Volatile.Read(ref cell.State) == Free)                // test-then-CAS (§4.5)
    if (Interlocked.CompareExchange(ref cell.State, Scheduled, Free) == Free)
        scheduler.Schedule(cellIdx)                       // first-dirty gating
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
    head++; n++;                                // slot NOT cleared — see §4.6
    if (ShouldFlush(n)) {
        Volatile.Write(ref cell.Head, head);    // batched publish + producer wake
        PublishProcessed();                     // per-worker count, same cadence (§5.6)
    }
}
```

**The slot is not cleared per message.** A drained reference stays in its slot until the producer
overwrites it (§5.1) or the chunk is harvested (§4.4) — the same trade-off `memory.md` records for
`ConflationSpWorker`, accepted for the same reason. See §4.6.

**A throwing handler consumes the message.** `OnError` is called, `head` still advances, and the
loop continues with the next message of the same key. The message is *not* retried, so a poison
message cannot stall the key — and the per-key ordering guarantee is unaffected, since the failed
message is consumed rather than reordered. Abandoning the rest of the batch instead would leave the
remainder queued and reschedule the key, turning one bad message into a repeating head-of-line stall.

**`batchBound` is required.** In-place drain is not self-limiting: under a firehose the producer
refills as fast as the worker drains, `Tail` never equals `Head`, and the worker is pinned forever.
(The rejected detach-a-snapshot design was self-limiting by construction; that property is lost and
the knob is the price.) This is exactly why Akka has a `throughput` setting.

**Default `batchBound = chunkSize`** — "drain what fits in one chunk". This makes the common case a
full drain and degenerates to exactly the self-limiting behaviour of the discarded detach design.
Akka's default of 5 is tuned for actors with expensive per-message work; handlers here are cheap and
the batch amortizes a scheduler round-trip, so a small default would be actively wrong.

### 5.3 Release — the crux

```
Volatile.Write(ref cell.Head, head);            // publish what we consumed
Interlocked.Exchange(ref cell.State, Free);     // ← FULL FENCE, deliberately not Volatile.Write
if (Volatile.Read(ref cell.Tail) != head) {     // producer appended while we were Owned,
    if (Interlocked.CompareExchange(              // or we stopped early on batchBound
            ref cell.State, Scheduled, Free) == Free)
        scheduler.Schedule(cellIdx);
}
// else: drained. The chunk is now reclaimable, but the worker does NOTHING — it must not touch
// ChunkBase (§4.2). The producer discovers it on its next cold harvest scan (§4.4).
```

#### Why it is correct

This is **Dekker's pattern**. Each side stores to its own variable, then loads the other's, and each
store is followed by a full fence:

| | store | fence | load |
| --- | --- | --- | --- |
| producer | `Tail` | `Interlocked.MemoryBarrier()` (§5.1) | `State` |
| worker | `State` | the `Interlocked.Exchange` itself | `Tail` |

The producer's fence is explicit because of the test-then-CAS in §4.5: once the common path is a
plain *read* of `State` rather than a CAS, the RMW is no longer there to supply the StoreLoad
ordering. Drop that barrier and the design strands messages on ARM64.

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

**Two dispatch entry points, both shipped in v1:**

| | Behaviour on backpressure |
| --- | --- |
| `Dispatch(key, msg)` | blocks until space — matches the family |
| `TryDispatch(key, msg)` | returns `false` immediately, message not enqueued |

`TryDispatch` costs a few lines (the machinery is shared) and earns its place by making head-of-line
blocking **measurable**: run the same benchmark both ways and the delta is exactly the cost of the
v1 per-key bound — the number that decides whether v2 chaining is worth building. Both return
`false` if the system is completed or disposed, as the family does.

> **Known debt — the worker's side of the parked-flag handshake has no StoreLoad fence.** The
> producer's side is fenced (`_producerParked = true` → `Interlocked.MemoryBarrier()` → re-read
> `Head`), but the worker's wake sites publish `Head` with a plain `Volatile.Write` and then read
> `_producerParked` — a release-store then an acquire-load of a *different* location, not
> StoreLoad-ordered. A wake can be missed entirely. Nothing hangs — the bounded 20 ms wait, not the
> recheck, covers that case — but it costs a full poll interval on the tail: a measurable
> tail-latency cliff on the backpressure path. Fix (a short spin before parking, matching
> `SpRingCore.WaitForSpaceSlow`, or a fence before the worker's `_producerParked` read) is deferred
> pending measurement.
>
> **Narrowed since this was written.** "The worker's wake sites" is now too broad: the **release** site
> (`ReleaseAndMaybeSchedule`) reads `_producerParked` immediately after
> `Interlocked.Exchange(ref cell.State, Free)`, which is a full fence, so that site is StoreLoad-ordered
> and no longer carries this debt. What remains is **`Activate`'s mid-batch flush** alone — the
> per-32-message `Volatile.Write(ref cell.Head, …)` followed by a plain `_producerParked` read. Still
> real, still bounded by the 20 ms poll, still deferred. See §11 known defect (a) for the fix that
> narrowed it.

A rejected `TryDispatch` does **not** increment `_dispatched` (§5.6).

### 5.5 Eviction

Cold LRU sweep, producer-side. Only cells with `State == Free` **and** `ChunkBase < 0` (storage
already harvested) are evictable — so eviction never contends with an active drain. Guarded by
`CAS(State, Tombstone, Free)`, then tombstone the key. A lost CAS, or a validation failure after
winning it, goes through the shared `ReleaseAndMaybeSchedule` path (§4.4) — never a plain revert.

> **Residual race, revisited — the double-token hole is now closed.** Eviction's `CAS(State,
> Reclaiming, Free)` used to be followed by a field wipe (`cell = default`) that stomped `State`
> back to `Free` for one instruction and then republished `Reclaiming` — a window in which a worker
> delayed between its own `State` release and its `Tail` recheck could win
> `CAS(State, Scheduled, Free)` and mint a second schedule token for a slot the producer was
> re-keying. The claim is now held continuously: the wipe touches every field except `State`, so
> `State` stays `Reclaiming` from the eviction CAS until `Place` republishes `Free` for the new key.
> A delayed worker's `CAS(State, Scheduled, Free)` therefore always fails against `Reclaiming` for
> the whole re-keying window — no second token is minted.
>
> What remains is not that hole — it's the already-handled, benign case `Activate` already guards
> for (`chunk == Chunk.None`, above): a token that was legitimately scheduled *before* eviction and
> is only popped and `Activate`d after the producer has evicted and re-keyed the slot. `Activate`
> finds no chunk, releases immediately, and does no work — one wasted no-op activation, bounded at
> one worker, never a second owner. It stays benign because there is still exactly one token in
> flight per legitimate schedule event.
>
> **Closed in Phase 2a.** `Activate`'s claim is now a validating CAS, not a blind exchange:
> `Interlocked.CompareExchange(ref cell.State, CellState.Owned, CellState.Scheduled)`. Losing it means
> "not ours" — the worker counts the rejection (`RejectedClaims`) and returns without touching the
> cell, per §4.2's discipline. A reviewer enumerated all eight writers of `cell.State` in the shipped
> code and established that while a schedule token is outstanding, every *other* writer that could
> touch the cell requires a comparand of `Free` (the producer's dispatch CAS, the worker's own release
> CAS, `Harvest`'s and `EvictFree`'s `Free → Reclaiming` claims) and therefore fails against
> `Scheduled` — so `Scheduled` is both the correct and the complete comparand, and no legitimate token
> can be dropped by this CAS. This also *removed* a latent strand that the old unconditional
> `Interlocked.Exchange(State, Owned)` carried: at one worker it was harmless (no second claimant to
> race), but the blind exchange could stomp `Owned` directly over a producer-held `Reclaiming` had a
> second worker ever raced it — the CAS makes that structurally impossible instead of merely
> improbable. With two workers now real, this was the load-bearing half of the Phase 2 entry
> criterion, and it is closed.

### 5.6 Completion and termination detection

With N workers and stealing, "everything is drained" is a real termination-detection problem. A
worker whose own queue is empty cannot exit merely because the producer completed: another worker
may be mid-activation and about to reschedule a key. With stealing, "all queues empty" is not even a
stable observation.

#### The scheme: message counts, not in-flight key counts

```
producer:  long _dispatched                // producer-private, plain increment, no atomics
worker i:  long _processed[i]              // padded, worker-private; published at Head-flush cadence

TryComplete():                             // producer thread, its last act
    Volatile.Write(ref _dispatchedFinal, _dispatched)
    Volatile.Write(ref _completed, true)
    scheduler.WakeAll()                    // UNCONDITIONAL broadcast — see below

quiescent():                               // evaluated on the idle path only
    if (!Volatile.Read(ref _completed)) return false      // ← read completed FIRST
    var sum = 0L
    for (var i = 0; i < N; i++) sum += Volatile.Read(ref _processed[i])
    return sum == Volatile.Read(ref _dispatchedFinal)
```

**Exact by construction.** Every message is handled exactly once — the cell state machine guarantees
that independently of anything here — so the sum converges to `_dispatchedFinal` regardless of
release races, `batchBound` reschedules, `ReleaseAndMaybeSchedule` reverts, tombstones, or steals.
No episode accounting exists to get wrong, and future protocol changes cannot break it.

**Conservative in the safe direction.** A worker that has processed messages but not yet published
makes the sum too *low*, never too high. An under-count delays quiescence; it cannot fabricate it.

**Cheap where it matters.** The hot-path cost is a worker-private store, published at the cadence of
the batched `Head` flush that already exists. (Same cadence — a *different* cache line, since
`_processed[i]` is per worker and `Head` is per cell.) The O(N) sum runs only on the idle path after
completion. (An earlier draft added "and §8 already plans these counters" — §8 planned per-worker
`activations` / `messages` counters that were never built, so the `_processed[i]` array shipped as
`ActorSystem`'s own state, not as scheduler observability. Nothing in §8 backs this path.)

#### Load order is load-bearing

The exit predicate must read `_completed` **first**, then the counters, all with acquire semantics.
The soundness argument is "every producer increment is sequenced-before `_completed = true`, so a
worker observing `_completed == true` must observe the corresponding counts" — and that implication
only holds if the count loads are ordered *after* the `_completed` load. Reversed, ARM64 will serve
a stale zero next to a fresh `_completed`, producing **false quiescence and silent message loss**.

This is precisely the existing soft-complete rule in `Threading.Workers/CLAUDE.md` — *"re-read
`_tail` **after** observing `_completed`"* — with the counter sum as the new `_tail`. Comment it the
same way and test it the same way.

#### Wake protocol — two edges, both required

1. **`TryComplete` on an already-drained system.** Everything drained long ago, every worker parked.
   No further count will ever be published, so nobody will ever observe quiescence. `TryComplete`
   must therefore **broadcast to all N unconditionally**, per the family rule that shutdown Sets
   everything and is never flag-gated. §6.2's "wake exactly one, not the herd" applies to `Schedule`
   only — never to shutdown.
2. **The last publisher after completion.** The worker whose publish completes the sum must then
   broadcast. Ordering on that thread: publish → **read `_completed` first** → then the sum → wake all.
   In other words, evaluate the *same* quiescence predicate the exit path uses; do not hand-roll a
   different order here.

   > **This ordering was wrong in an earlier draft of this spec, which said sum-then-flag.** That is
   > unsound as soon as the broadcast is made idempotent (and it must be — it sits on the per-32-message
   > publish path). `_dispatchedFinal` is **0 until `TryComplete` publishes it**, so `sum >= dispatched`
   > is unconditionally true beforehand; a worker that passes that term just before `TryComplete` lands
   > will then read the fresh flag and broadcast at a *partial* sum, burning the one-shot gate so the
   > real broadcast can never fire — and every parked peer waits forever.
   >
   > The asymmetry is the point: a **suppressed** broadcast is rescued three ways — `TryComplete` sets
   > the flag and then wakes everything unconditionally, the worker re-publishes within `batchBound`
   > and again in its exit path, and quiescence latches. A **premature** one is unrescuable. So order
   > against the harmful direction, not the harmless one.

   > **Phase 1 does not implement this edge, and correctly so.** With a single worker, the only
   > thread that could observe quiescence is the one already evaluating the predicate on its own
   > idle path, so there is nobody to wake. It becomes load-bearing the moment a second worker
   > exists: worker A publishes the count that completes the sum while workers B…N sit parked, and
   > without the broadcast they park forever. **The scheduler-seam task must add it** — it is the
   > single most likely way Phase 2 hangs.
   >
   > **Implemented in Phase 2a.** `PublishProcessed` — the same per-32-message publish path that writes
   > `_processedPublished[w.Id]` — evaluates `IsQuiescent()` immediately after the write and, if true,
   > gates the actual `WakeAll()` behind a one-shot `Interlocked.Exchange(ref _quiescenceBroadcast, 1)
   > == 0`. The check lives *here*, on the per-batch publish path, precisely because this is the site
   > where a worker's own publish can be the one that pushes the running sum over `_dispatchedFinal` —
   > nowhere else in the loop observes that transition. `IsQuiescent()` is reused as-is: flag read
   > first, counts second, the order this section already corrected above.
   >
   > It has a real guard, not just a green build: `Quiescence_Broadcast_Releases_Peers_Parked_Before_
   > The_Last_Publish` (`tests/…/Threading/Actors/MultiWorkerTests.cs`) holds one worker inside its
   > handler via a rendezvous so its seven peers park with nothing to do, fires `TryComplete` while
   > that worker is still busy — `TryComplete`'s own unconditional `WakeAll` reaches the parked peers
   > but is rejected as not-yet-quiescent (the busy worker's message is still in flight) and they
   > re-park — and only the busy worker's *own* final publish, the first to observe `IsQuiescent() ==
   > true`, can release them. Disabling the broadcast block makes this test stall instead of fail —
   > verified. (`Quiescence_Broadcast_Fires_At_Most_Once` and the N=8 lifecycle smoke test
   > `Eight_Workers_All_Complete_After_Drain` exercise the idempotence and the common case respectively,
   > but neither forces the broadcast to be the *only* thing that can release a parked peer — see the
   > doc comment on the smoke test for why not.)

**Parking is arm-then-recheck, with the quiescence predicate inside the recheck**: raise the parked
flag → `Interlocked.MemoryBarrier()` → re-check work availability *and* quiescence → park. Collapsing
this to check-then-park is the classic lost wakeup, and it will pass every test that never actually
parks — which is why the parking tests must run under `AwaitCompletionOrStall`.

#### Why not an in-flight key counter

The obvious scheme — one global counter of keys in `Scheduled ∪ Owned`, `+1` on `Schedule`, `−1` on
a final release — was designed, reviewed and **rejected with two confirmed bugs**. Recorded here so
nobody "simplifies" back to it:

- **It leaks on every `batchBound` reschedule.** Producer `+1`; worker drains to the bound, wins the
  release-recheck CAS and calls `Schedule` → `+1` again; next activation drains fully → one `−1`.
  Net `+1` per bounded activation, forever. `Completion` hangs deterministically under any firehose.
- **It leaks on the release race even if only the producer increments.** In §5.3's second row the
  producer wins the CAS and increments, and the worker's recheck CAS then *fails* — so the worker
  takes neither branch and never decrements. The episode ended via a failed CAS, which no
  final-release rule covers.

The naive repair (decrement at every `Owned → Free`) is balanced but strictly worse: it puts the
decrement *before* the recheck, so a worker that read a stale `Tail`, saw "empty" and decremented
can produce a transient zero while `_completed` is true — `Completion` fires with a message still
queued, which is then dropped on dispose. That is the same failure class as `memory.md` bug #2.

A correct token discipline does exist (`+1` only on producer-won transitions; worker-won recheck
transfers the token; worker-*lost* recheck decrements; confirmed-empty release decrements after the
confirming load). It is exact, and it was verified against all three §5.3 outcomes. It is rejected
anyway on two grounds: it is four counter sites keyed to CAS outcomes, each a latent reintroduction
of the bugs above; and it puts a **contended cross-core RMW on every activation from every worker**
— under high key churn with small batches, activations approach messages, making it a shared hot
line of exactly the kind the L2 signaling work existed to remove.

#### Hard stop

`Dispose` drops queued messages. A worker **finishes the message it is currently in** and abandons
the rest of the batch — tearing down under a running handler is something the family never does.
All N workers are joined against **one shared 1 s budget**, not 1 s each; N workers must not make
disposal take N seconds. `Completion` resolves as `OperationCanceledException`, as the family does.

## 6. Scheduler seam

The nine members as shipped (`IKeyScheduler.cs`, at the project root — not under `Schedulers/`,
which holds only the implementations — is the authority; this listing is a
convenience and must be re-read against the file before it is relied on):

```csharp
interface IKeyScheduler {
  void Start(int workerCount, Action<Exception> onFault);  // onFault: for a scheduler owning a thread
  void Schedule(int cellIdx);                              // producer first-dirty ONLY
  void ScheduleLocal(int workerId, int cellIdx);           // worker self-reschedule — a separate member
  bool TryAcquire(int workerId, out int cellIdx);          // non-blocking; may steal
  bool HasWork(int workerId);                              // racy by design; read before parking
  void ArmWait(int workerId);                              // Reset(), then raise the parked flag
  void Wait(int workerId);                                 // block until woken
  void WakeAll();                                          // unconditional broadcast — shutdown only
  void SignalStop();
}
```

> **This differs from an earlier draft of this spec in four ways, all deliberate.** That draft gave
> `Schedule` double duty ("producer first-dirty, *or* worker self-reschedule") and exposed a single
> `bool WaitForWork(int workerId)` seam member. As shipped: self-reschedule is its own member,
> `ScheduleLocal`, because it must reach the *calling worker's* own local structure (B's private stack,
> D's own deque) and a shared `Schedule` cannot express that; and `WaitForWork` is **not a seam member
> at all** — it is `ActorSystem`'s own method, built from `HasWork` / `ArmWait` / `Wait`. Splitting it
> that way is what lets `ActorSystem` own the arm-then-recheck sequence and put the *quiescence*
> predicate inside the recheck (§5.6), which a scheduler cannot see. `Start` also gained the `onFault`
> channel so a scheduler-owned background thread's death can fault `Completion` rather than wedge it.

Devirtualized the same way handlers are: `where TScheduler : struct, IKeyScheduler`, the struct
wrapping a reference to shared state (the `Rec` / `RecordingState` pattern from `SpWorkerTests`).

**Queues can never overflow.** A key index is in at most one place at a time (§3.1), so total
indices in flight is bounded by key-map capacity. Size the scheduler queues to that and there is no
backpressure logic in the scheduler at all.

> **That last sentence is now too flat.** D's global queue does carry a bounded retry loop on the
> enqueue side — see §6.2. It is not backpressure in the sizing sense the paragraph means (the queue
> still cannot durably fill), but it is a wait, and it has a ~65 ms worst-case tail. Read §6.2's retry
> subsection before quoting "no backpressure logic in the scheduler at all".

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

> **Known liveness hazard — local-first can starve the ring, and it is worse under B than D.**
> If key A is refilled faster than it drains, the worker pops A from its local list forever and key
> B, sitting in that worker's ring, **never runs** — unbounded latency, not merely unfairness.
> Backpressure does not fix it: the producer blocks on A, A drains, the producer refills A.
>
> This is **not** a single-worker artifact. Under **B**, each worker owns its ring and nothing can
> move B elsewhere, so the starvation survives any worker count. Under **D**, a thief can steal B,
> so D is largely immune. That asymmetry is a real, testable difference between the two schedulers
> and it is not something the original affinity argument accounted for.
>
> *(Every clause of that paragraph now needs scoping. The B half is still argued rather than measured;
> D's immunity holds only at **N ≥ 2**; and — most importantly — **"a thief can steal B" is the one
> part the measurement contradicts.** In the D arm the victim was rescued through the **global queue**,
> not by a steal: `GlobalDequeues` was constant at 2 (the two keys' initial schedules) in all 20
> executions, and 0–1 steals succeeded per run out of 18–33 attempts. Stealing is not the rescue
> mechanism that was measured. See the four caveats under the Phase 2b block below before quoting any
> of this.)*
>
> Phase 1 ships as-is (single worker, hazard recorded). The scheduler-seam task must bound
> consecutive local draining — check the ring every K local activations, or whenever the local list
> holds only the index just released — and the B-vs-D benchmark should include a
> hot-key-plus-starved-key workload so the difference is measured rather than argued.
>
> **Phase 2b measures the bound. It is needed, and the positions are structural, not magnitudes.**
> `RoutedScheduler.LocalDrainBound` (default 16) caps consecutive acquisitions from a worker's local
> stack before its ring gets priority (`TryAcquire`). Phase 2a could not demonstrate it did anything:
> the sustained-load stress test (`Eight_Workers_Sustained_Load_Preserves_Order_And_Loses_Nothing`)
> round-robins dispatch across keys, so no key ever monopolizes a worker's local stack, and setting the
> bound to `int.MaxValue` made no measurable difference to that test's timing (168/130 ms vs 182/188 ms).
> `StarvationAsymmetryTests` (`tests/…/Threading/Actors/`) is shaped for the hazard instead: one hot key
> dispatched densely and consecutively at `batchBound: 1` with a `Thread.SpinWait(5000)` per-message
> handler cost, plus one victim key placed on that same worker's ring under `RoutingPolicy.RoundRobin`.
> It reports the position in the 3,000-message hot stream at which the victim's single message is finally
> handled:
>
> | Arm | Scheduler | `LocalDrainBound` | Workers | Victim handled at position |
> | --- | --- | --- | --- | --- |
> | bounded | B | 16 | 1 | **17** — exactly `LocalDrainBound + 1` |
> | unbounded | B | `int.MaxValue` | 1 | **3000** — exactly `HotMessages`, the entire stream |
> | work stealing | D | none | 4 | **0** |
>
> **The D arm contains no rescuing steal, and the table must not be read as one.** `GlobalDequeues` was
> constant at **2** — the two keys' initial schedules — in every one of the 20 executions, so the victim
> was picked up from the *global queue* by an idle worker. Steals did occur (0–1 succeeded per run, out
> of 18–33 attempted), but none of them was the rescue. This is consistent with §6.2's depth-1 deque
> finding: D's steals only ever come from the one-push-wide window between a worker's push and its own
> next pop, which is not where a starved key sits. What the D arm demonstrates is *a shared structure an
> idle peer can drain*, not *stealing*.
>
> **Zero variance on every value across 20 executions** (10 runs × 2 TFMs), and the assertion threshold
> of 200 sits strictly inside the 17-to-3000 gap rather than inside either distribution. Both B positions
> are **closed forms derived from the code and then confirmed**, not empirical magnitudes that happened to
> land well: `TryAcquire` resets the local run counter on a ring pop and increments it only on local pops,
> so the bounded position must be exactly `bound + 1`; and the ring is unreachable while the hot key keeps
> self-rescheduling, so the unbounded position must be exactly the full hot stream. (Position ==
> `HotMessages` is also only reachable if the hot mailbox never emptied, so the oracle certifies its own
> precondition.) The **vacuity check** confirms the bound is what moves them: re-enabling
> `LocalDrainBound = 16` on the unbounded arm yields 17 in all six executions and correctly *fails* that
> arm's assertion.
>
> `RoutedScheduler` used to document `LocalDrainBound = 16` on the property itself as *"a guess pending
> measurement, not a result."* That comment was stale — the bound's *effect* is measured and exactly
> predictable (`bound + 1`); what remains unmeasured is only whether **16** is the right value — and the
> XML doc has now been updated to say so, in this same documentation pass.
>
> **Four caveats, without which the table above is misleading:**
>
> 1. **D's immunity is scoped to N ≥ 2 — the comparison varies two factors, not one.** The B arms run at
>    `workers: 1` and the D arm at `workers: 4`, so the scheduler is not the isolated variable.
>    `WorkStealingScheduler.TryAcquire` pops its own deque unconditionally before consulting the global
>    queue, so **D at `workers: 1` has the identical hazard and no bound to fix it** — the global queue is
>    unreachable until the hot key drains, and the position would also be the whole stream. D's
>    demonstrated immunity comes from *having idle peers*, not from a structural property B lacks at equal
>    worker count. A user running D at `workers: 1` gets exactly the starvation this section describes.
>    The asymmetry the data *does* support is the one stated above: under B a ring-queued key is
>    unreachable by idle peers **even at N > 1**, because the ring is per-worker; under D the starved key
>    sits in a shared structure any idle peer can take.
> 2. **The B measurement is single-worker.** The claim above that the hazard "is not a single-worker
>    artifact" is still **argued from `TryAcquire`'s per-worker structure, not measured.** This workload
>    cannot be extended by simply raising `workers`: only two keys ever reach the router, so round-robin
>    would place the victim on an idle worker's ring and the setup dissolves.
> 3. **`RoutingPolicy.RoundRobin` is inert at N = 1** — `Route` reduces to `n % 1`. It is required by the
>    *design* of the workload (`LeastLoaded` spreads work away from a visibly busy worker and defeats the
>    setup before it starts), but nothing in these runs measured it doing anything. Do not write that
>    round-robin was required "as measured".
> 4. **Severity scales with producer/consumer imbalance; the hazard is not unconditional.** At near-parity
>    — an earlier `Thread.SpinWait(200)` calibration, since abandoned — the unbounded arm reproduced total
>    starvation in only about 60% of executions. In that calibration's formal batch (10 executions,
>    against **that batch's own threshold of `HotMessages / 2` = 1500**, *not* the 200 the shipped tests
>    use) 4 executions landed below it, at positions 762, 775, 1360 and 1430; the passing 6 were 1992 and
>    3000×5. A separate exploratory batch at the same spin value went as low as **34**. Total starvation
>    is a *sustained-backpressure* regime, reached here only at `SpinWait(5000)`. Do not quote any
>    position from the abandoned calibration, and do not read its failures against the shipped
>    threshold — the two batches and the two thresholds are not interchangeable.
>
> The B-vs-D benchmark (§10) **must** carry this workload at N > 1 on **both** schedulers — not a
> generic hot-key-plus-starved-key mention, but specifically dense-consecutive-hot-key +
> round-robin-placed victim + a bound-relative latency assertion, now also at N > 1 and also under D.
> That is the only thing that turns caveats 1 and 2 from arguments into measurements, and without it
> the question this section raises stays half-answered indefinitely. It is written into §10's plan.

**Load metric = queued key count** (`QueuedApprox`), not queued messages. Message-depth load is the
"truer" metric, but it is stale the instant it is read and the router would pay cross-core reads per
decision to obtain it. Key count is already exposed and free.

**Ship round-robin as a second, metric-free router.** If `argmin(QueuedApprox)` and round-robin
benchmark the same, the router's cleverness is not earning its keep — which is a genuine result and
directly informs whether B is worth keeping at all. A null baseline you can actually run beats an
argument about which metric is better.

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
- **Steal granularity: one key.** The steal-half variant exists because fine-grained task deques
  make a single steal too small to amortize the CAS. That does not apply here — one key already *is*
  a batch of up to `batchBound` messages, so a single steal moves substantial work. Steal-half would
  also concentrate several hot keys onto one thief, which is precisely the imbalance stealing exists
  to fix. (Key-merge and richer policies are a later branch, deliberately unspecified.)
- **Idle** — bounded spin → random-victim steal scan → park. Track sleeping-worker count so
  `Schedule` wakes exactly one, not the herd. **`WakeAll` is the exception** — shutdown broadcasts
  unconditionally (§5.6).

> Use **Lê / Pop / Cohen / Zappa Nardelli (2013)** for the deque memory orderings, **not** the 2005
> paper. Apple Silicon will find the difference.

D's global queue is a contained mini-C — a shared SPMC pull. It is safe here for the reason C is
not: it carries *key indices whose ownership is arbitrated by the cell machine*, not messages whose
order depends on claim order.

#### As built (Phase 2b) — four deviations from the above

The paragraphs above are the design as originally specified and are **kept, not corrected in place**,
so the reasoning that produced them stays readable. Three flat assertions in them are false as built and
a skimmer hits them first: the diagram, the "Global queue" bullet and the closing "contained mini-C"
paragraph all say **SPMC**, and the queue is **MPMC** — see deviation 1. `WorkStealingScheduler`,
`GlobalKeyQueue` and `WorkStealingDeque` deviate from the text above in four places, each for a reason
worth keeping:

1. **The global queue is MPMC, not SPMC.** Overflow (bullet 3 above) means a worker whose deque is
   full spills into the same queue, so there are `workers + 1` possible enqueuers, not one. The
   enqueue side therefore CASes as well. It costs one CAS on a path that runs once per **activation**,
   not per message, and it removes a sizing invariant that would otherwise be silent to violate.
2. **`HasWork` consults peer deques.** The design above does not mention `HasWork` at all — the seam
   member did not exist in that draft (see §6's note). Reporting only on the worker's own deque and
   the global queue would let workers park while stealable work sits in a peer's deque — **D would
   silently degenerate into B without a router.** No correctness test would catch that, because
   quiescence still resolves; it is a liveness-*shaped* defect with a throughput-only symptom.
   Mutation-verified: making `HasWork` ignore peers is caught **deterministically, 15/15 on both
   TFMs**, by `An_Idle_Worker_Steals_From_A_Busy_Peer`. Do not "simplify" the peer scan away.

   > **Its cost is unmeasured and belongs on the benchmark's suspect list.** The scan reads every
   > peer's `CountApprox`, which loads that peer's `_bottom` — **the owner's hottest write line** —
   > and `ActorSystem.WaitForWork` calls `HasWork` up to 32 times in its spin phase plus twice per
   > park-loop iteration. At 8 workers that is a few hundred cross-core reads of 7 hot lines per park
   > attempt. Nothing has measured it. It is the price of deviation 2 and it lands squarely on §13
   > step 6: if D loses to B on low skew, **the park-path peer scan is a suspect alongside stealing
   > overhead**, and the benchmark should be able to tell them apart rather than blaming the steal
   > path by default.
3. **D owns no threads, so it has no stop flag and never invokes `onFault`.** The seam's fault
   callback exists for a scheduler that runs its own thread and could die on it silently — B's router.
   `SignalStop` here only has to release parked workers so each can observe the system's own exit
   condition.
4. **Both deque fences are `Interlocked.MemoryBarrier()`, not `Volatile` writes** — per Lê / Pop /
   Cohen / Zappa Nardelli (2013), as §12 requires. `Volatile` release/acquire does not order a store
   before a subsequent load of a different location on Apple Silicon; the deque needs exactly that
   ordering in `TryPopBottom` and `TrySteal`.

> **A publication precondition that is easy to lose.** `GlobalKeyQueue`'s constructor writes non-zero
> per-slot sequence stamps (`_cells[i].Sequence = i`) with **no publication fence**. That is safe today
> only because of how it is used: the queue is constructed in `Start` and the worker threads are started
> afterwards, and `Thread.Start` supplies the happens-before edge. **The instance must therefore never be
> handed to already-running workers through a non-volatile field.** `WorkStealingScheduler.Start` guards
> against *reuse* of a scheduler instance; it does not guard against *late publication*. Anyone who later
> makes the global queue lazily created, resizable, or swappable at runtime ships an unfenced publication
> unless they add the fence at the same time.

#### The deque never holds more than one item — and what that invalidates

**A worker's own deque never holds more than one item.** `ScheduleLocal` is the only push source into
a worker's deque; it is called exclusively by that worker's own thread, from `ReleaseAndMaybeSchedule`
inside `Activate`; and `RunLoop` then loops straight back to `TryAcquire`, which pops its own deque
first. Push, then immediately pop. This is the most consequential finding of Phase 2b, and it
invalidates several things the design above implies:

- **`DequeOverflows == 0` is structural, not headroom.** The spill-to-global path (bullet 3 above) and
  the bounded retry behind it are **unreachable through this `ActorSystem`/scheduler pairing at any
  `DequeCapacity`.** `DequeCapacity` is very nearly a dead knob — do not tune it expecting an effect,
  and do not read a zero overflow counter as "the deque was comfortably sized".
- **The spill path is therefore untested because it is unreachable**, not because coverage was
  skipped. Closing that gap needs a direct `WorkStealingDeque` unit test bypassing the `ActorSystem`
  discipline — which `WorkStealingDequeTests` in fact has.
- **The Chase-Lev deque is operating as a depth-1 handoff slot**, with its two carefully-argued fences
  guarding a single element.
- **Every successful steal comes from the narrow window between a worker's push and its own next
  pop.** That is what "work stealing" currently means in D, and it is directly material to the B-vs-D
  benchmark's interpretation.
- **Depth-1 makes the deque's cheap path unreachable, so D pays more per activation than B in this
  specific pairing.** Every successful `TryPopBottom` finds `top == bottom` and pays a contended
  `Interlocked.CompareExchange` plus a full `Interlocked.MemoryBarrier()` fence (see
  `WorkStealingDeque`'s type doc), where B's equivalent — an uncontended local-stack decrement — is not
  atomic at all. This is a benchmark suspect, not a measured cost; see §10.

#### The bounded `TryEnqueue` retry costs up to ~65 ms

`GlobalKeyQueue.TryEnqueue` returning `false` means "not right now", never durable fullness: a
participant preempted mid-claim makes a free slot look occupied. Both enqueue sites therefore retry —
**64 attempts (1 initial + 63 retries), of which ~44 escalate to `Thread.Sleep(1)`, giving a ~65 ms
worst-case wall clock** on a path in the scheduler we are about to latency-benchmark. (The escalation
count is approximate on purpose: 44 was derived for a 64-`SpinOnce` loop, and the shipped loop makes 63
calls — the removed one being the most-escalated — so 43 is the defensible figure. It was never
re-measured, so treat it as ~44 and the wall clock as ~65 ms, not as exact.) That escalation
is the right shape for the real mechanism: a `false` observed with `CountApprox == 0` proves the
blocking stamp is a **full lap stale** — the participant holding it claimed position `pos − capacity`
and has been stalled across roughly `capacity` subsequent operations. That is OS-preemption scale
(tens to hundreds of microseconds), not "a few instructions", so a busy-spin-only budget would not
survive it.

This is not defensive plumbing. Throwing on a single transient `false` was a **live intermittent
failure**, reproduced 7 times in 40 runs on unmutated code — and it had passed an earlier review that
explicitly classified it as "not a live defect". A one-shot retry was also measured insufficient
(5/50 clean). `GlobalEnqueueRetries` (§8) is what says whether the tail is ever actually paid.

**Only one of the two enqueue sites can reach that tail.** The code retries at both, but the spill site
in `ScheduleLocal` is unreachable through this `ActorSystem`/scheduler pairing (see the depth-1 finding
above). So the ~65 ms worst case belongs entirely to the **producer's `Schedule`** — which sharpens the
latency point rather than softening it: the tail sits on the dispatch path, not on a worker's.

### 6.3 Expected outcome

B should win on low skew (fewer atomics, one predictable hop). D should win on skew and variable
cost (idle workers self-serve instead of waiting for a router judging by a stale `QueuedApprox`).
**Steal rate is the number that says which regime you are in** — hence the counters in §8.

> **Steal rate alone turned out not to be that number.** At the one configuration measured so far,
> `GlobalDequeues` exceeded `StealsSucceeded` in **all 10 samples** (§9), and in the starvation workload
> the rescue was a global-queue pickup with essentially no steals at all (§6.1). Read the steal rate
> *against* `GlobalDequeues`, not on its own — a low steal rate may mean "no imbalance to fix" or it may
> mean "the global queue is doing the redistribution", and those are different regimes.

## 7. Public API

```csharp
readonly struct OrderHandler : IActorHandler<long, Order> {
  public void Handle(long key, Order msg) { /* exclusive per key, ordered */ }
  public void OnError(Exception error) { }             // Handle threw; worker continues (§5.2)
  public void OnCompleted(Exception? error) { }        // fires ONCE PER WORKER — see below
  public void OnSlotReused(ref Order slot) { }         // §4.6 — default no-op
  public void OnChunkReclaimed(Span<Order> chunk) { }  // §4.6 — default no-op
}

using var system = new ActorSystem<long, Order, OrderHandler, WorkStealingScheduler>(
  handler: new OrderHandler(),
  keyCapacity: 1 << 12,     // live keys
  chunkCount: 1 << 10,      // max concurrently-dirty keys
  chunkSize: 64,            // messages per key, v1 bound
  batchBound: 64,           // max per activation; defaults to chunkSize
  workers: 8,
  scheduler: new WorkStealingScheduler());

system.Start();
system.Dispatch(orderId, order);          // blocks on backpressure
system.TryDispatch(orderId, order);       // returns false instead of blocking (§5.4)
system.TryComplete();
await system.Completion;
```

`where TKey : notnull, IEquatable<TKey>` — keyed by its own `GetHashCode` / `Equals`, no
handler-supplied hash, exactly as `ConflationSpWorker` and `CoalescingSpWorker`. `T` is unconstrained.

Deliberately mirrors the existing family: struct handler (devirtualized), `Start` / `TryComplete` /
`Dispose` / `Completion`, hard-stop-drops vs soft-complete-drains.

**Handler copies, and `OnCompleted` fires N times.** Each worker gets its own copy of the handler
struct, so per-worker handler state is thread-local by construction — and each copy therefore needs
its own teardown, so **every** worker's copy receives `OnCompleted` exactly once. A single
designated call would leave N−1 copies un-torn-down. The one system-level signal is `Completion`,
which resolves once, after all N workers have exited. Shared state goes behind a reference the
struct captures — same rule as today, but now it genuinely matters.

**Worker pool is fixed at N and workers park indefinitely when idle.** No idle-timeout exit, no
dynamic sizing in v1: that is a control loop of its own (hysteresis, spawn latency, thrash under
bursty load) and it is orthogonal to the B-vs-D question this experiment exists to answer.
Instrument park/unpark rate instead (§8) — heavy parking is the evidence for making it dynamic, and
then it will have been measured rather than guessed.

> **That instrumentation does not yet exist in usable form.** D's `Parks` counts park *intent*, not
> sleeps (§8), and **there is no unpark counter at all**, so "park/unpark rate" is currently
> unmeasurable and `Parks` over-counts by an unknown margin. B exposes no park counter either. Anyone
> reaching for this evidence has to add the counters first.

## 8. Observability

- `IWorkerQueue` per worker (`QueuedApprox`, `Capacity`) — B's router needs it, and it makes depth
  samplable for free.

A work-stealing scheduler whose steal rate you cannot see is unbenchmarkable.

**As built (Phase 2b).** `WorkStealingScheduler` exposes six counters, all `long`, read through
`Volatile.Read`:

| Counter | Meaning |
| --- | --- |
| `StealsAttempted` | steal attempts including the ones that came up empty — denominator of the steal rate |
| `StealsSucceeded` | steals that took a key — the number that says which regime you are in (§6.3) |
| `GlobalDequeues` | keys taken from the global submission queue |
| `DequeOverflows` | self-reschedules that spilled to the global queue because the deque was full |
| `Parks` | see the caveat below |
| `GlobalEnqueueRetries` | failed `TryEnqueue` attempts that had to retry (§6.2) |

Two things about them are not what §8 originally proposed:

- **They are not behind a compile-time switch — that was not built.** They are unconditional
  `Interlocked` increments. `Schedule` fires roughly once per activation (§5.1), and `StealsSucceeded`,
  `Parks` and `GlobalDequeues` are all per-activation-or-idler events — but **`StealsAttempted` is not**:
  it is incremented once per *victim tried* inside `TrySteal`'s scan, on the **failed-acquire** path,
  bounded by neither messages nor activations. §9 measures 47k–300k attempts against 80,000 messages at
  8 workers — up to **3.75 increments per message**, on one unpadded shared field, up to 7 RMWs per
  failed-acquire scan (`workers - 1` peers tried before giving up). That cost is now measured, not
  unmeasured, and it lands squarely on the benchmark this counter exists to serve. A compile-time switch
  remains available if that turns out to matter; the counters are still worth having — `StealsSucceeded`
  is what says which regime you are in (§6.3) — the point above is only that their cost is not free and
  is no longer a guess.
- **`Parks` counts park *intent*, not sleep.** It is incremented on entry to `Wait`, before the block
  runs, so a worker whose event was already `Set` — a wake landing between the armed recheck and the
  call — records a park that never happened.

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
| 11 | Handler throws ⇒ `OnError`, next message of the same key still runs, order intact | §5.2 |
| 12 | Zero per-message allocation after warmup | family contract |
| 13 | **Quiescence under churn**: `Σ processed == dispatched` after every drain; predicate never true while any ring is non-empty | §5.6 |
| 14 | **`TryComplete` on an already-drained, fully-parked system completes** | §5.6 wake edge 1 |
| 15 | **`TryComplete` racing a final in-flight message** — the F2 interleaving: no `Completion` before that message is handled | §5.6 |
| 16 | `OnCompleted` fires exactly once per worker (N total); `Completion` resolves once, after all exit | §7 |
| 17 | `TryDispatch` returns false under backpressure and does not count toward `dispatched` | §5.4 |

Tests 3, 4, 8, 13 and 15 are the ones that will find real bugs; budget accordingly. All are stress
tests and must run under the stall watchdog, on both TFMs, in Debug and Release. Tests 14 and 15
must genuinely **park** — a lost-wakeup bug passes every test whose workers never sleep.

### Status after Phase 2b

**Test 8 (steal correctness under churn) is done.** `SchedulerBehaviourTests` is a generic fixture
closed over both `RoutedScheduler` and `WorkStealingScheduler` (5 test methods, each run twice per
TFM), and the starvation workload behind §6.1 is a third fixture, `StarvationAsymmetryTests`. The plan
above asked specifically for the *core* behavioural suite to run under both schedulers, and that is
what `SchedulerBehaviourTests` delivers — the work is in scope.

**"Every behavioural test runs against both schedulers" is not, however, literally true, and the
Phase 2b task report overstated it on the strength of those 5 methods.** The rest of the suite is still
scheduler-specific: `ActorSystemTests` (33 tests) and `ActorSystemStressTests` (8 tests) run only under
`SingleWorkerScheduler`; `FaultPropagationTests` (3 tests) runs only under `RoutedScheduler`; and
`MultiWorkerTests` (6 tests) runs 5 under `RoutedScheduler` and 1 under `SingleWorkerScheduler`. None of
these three fixtures runs any test under `WorkStealingScheduler`. Concretely, **the following have never
run under D**: backpressure (`WaitForMailboxSpace` / `WaitForChunk`, in `ActorSystemTests` /
`ActorSystemStressTests`), `TryDispatch`, hard-stop semantics, zero-allocation, and fault propagation.
Nothing structurally prevents extending any of these to `WorkStealingScheduler` — it is unclaimed
follow-up work, not a blocked one.

**`RejectedClaims == 0` is now asserted under D.** Before Phase 2b it had only ever been checked under
`SingleWorkerScheduler` and `RoutedScheduler` — neither of which can move a key once scheduled — so
"two tokens for one key index never exist" had never been checked under the scheduler where a key
moves between workers by stealing and where `ScheduleLocal` spills into a shared MPMC queue. It is
asserted in `Eight_Workers_Preserve_Per_Key_Order`, a regime where neither `Harvest` nor `EvictFree`
can run (64 chunks for 64 keys, 512 map slots), so a non-zero count there would be unambiguous.

#### Measured behaviour of D under churn

`Steals_Happen_Under_Churn_And_Preserve_Per_Key_Order`, at **80,000 messages / 4 keys / 8 workers /
`batchBound: 1`**, over 10 samples (5 runs × 2 TFMs):

| Counter | Range across the 10 samples |
| --- | --- |
| `StealsSucceeded` | 651 – 11,553 |
| `StealsAttempted` | ~47k – ~300k (a 1.4–7% hit rate, with a 6× run-to-run spread) |
| `GlobalDequeues` | ~10k – ~36k |
| `DequeOverflows` | 0 |
| `GlobalEnqueueRetries` | 0 |

**`GlobalDequeues` exceeded `StealsSucceeded` in all 10 of the 10 samples.** (The task report and this
spec's first draft both said "9 of 10"; recomputing from the per-run table, the closest sample is
11,056 steals against 12,025 global dequeues — still global. Because the table above prints ranges
rather than paired rows, a reader cannot check this, so it is stated explicitly.) At this configuration most
non-local acquisitions come from the global queue, not from stealing. The mechanism is
cold-start-always-global plus a round-robin producer plus idle-worker abundance: with `batchBound: 1`
and four keys round-robined, cells flip back to `Free` constantly, and every re-dirty is a cold start
that can only route through the global queue, where several idle workers win the race before a steal
is needed. Do not attribute a D-over-B throughput result at this key/worker ratio to stealing without
reading `GlobalDequeues` alongside it.

The test is not vacuous: making `TrySteal` an unconditional no-op turns it **red deterministically on
both TFMs** via the counter assertions, with no hang — quiescence still completes through the
global/local path, which is precisely the silent degeneration §6.2 warns about.

> **The two zero counters are different kinds of zero. Do not conflate them.**
> `DequeOverflows == 0` is **structural** — the deque is a depth-1 handoff slot and the spill path is
> unreachable (§6.2). `GlobalEnqueueRetries == 0` is **load-dependent** — that retry loop was entered
> tens of thousands of times and simply never needed a second attempt at this load.

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
handler cost (constant → high variance). Report throughput, p99 latency, and steal rate — and report
`GlobalDequeues` alongside the steal rate, per §6.3, or a redistribution result gets attributed to
stealing that the global queue actually did.

**The head-to-head is B vs. D at the same worker count**, both against the two baselines above. That
is §13's step 6 and it is the only thing that answers §1. Nothing measured before it compares the two
schedulers on an isolated variable.

#### Two benchmark suspects on D, open going into step 6

Both are unmeasured costs on D's hot path, both follow directly from the depth-1 finding (§6.2), and
step 6 should be able to tell them apart from genuine stealing overhead rather than lumping everything
into "D is slower":

1. **`TryPopBottom` writes to `_bottom` on the empty path.** Every call — and, given depth-1, the
   overwhelming majority of calls are the empty poll, not a successful pop — does two `Volatile.Write`s
   plus an `Interlocked.MemoryBarrier()`, dirtying the cache line every peer reads in `HasWork`'s scan.
   `TryAcquire` calls it unconditionally, and `WaitForWork` returns with no backoff whenever `HasWork` is
   true, so a worker that just lost a steal can re-enter this path in a tight loop — up to 3.75 failed
   acquires per message by §9's measured numbers. The standard early-out other implementations use
   (`if (_bottom.Value - Volatile.Read(ref _top.Value) <= 0) return false;`, as .NET's
   `ThreadPoolWorkQueue` carries) would remove the write-and-fence on the empty path entirely.
   **Recommended as the first act of Phase 3, before the benchmark runs**, so that D is measured in the
   form it was designed to have rather than with this avoidable cost baked into every sample.
2. **Depth-1 makes the deque's cheap path unreachable** (§6.2's fifth consequence): every successful
   `TryPopBottom` pays a contended CAS plus a full fence per activation, where B's equivalent local-stack
   pop is a plain, non-atomic decrement. Unlike suspect 1, this one is not a bug to fix before
   benchmarking — it is a structural property of this `ActorSystem`/scheduler pairing that the benchmark
   should attribute correctly rather than blaming on stealing.

#### Required workload: dense-hot-key + placed victim (the §6.1 starvation asymmetry)

This one is **not optional**, and it is the workload §6.1's open caveats depend on. Not a generic
"hot-key-plus-starved-key" mention — specifically:

- a single hot key dispatched **densely and consecutively** (not round-robin interleaved), with a small
  `batchBound` so every activation self-reschedules onto the same worker's local structure many times
  in a row, and a per-message handler cost high enough that the producer stays ahead of the consumer —
  §6.1 needed `Thread.SpinWait(5000)` to hold that precondition by construction rather than by luck;
- a victim key placed on **that same worker's** ring/deque, which under B requires
  `RoutingPolicy.RoundRobin` (`LeastLoaded` spreads work away from a visibly busy worker and defeats
  the setup before it starts);
- hot-key volume far exceeding `LocalDrainBound`, and an assertion on **when** the victim's message is
  handled relative to the hot stream — a bound-relative latency assertion, not a wall-clock one;
- **run at N > 1 and under both schedulers.** §6.1 measured B at 1 worker and D at 4, which varies two
  factors at once; only the N > 1 cross-product separates "the scheduler" from "having idle peers", and
  it is the only way to test whether D at `workers: 1` starves exactly as B does.

Report the victim's position, not a duration: §6.1's positions are closed forms (`bound + 1`,
`HotMessages`) and a regression shows up as a changed position long before it shows up in a timing mean.

The honest expectation: this substrate is **more expensive per message than a single `SpWorker`**,
and per §4.5 the cost is **two random accesses per message** (cell + chunk) against `SpWorker`'s one
sequential stream — the price of key affinity, not of the map. The question is therefore not whether
it beats one worker, but whether N-way dynamic balancing beats N statically sharded workers under
skew.

Because the gap is memory-access-shaped, key cardinality is the axis that will move it most: at low
cardinality the cells and chunks stay resident and the gap should mostly close; at high cardinality
it will not. Measure cardinality independently of skew for that reason.

## 11. Known limitations and upgrade path

**Phase 2b delivered the second scheduler, not the result.** The experiment's central question —
push-side (B) vs. pull-side (D) — is still unanswered: the head-to-head B-vs-D benchmark (§10, §13
step 6) has not run. What exists today is an N-worker key-affine processor running behind a pluggable
scheduler seam with **both** schedulers proven correct under churn and under one shared behavioural
suite — not a comparison, and not evidence that dynamic balancing beats static sharding. Treat every
throughput number produced so far as a sanity check on one scheduler at a time, not as an answer to
§1's question. The single measured B-vs-D difference is the starvation asymmetry in §6.1, and it
varies worker count alongside the scheduler.

| Limitation | v1 stance | Upgrade |
| --- | --- | --- |
| **Per-key ring is bounded ⇒ a full key blocks the single producer entirely** (head-of-line across all keys) | accept, measure — `TryDispatch` (§5.4) makes the cost visible | bigger chunks → v2 chaining |
| `batchBound` is a tuning knob | accept; defaults to `chunkSize` | none; inherent to in-place drain |
| Rigid memory (`liveKeys × chunkSize`) | accept | v2 chaining makes chunks fungible |
| Capacity model (`keyCapacity` vs chunk count vs `chunkSize`) not pinned down | deliberately left to evolve once measured | — |
| Terminology: "actor" is used loosely here and means the *worker* in the shipped `Threading.Workers` README | deliberately parked; v1 has no per-key state, so nothing turns on it yet | resolve when per-key state lands |
| Single producer | accept | out of scope |
| Fixed worker count, no idle-timeout exit | accept; instrument park rate | dynamic sizing once measured |
| **The B-vs-D benchmark has not run** — the experiment's question (§1) is open | accept for Phase 2b | Phase 3 (§13 step 6) |
| **D's `DequeCapacity` is very nearly a dead knob** — the deque is a depth-1 handoff slot, so the spill path is unreachable through this pairing (§6.2) | accept; do not tune it | a scheduler that pushes more than one key locally, or none |
| **D at `workers: 1` has B's starvation hazard with no bound to fix it** (§6.1 caveat 1) | accept; document | a local-drain bound on D, if a single-worker D is ever a real configuration |
| **Two lost-wakeup races are undefended by tests** — see below | accept; reasoned, not guarded | the probe hook described below |

### Undefended by tests — two lost-wakeup races in D's park protocol

Neither is a known bug. Both are places where the defence is the reasoning in the XML docs plus
review, and where **the test suite has been measured to have no power to catch a regression.** Recorded
so that a green mutation run is not mistaken for coverage.

#### (i) `WakeOne`'s `Interlocked.MemoryBarrier()` — redundant on arm64, load-bearing on x86-64

The fence orders the enqueue's publication before the parked flags are read; without it we can see
nobody parked and skip the wake while the sleeper's own armed recheck sees a not-yet-visible queue and
blocks.

**A mutation test removing this fence on Apple Silicon comes back clean for structural reasons, and
that is not evidence the fence is dead code.** On arm64, RyuJIT lowers `Volatile.Write`/`Volatile.Read`
to `stlr`/`ldar`, and ARMv8 release/acquire is RCsc — an `ldar` may not be reordered ahead of an earlier
`stlr`. Both publish paths (`GlobalKeyQueue.TryEnqueue`, `WorkStealingDeque.TryPushBottom`) end in a
`Volatile.Write` and the first flag read is a `Volatile.Read`, so on this hardware the fence is
**redundant** and removing it *cannot* produce a lost wakeup. Measured: 0/70 runs detected. It is
load-bearing on **x86-64**, where those lower to plain `mov`s and the store buffer permits exactly the
StoreLoad reordering this fence forbids. **Keep it**, and state the architecture whenever quoting a
mutation result here — this experiment has already been run once and nearly drew the wrong conclusion.

#### (ii) `ArmWait`'s statement order — the experiment cannot resolve the rate it would need to

`ArmWait` must `Reset()` the event *before* raising the parked flag; reversed, a `Set` landing between
the two is erased by the `Reset` and that wakeup is lost. Mutation-testing the reversal measured
**3 failures in 80,000 mutated trials against 2 in 80,000 baseline trials** (four paired 10,000-trial
runs, both TFMs).

State the bound honestly: against a background rate of 2/80,000, rejecting at 95% one-sided confidence
needs ≈6 events and 80% power needs ≈10, so **this design excludes a rate above roughly 10⁻⁴ per trial
and says nothing at all below it.** It is *not* evidence that the race sits at 10⁻⁵.

There is a physical reason the experiment has no power, which makes "undefendable by this test design"
a certainty rather than a guess: **the arrival floor is the main thread's `started.Wait()` kernel
wake-up latency** — tens of microseconds — while the worker reaches `ArmWait` and then `Wait` nanoseconds
after signalling. The timing sweep adds 0–100 µs *on top of* that floor, so essentially every trial
arrives after the worker has already parked. 3-vs-2 is the expected result, not a sampling accident.

**The remedy that would work, deliberately not built:** a test-only `internal Action? ArmWaitProbe` on
the scheduler's `State`, invoked between `ArmWait`'s `Reset()` and its `Parked` write. Under the
reversal the probe sits between `Parked = true` and `Reset()`, so a ~1 µs spin inside it lets a
concurrent `Schedule` `Set` the event and then have that `Set` erased — a **deterministic hang** rather
than a ~10⁻⁵ event, at the cost of one null-check per park, off the per-message path. Recorded as a
concrete design so this limitation is not a vague gesture.

> **At the system level the reversal is masked by design.** `ActorSystem.WaitForWork` does
> `ArmWait` → `Interlocked.MemoryBarrier()` → recheck `HasWork`/`_stopped`/`IsQuiescent` → `Wait`, so
> a `Set` erased by a reversed `ArmWait` is caught by that recheck. That masking is *why* the mutation
> is so hard to observe end-to-end — and also why the ordering inside `ArmWait` must not be relaxed on
> the grounds that "nothing failed".

### Known defects — STALE, both were addressed before this plan's base commit

> **Read this before acting on either entry below.** Both were written against Phase 2a and both have
> since had their prescribed fixes shipped — (a) in `c2f9506`, (b) in `658ae62` — *before* the Phase 2b
> plan's base commit. **Phase 2b did not re-verify either**, so neither is declared closed here; what is
> recorded is that **the stated mechanism no longer matches the code.** Each entry now carries a note
> naming the shipped fix sites. Re-verify against the code before treating either as a live defect, and
> in particular do not file a genuine new failure under (a) — its "intermittently red test suite" claim
> is no longer true (see below).

#### (a) Lost wakeup on the chunk-exhaustion backpressure path — FIXED in `c2f9506`; the text below describes the pre-fix code

> **Status.** Both prescribed fixes are shipped. `WaitForChunk` re-runs `Harvest()` inside its armed
> window (`ActorSystem.cs:296-303`), and `ReleaseAndMaybeSchedule` reads `_producerParked` **after**
> `Interlocked.Exchange(ref cell.State, CellState.Free)` and before the tail-check branch
> (`ActorSystem.cs:512-526`), with an in-code comment naming this defect. **The heading's original claim
> that this "currently causes an intermittently red test suite", and the body's "roughly one run in three
> under load", are both false as of this commit** — `Harvest_Racing_Release_Never_Strands` ran green in
> roughly fifteen full-suite runs across Phase 2b. The mechanism below is kept only because it explains
> *why* the two fix sites are where they are.
>
> **Consequence for §5.4: that section's known-debt blockquote is now over-broad.** It says the worker's
> wake sites publish `Head` and then read `_producerParked` without StoreLoad ordering. The **release**
> site no longer does — `Interlocked.Exchange` is a full fence and the `_producerParked` read now sits
> after it, which retroactively orders the earlier `Volatile.Write(ref cell.Head, …)` against that read.
> What still carries the debt is only **`Activate`'s mid-batch flush** (`ActorSystem.cs:480-487`), where
> the per-32-message `Volatile.Write(cell.Head, …)` is followed by a plain `_producerParked` read with no
> fence between them. That one is real, still bounded by the 20 ms poll, and deliberately unfixed
> (out of scope for Phase 2b). Keep it recorded; drop the claim that it covers the release path too.

`WaitForChunk` (§5.4 source: `ActorSystem.BindChunk`'s slab-exhaustion path) arms the producer's parked
flag and blocks **without re-running `Harvest()` inside the armed window**. Its own comment argues no
recheck is needed because "only a harvest adds a chunk, and only this thread harvests" — true, but it
answers the wrong question. The predicate that actually matters is "has some cell reached `Free` since
my last scan", and that predicate *is* producer-visible and can change while `WaitForChunk` is arming.

Compounding it, `ReleaseAndMaybeSchedule` reads `_producerParked` **before** it publishes
`Interlocked.Exchange(ref cell.State, CellState.Free)`. So the interleaving is: the producer's last
`Harvest()` scan finds nothing free and returns to `BindChunk`; the worker finishes a key and reads
`_producerParked` as `false` (the producer has not armed yet) and skips the `Set`; the worker then
publishes `Free`; the producer arms, fences, finds no recheck to run, and sleeps a full 20 ms poll with
a harvestable chunk already sitting there.

Liveness is safe — the bounded 20 ms poll guarantees the producer eventually wakes and re-scans — so
this is a **throughput and tail-latency defect, not a correctness one**. It reproduces as
`Harvest_Racing_Release_Never_Strands` (`tests/…/Threading/Actors/ActorSystemStressTests.cs`) failing
roughly one run in three under load, wedging around 100,000–120,000 of 128,000 dispatches against a
90 s budget. That test's failure message ("likely a stranded key") is misleading — nothing is
stranded; the producer is simply asleep for up to 20 ms at a time with harvestable work available,
which is enough to blow the budget under contention.

**The fix**: re-run `Harvest()` inside `WaitForChunk`'s armed window (mirroring `WaitForMailboxSpace`'s
recheck shape), and move the worker's `_producerParked` read in `ReleaseAndMaybeSchedule` to
**immediately after** the `Interlocked.Exchange(ref cell.State, CellState.Free)` and **before** the
tail-check/reschedule branch — reading it after the reschedule CAS instead would let `State` already be
back to `Scheduled` by the time the flag is checked, making the wake a no-op. Bonus: because
`Interlocked.Exchange` is a full fence, moving the `_producerParked` read after it also retroactively
StoreLoad-orders the earlier `Volatile.Write(cell.Head, …)` in the same method — which likely closes the
separately-recorded `WaitForMailboxSpace` debt (§5.4's known-debt blockquote) as a side effect, since
that debt is exactly the missing StoreLoad ordering between a worker's `Head` publish and its
`_producerParked` read.

#### (b) The router's fault path converts a process kill into a silent hang — FIXED in `658ae62`; the text below describes the pre-fix code

> **Status.** The root gap this entry names — *"`IKeyScheduler` has no fault channel back to
> `ActorSystem`"* — no longer holds. `Start(int workerCount, Action<Exception> onFault)` ships
> (`IKeyScheduler.cs:31`), `RoutedScheduler.RouteLoop` invokes `OnFault` first in its `catch`, before its
> own stop-and-signal cleanup, so the exception is recorded before anyone can observe `Stopping`
> (`RoutedScheduler.cs:179`), and `ActorSystem.Fault` records `_completionError`, sets `_stopped`,
> calls `SignalStop()` and sets `_spaceAvailable` (`ActorSystem.cs:336-341`). So the specific mechanism
> below — workers re-parking forever on a flag nothing reads — is broken at its first step: `_stopped`
> *is* now set. **Not declared closed:** Phase 2b did not re-verify the end-to-end behaviour, and
> `FaultPropagationTests` was not re-read as part of this write-up. Re-verify before relying on it.

`RoutedScheduler.RouteLoop` (§6.1) now catches an exception from its own body, sets its `Stopping`
flag and signals every wait primitive it owns — a real improvement over the exception escaping and
killing the process. But `Stopping` is scheduler-private state; `IKeyScheduler` has no fault channel
back to `ActorSystem`, so `ActorSystem._stopped` is never set and stays `false`. Workers wake once from
the router's signal, find `IsQuiescent()` false (the key the router was routing when it faulted is
never delivered, so its messages are never processed and the dispatched/processed sum never converges),
and re-park on an unbounded `Wait` — nothing will ever signal that event again, since the router thread
has already returned. `Completion` never resolves.

Net improvement in blast radius over the pre-fix behaviour (one dead router used to take the whole
process down; now it only wedges this one `ActorSystem`), but the stated goal — "nobody left parked" —
is not achieved. The root gap is structural: `IKeyScheduler` needs a fault channel to
`ActorSystem`'s `_completionError` / `_completion`, the same escape hatch `RunLoop`'s own `try`/`catch`
already has for a worker-thread fault, so a scheduler-owned background thread's fault can actually
resolve `Completion` (faulted, not hung) instead of dead-ending on a flag nothing reads.

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

1. ~~Slab + chunk free stack, with tests.~~ **Done.**
2. ~~Key cell map (fork of `CoalescingKeyMap` with mailbox cursors), with tests.~~ **Done.**
3. ~~Cell state machine + dispatch/activation/release, single worker, no scheduler
   (tests 1, 2, 3, 5, 6, 11, 12).~~ **Done.**
4. ~~Scheduler seam + **B**, N workers (tests 4, 7, 10; re-run 1–3 under B).~~ **Done** (Phase 2a).
5. ~~**D**: global queue, Chase-Lev deque, steal scan, parking (test 8; re-run all under D).~~
   **Done** (Phase 2b) — §6.2 records the four deviations, the depth-1 deque finding, and the
   retry cost; §9 records what test 8 measured.
6. Harness + baselines; B-vs-D measurement. **← the only open step.**

Steps 1–3 carried the correctness risk; 4–5 carry the performance question — and **step 6 is what
actually answers it.** Phase 2b makes D exist and proves it correct; it does not measure D against B.
Nothing above step 6 is evidence for or against either scheduler.
