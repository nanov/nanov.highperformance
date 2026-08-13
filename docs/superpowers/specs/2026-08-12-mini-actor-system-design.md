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
> Separately, `Activate`'s unconditional `Interlocked.Exchange(State, Owned)` still does not verify
> it won the key before taking ownership. That is fine with one worker (there is no second claimant
> to lose to) but **must become a validating claim before a second worker exists** — with two
> workers racing to acquire the same popped index, both could otherwise drain one mailbox from the
> same `Head`, destroying per-key order. That remains a Phase 2 entry criterion.

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
completion, and §8 already plans these counters.

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
   broadcast. Ordering on that thread: publish → observe the sum is complete → read `_completed`
   → wake all.

   > **Phase 1 does not implement this edge, and correctly so.** With a single worker, the only
   > thread that could observe quiescence is the one already evaluating the predicate on its own
   > idle path, so there is nobody to wake. It becomes load-bearing the moment a second worker
   > exists: worker A publishes the count that completes the sum while workers B…N sit parked, and
   > without the broadcast they park forever. **The scheduler-seam task must add it** — it is the
   > single most likely way Phase 2 hangs.

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

```csharp
interface IKeyScheduler {
  void Schedule(int cellIdx);                       // producer first-dirty, or worker self-reschedule
  bool TryAcquire(int workerId, out int cellIdx);   // non-blocking; may steal
  bool WaitForWork(int workerId);                   // park (arm-then-recheck, §5.6); false ⇒ exit
  void WakeAll();                                   // unconditional broadcast — shutdown only
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
> Phase 1 ships as-is (single worker, hazard recorded). The scheduler-seam task must bound
> consecutive local draining — check the ring every K local activations, or whenever the local list
> holds only the index just released — and the B-vs-D benchmark should include a
> hot-key-plus-starved-key workload so the difference is measured rather than argued.

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

### 6.3 Expected outcome

B should win on low skew (fewer atomics, one predictable hop). D should win on skew and variable
cost (idle workers self-serve instead of waiting for a router judging by a stale `QueuedApprox`).
**Steal rate is the number that says which regime you are in** — hence the counters in §8.

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
  workers: 8,
  keyCapacity: 1 << 12,     // live keys
  chunkSize: 64,            // messages per key, v1 bound
  batchBound: 64);          // max per activation; defaults to chunkSize

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

The honest expectation: this substrate is **more expensive per message than a single `SpWorker`**,
and per §4.5 the cost is **two random accesses per message** (cell + chunk) against `SpWorker`'s one
sequential stream — the price of key affinity, not of the map. The question is therefore not whether
it beats one worker, but whether N-way dynamic balancing beats N statically sharded workers under
skew.

Because the gap is memory-access-shaped, key cardinality is the axis that will move it most: at low
cardinality the cells and chunks stay resident and the gap should mostly close; at high cardinality
it will not. Measure cardinality independently of skew for that reason.

## 11. Known limitations and upgrade path

| Limitation | v1 stance | Upgrade |
| --- | --- | --- |
| **Per-key ring is bounded ⇒ a full key blocks the single producer entirely** (head-of-line across all keys) | accept, measure — `TryDispatch` (§5.4) makes the cost visible | bigger chunks → v2 chaining |
| `batchBound` is a tuning knob | accept; defaults to `chunkSize` | none; inherent to in-place drain |
| Rigid memory (`liveKeys × chunkSize`) | accept | v2 chaining makes chunks fungible |
| Capacity model (`keyCapacity` vs chunk count vs `chunkSize`) not pinned down | deliberately left to evolve once measured | — |
| Terminology: "actor" is used loosely here and means the *worker* in the shipped `Threading.Workers` README | deliberately parked; v1 has no per-key state, so nothing turns on it yet | resolve when per-key state lands |
| Single producer | accept | out of scope |
| Fixed worker count, no idle-timeout exit | accept; instrument park rate | dynamic sizing once measured |

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
