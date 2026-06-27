# ConflationSpWorker redesign — twin on the coalescing map (3-state presence, not a lock)

Date: 2026-06-27
Status: Design — pending review

## Problem

The current `ConflationSpWorker` rides its own `ConflationSpRingCore` — a two-array FIFO ring
(`TKey[]` + `TValue[]`) whose producer overrides an already-queued value slot **in place**, inside
`[head, tail)`. Keeping that override lossless against a concurrent consumer needs a full Dekker
protocol: a per-override `Interlocked.MemoryBarrier()`, a `_reading` flag the consumer raises around
every take, ordered `_reading`-then-`Head` reads, and a doubt-path fresh re-enqueue.

Benchmarks (capacity 32, 100k msgs, net10.0) show this isn't earning its keep — the conflation worker
is **1.7–2.2× slower** than the `CoalescingSpWorker` it should beat:

| KeySpace | Work | Coalescing | Conflation | Ratio |
|---|---|---|---|---|
| 32 | 0 | 6.35 ms | 11.32 ms | 1.79× slower |
| 32 | 200 | 0.52 ms | 1.13 ms | 2.16× slower |
| 64 | 0 | 9.63 ms | 15.9 ms | 1.66× slower |

(The 2.4 MB "alloc" is the benchmark boxing a class value per message — inherent to `TValue : class`,
not the worker, which is zero-alloc per dispatch.)

The ring-override machinery — the per-override barrier, the `_reading` fences, the in-place FIFO
override of a moving slot the consumer might be reading — costs more than it saves.

## Key insight

**Last-wins conflation is a degenerate coalesce.** If `Coalesce(current, incoming) => incoming`, the
coalescing worker *is* a conflation worker. So conflation should be the coalescer's **twin**, sharing
its two proven structures:

- `CoalescingKeyMap<TKey, TValue>` — producer-private, **fixed-capacity, no-relocation** open
  addressing; cells (`{Key, Value, State}`) live inline and are reached by a **stable slot index**;
  eviction is a producer-side, **state-gated** in-place tombstone.
- `SpRingCore<int>` — a plain FIFO ring carrying **dirty cell indices** (not values).

The map is already protocol-agnostic: its only coupling to the cell protocol is `EvictFree`'s test
`State == 0 ⟺ "consumer is provably done, safe to reclaim"`. Conflation reuses both structures
**verbatim**; only the worker's per-item protocol changes.

### What last-wins lets us delete vs the coalescer

The coalescer's per-cell lock is 4-state (`Free / Available / Merging / Consuming`). Three of those
exist to serialize a producer **read-modify-write** (`Coalesce`) against the consumer's take. Last-wins
has no read-modify-write — the producer just overwrites `Value` with a **single atomic reference
write** — so:

- **`Merging` state is gone** (no merge to lock), and with it the producer's CAS-to-`Merging` +
  CAS-back and the `AcquireDrained` spin.
- **The `Coalesce` call is gone** — plain overwrite.
- **The consumer's reset-to-`default` is gone** — last-wins never needs an identity; the producer
  overwrites next time. Consequence: **`Value` has a single writer (the producer)**, so there is no
  producer/consumer collision on the value field at all.

### Why not 2 states (what bit the dirty-flag attempt)

A 1-bit `Dirty` flag is cleared at **claim** time (consumer clears before reading, required for
losslessness). So `Dirty == 0` means "claimed-or-done" — it cannot distinguish a consumer **mid-read**
from a consumer **finished**. Eviction must key off "finished", so a 1-bit flag cannot gate eviction →
the producer reuses a slot index the consumer is still reading → **wrong (key,value) pair delivered**.
That was the real race in the dirty-flag snippet (the Dekker-on-`Dirty` argument itself is sound for
last-wins; the slot-reuse race is what's fatal). The fix is exactly one more bit of state.

So the floor is **2 bits of presence**: *queued?* (in the ring / has a fresh value) and *in-use?*
(consumer currently reading). Encoded as 3 reachable states — presence, not a lock.

## Design

### States

Reuse `CoalescingCell<TKey, TValue>` and its `State` word unchanged. Conflation uses three of the
coalescer's existing state constants and **never uses `Merging`**:

- `Free` (0) — drained / never dirtied; evictable.
- `Available` (1) — queued: index is in the dirty ring, `Value` holds the latest. (the coalescer's
  `Available`; "Queued" conceptually.)
- `Consuming` (3) — consumer has claimed the cell and is reading it; not done.

Keeping the **same numbering** as the coalescer (`Free = 0` is the only value `EvictFree` cares about)
means `CoalescingKeyMap` / `CoalescingCell` are shared with zero divergence.

### Producer — `Dispatch(key, value)`

```
ref var cell = ref _map.GetOrAddRef(key, out _, out var index);   // producer-only; may EvictFree to make room
Volatile.Write(ref cell.Value, value);                            // last-wins overwrite — single atomic ref write
var prev = Interlocked.Exchange(ref cell.State, Available);       // full fence; claim "queued"
if (prev == Available) return true;                               // already in ring → conflated in place, done
// prev was Free (never/ drained) or Consuming (consumer dequeued already & is reading) → not in ring → enqueue once
if (_ring.TryEnqueue(index)) return true;                         // TryEnqueue BLOCKS for backpressure (full ring);
Volatile.Write(ref cell.State, Free);                             //   returns false only when completed/stopped, so
return false;                                                     //   this rollback is the shutdown path → Free is safe
```

`SpRingCore<int>.TryEnqueue` blocks the producer when `Capacity` indices are already queued
(backpressure), exactly as in `CoalescingSpWorker`; it returns `false` only when the worker is
completed or stopped. So the rollback above runs only on shutdown, where `Free` (drained) is the
correct terminal state regardless of `prev`.

- Single atomic overwrite, single `Interlocked.Exchange`, at most one enqueue. No `Merging`, no spin,
  no per-override `MemoryBarrier` beyond the `Exchange`'s own fence.
- `prev == Consuming`: the consumer dequeued this index already and is mid-read. We set `Available` and
  enqueue a **fresh** index; the consumer's closing CAS (`Consuming → Free`) will then fail and it
  leaves the cell `Available`, so the fresh ring entry redelivers the latest value.

### Consumer — run loop (in the worker, not the core)

```
ref var cell = ref _map.RefAt(index);
if (Interlocked.CompareExchange(ref cell.State, Consuming, Available) != Available)
    continue;                                                     // spurious (Free/Consuming) → skip
var key = cell.Key;
var value = Volatile.Read(ref cell.Value);                        // latest (acquire — published before Exchange)
Interlocked.CompareExchange(ref cell.State, Free, Consuming);     // fails iff producer re-dirtied during read →
                                                                  //   fresh ring entry exists → leave Available
_handler.Handle(key, value);
```

No `_reading` flag, no per-take `MemoryBarrier`, no value reset.

### Losslessness (last-wins, exactly-once-latest)

The `State` word carries a total order via `Interlocked` on both sides. Cases:

- **Conflation while `Available`** (in ring, not yet consumed): producer overwrites `Value`,
  `Exchange` returns `Available` → no enqueue. Consumer dequeues the single index, reads the latest. ✓
- **Re-dirty during `Consuming`**: producer overwrites, `Exchange` returns `Consuming` → enqueues a
  fresh index. Consumer's closing `Consuming → Free` CAS fails (state is now `Available`) → it leaves
  the cell queued; the fresh entry redelivers the latest. Worst case a harmless duplicate of the
  latest value (fine for last-wins). ✓
- **Normal**: `Free → Available` enqueues once; consumer takes `Available → Consuming → Free`. ✓

### Eviction safety (the "who removes keys" answer)

Unchanged from the coalescer: `EvictFree` (producer-side, cold, only when the map fills) tombstones
**only `State == Free` cells**. A cell is `Free` only after the consumer's successful
`Consuming → Free` CAS — i.e. the consumer is done **and** the producer hasn't re-dirtied it. While an
index is in the ring (`Available`) or being read (`Consuming`), `State ≠ Free`, so the slot is never
reclaimed out from under a live index. Structural map ops stay producer-only; the consumer touches
only `State`/`Value`/`Key` by index via `RefAt`. The slot-reuse race that killed the dirty-flag design
cannot occur.

### `TValue : class` stays

The producer's overwrite is `Volatile.Write(ref cell.Value, value)` — atomic only for a reference (or
native-word) type. A struct `TValue` would tear under a concurrent consumer read. Keep the constraint
(matches today and the `CLAUDE.md` note).

## Components

- **New / rewritten:** `ConflationSpWorker<TKey, TValue, THandler>` — owns a `SpRingCore<int>` (dirty
  index ring) + a `CoalescingKeyMap<TKey, TValue>`, exactly like `CoalescingSpWorker`, but with the
  3-state overwrite protocol above. `Dispatch` and `RunLoop` live in the worker; the dirty ring is the
  unmodified `SpRingCore<int>`.
- **Deleted:** `ConflationSpRingCore.cs`, `ConflationKeyMap.cs` — the whole bespoke override core and
  its producer-private seq map. `SpscVirtualValueDictionary.cs` (empty placeholder) is removed.
- **Unchanged:** `IConflationWorkerHandler` (consumer-side `Handle`/`OnError`/`OnCompleted` — already
  has no merge/drop hook), `CoalescingKeyMap`, `CoalescingCell`, `SpRingCore`. `ISeqKeepPredicate`
  loses its only conflation user; keep it only if still used elsewhere, otherwise delete with
  `ConflationKeyMap`.

## Public API

No change. `ConflationSpWorker<TKey, TValue, THandler>` keeps its constructor, `Start`, `Dispatch`,
`TryComplete`, `Dispose`, `Completion`, `QueuedApprox`, `Capacity`, and `IWorkerQueue`/`IDisposable`.
Semantics unchanged (last-wins, exactly-once-latest, blocking backpressure at `Capacity` distinct
pending keys, zero per-message worker allocation).

## Backpressure & lifecycle

Inherited from `SpRingCore<int>` (the same ring the coalescer uses): a new dirty key blocks when
`Capacity` keys are already pending; conflating an already-pending key never blocks. Hard stop drops
queued cells; `TryComplete` drains them. `Start`/`Completion`/cancellation behave as today.

## Testing

Port `ConflationSpWorkerTests` to the new worker (semantics are identical, so the existing assertions
should hold). Add:

- **Conflation-in-ring**: dispatch K=v1, K=v2 before the worker drains → exactly v2 delivered once.
- **Re-dirty-during-consume**: a handler that blocks lets the producer overwrite mid-`Handle` → latest
  delivered, at most a harmless duplicate of the latest, never a lost update.
- **Eviction under churn**: more distinct keys than slots, drained between bursts → no wrong
  (key,value) pairing, no `InvalidOperationException` from a full map.
- **Backpressure**: `Capacity` distinct pending keys blocks a new key; an in-flight key does not.
- Reuse the `Rec`/`RecordingState`/`RecordingHandler` pattern and the
  `AwaitCompletionOrStall`/`AwaitProgress` watchdogs.

## Verify

- `dotnet build -c Release` → net9.0 + net10.0, 0 warnings.
- `dotnet test -c Release` and `dotnet test -c Debug` (overlap guard).
- `dotnet run -c Release --project benchmarks/... -- --filter '*ConflationWorker*'` — expect conflation
  to land at/below the coalescer (no merge lock, no `_reading` fences).

## Docs

Update `Threading/CLAUDE.md` and `Threading/README.md`: replace the "ConflationSpWorker invariants"
section (sibling override core + Dekker) with the twin-on-the-coalescing-map description (shared map +
dirty ring, 3-state presence, `Merging`-free). Note that `ConflationSpWorker` and `CoalescingSpWorker`
now share `CoalescingKeyMap`.
