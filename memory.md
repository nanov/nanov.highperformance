# Decision & learnings log

Durable, human-curated record of decisions, trade-offs, and hard-won findings worth keeping.
This is the *promoted* subset of session work — not an auto-log. Read it before revisiting
architecture, throughput, or concurrency choices so we don't re-litigate settled ground.

**Append newest at the top.** Keep entries terse: what was decided/learned, why, and the
implication ("so don't…"). If something becomes a permanent rule, move it into `CLAUDE.md`.

---

## 2026-07-10 — Two spotted concurrency bugs fixed (Conflation NRE + Dispatch/TryComplete race)

Both pre-existing, surfaced during the throughput work; fixed TDD (red repro → fix → green), verified on
net9.0 + net10.0 (Release + Debug), 5× flake check. Fable 5 reviewed bug #1's fix and CONFIRMED it.

- **Bug #1 — `ConflationSpWorker` NRE (`taken == null` at the `Handle` call).** Root cause: the producer's
  `Volatile.Write(cell.Value, v)` and its `Interlocked.Exchange(cell.State, Available)` are **two steps**,
  and the consumer's per-drain **null-clear** (`CompareExchange(cell.Value, null, taken)`) could null a
  value the producer had written but **not yet published** — the redelivered ring entry then read `null`.
  Fires on both producer branches (`prev==Free` and `prev==Consuming`); reference-identity can't tell a
  stale value from a written-but-unpublished one, so **no** consumer-side clear ordering is safe.
  - **Fix:** the consumer **no longer writes `cell.Value` at all** (only CASes `State Consuming→Free`).
    Delivery is state-machine-driven, so correctness is unchanged; `taken` is now provably non-null.
  - **Trade-off (accepted):** dropped eager GC-null-on-drain. A drained reference is released when the
    producer overwrites it (key re-dispatched) or the cell is evicted (`EvictFree`/`Place` reset). Fable's
    3-leg protocol *can* restore eager clear but costs +1 interlocked CAS/item — **not worth it**, don't
    add it without a measured reason. `TValue` carriers must still be per-dispatch fresh (same-ref reuse
    is ABA-fragile and unsupported).
  - Regression net: `Sustained_Load_Never_Delivers_A_Null_Value` + `Ref_Value_Slots_Are_Released_On_Eviction`.

- **Bug #2 — Dispatch racing TryComplete stranded an item (was the L3 backlog note).** A producer parked in
  `WaitForSpaceSlow` only re-checked `_stop`/`_completed` *inside* the while-full loop, and `TryComplete`
  didn't `Set` `_spaceEvt` (violating the "shutdown Sets everything" invariant). So a soft-complete could
  leave the parked producer hung, or let it wake on freed space and commit an item into a ring whose
  consumer had already exited (silent loss).
  - **Fix:** `TryComplete` now `_spaceEvt.Set()`s too; `WaitForSpaceSlow` re-checks `_stop || _completed`
    after the wait loop and returns false. A dispatch racing soft-complete is rejected, never stranded.
  - Regression net: `TryComplete_Unblocks_A_Producer_Parked_On_A_Full_Ring` (+ `SignalingStressTests`).

## 2026-07-10 — Lock-step squeeze L3: batched head publish in the FIFO drain loops

The four FIFO workers publish Head every `max(1, min(32, capacity/4))` items / at run boundaries /
immediately when `ProducerParked` is up, instead of per item (slots still cleared per item).
`HandleScope.Release` still publishes immediately; only auto-releases defer. Worst-case producer
wait coarsened from one slot-free to ONE HANDLER DURATION (accepted trade-off, documented).
Interleaved L2↔L3 A/B: deltas within the ±10–15% noise floor (untouched tests moved as much); no
confirmed regression, no measurable win in the saturated harness regime — kept on the
standard-technique basis (Disruptor batch-release; the win regime is streaming consumers, which
this harness cannot exercise). Known overhead mechanism if it ever matters: the saturated path
reads ProducerParked twice per item (ShouldFlush + PublishHead).
- Never remove the run-boundary flush (empty ring must imply all-published — deadlock otherwise).
- Keyed workers stay on per-item TryTakeAndAdvance.
- Backlog (pre-existing, found in final review): a Dispatch racing TryComplete can slip an item past the exiting consumer — WaitForSpaceSlow only rechecks _completed while the ring is still full, and TryComplete doesn't Set _spaceEvt. **RESOLVED 2026-07-10 (bug #2) — see the top entry.**

## 2026-07-10 — Lock-step squeeze L2: parked-flag signaling replaces per-item cross-index edge reads

`CommitTail` no longer reads `Head` per commit and `AdvanceHead` no longer reads `Tail` per item:
each side raises a padded parked flag on slow-path entry (before its spin/arm phase) and the
committer signals only when the flag is up — "a signal may be spurious, never lost". Sync paths
add one full fence per kernel park (cold); the async commit fence is unchanged. macOS perf delta
vs L1: mean throughput SpWorker +420% (18.9→98.0 Mops), ValueSpWorker +321% (19.1→80.4),
AsyncSpWorker +329% (15.1→65.0), AsyncValueSpWorker +205% (17.3→52.9); Coalescing flat (−3%,
noise); latency flat-to-better (ping-pong median 0.83→0.50–0.63 µs). Confirmed by interleaved
L2↔L3 runs: SpWorker ~126 Mops mean at L2 vs ~19 Mops at L1 — the layer with the step-change win.
- Don't reintroduce the index-edge Set test; don't gate SignalStop/TryComplete on the flags; don't
  collapse arm-then-recheck.

## 2026-07-10 — Lock-step squeeze L1: bounds-check elimination; lifecycle-flag fold dropped

All ring-slot accesses now go through `SlotRef` (`MemoryMarshal.GetArrayDataReference` +
`Unsafe.Add`), removing the per-access bounds check (safe by construction: `_mask = length − 1`,
power-of-two length). macOS perf delta: mixed variance within noise (-17% to +4%). Kept on the
standard-technique basis.
- The spec'd optional fold of `_completed || _stop` into one bitfield was **dropped**: two volatile
  bool loads on a warm line aren't worth converting the lifecycle writes to Interlocked bit ops.
  Don't revisit without a measured reason.

## 2026-06-28 — Throughput gap vs Disruptor is architectural, not a bug

Disruptor ~325 Mops vs worker ~35 Mops. Root cause: **design philosophy, not a missing
optimization.** Disruptor lets the consumer lag intentionally (3k–8k-item batches) → high
throughput; the worker runs lock-step (2–3-item batches) for low latency. Shared cache-line
contention between producer writes and consumer spin-wait on `_idx.Tail` adds a feedback loop
that suppresses producer throughput.
- **Implication:** can't maximize throughput *and* latency on the same code path. Don't "fix"
  the gap by bolting tweaks onto the latency path.
- macOS / Apple-Silicon micro-benchmarks are unreliable (no pinning, thermal decay) — don't
  draw definitive conclusions from them.

## 2026-06-28 — `aggressiveSpin` is a latency-mode opt-in, not a default

Added `Internal/AggressiveSpinWait.cs` (`SpinOnce(-1)`) and an opt-in `aggressiveSpin` flag on
`SpWorker` / `ValueSpWorker`. In throughput tests the **blocking strategy beat aggressive spin**.
- **Implication:** keep blocking as the default; treat `aggressiveSpin` as a latency-mode option
  only. A real throughput mode would need a different design (consumer-lag + batch accumulation).

## 2026-06-27 — Conflation worker adopts cell-based nexus-notify design

Replaced the lock-free ring-override design with a **cell-based nexus-notify** architecture for
message conflation (last-wins).
- **Implication:** ring-override approach is abandoned; don't reintroduce it for conflation.

## 2026-06-27 — NuGet release uses OIDC Trusted Publishing

Release workflow migrated to OIDC Trusted Publishing (no long-lived API key in CI).
