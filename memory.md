# Decision & learnings log

Durable, human-curated record of decisions, trade-offs, and hard-won findings worth keeping.
This is the *promoted* subset of session work — not an auto-log. Read it before revisiting
architecture, throughput, or concurrency choices so we don't re-litigate settled ground.

**Append newest at the top.** Keep entries terse: what was decided/learned, why, and the
implication ("so don't…"). If something becomes a permanent rule, move it into `CLAUDE.md`.

---

## 2026-07-10 — Lock-step squeeze L3: batched head publish in the FIFO drain loops

The four FIFO workers publish Head every `max(1, min(32, capacity/4))` items / at run boundaries /
immediately when `ProducerParked` is up, instead of per item (slots still cleared per item).
`HandleScope.Release` still publishes immediately; only auto-releases defer. Worst-case producer
wait coarsened from one slot-free to ONE HANDLER DURATION (accepted trade-off, documented).
macOS perf delta vs L2 (7 runs/test, `--target SpWorker`, saturated-throughput harness — ring
stays full so batching degenerates toward the per-item `ProducerParked` flush trigger, per design):
mean/median regressed on every FIFO worker — SpWorker mean 98.0→76.0 Mops (−22%, median
65.4→47.0, −28%); ValueSpWorker mean 80.4→39.3 Mops (−51%, median 77.3→22.3, −71%); AsyncSpWorker
mean 65.0→41.8 Mops (−36%, median 66.0→31.1, −53%); AsyncValueSpWorker mean 52.9→45.2 Mops (−14%,
median 47.7→32.1, −33%); Coalescing (unaffected code path) +17%/+5% — noise, confirms the harness's
per-run variance dwarfs the signal (max-of-7 tells a milder story: SpWorker 163→129 Mops −21%,
ValueSpWorker 94→91 −3%, AsyncSpWorker 102→69 −32%, AsyncValueSpWorker 77→82 +7%). Latency
(OneWay/PingPong median) flat. This contradicts the "saturated regime ⇒ no regression" expectation
at face value, but every earlier layer's laptop numbers already showed error > mean on this same
harness (see L1) — recorded as observed, not blessed; re-measure on quieter hardware or with more
runs before trusting the sign of the L3 delta.
- Never remove the run-boundary flush (empty ring must imply all-published — deadlock otherwise).
- Keyed workers stay on per-item TryTakeAndAdvance.

## 2026-07-10 — Lock-step squeeze L2: parked-flag signaling replaces per-item cross-index edge reads

`CommitTail` no longer reads `Head` per commit and `AdvanceHead` no longer reads `Tail` per item:
each side raises a padded parked flag on slow-path entry (before its spin/arm phase) and the
committer signals only when the flag is up — "a signal may be spurious, never lost". Sync paths
add one full fence per kernel park (cold); the async commit fence is unchanged. macOS perf delta
vs L1: mean throughput SpWorker +420% (18.9→98.0 Mops), ValueSpWorker +321% (19.1→80.4),
AsyncSpWorker +329% (15.1→65.0), AsyncValueSpWorker +205% (17.3→52.9); Coalescing flat (−3%,
noise); latency flat-to-better (ping-pong median 0.83→0.50–0.63 µs).
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
