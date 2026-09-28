# Decision & learnings log

Durable, human-curated record of decisions, trade-offs, and hard-won findings worth keeping.
This is the *promoted* subset of session work — not an auto-log. Read it before revisiting
architecture, throughput, or concurrency choices so we don't re-litigate settled ground.

**Append newest at the top.** Keep entries terse: what was decided/learned, why, and the
implication ("so don't…"). If something becomes a permanent rule, move it into `CLAUDE.md`.

---

## 2026-09-28 — Mini actor system: the `Activate` mid-batch-flush StoreLoad debt is closed

- **Fixed with a fence, not a spin-before-park.** `Interlocked.MemoryBarrier()` between the flush's
  `Head` publish and its `_producerParked` read — same shape as `RoutedScheduler.Schedule`. A spin only
  shrinks the lost-wakeup window; the fence closes it. Every worker wake site is now publish → full
  fence → read the parked flag. **Don't remove any of them to "save a fence"** — the failure is a
  silent 20 ms tail cliff, not a hang, so no test catches it.
- **Cost: in the noise where it was measured.** One fence per 32 messages; `ActorsDUniformCard64`, 5
  runs each, Apple Silicon: 4.75 vs 4.78 Mops/s, spread 4.51–4.98. Consumer-bound cell, arm64 only —
  the defect is live mainly on x86-64 (store buffer), where no number was taken.
- **No regression test, deliberately.** The race needs x86-64 StoreLoad reordering; on this host a
  test would pass with or without the fence (see the Phase 2b "green mutation run is not coverage"
  entry).

## 2026-08-17 — Mini actor system Phase 4: the wait ladder became a measured variable; the Helios-shaped ladder cost ~18% in the churn regime; 4b decided against

**This is an experiment** (`src/experiments/mini-actor-system`), not a shipping decision, and the
result below is **one converged run, one host, one operating point**. Branch
`experiment/mini-actor-system`, commits `16ae9e1` (seam) → `0659083` (bounded block) → `1fedbf9` /
`df0b44e` (backoff ladder + three-arm paired cell). Apple M4 Pro, 12 logical cores, macOS at load
~4.5; scheduler D only; 8 workers, 1 producer; cardinality 1024, uniform; `SpinPerMessage = 0`; 30M
messages per arm; 9 rounds, three arms interleaved with rotating lead; drain-only timer;
`DOTNET_TieredPGO=0`, one cell per process. Detail: spec §6.4 (result) and §10 (configuration).

- **The seam is a no-op by construction, which is the only reason the comparison means anything.**
  `IWaitPolicy` is an interface-typed constructor parameter (deliberately *not* a devirtualised type
  parameter like every other seam here — idle path only, and a fifth type parameter would churn every
  call site for no measured gain). `LegacyWaitPolicy` reproduces the pre-phase ladder exactly and is
  the default, so **no call site changed and the Phase 3 baseline did not move**.
- **Bounding the idle block is a SAFETY change first.** It turns a lost wakeup from a permanent hang
  into a latency hiccup — the two undefended lost-wakeup races are still there, only their cost
  changes. Measured cost on this run: `Legacy → Bounded` median **1.001**, range 0.815–1.255, i.e. it
  **spans 1.0** and nothing was measurable. **But the cost `BoundedWaitPolicy`'s own doc names was
  never exercised**: every cell runs a saturated producer where the 20 ms timeout essentially never
  fires. The idle-system wake cost needs a hold-N-workers-idle test that does not exist. **So: don't
  quote "the bound is free" — the regime that would price it wasn't run.**
- **The Helios-shaped ladder cost ~18% in the churn regime as measured here.** `Legacy → Backoff`
  median 0.836 (0.657–0.928), `Bounded → Backoff` 0.824 (0.729–0.962) — the first ratios in the phase
  whose range **excludes 1.0**, with Backoff slowest in **9 of 9 rounds** and same-arm spread down to
  ~1.23×. Mechanism, explicable rather than an artifact: Legacy spins 32×64 = 2048 iterations (~2 µs)
  before it ever blocks; Backoff's whole ladder is 319 iterations (~0.3 µs), then a yield, then a
  block. At spin 0 the system is producer-bound — workers starve, hit the ladder constantly, and work
  arrives frequently in small amounts, so spinning longer catches the next message without a kernel
  round trip while blocking sooner pays that round trip repeatedly. **The same argument says the sign
  is regime-dependent: this is not "exponential backoff is slower".**
- **THE TRANSFERABLE ONE — a headline statistic that flips SIGN between runs has no signal in it, no
  matter how tidy its median.** `Bounded → Backoff` came back 0.813, then 1.066, then 0.701 at spin
  32. Three attempts, three tidy medians, two directions. **And measuring at a knee is measuring a
  coin flip:** spin 32 is the calibration knee where the system flips producer-bound ↔ consumer-bound,
  and Phase 3 had already recorded the knee *moving between passes* (16 → 32). That bistability is a
  property of the operating point, not merely host noise. **So: pick a point on one side of the knee
  and say which side; and treat sign-flipping as "no result", never as "average them".**
- **A bias that counterbalancing is STRUCTURALLY unable to cancel: tiered-PGO devirtualisation.** The
  policy field is a plain interface and `WaitForWork`'s call site is JITted once per process, so
  whichever concrete policy that site sees *first* sets the class-profile guard every other policy
  then fails — un-inlined — for the life of the process. Rotating the execution order cannot help,
  because a process has only one "first ever" call. Fix is an invocation contract:
  `DOTNET_TieredPGO=0`, one cell per process, `--runs 1`. **Generalises: A/B-ing two implementations
  of an interface inside one process is confounded by PGO before you write a line of harness.**
- **Three more harness traps, each of which produced a wrong or unusable answer first.** (1) The perf
  selector sorts cells by type name ascending, so on a host whose load drifts, one arm systematically
  gets the quieter window — **a confound with a known sign**; the fix is interleaving arms *within* a
  round and comparing paired samples, not running them separately. (2) A two-arm comparison could not
  attribute anything, because the new policy changed **three** things at once (rungs, yield tier,
  bounded block) — a control arm was mandatory, and `Bounded → Backoff` is the ratio that answers the
  question actually asked. (3) The timer bracketed system construction, eight thread starts and
  disposal; at 10M/arm (~3 s) that variable overhead was a large share of every sample and two runs
  disagreed on the sign. **Time the thing you are comparing, and make the measured window long enough
  that fixed overhead's *variance* is small — not just its mean.**
- **Two B-vs-D asymmetries, argued from the code and previously unrecorded.** B creates one thread of
  its own (the router); D creates **zero**, so at 8 workers B is 10 threads to D's 9 — a bias against
  B, which is why it does not explain Phase 3's null away, but it belongs beside those numbers. And
  `IWaitPolicy` governs the **worker park path only**: all of D's idle path, but only part of B's,
  whose router still parks unbounded on `InboxNotEmpty`. **So: a B-vs-D difference under a new wait
  policy may be a coverage difference in what the policy reaches, not a scheduling difference.** The
  B-vs-D re-test was not run in Phase 4, so Phase 3's null stands exactly as measured.
- **Phase 4b (spinner/waiter preference) is decided against as the next step — a decision, not a
  deferral, and contingent on what has been measured.** It is the reference design's most valuable
  property and it cannot live behind this seam: the waker must know who is spinning, which means
  changing `WakeOne`, the meaning of the `Parked[]` flag, or replacing per-worker events with a shared
  unfair semaphore — **the same code as both undefended lost-wakeup races**, i.e. the most dangerous
  edit in this tree. The ladder work did not pay in the one regime that converged, so there is no
  measured basis for taking that risk next. **What would change it:** a regime where blocking sooner
  wins (idle/bursty rather than saturated, or consumer-bound past the knee), or park/unpark counters
  showing kernel wakes are a material share of the cost.
- **Confirmed again, already promoted below (2026-08-14): a mutation must remove the defective
  behaviour's *arbitrating effect*, not just its reported result** — override a return value while
  leaving the atomic RMW in place and the defect narrows to an unsamplable window, yielding a false
  "the test is blind here". Phase 4's Task 1 review re-derived it independently. Not restated at
  length; see that entry.
- **Still open, unchanged by this phase:** both external baselines (so §1's question is untouched),
  p99 / latency, the worker-count sweep, handler-cost variance, and the park/unpark counters that
  would make "how much of this is kernel wakes" directly measurable.

## 2026-08-14 — Mini actor system Phase 3: B vs D measured on throughput — no difference; skew costs 40%

The head-to-head finally ran. Branch `experiment/mini-actor-system`, commits `27858ca` (deque empty-poll
early-out) → `2abeb79` (key streams + cells) → `63fae03` (calibration) → `45af696` (the matrix). Apple M4
Pro, 12 logical cores, macOS; 8 workers, 1 producer; constant handler cost `SpinWait(32)`; 10M messages
per cell; 7 repeats per cell; **three independent passes** of a 2 schedulers × 2 skews × 2 cardinalities
matrix. Detail and the full configuration live in
`docs/superpowers/specs/2026-08-12-mini-actor-system-design.md` §6.3 and §10.

- **No measurable throughput difference between B (routed) and D (work stealing) at any coordinate.**
  Ratios 1.003x–1.176x. **The evidence is not the ratio sizes — it is that the winner alternates between
  passes at every one of the four coordinates** (uniform/64: B,D,D; uniform/8192: B,D,B; zipf/64: B,D,D;
  zipf/8192: D,B,B). A small real effect does not change sign three times. Ranges overlap in all three
  passes at both cardinality-64 coordinates and in two of three at cardinality 8192, and intra-cell
  spread (1.10x–1.50x over 7 repeats) is as large as every median gap. **So: don't pick B or D on
  throughput; there is nothing there to pick on.**
- **The §6.3 prediction ("D wins under skew") was aimed at the wrong metric.** Throughput at 8 saturated
  workers cannot see what dynamic scheduling buys — which message gets handled *when*. That shows up in
  latency, and Phase 2b already measured it exactly in positions (17 / 3000 / 0, §6.1). **A falsified
  prediction recorded beats a hedge:** the metric, not the scheduler, was the wrong choice.
- **Skew costs ~40% of throughput and neither scheduler recovers any of it** — uniform medians ~4.0–5.2M
  ops/s, Zipf(s=1) medians ~2.2–3.3M, at both cardinalities under both schedulers. Expected, and it is a
  confirmation of the design's own logic rather than a surprise: **D can move a key between workers but
  it cannot split a key**, so under skew the limit is the hot key's own serialisation, which no
  scheduling policy can address. At Zipf/64 key 0 takes `1/H(64)` ≈ 20% of traffic, capping the system at
  roughly 5x one worker's rate before any scheduler decides anything. **Implication: if skew throughput
  ever matters, the answer is a different affinity contract, not a different scheduler.**
- **THE TRANSFERABLE ONE — a single-producer system benchmarked with a cheap handler measures the
  producer, and that null result is indistinguishable from "the schedulers are equivalent".** With 8
  workers and a near-free handler every scheduler saturates at the same dispatch rate. So the handler
  cost is not a free parameter: calibrate it until the system is consumer-bound *before* believing any
  scheduler comparison. Here the ladder `spin ∈ {0..64}` was run first; the **knee moved between passes**
  (16 → 32) so it is *not* pinned, and **32 was chosen because run-to-run spread collapses to ~1.2x there
  against 3–6x below — reproducibility, not peak.** The rise from spin 0 is real, not an artefact: at low
  spin the producer races ahead, workers park/unpark and wake churn dominates, so *more* handler cost
  raises throughput. The low rungs are the noisy ones precisely because they are the producer-bound ones.
- **The mutation-methodology rule, sharpened (extends the Phase 2b entry below).** *A mutation must
  remove the defective behaviour's **arbitrating effect**, not just its reported result.* Overriding a
  CAS's returned bool while leaving the atomic RMW in place is still a genuine defect, but the real
  atomic keeps serialising the threads underneath, collapsing detection to two nested nanosecond windows
   — 20,000 barrier-synchronised rounds sampled it essentially never and came back **green with the race
  counter still non-zero**, i.e. it even looked exercised. Removing the CAS entirely: 17,721/20,000 and
  18,888/20,000. **So: when a mutation comes back green, first ask whether the mutant is still broken in
  an *observable* way before concluding the test is blind.**
- **macOS host caveats, permanent.** `ThreadAffinityUtil` is a documented no-op on macOS (no per-core
  pinning API), so this perf suite **cannot** isolate itself from placement noise even on an idle
  machine. And the first calibration attempt was unusable on a loaded host — three ladders that
  disagreed on *which rung is fastest*, load average 7.8–11.8 on 12 cores with ~32 dotnet-family
  background processes against a test needing 9 dedicated cores. It only became readable after the
  background .NET tooling was quieted. **Quiet the host or you are measuring the host** — and report any
  difference smaller than the observed spread as absent.
- **The Phase 2b D-side suspects (depth-1 contended CAS per activation, ~0.54 failed steal acquires per
  message) did not show up** — but they were not disproved either: at spin 32 the per-message work is
  ~196–228 ns/msg *aggregate over 8 workers*, so nanosecond-scale per-activation costs are unresolvable
  against it. Isolating them needs the cheap-handler regime this cut had to leave.
- **Still open, and easy to misread the above as having settled: neither baseline was built.** No
  hash-sharded `SpWorker`s, no `Channel<T>` + `ConcurrentDictionary`. "B ≈ D" says nothing about whether
  this substrate beats static sharding — §1's actual question is untouched. Also open: p99, worker
  sweep, handler-cost variance.

## 2026-08-13 — Mini actor system Phase 2b: scheduler D (work stealing) shipped; what it taught

D (`WorkStealingScheduler`) is in, behind the **unchanged** `IKeyScheduler` seam — B and D still differ
by exactly one type argument at the call site. Both now run the same behavioural fixture
(`SchedulerBehaviourTests`, generic over the scheduler), so nothing about the seam had to bend to admit
a pull-side scheduler. **This does not answer the experiment's question**: the B-vs-D benchmark has not
run. Phase 2b makes D exist and proves it correct, nothing more. Design detail lives in
`docs/superpowers/specs/2026-08-12-mini-actor-system-design.md`; this entry is the decisions and traps.

- **The global queue is MPMC, not the SPMC the spec described.** Deque overflow means a worker spills
  into the same queue, so there are `workers + 1` enqueuers. One CAS on a path that runs once per
  *activation*, not per message — cheaper than a sizing invariant that would be silent to violate.
- **`HasWork` must consult peer deques, and no correctness test would ever tell you.** Report on only
  your own deque plus the global queue and workers park while stealable work sits in a peer's deque —
  D silently degenerates into B without a router. Quiescence still resolves, so the symptom is
  throughput-only. Mutation-verified as caught deterministically (15/15 both TFMs) only because a test
  asserts the steal *directly*. Don't "simplify" that peer scan.
- **A worker's deque never holds more than one item.** `ScheduleLocal` is the only push source, runs on
  that worker's own thread inside `Activate`, and `RunLoop` goes straight back to `TryAcquire`, which
  pops its own deque first. So: `DequeOverflows == 0` is **structural, not headroom**; the spill path
  and its bounded retry are **unreachable at any `DequeCapacity`** (that knob is very nearly dead —
  don't tune it); the Chase-Lev deque is a depth-1 handoff slot; and every steal comes from the window
  between one push and its own next pop. Reframes what "work stealing" means in D — carry it into the
  benchmark's interpretation.
- **The starvation asymmetry is now measured, with a caveat that must travel with it.** B unbounded
  starves a ring-queued key for the *entire* hot stream (position 3000/3000); B bounded rescues it at
  exactly `LocalDrainBound + 1` (17); D at 4 workers rescues it at 0 — zero variance, 20/20 executions,
  and both B positions are closed forms predicted from `TryAcquire` before being confirmed. **But the
  comparison varies two factors**: B ran at 1 worker, D at 4, and **D at `workers: 1` has the identical
  hazard with no bound to fix it**. D's immunity comes from having idle peers, not from structure. The
  honest asymmetry: under B the ring is per-worker so a starved key is unreachable by idle peers even at
  N > 1; under D it sits in a shared structure. Severity also scales with producer/consumer imbalance —
  at near-parity the hazard was intermittent (~60% of runs), total only under sustained backpressure.
- **Two lost-wakeup races in D's park protocol are undefended, and a green mutation run is not
  coverage.** (1) `WakeOne`'s fence is **redundant on arm64** (RCsc release/acquire: an `ldar` can't
  hoist above an earlier `stlr`, and both publish paths end in a `Volatile.Write`) and **load-bearing on
  x86-64** (plain `mov`s, store buffer permits StoreLoad reordering). 0/70 on Apple Silicon is
  structurally required, not weak sampling — keep the fence and always name the architecture when
  quoting a result. (2) `ArmWait`'s reset-before-flag order: 3/80,000 mutated vs 2/80,000 baseline, i.e.
  the design excludes a rate above ~10⁻⁴/trial and says **nothing** below. It has no power because the
  arrival floor is the main thread's kernel wake-up (tens of µs) while the worker parks nanoseconds
  after signalling. The remedy that would work — a test-only probe hook between `Reset()` and the
  `Parked` write, turning the reversal into a deterministic hang — was deliberately not built.
- **`TryEnqueue`'s `false` is "not right now", never durable fullness — and throwing on it was a live
  bug.** Reproduced 7/40 on unmutated code, after a review had classified it as "not a live defect"; one
  retry measured insufficient (5/50). Now a bounded 64-attempt spin-retry, ~44 of which escalate to
  `Thread.Sleep(1)`: **~65 ms worst case**, which anyone budgeting dispatch tail latency needs to know.
  The escalation is right because a `false` with `CountApprox == 0` proves the blocking stamp is a full
  lap stale — OS-preemption scale, not a few instructions.
- **The most transferable trap: a test that cannot detect the thing it is named for.** Hit **five times**
  in this project. A deque race test whose thief always lost by 3–4 orders of magnitude, so forcing the
  arbitrating CAS to `true` still passed. A parked-worker test whose `Thread.Sleep(50)` guaranteed the
  schedule landed outside the arm window, making the exact defect it guarded invisible. A parked-system
  completion test whose broadcast block could be deleted entirely. Each was caught **only** by mutating
  the exact line the test names and requiring red — never by reading the test. **So: for any test whose
  name asserts a race, a fence or an arbitration, mutate that specific line and confirm the test fails
  before believing it.** A green suite is not evidence a guard works; it is evidence nothing broke
  loudly.
- **Process trap worth keeping.** Every fix round in this phase was a failure of *evidence*, never of
  the scheduler — the production code was approved in round 0 and survived all three rounds unchanged.
  The failures: a mutation result that measured a different bug, a "spin" that was a 30 ms
  `Thread.Sleep`, a threshold placed inside the distribution it was meant to separate, a bound stated an
  order of magnitude too tightly, and a position range quoted from an abandoned calibration. Attribute
  every number to the configuration that produced it, and say "argued from the code, not measured" when
  that is what it is.

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
