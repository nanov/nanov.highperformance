# Mini Actor System — Phase 3: the B-vs-D measurement (first cut)

## Where to pick up

**This plan is complete.** Read this section first; the rest of the file is the executed plan, kept for
provenance. Everything is on branch **`experiment/mini-actor-system`** (base `1075df1`).

**Done, and where it lives**

| Task | Commit | What it added |
| --- | --- | --- |
| 1 | `27858ca` | Empty-poll early-out in `WorkStealingDeque.TryPopBottom` (+ its test) — removes a D-side cost before measuring D |
| 2 | `2abeb79` | `Throughput/Actors/`: `KeySequence` (uniform/Zipf/hot-key), constant-cost handler, `ActorThroughputTestBase` |
| 3 | `63fae03` | `ProducerBoundCalibrationTest`; fixed `SpinPerMessage = 32` |
| 4 | `45af696` | `Cells.cs` — the 8 cells; the matrix run (3 passes × 7 repeats) |
| 5 | this commit | Spec §6.3 / §10 / §11 / §13 and `memory.md` updated with the result |

**The result, in two sentences.** No measurable throughput difference between scheduler B (routed) and
D (work stealing) at any of the four (skew × cardinality) coordinates — ratios 1.003x–1.176x, and the
winner alternates between passes at *every* coordinate, which a small real effect would not do. Skew
costs ~40% of throughput (uniform ~4.0–5.2M ops/s, Zipf ~2.2–3.3M) and **neither scheduler recovers
any of it**, because D can move a key between workers but cannot split one — under skew the limit is
the hot key's own serialisation. Full numbers and configuration: spec §6.3 and §10.

**Next options, in the order they are worth doing**

- **(a) The latency cut — the strongest candidate.** Throughput at 8 saturated workers is structurally
  blind to what dynamic scheduling buys: *which* message is handled *when*. That is where B and D
  already differ measurably — Phase 2b's starved-key positions were 17 (bounded B) / 3000 i.e. never
  (unbounded B) / 0 (D at 4 workers), but those varied worker count alongside the scheduler. Needed:
  p99 per-message latency and the §10 "dense-hot-key + placed victim" workload, run at N > 1 **under
  both schedulers**. The harness already has an `ILatencyTest` path and HdrHistogram is already
  referenced — no new dependency.
- **(b) The external baselines — the bigger question.** N hash-sharded `SpWorker`s, and
  `Channel<T>` + `ConcurrentDictionary`. Neither exists. Until one does, §1's actual question — does
  N-way dynamic balancing beat N static shards — is **unanswered**; "B ≈ D" says nothing about whether
  this substrate is worth its two-random-accesses-per-message cost at all.
- **(c) The sweeps.** Worker count (fixed at 8 here) and handler-cost variance (constant only here —
  and §6.3's original prediction named variable cost as one of the two regimes D was expected to win).
  Cheapest to add, weakest per unit of runtime.

**Debts not to trip over**

- `KeySequence.HotKey`'s non-hot branch is `rng.Next(1, Math.Max(2, cardinality))`, which at
  `cardinality == 1` returns key **1 — out of range**. Dead code today (no cell drives `HotKey`);
  **fix it before the latency cut uses it** at low cardinality.
- The perf-test project is still named `…Threading.Workers.PerfTests` while hosting actor cells.
  Cosmetic; a rename touches the harness's reflection discovery, so do it deliberately or not at all.
- Two **undefended lost-wakeup races** in D's park protocol (spec §11) and the **depth-1 deque** finding
  (§6.2) are recorded, not fixed. Depth-1 in particular reframes what "work stealing" means in D and
  makes `DequeCapacity` a near-dead knob.
- The two D-side cost suspects (per-activation contended CAS, ~0.54 failed steal acquires per message)
  are **unmeasured, not disproved** — at spin 32 they are unresolvable against the handler cost.

**How to re-run anything**

```bash
# whole matrix (~3 min/pass on an M4 Pro); --target takes a substring of the cell class name
dotnet run -c Release --project perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests -- \
  --target Actors --report false --print-spec false
# one cell, e.g.:            --target ActorsBZipfCard8192
# recalibrate the spin knee: --target ProducerBoundCalibration
```

**The host must be quiet or the numbers are noise.** These cells ask for 9 dedicated cores; with Rider/
ReSharper, OmniSharp or the Roslyn LSP running, three calibration ladders disagreed on *which rung is
fastest*. Close the IDE tooling, `dotnet build-server shutdown`, check `uptime`. And on macOS
`ThreadAffinityUtil` is a **no-op** (no per-core pinning API) — so even a quiet host removes contention
noise, never placement noise. Report any difference smaller than the observed spread as **absent**.

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Answer the question the experiment was started to ask — does dynamic scheduling (D, work stealing) beat static push-side routing (B) under key skew, and at what key cardinality — using a throughput harness, and report the answer honestly including when it is "no" or "no difference".

**Architecture:** Extend the existing Disruptor-style perf-test harness (`IThroughputTest`, reflection discovery, substring selection) with a family of actor-system throughput cells. One class per (scheduler × skew × cardinality), each producing a clean ops/sec figure the harness already knows how to run, repeat, and report.

**Tech Stack:** C# / .NET 10, the existing `perf-tests` harness, HdrHistogram (already referenced, not needed for this cut).

## Scope — deliberately narrow

This is the **first cut**: the two axes that should decide the question.

| In scope | Out of scope (recorded for a later cut) |
| --- | --- |
| Schedulers B and D | `SingleWorkerScheduler` |
| Key skew: uniform (control) + Zipf | Single-hot-key — see below |
| Key cardinality: 2 points (64, 8192) | Handler-cost variance (constant cost only) |
| Throughput (ops/sec) | Worker-count sweep (fixed at 8); p99 latency; `Channel<T>`/`ConcurrentDictionary` and hash-sharded `SpWorker` baselines |

**Eight cells, not eighteen.** Two schedulers × two skews × two cardinalities. The trims are principled, not arbitrary:

- **Single-hot-key is dropped from the throughput matrix** because it does not measure scheduling. One key is processed by one worker by construction, so both schedulers serialise identically and the cell reports ~1/8 of the uniform rate for reasons the design mandates. The interesting hot-key question is the *starved victim's* latency, and Phase 2b already answered that structurally and exactly: 17 messages under bounded B, 3000 under unbounded B, 0 under D.
- **The middle cardinality is dropped.** 64 and 8192 straddle the resident/non-resident boundary §10 predicts is the axis that moves the result most; a mid point tells you the shape of a curve, which is a later question than "is there a difference at all".

**Runtime budget: the full matrix should complete in about two minutes.** If a cell takes materially longer than a couple of seconds per repeat, stop and report it rather than waiting it out — that is a finding about the configuration, not a reason to be patient.

The external baselines and the latency story are §10's full matrix and are explicitly **not** in this plan. This cut answers "B or D, and when" — nothing more.

## Global Constraints

- Central package management: add/upgrade deps in `Directory.Packages.props` (`<PackageVersion>`). **Never** add `Version=` to a `.csproj`; never set a version property in a project.
- `TreatWarningsAsErrors=true`, `Nullable=enable`, `ImplicitUsings=enable`, `EnforceCodeStyleInBuild=true`. **A warning fails the build.**
- **Never** silence a warning with `<NoWarn>`, an `.editorconfig` suppression, or by disabling `GenerateDocumentationFile`. Fix the cause.
- Style (`.editorconfig`): tabs, `tab_width = 2`; file-scoped namespaces; `using` directives **inside** the namespace; K&R braces; no braces for single-line statements; `var`; no `this.`; `_camelCase` private fields; `csharp_style_prefer_primary_constructors = false`.
- Tests are NUnit 4. `[CancelAfter]` does not abort a blocking test body.
- Do not touch `CLAUDE.md`, `.claude/`, or `docs/agents/` — unrelated pre-existing local changes. **Stage with explicit paths; never `git add -A`.**
- Run builds and tests in the **foreground**.
- The whole repo must stay green: `dotnet test -c Release` → 244/244 on net9.0 and net10.0 (before this plan adds any test).

## The measurement hazard this plan exists to avoid

**The system is single-producer.** With eight workers and a cheap handler, the producer is the bottleneck, every scheduler saturates at the same dispatch rate, and B and D measure **identical** — a null result produced by the harness, not by the design.

So the handler cost is not a free parameter: it must be large enough that the system is **consumer-bound**. Task 3 measures where that point is and fixes the constant. Any cell run below it is measuring `Dispatch`, and its number must be discarded, not reported.

Two further known distortions, both recorded in the spec and both to be reported alongside the numbers rather than silently absorbed:

- D's steal scan costs up to ~0.54 failed acquires per message at 8 workers, each a `TrySteal` across up to 7 peers.
- Depth-1 means D pays a contended `CompareExchange` per activation where B pays a plain array decrement.

Task 1 removes a third distortion before any number is taken.

---

## File Structure

| Path | Responsibility |
| --- | --- |
| `src/…/Internal/WorkStealingDeque.cs` | **Modified** — early-out on the empty path (Task 1) |
| `perf-tests/…Workers.PerfTests/…csproj` | **Modified** — project reference to the Actors library |
| `perf-tests/…/Support/KeySequence.cs` | Precomputed key streams: uniform, Zipf, hot-key |
| `perf-tests/…/Support/ActorPerfHandler.cs` | Constant-cost handler + a completion counter |
| `perf-tests/…/Throughput/Actors/ActorThroughputTestBase.cs` | One matrix cell: builds the system, runs the stream, reports |
| `perf-tests/…/Throughput/Actors/Cells.cs` | The 18 concrete cells (2 schedulers × 3 skews × 3 cardinalities) |
| `perf-tests/…/Throughput/Actors/ProducerBoundCalibrationTest.cs` | Task 3's calibration probe |

Production source root: `src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/`
Perf-test root: `perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests/`

> **Note on the project name.** The harness project is named `…Workers.PerfTests` but will now also host actor-system cells. Renaming it would churn the solution, the artifacts path and the run scripts for no measurement benefit. Add the reference, keep the name, and record the mismatch as cosmetic debt in Task 5.

---

### Task 1: Remove the deque's write-on-empty poll

**Files:**
- Modify: `src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/Internal/WorkStealingDeque.cs`
- Test: `tests/Nanov.HighPerformance.Tests/Threading/Actors/WorkStealingDequeTests.cs` (add one test)

**Interfaces:**
- Consumes: nothing new.
- Produces: no API change. `TryPopBottom`'s observable behaviour is unchanged; only the empty path's cost changes.

**Why this is Task 1 and not a footnote.** `TryPopBottom` currently writes `_bottom` twice and issues an `Interlocked.MemoryBarrier()` on **every** call, including the empty poll — which, given a worker's deque never exceeds depth 1, is the overwhelming majority of calls. `TryAcquire` calls it unconditionally and `WaitForWork` returns with no backoff whenever `HasWork` is true, so a worker that loses a steal re-enters in a tight loop. That dirties the one line all seven peers read in `HasWork`'s scan. Benchmarking D with this in place measures an avoidable cost, not the design.

**The early-out is safe** for the same reason the existing full path is: `_top` only ever increases, and `_bottom` is owner-private. A stale-low `_top` read can only make the deque look *non*-empty, which falls through to the existing path and is handled there. It can never make a non-empty deque look empty.

- [ ] **Step 1: Write the failing test**

Add to `WorkStealingDequeTests.cs`:

```csharp
	/// <summary>
	///   The empty poll must not disturb the cursors. This is the path <c>TryAcquire</c> takes on every
	///   failed acquire — the overwhelming majority of calls, since a worker's deque never exceeds depth
	///   1 — so an early-out that got the cursor arithmetic wrong would corrupt the next push rather than
	///   fail visibly.
	/// </summary>
	[Test]
	public void An_Empty_Poll_Leaves_The_Cursors_Untouched_And_The_Deque_Usable() {
		var deque = new WorkStealingDeque(8);

		for (var i = 0; i < 1_000; i++)
			Assert.That(deque.TryPopBottom(out _), Is.False, $"poll {i} claimed an item from an empty deque");

		Assert.That(deque.CountApprox, Is.EqualTo(0), "an empty poll moved a cursor");

		// The real damage an off-by-one here would do is silent: the next push writes the wrong slot.
		for (var i = 0; i < 8; i++)
			Assert.That(deque.TryPushBottom(i), Is.True, $"push {i} failed after 1000 empty polls");

		var popped = new List<int>();
		while (deque.TryPopBottom(out var value))
			popped.Add(value);

		Assert.That(popped, Is.EqualTo(new List<int> { 7, 6, 5, 4, 3, 2, 1, 0 }));
	}
```

- [ ] **Step 2: Run it against the current code**

Run: `dotnet test -c Release --filter "FullyQualifiedName~An_Empty_Poll_Leaves_The_Cursors"`
Expected: **PASS** — the current implementation is already correct, this test pins the behaviour the optimisation must preserve. This is a characterisation test, not a red-green cycle; say so in the report.

- [ ] **Step 3: Add the early-out**

In `TryPopBottom`, before the existing `var bottom = _bottom.Value - 1;`:

```csharp
		// Early-out on the empty path. Without it every failed acquire writes _bottom twice and issues a
		// full fence — and with a depth that never exceeds 1 (see the type docs), the empty poll IS the
		// common case: TryAcquire calls this unconditionally and WaitForWork re-enters with no backoff
		// whenever HasWork is true, so a worker that loses a steal spins here. Those writes dirty the one
		// line every peer reads in HasWork's scan, which is O(N^2) coherence traffic on the idle path.
		//
		// Safe for the same reason the slow path is: _top only increases and _bottom is owner-private, so
		// a stale-low _top can only make the deque look NON-empty, which falls through to the code below
		// and is handled there. It cannot make a non-empty deque look empty.
		if (_bottom.Value - Volatile.Read(ref _top.Value) <= 0) {
			value = 0;
			return false;
		}
```

- [ ] **Step 4: Verify behaviour is unchanged**

Run, in the foreground:
- `dotnet build -c Release` — expect 0 warnings.
- `dotnet test -c Release --filter "FullyQualifiedName~WorkStealingDequeTests"` — **five times**, both TFMs.
- `dotnet test -c Release` — the whole repo, expect 245/245 on both TFMs (244 + the new test).

The single-element race test (`Owner_And_Thief_Never_Both_Take_The_Last_Item`) is the one that matters here: it asserts the CAS arbitration still holds with the early-out in front of it. Report its result explicitly.

- [ ] **Step 5: Re-run the mutation check that guards the race**

Temporarily replace the CAS result in `TryPopBottom` with `var won = true;` and confirm `Owner_And_Thief_Never_Both_Take_The_Last_Item` still **fails** (it reported ~19,400/20,000 double-takes before this change). This proves the early-out did not accidentally shield the race from the test. Revert and confirm green.

- [ ] **Step 6: Commit**

```bash
git add src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/Internal/WorkStealingDeque.cs \
        tests/Nanov.HighPerformance.Tests/Threading/Actors/WorkStealingDequeTests.cs
git commit -m "perf(actors): early-out on the deque's empty poll before benchmarking D"
```

---

### Task 2: Harness scaffolding — key streams, handler, and the cell base

**Files:**
- Modify: `perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests/Nanov.HighPerformance.Threading.Workers.PerfTests.csproj`
- Create: `perf-tests/…/Support/KeySequence.cs`
- Create: `perf-tests/…/Support/ActorPerfHandler.cs`
- Create: `perf-tests/…/Throughput/Actors/ActorThroughputTestBase.cs`

**Interfaces:**
- Consumes: `IThroughputTest`, `ThroughputSessionContext`, `ProgramOptions`, `ThreadAffinityUtil` (all existing).
- Produces: `KeySequence.Uniform/Zipf/HotKey(int cardinality, int length, int seed)` → `int[]`; `ActorPerfHandler` (a `readonly struct` implementing `IActorHandler<int, long>`); `abstract class ActorThroughputTestBase : IThroughputTest` with abstract `Cardinality`, `Skew`, and an abstract `Run` hook per scheduler.

**Two things that decide whether the numbers mean anything:**

1. **The key stream is precomputed.** Generating a Zipf sample inside the dispatch loop would put the sampler's cost — which is larger than `Dispatch`'s — inside the measurement, and it would differ between cells. Build an `int[]` before `sessionContext.Start()` and index it modulo its length.
2. **The stream is identical across schedulers.** Same seed, same length, same cardinality. B and D must see the byte-identical key order or the comparison is meaningless.

- [ ] **Step 1: Add the project reference**

In the perf-test `.csproj`, alongside the existing `ProjectReference`:

```xml
    <ProjectReference Include="../../src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/Nanov.HighPerformance.Threading.Actors.csproj" />
```

Run `dotnet build -c Release` and confirm 0 warnings. The perf-test project targets `net10.0` only and the Actors library multi-targets `net9.0;net10.0`; the reference resolves to `net10.0`.

- [ ] **Step 2: Write the key-stream generator**

Create `perf-tests/…/Support/KeySequence.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

using System;

/// <summary>
///   Precomputed key streams for the actor-system throughput cells.
///   <para>
///     <b>Precomputed on purpose.</b> A Zipf sampler costs more per draw than <c>Dispatch</c> does, so
///     generating inside the measured loop would put the sampler in the measurement — and a different
///     amount of it in each cell. Every cell builds its stream before the clock starts and then indexes
///     it modulo its length.
///   </para>
///   <para>
///     <b>Same seed, same stream.</b> B and D must see byte-identical key orders or the comparison means
///     nothing. The seed is fixed per (skew, cardinality), never per scheduler.
///   </para>
/// </summary>
public static class KeySequence {
	/// <summary>Stream length. Cycled by the cells; long enough that the cycle is not itself a pattern.</summary>
	public const int Length = 1 << 20;

	/// <summary>Every key equally likely, in shuffled order — the no-skew control.</summary>
	public static int[] Uniform(int cardinality, int seed) {
		var rng = new Random(seed);
		var keys = new int[Length];
		for (var i = 0; i < Length; i++)
			keys[i] = rng.Next(cardinality);
		return keys;
	}

	/// <summary>
	///   Zipf(s = 1) over <paramref name="cardinality" /> keys — the classic skew: key 0 takes roughly
	///   <c>1 / H(N)</c> of the traffic. Built by inverse-CDF over a precomputed cumulative table, which
	///   is O(N) memory and O(log N) per draw, both off the measured path.
	/// </summary>
	public static int[] Zipf(int cardinality, int seed) {
		var cumulative = new double[cardinality];
		var running = 0.0;
		for (var i = 0; i < cardinality; i++) {
			running += 1.0 / (i + 1);
			cumulative[i] = running;
		}

		for (var i = 0; i < cardinality; i++)
			cumulative[i] /= running;

		var rng = new Random(seed);
		var keys = new int[Length];
		for (var i = 0; i < Length; i++) {
			var u = rng.NextDouble();
			var idx = Array.BinarySearch(cumulative, u);
			if (idx < 0) idx = ~idx;
			if (idx >= cardinality) idx = cardinality - 1;
			keys[i] = idx;
		}

		return keys;
	}

	/// <summary>
	///   90% of traffic to one key, the rest spread uniformly — the pathological case B's per-worker ring
	///   cannot rebalance and D's shared structures can.
	/// </summary>
	public static int[] HotKey(int cardinality, int seed) {
		var rng = new Random(seed);
		var keys = new int[Length];
		for (var i = 0; i < Length; i++)
			keys[i] = rng.NextDouble() < 0.9 ? 0 : rng.Next(1, Math.Max(2, cardinality));
		return keys;
	}
}
```

- [ ] **Step 3: Write the constant-cost handler**

Create `perf-tests/…/Support/ActorPerfHandler.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

using System;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors;

/// <summary>Shared, mutable state observed through a reference — the struct handler is copied per worker.</summary>
public sealed class ActorPerfState {
	public long Handled;

	/// <summary>Spin iterations burned per message. See the calibration test for why this is not zero.</summary>
	public int SpinPerMessage;

	public long HandledCount => Volatile.Read(ref Handled);
}

/// <summary>
///   Constant-cost handler for the throughput cells.
///   <para>
///     <b>The cost is the point, not overhead to be minimised.</b> This system is single-producer: with a
///     cheap handler, eight workers starve on one dispatch thread, every scheduler saturates at the same
///     rate, and B and D measure identical — a null result manufactured by the harness. The spin makes
///     the system consumer-bound so the scheduler is what is being compared.
///   </para>
/// </summary>
public readonly struct ActorPerfHandler : IActorHandler<int, long> {
	private readonly ActorPerfState _state;

	public ActorPerfHandler(ActorPerfState state) => _state = state;

	public void Handle(int key, long message) {
		Thread.SpinWait(_state.SpinPerMessage);
		Interlocked.Increment(ref _state.Handled);
	}

	public void OnError(Exception error) => Interlocked.Increment(ref _state.Handled);

	public void OnCompleted(Exception? error) { }

	public void OnSlotReused(ref long slot) { }

	public void OnChunkReclaimed(Span<long> chunk) { }
}
```

> **Note the `Interlocked.Increment` in `Handle`.** It is a shared-line atomic on the per-message path and it *does* cost throughput — but it costs both schedulers identically, and without it there is no completion oracle. Record it as a constant of the measurement in Task 5; do not silently treat the resulting ops/sec as the system's ceiling.

- [ ] **Step 4: Write the cell base**

Create `perf-tests/…/Throughput/Actors/ActorThroughputTestBase.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Throughput.Actors;

using System;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>Which key distribution a cell drives.</summary>
public enum Skew {
	Uniform,
	Zipf,
	HotKey,
}

/// <summary>
///   One cell of the B-vs-D matrix: a scheduler, a skew and a key cardinality, driven by a precomputed
///   key stream at a fixed worker count and a fixed per-message cost.
///   <para>
///     <b>Everything except the scheduler is held constant across a comparison pair.</b> Same stream,
///     same seed, same cardinality, same worker count, same spin. If two cells differ in anything else,
///     the difference between their numbers is not attributable to the scheduler.
///   </para>
/// </summary>
public abstract class ActorThroughputTestBase : IThroughputTest {
	/// <summary>Fixed for this cut. A worker sweep is a later cut, not this one.</summary>
	protected const int Workers = 8;

	// 10M at the calibrated per-message cost is ~0.5s of work spread over 8 workers — enough to swamp
	// startup and JIT, short enough that the whole matrix runs in about two minutes.
	protected const long Iterations = 10L * 1000L * 1000L;

	/// <summary>Set by the calibration test in Task 3. Do NOT change without re-running it.</summary>
	protected const int SpinPerMessage = 0; // ← Task 3 replaces this with the measured value

	private readonly int? _producerCpu;

	protected ActorThroughputTestBase(ProgramOptions options) => _producerCpu = options.GetCustomCpu(0);

	protected abstract int Cardinality { get; }

	protected abstract Skew Skew { get; }

	/// <summary>Runs the stream through a system built on this cell's scheduler; returns messages handled.</summary>
	protected abstract long RunStream(int[] keys, ActorPerfState state, int? producerCpu);

	public int RequiredProcessorCount => Workers + 1;

	public long Run(ThroughputSessionContext sessionContext) {
		// Seeded by (skew, cardinality) only — never by scheduler, so a B cell and a D cell at the same
		// coordinates drive byte-identical key orders.
		var seed = (int)Skew * 7919 + Cardinality;
		var keys = Skew switch {
			Skew.Uniform => KeySequence.Uniform(Cardinality, seed),
			Skew.Zipf => KeySequence.Zipf(Cardinality, seed),
			Skew.HotKey => KeySequence.HotKey(Cardinality, seed),
			_ => throw new ArgumentOutOfRangeException(nameof(Skew)),
		};

		var state = new ActorPerfState { SpinPerMessage = SpinPerMessage };

		sessionContext.Start();
		var handled = RunStream(keys, state, _producerCpu);
		sessionContext.Stop();

		PerfTestUtil.FailIfNot(Iterations, handled,
			$"handler should have seen {Iterations} messages, saw {handled}");
		return Iterations;
	}

	/// <summary>
	///   Shared drive loop. Sizing: <c>keyCapacity</c> at 2x cardinality keeps the map's load factor low
	///   so eviction does not run; <c>chunkCount</c> at cardinality means every live key can hold a chunk
	///   so harvest does not run. Both cold paths are producer-side and scheduler-independent, so leaving
	///   them out of the measurement removes noise that would land on both cells equally but unevenly.
	/// </summary>
	protected static long Drive<TScheduler>(TScheduler scheduler, int cardinality, int[] keys,
		ActorPerfState state, int? producerCpu)
		where TScheduler : struct, IKeyScheduler {
		using var system = new ActorSystem<int, long, ActorPerfHandler, TScheduler>(
			new ActorPerfHandler(state), keyCapacity: cardinality * 2, chunkCount: cardinality,
			chunkSize: 256, batchBound: 16, workers: Workers, scheduler: scheduler);
		system.Start();

		using (ThreadAffinityUtil.SetThreadAffinity(producerCpu)) {
			var mask = keys.Length - 1;
			for (var i = 0L; i < Iterations; i++)
				system.Dispatch(keys[(int)(i & mask)], i);

			system.TryComplete();
			system.Completion.GetAwaiter().GetResult();
		}

		return state.HandledCount;
	}
}
```

- [ ] **Step 5: Build**

Run: `dotnet build -c Release`
Expected: 0 warnings. Nothing runs yet — there are no concrete cells.

- [ ] **Step 6: Commit**

```bash
git add perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests/
git commit -m "perf(actors): key streams, constant-cost handler and the B-vs-D cell base"
```

---

### Task 3: Calibrate the consumer-bound point

**Files:**
- Create: `perf-tests/…/Throughput/Actors/ProducerBoundCalibrationTest.cs`
- Modify: `perf-tests/…/Throughput/Actors/ActorThroughputTestBase.cs` (set `SpinPerMessage`)

**Interfaces:**
- Consumes: `ActorPerfState`, `ActorPerfHandler`, `KeySequence`, `Drive`.
- Produces: the measured `SpinPerMessage` constant that every cell in Task 4 uses.

**This task is the difference between a measurement and a null result.** If the producer is the bottleneck, B and D saturate at the same dispatch rate and the matrix returns "no difference" for reasons that have nothing to do with scheduling.

The probe: run a *uniform, high-cardinality* cell — the configuration where both schedulers should behave near-identically — at a ladder of spin values, on **D only** (the scheduler is irrelevant to the question being asked). Throughput will rise with spin while the producer is the bottleneck, plateau, and then fall once consumers dominate. **The lowest spin value on the falling side is the calibration point**: the smallest per-message cost at which the consumers, not the producer, set the rate.

- [ ] **Step 1: Write the calibration probe**

Create `perf-tests/…/Throughput/Actors/ProducerBoundCalibrationTest.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Throughput.Actors;

using System;
using System.Diagnostics;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>
///   Not a comparison — a calibration. Finds the per-message cost at which this machine stops being
///   producer-bound.
///   <para>
///     The actor system is single-producer. With a cheap handler, eight workers starve on one dispatch
///     thread, every scheduler saturates at the same rate, and a B-vs-D matrix run below this point
///     returns "no difference" for a reason that has nothing to do with scheduling. Throughput here
///     RISES with spin while the producer is the bottleneck and FALLS once the consumers are — the knee
///     is the answer, and the first value on the falling side is what the cells should use.
///   </para>
///   <para>
///     Runs on D only: the question is about the producer, and the scheduler is not part of it.
///   </para>
/// </summary>
public sealed class ProducerBoundCalibrationTest : IThroughputTest {
	private const int Cardinality = 1024;

	// 2M per rung, and the ladder stops at 64. The top rung already costs ~2 us/message, which is far
	// past any plausible knee — going higher only buys a longer wait for a number nobody will use.
	private const long Iterations = 2L * 1000L * 1000L;
	private static readonly int[] Ladder = [0, 1, 2, 4, 8, 16, 32, 64];

	private readonly int? _producerCpu;

	public ProducerBoundCalibrationTest(ProgramOptions options) => _producerCpu = options.GetCustomCpu(0);

	public int RequiredProcessorCount => 9;

	public long Run(ThroughputSessionContext sessionContext) {
		var keys = KeySequence.Uniform(Cardinality, seed: 12345);

		Console.WriteLine();
		Console.WriteLine("  spin | msgs/sec    | ns/msg");
		Console.WriteLine("  -----+-------------+-------");

		sessionContext.Start();
		foreach (var spin in Ladder) {
			var state = new ActorPerfState { SpinPerMessage = spin };
			var sw = Stopwatch.StartNew();
			var handled = Drive(keys, state, spin);
			sw.Stop();

			if (handled != Iterations)
				throw new InvalidOperationException($"spin {spin}: handled {handled} of {Iterations}");

			var perSecond = Iterations / sw.Elapsed.TotalSeconds;
			Console.WriteLine($"  {spin,4} | {perSecond,11:N0} | {1e9 / perSecond,6:F1}");
		}

		sessionContext.Stop();

		Console.WriteLine();
		Console.WriteLine("  Read the knee: throughput rises while the PRODUCER is the bottleneck and falls");
		Console.WriteLine("  once the CONSUMERS are. Take the first spin value on the falling side.");
		return Iterations * Ladder.Length;
	}

	private long Drive(int[] keys, ActorPerfState state, int spin) {
		using var system = new Nanov.HighPerformance.Threading.Actors.ActorSystem<int, long, ActorPerfHandler,
			WorkStealingScheduler>(
			new ActorPerfHandler(state), keyCapacity: Cardinality * 2, chunkCount: Cardinality,
			chunkSize: 256, batchBound: 16, workers: 8,
			scheduler: new WorkStealingScheduler { Capacity = 4096, DequeCapacity = 256 });
		system.Start();

		using (ThreadAffinityUtil.SetThreadAffinity(_producerCpu)) {
			var mask = keys.Length - 1;
			for (var i = 0L; i < Iterations; i++)
				system.Dispatch(keys[(int)(i & mask)], i);

			system.TryComplete();
			system.Completion.GetAwaiter().GetResult();
		}

		return state.HandledCount;
	}
}
```

- [ ] **Step 2: Run the calibration**

```bash
dotnet run -c Release --project perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests -- --target ProducerBoundCalibration
```

Record the full table in the task report. Identify the knee.

- [ ] **Step 3: Set the constant**

Replace `ActorThroughputTestBase`'s `SpinPerMessage = 0` with the measured value, and change its comment to record the measurement: the ladder value chosen, the ns/msg it produced, and the date. **If the ladder shows no knee inside its range — throughput still rising at spin 256 — stop and report that.** It means this machine cannot be made consumer-bound at a sane per-message cost with one producer, which is a finding about the design, not a calibration failure, and it changes what Task 4 can claim.

- [ ] **Step 4: Commit**

```bash
git add perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests/
git commit -m "perf(actors): calibrate the consumer-bound per-message cost"
```

---

### Task 4: The B-vs-D matrix

**Files:**
- Create: `perf-tests/…/Throughput/Actors/Cells.cs`

**Interfaces:**
- Consumes: `ActorThroughputTestBase`, `RoutedScheduler`, `WorkStealingScheduler`.
- Produces: 18 discoverable throughput cells.

**The matrix:** 2 schedulers × 2 skews × 2 cardinalities = **8 cells**. Cardinalities **64** and **8192**, straddling the resident/non-resident boundary §10 predicts is the axis that moves the result most.

**Naming matters** because the harness selects by name substring: `--target ActorsD` runs all four D cells, `--target Zipf` runs all four Zipf cells, `--target Card64` runs the four small-cardinality cells.

- [ ] **Step 1: Write the two scheduler bases**

In `Cells.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Workers.PerfTests.Throughput.Actors;

using Nanov.HighPerformance.Threading.Actors.Schedulers;
using Nanov.HighPerformance.Threading.Workers.PerfTests.Support;

/// <summary>Scheduler B — push-side routing through a dedicated router thread.</summary>
public abstract class ActorsBTestBase : ActorThroughputTestBase {
	protected ActorsBTestBase(ProgramOptions options) : base(options) { }

	protected override long RunStream(int[] keys, ActorPerfState state, int? producerCpu) =>
		Drive(new RoutedScheduler { Capacity = Cardinality * 4 }, Cardinality, keys, state, producerCpu);
}

/// <summary>Scheduler D — pull-side work stealing.</summary>
public abstract class ActorsDTestBase : ActorThroughputTestBase {
	protected ActorsDTestBase(ProgramOptions options) : base(options) { }

	protected override long RunStream(int[] keys, ActorPerfState state, int? producerCpu) =>
		Drive(new WorkStealingScheduler { Capacity = Cardinality * 4, DequeCapacity = 256 },
			Cardinality, keys, state, producerCpu);
}
```

Both scheduler capacities are `Cardinality * 4`, i.e. twice the map's slot count (`keyCapacity` is `Cardinality * 2`), so neither queue can fill and neither cell pays a resize or a retry the other does not.

- [ ] **Step 2: Write the eight cells**

Each is six lines. The full set, all in `Cells.cs`, following this exact template:

```csharp
public sealed class ActorsBUniformCard64ThroughputTest : ActorsBTestBase {
	public ActorsBUniformCard64ThroughputTest(ProgramOptions options) : base(options) { }
	protected override int Cardinality => 64;
	protected override Skew Skew => Skew.Uniform;
}
```

The eight class names and their two constants:

| Class name | Cardinality | Skew |
| --- | --- | --- |
| `ActorsBUniformCard64ThroughputTest` | 64 | `Uniform` |
| `ActorsBUniformCard8192ThroughputTest` | 8192 | `Uniform` |
| `ActorsBZipfCard64ThroughputTest` | 64 | `Zipf` |
| `ActorsBZipfCard8192ThroughputTest` | 8192 | `Zipf` |
| `ActorsDUniformCard64ThroughputTest` | 64 | `Uniform` |
| `ActorsDUniformCard8192ThroughputTest` | 8192 | `Uniform` |
| `ActorsDZipfCard64ThroughputTest` | 64 | `Zipf` |
| `ActorsDZipfCard8192ThroughputTest` | 8192 | `Zipf` |

The `ActorsB…` classes derive from `ActorsBTestBase`, the `ActorsD…` from `ActorsDTestBase`.

`Skew.HotKey` and `KeySequence.HotKey` stay in the codebase unused by this matrix — the latency cut will want them, and deleting them only to re-add them later is churn. Leave a one-line comment on `Skew.HotKey` saying so, or the next reader will assume a cell was forgotten.

- [ ] **Step 3: Build and smoke-run one pair**

```bash
dotnet build -c Release
dotnet run -c Release --project perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests -- --target ActorsBUniformCard1024
dotnet run -c Release --project perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests -- --target ActorsDUniformCard1024
```

Both must complete and report ops/sec. If either wedges, stop and report — a hang here is a real defect, not a tuning problem.

- [ ] **Step 4: Run the full matrix**

```bash
dotnet run -c Release --project perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests -- --target Actors
```

Record **every** cell's ops/sec in the task report as a 2×2×2 table. Run the whole matrix **three times** and report all three, not an average — run-to-run spread is itself a result, and this project has repeatedly found single samples hiding tails.

**Expected wall clock: roughly two minutes for all three passes.** If it is running materially longer, stop and report where the time is going rather than waiting it out.

- [ ] **Step 5: Report the numbers, and only the numbers**

In the task report, state for each skew: which scheduler won at each cardinality, by what margin, and whether the margin exceeds the run-to-run spread. **Do not conclude anything the data does not support.** In particular:
- If B and D are within the spread everywhere, the honest result is "no measurable difference at this configuration", and that is a legitimate and useful answer.
- If D wins only at high cardinality, say that, and do not generalise it to "D wins under skew".
- Zipf(s=1) at cardinality 64 gives key 0 roughly `1/H(64)` ≈ 20% of the traffic. That is skew, but it is not a hot key — do not describe it as one.

- [ ] **Step 6: Commit**

```bash
git add perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests/
git commit -m "perf(actors): the B-vs-D throughput matrix over skew and cardinality"
```

---

### Task 5: Record the result

**Files:**
- Modify: `docs/superpowers/specs/2026-08-12-mini-actor-system-design.md` (§6.3, §10, §13)
- Modify: `memory.md`

**Interfaces:** none — documentation only. No production, perf-test or test changes in this task.

**The governing rule, inherited from Phase 2b and non-negotiable:** write only what was measured, and attribute every number to the configuration that produced it — machine, worker count, spin value, cardinality, skew, iteration count, and which of the three runs it came from. Phase 2b shipped four confident claims that measured something other than what they said; every one was caught by reading. Do not add a fifth.

- [ ] **Step 1: Update §6.3 — "Expected outcome"**

§6.3 predicts *"B should win on low skew (fewer atomics, one predictable hop). D should win on skew and variable cost"* and calls steal rate the number that says which regime you are in. Replace the prediction with the measurement, and state plainly whether the prediction held. If it did not, say so — a falsified prediction recorded is worth more than a hedge.

- [ ] **Step 2: Update §10 — the benchmark plan**

Record what this cut covered (2 schedulers × 2 skews × 2 cardinalities, 8 workers, constant cost, throughput only) and what it did **not**: no single-hot-key throughput cell (and why it would not have measured scheduling), no p99, no worker sweep, no handler-cost variance, and neither external baseline (hash-sharded `SpWorker`s, `Channel<T>` + `ConcurrentDictionary`). §10's "the question is whether N-way dynamic balancing beats N static shards under skew" is therefore **still unanswered** — this cut compares B against D, not either against static sharding. Say that explicitly; it is the easiest thing for a later reader to get wrong.

Record the calibration too: the spin ladder, the knee, and the resulting ns/msg — because every number in the matrix is conditional on the system being consumer-bound, and a later reader must be able to check that it was.

- [ ] **Step 3: Update §13**

Step 6 is now partially done. Record precisely which part: the B-vs-D throughput comparison. The baselines and the latency story remain open.

- [ ] **Step 4: Add a `memory.md` entry**

Follow the existing format. Cover: the result; the producer-bound hazard and how it was calibrated away (this is the transferable lesson — a single-producer system benchmarked with a cheap handler measures the producer, and the null result looks exactly like "the schedulers are equivalent"); and whether the depth-1 and steal-scan costs recorded in Phase 2b showed up in the numbers.

- [ ] **Step 5: Verify nothing but docs changed**

Run: `git diff --stat -- src/ tests/ perf-tests/`
Expected: empty output.

- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/specs/2026-08-12-mini-actor-system-design.md memory.md
git commit -m "docs(actors): record the measured B-vs-D throughput comparison"
```

---

## Out of scope

Deliberately **not** in this plan:

- **p99 latency** — §10 wants it; the harness has an `ILatencyTest` path and HdrHistogram already referenced. A later cut.
- **The external baselines** — N hash-sharded `SpWorker`s and `Channel<T>` + `ConcurrentDictionary`. These answer "is this substrate worth it at all", which is a bigger and different question than "B or D".
- **Worker-count and handler-cost-variance sweeps.**
- **A BenchmarkDotNet entry** — §10 says the scrappy harness comes first and BDN once the shape settles. The shape is not settled until this cut reports.
- **Renaming the perf-test project** to drop `Workers` from its name now that it hosts actor cells. Cosmetic; record it, do not do it here.
