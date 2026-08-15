# Mini Actor System — Phase 4: the wait strategy as a measured variable

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the idle-worker wait ladder into its own seam, then walk it from today's crude version toward the Helios / CLR `UnfairSemaphore` design **one step at a time, measuring immediately after each step**, so every change is either justified by a number or reverted.

**Architecture:** A new `IWaitPolicy` seam owns the ladder — how long to spin, whether to yield, when to block, and for how long. `ActorSystem.WaitForWork` becomes the orchestrator that walks the ladder and does the rechecks; the policy decides the shape. Schedulers keep the blocking primitive (`ArmWait`/`Wait`), which is deliberately *not* abstracted in this phase — see "What this phase does not do".

**Tech Stack:** C# / .NET, the existing `perf-tests` harness.

## Why this phase exists

Phase 3 measured B against D and found **no measurable throughput difference** — but it also measured something the wait strategy explains. The calibration ladder rose from ~1.7–2.0M ops/s at `SpinWait(0)` to ~5M at the knee: a **2.5–3× effect sitting entirely in park/unpark churn**, larger than any B-vs-D gap observed.

That matters twice over:

1. It is the biggest known inefficiency in the system, and it is in code neither scheduler owns.
2. It is plausibly **asymmetric**. D generates more idle churn than B — up to ~0.54 failed steal acquires per message at 8 workers, each a scan across up to 7 peers. A crude wait ladder therefore taxes D more than B, which means Phase 3's null result **may be masking a real difference**. This phase does not assume that; it makes it testable.

## Reference design

Helios `DedicatedThreadPool`'s `UnfairSemaphore` (and the CLR ThreadPool it derives from) uses three tiers: fast-path acquire → spin with exponential backoff (budget ~256 normalised yields ≈ 9.5 µs, capped at 64 per iteration) → kernel park with a **timeout**. `Release` prefers **hot spinners over parked waiters**, because waking a spinner costs nothing while waking a parked thread costs a kernel round-trip.

Today's `ActorSystem.WaitForWork` is: flat `32 × SpinWait(64)` → arm → recheck → block on a per-worker `ManualResetEventSlim`, **unbounded**.

| Gap | Addressed by |
| --- | --- |
| No exponential backoff | Task 3 |
| No yield tier | Task 3 |
| Unbounded block | Task 2 |
| No spinner/waiter distinction | **Not this phase** — see below |

## What this phase does not do

**The spinner/waiter distinction is deferred to Phase 4b**, deliberately. It is the actual Helios idea and the most valuable of the four, but it cannot be done behind `IWaitPolicy`: it requires the *waker* to know which workers are spinning versus parked, which means changing `IKeyScheduler`'s wake side (`WakeOne`, the `Parked[]` flag, possibly a shared semaphore replacing per-worker events).

That is the single most dangerous edit available in this codebase — the Dekker handshake and both documented undefended lost-wakeup races (spec §11) live exactly there. It gets its own phase, its own review budget, and it should only be attempted **if Tasks 2–4 show the ladder work paid**. Record the decision either way in Task 5.

## Global Constraints

- Central package management: add/upgrade deps in `Directory.Packages.props` (`<PackageVersion>`). **Never** add `Version=` to a `.csproj`; never set a version property in a project.
- `TreatWarningsAsErrors=true`, `Nullable=enable`, `ImplicitUsings=enable`, `EnforceCodeStyleInBuild=true`. **A warning fails the build.**
- **Never** silence a warning with `<NoWarn>`, an `.editorconfig` suppression, or by disabling `GenerateDocumentationFile`.
- Style: tabs, `tab_width = 2`; file-scoped namespaces; `using` directives **inside** the namespace; K&R braces; no braces for single-line statements; `var`; no `this.`; `_camelCase` private fields; explicit (not primary) constructors.
- NUnit 4. `[CancelAfter]` does not abort a blocking test body.
- Do not touch `CLAUDE.md`, `.claude/`, `docs/agents/`. **Stage with explicit paths; never `git add -A`.**
- Run builds, tests and benchmarks in the **foreground**.
- **The whole repo must stay green after every task: `dotnet test -c Release` → 245/245 on net9.0 and net10.0.** This phase edits the park path; a regression here is a hang, not a failure.

## The measurement discipline for this phase

Every task from 2 onward ends with a number, and the rules from Phase 3 carry over unchanged:

- **The host must be quiet.** Close Rider / VS Code C# / OmniSharp, run `dotnet build-server shutdown`, check `uptime`. Three calibration ladders once disagreed on *which rung was fastest* on a loaded box.
- **`ThreadAffinityUtil` is a no-op on macOS.** This suite cannot isolate itself from placement noise. **Report any difference smaller than the observed spread as absent.**
- **Aggregate.** Median across the harness's repeats, with min–max range. A single repeat means nothing.
- **Measure where the effect lives.** The wait ladder bites hardest under *churn*, i.e. at **low** per-message cost — which is the opposite end from `SpinPerMessage = 32` where the Phase 3 matrix runs. Every policy comparison therefore runs at **two** points: `spin 0` (maximum churn, where the ladder should matter most) and `spin 32` (the matrix's regime, where it may not matter at all). Report both; they answer different questions.

---

## File Structure

| Path | Responsibility |
| --- | --- |
| `src/…/IWaitPolicy.cs` | The seam: the ladder's shape |
| `src/…/WaitPolicies/LegacyWaitPolicy.cs` | Today's behaviour, exactly — the baseline |
| `src/…/WaitPolicies/BoundedWaitPolicy.cs` | Task 2 |
| `src/…/WaitPolicies/BackoffWaitPolicy.cs` | Task 3 |
| `src/…/ActorSystem.cs` | **Modified** — `WaitForWork` walks the policy; optional ctor parameter |
| `src/…/IKeyScheduler.cs` | **Modified** — `Wait` gains a timeout (Task 2) |
| `src/…/Schedulers/*.cs` | **Modified** — three `Wait` implementations (Task 2) |
| `perf-tests/…/Throughput/Actors/WaitPolicyCells.cs` | The policy comparison cells |

Production root: `src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/`

---

### Task 1: Extract the seam, with today's behaviour as the baseline

**Files:**
- Create: `src/…/IWaitPolicy.cs`, `src/…/WaitPolicies/LegacyWaitPolicy.cs`
- Modify: `src/…/ActorSystem.cs`
- Test: `tests/…/Actors/WaitPolicyTests.cs`

**Interfaces:**
- Produces: `IWaitPolicy` with `WaitStep Next(int attempt)`; `readonly struct WaitStep`; `LegacyWaitPolicy`.
- `ActorSystem` gains an optional final constructor parameter `IWaitPolicy? waitPolicy = null`, defaulting to `LegacyWaitPolicy`.

**This task must change no behaviour at all.** It is a refactor whose only deliverable is that the seam exists and the suite is still green. Resist the temptation to "improve" the ladder here — Task 3 does that, with a measurement attached.

**Why an interface-typed constructor parameter and not a type parameter.** Every other seam in this codebase is a devirtualised struct type parameter, and that convention is correct for the per-message path. This one is different on both counts: the policy is consulted only on the **idle** path (at most a few dozen times per park, and a park happens at most once per activation), and making it a fifth type parameter on `ActorSystem<TKey, T, THandler, TScheduler>` would churn well over a hundred call sites across the tests and benchmarks for no measured gain. An interface call costing ~2 ns against a `SpinWait(64)` costing ~2 µs is noise. **State this trade in the XML doc**, and note it can be converted to a type parameter later if a measurement ever shows it mattering.

- [ ] **Step 1: Write the seam**

Create `src/…/IWaitPolicy.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Actors;

/// <summary>What a worker should do next while waiting for work.</summary>
public enum WaitStepKind {
	/// <summary>Burn <see cref="WaitStep.SpinIterations" /> spin iterations, then re-test for work.</summary>
	Spin,

	/// <summary>Yield the remainder of this time slice to another runnable thread, then re-test.</summary>
	Yield,

	/// <summary>Arm and block, for at most <see cref="WaitStep.BlockMilliseconds" />.</summary>
	Block,
}

/// <summary>One rung of the wait ladder.</summary>
public readonly struct WaitStep {
	public WaitStep(WaitStepKind kind, int spinIterations, int blockMilliseconds) {
		Kind = kind;
		SpinIterations = spinIterations;
		BlockMilliseconds = blockMilliseconds;
	}

	public WaitStepKind Kind { get; }

	public int SpinIterations { get; }

	/// <summary>Block timeout; <see cref="System.Threading.Timeout.Infinite" /> for an unbounded wait.</summary>
	public int BlockMilliseconds { get; }

	public static WaitStep Spin(int iterations) => new(WaitStepKind.Spin, iterations, 0);

	public static WaitStep Yield() => new(WaitStepKind.Yield, 0, 0);

	public static WaitStep Block(int milliseconds) => new(WaitStepKind.Block, 0, milliseconds);
}

/// <summary>
///   The shape of the idle-worker wait ladder: how long to spin, whether to yield, and when to block.
///   <para>
///     <b>Policy only — never mechanism.</b> The policy decides the ladder's shape; <c>ActorSystem</c>
///     walks it and performs every work and quiescence recheck, and the scheduler owns the blocking
///     primitive. A policy that could observe or influence <i>who</i> gets woken would be able to break
///     the arm-then-recheck protocol; keeping it to shape means it cannot.
///   </para>
///   <para>
///     <b>Why this is an interface parameter and not a devirtualised struct type parameter</b>, unlike
///     every other seam here: it is consulted on the IDLE path only — a few dozen times per park, and a
///     park happens at most once per activation — so an interface call of a couple of nanoseconds sits
///     against a <c>SpinWait(64)</c> of a couple of microseconds. Making it a fifth type parameter on
///     <c>ActorSystem</c> would churn a hundred-plus call sites for no measured gain. Convert it if a
///     measurement ever shows otherwise.
///   </para>
/// </summary>
public interface IWaitPolicy {
	/// <summary>
	///   The rung for this attempt. <paramref name="attempt" /> starts at 0 and increments for every
	///   rung walked within a single park; it resets when the worker finds work. A policy that returns
	///   <see cref="WaitStepKind.Block" /> forever is legal — that is what the unbounded legacy ladder
	///   does once its spin budget is gone.
	/// </summary>
	WaitStep Next(int attempt);
}
```

- [ ] **Step 2: Write the baseline policy**

Create `src/…/WaitPolicies/LegacyWaitPolicy.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Actors.WaitPolicies;

using System.Threading;

/// <summary>
///   The ladder as it was before Phase 4, reproduced EXACTLY so later policies are measured against a
///   real baseline rather than a remembered one: 32 rungs of <c>SpinWait(64)</c>, then an unbounded
///   block, forever.
///   <para>
///     Its shortcomings are the point of the phase and are documented rather than fixed here: no
///     exponential backoff, no yield tier before the kernel block, and an unbounded wait that turns a
///     lost wakeup into a permanent hang rather than a latency hiccup.
///   </para>
/// </summary>
public sealed class LegacyWaitPolicy : IWaitPolicy {
	internal const int SpinRungs = 32;
	internal const int SpinIterations = 64;

	public WaitStep Next(int attempt) =>
		attempt < SpinRungs ? WaitStep.Spin(SpinIterations) : WaitStep.Block(Timeout.Infinite);
}
```

- [ ] **Step 3: Rewrite `WaitForWork` to walk the ladder**

Replace `ActorSystem.WaitForWork`'s body. The rechecks and their ordering are load-bearing and must be preserved exactly — arm, fence, re-test work, re-test quiescence, block, re-test both again:

```csharp
	private bool WaitForWork(WorkerState<THandler> w) {
		for (var attempt = 0; ; attempt++) {
			var step = _waitPolicy.Next(attempt);

			if (step.Kind == WaitStepKind.Spin) {
				if (_scheduler.HasWork(w.Id) || _stopped) return !_stopped;
				Thread.SpinWait(step.SpinIterations);
				continue;
			}

			if (step.Kind == WaitStepKind.Yield) {
				if (_scheduler.HasWork(w.Id) || _stopped) return !_stopped;
				Thread.Yield();
				continue;
			}

			// Arm-then-recheck, unchanged: clear the flag, fence, re-test work AND quiescence, and only
			// then block. Collapsing this into check-then-park is the classic lost wakeup, and it passes
			// every test whose worker never actually sleeps.
			_scheduler.ArmWait(w.Id);
			Interlocked.MemoryBarrier();
			if (_scheduler.HasWork(w.Id) || _stopped) return !_stopped;
			if (IsQuiescent()) return false;
			_scheduler.Wait(w.Id);
			if (_scheduler.HasWork(w.Id) || _stopped) return !_stopped;
			if (IsQuiescent()) return false;
		}
	}
```

Note the loop no longer has the old two-phase structure; the `Block` rung repeats naturally because `LegacyWaitPolicy` returns `Block` for every attempt past 32. **`step.BlockMilliseconds` is deliberately unused in this task** — `IKeyScheduler.Wait` does not take a timeout yet, and adding it is Task 2. Do not add it early.

Add the field and constructor parameter:

```csharp
	private readonly IWaitPolicy _waitPolicy;
```

and as the final constructor parameter, `IWaitPolicy? waitPolicy = null`, assigned `_waitPolicy = waitPolicy ?? new LegacyWaitPolicy();`. Document that the default reproduces pre-Phase-4 behaviour so existing call sites are unaffected.

- [ ] **Step 4: Write the tests**

Create `tests/…/Actors/WaitPolicyTests.cs`:

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Actors;

using System.Collections.Generic;
using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.WaitPolicies;
using NUnit.Framework;

[TestFixture]
public class WaitPolicyTests {
	/// <summary>
	///   Pins the baseline ladder rung by rung. Later policies are measured against this one, so if it
	///   ever silently stops reproducing pre-Phase-4 behaviour, every number in the phase is measured
	///   against the wrong thing.
	/// </summary>
	[Test]
	public void Legacy_Ladder_Is_Thirty_Two_Spins_Then_Unbounded_Blocks() {
		var policy = new LegacyWaitPolicy();

		for (var attempt = 0; attempt < LegacyWaitPolicy.SpinRungs; attempt++) {
			var step = policy.Next(attempt);
			Assert.Multiple(() => {
				Assert.That(step.Kind, Is.EqualTo(WaitStepKind.Spin), $"attempt {attempt}");
				Assert.That(step.SpinIterations, Is.EqualTo(LegacyWaitPolicy.SpinIterations));
			});
		}

		foreach (var attempt in new[] { LegacyWaitPolicy.SpinRungs, 100, 10_000 }) {
			var step = policy.Next(attempt);
			Assert.Multiple(() => {
				Assert.That(step.Kind, Is.EqualTo(WaitStepKind.Block), $"attempt {attempt}");
				Assert.That(step.BlockMilliseconds, Is.EqualTo(System.Threading.Timeout.Infinite));
			});
		}
	}

	/// <summary>The seam must not have changed the default: an unspecified policy is the legacy ladder.</summary>
	[Test]
	public void An_Unspecified_Policy_Defaults_To_Legacy() {
		var state = new RecordingState();
		using var system = new ActorSystem<int, int, RecordingHandler, SingleWorkerScheduler>(
			new RecordingHandler(state), keyCapacity: 16, chunkCount: 8, chunkSize: 16,
			batchBound: 0, workers: 1, scheduler: new SingleWorkerScheduler { Capacity = 64 });
		system.Start();

		for (var i = 0; i < 100; i++)
			system.Dispatch(i % 4, i);

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 100);
		Assert.That(state.HandledCount, Is.EqualTo(100));
	}
}
```

Add `using Nanov.HighPerformance.Threading.Actors.Schedulers;` and `using Nanov.HighPerformance.Tests.Threading.Workers;` as the neighbouring fixtures do.

- [ ] **Step 5: Prove nothing changed**

Run in the foreground:
- `dotnet build -c Release` — 0 warnings.
- `dotnet test -c Release` — **247/247** both TFMs (245 + the 2 new tests).
- `dotnet test -c Release --filter "FullyQualifiedName~Threading.Actors"` — **three times**, confirming the park-path refactor introduced no flakiness. Report all three.

The tests that matter most here are the parking ones — `A_Fully_Parked_System_Still_Completes`, `Completion_Never_Resolves_Before_A_Racing_Final_Message_Is_Handled`, and `Eight_Workers_Preserve_Per_Key_Order_And_Lose_Nothing` under both schedulers. Name their results explicitly.

- [ ] **Step 6: Commit**

```bash
git add src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/ \
        tests/Nanov.HighPerformance.Tests/Threading/Actors/WaitPolicyTests.cs
git commit -m "refactor(actors): extract the wait ladder behind IWaitPolicy, legacy behaviour unchanged"
```

---

### Task 2: `BoundedWaitPolicy` — give the block a timeout, and measure the cost

**Files:**
- Create: `src/…/WaitPolicies/BoundedWaitPolicy.cs`
- Modify: `src/…/IKeyScheduler.cs`, `src/…/Schedulers/SingleWorkerScheduler.cs`, `RoutedScheduler.cs`, `WorkStealingScheduler.cs`, `src/…/ActorSystem.cs`
- Create: `perf-tests/…/Throughput/Actors/WaitPolicyCells.cs`

**Interfaces:**
- `IKeyScheduler.Wait(int workerId)` becomes `bool Wait(int workerId, int millisecondsTimeout)`, returning whether it was signalled (`false` on timeout). Three implementations to update.
- `BoundedWaitPolicy(int blockMilliseconds = 20)`.

**Why this comes before the interesting change.** A bounded block converts a lost wakeup from a **permanent hang** into a latency hiccup. The system has two documented, undefended lost-wakeup races (spec §11); this does not fix them, but it stops them being fatal. That safety argument stands on its own, independent of any throughput result — so it lands first, and its measurement question is only *"what does the safety cost?"*, not *"is it faster?"*.

The timeout also becomes the mechanism a later phase would need for adaptive parking, so the seam change is not wasted either way.

- [ ] **Step 1: Widen the scheduler seam**

`IKeyScheduler.Wait` gains the timeout and a return value. Document on the interface that `false` means the wait timed out rather than being signalled, that a timeout is **not** an error, and that the caller must re-test work and quiescence either way — which `WaitForWork` already does unconditionally.

Update all three schedulers. `ManualResetEventSlim.Wait(int)` already returns `bool`, so each is a one-line change; keep the `Parks` counter increment where it is.

- [ ] **Step 2: Write the policy**

```csharp
namespace Nanov.HighPerformance.Threading.Actors.WaitPolicies;

/// <summary>
///   The legacy ladder with a bounded block: 32 rungs of <c>SpinWait(64)</c>, then repeated blocks of
///   at most <see cref="BlockMilliseconds" />.
///   <para>
///     <b>This is a safety change before it is a performance change.</b> An unbounded block turns a lost
///     wakeup into a permanent hang; a bounded one turns it into a latency hiccup that the next recheck
///     clears. The system has two documented, undefended lost-wakeup races (spec §11) — this does not
///     fix them, it stops them being fatal.
///   </para>
///   <para>
///     The cost is a periodic wake per idle worker. At the default 20 ms and 8 workers that is 400
///     wakeups/sec across a fully idle system, which is why the phase measures it rather than assuming
///     it is free.
///   </para>
/// </summary>
public sealed class BoundedWaitPolicy : IWaitPolicy {
	public BoundedWaitPolicy(int blockMilliseconds = 20) => BlockMilliseconds = blockMilliseconds;

	public int BlockMilliseconds { get; }

	public WaitStep Next(int attempt) =>
		attempt < LegacyWaitPolicy.SpinRungs
			? WaitStep.Spin(LegacyWaitPolicy.SpinIterations)
			: WaitStep.Block(BlockMilliseconds);
}
```

Make `LegacyWaitPolicy`'s two constants `internal` if they are not already, so this policy can share them and the two ladders provably differ **only** in the block.

- [ ] **Step 3: Add the policy comparison cells**

Create `perf-tests/…/Throughput/Actors/WaitPolicyCells.cs`. These are **not** part of the Phase 3 B-vs-D matrix; they vary the policy while holding the scheduler fixed.

Fix scheduler = D (`WorkStealingScheduler`) — it has the most idle churn, so it is where a ladder change should show first. Cardinality 1024, uniform skew. Two per-message costs, because the ladder bites at low cost and may be invisible at high:

| Class name | Policy | `SpinPerMessage` |
| --- | --- | --- |
| `WaitLegacySpin0ThroughputTest` | `LegacyWaitPolicy` | 0 |
| `WaitLegacySpin32ThroughputTest` | `LegacyWaitPolicy` | 32 |
| `WaitBoundedSpin0ThroughputTest` | `BoundedWaitPolicy` | 0 |
| `WaitBoundedSpin32ThroughputTest` | `BoundedWaitPolicy` | 32 |

`ActorThroughputTestBase` currently hard-codes `SpinPerMessage` as a `const`. Make it a `protected virtual int SpinPerMessage => 32;` so a cell can override it, leaving the Phase 3 cells' behaviour and their calibration comment untouched. **Do not change the value 32 itself** — the Phase 3 matrix depends on it.

- [ ] **Step 4: Measure**

Quiet the host first (`dotnet build-server shutdown`, close IDE tooling, check `uptime`).

```bash
dotnet run -c Release --project perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests -- --target Wait
```

Run **three passes**. Report per-cell median and min–max across the harness's repeats, for all four cells, all three passes.

**The question is "what does the safety cost?"** Report the Legacy→Bounded delta at each spin point and whether it exceeds spread. A cost within spread means the safety is free at this configuration — a good result. A cost clearly exceeding spread is worth knowing before adopting it.

- [ ] **Step 5: Verify and commit**

`dotnet test -c Release` → 247/247 both TFMs, run twice. Then commit.

---

### Task 3: `BackoffWaitPolicy` — the Helios ladder, and the measurement that justifies it

**Files:**
- Create: `src/…/WaitPolicies/BackoffWaitPolicy.cs`
- Modify: `perf-tests/…/Throughput/Actors/WaitPolicyCells.cs`

**Interfaces:** `BackoffWaitPolicy(int spinBudget = 256, int maxSpinIterations = 64, int blockMilliseconds = 20)`.

**The ladder**, following the reference design: start at 1 spin iteration and double each rung, capped at `maxSpinIterations`, until the cumulative budget is spent; then insert a **yield** rung; then block with a timeout. The yield tier is the piece with the clearest argument on this host — 8 workers plus a producer plus a router on 12 logical cores is oversubscribed, and handing the slice to a runnable peer is far cheaper than a kernel wait round-trip.

- [ ] **Step 1: Write the policy**

```csharp
namespace Nanov.HighPerformance.Threading.Actors.WaitPolicies;

/// <summary>
///   Exponential-backoff spin, then a yield tier, then a bounded block — the shape Helios
///   <c>DedicatedThreadPool</c>'s <c>UnfairSemaphore</c> and the CLR thread pool both use.
///   <para>
///     <b>Why backoff.</b> A flat spin pays its full cost every time even when work was one iteration
///     away, and pays the same cost when it was never coming. Doubling makes the cheap case cheap and
///     reaches the block in a logarithmic number of rungs.
///   </para>
///   <para>
///     <b>Why a yield tier.</b> Eight workers plus a producer plus a router on twelve logical cores is
///     oversubscribed. Going straight from spinning to a kernel wait skips the cheapest useful action
///     available — handing the rest of the slice to a runnable peer, which on this workload is usually
///     the very producer or worker whose progress would end this wait.
///   </para>
///   <para>
///     <b>What this does NOT do.</b> The reference design's most valuable property is that a release
///     prefers a hot spinner over a parked waiter. That cannot be expressed here: this seam controls
///     only the ladder's shape, never who gets woken. See the plan's Phase 4b note.
///   </para>
/// </summary>
public sealed class BackoffWaitPolicy : IWaitPolicy {
	private readonly int[] _ladder;

	public BackoffWaitPolicy(int spinBudget = 256, int maxSpinIterations = 64, int blockMilliseconds = 20) {
		SpinBudget = spinBudget;
		MaxSpinIterations = maxSpinIterations;
		BlockMilliseconds = blockMilliseconds;

		// Precomputed so Next() is a bounds-checked array read on the idle path rather than arithmetic.
		var rungs = new System.Collections.Generic.List<int>();
		var spent = 0;
		var next = 1;
		while (spent < spinBudget) {
			rungs.Add(next);
			spent += next;
			next = System.Math.Min(next << 1, maxSpinIterations);
		}

		_ladder = rungs.ToArray();
	}

	public int SpinBudget { get; }

	public int MaxSpinIterations { get; }

	public int BlockMilliseconds { get; }

	/// <summary>Rungs of exponential spin, then exactly one yield, then blocks forever.</summary>
	public WaitStep Next(int attempt) {
		if (attempt < _ladder.Length) return WaitStep.Spin(_ladder[attempt]);
		if (attempt == _ladder.Length) return WaitStep.Yield();
		return WaitStep.Block(BlockMilliseconds);
	}
}
```

- [ ] **Step 2: Test the ladder's shape**

Add to `WaitPolicyTests.cs`: assert the rungs double and cap (`1, 2, 4, 8, 16, 32, 64, 64, …`), that the cumulative spin does not exceed the budget by more than one rung, that exactly one `Yield` appears, and that every later attempt is `Block`. This is a pure-function test — cheap, deterministic, and it pins the shape a later reader might otherwise "simplify".

- [ ] **Step 3: Add the cells and measure**

Add `WaitBackoffSpin0ThroughputTest` and `WaitBackoffSpin32ThroughputTest` on the same pattern. Re-run all six cells, three passes, quiet host, medians and ranges.

**Report the three-way comparison at each spin point**, and state for each whether the difference clears spread. The interesting cell is `spin 0`: that is where Phase 3 measured churn costing 2.5–3×, and where a better ladder should show up if it is going to.

- [ ] **Step 4: Verify and commit**

`dotnet test -c Release` → 247/247 both TFMs, twice.

---

### Task 4: Does the better ladder change the B-vs-D verdict?

**Files:** modify `perf-tests/…/Throughput/Actors/ActorThroughputTestBase.cs` only if the policy needs threading through; otherwise no production change.

**This is the task the whole phase is for.** Phase 3 found no measurable B-vs-D difference under the legacy ladder. If the ladder was taxing D's higher idle churn more than B's, the difference should appear — or provably still not.

- [ ] **Step 1: Re-run the Phase 3 matrix under the best policy**

Take whichever of `Bounded` / `Backoff` won Task 3 (or `Legacy`, if neither cleared spread — record that honestly and re-run under `Legacy` as a no-op control). Thread it into the eight Phase 3 cells and run the full matrix **three passes**, exactly as Phase 3 did.

- [ ] **Step 2: Compare verdicts, not just numbers**

For each of the four coordinates report: the Phase 3 verdict, the Phase 4 verdict, and whether the *winner alternates between passes* as it did before. **Alternation is the signal that matters** — a real effect keeps its sign.

State plainly which of these happened:
- **The verdict is unchanged** — B ≈ D under a better ladder too. That strengthens Phase 3's conclusion considerably, since the most plausible confounder has been removed.
- **A difference appeared** — the ladder was masking it. Report the direction, the magnitude, and at which coordinates.
- **The numbers all moved but the verdict did not** — the ladder helped both schedulers equally. Also a real result.

Do **not** tune anything to produce a difference. If none appears, that is the answer.

- [ ] **Step 3: Commit the results**

---

### Task 5: Record the result and decide on Phase 4b

**Files:** modify the spec (§6.3, §10, §13, and a new subsection for the wait ladder), `memory.md`, and this plan's "Where to pick up".

**Interfaces:** none — documentation only. `git diff --stat -- src/ tests/ perf-tests/` must be empty.

The governing rule from Phases 2b and 3 applies unchanged: **write only what was measured, and attribute every number to the configuration that produced it** — host, worker count, policy, spin value, cardinality, skew, pass.

- [ ] **Step 1: Record the ladder measurements** — the three-way comparison at both spin points, with spreads, and the Legacy→Bounded safety cost.

- [ ] **Step 2: Record the B-vs-D re-test** — and whether Phase 3's verdict survived.

- [ ] **Step 3: Make the Phase 4b call, explicitly.** The spinner/waiter distinction is the reference design's most valuable property and this phase deliberately did not attempt it. Decide from the data:
  - If the ladder work moved throughput materially, the wake side is likely worth the risk — say so, and record what it needs (changing `WakeOne` and the `Parked[]` flag's meaning, or replacing per-worker events with a shared unfair semaphore) and that it lands in the same code as the two undefended lost-wakeup races.
  - If the ladder work changed nothing measurable, say that too, and record that the wake side is **not** justified by anything measured — which is a decision, not a deferral.

- [ ] **Step 4: `memory.md`** — a new entry. The transferable lesson candidates: that the wait ladder was a 2.5–3× effect hiding in code neither scheduler owned; and whatever Task 4 concluded about whether it was confounding the scheduler comparison.

- [ ] **Step 5: Update the handoff** at the top of this plan and in the Phase 3 plan, so the next session starts from the current state.

---

## Out of scope

- **Phase 4b — the spinner/waiter distinction.** See "What this phase does not do".
- **Adaptive parking / worker retirement.** Fixed N, parked indefinitely, per the original design decision.
- **The Phase 3 leftovers**, unchanged and still open: p99 latency, the external baselines (hash-sharded `SpWorker`s, `Channel<T>` + `ConcurrentDictionary`), the worker-count sweep, handler-cost variance.
- **`KeySequence.HotKey`'s out-of-range key at cardinality 1** — still dead code, still to be fixed before the latency cut uses it.
