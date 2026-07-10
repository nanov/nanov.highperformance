# SpRingCore Lock-Step Throughput Squeeze Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Squeeze the existing lock-step SPSC path in `SpRingCore<T>` for throughput via three gated layers — bounds-check elimination, parked-flag signaling (removes both per-item cross-core index reads), and batched head publish — per the approved spec `docs/superpowers/specs/2026-07-10-spring-core-throughput-squeeze-design.md`.

**Architecture:** All changes live in `SpRingCore<T>`, the four FIFO worker drain loops, and `HandleScope<T>`. No public API changes. Each layer is its own commit, gated by the full test suite (Debug + Release, net9.0 + net10.0), new stress tests, and interleaved A/B perf runs.

**Tech Stack:** C# / .NET (net9.0 + net10.0 multi-target), NUnit, the in-repo perf-tests harness (HdrHistogram), BenchmarkDotNet.

## Global Constraints

- **No public API surface changes.** New members are `internal` only (`HandleScope<T>.ReleaseDeferred` is internal on a public ref struct — that's allowed and intended).
- **Untouchable contracts:** FIFO order; soft-complete drains, hard stop drops; the SPSC signaling invariant; the async empty-wait stays allocation-free; `HandleScope.Release` frees the slot for the producer immediately; `OnError` always synchronous.
- **Build gate:** `TreatWarningsAsErrors=true` — `dotnet build -c Release` must produce 0 warnings on both TFMs. Style is enforced from `.editorconfig`.
- **House style:** tabs (width 2), file-scoped namespaces with `using` lines after the namespace declaration, K&R braces, `var`, no braces for single-line statements, `_camelCase` private fields.
- **Tests:** NUnit, in `tests/Nanov.HighPerformance.Tests/Threading/Workers/`. Struct handlers wrap a shared reference-type state object (the repo's `Rec`/`RecordingState` pattern). Use `WorkerTestUtil.AwaitCompletionOrStall` / `AwaitProgress` for anything that could hang on a lost wakeup.
- **Perf methodology:** local macOS numbers are noisy — only interleaved A/B relative deltas count; a layer with no measurable win is kept/dropped on the standard-technique vs. complexity balance, and the outcome is recorded in `memory.md` either way. No absolute Mops targets.
- **Branch:** all work on `feature/lockstep-throughput-squeeze` (created in Task 1 from the current `docs/project-memory` HEAD). One commit per task.
- **Keyed workers (`ConflationSpWorker`/`CoalescingSpWorker`) are out of scope.** They keep using `TryClaimRun` + `TryTakeAndAdvance` (per-item advance); they inherit the layer-1/2 core wins automatically. Do not move them to the batched API.
- Central package management: any new package version goes in `Directory.Packages.props`, never inline in a `.csproj` (no new packages are expected).

---

### Task 1: Commit the perf harness and capture baseline numbers

The perf-tests harness (untracked `perf-tests/`, plus `Directory.Packages.props` and `Nanov.HighPerformance.slnx` edits adding HdrHistogram and the project) is already written in the working tree but not committed. The whole plan's measurement gate depends on it.

**Files:**
- Commit (already exist, untracked/modified): `perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests/**`, `Directory.Packages.props`, `Nanov.HighPerformance.slnx`
- Modify: `.gitignore` (add local perf-notes dir)

**Interfaces:**
- Produces: the `perf-tests` runner invoked as `dotnet run -c Release --project perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests -- --target SpWorker --report false --print-spec false`. `--target` is a substring match: `SpWorker` selects all worker throughput tests (`OneToOne{Sp,ValueSp,AsyncSp,AsyncValueSp,Conflation,Coalescing}WorkerThroughputTest`) and the two SpWorker latency tests. Default runs: 7 throughput / 3 latency.
- Produces: baseline console captures under `perf-local/` (gitignored) used by every later task's A/B comparison.

- [ ] **Step 1: Create the feature branch**

```bash
git checkout -b feature/lockstep-throughput-squeeze
```

- [ ] **Step 2: Add a gitignored dir for local perf captures**

Append to `.gitignore` (after the `## Agent / tooling` block):

```gitignore
## Local perf captures (machine-specific, never committed)
perf-local/
```

- [ ] **Step 3: Build the full solution including the harness**

Run: `dotnet build -c Release`
Expected: `0 Warning(s)`, `0 Error(s)` for all projects on net9.0 + net10.0 (the perf-tests project is net10.0-only — that's how its csproj is set up; leave it).

If the perf-tests project fails to build, fix the build error inside `perf-tests/` only (it's pre-existing work-in-progress) — do not touch `src/`.

- [ ] **Step 4: Run the existing test suite (sanity gate before any change)**

Run: `dotnet test -c Release`
Expected: all tests pass on both TFMs.

- [ ] **Step 5: Commit the harness**

```bash
git add Directory.Packages.props Nanov.HighPerformance.slnx perf-tests/ .gitignore
git commit -m "test: add throughput/latency perf-test harness (HdrHistogram) for the worker family"
```

- [ ] **Step 6: Capture the baseline**

```bash
mkdir -p perf-local
dotnet run -c Release --project perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests -- --target SpWorker --report false --print-spec false 2>&1 | tee perf-local/L0-baseline.txt
```

Expected: each test prints per-run ops/sec (throughput) / histogram percentiles (latency). This takes several minutes. Close other heavy apps first; don't run anything else concurrently.

- [ ] **Step 7: Record the baseline summary**

Read `perf-local/L0-baseline.txt` and note (in the task summary you report, not in a committed file yet) the mean ops/sec per throughput test. These are the "before" numbers every layer compares against.

---

### Task 2: Layer 1 — bounds-check elimination in `SpRingCore`

Semantics-preserving codegen change: all ring-slot accesses go through one bounds-check-free helper. Also records the (pre-decided) drop of the `_completed || _stop` bitfield fold.

**Files:**
- Modify: `src/Nanov.HighPerformance.Threading.Workers/Internal/SpRingCore.cs`
- Modify: `memory.md` (prepend layer entry)

**Interfaces:**
- Produces: `private ref T SlotRef(int index)` inside `SpRingCore<T>` — used by all later tasks' slot accesses.
- Consumes: nothing new.

There is no new behavior, so no new test (the full existing suite is the guard — it covers every path through these four call sites, including the ref-type slot-clearing GC test `Ref_Type_Slots_Are_Cleared_After_Drain`).

- [ ] **Step 1: Add the `SlotRef` helper**

In `SpRingCore.cs`, immediately above `GetSlotRef` (around line 269), add:

```csharp
	/// <summary>
	///   Ring slot ref without the array bounds check. The JIT can't prove
	///   <c>(index &amp; _mask) &lt; _buffer.Length</c> across fields, but it holds by construction:
	///   <c>_mask = Length − 1</c> with Length a power of two, so the masked index is always in range.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ref T SlotRef(int index) =>
		ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_buffer), (nint)(uint)(index & _mask));
```

(`System.Runtime.CompilerServices` and `System.Runtime.InteropServices` are already imported in this file.)

- [ ] **Step 2: Route the four slot accesses through it**

In `TryEnqueue` (line ~239), replace:

```csharp
		_buffer[tail & _mask] = item;
```

with:

```csharp
		SlotRef(tail) = item;
```

In `GetSlotRef` (line ~271), replace:

```csharp
	internal ref T GetSlotRef(int head) => ref _buffer[head & _mask];
```

with:

```csharp
	internal ref T GetSlotRef(int head) => ref SlotRef(head);
```

In `ClearAndAdvanceHead` (line ~282), replace:

```csharp
		_cleaner.Clear(ref _buffer[head & _mask]);
```

with:

```csharp
		_cleaner.Clear(ref SlotRef(head));
```

In `TakeAndAdvance` (line ~322), replace:

```csharp
		ref var slot = ref _buffer[head & _mask];
```

with:

```csharp
		ref var slot = ref SlotRef(head);
```

- [ ] **Step 3: Build and run the full suite, both configs**

```bash
dotnet build -c Release
dotnet test -c Release
dotnet test -c Debug
```

Expected: 0 warnings, all tests pass on both TFMs in both configs (Debug additionally runs the producer-overlap guard test).

- [ ] **Step 4: A/B perf run**

```bash
dotnet run -c Release --project perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests -- --target SpWorker --report false --print-spec false 2>&1 | tee perf-local/L1-bce.txt
```

Compare mean ops/sec per test against `perf-local/L0-baseline.txt`. Expected: within noise to slightly positive. This layer is kept regardless (standard technique, strictly less work per access).

- [ ] **Step 5: Record the layer in memory.md**

Prepend to `memory.md` (directly under the `---` separator, above the 2026-06-28 entry):

```markdown
## 2026-07-10 — Lock-step squeeze L1: bounds-check elimination; lifecycle-flag fold dropped

All ring-slot accesses now go through `SlotRef` (`MemoryMarshal.GetArrayDataReference` +
`Unsafe.Add`), removing the per-access bounds check (safe by construction: `_mask = length − 1`,
power-of-two length). macOS perf delta: <fill in: e.g. "within noise" or "+X%">. Kept on the
standard-technique basis.
- The spec'd optional fold of `_completed || _stop` into one bitfield was **dropped**: two volatile
  bool loads on a warm line aren't worth converting the lifecycle writes to Interlocked bit ops.
  Don't revisit without a measured reason.
```

Replace `<fill in: …>` with the actual observation from Step 4 before committing.

- [ ] **Step 6: Commit**

```bash
git add src/Nanov.HighPerformance.Threading.Workers/Internal/SpRingCore.cs memory.md
git commit -m "perf: eliminate ring-slot bounds checks via SlotRef helper (L1)"
```

---

### Task 3: Layer 2 guard tests — signaling stress suite (green-first)

These tests must pass on the **current** (pre-flag) code — they guard behavior that layer 2 must preserve. They land first so Task 4 has a hard gate. Tiny capacities maximize empty↔non-empty / full↔non-full churn; ≥1M items make a lost wakeup a near-certain plateau, which the stall watchdog turns into a fast failure.

**Files:**
- Create: `tests/Nanov.HighPerformance.Tests/Threading/Workers/SignalingStressTests.cs`

**Interfaces:**
- Consumes: `SpWorker<T,THandler>`, `AsyncSpWorker<T,THandler>`, `IWorkerHandler<int>`, `IAsyncWorkerHandler<int>` (exact signatures: `void Handle(T)`, `ValueTask HandleAsync(T, CancellationToken)`, `void OnError(Exception)`, `void OnCompleted(Exception?)` / `ValueTask OnCompletedAsync(Exception?)`), `WorkerTestUtil`.
- Produces: the `SignalingStressTests` fixture — Task 4's and Task 6's regression gate.

- [ ] **Step 1: Write the stress fixture**

Create `tests/Nanov.HighPerformance.Tests/Threading/Workers/SignalingStressTests.cs`:

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Workers;

using Nanov.HighPerformance.Threading.Workers;
using NUnit.Framework;

/// <summary>
///   Lost-wakeup stress guards for SpRingCore's wake signaling (see SIGNALING INVARIANT). Tiny
///   capacities maximize empty↔non-empty / full↔non-full boundary churn — every crossing is a
///   park/signal opportunity — and ≥1M items make a lost wakeup a near-certain plateau, which the
///   stall watchdog turns into a fast failure instead of a hung run.
/// </summary>
[TestFixture]
public class SignalingStressTests {
	// Shared observable state; the struct handlers write through this reference (house Rec pattern).
	// Expected/OutOfOrder are consumer-thread-only; Processed is read cross-thread via Volatile.
	private sealed class CountState {
		public int Expected;
		public int OutOfOrder;
		public int Processed;
	}

	private readonly struct CountingHandler : IWorkerHandler<int> {
		private readonly CountState _state;
		private readonly int _sleepEvery;

		public CountingHandler(CountState state, int sleepEvery = 0) {
			_state = state;
			_sleepEvery = sleepEvery;
		}

		public void Handle(int item) {
			if (item != _state.Expected++) _state.OutOfOrder++;
			if (_sleepEvery > 0 && item % _sleepEvery == 0) Thread.Sleep(1);
			Volatile.Write(ref _state.Processed, _state.Expected);
		}

		public void OnError(Exception error) { }
		public void OnCompleted(Exception? error) { }
	}

	private readonly struct AsyncCountingHandler : IAsyncWorkerHandler<int> {
		private readonly CountState _state;

		public AsyncCountingHandler(CountState state) => _state = state;

		// Completes synchronously — keeps the loop in its tight drain; the signaling churn under test
		// happens at the ring boundaries, not inside the handler.
		public ValueTask HandleAsync(int item, CancellationToken cancellationToken) {
			if (item != _state.Expected++) _state.OutOfOrder++;
			Volatile.Write(ref _state.Processed, _state.Expected);
			return default;
		}

		public void OnError(Exception error) { }
		public ValueTask OnCompletedAsync(Exception? error) => default;
	}

	[TestCase(1)]
	[TestCase(2)]
	public async Task Sync_Churn_1M_Items_No_Lost_Wakeup(int capacity) {
		var state = new CountState();
		using var w = new SpWorker<int, CountingHandler>(new CountingHandler(state), capacity);
		w.Start();

		const int n = 1_000_000;
		await Task.Run(() => {
			for (var i = 0; i < n; i++) w.Dispatch(i);
		});
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => Volatile.Read(ref state.Processed), n);

		Assert.That(Volatile.Read(ref state.Processed), Is.EqualTo(n));
		Assert.That(state.OutOfOrder, Is.Zero);
	}

	// An intermittent 1ms handler stall at tiny capacity keeps the ring full, so most dispatches cross
	// the full→non-full boundary — hammering the producer park + space-signal path specifically.
	[TestCase(1)]
	[TestCase(2)]
	public async Task Sync_Blocked_Producer_Churn_No_Lost_Wakeup(int capacity) {
		var state = new CountState();
		using var w = new SpWorker<int, CountingHandler>(new CountingHandler(state, sleepEvery: 1024), capacity);
		w.Start();

		const int n = 200_000;
		await Task.Run(() => {
			for (var i = 0; i < n; i++) w.Dispatch(i);
		});
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => Volatile.Read(ref state.Processed), n);

		Assert.That(Volatile.Read(ref state.Processed), Is.EqualTo(n));
		Assert.That(state.OutOfOrder, Is.Zero);
	}

	[TestCase(1)]
	[TestCase(2)]
	public async Task Async_Churn_1M_Items_No_Lost_Wakeup(int capacity) {
		var state = new CountState();
		using var w = new AsyncSpWorker<int, AsyncCountingHandler>(new AsyncCountingHandler(state), capacity);
		w.Start();

		const int n = 1_000_000;
		await Task.Run(() => {
			for (var i = 0; i < n; i++) w.Dispatch(i);
		});
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => Volatile.Read(ref state.Processed), n);

		Assert.That(Volatile.Read(ref state.Processed), Is.EqualTo(n));
		Assert.That(state.OutOfOrder, Is.Zero);
	}

	// Deterministic async idle-park wake: parks the ThreadPool loop on the armed AsyncAutoResetEvent
	// (no spin phase on the async side), then a lone Dispatch must complete — a skipped Set on the
	// armed waiter fails AwaitProgress instead of hanging.
	[Test]
	public void Async_Dispatch_Wakes_A_Parked_Loop() {
		var state = new CountState();
		using var w = new AsyncSpWorker<int, AsyncCountingHandler>(new AsyncCountingHandler(state), 4);
		w.Start();

		Thread.Sleep(150); // the loop finds the ring empty and arms the async wait
		Assert.That(w.Dispatch(0), Is.True);
		WorkerTestUtil.AwaitProgress(() => Volatile.Read(ref state.Processed), 1);

		Thread.Sleep(150); // loop re-arms (ring drained, empty again)
		Assert.That(w.Dispatch(1), Is.True);
		WorkerTestUtil.AwaitProgress(() => Volatile.Read(ref state.Processed), 2);

		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => Volatile.Read(ref state.Processed), 2);
	}
}
```

- [ ] **Step 2: Run the new fixture — must be green on current code**

Run: `dotnet test -c Release --filter "FullyQualifiedName~SignalingStressTests"`
Expected: PASS, all 7 test cases, both TFMs. (These are regression guards, not red-green TDD — the current code must already satisfy them. If any fails here, STOP: something else is broken; investigate before proceeding.)

- [ ] **Step 3: Commit**

```bash
git add tests/Nanov.HighPerformance.Tests/Threading/Workers/SignalingStressTests.cs
git commit -m "test: signaling lost-wakeup stress guards ahead of the parked-flag rework"
```

---

### Task 4: Layer 2 — parked-flag signaling in `SpRingCore`

Replaces the per-item cross-core edge reads (`CommitTail` reading `Head`, `AdvanceHead` reading `Tail`) with parked-waiter flags. The safety invariant, stated once and preserved everywhere: **a signal may be spurious, never lost** — each side raises its flag *before* its spin-guarded (sync) or fenced (async) final recheck of the other index, so either the committer sees the flag or the waiter's recheck sees the fresh index and never parks. Shutdown signals are never flag-gated.

**Files:**
- Modify: `src/Nanov.HighPerformance.Threading.Workers/Internal/SpRingCore.cs`
- Modify: `src/Nanov.HighPerformance.Threading.Workers/AsyncSpWorker.cs:86-96`
- Modify: `src/Nanov.HighPerformance.Threading.Workers/AsyncValueSpWorker.cs:105-135`
- Modify: `src/Nanov.HighPerformance.Threading.Workers/CLAUDE.md` (signaling sections)
- Modify: `memory.md`

**Interfaces:**
- Produces: `internal void AckWake()` on `SpRingCore<T>` — clears the async consumer's parked flag; called by both async worker loops once per wake (Task 6 keeps these call sites).
- Produces: `PaddedIndices.ConsumerParked` / `PaddedIndices.ProducerParked` fields (core-internal).
- Consumes: `SignalingStressTests` (Task 3) as the gate.

- [ ] **Step 1: Extend `PaddedIndices` with the two flags**

At the bottom of `SpRingCore.cs`, replace the whole `PaddedIndices` struct (including its XML doc) with:

```csharp
/// <summary>
///   Cache-line-separated SPSC ring indices + parked-waiter flags. Non-generic on purpose — generic
///   types can't have explicit layout. Each field sits on its own 128-byte stride (one full cache
///   line on Apple Silicon, two on x64) so no two of them — nor any neighboring class field — share
///   a line: <see cref="Head" /> is written per item by the consumer, <see cref="Tail" /> per item
///   by the producer, and the parked flags are written only on slow-path entry/exit but read per
///   item by the opposite side (see SIGNALING INVARIANT).
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = 640)]
internal struct PaddedIndices {
	[FieldOffset(128)] public int Head;
	[FieldOffset(256)] public int Tail;
	[FieldOffset(384)] public int ConsumerParked; // 1 while the consumer is in WaitForItemSlow / armed via WaitForItemAsync
	[FieldOffset(512)] public int ProducerParked; // 1 while the producer is in WaitForSpaceSlow
}
```

- [ ] **Step 2: Rewrite `CommitTail`**

Replace the whole `CommitTail` method (keep its `<summary>` line, replace body + inner comments):

```csharp
	/// <summary>Publish the slot at <paramref name="tail" /> and wake the consumer if it is parked.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void CommitTail(int tail) {
		Volatile.Write(ref _idx.Tail, tail + 1);
		// Parked-flag signaling: wake the consumer only when its parked flag is up, instead of
		// re-deriving the empty→non-empty edge from a per-commit cross-core read of _idx.Head. The
		// flag's line changes only on slow-path entry/exit, so this read stays warm while the consumer
		// is running. A signal may be SPURIOUS (flag up = consumer still in its spin phase — harmless,
		// the events latch), never LOST: the consumer raises the flag BEFORE its spin-guarded (sync) /
		// fenced (async) final recheck of _idx.Tail. See SIGNALING INVARIANT.
		if (_itemReadyAsync is null) {
			// Sync consumer: no per-commit fence. The flag goes up before WaitForItemSlow's whole
			// ~200-iteration spin phase (which re-reads _idx.Tail), so a commit whose — possibly
			// store-buffer-hoisted — flag read missed the flag has its Tail store visible long before
			// the consumer's fenced pre-park recheck.
			if (Volatile.Read(ref _idx.ConsumerParked) != 0)
				_itemEvt.Set();
		} else {
			// Async consumer parks without a spin phase, so the Dekker window is real: the full fence
			// makes the Tail publish globally visible before the flag read, paired with the
			// arm-then-recheck in WaitForItemAsync (whose WaitAsync CAS is the consumer-side fence).
			Interlocked.MemoryBarrier();
			if (Volatile.Read(ref _idx.ConsumerParked) != 0)
				_itemReadyAsync.Set();
		}
	}
```

- [ ] **Step 3: Rewrite `AdvanceHead`**

Replace the whole `AdvanceHead` method:

```csharp
	/// <summary>Release the slot at <paramref name="head" /> and wake the producer if it is parked.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void AdvanceHead(int head) {
		Volatile.Write(ref _idx.Head, head + 1);
		// Parked-flag mirror of CommitTail: wake a blocked producer via its parked flag instead of a
		// per-item cross-core read of _idx.Tail for the full→non-full edge. Spurious-never-lost: the
		// producer raises the flag before WaitForSpaceSlow's spin phase, which re-reads _idx.Head.
		if (Volatile.Read(ref _idx.ProducerParked) != 0)
			_spaceEvt.Set();
	}
```

- [ ] **Step 4: Rewrite `WaitForItemSlow`**

Replace the whole method:

```csharp
	// Cold path: consumer parks until an item arrives, or the worker stops/completes-drained.
	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool WaitForItemSlow() {
		// Raise the parked flag for the WHOLE slow path (spin phase included): CommitTail may then fire
		// a spurious Set while we're still spinning — harmless (latching event) — but a commit can never
		// find the flag down while we end up parked. Cleared on every exit.
		Volatile.Write(ref _idx.ConsumerParked, 1);
		try {
			var spins = 0;
			while (_idx.Head == Volatile.Read(ref _idx.Tail)) {
				if (_stop) return false;
				// Soft-complete drains: re-read _idx.Tail after observing _completed (acquire) so an item
				// committed right before completion — not yet visible to the empty check above — is still
				// drained.
				if (_completed) return _idx.Head != Volatile.Read(ref _idx.Tail);
				if (spins++ < SpinThreshold) {
					Thread.SpinWait(10);
					continue;
				}

				// About to kernel-park. One full fence per park (cold): guarantees the flag write above is
				// globally visible before this final emptiness recheck — the Dekker pairing with
				// CommitTail's Tail-write→flag-read. If an item slipped in, drain it instead of parking.
				Interlocked.MemoryBarrier();
				if (_idx.Head != Volatile.Read(ref _idx.Tail)) return true;
				_itemEvt.Wait();
				_itemEvt.Reset();
				spins = 0;
			}

			return true;
		}
		finally {
			Volatile.Write(ref _idx.ConsumerParked, 0);
		}
	}
```

- [ ] **Step 5: Rewrite `WaitForSpaceSlow`**

Replace the whole method:

```csharp
	// Cold path: producer parks until space frees, or the worker stops/completes.
	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool WaitForSpaceSlow() {
		// Parked flag mirrors WaitForItemSlow: raised for the whole slow path, read by AdvanceHead
		// instead of a per-item _idx.Tail read. Spurious Sets while still spinning are harmless.
		Volatile.Write(ref _idx.ProducerParked, 1);
		try {
			var spins = 0;
			int head;
			while (_idx.Tail - (head = Volatile.Read(ref _idx.Head)) == _buffer.Length) {
				if (_stop || _completed) return false;
				if (spins++ < SpinThreshold) {
					Thread.SpinWait(10);
					continue;
				}

				// About to kernel-park: full fence + final recheck (Dekker pairing with AdvanceHead's
				// Head-write→flag-read), then park. See WaitForItemSlow.
				Interlocked.MemoryBarrier();
				if (_idx.Tail - Volatile.Read(ref _idx.Head) != _buffer.Length) continue;
				_spaceEvt.Wait();
				_spaceEvt.Reset();
				spins = 0;
			}

			_cachedHead = head; // refresh the gating cache with the Head that proved there is space
			return true;
		}
		finally {
			Volatile.Write(ref _idx.ProducerParked, 0);
		}
	}
```

- [ ] **Step 6: Update `WaitForItemAsync` and add `AckWake`**

Replace the whole `WaitForItemAsync` method with:

```csharp
	/// <summary>
	///   Item wait for the ThreadPool-async loop when the ring is empty. Returns a
	///   <see cref="ValueTask{Boolean}" />: completed-<see langword="false" /> on hard stop or
	///   soft-complete-drained (caller exits); completed-<see langword="true" /> when an item is
	///   already available; otherwise the <see cref="AsyncAutoResetEvent" />'s pending wait (result
	///   <see langword="true" /> = "woke" — the caller re-checks via <see cref="TryClaimRunNow" /> on
	///   the next loop turn, after <see cref="AckWake" />). Deliberately NOT an <c>async</c> method, so
	///   an empty wait allocates nothing — the suspension reuses the caller's single (one-time)
	///   RunLoopAsync state machine. Hard stop is checked first, so queued items are dropped on stop.
	///   Single logical consumer only.
	/// </summary>
	internal ValueTask<bool> WaitForItemAsync() {
		if (_stop) return new ValueTask<bool>(false);
		if (_idx.Head != Volatile.Read(ref _idx.Tail)) return new ValueTask<bool>(true);
		if (_completed) return DrainedOnComplete();

		// Raise the parked flag BEFORE arming, then recheck. CommitTail's fenced flag read pairs with
		// this: either the producer sees the flag (and Sets), or this recheck — ordered after the flag
		// write by WaitAsync's Interlocked CAS (a full fence) — sees the fresh Tail and self-signals so
		// the loop drains instead of parking on a skipped edge. Also catches a late stop/complete. The
		// flag is cleared by the loop via AckWake once the wait completes, so it stays up while parked.
		Volatile.Write(ref _idx.ConsumerParked, 1);
		var vt = _itemReadyAsync!.WaitAsync();
		if (vt.IsCompleted) return vt; // a Set was latched between the empty check above and the arm
		if (_stop || _completed || _idx.Head != Volatile.Read(ref _idx.Tail))
			_itemReadyAsync.Set(); // self-signal: complete the armed wait so the loop re-evaluates
		return vt;
	}

	/// <summary>
	///   Clear the async consumer's parked flag — called by the async loops once per wake, before
	///   draining, so producers stop paying the (cheap, latching) spurious Set per commit while the
	///   consumer is running. Clearing an already-0 flag is a harmless consumer-owned-line write.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void AckWake() => Volatile.Write(ref _idx.ConsumerParked, 0);
```

- [ ] **Step 7: Call `AckWake` from both async loops**

In `AsyncSpWorker.RunLoopAsync` (line ~86), change the outer loop from:

```csharp
			do {
				while (_core.TryClaimRunNow(out var head, out var available)) {
```

to:

```csharp
			do {
				_core.AckWake(); // clear the parked flag once per wake, before draining
				while (_core.TryClaimRunNow(out var head, out var available)) {
```

In `AsyncValueSpWorker.RunLoopAsync` (line ~105), change:

```csharp
			while (true) {
				while (_core.TryClaimRunNow(out var head, out var available)) {
```

to:

```csharp
			while (true) {
				_core.AckWake(); // clear the parked flag once per wake, before draining
				while (_core.TryClaimRunNow(out var head, out var available)) {
```

- [ ] **Step 8: Rewrite the `SpRingCore` class-doc SIGNALING INVARIANT paragraph**

In the `SpRingCore<T>` XML doc (lines ~25–37), replace the `SIGNALING INVARIANT` `<para>` with:

```csharp
///   <para>
///     SIGNALING INVARIANT (strict SPSC, parked-flag protocol). Exactly one producer thread
///     reserves+commits slots; the single consumer is the only writer of <c>_idx.Head</c>. Wakeups
///     are gated by parked-waiter flags, not by re-deriving the empty/full edge from the other
///     side's index: a waiter raises its flag (<c>_idx.ConsumerParked</c> / <c>_idx.ProducerParked</c>)
///     when it ENTERS its slow path and clears it on exit; the opposite side signals only when the
///     flag is up. A signal may be SPURIOUS, never LOST: the flag goes up before the waiter's final
///     recheck of the other index, and that recheck is ordered after the flag write — by a full
///     fence before every kernel park (sync, cold) or by the <c>WaitAsync</c> CAS in
///     arm-then-recheck (async) — so either the committer sees the flag, or the waiter sees the
///     fresh index and never parks. The sync consumer's ~200-iteration spin additionally masks the
///     store-buffer window without any per-commit fence; the async consumer has no spin phase, so
///     the async branch of <c>CommitTail</c> keeps a per-commit <c>Interlocked.MemoryBarrier()</c>
///     (StoreLoad — <c>Volatile</c> release/acquire does not order a store before a subsequent load
///     of a different location on ARM64). Shutdown rides <c>volatile _stop</c> / <c>_completed</c>;
///     <c>SignalStop</c>/<c>TryComplete</c> Set every event UNCONDITIONALLY (never flag-gated) and
///     both slow paths recheck them before every park. WARNING: a second concurrent producer, or
///     advancing <c>_idx.Head</c> off the worker thread, breaks this and hangs silently.
///   </para>
```

- [ ] **Step 9: Rewrite the worker-tree CLAUDE.md signaling sections**

In `src/Nanov.HighPerformance.Threading.Workers/CLAUDE.md`, replace the section `## The one rule: SPSC SIGNALING INVARIANT` **and** its subsection `### Async-path StoreLoad fences (don't remove — ARM64 lost-wakeup)` (everything from the `## The one rule` heading down to — but not including — the bullet `- **Single consumer is structural**`) with:

```markdown
## The one rule: SPSC SIGNALING INVARIANT (parked-flag protocol)

The whole design is correct **only** under strict single-producer / single-consumer. Wakeups are
gated by **parked-waiter flags**, not by re-deriving the empty/full edge from the other side's
index: each side raises its flag (`_idx.ConsumerParked` / `_idx.ProducerParked`, each on its own
padded cache line) when it ENTERS its slow path — before the spin phase (`WaitForItemSlow` /
`WaitForSpaceSlow`) or before arming the async wait (`WaitForItemAsync`) — and clears it on exit
(sync: `finally`; async: `AckWake`, called by the loop once per wake). The opposite side signals
(`_itemEvt` / `_spaceEvt` / `_itemReadyAsync`) only when it sees the flag up — a warm-line read,
replacing the per-item cross-core read of the opposite index.

**A signal may be spurious, never lost.** The flag goes up *before* the waiter's final recheck of
the other index, and that recheck is ordered after the flag write — by an `Interlocked.MemoryBarrier()`
before every kernel park (cold, sync side) or by the `WaitAsync` CAS in arm-then-recheck (async
side). So either the committer sees the flag (and Sets — possibly spuriously while the waiter is
still spinning: harmless, the events latch), or the waiter sees the fresh index and never parks.
The sync consumer's ~200-iteration spin additionally masks the store-buffer window with no
per-commit fence; the async consumer has no spin phase, so `CommitTail`'s async branch keeps its
per-commit `Interlocked.MemoryBarrier()` (StoreLoad — `Volatile` release/acquire does NOT order a
store before a subsequent load of a different location on ARM64/Apple Silicon).

Things that must stay exactly as they are:

- exactly one producer writes `_tail`; exactly one consumer writes `_head`; each flag is written
  only by its own side.
- `SignalStop` / `TryComplete` Set everything **unconditionally** — shutdown is never flag-gated.
- **Arm-then-recheck in `WaitForItemAsync`**: raise flag → `WaitAsync()` (CAS = full fence) →
  recheck `_tail`/`_stop`/`_completed` → self-`Set()` if anything appeared. Don't collapse it to a
  single check-then-wait.
- **Soft-complete drain** (`WaitForItemAsync` + `WaitForItemSlow`): when the ring looks empty,
  re-read `_tail` **after** observing `_completed` (published with release after the producer's
  last commit) — without the re-read a stale empty check silently drops the tail items. `_stop`
  (hard stop) still exits without draining — that's the drop-on-cancel contract.
- The pre-park fence + final recheck in both sync slow paths.

**A second concurrent producer, or advancing `_head` off the worker thread, breaks this and hangs
silently.** If you touch `SpRingCore` producer/consumer/signaling code, preserve this exactly. The
full write-up lives in the `SpRingCore<T>` XML doc. The regression net is
`SignalingStressTests` + `AsyncAutoResetEventTests` — run them after any change here.
```

- [ ] **Step 10: Build and run the full gate**

```bash
dotnet build -c Release
dotnet test -c Release
dotnet test -c Debug
```

Expected: 0 warnings; all tests green on both TFMs, both configs — including `SignalingStressTests` (the layer's specific gate) and `AsyncAutoResetEventTests` (untouched, must stay green). A lost wakeup shows up as a `TimeoutException` from a stall watchdog within ~5s — if that happens, the flag protocol has a hole; re-check Steps 2–6 against the invariant before touching anything else.

- [ ] **Step 11: A/B perf run**

```bash
dotnet run -c Release --project perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests -- --target SpWorker --report false --print-spec false 2>&1 | tee perf-local/L2-flags.txt
```

Compare against `perf-local/L1-bce.txt` (and L0). This layer attacks the per-item cross-core traffic directly — expect a visible throughput delta. Record whatever is observed.

- [ ] **Step 12: Record the layer in memory.md**

Prepend to `memory.md` (above the L1 entry):

```markdown
## 2026-07-10 — Lock-step squeeze L2: parked-flag signaling replaces per-item cross-index edge reads

`CommitTail` no longer reads `Head` per commit and `AdvanceHead` no longer reads `Tail` per item:
each side raises a padded parked flag on slow-path entry (before its spin/arm phase) and the
committer signals only when the flag is up — "a signal may be spurious, never lost". Sync paths
add one full fence per kernel park (cold); the async commit fence is unchanged. macOS perf delta
vs L1: <fill in>.
- Don't reintroduce the index-edge Set test; don't gate SignalStop/TryComplete on the flags; don't
  collapse arm-then-recheck.
```

Replace `<fill in>` with the Step 11 observation.

- [ ] **Step 13: Commit**

```bash
git add src/Nanov.HighPerformance.Threading.Workers/Internal/SpRingCore.cs \
        src/Nanov.HighPerformance.Threading.Workers/AsyncSpWorker.cs \
        src/Nanov.HighPerformance.Threading.Workers/AsyncValueSpWorker.cs \
        src/Nanov.HighPerformance.Threading.Workers/CLAUDE.md \
        memory.md
git commit -m "perf: parked-flag wake signaling — drop both per-item cross-core index reads (L2)"
```

---

### Task 5: Layer 3 guard tests — batched-release hazards (green-first)

Deterministic guards for the two layer-3 hazards (blocked-producer stall bound, `Release` immediacy) plus a drain-with-pending-batch check. They must pass on the layer-2 code first (they assert liveness/ordering the current per-item-advance code trivially satisfies), then keep passing after Task 6.

**Files:**
- Create: `tests/Nanov.HighPerformance.Tests/Threading/Workers/BatchedReleaseTests.cs`

**Interfaces:**
- Consumes: `SpWorker`, `ValueSpWorker`, `IWorkerHandler<int>`, `IValueWorkerHandler<T>` (`void Handle(ref HandleScope<T>)`, `void Clear(ref T)`, `void OnError(Exception)`, `void OnCompleted(Exception?)`), `HandleScope<T>.Release`, `PublishScope<T>`, `WorkerTestUtil`.
- Produces: the `BatchedReleaseTests` fixture — Task 6's gate.

- [ ] **Step 1: Write the fixture**

Create `tests/Nanov.HighPerformance.Tests/Threading/Workers/BatchedReleaseTests.cs`:

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Workers;

using Nanov.HighPerformance.Threading.Workers;
using NUnit.Framework;

/// <summary>
///   Guards for the batched-head-publish hazards (see BATCHED HEAD PUBLISH in the worker CLAUDE.md):
///   a parked producer must be flushed within at most one handler execution; HandleScope.Release must
///   publish immediately (never join the batch); soft-complete must drain everything even when
///   releases are sitting unpublished in the batch at complete time. All three also hold trivially on
///   the pre-batching (per-item advance) code — they land first as green regression guards.
/// </summary>
[TestFixture]
public class BatchedReleaseTests {
	private sealed class HookState {
		public Action<int>? Hook;
		public int Processed;
	}

	private readonly struct HookHandler : IWorkerHandler<int> {
		private readonly HookState _state;

		public HookHandler(HookState state) => _state = state;

		public void Handle(int item) {
			_state.Hook?.Invoke(item);
			Volatile.Write(ref _state.Processed, _state.Processed + 1);
		}

		public void OnError(Exception error) { }
		public void OnCompleted(Exception? error) { }
	}

	// Consumer gated inside item 0's handler while the producer fills the ring and parks. Ungating
	// the handler must unblock the producer promptly (run-boundary / parked-flag flush) — a batch
	// that only flushes on a count threshold would leave the producer parked forever here.
	// Fill math is behavior-agnostic on purpose (this must be green BEFORE batching lands too):
	// after item 0 is claimed, Head is 1 under per-item advance but 0 under a pending batch, so the
	// foreground fill stops at 15 extra items (safe either way) and the background task dispatches
	// TWO more — it blocks on the first or the second, under either behavior, while the handler is
	// gated.
	[Test]
	public async Task Parked_Producer_Unblocks_After_At_Most_One_Handler() {
		var entered = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		var state = new HookState {
			Hook = i => {
				if (i != 0) return;
				entered.Set();
				gate.Wait();
			}
		};
		using var w = new SpWorker<int, HookHandler>(new HookHandler(state), 16); // flush threshold 4
		w.Start();

		Assert.That(w.Dispatch(0), Is.True);
		Assert.That(entered.Wait(2000), Is.True, "consumer must be gated inside item 0's handler");
		for (var i = 1; i <= 15; i++) Assert.That(w.Dispatch(i), Is.True); // never blocks, either behavior

		var blocked = Task.Run(() => w.Dispatch(16) && w.Dispatch(17));
		Assert.That(blocked.Wait(50), Is.False, "producer should be parked on the full ring");

		gate.Set(); // finish item 0's handler — flush must free space within this handler boundary
		Assert.That(await blocked, Is.True);

		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => Volatile.Read(ref state.Processed), 18);
		Assert.That(Volatile.Read(ref state.Processed), Is.EqualTo(18));
	}

	// Soft-complete fired while the consumer is mid-run (intermittent stalls keep releases batched)
	// must still deliver every committed item — the drain path always flushes at run/park boundaries.
	[Test]
	public void Soft_Complete_Drains_With_Batched_Releases_Pending() {
		var state = new HookState { Hook = i => { if ((i & 255) == 0) Thread.Sleep(1); } };
		using var w = new SpWorker<int, HookHandler>(new HookHandler(state), 64); // flush threshold 16
		w.Start();

		const int n = 10_000;
		for (var i = 0; i < n; i++) w.Dispatch(i); // this test thread is the single producer
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => Volatile.Read(ref state.Processed), n);

		Assert.That(Volatile.Read(ref state.Processed), Is.EqualTo(n));
	}

	private struct Slot {
		public int Value;
	}

	private sealed class EarlyReleaseState {
		public readonly ManualResetEventSlim Entered = new(false);
		public readonly ManualResetEventSlim CanRelease = new(false);
		public readonly ManualResetEventSlim Released = new(false);
		public readonly ManualResetEventSlim Finish = new(false);
		public int Processed;
	}

	private readonly struct EarlyReleaseHandler : IValueWorkerHandler<Slot> {
		private readonly EarlyReleaseState _state;

		public EarlyReleaseHandler(EarlyReleaseState state) => _state = state;

		public void Handle(ref HandleScope<Slot> scope) {
			var value = scope.Event().Value;
			if (value == 0) {
				_state.Entered.Set();
				_state.CanRelease.Wait();
				scope.Release(); // must publish IMMEDIATELY — the parked producer unblocks mid-handler
				_state.Released.Set();
				_state.Finish.Wait();
			}

			Volatile.Write(ref _state.Processed, _state.Processed + 1);
		}

		public void Clear(ref Slot value) { }
		public void OnError(Exception error) { }
		public void OnCompleted(Exception? error) { }
	}

	// The early-release contract survives batching: a producer parked on the full ring resumes the
	// moment the handler calls Release — while the handler is still running — because Release
	// bypasses the batch and publishes immediately.
	[Test]
	public void HandleScope_Release_Unblocks_A_Parked_Producer_Mid_Handler() {
		var state = new EarlyReleaseState();
		using var w = new ValueSpWorker<Slot, EarlyReleaseHandler>(new EarlyReleaseHandler(state), 4);
		w.Start();

		using (var s = w.Publish()) s.Event().Value = 0;
		Assert.That(state.Entered.Wait(2000), Is.True, "consumer must be inside item 0's handler");
		// Slot 0 is still held by the scope (not released) → Head = 0. Three more publishes make the
		// cap-4 ring full; the next one blocks.
		for (var i = 1; i <= 3; i++)
			using (var s = w.Publish())
				s.Event().Value = i;

		var blocked = Task.Run(() => {
			using var s = w.Publish();
			if (!s.IsOpen) return false;
			s.Event().Value = 4;
			return true;
		});
		Assert.That(blocked.Wait(50), Is.False, "producer should be blocked on the full ring");

		state.CanRelease.Set();
		Assert.That(state.Released.Wait(2000), Is.True);
		Assert.That(blocked.Wait(2000), Is.True, "Release must publish immediately and unblock the producer");
		Assert.That(blocked.Result, Is.True);
		Assert.That(Volatile.Read(ref state.Processed), Is.Zero, "item 0's handler must still be running");

		state.Finish.Set();
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => Volatile.Read(ref state.Processed), 5);
		Assert.That(Volatile.Read(ref state.Processed), Is.EqualTo(5));
	}
}
```

- [ ] **Step 2: Run the new fixture — must be green on layer-2 code**

Run: `dotnet test -c Release --filter "FullyQualifiedName~BatchedReleaseTests"`
Expected: PASS, all 3 tests, both TFMs. (Green-first regression guards; a failure here means layer 2 broke something — stop and investigate.)

- [ ] **Step 3: Commit**

```bash
git add tests/Nanov.HighPerformance.Tests/Threading/Workers/BatchedReleaseTests.cs
git commit -m "test: batched-release hazard guards ahead of the deferred head publish"
```

---

### Task 6: Layer 3 — batched head publish

The consumer keeps a local head cursor and publishes per policy instead of per item. Slots are still cleared per item. The three flush triggers: every `_flushThreshold` items, at every run boundary (and hence before every park — an empty ring implies everything is published, making the two-sided-park deadlock structurally impossible), and immediately when `ProducerParked` is up (checked per item — warm line — so a parked producer waits out at most the one handler currently running). `HandleScope.Release` keeps publishing immediately; only the loops' automatic releases defer.

**Files:**
- Modify: `src/Nanov.HighPerformance.Threading.Workers/Internal/SpRingCore.cs` (new consumer batch API + `_flushThreshold`)
- Modify: `src/Nanov.HighPerformance.Threading.Workers/Scopes.cs` (`HandleScope.ReleaseDeferred`)
- Modify: `src/Nanov.HighPerformance.Threading.Workers/SpWorker.cs:61-90` (RunLoop)
- Modify: `src/Nanov.HighPerformance.Threading.Workers/ValueSpWorker.cs:89-124` (RunLoop)
- Modify: `src/Nanov.HighPerformance.Threading.Workers/AsyncSpWorker.cs:79-109` (RunLoopAsync)
- Modify: `src/Nanov.HighPerformance.Threading.Workers/AsyncValueSpWorker.cs:98-148` (RunLoopAsync)
- Modify: `src/Nanov.HighPerformance.Threading.Workers/CLAUDE.md` (new section + two stale mentions)
- Modify: `memory.md`

**Interfaces:**
- Produces (on `SpRingCore<T>`): `internal bool TryTake(int head, out T item)` (stop-check + copy + conditional clear, NO head publish); `internal void ClearSlot(int head)` (cleaner only, no publish); `internal void PublishHead(int nextHead)` (absolute publish + parked-producer wake); `internal bool ShouldFlush(int pending)` (`pending >= _flushThreshold || ProducerParked`).
- Produces (on `HandleScope<T>`): `internal void ReleaseDeferred()` — clear-without-publish for the loops' auto-release.
- Consumes: `AckWake` (Task 4), `SlotRef` (Task 2), flags (Task 4). Keyed workers keep consuming `TryClaimRun`/`TryTakeAndAdvance`/`AdvanceHead` unchanged.

- [ ] **Step 1: Add `_flushThreshold` and the batch API to `SpRingCore`**

Add the field below `_cachedHead` (line ~74):

```csharp
	// Batched-release flush threshold for the FIFO drain loops: publish Head every this-many deferred
	// releases (also always at run boundaries, and immediately when the producer is parked). max(1, …)
	// disables batching for tiny rings; capacity/4 keeps at least ~3/4 of a small ring usable while a
	// batch is pending; 32 caps the staleness for big rings.
	private readonly int _flushThreshold;
```

In **both** real constructors (the dedicated-thread one at line ~85 and the ThreadPool-async one at line ~102), add after the `_mask = capacity - 1;` line:

```csharp
		_flushThreshold = Math.Max(1, Math.Min(32, capacity >> 2));
```

Then add the four methods, right after `ClearAndAdvanceHead`:

```csharp
	/// <summary>
	///   Batched-release twin of <see cref="TryTakeAndAdvance" />: hard-stop check + copy-out + slot
	///   clear, WITHOUT publishing <c>Head</c>. The FIFO drain loops batch the publishes through
	///   <see cref="PublishHead" /> per <see cref="ShouldFlush" /> — and ALWAYS flush at run
	///   boundaries, i.e. before every re-claim/park: an empty ring implies everything is published
	///   (the no-deadlock rule; see BATCHED HEAD PUBLISH in CLAUDE.md). The keyed workers keep the
	///   per-item <see cref="TryTakeAndAdvance" /> path.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryTake(int head, out T item) {
		if (_stop) {
			item = default!;
			return false;
		}

		ref var slot = ref SlotRef(head);
		item = slot;
		if (RuntimeHelpers.IsReferenceOrContainsReferences<T>())
			slot = default!;
		return true;
	}

	/// <summary>
	///   Run the bound cleaner on slot <paramref name="head" /> without publishing — the deferred
	///   (auto-release) half of the value path; <see cref="HandleScope{T}.ReleaseDeferred" /> calls
	///   this, the loop publishes later per its flush policy.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void ClearSlot(int head) => _cleaner.Clear(ref SlotRef(head));

	/// <summary>
	///   Publish the consumer's head cursor (ABSOLUTE value, unlike <see cref="AdvanceHead" />'s
	///   item index) and wake the producer if it is parked. Every slot below
	///   <paramref name="nextHead" /> must already be cleared/copied out.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void PublishHead(int nextHead) {
		Volatile.Write(ref _idx.Head, nextHead);
		if (Volatile.Read(ref _idx.ProducerParked) != 0)
			_spaceEvt.Set();
	}

	/// <summary>
	///   Flush policy for the batched release: publish every <c>_flushThreshold</c> deferred items, or
	///   immediately when the producer is parked (warm-line read, checked per item) — so a blocked
	///   producer never waits on the batch, only on the one handler currently running.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool ShouldFlush(int pending) =>
		pending >= _flushThreshold || Volatile.Read(ref _idx.ProducerParked) != 0;
```

And simplify `AdvanceHead` to route through `PublishHead` (one signal site):

```csharp
	/// <summary>Release the slot at <paramref name="head" /> and wake the producer if it is parked.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void AdvanceHead(int head) => PublishHead(head + 1);
```

- [ ] **Step 2: Add `ReleaseDeferred` to `HandleScope<T>`**

In `Scopes.cs`, after the `Release()` method (line ~89), add:

```csharp
	/// <summary>
	///   Loop-internal deferred release: run the handler's <c>Clear</c> on the slot but leave the
	///   <c>Head</c> publish to the drain loop's batch flush. The public <see cref="Release" /> — the
	///   handler-facing "free this slot NOW" API — still publishes immediately: early release must
	///   unblock a waiting producer without waiting for the batch.
	/// </summary>
	internal void ReleaseDeferred() {
		if (IsReleased) return;
		IsReleased = true;
		_core.ClearSlot(_head);
	}
```

- [ ] **Step 3: Rewrite `SpWorker.RunLoop`**

Replace the `try` block's contents (the comment + `while` loop, lines ~63–77) with:

```csharp
			// Claim the whole available run, drain it item-by-item, batching the Head publishes: each
			// slot is still copied out + cleared per item (TryTake), but Head is published only every
			// _flushThreshold items, at the run boundary, or immediately when the producer is parked
			// (ShouldFlush, warm-line read) — cutting the per-item volatile Head write and the
			// producer-side fullness-refresh misses while keeping a parked producer's wait bounded by
			// the one handler currently running. The run-boundary flush runs before every re-claim (and
			// thus before every park): an empty ring implies everything is published — no deadlock.
			while (_core.TryClaimRun(out var head, out var available)) {
				var pending = 0;
				while (head != available && _core.TryTake(head, out var item)) {
					head++;
					pending++;
					if (_core.ShouldFlush(pending)) {
						_core.PublishHead(head);
						pending = 0;
					}

					try {
						_handler.Handle(item);
					}
					catch (Exception ex) {
						_handler.OnError(ex);
					}
				}

				if (pending > 0) _core.PublishHead(head);
			}
```

(The `finally` block is unchanged.)

- [ ] **Step 4: Rewrite `ValueSpWorker.RunLoop`**

Replace the `try` block's contents (comment + loop, lines ~91–111) with:

```csharp
			// Claim the whole available run, drain it scope-by-scope with batched Head publishes. The
			// pre-handler ShouldFlush check publishes PRIOR items only (head = this item's index — its
			// slot is still owned by the scope), so a producer parked during the previous handler never
			// also waits out this one. A handler that Releases early publishes immediately through the
			// scope (Head is absolute, covering any deferred predecessors) — the batch restarts empty.
			// Auto-releases defer via ReleaseDeferred (clear now, publish with the batch). Run-boundary
			// flush before every re-claim/park: empty ring ⇒ all published — no deadlock.
			while (_core.TryClaimRun(out var head, out var available)) {
				var pending = 0;
				do {
					if (_core.ShouldFlush(pending)) {
						_core.PublishHead(head);
						pending = 0;
					}

					var scope = new HandleScope<T>(_core, head);
					try {
						_handler.Handle(ref scope);
					}
					catch (Exception ex) {
						_handler.OnError(ex);
					}
					finally {
						if (scope.IsReleased) {
							pending = 0; // early release already published past this item
						} else {
							scope.ReleaseDeferred();
							pending++;
						}
					}
				}
				while (!_core.IsStopped && ++head != available);

				if (pending > 0) _core.PublishHead(head);
			}
```

(On the hard-stop exit `head` was not incremented past the last item, so the trailing `PublishHead(head)` under-publishes by one — safe: hard stop drops everything anyway, and it never exposes an uncleared slot.)

- [ ] **Step 5: Rewrite `AsyncSpWorker.RunLoopAsync` drain**

Replace the `do { … } while (await …)` block (lines ~86–96, including the `AckWake` line added in Task 4) with:

```csharp
			do {
				_core.AckWake(); // clear the parked flag once per wake, before draining
				while (_core.TryClaimRunNow(out var head, out var available)) {
					var pending = 0;
					while (head != available && _core.TryTake(head, out var item)) {
						head++;
						pending++;
						if (_core.ShouldFlush(pending)) {
							_core.PublishHead(head);
							pending = 0;
						}

						try {
							await _handler.HandleAsync(item, ct).ConfigureAwait(false);
						} catch (Exception ex) {
							_handler.OnError(ex);
						}
					}

					if (pending > 0) _core.PublishHead(head);
				}
			} while (await _core.WaitForItemAsync().ConfigureAwait(false));
```

Also update the comment above the loop (lines ~82–85): replace the sentence starting `Head still advances per item via TryTakeAndAdvance` with `Head publishes are batched (TryTake + PublishHead per ShouldFlush — every K items / run boundary / producer parked); slots are still copied out per item.`

- [ ] **Step 6: Rewrite `AsyncValueSpWorker.RunLoopAsync` drain**

Replace the `while (true) { … }` block (lines ~105–135, including the Task 4 `AckWake` line) with:

```csharp
			while (true) {
				_core.AckWake(); // clear the parked flag once per wake, before draining
				while (_core.TryClaimRunNow(out var head, out var available)) {
					var pending = 0;
					do {
						if (_core.ShouldFlush(pending)) {
							_core.PublishHead(head);
							pending = 0;
						}

						// The ref-struct scope is consumed synchronously (read + Release) before the await —
						// it never crosses the suspension. The handler returns a ValueTask for any async tail.
						var vt = default(ValueTask);
						var scope = new HandleScope<T>(_core, head);
						try {
							vt = _handler.HandleAsync(ref scope, ct);
						}
						catch (Exception ex) {
							_handler.OnError(ex);
							vt = default; // nothing to await; the finally still releases the slot
						}
						finally {
							if (scope.IsReleased) {
								pending = 0; // early release already published past this item
							} else {
								scope.ReleaseDeferred(); // clear now, publish with the batch
								pending++;
							}
						}

						try {
							await vt.ConfigureAwait(false);
						}
						catch (Exception ex) {
							_handler.OnError(ex);
						}
					}
					while (!_core.IsStopped && ++head != available);

					if (pending > 0) _core.PublishHead(head);
				}

				if (!await _core.WaitForItemAsync().ConfigureAwait(false))
					break; // hard stop, or soft complete with the ring drained
			}
```

Also update the comment above it (lines ~101–104): replace the sentence `Head still advances per item via HandleScope.Release; …` with `Head publishes are batched (ReleaseDeferred + PublishHead per ShouldFlush); an early HandleScope.Release still publishes immediately. IsStopped is rechecked per item so a hard stop drops the rest of the run.`

- [ ] **Step 7: Document the new invariant in the worker-tree CLAUDE.md**

In `src/Nanov.HighPerformance.Threading.Workers/CLAUDE.md`, insert a new section at the end of the `## The one rule: SPSC SIGNALING INVARIANT (parked-flag protocol)` section — i.e. after the `- **Single producer is the caller's contract**` bullet, immediately before the `## Architecture (don't regress these)` heading:

```markdown
### Batched head publish (FIFO drain loops)

The four FIFO workers release slots in batches: the drain loop keeps a LOCAL head cursor and
publishes (`PublishHead`, absolute value) every `_flushThreshold` items
(`max(1, min(32, capacity/4))`), at every run boundary, and IMMEDIATELY when `ProducerParked` is
up (checked per item — warm line). Slots are still copied out / cleared per item (`TryTake` /
`ReleaseDeferred`). Rules that keep this safe — don't break them:

- **Flush at every run boundary, before every re-claim/park.** An empty ring must imply everything
  is published, or producer and consumer deadlock on stale state. `TryClaimRun` /
  `WaitForItemAsync` are only ever reached with the batch flushed.
- **`HandleScope.Release` publishes immediately** (it's the handler-facing "free this slot NOW"
  API); only the loops' automatic releases defer via the internal `ReleaseDeferred`. Worst-case
  producer wait on a full ring is one handler duration (pre-handler flag check), not one batch —
  the documented coarsening of the old per-item bound.
- **`PublishHead(n)` requires every slot below `n` to be cleared/copied out already.** The loops
  guarantee it by construction (clear-at-take / clear-at-deferral, in order).
- The keyed workers still use the per-item `TryTakeAndAdvance`/`AdvanceHead` path — don't move
  them to the batched API without revisiting their cell-state protocol.

Regression net: `BatchedReleaseTests` (+ `SignalingStressTests` for the park churn).
```

Then fix the two stale mentions in the same file:
1. In the `**Fast/slow split.**` bullet, replace `` (`TryReserve`/`TryClaimRun`/`CommitTail`/`AdvanceHead`/`TryEnqueue`/`TryTakeAndAdvance`) `` with `` (`TryReserve`/`TryClaimRun`/`CommitTail`/`AdvanceHead`/`TryEnqueue`/`TryTakeAndAdvance`/`TryTake`/`PublishHead`/`ShouldFlush`) ``.
2. In the `*Async*` bullet of **Sync vs async use two different core paths**, replace `drain `[head, available)` with `TryTakeAndAdvance` (copy) / `HandleScope` (value) per item, one `_idx.Tail` read per run, then re-claim` with `drain `[head, available)` with `TryTake` (copy) / `HandleScope`+`ReleaseDeferred` (value) per item under the batched head publish, one `_idx.Tail` read per run, then re-claim`.

- [ ] **Step 8: Build and run the full gate**

```bash
dotnet build -c Release
dotnet test -c Release
dotnet test -c Debug
```

Expected: 0 warnings; ALL tests green on both TFMs and configs — the existing suite (drain/drop contracts, GC slot-clear guard, blocking-producer tests), `SignalingStressTests`, and `BatchedReleaseTests`. A hang→`TimeoutException` in any watchdogged test means a missed flush point — re-check Steps 3–6 against the "flush before every re-claim/park" rule.

- [ ] **Step 9: A/B perf run**

```bash
dotnet run -c Release --project perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests -- --target SpWorker --report false --print-spec false 2>&1 | tee perf-local/L3-batch.txt
```

Compare against `perf-local/L2-flags.txt`. Note: in the saturated regime (producer permanently faster, ring always full) batching degenerates to per-item flush via the parked-flag trigger — same as before, no regression expected; the win shows in the streaming regime. Record what's observed.

- [ ] **Step 10: Record the layer in memory.md**

Prepend to `memory.md`:

```markdown
## 2026-07-10 — Lock-step squeeze L3: batched head publish in the FIFO drain loops

The four FIFO workers publish Head every `max(1, min(32, capacity/4))` items / at run boundaries /
immediately when `ProducerParked` is up, instead of per item (slots still cleared per item).
`HandleScope.Release` still publishes immediately; only auto-releases defer. Worst-case producer
wait coarsened from one slot-free to ONE HANDLER DURATION (accepted trade-off, documented).
macOS perf delta vs L2: <fill in>.
- Never remove the run-boundary flush (empty ring must imply all-published — deadlock otherwise).
- Keyed workers stay on per-item TryTakeAndAdvance.
```

Replace `<fill in>` with the Step 9 observation.

- [ ] **Step 11: Commit**

```bash
git add src/Nanov.HighPerformance.Threading.Workers/ memory.md
git commit -m "perf: batched head publish in the FIFO drain loops (L3)"
```

---

### Task 7: Final verification sweep and wrap-up

**Files:**
- Modify: `memory.md` (only if any layer's entry needs a correction after the final numbers)

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Full matrix**

```bash
dotnet build -c Release
dotnet test -c Release
dotnet test -c Debug
```

Expected: 0 warnings, everything green (both TFMs × both configs).

- [ ] **Step 2: BenchmarkDotNet cross-check**

```bash
dotnet run -c Release --project benchmarks/Nanov.HighPerformance.Benchmarks -- --filter '*SpWorker*' 2>&1 | tee perf-local/L3-bdn.txt
```

Expected: benchmarks complete; `[MemoryDiagnoser]` allocation columns for the worker loops unchanged vs their historical values (0 B/op on the sync paths; the async loop's one-time box only). Any new per-op allocation is a defect — investigate before finishing.

- [ ] **Step 3: Final interleaved A/B (settles the headline numbers)**

Run the baseline commit and the final commit back-to-back twice, interleaved, to defeat thermal drift:

```bash
git stash --include-untracked   # only if the tree is dirty; normally it is clean here
git checkout $(git log --format=%H --grep 'perf-test harness' -n 1)
dotnet run -c Release --project perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests -- --target SpWorker --include-latency false --report false --print-spec false 2>&1 | tee perf-local/final-A1.txt
git checkout feature/lockstep-throughput-squeeze
dotnet run -c Release --project perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests -- --target SpWorker --include-latency false --report false --print-spec false 2>&1 | tee perf-local/final-B1.txt
git checkout $(git log --format=%H --grep 'perf-test harness' -n 1)
dotnet run -c Release --project perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests -- --target SpWorker --include-latency false --report false --print-spec false 2>&1 | tee perf-local/final-A2.txt
git checkout feature/lockstep-throughput-squeeze
dotnet run -c Release --project perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests -- --target SpWorker --include-latency false --report false --print-spec false 2>&1 | tee perf-local/final-B2.txt
```

Compute per-test relative deltas (B mean vs A mean). If any layer's memory.md `<fill in>` placeholder text no longer matches the final picture, amend `memory.md` now and commit the correction:

```bash
git add memory.md
git commit -m "docs: final measured numbers for the lock-step squeeze layers"
```

- [ ] **Step 4: Report**

Summarize for the user: per-test throughput deltas (final interleaved numbers), test-suite status, what was kept/dropped per layer, and that the branch `feature/lockstep-throughput-squeeze` is ready for review/merge (do not merge or push without being asked).
