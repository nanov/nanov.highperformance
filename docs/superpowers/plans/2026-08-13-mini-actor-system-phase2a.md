# Mini Actor System — Phase 2a Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Take the working single-worker key-affine processor to **N workers**, correctly — closing the three recorded Phase 2 entry criteria first, then extracting the `IKeyScheduler` seam and building **scheduler B (routed)** behind it.

**Architecture:** The storage and ownership layers are unchanged and stay scheduler-agnostic. What changes: `Activate` must *validate* its claim rather than assume it; the processed count becomes a padded per-worker array summed at quiescence; a last-publisher broadcast wakes parked workers; each worker gets its own handler copy and local stack. Then scheduling moves behind a devirtualized struct seam, with a router thread feeding one SPSC ring per worker.

**Tech Stack:** C# / .NET, multi-targeting net9.0 + net10.0, NUnit 4, `Interlocked`/`Volatile` only — no locks, no `Channel`, no LINQ on hot paths.

**Spec:** `docs/superpowers/specs/2026-08-12-mini-actor-system-design.md`. Section references (§5.6, §6, §6.1) point into it.

**Phase 1 plan (for context on what exists):** `docs/superpowers/plans/2026-08-12-mini-actor-system-phase1.md`.

## Why this is Phase 2**a**

Adding the second worker forces the scheduler decision immediately. Today a single `KeyRing` carries producer→worker; with N workers it would have N consumers, breaking its strict SPSC contract. There is no "N workers on the current arrangement" intermediate step. So:

- **2a (this plan):** entry criteria → seam → scheduler B. Delivers a working multi-worker system.
- **2b:** scheduler D — Vyukov global submission queue, Chase-Lev per-worker deques, steal scan. Drops into the same seam.
- **3:** the B-vs-D benchmark, which is the experiment's actual question. **2a alone does not answer it.**

## Global Constraints

- **Never add `Version=` to a `.csproj`.** Package versions live in `Directory.Packages.props` as `<PackageVersion>`. Nothing here needs a new package.
- **Never set a version property in a project.** `VersionPrefix` is central in `Directory.Build.props`.
- **`TreatWarningsAsErrors=true`** and **`EnforceCodeStyleInBuild=true`** are inherited. **A warning fails the build.** In particular: CS0649 (field read but never assigned) and CS1574 (`<see cref>` at a type that does not exist yet) are build errors. Never fix either with `<NoWarn>` or an `.editorconfig` suppression — reword the comment, or add an explicit initializer.
- **Style, from `.editorconfig`:** tabs, `tab_width = 2`; file-scoped namespaces; **`using` directives INSIDE the namespace**; K&R braces; no braces for single-line statements; `var` everywhere; no `this.` qualifier; `_camelCase` private fields. `csharp_style_prefer_primary_constructors = false` — use explicit constructors.
- **Target frameworks:** `net9.0;net10.0`.
- **Tests are NUnit 4**, under `tests/Nanov.HighPerformance.Tests/Threading/Actors/`, and need an explicit `using NUnit.Framework;` ordered **after** the `Nanov.*` usings.
- **CS8175:** a `ref` local cannot be captured in a lambda — never inside `Assert.Multiple`. Copy fields to plain locals first.
- **Watch CS0128** (local name collisions) when appending to existing test methods.
- **No `Assert` inside a background-thread lambda.** Capture the exception, re-surface it on the test thread after a bounded `Join`. Order: **join → re-surface exception → assert progress.**
- **`[CancelAfter]` does NOT abort a blocking test.** It arms a `CancellationToken` the body must poll; our blocking paths do not. `[Timeout]` is `[Obsolete]` and fails the build. So the real protection for any test that can block is a bounded `Thread.Join(TimeSpan)` on a background producer, plus `WorkerTestUtil.AwaitCompletionOrStall`.
- **Explicit layout is illegal on a generic type.** Any padded struct must be a **non-generic top-level type** in `Internal/`. (Same reason `SpRingCore`'s `PaddedIndices` is non-generic.)
- **Verify with:** `dotnet build -c Release` (both TFMs, 0 warnings), `dotnet test -c Release`, and `dotnet test -c Debug --filter "FullyQualifiedName~Threading.Actors"`.

## Invariants that must survive every task

These were established and reviewed across 35 commits in Phase 1. Breaking one silently is the main risk of this phase.

- **§3.1:** a key index is in exactly one place at any instant — nowhere, one scheduler queue, or one worker. **Never two.**
- `DispatchCore`: `Interlocked.MemoryBarrier()` between the `Volatile.Write` of `cell.Tail` and the `Volatile.Read` of `cell.State`; **test-then-CAS**, not an unconditional CAS.
- `ReleaseAndMaybeSchedule`: releases `State` with `Interlocked.Exchange` (**not** `Volatile.Write` — it is there for the fence); order **publish-`Head` → `Exchange`-`State` → read-`Tail`**; worker-only work gated behind `fromWorker`.
- `Harvest`: bails on `_completed`; claims via `CAS(Free→Reclaiming)`; routes failed validation through `ReleaseAndMaybeSchedule(..., fromWorker: false)`.
- `KeyCellMap.EvictFree`: holds its `Reclaiming` claim **continuously** — never writes `State` back to 0. `Place` publishes `State = Free` **last**.
- `Activate`'s `chunk == Chunk.None` guard.
- `IsQuiescent` reads `_completed` **before** the counters. `TryComplete` publishes `_dispatchedFinal` **before** `_completed`, and wakes **unconditionally**.
- All park methods are arm-then-recheck: `Reset` → raise flag → fence → recheck → wait.
- A rejected dispatch must not increment `_dispatched`.
- Neither `ManualResetEventSlim` is ever disposed.

---

## File Structure

| File | Responsibility |
| --- | --- |
| `…/Internal/PaddedLong.cs` | **new** — non-generic 128-byte-padded `long`, for per-worker counters |
| `…/Internal/WorkerState.cs` | **new** — one worker's private state: id, handler copy, local stack, counters |
| `…/IKeyScheduler.cs` | **new** — the scheduling seam |
| `…/Schedulers/SingleWorkerScheduler.cs` | **new** — today's ring + local arrangement, behind the seam. The N=1 baseline |
| `…/Schedulers/RoutedScheduler.cs` | **new** — scheduler B: router thread + per-worker rings |
| `…/ActorSystem.cs` | modified throughout: validating claim, per-worker state, generic over `TScheduler` |
| `tests/…/Actors/MultiWorkerTests.cs` | **new** — N-worker correctness |
| `tests/…/Actors/RoutedSchedulerTests.cs` | **new** — B-specific behaviour |
| `tests/…/Actors/ActorSystemStressTests.cs` | extended with N-worker stress |

`ActorSystem.cs` is already ~550 lines and this phase adds to it. If it passes ~700, split the worker loop into `ActorSystem.Worker.cs` as a partial — do not restructure otherwise.

---

### Task 1: `Activate` must validate its claim

The single most important entry criterion. Today `Activate` does `Interlocked.Exchange(ref cell.State, CellState.Owned)` — a blind store that never checks it actually won the key. At one worker that is survivable; with two workers and two tokens for one index, **both** workers Exchange to `Owned` and both drain the same mailbox from the same `Head`: duplicate delivery, lost messages, per-key order destroyed. Exclusive ownership is the *only* thing providing order here.

Do this first, before any multi-worker machinery exists, so it can be reasoned about in isolation.

**Files:**
- Modify: `src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/ActorSystem.cs` (`Activate`)
- Test: `tests/Nanov.HighPerformance.Tests/Threading/Actors/ActorSystemTests.cs`

**Interfaces:**
- Consumes: `CellState.{Free,Scheduled,Owned,Reclaiming}`, `KeyCellMap.RefAt`
- Produces: `internal int RejectedClaims { get; }` — a test hook counting failed acquisitions

- [ ] **Step 1: Write the failing test**

Append inside the `ActorSystemTests` fixture:

```csharp
	[Test]
	public void Activate_Rejects_A_Token_It_Did_Not_Win() {
		var state = new RecordingState();
		using var system = New(state, chunkSize: 64);
		system.Start();

		system.Dispatch(1, 100);
		WorkerTestUtil.AwaitProgress(() => state.HandledCount, 1);
		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 1);

		// A legitimately scheduled key is always Scheduled when the worker picks it up — the producer's
		// dispatch CAS only fires Free -> Scheduled, and Harvest only claims Free -> Reclaiming. So a
		// correct run must never reject a claim. This is the mechanical guard that the validating CAS
		// is not spuriously failing; the duplicate-token case it protects against needs two workers.
		Assert.That(system.RejectedClaims, Is.EqualTo(0));
	}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `dotnet test -c Release --filter "FullyQualifiedName~Activate_Rejects_A_Token"`
Expected: compile error — `RejectedClaims` does not exist.

- [ ] **Step 3: Implement the validating claim**

In `ActorSystem.cs`, add the counter beside the other test-hook fields (near `_activations`):

```csharp
	private int _rejectedClaims; // test hook: acquisitions that lost the Scheduled -> Owned CAS
```

Replace the opening of `Activate` — currently:
```csharp
	private void Activate(int index) {
		_activations++;
		ref var cell = ref _map.RefAt(index);
		Interlocked.Exchange(ref cell.State, CellState.Owned);
```
with:
```csharp
	private void Activate(int index) {
		_activations++;
		ref var cell = ref _map.RefAt(index);

		// VALIDATE the claim — do not assume it. A blind Exchange to Owned would let two workers
		// holding two tokens for one index both "own" the key and drain the same mailbox from the same
		// Head: duplicate delivery, lost messages, and per-key order gone. Exclusive ownership is the
		// only thing providing order here (design 3.1), so acquisition must be a CAS whose failure
		// means "not ours".
		//
		// A legitimate token always finds Scheduled: the producer's dispatch CAS only fires
		// Free -> Scheduled, the worker's release CAS the same, and Harvest/EvictFree only claim
		// Free -> Reclaiming. Losing this CAS therefore means a duplicate token or a reclaimed cell —
		// in both cases there is nothing for us to do, and touching the cell would be the bug.
		if (Interlocked.CompareExchange(ref cell.State, CellState.Owned, CellState.Scheduled) !=
			CellState.Scheduled) {
			_rejectedClaims++;
			return;
		}
```

Add the test hook beside the others at the bottom of the class:

```csharp
	internal int RejectedClaims => Volatile.Read(ref _rejectedClaims);
```

- [ ] **Step 4: Run the tests**

Run: `dotnet test -c Release --filter "FullyQualifiedName~Threading.Actors"`
Expected: all pass on both TFMs, including the new test with `RejectedClaims == 0`.

- [ ] **Step 5: Run in Debug**

Run: `dotnet test -c Debug --filter "FullyQualifiedName~Threading.Actors"`
Expected: all pass.

- [ ] **Step 6: Confirm the guard is real, then revert the probe**

Temporarily change the CAS's comparand from `CellState.Scheduled` to `CellState.Free` (so a legitimate token always loses), run `Activate_Rejects_A_Token_It_Did_Not_Win`, and confirm it FAILS with a non-zero `RejectedClaims`. **Revert the probe** and confirm the suite is green again. Report the observed number.

- [ ] **Step 7: Commit**

```bash
git add src/experiments tests/Nanov.HighPerformance.Tests
git commit -m "fix(actors): Activate must validate its claim, not assume it"
```

---

### Task 2: Extract the `IKeyScheduler` seam

A pure refactor: move today's "one SPSC ring + one worker-private local stack" arrangement behind an interface, with `ActorSystem` generic over it. Behaviour must not change — the whole existing suite is the regression net.

**Files:**
- Create: `…/IKeyScheduler.cs`
- Create: `…/Schedulers/SingleWorkerScheduler.cs`
- Modify: `…/ActorSystem.cs`
- Modify: every test file that constructs an `ActorSystem` (the type gains a parameter)

**Interfaces:**
- Consumes: `KeyRing` (Phase 1)
- Produces:
  - `public interface IKeyScheduler` — `Start(int workerCount)`, `Schedule(int cellIdx)`, `ScheduleLocal(int workerId, int cellIdx)`, `TryAcquire(int workerId, out int cellIdx)`, `HasWork(int workerId)`, `WakeAll()`, `SignalStop()`, `ArmWait(int workerId)`, `Wait(int workerId)`
  - `public struct SingleWorkerScheduler : IKeyScheduler`
  - `ActorSystem<TKey, T, THandler, TScheduler>` — the type gains a fourth parameter

- [ ] **Step 1: Write the seam**

Create `…/IKeyScheduler.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Actors;

/// <summary>
///   The scheduling policy: which worker gets which key next. Implement as a <see langword="struct" />
///   so the calls devirtualize — the struct wraps a reference to its own shared state, the same shape
///   the test recorders use.
///   <para>
///     The scheduler carries <b>key indices</b>, never messages, and per-key ordering does NOT depend
///     on it: a key index is in exactly one place at any instant, arbitrated by the cell state machine.
///     A scheduler may therefore use any queue discipline it likes — LIFO, stealing from either end,
///     out-of-order claims — without affecting correctness.
///   </para>
/// </summary>
public interface IKeyScheduler {
	/// <summary>Allocate per-worker structures. Called once, before any worker starts.</summary>
	void Start(int workerCount);

	/// <summary>
	///   Schedule a newly-dirty key from the PRODUCER thread. Must not be called from a worker —
	///   <see cref="ScheduleLocal" /> exists for that, and mixing them breaks single-producer queues.
	/// </summary>
	void Schedule(int cellIdx);

	/// <summary>
	///   Schedule from a WORKER (a self-reschedule after a bounded batch). Routing this separately is
	///   what keeps a producer-owned queue strictly single-producer.
	/// </summary>
	void ScheduleLocal(int workerId, int cellIdx);

	/// <summary>Take the next key for this worker. Non-blocking; may steal.</summary>
	bool TryAcquire(int workerId, out int cellIdx);

	/// <summary>Whether this worker could acquire right now. Racy by design; used before parking.</summary>
	bool HasWork(int workerId);

	/// <summary>
	///   Wake EVERY worker unconditionally. Shutdown and quiescence only — never flag-gated. A system
	///   that is already drained and parked has no other way to be told.
	/// </summary>
	void WakeAll();

	/// <summary>Stop: release every parked worker so each can observe the exit condition.</summary>
	void SignalStop();

	/// <summary>
	///   Arm this worker's wait. Half of arm-then-recheck — the CALLER does the recheck between this
	///   and <see cref="Wait" />. Collapsing the two is the classic lost wakeup.
	/// </summary>
	void ArmWait(int workerId);

	/// <summary>Block this worker until signalled.</summary>
	void Wait(int workerId);
}
```

> **No `SignalWork`.** Waking after a schedule is the scheduler's own business and the two
> implementations do it in different places — `SingleWorkerScheduler` inside `Schedule`, `RoutedScheduler`
> from its router thread once it knows the target worker. A seam member nobody calls is dead surface.

> **`default(TScheduler)` is not usable.** Both implementations allocate their shared state in a
> parameterless constructor, so a defaulted struct has a null state field and throws on first use.
> That is fine — the scheduler is always passed in explicitly — but do not add a `new()` constraint
> and construct one internally.

- [ ] **Step 2: Implement the single-worker scheduler**

Create `…/Schedulers/SingleWorkerScheduler.cs`. This is exactly today's arrangement, lifted:

```csharp
namespace Nanov.HighPerformance.Threading.Actors.Schedulers;

using System;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors.Internal;

/// <summary>
///   The Phase 1 arrangement behind the seam: ONE producer-fed SPSC ring plus a worker-private local
///   stack, drained local-first for affinity.
///   <para>
///     <b>Single worker only.</b> The ring is strictly SPSC; a second consumer corrupts it silently.
///     <see cref="Start" /> enforces this rather than trusting the caller.
///   </para>
/// </summary>
public struct SingleWorkerScheduler : IKeyScheduler {
	private readonly State _state;

	public SingleWorkerScheduler() => _state = new State();

	private sealed class State {
		public readonly ManualResetEventSlim WorkAvailable = new(false);
		public KeyRing? Ring;
		public int[]? Local;
		public int LocalCount;
	}

	/// <summary>Ring capacity; must be at least the key map's slot count so it can never fill.</summary>
	public int Capacity { get; set; } = 1024;

	public void Start(int workerCount) {
		if (workerCount != 1)
			throw new ArgumentOutOfRangeException(nameof(workerCount),
				"SingleWorkerScheduler drives exactly one worker — its ring is strictly SPSC. " +
				"Use RoutedScheduler for more than one.");
		_state.Ring = new KeyRing(Capacity);
		_state.Local = new int[Capacity];
	}

	public void Schedule(int cellIdx) {
		if (!_state.Ring!.TryEnqueue(cellIdx))
			throw new InvalidOperationException(
				"scheduler ring full — it is sized to the map, so this indicates a key index was scheduled twice.");
		_state.WorkAvailable.Set();
	}

	public void ScheduleLocal(int workerId, int cellIdx) => _state.Local![_state.LocalCount++] = cellIdx;

	public bool TryAcquire(int workerId, out int cellIdx) {
		if (_state.LocalCount > 0) {
			cellIdx = _state.Local![--_state.LocalCount];
			return true;
		}

		return _state.Ring!.TryDequeue(out cellIdx);
	}

	public bool HasWork(int workerId) => _state.LocalCount > 0 || _state.Ring!.CountApprox > 0;

	public void WakeAll() => _state.WorkAvailable.Set();

	public void SignalStop() => _state.WorkAvailable.Set();

	public void ArmWait(int workerId) => _state.WorkAvailable.Reset();

	public void Wait(int workerId) => _state.WorkAvailable.Wait();
}
```

- [ ] **Step 3: Make `ActorSystem` generic over the scheduler**

In `ActorSystem.cs`:

1. Change the declaration to:
```csharp
public sealed class ActorSystem<TKey, T, THandler, TScheduler> : IDisposable
	where TKey : notnull, IEquatable<TKey>
	where THandler : struct, IActorHandler<TKey, T>
	where TScheduler : struct, IKeyScheduler {
```
2. Add `private TScheduler _scheduler;` beside `_handler` (mutable struct field — calls go through it so they devirtualize).
3. Add a `TScheduler scheduler` parameter to the constructor, assign it, and call `_scheduler.Start(1)` after the capacity guard. Delete the `_scheduled` and `_local`/`_localCount` fields and their initialization.
4. `ScheduleFromProducer(index)` becomes `_scheduler.Schedule(index)`. Delete the old method.
5. `PushLocal(index)` becomes `_scheduler.ScheduleLocal(0, index)`. Delete the old method.
6. `TryAcquire(out var index)` becomes `_scheduler.TryAcquire(0, out var index)`. Delete the old method.
7. `HasWork()` becomes `_scheduler.HasWork(0)`. Delete the old method.
8. `WakeAll()` becomes `_scheduler.WakeAll()`. Delete the old method.
9. In `WaitForWork`, replace `_workAvailable.Reset()` with `_scheduler.ArmWait(0)` and `_workAvailable.Wait()` with `_scheduler.Wait(0)`. **Keep the arm-then-recheck shape exactly**: arm → `Interlocked.MemoryBarrier()` → re-check work AND quiescence → wait → re-check both again.
10. Delete the `_workAvailable` field entirely — the scheduler owns it now. **Keep `_spaceAvailable`**: that is producer backpressure, not scheduling, and the scheduler has no business in it.
11. Call `_scheduler.SignalStop()` from **both** `TryComplete` and `Dispose`, right where `WakeAll()` is called today. Shutdown must release every parked worker unconditionally.
12. Delete the `ScheduledCountApprox` test hook and fix its uses in `ActorSystemTests` to assert on observable behaviour instead (the two call sites assert first-dirty gating; assert on `Activations` or on handled counts). A hook that reports one scheduler's internals cannot survive the seam, and a hook that lies is worse than no hook.

- [ ] **Step 4: Update every construction site**

The type now takes four parameters. In each test file's `New(...)` helper, change the return type to
`ActorSystem<int, int, RecordingHandler, SingleWorkerScheduler>` and pass `new SingleWorkerScheduler { Capacity = <mapSlotCount> }`.

**Capacity matters:** the ring must be at least the map's slot count or the "can never fill" argument breaks. The map rounds `keyCapacity` up to a power of two and then doubles it, so pass `keyCapacity * 4` to be safely above it. Add a comment saying why.

- [ ] **Step 5: Build and run the whole suite**

Run: `dotnet build -c Release`
Expected: 0 warnings, both TFMs.

Run: `dotnet test -c Release`
Expected: **identical results to before the refactor** — same test count, all passing. This is a behaviour-preserving change; any difference is a bug in the extraction.

Run: `dotnet test -c Debug --filter "FullyQualifiedName~Threading.Actors"`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/experiments tests/Nanov.HighPerformance.Tests
git commit -m "refactor(actors): extract the IKeyScheduler seam"
```

---

### Task 3: Per-worker state

Generalize the worker's private state to N copies. **N is still 1 after this task** — no multi-worker scheduler exists until Task 5 — but every structure it needs is in place and the existing suite still guards it.

**Files:**
- Create: `…/Internal/PaddedLong.cs`
- Create: `…/Internal/WorkerState.cs`
- Modify: `…/ActorSystem.cs`

**Interfaces:**
- Produces:
  - `internal struct PaddedLong` with `public long Value` — 128 bytes, non-generic
  - `internal sealed class WorkerState<THandler>` — `Id`, `Handler`, `ProcessedLocal`, `Activations`, `RejectedClaims`, `ProducerWakes`
  - `ActorSystem` gains a `workers` constructor parameter (default 1)

- [ ] **Step 1: The padded counter**

Create `…/Internal/PaddedLong.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Actors.Internal;

using System.Runtime.InteropServices;

/// <summary>
///   A <see langword="long" /> alone on its own 128-byte cache line, for per-worker counters that are
///   written by their owner and read by everyone at quiescence. Without the padding, N workers'
///   counters share lines and every publish invalidates its neighbours.
///   <para>
///     <b>Non-generic on purpose.</b> A generic type cannot carry
///     <see cref="StructLayoutAttribute" /> with <see cref="LayoutKind.Explicit" /> — the same reason
///     <c>SpRingCore</c>'s <c>PaddedIndices</c> is a standalone type.
///   </para>
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = 128)]
internal struct PaddedLong {
	[FieldOffset(0)] public long Value;
}
```

- [ ] **Step 2: The worker's private state**

Create `…/Internal/WorkerState.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Actors.Internal;

using System.Threading;

/// <summary>
///   One worker's private state. A separate heap object per worker, so the hot fields of different
///   workers do not share a cache line.
///   <para>
///     <c>Handler</c> is a per-worker COPY of the caller's handler struct. That is what makes handler
///     fields per-worker state rather than a data race: at one worker a single shared handler was
///     survivable, at N it is not. Calls go through the field (<c>worker.Handler.Handle(...)</c>) so
///     they still devirtualize.
///   </para>
/// </summary>
internal sealed class WorkerState<THandler>
	where THandler : struct {
	public readonly int Id;
	public THandler Handler;
	public long ProcessedLocal; // worker-private; published into the padded array
	public int Activations;
	public int RejectedClaims;
	public int ProducerWakes;
	public Thread? Thread;

	public WorkerState(int id, THandler handler) {
		Id = id;
		Handler = handler;
	}
}
```

- [ ] **Step 3: Wire it into `ActorSystem`**

In `ActorSystem.cs`:

1. Add a `workers` parameter. **The constructor signature must end up exactly this, in this order** — every test call site and both later tasks depend on it:
```csharp
	public ActorSystem(
		THandler handler,
		int keyCapacity,
		int chunkCount,
		int chunkSize,
		int batchBound,
		int workers,
		TScheduler scheduler)
```
   Validate `workers >= 1`. Note this drops `batchBound`'s default — with two parameters after it, a default there only invites positional mistakes. Update every existing call site to pass `batchBound: 0` explicitly where it previously relied on the default.
2. Replace `_processedLocal`, `_activations`, `_rejectedClaims`, `_producerWakes` and `_worker` with:
```csharp
	private readonly WorkerState<THandler>[] _workers;
	private readonly PaddedLong[] _processedPublished; // one padded slot per worker
```
   Build both in the constructor: `new WorkerState<THandler>(i, handler)` for each `i`, and `new PaddedLong[workers]`.
3. **Keep `_handler` as well** — the PRODUCER's copy. It is the one that services `OnSlotReused` and `OnChunkReclaimed`, which run on the producer thread. Comment that there are `workers + 1` handler copies in total: one per worker, plus the producer's.
4. `PublishProcessed()` becomes `PublishProcessed(WorkerState<THandler> w)`:
```csharp
	private void PublishProcessed(WorkerState<THandler> w) =>
		Volatile.Write(ref _processedPublished[w.Id].Value, w.ProcessedLocal);
```
5. `IsQuiescent()` sums:
```csharp
	private bool IsQuiescent() {
		if (!_completed) return false; // flag FIRST — see the doc comment; do not reorder
		var processed = 0L;
		for (var i = 0; i < _processedPublished.Length; i++)
			processed += Volatile.Read(ref _processedPublished[i].Value);
		return processed >= Volatile.Read(ref _dispatchedFinal);
	}
```
   **Keep the `_completed`-before-counters order.** Reversing it serves a stale sum beside a fresh flag: false quiescence and dropped messages.
6. Thread every worker-touching method through a `WorkerState<THandler> w` parameter: `RunLoop(object? state)` casts it, then `Activate(w, index)`, `ReleaseAndMaybeSchedule(w, index, ref cell, head, fromWorker)`, `WaitForWork(w)`. Inside, `_handler.Handle(...)` becomes `w.Handler.Handle(...)`, `_activations++` becomes `w.Activations++`, and so on. The producer's cold path passes `null` for `w` — so give `ReleaseAndMaybeSchedule` a `WorkerState<THandler>? w` and guard the worker-only block on `fromWorker` exactly as today.
7. `Start()` creates and starts one thread per worker:
```csharp
	public void Start() {
		for (var i = 0; i < _workers.Length; i++) {
			var w = _workers[i];
			w.Thread = new Thread(RunLoop) {
				IsBackground = true,
				Name = $"Actor<{typeof(TKey).Name}>#{w.Id}",
			};
			w.Thread.Start(w);
		}
	}
```
   `RunLoop` becomes `private void RunLoop(object? state)` and starts with `var w = (WorkerState<THandler>)state!;`.
8. **`RunLoop`'s `finally` must only resolve `Completion` once all workers have exited.** Today one worker's exit resolves it. Change: keep an `_liveWorkers` count initialised to `workers`, decremented with `Interlocked.Decrement` in the `finally`; only the worker that takes it to zero runs the completion block (`OnCompleted` still fires **per worker**, before that — that is the documented contract). Concretely, the `finally` becomes:
```csharp
		finally {
			PublishProcessed(w);
			var drainedCleanly = _completed && QuiescentSum() >= Volatile.Read(ref _dispatchedFinal);
			var hardStopped = _stopped && !drainedCleanly;
			Exception? error = hardStopped ? new OperationCanceledException() : _completionError;
			try {
				w.Handler.OnCompleted(error); // once per worker — the documented contract
			}
			catch (Exception ex) {
				SafeOnError(w, ex);
			}

			// Only the LAST worker out resolves Completion; otherwise the first to exit would report
			// the system finished while its peers are still draining.
			if (Interlocked.Decrement(ref _liveWorkers) == 0) {
				if (hardStopped) _completion.TrySetCanceled();
				else if (error is not null) _completion.TrySetException(error);
				else _completion.TrySetResult();
			}
		}
```
   Extract `QuiescentSum()` as a small private helper returning the sum, used by both `IsQuiescent` and here, so the two cannot drift.
9. `Dispose` joins every worker against **one shared budget**:
```csharp
		var deadline = Stopwatch.StartNew();
		var budget = TimeSpan.FromSeconds(1);
		foreach (var w in _workers) {
			var remaining = budget - deadline.Elapsed;
			if (remaining > TimeSpan.Zero) w.Thread?.Join(remaining);
		}
```
   Add `using System.Diagnostics;` inside the namespace. The `_worker is null` checks in `TryComplete`/`Dispose` become "no worker thread was ever started", i.e. `_workers[0].Thread is null`.
10. Update the test hooks to aggregate: `Activations`, `RejectedClaims`, `ProducerWakes` sum across `_workers`.

- [ ] **Step 4: Build and run the whole suite**

Run: `dotnet build -c Release` → 0 warnings both TFMs.
Run: `dotnet test -c Release` → **identical to before**; still one worker, so nothing observable should change.
Run: `dotnet test -c Debug --filter "FullyQualifiedName~Threading.Actors"` → all pass.

- [ ] **Step 5: Commit**

```bash
git add src/experiments
git commit -m "refactor(actors): per-worker state, padded counters, N-worker lifecycle"
```

---

### Task 4: The last-publisher broadcast

Spec §5.6's second wake edge, which Phase 1 deliberately omitted because at one worker the sole predicate evaluator is the worker itself. At N > 1 its absence is **the** most likely hang: worker A publishes the count that completes the sum while B…N sit parked, and nobody tells them.

**Files:**
- Modify: `…/ActorSystem.cs`
- Test: `tests/…/Actors/MultiWorkerTests.cs` (created here, exercised properly in Task 6)

**Interfaces:**
- Consumes: `PublishProcessed`, `QuiescentSum`, `IKeyScheduler.WakeAll`

- [ ] **Step 1: Implement the broadcast**

In `ActorSystem.cs`, change `PublishProcessed` so the publisher checks whether it just completed the sum, and if so wakes everyone:

```csharp
	/// <summary>
	///   Publish this worker's processed count, and — if that publish just satisfied quiescence —
	///   broadcast so parked peers can exit.
	///   <para>
	///     <b>This second wake edge is required at N &gt; 1</b> (§5.6). Without it, the worker whose
	///     publish completes the sum simply carries on to its own idle check and exits, while every
	///     other worker stays parked forever with nothing left to wake them: no further message will be
	///     processed, so no further publish will ever occur. At N = 1 it is unreachable — the sole
	///     publisher is also the sole predicate evaluator — which is why Phase 1 omitted it.
	///   </para>
	///   Order: publish → observe the sum → read <c>_completed</c> → wake. Reading the flag before the
	///   sum would let a stale flag suppress the broadcast.
	/// </summary>
	private void PublishProcessed(WorkerState<THandler> w) {
		Volatile.Write(ref _processedPublished[w.Id].Value, w.ProcessedLocal);
		if (QuiescentSum() >= Volatile.Read(ref _dispatchedFinal) && _completed)
			_scheduler.WakeAll();
	}
```

- [ ] **Step 2: Guard against a wake storm**

`PublishProcessed` runs every 32 messages inside the drain loop, so the check above is on a warm path. It is two `Volatile.Read`s per worker plus a flag read — acceptable — but the `WakeAll` must not fire repeatedly. Add an idempotence gate beside `_completeGate`:

```csharp
	private int _quiescenceBroadcast;
```
and make the wake:
```csharp
		if (QuiescentSum() >= Volatile.Read(ref _dispatchedFinal) && _completed &&
			Interlocked.Exchange(ref _quiescenceBroadcast, 1) == 0)
			_scheduler.WakeAll();
```
One broadcast total. A worker that parks after it still re-checks quiescence inside its arm-then-recheck, so it cannot miss the exit.

- [ ] **Step 3: Create the multi-worker test file**

Create `tests/…/Actors/MultiWorkerTests.cs` with the fixture skeleton and one test that runs at N=1 for now (Task 6 raises N once a multi-worker scheduler exists):

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Actors;

using Nanov.HighPerformance.Tests.Threading.Workers;
using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using NUnit.Framework;

[TestFixture]
public class MultiWorkerTests {
	[Test]
	public void Quiescence_Broadcast_Fires_At_Most_Once() {
		var state = new RecordingState();
		using var system = new ActorSystem<int, int, RecordingHandler, SingleWorkerScheduler>(
			new RecordingHandler(state), keyCapacity: 64, chunkCount: 16, chunkSize: 64,
			batchBound: 0, workers: 1, scheduler: new SingleWorkerScheduler { Capacity = 256 });
		system.Start();

		for (var i = 0; i < 500; i++)
			system.Dispatch(i % 8, i);

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 500);

		Assert.Multiple(() => {
			Assert.That(state.HandledCount, Is.EqualTo(500));
			Assert.That(system.QuiescenceBroadcasts, Is.LessThanOrEqualTo(1),
				"the quiescence broadcast must be idempotent — it is on the per-32-message publish path");
		});
	}
}
```

Add the hook to `ActorSystem`: `internal int QuiescenceBroadcasts => Volatile.Read(ref _quiescenceBroadcast);`

Match the constructor's actual parameter order to whatever Task 3 produced; if it differs from the named arguments above, use the real one.

- [ ] **Step 4: Build and run**

Run: `dotnet build -c Release` → 0 warnings.
Run: `dotnet test -c Release --filter "FullyQualifiedName~Threading.Actors"` → all pass, both TFMs.
Run: `dotnet test -c Debug --filter "FullyQualifiedName~Threading.Actors"` → all pass.

- [ ] **Step 5: Commit**

```bash
git add src/experiments tests/Nanov.HighPerformance.Tests
git commit -m "feat(actors): last-publisher quiescence broadcast"
```

---

### Task 5: Scheduler B — the routed scheduler

The first genuinely multi-worker scheduler. A router thread drains the producer's ring and pushes each key index into the least-loaded worker's own SPSC ring. Every edge stays single-producer/single-consumer.

**Files:**
- Create: `…/Schedulers/RoutedScheduler.cs`
- Test: `tests/…/Actors/RoutedSchedulerTests.cs`

**Interfaces:**
- Consumes: `KeyRing`, `IKeyScheduler`
- Produces: `public struct RoutedScheduler : IKeyScheduler` with `int Capacity { get; set; }` and `RoutingPolicy Policy { get; set; }`; `public enum RoutingPolicy { LeastLoaded, RoundRobin }`

- [ ] **Step 1: Write the failing test**

Create `tests/…/Actors/RoutedSchedulerTests.cs`:

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Actors;

using System.Linq;
using Nanov.HighPerformance.Tests.Threading.Workers;
using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using NUnit.Framework;

[TestFixture]
public class RoutedSchedulerTests {
	private static ActorSystem<int, int, RecordingHandler, RoutedScheduler> New(
		RecordingState state, int workers, RoutingPolicy policy = RoutingPolicy.LeastLoaded,
		int keyCapacity = 128, int chunkCount = 32, int chunkSize = 64, int batchBound = 0) =>
		new(new RecordingHandler(state), keyCapacity, chunkCount, chunkSize, batchBound, workers,
			new RoutedScheduler { Capacity = keyCapacity * 4, Policy = policy });

	[Test]
	public void Four_Workers_Preserve_Order_Within_Every_Key() {
		const int keys = 32;
		const int perKey = 500;
		var state = new RecordingState();
		using var system = New(state, workers: 4);
		system.Start();

		for (var i = 0; i < perKey; i++)
			for (var k = 0; k < keys; k++)
				Assert.That(system.Dispatch(k, i), Is.True);

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, keys * perKey);

		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");
	}

	[Test]
	public void Four_Workers_All_Do_Work() {
		var state = new RecordingState();
		using var system = New(state, workers: 4);
		system.Start();

		for (var i = 0; i < 4_000; i++)
			system.Dispatch(i % 64, i);

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 4_000);

		Assert.That(system.WorkersThatActivated, Is.GreaterThan(1),
			"only one worker ever activated — routing is not distributing");
	}

	[Test]
	public void Completion_Resolves_With_Every_Worker_Parked() {
		var state = new RecordingState();
		using var system = New(state, workers: 4);
		system.Start();
		system.Dispatch(1, 1);
		WorkerTestUtil.AwaitProgress(() => state.HandledCount, 1);
		Thread.Sleep(150); // let every worker genuinely park

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 1);
		Assert.That(Volatile.Read(ref state.CompletedCalls), Is.EqualTo(4),
			"OnCompleted fires once per worker");
	}
}
```

`WorkersThatActivated` is a new test hook — add to `ActorSystem`:
```csharp
	internal int WorkersThatActivated {
		get {
			var n = 0;
			foreach (var w in _workers)
				if (Volatile.Read(ref w.Activations) > 0) n++;
			return n;
		}
	}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `dotnet test -c Release --filter "FullyQualifiedName~RoutedSchedulerTests"`
Expected: compile error — `RoutedScheduler` does not exist.

- [ ] **Step 3: Implement the routed scheduler**

Create `…/Schedulers/RoutedScheduler.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Actors.Schedulers;

using System;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors.Internal;

/// <summary>How the router picks a worker for a newly-dirty key.</summary>
public enum RoutingPolicy {
	/// <summary>Fewest queued keys wins. Free — <c>KeyRing.CountApprox</c> is already exposed.</summary>
	LeastLoaded,

	/// <summary>
	///   The metric-free null baseline. If this matches <see cref="LeastLoaded" /> in the benchmark,
	///   the router's cleverness is not earning its keep — which is a real result, not a failure.
	/// </summary>
	RoundRobin,
}

/// <summary>
///   Scheduler B — push-side routing. The producer enqueues newly-dirty keys into one SPSC ring; a
///   dedicated router thread drains it and pushes each index into one worker's own SPSC ring.
///   <para>
///     <b>Every edge is single-producer / single-consumer.</b> producer → router is one ring with one
///     writer and one reader; router → worker <c>i</c> likewise. A worker's SELF-reschedule must never
///     enter its ring — that would make the router a second producer — so it goes on that worker's
///     private local stack instead (<see cref="ScheduleLocal" />).
///   </para>
///   <para>
///     Local-first draining gives affinity for free: a key that keeps receiving messages stays on the
///     worker that already has its cell lines warm, which is why B needs no key→worker map at all.
///     But unbounded local-first STARVES the ring — a continuously-refilled key is popped from local
///     forever while a ring-queued key never runs. <see cref="LocalDrainBound" /> caps consecutive
///     local acquisitions; after that the ring is checked first. This is a real B-vs-D asymmetry: under
///     B nothing can move the starved key elsewhere, whereas D's thieves can steal it.
///   </para>
/// </summary>
public struct RoutedScheduler : IKeyScheduler {
	private readonly State _state;

	public RoutedScheduler() => _state = new State();

	private sealed class State {
		public KeyRing? Inbox;              // producer → router
		public KeyRing[]? WorkerRings;      // router → worker i
		public int[][]? Local;              // worker i's private stack
		public int[]? LocalCount;
		public int[]? LocalRun;             // consecutive local acquisitions, for the starvation bound
		public ManualResetEventSlim[]? WorkAvailable;
		public Thread? Router;
		public volatile bool Stopping;
		public int NextRoundRobin;
	}

	/// <summary>Ring capacity; must be at least the key map's slot count so a ring can never fill.</summary>
	public int Capacity { get; set; } = 1024;

	public RoutingPolicy Policy { get; set; } = RoutingPolicy.LeastLoaded;

	/// <summary>
	///   Maximum consecutive acquisitions from a worker's local stack before its ring gets priority.
	///   Bounds the starvation described on the type. 16 is a guess pending measurement, not a result.
	/// </summary>
	public int LocalDrainBound { get; set; } = 16;

	public void Start(int workerCount) {
		if (workerCount < 1) throw new ArgumentOutOfRangeException(nameof(workerCount));
		_state.Inbox = new KeyRing(Capacity);
		_state.WorkerRings = new KeyRing[workerCount];
		_state.Local = new int[workerCount][];
		_state.LocalCount = new int[workerCount];
		_state.LocalRun = new int[workerCount];
		_state.WorkAvailable = new ManualResetEventSlim[workerCount];
		for (var i = 0; i < workerCount; i++) {
			_state.WorkerRings[i] = new KeyRing(Capacity);
			_state.Local[i] = new int[Capacity];
			_state.WorkAvailable[i] = new ManualResetEventSlim(false);
		}

		var s = _state;
		var policy = Policy;
		s.Router = new Thread(() => RouteLoop(s, policy)) { IsBackground = true, Name = "ActorRouter" };
		s.Router.Start();
	}

	private static void RouteLoop(State s, RoutingPolicy policy) {
		while (!s.Stopping) {
			if (!s.Inbox!.TryDequeue(out var cellIdx)) {
				Thread.SpinWait(64);
				continue;
			}

			var target = policy == RoutingPolicy.RoundRobin
				? s.NextRoundRobin++ % s.WorkerRings!.Length
				: LeastLoadedIndex(s);

			if (!s.WorkerRings![target].TryEnqueue(cellIdx))
				throw new InvalidOperationException(
					"worker ring full — rings are sized to the map, so this means a key index was scheduled twice.");
			s.WorkAvailable![target].Set();
		}
	}

	private static int LeastLoadedIndex(State s) {
		// Queued KEY count, not queued messages. Message depth is the truer metric but is stale the
		// instant it is read and would cost cross-core reads per decision; key count is already exposed.
		var best = 0;
		var bestDepth = s.WorkerRings![0].CountApprox;
		for (var i = 1; i < s.WorkerRings.Length; i++) {
			var depth = s.WorkerRings[i].CountApprox;
			if (depth >= bestDepth) continue;
			best = i;
			bestDepth = depth;
		}

		return best;
	}

	public void Schedule(int cellIdx) {
		if (!_state.Inbox!.TryEnqueue(cellIdx))
			throw new InvalidOperationException(
				"router inbox full — it is sized to the map, so this means a key index was scheduled twice.");
	}

	public void ScheduleLocal(int workerId, int cellIdx) =>
		_state.Local![workerId][_state.LocalCount![workerId]++] = cellIdx;

	public bool TryAcquire(int workerId, out int cellIdx) {
		var localCount = _state.LocalCount![workerId];

		// Starvation bound: after LocalDrainBound consecutive local pops, look at the ring first so a
		// ring-queued key cannot be starved by a continuously-refilled local one.
		if (localCount > 0 && _state.LocalRun![workerId] < LocalDrainBound) {
			cellIdx = _state.Local![workerId][--_state.LocalCount[workerId]];
			_state.LocalRun[workerId]++;
			return true;
		}

		_state.LocalRun![workerId] = 0;
		if (_state.WorkerRings![workerId].TryDequeue(out cellIdx)) return true;

		if (localCount > 0) {
			cellIdx = _state.Local![workerId][--_state.LocalCount[workerId]];
			return true;
		}

		return false;
	}

	public bool HasWork(int workerId) =>
		_state.LocalCount![workerId] > 0 || _state.WorkerRings![workerId].CountApprox > 0;

	public void WakeAll() {
		foreach (var evt in _state.WorkAvailable!)
			evt.Set();
	}

	public void SignalStop() {
		_state.Stopping = true;
		WakeAll();
	}

	public void ArmWait(int workerId) => _state.WorkAvailable![workerId].Reset();

	public void Wait(int workerId) => _state.WorkAvailable![workerId].Wait();
}
```

- [ ] **Step 4: Run the routed tests**

Run: `dotnet test -c Release --filter "FullyQualifiedName~RoutedSchedulerTests"`
Expected: all 3 pass on both TFMs.

**If `Four_Workers_Preserve_Order_Within_Every_Key` fails, STOP.** An ordering failure at N=4 means the ownership layer is not actually exclusive — that is the entry criterion Task 1 addressed, and a failure here means it is incomplete. Do not adjust the test; report it.

- [ ] **Step 5: Run everything**

Run: `dotnet test -c Release` → whole repo, both TFMs.
Run: `dotnet test -c Debug --filter "FullyQualifiedName~Threading.Actors"` → both TFMs.
Run the `Threading.Actors` filter **3 times** in Release and report each.

- [ ] **Step 6: Commit**

```bash
git add src/experiments tests/Nanov.HighPerformance.Tests
git commit -m "feat(actors): scheduler B — routed, least-loaded and round-robin"
```

---

### Task 6: Multi-worker stress and race guards

The tests that will actually find the N-worker bugs. Everything before this ran at N=1 or on short workloads.

**Files:**
- Modify: `tests/…/Actors/MultiWorkerTests.cs`
- Modify: `tests/…/Actors/ActorSystemStressTests.cs`

- [ ] **Step 1: Add the multi-worker guards**

Append to `MultiWorkerTests.cs`. Every test that dispatches in bulk must use the file-established safe pattern: background producer thread, captured exception, bounded `Join`, then **join → re-surface exception → assert progress**.

```csharp
	[Test]
	[CancelAfter(120_000)]
	public void Eight_Workers_Sustained_Load_Preserves_Order_And_Loses_Nothing() {
		const int keys = 64;
		const int perKey = 2_000;
		const int total = keys * perKey;
		var state = new RecordingState();
		using var system = new ActorSystem<int, int, RecordingHandler, RoutedScheduler>(
			new RecordingHandler(state), keyCapacity: 256, chunkCount: 64, chunkSize: 16,
			batchBound: 4, workers: 8, scheduler: new RoutedScheduler { Capacity = 1024 });
		system.Start();

		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				for (var i = 0; i < perKey; i++)
					for (var k = 0; k < keys; k++) {
						if (!system.Dispatch(k, i)) return;
						Volatile.Write(ref dispatched, Volatile.Read(ref dispatched) + 1);
					}
			}
			catch (Exception ex) {
				producerError = ex;
			}
		}) { IsBackground = true };
		producer.Start();

		Assert.That(producer.Join(TimeSpan.FromSeconds(90)), Is.True,
			$"producer wedged after {Volatile.Read(ref dispatched)}/{total} dispatches");
		if (producerError is not null) Assert.Fail($"producer threw: {producerError}");
		Assert.That(Volatile.Read(ref dispatched), Is.EqualTo(total));

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, total);

		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");
	}

	[Test]
	[CancelAfter(60_000)]
	public void No_Worker_Ever_Rejects_A_Claim_Under_Multi_Worker_Churn() {
		const int keys = 16;
		const int perKey = 3_000;
		var state = new RecordingState();
		using var system = new ActorSystem<int, int, RecordingHandler, RoutedScheduler>(
			new RecordingHandler(state), keyCapacity: 128, chunkCount: 32, chunkSize: 8,
			batchBound: 1, workers: 8, scheduler: new RoutedScheduler { Capacity = 512 });
		system.Start();

		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				for (var i = 0; i < perKey; i++)
					for (var k = 0; k < keys; k++) {
						if (!system.Dispatch(k, i)) return;
						Volatile.Write(ref dispatched, Volatile.Read(ref dispatched) + 1);
					}
			}
			catch (Exception ex) {
				producerError = ex;
			}
		}) { IsBackground = true };
		producer.Start();

		Assert.That(producer.Join(TimeSpan.FromSeconds(45)), Is.True, "producer wedged");
		if (producerError is not null) Assert.Fail($"producer threw: {producerError}");

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, keys * perKey);

		// A rejected claim means two tokens existed for one index — the "never two" invariant broke.
		// batchBound 1 maximises release churn, which is when a duplicate would appear.
		Assert.That(system.RejectedClaims, Is.EqualTo(0),
			"a worker lost the Scheduled -> Owned CAS: two tokens existed for one key index");
	}

	[Test]
	[CancelAfter(60_000)]
	public void Every_Worker_Parks_And_Still_Completes() {
		var state = new RecordingState();
		using var system = new ActorSystem<int, int, RecordingHandler, RoutedScheduler>(
			new RecordingHandler(state), keyCapacity: 64, chunkCount: 16, chunkSize: 64,
			batchBound: 0, workers: 8, scheduler: new RoutedScheduler { Capacity = 256 });
		system.Start();

		system.Dispatch(1, 1);
		WorkerTestUtil.AwaitProgress(() => state.HandledCount, 1);
		Thread.Sleep(200); // every worker parks

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 1);
		Assert.That(Volatile.Read(ref state.CompletedCalls), Is.EqualTo(8));
	}
```

Add `using System;` and `using System.Linq;` inside the namespace if not already present, and `using Nanov.HighPerformance.Threading.Actors.Schedulers;`.

- [ ] **Step 2: Run them**

Run: `dotnet test -c Release --filter "FullyQualifiedName~MultiWorkerTests"`
Expected: all pass, both TFMs.

**`Every_Worker_Parks_And_Still_Completes` is the direct guard for Task 4's broadcast.** If it hangs, the last-publisher wake edge is missing or wrong — that is the finding, do not work around it.

- [ ] **Step 3: Prove the starvation bound is load-bearing**

Temporarily set `LocalDrainBound` to `int.MaxValue` (unbounded local draining) and run `Eight_Workers_Sustained_Load_Preserves_Order_And_Loses_Nothing`. Record whether it slows dramatically or times out. **Revert.** Report what you observed — if unbounded draining makes no difference at these sizes, say so plainly rather than claiming the bound is proven; it would mean the test does not reproduce the starvation and a targeted one is owed.

- [ ] **Step 4: Five consecutive clean runs**

```bash
for i in 1 2 3 4 5; do
  dotnet test -c Release --filter "FullyQualifiedName~Threading.Actors" || { echo "FAILED on run $i"; break; }
done
```
Expected: five clean runs. Report each.

- [ ] **Step 5: Whole repo, both configs**

Run: `dotnet test -c Release` → the existing worker-family suite must be unaffected.
Run: `dotnet test -c Debug --filter "FullyQualifiedName~Threading.Actors"`.

- [ ] **Step 6: Commit**

```bash
git add tests/Nanov.HighPerformance.Tests
git commit -m "test(actors): multi-worker order, claim and quiescence guards"
```

---

### Task 7: Update the spec and close the entry criteria

Documentation only, but it is what stops the next reader from re-deriving all of this.

**Files:**
- Modify: `docs/superpowers/specs/2026-08-12-mini-actor-system-design.md`

- [ ] **Step 1: Mark the entry criteria closed**

Three places in the spec record Phase 2 blockers. Update each to say it is now implemented, and where:
1. **§5.5's residual-race blockquote** — the `Activate` claim-validation item is now done (Task 1). Keep the generation-tag note if it is still open; say plainly which half closed.
2. **§5.6's "Phase 1 does not implement this edge" blockquote** — the last-publisher broadcast now exists (Task 4). Note the idempotence gate.
3. **§6.1's starvation blockquote** — bounded local draining now exists (`RoutedScheduler.LocalDrainBound`, Task 5). Record the default (16) and that it is **a guess pending measurement, not a result**, and that the B-vs-D benchmark must include the hot-key-plus-starved-key workload.

- [ ] **Step 2: Record what Phase 2a did NOT deliver**

Add to §11's limitations table: scheduler D and the head-to-head benchmark are still outstanding, so **the experiment's question is still unanswered**. Phase 2a delivers the machinery, not the result.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs
git commit -m "docs: close the Phase 2 entry criteria in the design spec"
```

---

## Phase 2a done — what comes next

A working N-worker key-affine processor with one scheduler behind a seam.

**Phase 2b — scheduler D.** Global submission queue (Vyukov per-slot sequence stamps, producer-single/worker-many), per-worker Chase-Lev deques with local overflow to the global queue, random-victim steal scan, and sleeping-worker tracking so `Schedule` wakes exactly one. Use **Lê / Pop / Cohen / Zappa Nardelli (PPoPP 2013)** for the deque memory orderings, **not** the 2005 Chase-Lev paper — this repo targets Apple Silicon and the 2005 orderings are wrong on weak memory. Steal granularity is **one key** (a key already is a batch of up to `batchBound` messages).

**Phase 3 — the benchmark.** Baselines: N statically hash-sharded `SpWorker`s, and a `Channel<T>` + `ConcurrentDictionary` equivalent. Axes: worker count; key cardinality (independently of skew — the gap is memory-access-shaped); skew from uniform through Zipf to a single hot key; per-message handler cost. Report throughput, p99, and **steal rate** — the number that says which regime you are in. Include the hot-key-plus-starved-key workload that §6.1 now requires.

## Known debt carried into Phase 2a

- The worker's side of the parked-producer handshake has **no StoreLoad fence**, so a wake can be missed entirely and the bounded 20 ms poll is what covers it — a tail-latency cliff on the backpressure path. Documented in `WaitForMailboxSpace` and spec §5.4. Fixing it means a short spin before parking (matching `SpRingCore.WaitForSpaceSlow`) or a fence before the `_producerParked` read; deferred pending measurement. **With N workers this gets worse, not better** — more workers means more publish sites racing one producer flag. Measure it in Phase 3.
- `BindChunk` zeroes `Head`/`Tail`/`CachedHead` on a cell a worker may still be mid-release on. Safety rests on the pair being observed consistently — true in practice, not guaranteed by the memory model. The hardening (stop zeroing; the cursors are absolute and `Head == Tail` already holds at every rebind) is cheap and unclaimed.
