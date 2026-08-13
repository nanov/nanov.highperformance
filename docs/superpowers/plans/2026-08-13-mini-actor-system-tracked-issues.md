# Mini Actor System — Tracked Issues Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the two issues Phase 2a's final review tracked as "should land before Phase 2b" — background-thread faults that hang the system instead of faulting it, and two cold paths whose entire correctness argument is untested above one worker.

**Architecture:** A fault on any background thread (worker or router) must do three things: record the exception, stop the system, and release everyone parked. Today the worker path does only the first and the router path cannot reach `ActorSystem` at all, because `IKeyScheduler` has no fault channel. This adds one — `Start` takes a fault callback — and makes the completion verdict prefer a recorded fault over a cancellation.

**Tech Stack:** C# / .NET, net9.0 + net10.0, NUnit 4, `Interlocked`/`Volatile` only.

**Spec:** `docs/superpowers/specs/2026-08-12-mini-actor-system-design.md`. Phase 2a's ledger: `.superpowers/sdd/2026-08-13-mini-actor-system-phase2a/progress.md`.

## Why these two, now

Phase 2b adds a work-stealing scheduler. Both issues get harder there, not easier:

- **Faults**: a stealing scheduler has more background threads and more places a fault can originate. Building it on a system where a background-thread death silently hangs means debugging D's bugs through that fog.
- **Cold paths**: `Harvest()` is reachable from exactly one N>1 test and `KeyCellMap.EvictFree()` from none. Both paths' correctness argument is entirely about racing a worker that is mid-release between its `Exchange(State, Free)` and its reschedule CAS — and at one worker that race is one thread on one core. Stealing makes those races strictly more frequent.

## Global Constraints

- **Never add `Version=` to a `.csproj`** — package versions are central in `Directory.Packages.props`.
- **`TreatWarningsAsErrors=true`** and **`EnforceCodeStyleInBuild=true`** — a warning fails the build. CS0649 (field read, never assigned) and CS1574 (`<see cref>` at a nonexistent type) are build errors; never suppress either.
- **Style:** tabs, `tab_width = 2`; file-scoped namespaces; `using` directives INSIDE the namespace; K&R braces; no braces for single-line statements; `var`; no `this.`; `_camelCase` private fields; explicit constructors.
- **Tests** are NUnit 4 under `tests/Nanov.HighPerformance.Tests/Threading/Actors/`, needing `using NUnit.Framework;` ordered after the `Nanov.*` usings.
- **CS8175**: a `ref` local cannot be captured in a lambda — never inside `Assert.Multiple`.
- **No `Assert` inside a background-thread lambda.** Capture the exception, re-surface on the test thread after a bounded `Join`. Order: **join → re-surface exception → assert progress.**
- **`[CancelAfter]` does not abort a blocking test** — it arms a token nothing here polls. The bounded `Thread.Join(TimeSpan)` is the real protection.
- **Verify with** `dotnet build -c Release` (both TFMs, 0 warnings), `dotnet test -c Release`, and `dotnet test -c Debug --filter "FullyQualifiedName~Threading.Actors"`.

## Invariants that must survive

- **§3.1:** a key index is in exactly one place at any instant — never two.
- `DispatchCore`: `Interlocked.MemoryBarrier()` between the `Volatile.Write` of `cell.Tail` and the `Volatile.Read` of `cell.State`; test-then-CAS.
- `Activate`: acquires with `CompareExchange(Owned, Scheduled)` and returns without touching the cell on failure; the `chunk == Chunk.None` guard stays after it.
- `ReleaseAndMaybeSchedule`: publish-`Head` → `Interlocked.Exchange`-`State` → read-`Tail`, with the producer-wake block between the `Exchange` and the `Tail` read; worker-only work behind `fromWorker`.
- `WaitForChunk` re-runs `Harvest()` inside its armed window.
- `PublishProcessed` gates its broadcast on `IsQuiescent()` — flag first, then the sum.
- `IsQuiescent` reads `_completed` before summing. `TryComplete` publishes `_dispatchedFinal` before `_completed`, calls `SignalStop` unconditionally, and does not clobber an existing `_completionError`.
- `RunLoop`'s `finally`: `w`-dependent statements guarded; `Interlocked.Decrement` and completion resolution unconditional; `QuiescentSum()` recomputed inside the `== 0` branch.
- `KeyCellMap.EvictFree` holds its `Reclaiming` claim continuously; `Place` publishes `Free` last.
- All four park sites are arm-then-recheck. Neither `ActorSystem` event is disposed.

---

## File Structure

| File | Change |
| --- | --- |
| `…/IKeyScheduler.cs` | `Start` gains a fault callback parameter |
| `…/Schedulers/RoutedScheduler.cs` | stores the callback; `RouteLoop`'s catch invokes it |
| `…/Schedulers/SingleWorkerScheduler.cs` | stores the callback (no background thread, so never invokes it) |
| `…/ActorSystem.cs` | supplies the callback; worker catch stops the system; completion prefers a fault over a cancel; two parked residuals |
| `tests/…/Actors/FaultPropagationTests.cs` | **new** — faults resolve `Completion` rather than hanging |
| `tests/…/Actors/MultiWorkerTests.cs` | extended with the N>1 cold-path test |

---

### Task 1: A fault channel that reaches `Completion`

**Files:**
- Modify: `src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/IKeyScheduler.cs`
- Modify: `…/Schedulers/RoutedScheduler.cs`
- Modify: `…/Schedulers/SingleWorkerScheduler.cs`
- Modify: `…/ActorSystem.cs`
- Test: `tests/Nanov.HighPerformance.Tests/Threading/Actors/FaultPropagationTests.cs` (new)

**Interfaces:**
- Produces: `void Start(int workerCount, Action<Exception> onFault)` on `IKeyScheduler`; `ActorSystem.Fault(Exception)` private

- [ ] **Step 1: Write the failing tests**

Create `tests/Nanov.HighPerformance.Tests/Threading/Actors/FaultPropagationTests.cs`:

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Actors;

using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using NUnit.Framework;

/// <summary>
///   A fault on ANY background thread must resolve <c>Completion</c>, not hang the system.
///   <para>
///     Before this, a worker's catch recorded the exception but never set <c>_stopped</c>, and the
///     router's catch set a scheduler-private flag <c>ActorSystem</c> never read. Either way the
///     surviving workers found <c>IsQuiescent()</c> false — the dead thread's messages are never
///     processed — and re-parked on an unbounded wait. <c>_liveWorkers</c> never reached zero and
///     <c>Completion</c> never resolved. A hang is the worst possible failure mode: it reports
///     nothing and cannot be diagnosed from the outside.
///   </para>
/// </summary>
[TestFixture]
public class FaultPropagationTests {
	private static ActorSystem<int, int, RecordingHandler, RoutedScheduler> New(
		RecordingState state, int workers) =>
		new(new RecordingHandler(state), keyCapacity: 64, chunkCount: 16, chunkSize: 64,
			batchBound: 0, workers: workers, scheduler: new RoutedScheduler { Capacity = 256 });

	[Test]
	[CancelAfter(30_000)]
	public void A_Handler_Exception_Is_Not_A_Fault_And_Completion_Still_Succeeds() {
		// Guards the fault channel against being wired too aggressively. A throw from Handle is caught
		// per message, reported to OnError, and the message consumed — the worker continues. If that
		// ever reached Fault(), one bad message would tear the whole system down.
		var state = new RecordingState();
		state.Throw = (_, value) => value == 0;
		using var system = New(state, workers: 4);
		system.Start();

		for (var i = 0; i < 20; i++)
			Assert.That(system.Dispatch(i % 4, i), Is.True);

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 20);

		Assert.Multiple(() => {
			Assert.That(state.Errors, Is.Not.Empty, "the handler throw was never reported to OnError");
			Assert.That(system.Completion.IsCompletedSuccessfully, Is.True,
				"a handler exception must not fault Completion — it is not an escaping fault");
		});
	}

	[Test]
	[CancelAfter(30_000)]
	public void A_Background_Fault_Faults_Completion_With_That_Exception() {
		var state = new RecordingState();
		using var system = New(state, workers: 4);
		system.Start();

		system.Dispatch(1, 1);
		WorkerTestUtil.AwaitProgress(() => state.HandledCount, 1);

		// Injected deliberately. The router's only natural trigger is a misconfiguration that is now
		// bounds-checked, and a worker-loop escape has no natural trigger at all — so a hook is the
		// honest instrument. Contriving a "natural" trigger would make the test lie about its subject.
		system.InjectSchedulerFaultForTesting(new InvalidOperationException("router boom"));

		var agg = Assert.Throws<AggregateException>(() => system.Completion.Wait(TimeSpan.FromSeconds(10)));
		Assert.That(agg!.InnerException, Is.InstanceOf<InvalidOperationException>());
		Assert.That(agg.InnerException!.Message, Is.EqualTo("router boom"),
			"the fault was recorded but the wrong exception surfaced");
	}

	[Test]
	[CancelAfter(30_000)]
	public void A_Fault_Releases_Workers_Already_Parked_On_An_Unbounded_Wait() {
		// The case that actually hangs today: every worker is asleep on a wait with no timeout when the
		// fault arrives. Nothing else will ever signal them — no message will be processed, so no
		// publish, so no quiescence broadcast — which is why Fault() must set _stopped AND signal.
		var state = new RecordingState();
		using var system = New(state, workers: 8);
		system.Start();

		system.Dispatch(1, 1);
		WorkerTestUtil.AwaitProgress(() => state.HandledCount, 1);
		Thread.Sleep(200); // all eight park

		system.InjectSchedulerFaultForTesting(new InvalidOperationException("boom while parked"));

		// Assert.Throws fails with "no exception" if Wait times out, so a hang is a red test, not a
		// hang — the bounded Wait is what makes that true.
		var agg = Assert.Throws<AggregateException>(() => system.Completion.Wait(TimeSpan.FromSeconds(10)));
		Assert.That(agg!.InnerException!.Message, Is.EqualTo("boom while parked"));
		Assert.That(Volatile.Read(ref state.CompletedCalls), Is.EqualTo(8),
			"not every worker reached OnCompleted — some never woke");
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `dotnet test -c Release --filter "FullyQualifiedName~FaultPropagationTests"`
Expected: compile error — `InjectSchedulerFaultForTesting` does not exist.

- [ ] **Step 3: Add the fault channel to the seam**

In `IKeyScheduler.cs`, change `Start` and document why:

```csharp
	/// <summary>
	///   Allocate per-worker structures and start any scheduler-owned threads. Called once, before any
	///   worker starts.
	///   <para>
	///     <paramref name="onFault" /> is the scheduler's channel back to the system for a fault on a
	///     thread the scheduler owns. A scheduler-owned thread that dies silently is the worst failure
	///     mode available: the system's workers park on an unbounded wait, quiescence becomes
	///     unreachable, and <c>Completion</c> never resolves. Invoke it from any catch on such a thread
	///     BEFORE stopping, so the exception is recorded rather than lost.
	///   </para>
	/// </summary>
	void Start(int workerCount, Action<Exception> onFault);
```

In `RoutedScheduler`: add `public Action<Exception>? OnFault;` to `State`, assign it in `Start`, and in
`RouteLoop`'s existing catch invoke it **before** the existing stop-and-signal cleanup:

```csharp
			catch (Exception ex) {
				// Tell the system first: it records the exception and sets _stopped, which is what lets
				// the workers exit rather than park forever. Then the existing cleanup releases anyone
				// already parked.
				s.OnFault?.Invoke(ex);
				s.Stopping = true;
				s.InboxNotEmpty.Set();
				foreach (var evt in s.WorkAvailable!)
					evt.Set();
			}
```

In `SingleWorkerScheduler`: accept and store the callback for interface conformance. It owns no
background thread, so nothing invokes it — say that in a comment rather than leaving a reader wondering.

- [ ] **Step 4: Wire it in `ActorSystem`**

Add the fault entry point, and call `_scheduler.Start(workers, Fault)` in the constructor:

```csharp
	/// <summary>
	///   Record a fault from a background thread and stop the system so everyone can exit.
	///   <para>
	///     All three steps are required. Recording alone leaves the survivors parked; stopping alone
	///     loses the exception; and without the wake, workers already blocked on an unbounded wait
	///     never re-check <c>_stopped</c>.
	///   </para>
	/// </summary>
	private void Fault(Exception error) {
		_completionError ??= error;
		_stopped = true;
		_scheduler.SignalStop();
	}
```

Then make `RunLoop`'s catch use it, so a worker-loop escape stops the system rather than only recording:

```csharp
		catch (Exception ex) {
			Fault(ex);
			if (w is not null) SafeOnError(w, ex);
		}
```

And add the test hook beside the others:

```csharp
	internal void InjectSchedulerFaultForTesting(Exception error) => Fault(error);
```

- [ ] **Step 5: Make the completion verdict prefer a fault over a cancel**

In `RunLoop`'s `finally`, the `Decrement == 0` branch currently tests `_stopped` first, so a faulted
system reports *cancelled* and loses the exception. Reorder:

```csharp
			if (Interlocked.Decrement(ref _liveWorkers) == 0) {
				var allDrained = _completed && QuiescentSum() >= Volatile.Read(ref _dispatchedFinal);
				// A recorded fault wins over a cancellation: Fault() sets _stopped as part of stopping
				// the system, so testing _stopped first would report every fault as a plain cancel and
				// discard the exception. A cancel is what a fault-free hard stop looks like.
				if (_completionError is not null) _completion.TrySetException(_completionError);
				else if (_stopped && !allDrained) _completion.TrySetCanceled();
				else _completion.TrySetResult();
			}
```

**Check this against the existing tests before running them**: `Dispose_After_TryComplete_With_Work_Outstanding_Cancels_Completion` calls `TryComplete()` with no argument, so `_completionError` stays null and it still cancels. `TryComplete_With_An_Exception_Faults_Completion` sets it and still faults. If either assumption is wrong, stop and report rather than editing the test.

- [ ] **Step 6: Close the two parked residuals**

Both are in the files you are already touching.

(a) `WaitForMailboxSpace`'s XML doc still names `ReleaseAndMaybeSchedule` as an unfenced wake site. It is
fenced now — its `_producerParked` read sits after `Interlocked.Exchange`. The remaining debt is
`Activate`'s mid-batch flush alone. Correct the doc, and the matching sentence in `RoutedScheduler`
that says "the worker's wake site" in the singular.

(b) `WaitForChunk`'s new `return true` fast path skips the `_stopped` re-check the sleep path does, so a
`Dispose` landing in that window lets a chunk-starved `Dispatch` return true for a message that is then
dropped. Change it to `return !_stopped;`.

- [ ] **Step 7: Run everything**

Run: `dotnet build -c Release` → 0 warnings, both TFMs.
Run: `dotnet test -c Release --filter "FullyQualifiedName~FaultPropagationTests"` → 3 tests, both TFMs.
Run: `dotnet test -c Release` → whole repo, both TFMs. Report totals (currently 208).
Run: `dotnet test -c Debug --filter "FullyQualifiedName~Threading.Actors"` → both TFMs.

**Then prove the fix is load-bearing**: temporarily remove the `_stopped = true;` line from `Fault`, run
`A_Fault_Releases_Workers_Parked_On_An_Unbounded_Wait`, and confirm it now times out rather than passing.
Restore, confirm green, and report what you observed. A fault-channel that cannot be shown to matter is
not a fix.

- [ ] **Step 8: Commit**

```bash
git add src/experiments tests/Nanov.HighPerformance.Tests
git commit -m "fix(actors): give schedulers a fault channel so background deaths fault Completion"
```

---

### Task 2: Exercise the cold paths above one worker

`Harvest()` is reachable from exactly one N>1 test (which asserts neither ordering nor claims), and
`KeyCellMap.EvictFree()` from none. Both exist to race a worker that is mid-release between its
`Exchange(State, Free)` and its reschedule CAS — the very race that produced two of Phase 1's bugs — and
that race has only ever been tested against a single thread.

**Files:**
- Modify: `tests/Nanov.HighPerformance.Tests/Threading/Actors/MultiWorkerTests.cs`

**Interfaces:**
- Consumes: `ActorSystem`, `RoutedScheduler`, `RecordingState`/`RecordingHandler`, `WorkerTestUtil`

- [ ] **Step 1: Write the test**

Append to `MultiWorkerTests.cs`:

```csharp
	/// <summary>
	///   Forces BOTH producer cold paths — chunk harvest and cell eviction — against eight concurrently
	///   releasing workers.
	///   <para>
	///     <c>keyCapacity: 8</c> gives 16 map slots against 400 distinct keys, so once 16 are occupied
	///     every new key must reach <c>GetOrAddRefCold</c> and evict. <c>chunkCount: 4</c> against those
	///     same 400 keys means <c>TryRent</c> fails constantly and <c>Harvest()</c> runs on nearly every
	///     bind. Both paths' correctness argument is entirely about racing a worker between its
	///     <c>Exchange(State, Free)</c> and its reschedule CAS, and until this test that race had only
	///     ever run against one worker on one core.
	///   </para>
	/// </summary>
	[Test]
	[CancelAfter(120_000)]
	public void Eviction_And_Harvest_Under_Eight_Workers_Preserve_Order_And_Lose_Nothing() {
		const int keys = 400;
		const int perKey = 3;
		const int total = keys * perKey;
		var state = new RecordingState();
		using var system = new ActorSystem<int, int, RecordingHandler, RoutedScheduler>(
			new RecordingHandler(state), keyCapacity: 8, chunkCount: 4, chunkSize: 4,
			batchBound: 1, workers: 8, scheduler: new RoutedScheduler { Capacity = 64 });
		system.Start();

		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				for (var k = 0; k < keys; k++)
					for (var i = 0; i < perKey; i++) {
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

		Assert.That(state.HandledCount, Is.EqualTo(total));
		Assert.That(system.RejectedClaims, Is.EqualTo(0),
			"a worker lost the Scheduled -> Owned CAS: two tokens existed for one key index");

		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");
	}
```

Add `using System.Linq;` inside the namespace if it is not already present.

- [ ] **Step 2: Run it**

Run: `dotnet test -c Release --filter "FullyQualifiedName~Eviction_And_Harvest_Under_Eight"`
Expected: passes on both TFMs.

**If it FAILS or HANGS, stop and report BLOCKED.** That is the most valuable outcome this task can
produce — it would mean one of the two cold paths has an N>1 bug that a single worker was hiding. Do not
adjust the parameters, lower the counts, or add sleeps to make it pass.

- [ ] **Step 3: Prove it actually reaches both paths**

The whole point is coverage, so demonstrate it rather than assuming. Add temporary counters — one
incremented in `Harvest()` when it frees a chunk, one in `KeyCellMap.EvictFree()` when it tombstones —
run the test, and report both observed values. Then **revert the instrumentation** and confirm the
working tree is clean before committing. If either count is zero, the fixture does not do what its doc
comment claims and must be resized.

- [ ] **Step 4: Run everything**

Run: `dotnet build -c Release` → 0 warnings, both TFMs.
Run: `dotnet test -c Release` → whole repo, both TFMs.
Run: `dotnet test -c Debug --filter "FullyQualifiedName~Threading.Actors"` → both TFMs.
Run the `Threading.Actors` filter **five times** in Release, reporting each — this test races two cold
paths against eight workers and is the most likely place in the suite to be intermittent.

- [ ] **Step 5: Commit**

```bash
git add tests/Nanov.HighPerformance.Tests
git commit -m "test(actors): force harvest and eviction against eight concurrent workers"
```

---

## After this

Both tracked issues closed, and Phase 2b (scheduler D) can start on a system where a background-thread
fault is observable and where the producer's cold paths have multi-worker coverage.

Still outstanding and unchanged: the `Activate` mid-batch-flush StoreLoad debt (bounded by the 20 ms
poll, documented); the unproven `LocalDrainBound` and the targeted starvation test §6.1 specifies; and
the experiment's central question, which needs scheduler D and the benchmark.
