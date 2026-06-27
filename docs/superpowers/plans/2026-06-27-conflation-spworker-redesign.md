# ConflationSpWorker Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans or subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Rebuild `ConflationSpWorker` as the coalescer's twin — sharing `CoalescingKeyMap` + `SpRingCore<int>` with a 3-state presence protocol instead of its bespoke Dekker override core — to make it as fast as / faster than `CoalescingSpWorker`.

**Architecture:** Producer overwrites a stable per-key cell's `Value` (single atomic ref write) and manages a dirty-index ring via one `Interlocked.Exchange` on a 3-state word (`Free/Available/Consuming`). Consumer claims `Available→Consuming`, reads, conditionally releases the value ref to null, then `Consuming→Free`. Eviction is the unchanged producer-side state-gated `EvictFree`.

**Tech Stack:** C# / .NET 9 + 10, NUnit, BenchmarkDotNet. House high-performance rules apply (zero per-dispatch alloc, devirtualized struct handler, `[AggressiveInlining]` hot path).

## Global Constraints

- `TValue : class` — the overwrite is a single atomic reference write; a struct would tear.
- Single-producer is the caller's contract (DEBUG overlap guard); single-consumer is the worker's thread.
- Public API of `ConflationSpWorker` is unchanged (ctor, `Start`/`Dispatch`/`TryComplete`/`Dispose`/`Completion`/`QueuedApprox`/`Capacity`, `IWorkerQueue`+`IDisposable`).
- `dotnet build -c Release` → net9.0 + net10.0, **0 warnings** (warnings-as-errors).
- House style: tabs width 2, file-scoped namespace with usings inside, K&R braces, `var`.

---

### Task 1: Rewrite ConflationSpWorker onto the shared infra

**Files:**
- Rewrite: `src/Nanov.HighPerformance/Threading/Workers/ConflationSpWorker.cs`
- Reuse (no change): `Internal/CoalescingKeyMap.cs` (`CoalescingCell`), `Internal/SpRingCore.cs`
- Regression test (no change): `tests/.../Threading/Workers/ConflationSpWorkerTests.cs`

**Interfaces:**
- Consumes: `CoalescingKeyMap<TKey,TValue>` (`GetOrAddRef(key, out existing, out index)`, `RefAt(index)`, `Dispose`), `SpRingCore<int>` (`new(capacity, threadName, runLoop)`, `TryEnqueue`, `TryClaimRun`, `TryTakeAndAdvance`, `Completion`, `QueuedApprox`, `Capacity`, `Start`, `TryComplete`, `Dispose`, `CompletionException`, `SetCompletion`), `IConflationWorkerHandler<TKey,TValue>`.
- Produces: unchanged public `ConflationSpWorker<TKey,TValue,THandler>` surface.

- [ ] **Step 1: Replace the file body** with the twin implementation (owns `SpRingCore<int> _ring` + `CoalescingKeyMap<TKey,TValue> _map`; states `Free=0/Available=1/Consuming=2`; producer = overwrite + `Exchange`; consumer run loop = claim/read/conditional-null-clear/release; DEBUG overlap guard). Constraint `where TValue : class`. (Full code in Task body below.)

- [ ] **Step 2: Build** `dotnet build -c Release` → 0 warnings, net9.0+net10.0.

- [ ] **Step 3: Run the regression suite** `dotnet test -c Release --filter 'ConflationSpWorkerTests'` → all green (incl. `Same_Key_Conflates_To_Latest_Keeping_Position`, `Conflation_Stress_Delivers_Latest_Per_Key`, `New_Key_Blocks_When_Full_But_Conflation_Does_Not`, `Ref_Value_Slots_Are_Cleared_After_Drain`, `Many_One_Shot_Keys_Force_Compaction_And_Stay_Correct`).

- [ ] **Step 4: Debug suite** `dotnet test -c Debug --filter 'ConflationSpWorkerTests'` → exercises the overlap guard.

### Task 2: Delete the dead bespoke core + its map + predicate + map tests

**Files:**
- Delete: `Internal/ConflationSpRingCore.cs`, `Internal/ConflationKeyMap.cs`, `Internal/ISeqKeepPredicate.cs`
- Delete: `tests/.../Threading/Workers/ConflationKeyMapTests.cs`

**Interfaces:** none produced; removes `ConflationSpRingCore`, `ConflationKeyMap`, `ISeqKeepPredicate` (confirmed: only users were the deleted core/map).

- [ ] **Step 1:** `git rm` the four files.
- [ ] **Step 2: Build + full test** `dotnet build -c Release` then `dotnet test -c Release` → all green, 0 warnings.

### Task 3: Update docs

**Files:**
- Modify: `src/Nanov.HighPerformance/Threading/CLAUDE.md` (replace the "ConflationSpWorker invariants" section: sibling override core + Dekker → twin on `CoalescingKeyMap` + dirty ring, 3-state presence, `Merging`-free, conditional value clear).
- Modify: `src/Nanov.HighPerformance/Threading/README.md` (conflation description if it references the override core).

- [ ] **Step 1:** Edit both, matching surrounding tone.
- [ ] **Step 2: Commit** the worker rewrite + deletions + docs together.

### Task 4: Benchmark

- [ ] **Step 1:** `dotnet run -c Release --project benchmarks/Nanov.HighPerformance.Benchmarks -- --filter '*ConflationWorker*' --filter '*CoalescingWorker*'`.
- [ ] **Step 2:** Confirm conflation lands at/below coalescing (was 1.7–2.2× slower). Report the table.

## Self-Review

- Spec coverage: 3-state protocol (Task 1), shared map reuse (Task 1), delete bespoke core (Task 2), eviction via existing `EvictFree` (reuse, Task 1), docs (Task 3), benchmark (Task 4). ✓
- Correction vs spec: consumer DOES release the value ref, but **conditionally** (`Interlocked.CompareExchange(ref cell.Value, null, taken)`) — required by `Ref_Value_Slots_Are_Cleared_After_Drain`; safe because the consumer only writes null and only when the slot still holds the value it read (no lost update). ✓
- Placeholder scan: none. Type consistency: `Free/Available/Consuming`, `GetOrAddRef`/`RefAt`/`TryEnqueue`/`TryClaimRun`/`TryTakeAndAdvance` match `CoalescingSpWorker`. ✓
