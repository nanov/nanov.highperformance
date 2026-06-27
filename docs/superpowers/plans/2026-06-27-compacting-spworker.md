# CompactingSpWorker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a key-conflating member to the SPSC worker family — `CompactingSpWorker<TKey,TValue,THandler>` where `Dispatch(key, value)` overrides an already-queued value for the same key in place (keeping its position) via a handler `Merge` hook, guaranteeing the consumer always sees the latest value per key.

**Architecture:** A producer-private open-addressing map (`TKey → int seq`) sits over a two-array SPSC ring (`TKey[]` + `TValue[]`). A sibling core `CompactingSpRingCore<TKey,TValue,TMerger>` (generic over the merge facet only, so a future async worker reuses it) owns the ring + map + producer conflation; the consumer run loop (`Handle`) lives in the worker. In-place override is made lossless with a Dekker store/load fence + post-write head recheck that enqueues a fresh slot on doubt.

**Tech Stack:** C# (net9.0 + net10.0), NUnit, BenchmarkDotNet. House high-performance style (tabs width 2, K&R, `var`, file-scoped namespace with usings inside, no LINQ/closures/allocations on hot paths).

**Design spec:** `docs/superpowers/specs/2026-06-27-compacting-spworker-design.md` (read it first).

## Global Constraints

- Target frameworks: `net9.0;net10.0`. Build must be **0 warnings** (`TreatWarningsAsErrors=true`).
- AOT-compatible (`IsAotCompatible=true`) — no reflection, no runtime codegen.
- House style: tabs (width 2), file-scoped namespace with `using` directives **inside** the namespace, K&R braces (open brace same line), no braces for single-line statements, `var` everywhere, expression-bodied members where they fit, no `this.`, `_camelCase` private fields, `IPascalCase`/`TPascalCase` interfaces/type-params.
- Hot paths (`Dispatch`, `TryGetSeq`, `Set`, `TryTake`, `TryClaimRun`, `TryReserve`, `CommitTail`, `AdvanceHead`): no LINQ, no closures capturing locals, no per-call allocation, no virtual dispatch. Fast path `[MethodImpl(MethodImplOptions.AggressiveInlining)]`; cold parking loops `[MethodImpl(MethodImplOptions.NoInlining)]`.
- SPSC contract: single producer (caller's responsibility, DEBUG-guarded), single consumer (the worker's own thread — structural).
- Public surface stays minimal: `CompactingSpWorker`, `ICompactingWorkerHandler`, `IItemMerger`. Everything else `internal`.
- Verify after each task: `dotnet build -c Release` (both TFMs, 0 warnings) and `dotnet test -c Release`.
- Scope: **sync, reference-`TValue`, replace-style merge only.** No async, no value-struct payloads, no additive merge.

---

### Task 1: Public interfaces — `IItemMerger` + `ICompactingWorkerHandler`

**Files:**
- Create: `src/Nanov.HighPerformance/Threading/Workers/IItemMerger.cs`
- Create: `src/Nanov.HighPerformance/Threading/Workers/ICompactingWorkerHandler.cs`

**Interfaces:**
- Produces: `IItemMerger<TValue>.Merge(TValue current, TValue incoming) → TValue`; `ICompactingWorkerHandler<TKey,TValue>` (inherits `IItemMerger<TValue>`) with `Handle(TKey, TValue)`, `OnError(Exception)`, `OnCompleted(Exception?)`.

- [ ] **Step 1: Create `IItemMerger.cs`**

```csharp
namespace Nanov.HighPerformance.Threading.Workers;

/// <summary>
///   Narrow, producer-side conflation facet for <see cref="CompactingSpWorker{TKey, TValue, THandler}" />.
///   Separated from the handler so the (consumer-model-agnostic) core depends only on this — it never
///   references <c>Handle</c>, which keeps the core reusable by a future async compacting worker.
///   Implement as part of a <see langword="struct" /> handler so the call devirtualizes and inlines.
/// </summary>
public interface IItemMerger<TValue> {
	/// <summary>
	///   Conflate an <paramref name="incoming" /> value into the one already queued for the same key,
	///   returning the value to publish. Runs on the PRODUCER thread (not the worker thread), so it
	///   <b>must be pure</b>: read only its two arguments, return a value, touch no mutable handler
	///   state. Must be <b>replace-style / idempotent</b> — each returned value self-contained — because
	///   the lossless override protocol may, under a race, deliver the previous value and then
	///   <paramref name="incoming" /> as two separate items. (Additive/accumulate merges are unsafe here.)
	///   "Keep latest / react on replacement" = <c>return incoming;</c>.
	/// </summary>
	TValue Merge(TValue current, TValue incoming);
}
```

- [ ] **Step 2: Create `ICompactingWorkerHandler.cs`**

```csharp
namespace Nanov.HighPerformance.Threading.Workers;

using System;

/// <summary>
///   Synchronous handler for <see cref="CompactingSpWorker{TKey, TValue, THandler}" />. Implement as a
///   <see langword="struct" /> — the worker stores it by value and dispatches through a constrained
///   generic call, so the JIT devirtualizes and inlines per closed worker type. Inherits the producer-
///   side <see cref="IItemMerger{TValue}" /> (conflation) and adds the consumer side. Note the thread
///   split: <see cref="IItemMerger{TValue}.Merge" /> runs on the producer thread; <see cref="Handle" />/
///   <see cref="OnError" />/<see cref="OnCompleted" /> run on the worker thread.
/// </summary>
public interface ICompactingWorkerHandler<TKey, TValue> : IItemMerger<TValue> {
	/// <summary>Process the latest value for one key. Invoked in first-seen key order on the worker thread.</summary>
	void Handle(TKey key, TValue value);

	/// <summary>Invoked synchronously when <see cref="Handle" /> throws. The worker continues with the next item.</summary>
	void OnError(Exception error);

	/// <summary>
	///   Invoked once after the worker has drained and is about to exit. <paramref name="error" /> is
	///   <see langword="null" /> for normal completion, an <see cref="OperationCanceledException" /> for
	///   cancellation, or the exception passed to <c>TryComplete</c>.
	/// </summary>
	void OnCompleted(Exception? error);
}
```

- [ ] **Step 3: Build to verify the interfaces compile**

Run: `dotnet build -c Release src/Nanov.HighPerformance/Nanov.HighPerformance.csproj`
Expected: build succeeds, 0 warnings (both TFMs).

- [ ] **Step 4: Commit**

```bash
git add src/Nanov.HighPerformance/Threading/Workers/IItemMerger.cs src/Nanov.HighPerformance/Threading/Workers/ICompactingWorkerHandler.cs
git commit -m "feat(compacting): add IItemMerger + ICompactingWorkerHandler interfaces"
```

---

### Task 2: `CompactingKeyMap<TKey>` + `ISeqKeepPredicate` (producer-private map)

**Files:**
- Create: `src/Nanov.HighPerformance/Threading/Workers/Internal/ISeqKeepPredicate.cs`
- Create: `src/Nanov.HighPerformance/Threading/Workers/Internal/CompactingKeyMap.cs`
- Test: `tests/Nanov.HighPerformance.Tests/Threading/Workers/CompactingKeyMapTests.cs`

**Interfaces:**
- Consumes: nothing.
- Produces (all `internal`):
  - `ISeqKeepPredicate.Keep(int seq) → bool`
  - `CompactingKeyMap<TKey>` (`struct`, `IDisposable`, `where TKey : notnull, IEquatable<TKey>`): ctor `(int ringCapacity)`; `int Count {get;}`; `bool IsFull {get;}`; `bool TryGetSeq(TKey key, out int seq)`; `void Set(TKey key, int seq)` (insert-or-update, caller ensures room when key may be new); `void Compact<TPred>(in TPred pred) where TPred : struct, ISeqKeepPredicate`; `void Dispose()`.

- [ ] **Step 1: Write the failing test file**

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Workers;

using Nanov.HighPerformance.Threading.Workers.Internal;
using NUnit.Framework;

[TestFixture]
public class CompactingKeyMapTests {
	// Keep-all predicate (no eviction) and a head/tail "pending" predicate for compaction tests.
	private readonly struct KeepAll : ISeqKeepPredicate {
		public bool Keep(int seq) => true;
	}

	private readonly struct Pending : ISeqKeepPredicate {
		private readonly int _head;
		private readonly int _tail;
		public Pending(int head, int tail) { _head = head; _tail = tail; }
		public bool Keep(int seq) => seq - _head >= 0 && _tail - seq > 0;
	}

	[Test]
	public void Missing_Key_Returns_False() {
		using var map = new CompactingKeyMap<int>(8);
		Assert.That(map.TryGetSeq(42, out _), Is.False);
		Assert.That(map.Count, Is.Zero);
	}

	[Test]
	public void Set_Then_Get_Returns_Seq() {
		using var map = new CompactingKeyMap<int>(8);
		map.Set(42, 1000);
		Assert.That(map.TryGetSeq(42, out var seq), Is.True);
		Assert.That(seq, Is.EqualTo(1000));
		Assert.That(map.Count, Is.EqualTo(1));
	}

	[Test]
	public void Set_Existing_Key_Updates_In_Place_No_Growth() {
		using var map = new CompactingKeyMap<int>(8);
		map.Set(42, 1000);
		map.Set(42, 2000);
		Assert.That(map.TryGetSeq(42, out var seq), Is.True);
		Assert.That(seq, Is.EqualTo(2000));
		Assert.That(map.Count, Is.EqualTo(1), "updating an existing key must not add an entry");
	}

	[Test]
	public void Many_Keys_With_Probe_Collisions_All_Resolve() {
		using var map = new CompactingKeyMap<int>(64);
		for (var i = 0; i < 48; i++) map.Set(i, i * 10);
		for (var i = 0; i < 48; i++) {
			Assert.That(map.TryGetSeq(i, out var seq), Is.True, $"key {i} should be present");
			Assert.That(seq, Is.EqualTo(i * 10));
		}
	}

	[Test]
	public void Compact_Drops_Entries_Where_Predicate_False() {
		using var map = new CompactingKeyMap<int>(64);
		// seqs 0..31; "pending" window [16, 32) keeps 16..31, drops 0..15.
		for (var i = 0; i < 32; i++) map.Set(i, i);
		map.Compact(new Pending(16, 32));
		Assert.That(map.Count, Is.EqualTo(16));
		for (var i = 0; i < 16; i++) Assert.That(map.TryGetSeq(i, out _), Is.False, $"key {i} should be evicted");
		for (var i = 16; i < 32; i++) {
			Assert.That(map.TryGetSeq(i, out var seq), Is.True, $"key {i} should survive");
			Assert.That(seq, Is.EqualTo(i));
		}
	}

	[Test]
	public void Compact_Then_Set_Reuses_Freed_Capacity() {
		using var map = new CompactingKeyMap<int>(8); // entriesCapacity = 16
		for (var i = 0; i < 16; i++) map.Set(i, i);
		Assert.That(map.IsFull, Is.True);
		map.Compact(new Pending(8, 16)); // keep 8..15, drop 0..7
		Assert.That(map.IsFull, Is.False);
		map.Set(100, 999); // re-add into reclaimed room
		Assert.That(map.TryGetSeq(100, out var seq), Is.True);
		Assert.That(seq, Is.EqualTo(999));
	}

	[Test]
	public void KeepAll_Compact_Is_Identity() {
		using var map = new CompactingKeyMap<int>(8);
		map.Set(1, 10);
		map.Set(2, 20);
		map.Compact(new KeepAll());
		Assert.That(map.Count, Is.EqualTo(2));
		Assert.That(map.TryGetSeq(1, out var a), Is.True);
		Assert.That(a, Is.EqualTo(10));
		Assert.That(map.TryGetSeq(2, out var b), Is.True);
		Assert.That(b, Is.EqualTo(20));
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `dotnet test -c Release tests/Nanov.HighPerformance.Tests --filter "FullyQualifiedName~CompactingKeyMapTests"`
Expected: FAIL to compile — `CompactingKeyMap` / `ISeqKeepPredicate` do not exist.

- [ ] **Step 3: Create `ISeqKeepPredicate.cs`**

```csharp
namespace Nanov.HighPerformance.Threading.Workers.Internal;

/// <summary>
///   Struct-dispatched keep/drop predicate over a slot sequence, used by
///   <see cref="CompactingKeyMap{TKey}.Compact{TPred}" /> so the compaction loop devirtualizes and
///   inlines the test (no delegate alloc, no virtual call). Implement as a <see langword="struct" />.
/// </summary>
internal interface ISeqKeepPredicate {
	/// <summary>True to keep the entry with this <paramref name="seq" />, false to evict it.</summary>
	bool Keep(int seq);
}
```

- [ ] **Step 4: Create `CompactingKeyMap.cs`**

```csharp
namespace Nanov.HighPerformance.Threading.Workers.Internal;

using System;
using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

/// <summary>
///   Producer-private open-addressing (linear-probe) map <c>TKey → int seq</c> for the compacting
///   worker: it remembers, per key, the absolute ring sequence of that key's currently-queued slot.
///   <b>Single-threaded</b> — touched only by the producer thread, so it needs no synchronization.
///   Inspired by Prague's <c>ValueDictionary</c> hot-path layout (metadata slot → packed entry index)
///   but trimmed to exactly what the worker needs: lookup, insert-or-update, and a compact-and-rebuild
///   that evicts stale (already-consumed) keys. No per-key delete (so probe chains never hold
///   tombstones); eviction is the bulk <see cref="Compact{TPred}" />. ArrayPool-backed, zero
///   steady-state allocation.
/// </summary>
internal struct CompactingKeyMap<TKey> : IDisposable where TKey : notnull, IEquatable<TKey> {
	private int[] _metadata;          // probe slot → entry index; -1 = empty
	private TKey[] _keys;             // entry index → key   (packed [0, _count))
	private int[] _seqs;              // entry index → seq   (packed [0, _count))
	private int _slotMask;            // _metadata.Length - 1 (power of two)
	private int _count;               // live + stale entries packed in [0, _count)
	private readonly int _entriesCapacity;

	/// <summary>
	///   <paramref name="ringCapacity" /> is the worker's (power-of-two) ring capacity. Entry capacity is
	///   sized to 2× so stale entries can accumulate between compactions while still guaranteeing room: at
	///   most <c>ringCapacity</c> keys are ever pending, so a compaction always reclaims enough.
	/// </summary>
	internal CompactingKeyMap(int ringCapacity) {
		_entriesCapacity = Math.Max(ringCapacity * 2, 4);
		// Load factor ≈ 0.75 → slots ≥ entries * 4/3, rounded up to a power of two.
		var slots = (int)BitOperations.RoundUpToPowerOf2((uint)(_entriesCapacity * 4L / 3 + 1));
		_slotMask = slots - 1;
		_metadata = ArrayPool<int>.Shared.Rent(slots);
		Array.Fill(_metadata, -1, 0, slots);
		_keys = ArrayPool<TKey>.Shared.Rent(_entriesCapacity);
		_seqs = ArrayPool<int>.Shared.Rent(_entriesCapacity);
		_count = 0;
	}

	internal readonly int Count => _count;
	internal readonly bool IsFull => _count >= _entriesCapacity;

	/// <summary>Look up the recorded seq for <paramref name="key" />. False if absent.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal readonly bool TryGetSeq(TKey key, out int seq) {
		var mask = _slotMask;
		var slot = key.GetHashCode() & mask;
		ref var meta0 = ref MemoryMarshal.GetArrayDataReference(_metadata);
		ref var keys0 = ref MemoryMarshal.GetArrayDataReference(_keys);
		ref var seqs0 = ref MemoryMarshal.GetArrayDataReference(_seqs);
		while (true) {
			var idx = Unsafe.Add(ref meta0, slot);
			if (idx < 0) {
				seq = 0;
				return false;
			}

			if (Unsafe.Add(ref keys0, idx).Equals(key)) {
				seq = Unsafe.Add(ref seqs0, idx);
				return true;
			}

			slot = (slot + 1) & mask;
		}
	}

	/// <summary>
	///   Insert <paramref name="key" /> → <paramref name="seq" />, or update its seq if already present.
	///   The caller MUST guarantee room (see <see cref="IsFull" /> / <see cref="Compact{TPred}" />) before
	///   calling when the key may be new; an update of an existing key never grows the map.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void Set(TKey key, int seq) {
		var mask = _slotMask;
		var slot = key.GetHashCode() & mask;
		ref var meta0 = ref MemoryMarshal.GetArrayDataReference(_metadata);
		ref var keys0 = ref MemoryMarshal.GetArrayDataReference(_keys);
		ref var seqs0 = ref MemoryMarshal.GetArrayDataReference(_seqs);
		while (true) {
			var idx = Unsafe.Add(ref meta0, slot);
			if (idx < 0) break;
			if (Unsafe.Add(ref keys0, idx).Equals(key)) {
				Unsafe.Add(ref seqs0, idx) = seq;
				return;
			}

			slot = (slot + 1) & mask;
		}

		var count = _count;
		Unsafe.Add(ref keys0, count) = key;
		Unsafe.Add(ref seqs0, count) = seq;
		Unsafe.Add(ref meta0, slot) = count;
		_count = count + 1;
	}

	/// <summary>
	///   Compact-and-rebuild: keep only entries whose seq satisfies <paramref name="pred" /> (the live
	///   "pending" ones), drop the rest, then rebuild the probe metadata. O(capacity), producer-side.
	/// </summary>
	internal void Compact<TPred>(in TPred pred) where TPred : struct, ISeqKeepPredicate {
		ref var keys0 = ref MemoryMarshal.GetArrayDataReference(_keys);
		ref var seqs0 = ref MemoryMarshal.GetArrayDataReference(_seqs);
		var count = _count;
		var write = 0;
		for (var read = 0; read < count; read++) {
			var seq = Unsafe.Add(ref seqs0, read);
			if (!pred.Keep(seq)) continue;
			if (write != read) {
				Unsafe.Add(ref keys0, write) = Unsafe.Add(ref keys0, read);
				Unsafe.Add(ref seqs0, write) = seq;
			}

			write++;
		}

		_count = write;

		var slots = _slotMask + 1;
		Array.Fill(_metadata, -1, 0, slots);
		var mask = _slotMask;
		ref var meta0 = ref MemoryMarshal.GetArrayDataReference(_metadata);
		for (var i = 0; i < write; i++) {
			var slot = Unsafe.Add(ref keys0, i).GetHashCode() & mask;
			while (Unsafe.Add(ref meta0, slot) >= 0) slot = (slot + 1) & mask;
			Unsafe.Add(ref meta0, slot) = i;
		}
	}

	public void Dispose() {
		if (_metadata is null) return;
		ArrayPool<int>.Shared.Return(_metadata);
		ArrayPool<TKey>.Shared.Return(_keys, RuntimeHelpers.IsReferenceOrContainsReferences<TKey>());
		ArrayPool<int>.Shared.Return(_seqs);
		_metadata = null!;
		_keys = null!;
		_seqs = null!;
		_count = 0;
	}
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `dotnet test -c Release tests/Nanov.HighPerformance.Tests --filter "FullyQualifiedName~CompactingKeyMapTests"`
Expected: PASS (all 7 tests, both TFMs).

- [ ] **Step 6: Commit**

```bash
git add src/Nanov.HighPerformance/Threading/Workers/Internal/ISeqKeepPredicate.cs src/Nanov.HighPerformance/Threading/Workers/Internal/CompactingKeyMap.cs tests/Nanov.HighPerformance.Tests/Threading/Workers/CompactingKeyMapTests.cs
git commit -m "feat(compacting): add CompactingKeyMap (producer-private TKey->seq map)"
```

---

### Task 3: `CompactingSpRingCore` + `CompactingSpWorker` + behavioral tests

**Files:**
- Create: `src/Nanov.HighPerformance/Threading/Workers/Internal/CompactingSpRingCore.cs`
- Create: `src/Nanov.HighPerformance/Threading/Workers/CompactingSpWorker.cs`
- Test: `tests/Nanov.HighPerformance.Tests/Threading/Workers/CompactingSpWorkerTests.cs`

**Interfaces:**
- Consumes: `IItemMerger<TValue>`, `ICompactingWorkerHandler<TKey,TValue>` (Task 1); `CompactingKeyMap<TKey>`, `ISeqKeepPredicate` (Task 2); `PaddedIndices` (existing, in `SpRingCore.cs`, same `Internal` namespace).
- Produces:
  - `CompactingSpRingCore<TKey,TValue,TMerger>` (`internal sealed`, `where TKey : notnull, IEquatable<TKey>`, `where TValue : class`, `where TMerger : struct, IItemMerger<TValue>`): ctor `(TMerger merger, int capacity, string threadName, ThreadStart runLoop)`; `Task Completion`; `int QueuedApprox`; `int Capacity`; `void Start(CancellationToken)`; `bool Dispatch(TKey, TValue)`; `bool TryComplete(Exception?)`; `bool TryClaimRun(out int head, out int available)`; `bool TryTake(int head, out TKey key, out TValue value)`; `Exception? CompletionException()`; `void SetCompletion(Exception?)`; `void Dispose()`.
  - `CompactingSpWorker<TKey,TValue,THandler>` (`public sealed`, `IDisposable`, `IWorkerQueue`): ctor `(THandler handler, int capacity, string? threadName = null)`; `Task Completion`; `int QueuedApprox`; `int Capacity`; `void Start(CancellationToken ct = default)`; `bool Dispatch(TKey, TValue)`; `bool TryComplete(Exception? = null)`; `void Dispose()`.

- [ ] **Step 1: Write the failing behavioral test file**

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Workers;

using System.Linq;
using Nanov.HighPerformance.Threading.Workers;
using NUnit.Framework;

[TestFixture]
public class CompactingSpWorkerTests {
	// Shared observable state the struct handler writes through (struct is copied into the worker;
	// the test reads results via this reference). Handle runs on the worker thread; Merge runs on the
	// producer thread and must be pure (it does NOT touch this state).
	private sealed class RecordingState {
		public readonly List<(int Key, int Value)> Processed = new();
		public readonly List<Exception> Errors = new();
		public readonly List<Exception?> CompletedWith = new();
	}

	// Replace-style merge: keep the latest value (return incoming).
	private readonly struct LatestHandler : ICompactingWorkerHandler<int, Boxed> {
		private readonly RecordingState _state;
		private readonly TimeSpan _delay;
		private readonly Action<int>? _hook;

		public LatestHandler(RecordingState state, TimeSpan delay, Action<int>? hook) {
			_state = state;
			_delay = delay;
			_hook = hook;
		}

		public Boxed Merge(Boxed current, Boxed incoming) => incoming; // latest-wins, pure

		public void Handle(int key, Boxed value) {
			_hook?.Invoke(key);
			if (_delay > TimeSpan.Zero) Thread.Sleep(_delay);
			lock (_state.Processed) _state.Processed.Add((key, value.Value));
		}

		public void OnError(Exception error) {
			lock (_state.Errors) _state.Errors.Add(error);
		}

		public void OnCompleted(Exception? error) {
			lock (_state.CompletedWith) _state.CompletedWith.Add(error);
		}
	}

	// Reference payload (TValue : class) carrying an int.
	private sealed class Boxed {
		public readonly int Value;
		public Boxed(int value) => Value = value;
	}

	private sealed class Rec : IDisposable {
		private readonly CompactingSpWorker<int, Boxed, LatestHandler> _worker;
		private readonly RecordingState _state = new();

		public Rec(int capacity, TimeSpan delay = default, Action<int>? hook = null) =>
			_worker = new CompactingSpWorker<int, Boxed, LatestHandler>(new LatestHandler(_state, delay, hook), capacity);

		public List<(int Key, int Value)> Processed => _state.Processed;
		public List<Exception> Errors => _state.Errors;
		public List<Exception?> CompletedWith => _state.CompletedWith;
		public Task Completion => _worker.Completion;

		public void Start(CancellationToken ct = default) => _worker.Start(ct);
		public bool Dispatch(int key, int value) => _worker.Dispatch(key, new Boxed(value));
		public bool TryComplete(Exception? ex = null) => _worker.TryComplete(ex);
		public void Dispose() => _worker.Dispose();
	}

	private static int CountOf(Rec w) {
		lock (w.Processed) return w.Processed.Count;
	}

	[Test]
	public void Distinct_Keys_Process_In_First_Seen_Order() {
		using var w = new Rec(16);
		w.Start();
		for (var i = 0; i < 50; i++) Assert.That(w.Dispatch(i, i * 10), Is.True);
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 50);
		Assert.That(w.Processed.Select(p => p.Key), Is.EqualTo(Enumerable.Range(0, 50)));
		Assert.That(w.Processed.All(p => p.Value == p.Key * 10), Is.True);
	}

	[Test]
	public void Capacity_Rounds_Up_To_Power_Of_2() {
		using var w = new Rec(5);
		w.Start();
		for (var i = 0; i < 8; i++) Assert.That(w.Dispatch(i, i), Is.True);
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 8);
		Assert.That(w.Processed, Has.Count.EqualTo(8));
	}

	// The defining behavior: re-dispatching a queued key overrides its value in place, keeps its
	// position, and the consumer sees the LATEST value. Gate the consumer so all updates land while
	// the key is still queued.
	[Test]
	public void Same_Key_Conflates_To_Latest_Keeping_Position() {
		var entered = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		// First item (key 0) blocks in the hook so keys 1 and 2 sit queued while we re-dispatch key 1.
		using var w = new Rec(16, hook: k => {
			if (k == 0) {
				entered.Set();
				gate.Wait();
			}
		});
		w.Start();

		Assert.That(w.Dispatch(0, 0), Is.True);
		Assert.That(entered.Wait(2000), Is.True);
		// Queue (after key 0 picked up): key 1, key 2 — then conflate key 1 three times.
		Assert.That(w.Dispatch(1, 100), Is.True);
		Assert.That(w.Dispatch(2, 200), Is.True);
		Assert.That(w.Dispatch(1, 101), Is.True); // override in place
		Assert.That(w.Dispatch(1, 102), Is.True); // override again

		gate.Set();
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 3);

		// key 1 kept its position (before key 2) and carries the LATEST value 102.
		Assert.That(w.Processed, Is.EqualTo(new[] { (0, 0), (1, 102), (2, 200) }));
	}

	[Test]
	public void Merge_Hook_Combines_Values() {
		// A summing merge to prove Merge's result is what Handle sees (still replace-style: each
		// returned Boxed is self-contained — it is the running total, not a delta).
		var entered = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		var state = new RecordingState();
		var worker = new CompactingSpWorker<int, Boxed, SumHandler>(new SumHandler(state, entered, gate), 16);
		worker.Start();

		Assert.That(worker.Dispatch(0, new Boxed(0)), Is.True); // blocks consumer in hook
		Assert.That(entered.Wait(2000), Is.True);
		Assert.That(worker.Dispatch(7, new Boxed(10)), Is.True);
		Assert.That(worker.Dispatch(7, new Boxed(5)), Is.True);  // merged: 10 + 5
		Assert.That(worker.Dispatch(7, new Boxed(1)), Is.True);  // merged: 15 + 1

		gate.Set();
		worker.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(worker.Completion, () => { lock (state.Processed) return state.Processed.Count; }, 2);

		Assert.That(state.Processed, Does.Contain((7, 16)));
		worker.Dispose();
	}

	// Summing replace-style handler: Merge returns the running total as a fresh self-contained value.
	private readonly struct SumHandler : ICompactingWorkerHandler<int, Boxed> {
		private readonly RecordingState _state;
		private readonly ManualResetEventSlim _entered;
		private readonly ManualResetEventSlim _gate;

		public SumHandler(RecordingState state, ManualResetEventSlim entered, ManualResetEventSlim gate) {
			_state = state;
			_entered = entered;
			_gate = gate;
		}

		public Boxed Merge(Boxed current, Boxed incoming) => new(current.Value + incoming.Value);

		public void Handle(int key, Boxed value) {
			if (key == 0) {
				_entered.Set();
				_gate.Wait();
			}

			lock (_state.Processed) _state.Processed.Add((key, value.Value));
		}

		public void OnError(Exception error) {
			lock (_state.Errors) _state.Errors.Add(error);
		}

		public void OnCompleted(Exception? error) {
			lock (_state.CompletedWith) _state.CompletedWith.Add(error);
		}
	}

	[Test]
	public void TryComplete_Drains_And_Completes_Normally() {
		using var w = new Rec(16);
		w.Start();
		for (var i = 0; i < 40; i++) w.Dispatch(i, i);
		Assert.That(w.TryComplete(), Is.True);
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 40);
		Assert.That(w.Processed, Has.Count.EqualTo(40));
		Assert.That(w.CompletedWith, Has.Count.EqualTo(1));
		Assert.That(w.CompletedWith[0], Is.Null);
	}

	[Test]
	public void Dispatch_After_Complete_Returns_False() {
		using var w = new Rec(8);
		w.Start();
		w.TryComplete();
		Assert.That(w.Completion.Wait(2000), Is.True);
		Assert.That(w.Dispatch(1, 1), Is.False);
	}

	[Test]
	public void Handler_Exception_Is_Reported_To_OnError_And_Worker_Continues() {
		using var w = new Rec(8, hook: k => {
			if (k == 2) throw new InvalidOperationException("handler failed");
		});
		w.Start();
		for (var i = 0; i < 5; i++) w.Dispatch(i, i);
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 4);
		Assert.That(w.Processed.Select(p => p.Key), Is.SupersetOf(new[] { 0, 1, 3, 4 }));
		Assert.That(w.Errors, Has.Count.EqualTo(1));
		Assert.That(w.Errors[0], Is.InstanceOf<InvalidOperationException>());
	}

	[Test]
	public void Cancellation_Stops_And_Completion_Is_Cancelled() {
		using var w = new Rec(16, TimeSpan.FromMilliseconds(50));
		using var cts = new CancellationTokenSource();
		w.Start(cts.Token);
		for (var i = 0; i < 100; i++) w.Dispatch(i, i);
		Thread.Sleep(80);
		cts.Cancel();
		var agg = Assert.Throws<AggregateException>(() => w.Completion.Wait(2000));
		Assert.That(agg!.InnerException, Is.InstanceOf<OperationCanceledException>());
		Assert.That(w.CompletedWith[0], Is.InstanceOf<OperationCanceledException>());
	}

	[Test]
	public void Dispose_Is_Idempotent() {
		var w = new Rec(8);
		w.Start();
		w.Dispose();
		Assert.DoesNotThrow(() => w.Dispose());
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `dotnet test -c Release tests/Nanov.HighPerformance.Tests --filter "FullyQualifiedName~CompactingSpWorkerTests"`
Expected: FAIL to compile — `CompactingSpWorker` does not exist.

- [ ] **Step 3: Create the core `CompactingSpRingCore.cs`**

```csharp
namespace Nanov.HighPerformance.Threading.Workers.Internal;

using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
///   SPSC ring core for the key-conflating worker. A two-array ring (<c>TKey[]</c> + <c>TValue[]</c>)
///   plus a producer-private <see cref="CompactingKeyMap{TKey}" />. Unlike the strict-FIFO
///   <see cref="SpRingCore{T}" />, the producer may write a value slot inside <c>[head, tail)</c> (an
///   in-place override of an already-queued key) — so this is a deliberate SIBLING, not a modification
///   of <see cref="SpRingCore{T}" />, keeping that core's FIFO invariant pristine. Generic over the
///   merge facet <typeparamref name="TMerger" /> only (never <c>Handle</c>), so the consumer run loop
///   lives in the worker and a future async worker can reuse this core.
///   <para>
///     LOSSLESS OVERRIDE PROTOCOL. The override writes the value slot (atomic reference
///     <c>Volatile.Write</c>), issues a StoreLoad fence (<see cref="Interlocked.MemoryBarrier" />), then
///     re-reads <c>Head</c>: if the consumer is strictly behind the slot it will acquire-read the new
///     value; otherwise the producer enqueues a FRESH slot with the un-merged incoming value (a harmless
///     duplicate under replace-style merge). The consumer acquire-reads the value
///     (<c>Volatile.Read</c>) because an override publishes without moving <c>Tail</c>. Single producer
///     (caller contract, DEBUG-guarded); single consumer (the worker's own thread).
///   </para>
/// </summary>
internal sealed class CompactingSpRingCore<TKey, TValue, TMerger> : IDisposable
	where TKey : notnull, IEquatable<TKey>
	where TValue : class
	where TMerger : struct, IItemMerger<TValue> {
	private const int SpinThreshold = 200;

	private readonly TKey[] _keys;
	private readonly TValue?[] _values;
	private readonly int _mask;
	private TMerger _merger;                  // producer-side; pure Merge only
	private CompactingKeyMap<TKey> _pending;  // PRODUCER-THREAD-ONLY: key → seq

	private readonly ManualResetEventSlim _itemEvt = new(false);
	private readonly ManualResetEventSlim _spaceEvt = new(false);
	private readonly TaskCompletionSource _completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
	private readonly Thread _thread;
	private volatile bool _completed;
	private Exception? _completeException;
	private CancellationToken _ct;
	private CancellationTokenRegistration _ctRegistration;
	private bool _disposed;
	private volatile bool _stop;
	private PaddedIndices _idx;               // cache-line-separated Head/Tail (reused from SpRingCore.cs)
	private int _cachedHead;                  // producer-private gating cache (LMAX)
#if DEBUG
	private int _producing;                   // DEBUG-only producer overlap guard
#endif

	internal CompactingSpRingCore(TMerger merger, int capacity, string threadName, ThreadStart runLoop) {
		capacity = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(capacity, 1));
		_keys = new TKey[capacity];
		_values = new TValue?[capacity];
		_mask = capacity - 1;
		_merger = merger;
		_pending = new CompactingKeyMap<TKey>(capacity);
		_thread = new Thread(runLoop) {
			IsBackground = true,
			Name = threadName
		};
	}

	internal Task Completion => _completionTcs.Task;
	internal int QueuedApprox => _idx.Tail - _idx.Head;
	internal int Capacity => _keys.Length;

	internal void Start(CancellationToken ct) {
		_ct = ct;
		if (ct.CanBeCanceled)
			_ctRegistration = ct.Register(static s => ((CompactingSpRingCore<TKey, TValue, TMerger>)s!).SignalStop(), this);
		_thread.Start();
	}

	internal void SignalStop() {
		_stop = true;
		_itemEvt.Set();
		_spaceEvt.Set();
	}

	internal bool TryComplete(Exception? exception) {
		if (_completed) return false;
		_completeException = exception;
		_completed = true;
		_itemEvt.Set();
		return true;
	}

	internal Exception? CompletionException() =>
		_completeException ?? (_ct.IsCancellationRequested ? new OperationCanceledException(_ct) : null);

	internal void SetCompletion(Exception? exception) {
		if (exception is OperationCanceledException)
			_completionTcs.TrySetCanceled(_ct);
		else if (exception is not null)
			_completionTcs.TrySetException(exception);
		else
			_completionTcs.TrySetResult();
		_ctRegistration.Dispose();
	}

	public void Dispose() {
		if (_disposed) return;
		_disposed = true;
		SignalStop();
		try {
			_thread.Join(1000);
		}
		catch {
			/* ignore */
		}

		_itemEvt.Dispose();
		_spaceEvt.Dispose();
		_pending.Dispose();
		GC.SuppressFinalize(this);
	}

	// --- Producer: conflation dispatch -----------------------------------------------------------

	/// <summary>
	///   Conflating enqueue. If the key is still pending, merge in place + fenced recheck (deliver the
	///   merged value, or enqueue fresh on doubt). Otherwise enqueue a fresh slot. False if completed/stopped.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool Dispatch(TKey key, TValue value) {
		DebugBeginProduce();
		try {
			if (_completed || _stop) return false;

			if (_pending.TryGetSeq(key, out var seq) && IsPending(seq)) {
				var slot = seq & _mask;
				var current = Volatile.Read(ref _values[slot])!;
				var merged = _merger.Merge(current, value);
				Volatile.Write(ref _values[slot], merged);
				Interlocked.MemoryBarrier(); // StoreLoad: publish merged before re-reading Head
				if (Volatile.Read(ref _idx.Head) - seq < 0)
					return true; // consumer strictly behind → it will acquire-read merged
				// else: consumer may be at/past the slot → enqueue the un-merged value fresh (harmless dup)
			}

			if (!TryReserve(out var tail)) return false;
			var freshSlot = tail & _mask;
			_keys[freshSlot] = key;
			Volatile.Write(ref _values[freshSlot], value);
			if (_pending.IsFull)
				_pending.Compact(new PendingPredicate(Volatile.Read(ref _idx.Head), tail));
			_pending.Set(key, tail);
			CommitTail(tail);
			return true;
		}
		finally {
			DebugEndProduce();
		}
	}

	// seq is in the live window [Head, Tail). Producer owns Tail (plain read); Head is acquire-read.
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private bool IsPending(int seq) {
		var head = Volatile.Read(ref _idx.Head);
		return seq - head >= 0 && _idx.Tail - seq > 0;
	}

	private readonly struct PendingPredicate : ISeqKeepPredicate {
		private readonly int _head;
		private readonly int _tail;

		internal PendingPredicate(int head, int tail) {
			_head = head;
			_tail = tail;
		}

		public bool Keep(int seq) => seq - _head >= 0 && _tail - seq > 0;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private bool TryReserve(out int tail) {
		if (_completed || _stop) {
			tail = 0;
			return false;
		}

		if (_idx.Tail - _cachedHead == _keys.Length) {
			_cachedHead = Volatile.Read(ref _idx.Head);
			if (_idx.Tail - _cachedHead == _keys.Length && !WaitForSpaceSlow()) {
				tail = 0;
				return false;
			}
		}

		tail = _idx.Tail;
		return true;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void CommitTail(int tail) {
		Volatile.Write(ref _idx.Tail, tail + 1);
		// Edge-triggered: signal only on empty→non-empty. Sync consumer's spin-recheck masks the
		// StoreLoad window (see SpRingCore SIGNALING INVARIANT) — no per-commit fence needed here.
		if (Volatile.Read(ref _idx.Head) == tail)
			_itemEvt.Set();
	}

	// --- Consumer (driven by the worker's run loop) ----------------------------------------------

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryClaimRun(out int head, out int available) {
		var tail = Volatile.Read(ref _idx.Tail);
		if (_idx.Head == tail) {
			if (!WaitForItemSlow()) {
				head = 0;
				available = 0;
				return false;
			}

			tail = Volatile.Read(ref _idx.Tail);
		}

		if (_stop) {
			head = 0;
			available = 0;
			return false;
		}

		head = _idx.Head;
		available = tail;
		return true;
	}

	/// <summary>
	///   Take the (key, value) at <paramref name="head" />, clear the slot, advance + space-signal. The
	///   value is acquire-read because a conflated override publishes it without moving <c>Tail</c>.
	///   False on hard stop (drops the item).
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryTake(int head, out TKey key, out TValue value) {
		if (_stop) {
			key = default!;
			value = default!;
			return false;
		}

		var slot = head & _mask;
		key = _keys[slot];
		value = Volatile.Read(ref _values[slot])!;
		_keys[slot] = default!;
		Volatile.Write(ref _values[slot], null);
		AdvanceHead(head);
		return true;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void AdvanceHead(int head) {
		Volatile.Write(ref _idx.Head, head + 1);
		if (Volatile.Read(ref _idx.Tail) - head == _keys.Length)
			_spaceEvt.Set();
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool WaitForSpaceSlow() {
		var spins = 0;
		int head;
		while (_idx.Tail - (head = Volatile.Read(ref _idx.Head)) == _keys.Length) {
			if (_stop || _completed) return false;
			if (spins++ < SpinThreshold) {
				Thread.SpinWait(10);
				continue;
			}

			_spaceEvt.Wait();
			_spaceEvt.Reset();
			spins = 0;
		}

		_cachedHead = head;
		return true;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool WaitForItemSlow() {
		var spins = 0;
		while (_idx.Head == Volatile.Read(ref _idx.Tail)) {
			if (_stop) return false;
			if (_completed) return _idx.Head != Volatile.Read(ref _idx.Tail);
			if (spins++ < SpinThreshold) {
				Thread.SpinWait(10);
				continue;
			}

			_itemEvt.Wait();
			_itemEvt.Reset();
			spins = 0;
		}

		return true;
	}

	[Conditional("DEBUG")]
	private void DebugBeginProduce() {
#if DEBUG
		if (Interlocked.Exchange(ref _producing, 1) != 0)
			throw new InvalidOperationException(
				"Concurrent producer detected — CompactingSpWorker requires single-producer (non-overlapping) access.");
#endif
	}

	[Conditional("DEBUG")]
	private void DebugEndProduce() {
#if DEBUG
		Volatile.Write(ref _producing, 0);
#endif
	}
}
```

- [ ] **Step 4: Create the public worker `CompactingSpWorker.cs`**

```csharp
namespace Nanov.HighPerformance.Threading.Workers;

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Nanov.HighPerformance.Threading.Workers.Internal;

/// <summary>
///   Single-producer / single-consumer worker that <b>conflates by key</b>: the producer calls
///   <see cref="Dispatch" /> with a <c>(key, value)</c>; if that key is still queued, the value is
///   merged into the queued one IN PLACE (keeping its position) via the handler's
///   <see cref="IItemMerger{TValue}.Merge" />, so the consumer sees the latest value per key. Zero
///   per-message allocation; blocking backpressure when <see cref="Capacity" /> distinct keys are
///   pending. The handler is a <see langword="struct" /> constrained to
///   <see cref="ICompactingWorkerHandler{TKey, TValue}" /> — calls devirtualize and inline.
///   <para>Sync, reference-value variant. Single producer is the caller's contract.</para>
/// </summary>
public sealed class CompactingSpWorker<TKey, TValue, THandler> : IDisposable, IWorkerQueue
	where TKey : notnull, IEquatable<TKey>
	where TValue : class
	where THandler : struct, ICompactingWorkerHandler<TKey, TValue> {
	private readonly CompactingSpRingCore<TKey, TValue, THandler> _core;
	private THandler _handler; // mutable struct — Handle/OnError/OnCompleted go through this field

	public CompactingSpWorker(THandler handler, int capacity, string? threadName = null) {
		_handler = handler;
		// The core gets the handler as its merge facet (TMerger = THandler). Merge is pure, so the core's
		// copy needs no shared mutable state with this worker's _handler (which drives the consumer side).
		_core = new CompactingSpRingCore<TKey, TValue, THandler>(handler, capacity,
			threadName ?? $"CompactingSpWorker<{typeof(TKey).Name},{typeof(TValue).Name}>", RunLoop);
	}

	/// <summary>Completes when the worker has drained and exited (after Complete() or Dispose()).</summary>
	public Task Completion => _core.Completion;

	/// <summary>Approximate distinct keys currently queued (<c>Tail − Head</c>). Lock-free, racy by a few.</summary>
	public int QueuedApprox => _core.QueuedApprox;

	/// <summary>Ring capacity (power of two) = max distinct pending keys.</summary>
	public int Capacity => _core.Capacity;

	public void Start(CancellationToken ct = default) => _core.Start(ct);

	/// <summary>
	///   Enqueue <paramref name="value" /> under <paramref name="key" />, conflating with any still-queued
	///   value for the same key. Returns false if completed/disposed; blocks if <see cref="Capacity" />
	///   distinct keys are already pending.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool Dispatch(TKey key, TValue value) => _core.Dispatch(key, value);

	public bool TryComplete(Exception? exception = null) => _core.TryComplete(exception);

	public void Dispose() => _core.Dispose();

	private void RunLoop() {
		try {
			while (_core.TryClaimRun(out var head, out var available)) {
				while (head != available && _core.TryTake(head++, out var key, out var value)) {
					try {
						_handler.Handle(key, value);
					}
					catch (Exception ex) {
						_handler.OnError(ex);
					}
				}
			}
		}
		finally {
			var ex = _core.CompletionException();
			try {
				_handler.OnCompleted(ex);
			}
			catch (Exception onCompletedEx) {
				_handler.OnError(onCompletedEx);
			}

			_core.SetCompletion(ex);
		}
	}
}
```

- [ ] **Step 5: Run the behavioral tests to verify they pass**

Run: `dotnet test -c Release tests/Nanov.HighPerformance.Tests --filter "FullyQualifiedName~CompactingSpWorkerTests"`
Expected: PASS (all behavioral tests, both TFMs).

- [ ] **Step 6: Full build + full test suite (no regressions, 0 warnings)**

Run: `dotnet build -c Release && dotnet test -c Release`
Expected: build 0 warnings (net9.0 + net10.0); all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/Nanov.HighPerformance/Threading/Workers/Internal/CompactingSpRingCore.cs src/Nanov.HighPerformance/Threading/Workers/CompactingSpWorker.cs tests/Nanov.HighPerformance.Tests/Threading/Workers/CompactingSpWorkerTests.cs
git commit -m "feat(compacting): add CompactingSpWorker + sibling core (conflation, in-place override)"
```

---

### Task 4: Concurrency & stress tests

**Files:**
- Modify: `tests/Nanov.HighPerformance.Tests/Threading/Workers/CompactingSpWorkerTests.cs` (append tests inside the existing fixture)

**Interfaces:**
- Consumes: `CompactingSpWorker`, `Rec`, `RecordingState`, `Boxed`, `LatestHandler`, `CountOf`, `WorkerTestUtil` (Task 3 + existing).

- [ ] **Step 1: Write the stress/concurrency tests (append before the closing `}` of the fixture)**

```csharp
	// Distinct-key flood with a stall watchdog — a dropped wakeup plateaus progress and fails fast.
	[Test]
	public async Task Distinct_Key_Flood_Preserves_Order() {
		using var w = new Rec(16);
		w.Start();
		const int n = 10_000;
		await Task.Run(() => {
			for (var i = 0; i < n; i++) w.Dispatch(i, i);
		});
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), n);
		Assert.That(w.Processed.Select(p => p.Key), Is.EqualTo(Enumerable.Range(0, n)));
		Assert.That(w.Processed.All(p => p.Value == p.Key), Is.True);
	}

	// Lost-update / lost-wakeup stress: a small key space heavily re-dispatched against a slightly slow
	// consumer maximizes the in-place-override race. Guarantee: for every key, the LAST value dispatched
	// is delivered at some point (latest-wins merge). Watchdog catches a hang from a dropped wakeup.
	[TestCase(1)]
	[TestCase(2)]
	[TestCase(8)]
	public async Task Conflation_Stress_Delivers_Latest_Per_Key(int capacity) {
		using var w = new Rec(capacity, hook: _ => Thread.SpinWait(50));
		w.Start();

		const int keys = 4;
		const int rounds = 30_000;
		var lastSent = new int[keys];
		await Task.Run(() => {
			for (var r = 0; r < rounds; r++) {
				var k = r % keys;
				var v = r;
				lastSent[k] = v;
				while (!w.Dispatch(k, v)) { } // single producer
			}
		});
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), keys); // at least one per key

		// For each key, the final delivered value must equal the last value we sent for that key.
		var lastSeen = new Dictionary<int, int>();
		lock (w.Processed)
			foreach (var (k, v) in w.Processed)
				lastSeen[k] = v;
		for (var k = 0; k < keys; k++) {
			Assert.That(lastSeen.ContainsKey(k), Is.True, $"key {k} was never delivered");
			Assert.That(lastSeen[k], Is.EqualTo(lastSent[k]),
				$"key {k}: last delivered {lastSeen[k]} != last sent {lastSent[k]} (lost update)");
		}
	}

	// Backpressure: with capacity distinct keys pending and the consumer gated, a NEW distinct key blocks
	// until space frees; conflating an EXISTING key never blocks.
	[Test]
	public async Task New_Key_Blocks_When_Full_But_Conflation_Does_Not() {
		var entered = new ManualResetEventSlim(false);
		var gate = new ManualResetEventSlim(false);
		using var w = new Rec(2, hook: k => {
			if (k == 0) {
				entered.Set();
				gate.Wait();
			}
		});
		w.Start();

		Assert.That(w.Dispatch(0, 0), Is.True);
		Assert.That(entered.Wait(2000), Is.True); // head advanced to 1; ring (cap 2) can hold 2 more
		Assert.That(w.Dispatch(1, 1), Is.True);
		Assert.That(w.Dispatch(2, 2), Is.True); // ring now full (keys 1,2 pending)

		// Conflating an already-pending key must NOT block.
		var conflate = Task.Run(() => w.Dispatch(1, 100));
		Assert.That(conflate.Wait(200), Is.True, "conflation of a pending key must not block");
		Assert.That(await conflate, Is.True);

		// A NEW distinct key must block until the consumer frees a slot.
		var blocked = Task.Run(() => w.Dispatch(3, 3));
		Assert.That(blocked.Wait(50), Is.False, "new key must block when full");

		gate.Set();
		Assert.That(await blocked, Is.True);
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 4);

		// key 1 delivered its latest (100); key 3 eventually delivered.
		var lastSeen = new Dictionary<int, int>();
		lock (w.Processed)
			foreach (var (k, v) in w.Processed)
				lastSeen[k] = v;
		Assert.That(lastSeen[1], Is.EqualTo(100));
		Assert.That(lastSeen.ContainsKey(3), Is.True);
	}

	// Deterministic wake of a parked consumer (mirrors SpWorkerTests).
	[Test]
	public void Dispatch_Wakes_An_Idle_Parked_Worker() {
		using var w = new Rec(4);
		w.Start();
		Thread.Sleep(150); // exhaust spin budget, park on _itemEvt
		Assert.That(w.Dispatch(1, 10), Is.True);
		WorkerTestUtil.AwaitProgress(() => CountOf(w), 1);
		Thread.Sleep(150);
		Assert.That(w.Dispatch(2, 20), Is.True);
		WorkerTestUtil.AwaitProgress(() => CountOf(w), 2);
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), 2);
		Assert.That(w.Processed, Is.EqualTo(new[] { (1, 10), (2, 20) }));
	}

	// Map eviction: a long flood of one-shot keys must not grow unbounded — the producer compacts. We
	// can't read the map directly (it's producer-private), so assert the run completes and stays correct
	// with far more distinct keys than capacity (forces repeated compaction).
	[Test]
	public async Task Many_One_Shot_Keys_Force_Compaction_And_Stay_Correct() {
		using var w = new Rec(8);
		w.Start();
		const int n = 100_000;
		await Task.Run(() => {
			for (var i = 0; i < n; i++) w.Dispatch(i, i);
		});
		w.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(w.Completion, () => CountOf(w), n);
		Assert.That(w.Processed, Has.Count.EqualTo(n));
		Assert.That(w.Processed.Select(p => p.Key), Is.EqualTo(Enumerable.Range(0, n)));
	}
```

- [ ] **Step 2: Run the new tests (Release) to verify they pass**

Run: `dotnet test -c Release tests/Nanov.HighPerformance.Tests --filter "FullyQualifiedName~CompactingSpWorkerTests"`
Expected: PASS (both TFMs). The stress tests should complete well within their watchdog bounds.

- [ ] **Step 3: Run in Debug to exercise the producer-overlap guard path**

Run: `dotnet test -c Debug tests/Nanov.HighPerformance.Tests --filter "FullyQualifiedName~CompactingSpWorkerTests"`
Expected: PASS (the DEBUG `_producing` guard never trips because every test uses a single producer).

- [ ] **Step 4: Commit**

```bash
git add tests/Nanov.HighPerformance.Tests/Threading/Workers/CompactingSpWorkerTests.cs
git commit -m "test(compacting): add conflation stress, backpressure, wake, and eviction tests"
```

---

### Task 5: Benchmark (conflation vs ConcurrentDictionary+Channel)

**Files:**
- Create: `benchmarks/Nanov.HighPerformance.Benchmarks/CompactingWorkerBenchmarks.cs`

**Interfaces:**
- Consumes: `CompactingSpWorker`, `ICompactingWorkerHandler` (public API).

- [ ] **Step 1: Create the benchmark**

```csharp
namespace Nanov.HighPerformance.Benchmarks;

using System.Collections.Concurrent;
using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using Nanov.HighPerformance.Threading.Workers;

[MemoryDiagnoser]
public class CompactingWorkerBenchmarks {
	private const int Capacity = 1024;

	[Params(100_000)]
	public int Messages;

	[Params(64, 1024)]
	public int KeySpace;

	private sealed class Tick {
		public long Value;
	}

	private readonly struct DropHandler : ICompactingWorkerHandler<int, Tick> {
		public Tick Merge(Tick current, Tick incoming) => incoming; // latest-wins
		public void Handle(int key, Tick value) { _ = value.Value; }
		public void OnError(Exception error) { }
		public void OnCompleted(Exception? error) { }
	}

	[Benchmark(Baseline = true)]
	public async Task ConcurrentDictionary_Plus_Channel() {
		var latest = new ConcurrentDictionary<int, Tick>();
		var ch = Channel.CreateBounded<int>(new BoundedChannelOptions(Capacity) {
			SingleReader = true,
			SingleWriter = true
		});
		var consumer = Task.Run(async () => {
			await foreach (var key in ch.Reader.ReadAllAsync().ConfigureAwait(false))
				if (latest.TryRemove(key, out var t)) _ = t.Value;
		});

		for (var i = 0; i < Messages; i++) {
			var key = i % KeySpace;
			latest[key] = new Tick { Value = i };
			await ch.Writer.WriteAsync(key).ConfigureAwait(false);
		}

		ch.Writer.Complete();
		await consumer.ConfigureAwait(false);
	}

	[Benchmark]
	public void CompactingSpWorker() {
		using var w = new CompactingSpWorker<int, Tick, DropHandler>(new DropHandler(), Capacity);
		w.Start();
		for (var i = 0; i < Messages; i++)
			w.Dispatch(i % KeySpace, new Tick { Value = i });
		w.TryComplete();
		w.Completion.Wait();
	}
}
```

- [ ] **Step 2: Build the benchmark project (compile check, don't run the full suite)**

Run: `dotnet build -c Release benchmarks/Nanov.HighPerformance.Benchmarks/Nanov.HighPerformance.Benchmarks.csproj`
Expected: build succeeds, 0 warnings.

- [ ] **Step 3: Smoke-run the benchmark filter (optional, longer)**

Run: `dotnet run -c Release --project benchmarks/Nanov.HighPerformance.Benchmarks -- --filter '*CompactingWorkerBenchmarks*'`
Expected: BenchmarkDotNet completes; the `CompactingSpWorker` row shows lower allocation than the baseline (conflation drops per-message `Channel`/dictionary overhead).

- [ ] **Step 4: Commit**

```bash
git add benchmarks/Nanov.HighPerformance.Benchmarks/CompactingWorkerBenchmarks.cs
git commit -m "bench(compacting): add conflation benchmark vs ConcurrentDictionary+Channel"
```

---

### Task 6: Documentation — Threading README + scoped CLAUDE.md

**Files:**
- Modify: `src/Nanov.HighPerformance/Threading/README.md` (add a `CompactingSpWorker` section)
- Modify: `src/Nanov.HighPerformance/Threading/CLAUDE.md` (add agent notes for the compacting variant + its invariant)

**Interfaces:** none (docs).

- [ ] **Step 1: Add a `CompactingSpWorker` section to `README.md`**

Read the existing `README.md` structure first, then add a section (matching the file's existing heading style and tone) covering:
- What it is: a key-conflating SPSC worker — `Dispatch(key, value)`; same-key still-queued → in-place override keeping position; consumer sees latest per key.
- The handler: `ICompactingWorkerHandler<TKey,TValue> : IItemMerger<TValue>` — `Merge` (producer thread, pure, replace-style), `Handle(TKey,TValue)`, `OnError`, `OnCompleted`.
- Constraints: `TKey : notnull, IEquatable<TKey>`, `TValue : class`; capacity = max distinct pending keys.
- A short usage example mirroring the README's existing example style.

- [ ] **Step 2: Add agent notes to `CLAUDE.md`**

Add a subsection capturing the non-obvious invariants an editor must preserve:
- The lossless override protocol (store value → `Interlocked.MemoryBarrier` → recheck `Head`; enqueue fresh when `Head − seq >= 0`); consumer **acquire-reads** the value slot (override publishes without moving `Tail`).
- `CompactingSpRingCore` is a **sibling** of `SpRingCore` (producer writes inside `[head, tail)`) — do not fold it into `SpRingCore`; keep that core's FIFO invariant pristine.
- The map (`CompactingKeyMap`) is **producer-thread-only**; never read/write it from the consumer.
- `Merge` runs on the **producer** thread and must be pure/replace-style; additive merge is unsafe (would double-count on the duplicate fallback).
- Core is generic over the **merger facet** (`TMerger`), not `THandler`, so a future `AsyncCompactingSpWorker` reuses it; `Handle` stays in the worker.

- [ ] **Step 3: Verify the docs build isn't affected**

Run: `dotnet build -c Release`
Expected: 0 warnings (docs are not compiled, but confirms nothing else broke).

- [ ] **Step 4: Commit**

```bash
git add src/Nanov.HighPerformance/Threading/README.md src/Nanov.HighPerformance/Threading/CLAUDE.md
git commit -m "docs(compacting): document CompactingSpWorker in Threading README + CLAUDE.md"
```

---

## Self-Review

**Spec coverage:**
- API `CompactingSpWorker<TKey,TValue,THandler>` + `Dispatch(key,value)` → Task 3. ✓
- `IItemMerger` + handler inherits it → Task 1. ✓
- `TKey : notnull, IEquatable<TKey>`, `TValue : class` → Tasks 1 & 3 (constraints). ✓
- Producer-private `CompactingKeyMap` (purpose-built, `int` seq, `Compact`) → Task 2. ✓
- Sibling core generic over the merger facet, direct devirtualized `Merge`, `Handle` in the worker → Task 3. ✓
- Lossless in-place override (fence + recheck + enqueue-fresh) → Task 3 `Dispatch`; verified Task 4 `Conflation_Stress_Delivers_Latest_Per_Key`. ✓
- Keep-original-position conflation → Task 3 `Same_Key_Conflates_To_Latest_Keeping_Position`. ✓
- Merge replace-style + result delivered → Task 3 `Merge_Hook_Combines_Values`. ✓
- Capacity = max distinct pending keys; backpressure → Task 4 `New_Key_Blocks_When_Full_But_Conflation_Does_Not`. ✓
- Map eviction / steady bounded → Task 4 `Many_One_Shot_Keys_Force_Compaction_And_Stay_Correct`. ✓
- Lifecycle (complete-drains, cancel, OnError, dispose-idempotent) → Task 3. ✓
- Benchmark vs ConcurrentDictionary+Channel → Task 5. ✓
- Docs → Task 6. ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code; every command has expected output. The one scaffold line is explicitly removed in Task 3 Step 5. ✓

**Type consistency:** `TryGetSeq`/`Set`/`Compact`/`IsFull`/`Count`/`Dispose` (map) used identically in Task 2 and the core in Task 3. `Dispatch`/`TryClaimRun`/`TryTake`/`CompletionException`/`SetCompletion` (core) match their use in the worker `RunLoop`. `ISeqKeepPredicate.Keep(int)` matches `PendingPredicate` and the test predicates. `IItemMerger<TValue>.Merge(TValue,TValue)` matches all handler implementations. ✓
