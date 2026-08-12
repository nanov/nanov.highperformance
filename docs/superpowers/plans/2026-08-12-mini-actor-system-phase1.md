# Mini Actor System — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A key-ordered message processor with one worker — single producer dispatches `(key, message)`, one worker drains each key's mailbox exclusively and in order — with the full ownership protocol, backpressure, and termination detection in place.

**Architecture:** One pre-allocated slab carved into fixed chunks; each live key rents a chunk and uses it as a bounded SPSC mailbox. A per-key cell holds `{ Key, State, ChunkBase, Head, Tail, CachedHead }`; a 3-state machine (`Free`/`Scheduled`/`Owned`) on `State` arbitrates exclusive ownership, released via a Dekker handshake against the producer. The scheduler carries **key indices**, never messages.

**Tech Stack:** C# / .NET, multi-targeting net9.0 + net10.0, NUnit 4, `Interlocked`/`Volatile` primitives only (no locks, no `Channel`, no LINQ).

**Spec:** `docs/superpowers/specs/2026-08-12-mini-actor-system-design.md`. Section references below (§4.1, §5.3, …) point into it.

**Scope:** This plan is §13 steps 1–3 only. Steps 4–5 (routed and work-stealing schedulers) and step 6 (benchmark harness) get their own plans once this one is green — they build on this substrate unchanged. Phase 1 delivers working, testable software on its own.

## Global Constraints

- **Never add `Version=` to a `.csproj`.** All package versions live in `Directory.Packages.props` as `<PackageVersion>`. Every needed package is already listed there.
- **Never set a version property in a project.** `VersionPrefix` is central, in `Directory.Build.props`.
- **`TreatWarningsAsErrors=true`** and `EnforceCodeStyleInBuild=true` are inherited. A warning fails the build. `Nullable` and `ImplicitUsings` are enabled; `AllowUnsafeBlocks` is on.
- **Style, from `.editorconfig`:** tabs, `tab_width = 2`; file-scoped namespaces; **`using` directives go INSIDE the namespace**; K&R braces (open brace on the same line); no braces for single-line statements; `var` everywhere; expression-bodied members when they fit on one line; no `this.` qualifier; `_camelCase` private fields, `IPascalCase` interfaces, `TPascalCase` type parameters.
- **Assembly and root namespace:** `Nanov.HighPerformance.Threading.Actors`, even though the folder lives under `src/experiments/`. Internals go in `Nanov.HighPerformance.Threading.Actors.Internal`.
- **Target frameworks:** `net9.0;net10.0` on every new project.
- **Tests are NUnit 4.** Test files live under `tests/Nanov.HighPerformance.Tests/Threading/Actors/`.
- **Handlers are structs** constrained to the handler interface, so calls devirtualize. Never store a handler as an interface reference.
- **Test handler pattern:** a struct handler wrapping a **shared reference-type state object**. The struct is copied into the system; you observe results through the reference. Mirror `SpWorkerTests`'s `Rec`/`RecordingState` pattern.
- **Concurrency tests must use the stall watchdogs** in `tests/Nanov.HighPerformance.Tests/Threading/Workers/WorkerTestUtil.cs` (`AwaitCompletionOrStall`, `AwaitProgress`) so a lost wakeup fails fast instead of hanging CI.
- **Verify commands:** `dotnet build -c Release` (both TFMs, 0 warnings) and `dotnet test -c Release`. Concurrency work must also pass `dotnet test -c Debug`.

---

## File Structure

| File | Responsibility |
| --- | --- |
| `src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/Nanov.HighPerformance.Threading.Actors.csproj` | project definition |
| `…/Internal/Chunk.cs` | the `Chunk.None` sentinel — non-generic so both generic types can use it |
| `…/Internal/ChunkSlab.cs` | the message slab: rent/return chunks, slot access. Producer-private free list, no atomics |
| `…/Internal/KeyCell.cs` | the per-key cell struct + `CellState` constants |
| `…/Internal/KeyCellMap.cs` | producer-private open-addressing map `TKey → cell index` (fork of `CoalescingKeyMap`) |
| `…/Internal/KeyRing.cs` | bounded SPSC ring of `int` key indices — producer → worker |
| `…/IActorHandler.cs` | the public handler interface |
| `…/ActorSystem.cs` | the system: dispatch, worker loop, ownership protocol, lifecycle |
| `tests/…/Threading/Actors/ChunkSlabTests.cs` | Task 2 |
| `tests/…/Threading/Actors/KeyCellMapTests.cs` | Task 3 |
| `tests/…/Threading/Actors/KeyRingTests.cs` | Task 4 |
| `tests/…/Threading/Actors/ActorSystemTests.cs` | Tasks 5–10 behaviour |
| `tests/…/Threading/Actors/ActorSystemStressTests.cs` | Task 11 race guards |
| `tests/…/Threading/Actors/ActorTestUtil.cs` | shared recording handler |

`ActorSystem.cs` will be the largest file. Keep the producer path, the worker path, and the lifecycle path as clearly separated regions; if it exceeds ~450 lines, split the worker loop into `ActorSystem.Worker.cs` as a partial.

---

### Task 1: Project scaffolding

**Files:**
- Create: `src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/Nanov.HighPerformance.Threading.Actors.csproj`
- Create: `src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/Internal/Chunk.cs`
- Modify: `Nanov.HighPerformance.slnx`
- Modify: `tests/Nanov.HighPerformance.Tests/Nanov.HighPerformance.Tests.csproj`
- Test: `tests/Nanov.HighPerformance.Tests/Threading/Actors/ChunkSlabTests.cs`

**Interfaces:**
- Produces: assembly `Nanov.HighPerformance.Threading.Actors`; `internal static class Chunk` with `internal const int None = -1;`

- [ ] **Step 1: Create the project file**

Create `src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/Nanov.HighPerformance.Threading.Actors.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFrameworks>net9.0;net10.0</TargetFrameworks>
    <RootNamespace>Nanov.HighPerformance.Threading.Actors</RootNamespace>
    <AssemblyName>Nanov.HighPerformance.Threading.Actors</AssemblyName>
    <IsAotCompatible>true</IsAotCompatible>
    <GenerateDocumentationFile>true</GenerateDocumentationFile>
    <IsPackable>false</IsPackable>
  </PropertyGroup>

  <ItemGroup>
    <InternalsVisibleTo Include="Nanov.HighPerformance.Tests" />
  </ItemGroup>

</Project>
```

`IsPackable=false` is deliberate: §2 of the spec defers packaging until B-vs-D is measured.

- [ ] **Step 2: Create the chunk sentinel**

Create `…/Internal/Chunk.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Actors.Internal;

/// <summary>
///   Non-generic home for the "no chunk bound" sentinel, so the chunk slab (generic over the message
///   type) and the key cell (generic over the key type) can share it without coupling.
/// </summary>
internal static class Chunk {
	/// <summary>No chunk is bound to this cell.</summary>
	internal const int None = -1;
}
```

- [ ] **Step 3: Register the project in the solution**

In `Nanov.HighPerformance.slnx`, add a folder entry. The file becomes:

```xml
<Solution>
  <Folder Name="/benchmarks/">
    <Project Path="benchmarks/Nanov.HighPerformance.Benchmarks/Nanov.HighPerformance.Benchmarks.csproj" />
  </Folder>
  <Folder Name="/perf-tests/">
    <Project Path="perf-tests/Nanov.HighPerformance.Threading.Workers.PerfTests/Nanov.HighPerformance.Threading.Workers.PerfTests.csproj" />
  </Folder>
  <Folder Name="/src/">
    <Project Path="src/Nanov.HighPerformance.Threading.Workers/Nanov.HighPerformance.Threading.Workers.csproj" />
  </Folder>
  <Folder Name="/src/experiments/">
    <Project Path="src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/Nanov.HighPerformance.Threading.Actors.csproj" />
  </Folder>
  <Folder Name="/tests/">
    <Project Path="tests/Nanov.HighPerformance.Tests/Nanov.HighPerformance.Tests.csproj" />
  </Folder>
</Solution>
```

- [ ] **Step 4: Reference it from the test project**

In `tests/Nanov.HighPerformance.Tests/Nanov.HighPerformance.Tests.csproj`, add to the existing `ItemGroup` containing `ProjectReference`:

```xml
    <ProjectReference Include="../../src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/Nanov.HighPerformance.Threading.Actors.csproj" />
```

- [ ] **Step 5: Write a scaffolding smoke test**

Create `tests/Nanov.HighPerformance.Tests/Threading/Actors/ChunkSlabTests.cs`:

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Actors;

using Nanov.HighPerformance.Threading.Actors.Internal;
using NUnit.Framework;

[TestFixture]
public class ChunkSlabTests {
	[Test]
	public void Chunk_None_Is_Negative_One() =>
		Assert.That(Chunk.None, Is.EqualTo(-1));
}
```

- [ ] **Step 6: Build and run**

Run: `dotnet build -c Release`
Expected: succeeds for net9.0 and net10.0, **0 warnings**.

Run: `dotnet test -c Release --filter "FullyQualifiedName~Threading.Actors"`
Expected: 1 test passes per TFM (2 total).

- [ ] **Step 7: Commit**

```bash
git add src/experiments Nanov.HighPerformance.slnx tests/Nanov.HighPerformance.Tests
git commit -m "feat(actors): scaffold Threading.Actors project"
```

---

### Task 2: ChunkSlab

The message store. One flat `T[]` carved into fixed-size chunks; a producer-private intrusive free list of chunk indices. **No atomics** — §4.1 of the spec: both rent and return happen on the producer thread.

**Files:**
- Create: `…/Internal/ChunkSlab.cs`
- Test: `tests/…/Threading/Actors/ChunkSlabTests.cs` (extend)

**Interfaces:**
- Consumes: `Chunk.None` (Task 1)
- Produces:
  - `internal sealed class ChunkSlab<T>`
  - `internal ChunkSlab(int chunkCount, int chunkSize)` — both rounded up to powers of two
  - `internal int ChunkSize { get; }`, `internal int ChunkMask { get; }`, `internal int ChunkCount { get; }`
  - `internal int FreeCount { get; }`
  - `internal bool TryRent(out int chunk)`
  - `internal void Return(int chunk)`
  - `internal ref T SlotRef(int chunk, int indexInChunk)`
  - `internal Span<T> ChunkSpan(int chunk)`

- [ ] **Step 1: Write the failing tests**

Replace the body of `tests/…/Threading/Actors/ChunkSlabTests.cs` with:

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Actors;

using Nanov.HighPerformance.Threading.Actors.Internal;
using NUnit.Framework;

[TestFixture]
public class ChunkSlabTests {
	[Test]
	public void Chunk_None_Is_Negative_One() =>
		Assert.That(Chunk.None, Is.EqualTo(-1));

	[Test]
	public void Rounds_Capacities_Up_To_Powers_Of_Two() {
		var slab = new ChunkSlab<int>(chunkCount: 5, chunkSize: 3);
		Assert.Multiple(() => {
			Assert.That(slab.ChunkCount, Is.EqualTo(8));
			Assert.That(slab.ChunkSize, Is.EqualTo(4));
			Assert.That(slab.ChunkMask, Is.EqualTo(3));
		});
	}

	[Test]
	public void Rents_Every_Chunk_Then_Fails() {
		var slab = new ChunkSlab<int>(chunkCount: 4, chunkSize: 4);
		Assert.That(slab.FreeCount, Is.EqualTo(4));

		for (var i = 0; i < 4; i++)
			Assert.That(slab.TryRent(out _), Is.True, $"rent {i}");

		Assert.Multiple(() => {
			Assert.That(slab.FreeCount, Is.EqualTo(0));
			Assert.That(slab.TryRent(out _), Is.False);
		});
	}

	[Test]
	public void Rents_Distinct_Chunks() {
		var slab = new ChunkSlab<int>(chunkCount: 4, chunkSize: 4);
		var seen = new HashSet<int>();
		while (slab.TryRent(out var chunk))
			Assert.That(seen.Add(chunk), Is.True, $"chunk {chunk} handed out twice");

		Assert.That(seen, Has.Count.EqualTo(4));
	}

	[Test]
	public void Returned_Chunk_Is_Rentable_Again() {
		var slab = new ChunkSlab<int>(chunkCount: 2, chunkSize: 4);
		Assert.That(slab.TryRent(out var a), Is.True);
		Assert.That(slab.TryRent(out _), Is.True);
		Assert.That(slab.TryRent(out _), Is.False);

		slab.Return(a);
		Assert.Multiple(() => {
			Assert.That(slab.FreeCount, Is.EqualTo(1));
			Assert.That(slab.TryRent(out var again), Is.True);
			Assert.That(again, Is.EqualTo(a));
		});
	}

	[Test]
	public void Slots_Of_Different_Chunks_Do_Not_Alias() {
		var slab = new ChunkSlab<int>(chunkCount: 4, chunkSize: 4);
		Assert.That(slab.TryRent(out var a), Is.True);
		Assert.That(slab.TryRent(out var b), Is.True);

		for (var i = 0; i < slab.ChunkSize; i++) {
			slab.SlotRef(a, i) = 100 + i;
			slab.SlotRef(b, i) = 200 + i;
		}

		for (var i = 0; i < slab.ChunkSize; i++)
			Assert.Multiple(() => {
				Assert.That(slab.SlotRef(a, i), Is.EqualTo(100 + i));
				Assert.That(slab.SlotRef(b, i), Is.EqualTo(200 + i));
			});
	}

	[Test]
	public void ChunkSpan_Covers_Exactly_That_Chunk() {
		var slab = new ChunkSlab<int>(chunkCount: 4, chunkSize: 4);
		Assert.That(slab.TryRent(out var a), Is.True);
		for (var i = 0; i < slab.ChunkSize; i++)
			slab.SlotRef(a, i) = 7;

		var span = slab.ChunkSpan(a);
		Assert.That(span.Length, Is.EqualTo(slab.ChunkSize));
		foreach (var v in span)
			Assert.That(v, Is.EqualTo(7));

		span.Clear();
		Assert.That(slab.SlotRef(a, 0), Is.EqualTo(0));
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test -c Release --filter "FullyQualifiedName~ChunkSlabTests"`
Expected: compile error — `ChunkSlab<>` does not exist.

- [ ] **Step 3: Implement `ChunkSlab`**

Create `…/Internal/ChunkSlab.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Actors.Internal;

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

/// <summary>
///   The message store: one pre-allocated <typeparamref name="T" /> array carved into fixed-size
///   chunks. A chunk is lent to one key for as long as that key has messages, and is used as that
///   key's bounded mailbox.
///   <para>
///     The free list is a <b>producer-private intrusive stack</b> of chunk indices — a plain head
///     plus a <c>next</c> array, with <b>no interlocked operations</b>. Both rent and return happen
///     on the producer thread (the worker never touches <c>ChunkBase</c>), so there is exactly one
///     reader and one writer and they are the same thread. Do not "make this thread-safe": if a
///     second thread ever needs to return a chunk, the fix is to route it through the producer, not
///     to add a CAS.
///   </para>
/// </summary>
internal sealed class ChunkSlab<T> {
	private readonly T[] _slots;
	private readonly int[] _nextFree; // intrusive free-list links, indexed by chunk
	private readonly int _chunkSize;
	private readonly int _chunkMask;
	private readonly int _chunkShift;
	private readonly int _chunkCount;
	private int _freeHead;
	private int _freeCount;

	/// <param name="chunkCount">Max concurrently-dirty keys. Rounded up to a power of two.</param>
	/// <param name="chunkSize">Messages a single key may hold. Rounded up to a power of two.</param>
	internal ChunkSlab(int chunkCount, int chunkSize) {
		_chunkCount = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(chunkCount, 1));
		_chunkSize = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(chunkSize, 1));
		_chunkMask = _chunkSize - 1;
		_chunkShift = BitOperations.TrailingZeroCount(_chunkSize);

		var total = (long)_chunkCount * _chunkSize;
		if (total > int.MaxValue)
			throw new ArgumentOutOfRangeException(nameof(chunkCount), "chunkCount * chunkSize overflows int");

		_slots = new T[(int)total];
		_nextFree = new int[_chunkCount];

		// Seed the free list with every chunk, ascending so the first rent hands out chunk 0.
		for (var i = 0; i < _chunkCount; i++)
			_nextFree[i] = i - 1;
		_freeHead = _chunkCount - 1;
		_freeCount = _chunkCount;
	}

	internal int ChunkSize => _chunkSize;

	/// <summary>Mask for turning an absolute mailbox cursor into an in-chunk slot index.</summary>
	internal int ChunkMask => _chunkMask;

	internal int ChunkCount => _chunkCount;

	/// <summary>Chunks currently on the free list. Producer-thread view; not thread-safe.</summary>
	internal int FreeCount => _freeCount;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryRent(out int chunk) {
		var head = _freeHead;
		if (head < 0) {
			chunk = Chunk.None;
			return false;
		}

		_freeHead = _nextFree[head];
		_freeCount--;
		chunk = head;
		return true;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void Return(int chunk) {
		_nextFree[chunk] = _freeHead;
		_freeHead = chunk;
		_freeCount++;
	}

	/// <summary>
	///   Ref to a slot. <paramref name="indexInChunk" /> must already be masked with
	///   <see cref="ChunkMask" />; the access is unchecked (safe by construction — power-of-two sizes).
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal ref T SlotRef(int chunk, int indexInChunk) =>
		ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_slots),
			(nint)(uint)((chunk << _chunkShift) + indexInChunk));

	/// <summary>The whole chunk, for the cold reclaim walk (§4.6).</summary>
	internal Span<T> ChunkSpan(int chunk) =>
		_slots.AsSpan(chunk * _chunkSize, _chunkSize);
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test -c Release --filter "FullyQualifiedName~ChunkSlabTests"`
Expected: all 7 tests PASS on both TFMs (14 total).

- [ ] **Step 5: Build clean**

Run: `dotnet build -c Release`
Expected: 0 warnings.

- [ ] **Step 6: Commit**

```bash
git add src/experiments tests/Nanov.HighPerformance.Tests
git commit -m "feat(actors): ChunkSlab with producer-private free list"
```

---

### Task 3: KeyCell and KeyCellMap

A fork of `CoalescingKeyMap` (`src/Nanov.HighPerformance.Threading.Workers/Internal/CoalescingKeyMap.cs`) — **read it first**, the probe, `Place` and `EvictFree` shapes carry over almost verbatim. Two differences: the cell carries mailbox cursors instead of a value, and eviction is gated on `State == Free` **and** `ChunkBase == Chunk.None` (§5.5: a cell holding storage is never evictable).

**Files:**
- Create: `…/Internal/KeyCell.cs`
- Create: `…/Internal/KeyCellMap.cs`
- Test: `tests/…/Threading/Actors/KeyCellMapTests.cs`

**Interfaces:**
- Consumes: `Chunk.None` (Task 1)
- Produces:
  - `internal struct KeyCell<TKey>` with public fields `Key`, `State`, `ChunkBase`, `Head`, `Tail`, `CachedHead`
  - `internal static class CellState` with `Free = 0`, `Scheduled = 1`, `Owned = 2`, `Reclaiming = 3`
  - `internal sealed class KeyCellMap<TKey> where TKey : notnull, IEquatable<TKey>`
  - `internal KeyCellMap(int capacity)`, `internal int Count`, `internal int SlotCount`
  - `internal ref KeyCell<TKey> RefAt(int index)`
  - `internal ref KeyCell<TKey> GetOrAddRef(TKey key, out bool existing, out int index)`
  - `internal bool IsOccupied(int slot)`

> **Build note.** `KeyCell.cs` (Step 1) `cref`s `KeyCellMap{TKey}`, which Step 4 creates. Because
> `GenerateDocumentationFile=true` and warnings are errors, building between Step 1 and Step 4 fails
> with **CS1574** — that is expected and is not a defect. Create both files before building. Never
> silence CS1574 with `NoWarn` or an `.editorconfig` suppression; if a `cref` must point at something
> that will not exist, reword the comment to plain text instead.

- [ ] **Step 1: Write the cell and state constants**

Create `…/Internal/KeyCell.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Actors.Internal;

/// <summary>
///   A key's durable record. Lives inline in <see cref="KeyCellMap{TKey}" />'s array and is reached by
///   a <b>stable slot index</b> — a placed cell never moves, so a held index stays valid.
///   <para>
///     Access discipline (do not violate — it is what keeps the map probe race-free):
///     <list type="bullet">
///       <item><c>Key</c>, <c>ChunkBase</c>, <c>CachedHead</c> — <b>producer only</b>.</item>
///       <item><c>Tail</c> — <b>producer only</b>.</item>
///       <item><c>Head</c> — <b>the current owner only</b>.</item>
///       <item><c>State</c> — producer and workers, always via <c>Interlocked</c>.</item>
///     </list>
///   </para>
///   Layout is array-of-structs with no padding on purpose: the worker touches the cell only once per
///   batch (publishing <c>Head</c>) and once per activation (<c>State</c>) — its per-message work is
///   entirely inside the chunk — so false sharing amortizes over the batch.
/// </summary>
internal struct KeyCell<TKey> {
	public TKey Key;
	public int State;
	public int ChunkBase;
	public int Head;
	public int Tail;
	public int CachedHead;
}

/// <summary>
///   The cell ownership states. <c>Free</c> MUST be 0 — <see cref="KeyCellMap{TKey}" />'s eviction
///   sweep keys off it, exactly as <c>CoalescingKeyMap</c> does.
/// </summary>
internal static class CellState {
	/// <summary>Nobody holds the key and it has no queued messages.</summary>
	internal const int Free = 0;

	/// <summary>The key's index is sitting in a scheduler queue.</summary>
	internal const int Scheduled = 1;

	/// <summary>A worker holds the key exclusively and is draining it.</summary>
	internal const int Owned = 2;

	/// <summary>Producer-only, cold: the producer is harvesting this cell's chunk (§4.4).</summary>
	internal const int Reclaiming = 3;
}
```

- [ ] **Step 2: Write the failing map tests**

Create `tests/…/Threading/Actors/KeyCellMapTests.cs`:

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Actors;

using Nanov.HighPerformance.Threading.Actors.Internal;
using NUnit.Framework;

[TestFixture]
public class KeyCellMapTests {
	[Test]
	public void Inserts_Then_Finds_The_Same_Cell() {
		var map = new KeyCellMap<int>(16);

		ref var first = ref map.GetOrAddRef(42, out var existing, out var index);
		Assert.That(existing, Is.False);
		first.Tail = 7;

		ref var again = ref map.GetOrAddRef(42, out var existingAgain, out var indexAgain);
		var tail = again.Tail;

		Assert.Multiple(() => {
			Assert.That(existingAgain, Is.True);
			Assert.That(indexAgain, Is.EqualTo(index));
			Assert.That(tail, Is.EqualTo(7));
		});
	}

	[Test]
	public void Fresh_Cell_Starts_Free_With_No_Chunk() {
		var map = new KeyCellMap<int>(16);
		ref var cell = ref map.GetOrAddRef(1, out _, out _);

		var state = cell.State;
		var chunkBase = cell.ChunkBase;
		var head = cell.Head;
		var tail = cell.Tail;
		var cachedHead = cell.CachedHead;

		Assert.Multiple(() => {
			Assert.That(state, Is.EqualTo(CellState.Free));
			Assert.That(chunkBase, Is.EqualTo(Chunk.None));
			Assert.That(head, Is.EqualTo(0));
			Assert.That(tail, Is.EqualTo(0));
			Assert.That(cachedHead, Is.EqualTo(0));
		});
	}

	[Test]
	public void RefAt_Returns_The_Live_Cell() {
		var map = new KeyCellMap<int>(16);
		map.GetOrAddRef(5, out _, out var index).Tail = 3;

		Assert.That(map.RefAt(index).Tail, Is.EqualTo(3));
		map.RefAt(index).Tail = 9;
		Assert.That(map.GetOrAddRef(5, out _, out _).Tail, Is.EqualTo(9));
	}

	[Test]
	public void Distinct_Keys_Get_Distinct_Indices() {
		var map = new KeyCellMap<int>(64);
		var seen = new HashSet<int>();
		for (var k = 0; k < 32; k++) {
			map.GetOrAddRef(k, out var existing, out var index);
			Assert.That(existing, Is.False, $"key {k}");
			Assert.That(seen.Add(index), Is.True, $"key {k} reused index {index}");
		}

		Assert.That(map.Count, Is.EqualTo(32));
	}

	[Test]
	public void Evicts_Free_Unbound_Cells_When_Table_Fills() {
		var map = new KeyCellMap<int>(4); // 8 slots

		// Fill with drained cells (Free + no chunk) — all evictable.
		for (var k = 0; k < 8; k++)
			map.GetOrAddRef(k, out _, out _);

		// A ninth distinct key forces the cold path; it must evict rather than throw.
		map.GetOrAddRef(999, out var existing, out _);
		Assert.That(existing, Is.False);
	}

	[Test]
	public void Never_Evicts_A_Cell_That_Still_Holds_A_Chunk() {
		var map = new KeyCellMap<int>(4); // 8 slots

		for (var k = 0; k < 8; k++) {
			ref var cell = ref map.GetOrAddRef(k, out _, out _);
			cell.ChunkBase = k; // Free, but storage still bound → not evictable
		}

		Assert.Throws<InvalidOperationException>(() => map.GetOrAddRef(999, out _, out _));
	}

	[Test]
	public void Never_Evicts_A_Non_Free_Cell() {
		var map = new KeyCellMap<int>(4); // 8 slots

		for (var k = 0; k < 8; k++) {
			ref var cell = ref map.GetOrAddRef(k, out _, out _);
			cell.State = CellState.Owned;
		}

		Assert.Throws<InvalidOperationException>(() => map.GetOrAddRef(999, out _, out _));
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `dotnet test -c Release --filter "FullyQualifiedName~KeyCellMapTests"`
Expected: compile error — `KeyCellMap<>` does not exist.

- [ ] **Step 4: Implement `KeyCellMap`**

Create `…/Internal/KeyCellMap.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Actors.Internal;

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

/// <summary>
///   Producer-private, <b>fixed-capacity, no-relocation</b> open-addressing (linear-probe) map
///   <c>TKey → </c><see cref="KeyCell{TKey}" />. Cells live inline and are reached by a <b>stable slot
///   index</b> (which is what the scheduler carries): a placed cell never moves, so a held index stays
///   valid. Eviction is an in-place tombstone, never a relocation.
///   <para>
///     Structural operations (probe / insert / evict) are <b>producer-only</b>. Workers reach a cell
///     by index via <see cref="RefAt" /> and touch only <c>State</c> and <c>Head</c> — never the map
///     metadata — so the probe can never race a key clear.
///   </para>
///   Forked from <c>CoalescingKeyMap</c> in the Workers assembly; the probe and eviction shapes are
///   deliberately identical.
/// </summary>
internal sealed class KeyCellMap<TKey>
	where TKey : notnull, IEquatable<TKey> {
	private const byte Empty = 0;
	private const byte Occupied = 1;
	private const byte Tombstone = 2;

	private readonly KeyCell<TKey>[] _cells;
	private readonly byte[] _control;
	private readonly int _mask;
	private int _count;

	/// <param name="capacity">Max live keys. Slots are sized to ~2× so the load factor stays low.</param>
	internal KeyCellMap(int capacity) {
		var slots = (int)BitOperations.RoundUpToPowerOf2((uint)(Math.Max(capacity, 4) * 2));
		_cells = new KeyCell<TKey>[slots];
		_control = new byte[slots];
		_mask = slots - 1;
	}

	internal int Count => _count;
	internal int SlotCount => _cells.Length;

	internal bool IsOccupied(int slot) => _control[slot] == Occupied;

	/// <summary>Ref to the cell at a stable slot <paramref name="index" /> — the worker's path.</summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal ref KeyCell<TKey> RefAt(int index) =>
		ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_cells), (nint)(uint)index);

	/// <summary>
	///   Get the cell for <paramref name="key" /> by ref, or insert a fresh one (<c>Free</c>, no chunk,
	///   cursors zeroed). <paramref name="index" /> is the stable slot handle. Producer-only.
	/// </summary>
	internal ref KeyCell<TKey> GetOrAddRef(TKey key, out bool existing, out int index) {
		var mask = _mask;
		ref var cells = ref MemoryMarshal.GetArrayDataReference(_cells);
		ref var control = ref MemoryMarshal.GetArrayDataReference(_control);

		var slot = key.GetHashCode() & mask;
		var firstFree = -1;
		for (var n = 0; n <= mask; n++) {
			var ctl = Unsafe.Add(ref control, (nint)(uint)slot);
			if (ctl == Empty) {
				var at = firstFree >= 0 ? firstFree : slot;
				Place(at, key);
				existing = false;
				index = at;
				return ref Unsafe.Add(ref cells, (nint)(uint)at);
			}

			if (ctl == Occupied) {
				ref var cell = ref Unsafe.Add(ref cells, (nint)(uint)slot);
				if (cell.Key.Equals(key)) {
					existing = true;
					index = slot;
					return ref cell;
				}
			} else if (firstFree < 0) {
				firstFree = slot;
			}

			slot = (slot + 1) & mask;
		}

		return ref GetOrAddRefCold(key, firstFree, out existing, out index);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private ref KeyCell<TKey> GetOrAddRefCold(TKey key, int firstFree, out bool existing, out int index) {
		if (firstFree >= 0) {
			Place(firstFree, key);
			existing = false;
			index = firstFree;
			return ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_cells), (nint)(uint)firstFree);
		}

		if (!EvictFree())
			throw new InvalidOperationException(
				"KeyCellMap is full of live cells — concurrent live keys exceeded capacity.");

		return ref GetOrAddRef(key, out existing, out index);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void Place(int slot, TKey key) {
		var offset = (nint)(uint)slot;
		Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_control), offset) = Occupied;
		ref var cell = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(_cells), offset);
		cell = default;
		cell.Key = key;
		cell.ChunkBase = Chunk.None;
		_count++;
	}

	/// <summary>
	///   Tombstone every cell that is <c>Free</c> <b>and</b> holds no chunk, reclaiming its slot.
	///   Both conditions matter: a cell holding storage must keep its identity until the producer has
	///   harvested the chunk (§4.4/§5.5), and a non-<c>Free</c> cell is in use by a worker. <c>State</c>
	///   is acquire-read because the worker publishes it. Cold path.
	/// </summary>
	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool EvictFree() {
		var mask = _mask;
		ref var cells = ref MemoryMarshal.GetArrayDataReference(_cells);
		ref var control = ref MemoryMarshal.GetArrayDataReference(_control);
		var freed = false;
		for (var slot = 0; slot <= mask; slot++) {
			ref var ctl = ref Unsafe.Add(ref control, (nint)(uint)slot);
			if (ctl != Occupied) continue;
			ref var cell = ref Unsafe.Add(ref cells, (nint)(uint)slot);
			if (cell.ChunkBase != Chunk.None) continue;
			if (Volatile.Read(ref cell.State) != CellState.Free) continue;
			ctl = Tombstone;
			cell = default;
			cell.ChunkBase = Chunk.None;
			_count--;
			freed = true;
		}

		return freed;
	}
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `dotnet test -c Release --filter "FullyQualifiedName~KeyCellMapTests"`
Expected: all 7 PASS on both TFMs.

- [ ] **Step 6: Commit**

```bash
git add src/experiments tests/Nanov.HighPerformance.Tests
git commit -m "feat(actors): KeyCell and producer-private KeyCellMap"
```

---

### Task 4: KeyRing

A bounded SPSC ring of `int` key indices — producer → worker. Sized to the map's slot count so it **can never fill** (§6: a key index is in at most one place at a time, so indices in flight are bounded by the map).

**Files:**
- Create: `…/Internal/KeyRing.cs`
- Test: `tests/…/Threading/Actors/KeyRingTests.cs`

**Interfaces:**
- Produces:
  - `internal sealed class KeyRing`
  - `internal KeyRing(int capacity)`, `internal int Capacity { get; }`, `internal int CountApprox { get; }`
  - `internal bool TryEnqueue(int value)`, `internal bool TryDequeue(out int value)`

- [ ] **Step 1: Write the failing tests**

Create `tests/…/Threading/Actors/KeyRingTests.cs`:

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Actors;

using Nanov.HighPerformance.Threading.Actors.Internal;
using NUnit.Framework;

[TestFixture]
public class KeyRingTests {
	[Test]
	public void Rounds_Capacity_Up_To_Power_Of_Two() =>
		Assert.That(new KeyRing(5).Capacity, Is.EqualTo(8));

	[Test]
	public void Dequeue_On_Empty_Returns_False() {
		var ring = new KeyRing(4);
		Assert.Multiple(() => {
			Assert.That(ring.TryDequeue(out _), Is.False);
			Assert.That(ring.CountApprox, Is.EqualTo(0));
		});
	}

	[Test]
	public void Preserves_Fifo_Order() {
		var ring = new KeyRing(8);
		for (var i = 0; i < 8; i++)
			Assert.That(ring.TryEnqueue(i), Is.True);

		for (var i = 0; i < 8; i++) {
			Assert.That(ring.TryDequeue(out var v), Is.True);
			Assert.That(v, Is.EqualTo(i));
		}
	}

	[Test]
	public void Rejects_When_Full() {
		var ring = new KeyRing(4);
		for (var i = 0; i < 4; i++)
			Assert.That(ring.TryEnqueue(i), Is.True);

		Assert.That(ring.TryEnqueue(99), Is.False);
	}

	[Test]
	public void Wraps_Around_Correctly() {
		var ring = new KeyRing(4);
		for (var round = 0; round < 10; round++) {
			Assert.That(ring.TryEnqueue(round), Is.True);
			Assert.That(ring.TryDequeue(out var v), Is.True);
			Assert.That(v, Is.EqualTo(round));
		}
	}

	[Test]
	public void Spsc_Transfers_Every_Item_In_Order() {
		const int count = 200_000;
		var ring = new KeyRing(1024);
		var received = 0;

		var consumer = new Thread(() => {
			var expected = 0;
			while (expected < count)
				if (ring.TryDequeue(out var v)) {
					Assert.That(v, Is.EqualTo(expected));
					expected++;
					Volatile.Write(ref received, expected);
				}
		}) { IsBackground = true };
		consumer.Start();

		for (var i = 0; i < count; i++)
			while (!ring.TryEnqueue(i)) Thread.SpinWait(1);

		Assert.That(consumer.Join(TimeSpan.FromSeconds(30)), Is.True, "consumer stalled");
		Assert.That(Volatile.Read(ref received), Is.EqualTo(count));
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test -c Release --filter "FullyQualifiedName~KeyRingTests"`
Expected: compile error — `KeyRing` does not exist.

- [ ] **Step 3: Implement `KeyRing`**

Create `…/Internal/KeyRing.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Actors.Internal;

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
///   Bounded single-producer / single-consumer ring of key indices.
///   <para>
///     <b>Strictly one producer and one consumer.</b> A worker's self-reschedule must NOT be enqueued
///     here — that would be a second producer, which breaks the ring silently. Self-rescheduled keys go
///     on the worker's private local stack instead (see <c>ActorSystem</c>).
///   </para>
///   Sized to the key map's slot count, so it can never fill: a key index is in at most one place at a
///   time, so the number in flight is bounded by the map (§6).
/// </summary>
internal sealed class KeyRing {
	private readonly int[] _slots;
	private readonly int _mask;
	private int _head; // consumer-written
	private int _tail; // producer-written

	internal KeyRing(int capacity) {
		var n = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(capacity, 2));
		_slots = new int[n];
		_mask = n - 1;
	}

	internal int Capacity => _slots.Length;

	/// <summary>Approximate depth (<c>Tail − Head</c>). Lock-free, racy by a few.</summary>
	internal int CountApprox => Volatile.Read(ref _tail) - Volatile.Read(ref _head);

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryEnqueue(int value) {
		var tail = _tail;
		if (tail - Volatile.Read(ref _head) >= _slots.Length) return false;
		_slots[tail & _mask] = value;
		Volatile.Write(ref _tail, tail + 1); // release: publishes the slot write
		return true;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryDequeue(out int value) {
		var head = _head;
		if (head == Volatile.Read(ref _tail)) { // acquire: pairs with the producer's release
			value = 0;
			return false;
		}

		value = _slots[head & _mask];
		Volatile.Write(ref _head, head + 1);
		return true;
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test -c Release --filter "FullyQualifiedName~KeyRingTests"`
Expected: all 6 PASS on both TFMs.

- [ ] **Step 5: Commit**

```bash
git add src/experiments tests/Nanov.HighPerformance.Tests
git commit -m "feat(actors): bounded SPSC KeyRing"
```

---

### Task 5: Handler interface and the dispatch path

Producer side only — no worker thread yet. Tests drive `Dispatch` and assert on cell state through internals.

**Files:**
- Create: `…/IActorHandler.cs`
- Create: `…/ActorSystem.cs`
- Create: `tests/…/Threading/Actors/ActorTestUtil.cs`
- Test: `tests/…/Threading/Actors/ActorSystemTests.cs`

**Interfaces:**
- Consumes: `ChunkSlab<T>`, `KeyCellMap<TKey>`, `KeyCell<TKey>`, `CellState`, `KeyRing`, `Chunk.None`
- Produces:
  - `public interface IActorHandler<in TKey, T>` — `Handle(TKey, T)`, `OnError(Exception)`, `OnCompleted(Exception?)`, `OnSlotReused(ref T)`, `OnChunkReclaimed(Span<T>)`
  - `public sealed class ActorSystem<TKey, T, THandler>` with ctor `(THandler handler, int keyCapacity, int chunkCount, int chunkSize, int batchBound = 0)`
  - `public bool Dispatch(TKey key, T message)`
  - `internal ref KeyCell<TKey> CellOf(TKey key)` — test hook
  - `internal int ScheduledCountApprox` — test hook

- [ ] **Step 1: Write the handler interface**

Create `…/IActorHandler.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Actors;

using System;

/// <summary>
///   Per-message logic plus lifecycle hooks. Implement as a <see langword="struct" /> so the calls
///   devirtualize and inline — never store one as an interface reference.
///   <para>
///     Each worker receives its <b>own copy</b> of the handler, so handler fields are per-worker state.
///     Shared state must live behind a reference the struct captures.
///   </para>
/// </summary>
public interface IActorHandler<in TKey, T> {
	/// <summary>
	///   Handle one message. Runs on a worker thread with <b>exclusive ownership</b> of the key, and
	///   messages of one key arrive strictly in dispatch order.
	/// </summary>
	void Handle(TKey key, T message);

	/// <summary>
	///   <see cref="Handle" /> threw. Always synchronous. The message is <b>consumed</b>, not retried,
	///   and the worker continues with the next message of the same key — so a poison message cannot
	///   stall the key, and per-key ordering is unaffected.
	/// </summary>
	void OnError(Exception error);

	/// <summary>
	///   The worker has drained and is exiting. Fires <b>once per worker</b> (so N times for N workers),
	///   on that worker's own handler copy. <c>null</c> = normal completion.
	/// </summary>
	void OnCompleted(Exception? error);

	/// <summary>
	///   A mailbox slot is about to be overwritten, so <paramref name="slot" /> holds a message that has
	///   already been fully processed. Runs on the <b>producer</b> thread. Hook for returning the old
	///   message to a pool; leave empty to keep the reference until the next overwrite (§4.6).
	/// </summary>
	void OnSlotReused(ref T slot);

	/// <summary>
	///   A key drained and its chunk is being recycled; every slot holds an already-processed message.
	///   Runs on the <b>producer</b> thread, cold — once per key lifetime. Hook for draining the tail
	///   into a pool (§4.6).
	/// </summary>
	void OnChunkReclaimed(Span<T> chunk);
}
```

- [ ] **Step 2: Write the shared test handler**

Create `tests/…/Threading/Actors/ActorTestUtil.cs`:

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Actors;

using System.Collections.Concurrent;
using Nanov.HighPerformance.Threading.Actors;

/// <summary>Shared, mutable state observed through a reference — the struct handler is copied.</summary>
internal sealed class RecordingState {
	public readonly ConcurrentQueue<(int Key, int Value)> Seen = new();
	public readonly ConcurrentQueue<Exception> Errors = new();
	public int Handled;
	public int CompletedCalls;
	public int SlotReuseCalls;
	public int ChunkReclaimCalls;
	public Func<int, int, bool>? Throw;

	public int HandledCount => Volatile.Read(ref Handled);

	/// <summary>Per-key values in arrival order — the ordering assertion's raw material.</summary>
	public Dictionary<int, List<int>> ByKey() {
		var result = new Dictionary<int, List<int>>();
		foreach (var (key, value) in Seen) {
			if (!result.TryGetValue(key, out var list)) {
				list = new List<int>();
				result[key] = list;
			}

			list.Add(value);
		}

		return result;
	}
}

// NOTE: an explicit constructor, not a primary constructor — .editorconfig sets
// csharp_style_prefer_primary_constructors = false and EnforceCodeStyleInBuild is on.
internal readonly struct RecordingHandler : IActorHandler<int, int> {
	private readonly RecordingState _state;

	public RecordingHandler(RecordingState state) => _state = state;

	public void Handle(int key, int message) {
		if (_state.Throw?.Invoke(key, message) == true)
			throw new InvalidOperationException($"poison {key}/{message}");
		_state.Seen.Enqueue((key, message));
		Interlocked.Increment(ref _state.Handled);
	}

	public void OnError(Exception error) {
		_state.Errors.Enqueue(error);
		Interlocked.Increment(ref _state.Handled); // consumed, even though it threw
	}

	public void OnCompleted(Exception? error) => Interlocked.Increment(ref _state.CompletedCalls);

	public void OnSlotReused(ref int slot) => Interlocked.Increment(ref _state.SlotReuseCalls);

	public void OnChunkReclaimed(Span<int> chunk) => Interlocked.Increment(ref _state.ChunkReclaimCalls);
}
```

- [ ] **Step 3: Write the failing dispatch tests**

Create `tests/…/Threading/Actors/ActorSystemTests.cs`:

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Actors;

using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Internal;
using NUnit.Framework;

[TestFixture]
public class ActorSystemTests {
	private static ActorSystem<int, int, RecordingHandler> New(RecordingState state,
		int keyCapacity = 64, int chunkCount = 16, int chunkSize = 8, int batchBound = 0) =>
		new(new RecordingHandler(state), keyCapacity, chunkCount, chunkSize, batchBound);

	[Test]
	public void First_Dispatch_Binds_A_Chunk_And_Schedules() {
		var state = new RecordingState();
		using var system = New(state);

		Assert.That(system.Dispatch(1, 100), Is.True);

		// Copy out before asserting: a ref local cannot be captured in Assert.Multiple's lambda (CS8175).
		ref var cell = ref system.CellOf(1);
		var chunkBase = cell.ChunkBase;
		var tail = cell.Tail;
		var head = cell.Head;
		var state = cell.State;

		Assert.Multiple(() => {
			Assert.That(chunkBase, Is.Not.EqualTo(Chunk.None));
			Assert.That(tail, Is.EqualTo(1));
			Assert.That(head, Is.EqualTo(0));
			Assert.That(state, Is.EqualTo(CellState.Scheduled));
			Assert.That(system.ScheduledCountApprox, Is.EqualTo(1));
		});
	}

	[Test]
	public void Second_Dispatch_To_A_Scheduled_Key_Does_Not_Reschedule() {
		var state = new RecordingState();
		using var system = New(state);

		system.Dispatch(1, 100);
		system.Dispatch(1, 101);

		Assert.Multiple(() => {
			Assert.That(system.CellOf(1).Tail, Is.EqualTo(2));
			Assert.That(system.ScheduledCountApprox, Is.EqualTo(1), "first-dirty gating failed");
		});
	}

	[Test]
	public void Distinct_Keys_Each_Schedule_Once() {
		var state = new RecordingState();
		using var system = New(state);

		for (var k = 0; k < 5; k++) {
			system.Dispatch(k, k * 10);
			system.Dispatch(k, k * 10 + 1);
		}

		Assert.That(system.ScheduledCountApprox, Is.EqualTo(5));
	}

	[Test]
	public void Distinct_Keys_Get_Distinct_Chunks() {
		var state = new RecordingState();
		using var system = New(state);

		var chunks = new HashSet<int>();
		for (var k = 0; k < 5; k++) {
			system.Dispatch(k, k);
			Assert.That(chunks.Add(system.CellOf(k).ChunkBase), Is.True, $"key {k} shares a chunk");
		}
	}

	[Test]
	public void BatchBound_Defaults_To_ChunkSize() {
		var state = new RecordingState();
		using var system = New(state, chunkSize: 16, batchBound: 0);
		Assert.That(system.BatchBound, Is.EqualTo(16));
	}
}
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `dotnet test -c Release --filter "FullyQualifiedName~ActorSystemTests"`
Expected: compile error — `ActorSystem<,,>` does not exist.

- [ ] **Step 5: Implement the dispatch path**

Create `…/ActorSystem.cs`. This file grows across Tasks 5–10; this step establishes the producer path only.

```csharp
namespace Nanov.HighPerformance.Threading.Actors;

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors.Internal;

/// <summary>
///   Key-affine message processor: a single producer dispatches <c>(key, message)</c>; workers process
///   them with <b>strict FIFO order per key</b> and exclusive per-key ownership.
///   <para>
///     Single producer is the caller's contract, as with the SPSC worker family. Ownership is arbitrated
///     by a 3-state machine on each key's cell; storage is a chunk of a shared slab used as that key's
///     bounded mailbox.
///   </para>
/// </summary>
public sealed class ActorSystem<TKey, T, THandler> : IDisposable
	where TKey : notnull, IEquatable<TKey>
	where THandler : struct, IActorHandler<TKey, T> {
	private readonly KeyCellMap<TKey> _map;
	private readonly ChunkSlab<T> _slab;
	private readonly KeyRing _scheduled;
	private readonly int _chunkSize;
	private readonly int _chunkMask;
	private readonly int _batchBound;
	private THandler _handler; // mutable struct — calls go through the field so state persists

	private int _dispatched; // producer-private; no atomics

	/// <param name="handler">Struct handler; copied into each worker.</param>
	/// <param name="keyCapacity">Max live keys.</param>
	/// <param name="chunkCount">Max concurrently-dirty keys.</param>
	/// <param name="chunkSize">Messages one key may hold before backpressure.</param>
	/// <param name="batchBound">Max messages per activation; 0 ⇒ <paramref name="chunkSize" />.</param>
	public ActorSystem(THandler handler, int keyCapacity, int chunkCount, int chunkSize, int batchBound = 0) {
		_handler = handler;
		_slab = new ChunkSlab<T>(chunkCount, chunkSize);
		_chunkSize = _slab.ChunkSize;
		_chunkMask = _slab.ChunkMask;
		_map = new KeyCellMap<TKey>(keyCapacity);

		// A chunk-bound cell is never evictable, so if chunks are scarcer than cells an evictable cell
		// always exists — cell exhaustion becomes structurally impossible and chunk exhaustion is the
		// only blocking condition.
		if (_slab.ChunkCount >= _map.SlotCount)
			throw new ArgumentOutOfRangeException(nameof(chunkCount),
				$"chunkCount ({_slab.ChunkCount}) must be less than the map's slot count ({_map.SlotCount}); " +
				"raise keyCapacity or lower chunkCount.");

		_scheduled = new KeyRing(_map.SlotCount);
		_batchBound = batchBound > 0 ? batchBound : _chunkSize;
	}

	/// <summary>Max messages consumed in one activation before the key is released and rescheduled.</summary>
	public int BatchBound => _batchBound;

	/// <summary>
	///   Enqueue a message for <paramref name="key" />. Single-producer: never call concurrently.
	///   Returns false if the system is completed or disposed.
	/// </summary>
	public bool Dispatch(TKey key, T message) {
		ref var cell = ref _map.GetOrAddRef(key, out _, out var index);
		if (cell.ChunkBase == Chunk.None && !TryBindChunk(ref cell)) return false;

		ref var slot = ref _slab.SlotRef(cell.ChunkBase, cell.Tail & _chunkMask);
		_handler.OnSlotReused(ref slot);
		slot = message;
		Volatile.Write(ref cell.Tail, cell.Tail + 1); // release: publishes the slot write
		_dispatched++;

		// StoreLoad: the Tail store must be ordered before the State load. Volatile does NOT give this
		// on ARM64, and the test-then-CAS below means there is no RMW here to supply it. Removing this
		// barrier strands messages — see the release handshake in ReleaseAndMaybeSchedule.
		Interlocked.MemoryBarrier();
		if (Volatile.Read(ref cell.State) == CellState.Free &&
			Interlocked.CompareExchange(ref cell.State, CellState.Scheduled, CellState.Free) == CellState.Free)
			ScheduleFromProducer(index);

		return true;
	}

	private bool TryBindChunk(ref KeyCell<TKey> cell) {
		if (!_slab.TryRent(out var chunk)) return false; // backpressure lands here in Task 9
		cell.ChunkBase = chunk;
		cell.Head = 0;
		cell.Tail = 0;
		cell.CachedHead = 0;
		return true;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void ScheduleFromProducer(int index) {
		if (!_scheduled.TryEnqueue(index))
			throw new InvalidOperationException(
				"scheduler ring full — it is sized to the map, so this indicates a key index was scheduled twice.");
	}

	public void Dispose() { }

	// ---- test hooks -------------------------------------------------------------------------------

	internal ref KeyCell<TKey> CellOf(TKey key) => ref _map.GetOrAddRef(key, out _, out _);

	internal int ScheduledCountApprox => _scheduled.CountApprox;

	internal int DispatchedCount => _dispatched;
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `dotnet test -c Release --filter "FullyQualifiedName~ActorSystemTests"`
Expected: all 5 PASS on both TFMs.

- [ ] **Step 7: Commit**

```bash
git add src/experiments tests/Nanov.HighPerformance.Tests
git commit -m "feat(actors): handler interface and producer dispatch path"
```

---

### Task 6: Worker loop — activation, drain, release

The ownership protocol. **This is the highest-risk task in the plan** — re-read §5.2 and §5.3 of the spec before starting, especially the Dekker argument and the note on why `Interlocked.Exchange` is used rather than `Volatile.Write`.

**Files:**
- Modify: `…/ActorSystem.cs`
- Test: `tests/…/Threading/Actors/ActorSystemTests.cs` (extend)

**Interfaces:**
- Produces: `public void Start()`, and internally the worker loop, `Activate`, `ReleaseAndMaybeSchedule`

- [ ] **Step 1: Write the failing tests**

Append to `ActorSystemTests.cs` (inside the fixture):

```csharp
	[Test]
	public void Processes_All_Messages_Of_One_Key_In_Order() {
		var state = new RecordingState();
		using var system = New(state, chunkSize: 8);
		system.Start();

		const int count = 500;
		for (var i = 0; i < count; i++)
			Assert.That(system.Dispatch(1, i), Is.True);

		WorkerTestUtil.AwaitProgress(() => state.HandledCount, count);
		Assert.That(state.ByKey()[1], Is.EqualTo(Enumerable.Range(0, count).ToList()));
	}

	[Test]
	public void Preserves_Order_Within_Every_Key_Across_Many_Keys() {
		var state = new RecordingState();
		using var system = New(state, keyCapacity: 128, chunkCount: 32, chunkSize: 8);
		system.Start();

		const int keys = 16;
		const int perKey = 200;
		for (var i = 0; i < perKey; i++)
			for (var k = 0; k < keys; k++)
				Assert.That(system.Dispatch(k, i), Is.True);

		WorkerTestUtil.AwaitProgress(() => state.HandledCount, keys * perKey);

		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");
	}

	[Test]
	public void Reschedules_A_Key_That_Exceeds_BatchBound() {
		var state = new RecordingState();
		using var system = New(state, chunkSize: 64, batchBound: 4);
		system.Start();

		const int count = 100;
		for (var i = 0; i < count; i++)
			system.Dispatch(1, i);

		WorkerTestUtil.AwaitProgress(() => state.HandledCount, count);
		Assert.That(state.ByKey()[1], Is.EqualTo(Enumerable.Range(0, count).ToList()));
	}

	[Test]
	public void Handler_Exception_Is_Reported_And_Processing_Continues() {
		var state = new RecordingState { Throw = (_, value) => value == 2 };
		using var system = New(state);
		system.Start();

		for (var i = 0; i < 5; i++)
			system.Dispatch(1, i);

		WorkerTestUtil.AwaitProgress(() => state.HandledCount, 5);
		Assert.Multiple(() => {
			Assert.That(state.Errors, Has.Count.EqualTo(1));
			Assert.That(state.ByKey()[1], Is.EqualTo(new List<int> { 0, 1, 3, 4 }), "order broke around the throw");
		});
	}
```

Add this using inside the namespace at the top of the file, keeping the list sorted as the existing
test files do (`Nanov.*` before `NUnit.Framework`):

```csharp
using Nanov.HighPerformance.Tests.Threading.Workers;
```

`System.Linq` is already covered by `ImplicitUsings`, so it is not needed — though adding it is
harmless, as `SpWorkerTests` does exactly that and still builds warning-free.

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test -c Release --filter "FullyQualifiedName~ActorSystemTests"`
Expected: compile error — `Start` does not exist.

- [ ] **Step 3: Add worker fields and the loop**

In `ActorSystem.cs`, add these fields after `private int _dispatched;`:

```csharp
	private readonly int[] _local; // worker-private reschedule stack — never touched by the producer
	private int _localCount;
	private Thread? _worker;
	private volatile bool _stopped;
	private long _processedLocal;    // worker-private running count
	private long _processedPublished; // published for the quiescence check (Task 7)
```

Initialize `_local` in the constructor, after `_scheduled` is created:

```csharp
		_local = new int[_map.SlotCount];
```

Add the public start plus the loop:

```csharp
	/// <summary>Start the worker thread.</summary>
	public void Start() {
		_worker = new Thread(RunLoop) { IsBackground = true, Name = $"Actor<{typeof(TKey).Name}>" };
		_worker.Start();
	}

	private void RunLoop() {
		while (!_stopped) {
			if (TryAcquire(out var index)) {
				Activate(index);
				continue;
			}

			if (!WaitForWork()) break;
		}
	}

	/// <summary>
	///   Take the next key. The worker-private local stack has priority over the producer's ring, which
	///   keeps a busy key pinned to this worker (affinity) and — critically — keeps the ring
	///   single-producer, since self-rescheduled keys never enter it.
	/// </summary>
	private bool TryAcquire(out int index) {
		if (_localCount > 0) {
			index = _local[--_localCount];
			return true;
		}

		return _scheduled.TryDequeue(out index);
	}

	private void Activate(int index) {
		ref var cell = ref _map.RefAt(index);
		Interlocked.Exchange(ref cell.State, CellState.Owned);

		var chunk = cell.ChunkBase;
		var head = cell.Head;
		var n = 0;
		while (n < _batchBound && head != Volatile.Read(ref cell.Tail)) {
			if (_stopped) break;
			try {
				_handler.Handle(cell.Key, _slab.SlotRef(chunk, head & _chunkMask));
			}
			catch (Exception ex) {
				SafeOnError(ex);
			}

			head++;
			n++;
			_processedLocal++;
		}

		ReleaseAndMaybeSchedule(index, ref cell, head, fromWorker: true);
	}

	/// <summary>
	///   Release ownership, and reschedule if the mailbox is not empty. The ordering here is
	///   load-bearing — see §5.3. Shared by the worker and by the producer's cold paths (Task 10), which
	///   is why it takes <paramref name="fromWorker" /> rather than being open-coded twice.
	/// </summary>
	private void ReleaseAndMaybeSchedule(int index, ref KeyCell<TKey> cell, int head, bool fromWorker) {
		Volatile.Write(ref cell.Head, head);
		PublishProcessed();

		// FULL FENCE, deliberately not Volatile.Write: the Tail load below must not be hoisted above
		// this store. Producer and worker each store to their own variable then load the other's, with a
		// full fence between (Dekker) — so they cannot both miss each other, and the State CAS (never
		// the emptiness check) is the arbiter of who reschedules.
		Interlocked.Exchange(ref cell.State, CellState.Free);

		if (Volatile.Read(ref cell.Tail) == head) return;
		// else: drained. The chunk is now reclaimable, but we must NOT touch ChunkBase — it is
		// producer-owned. The producer discovers it on its next cold harvest scan (Task 10).

		if (Interlocked.CompareExchange(ref cell.State, CellState.Scheduled, CellState.Free) != CellState.Free)
			return; // the producer got there first and already scheduled it — no double-schedule

		if (fromWorker) PushLocal(index);
		else ScheduleFromProducer(index);
	}

	private void PushLocal(int index) => _local[_localCount++] = index;

	private void PublishProcessed() => Volatile.Write(ref _processedPublished, _processedLocal);

	private void SafeOnError(Exception error) {
		try {
			_handler.OnError(error);
		}
		catch {
			// A throwing OnError must not kill the worker.
		}
	}

	private bool WaitForWork() {
		Thread.SpinWait(64); // replaced by real parking in Task 7
		return !_stopped;
	}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test -c Release --filter "FullyQualifiedName~ActorSystemTests"`
Expected: all 9 PASS on both TFMs.

- [ ] **Step 5: Run in Debug too**

Run: `dotnet test -c Debug --filter "FullyQualifiedName~ActorSystemTests"`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/experiments tests/Nanov.HighPerformance.Tests
git commit -m "feat(actors): worker loop with Dekker release handshake"
```

---

### Task 7: Termination detection and completion

Implements §5.6. The wake protocol and the load order are the parts that must not be "tidied" later.

**Files:**
- Modify: `…/ActorSystem.cs`
- Test: `tests/…/Threading/Actors/ActorSystemTests.cs` (extend)

**Interfaces:**
- Produces: `public bool TryComplete(Exception? exception = null)`, `public Task Completion { get; }`

- [ ] **Step 1: Write the failing tests**

Append to `ActorSystemTests.cs`:

```csharp
	[Test]
	public void TryComplete_Drains_Everything_Then_Completes() {
		var state = new RecordingState();
		using var system = New(state, chunkSize: 8);
		system.Start();

		const int count = 2_000;
		for (var i = 0; i < count; i++)
			system.Dispatch(i % 8, i);

		Assert.That(system.TryComplete(), Is.True);
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, count);
		Assert.That(state.HandledCount, Is.EqualTo(count));
	}

	[Test]
	public void TryComplete_On_An_Already_Drained_Parked_System_Completes() {
		var state = new RecordingState();
		using var system = New(state);
		system.Start();

		system.Dispatch(1, 1);
		WorkerTestUtil.AwaitProgress(() => state.HandledCount, 1);
		Thread.Sleep(100); // let the worker genuinely park

		Assert.That(system.TryComplete(), Is.True);
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 1);
	}

	[Test]
	public void TryComplete_On_A_System_That_Never_Received_Anything_Completes() {
		var state = new RecordingState();
		using var system = New(state);
		system.Start();
		Thread.Sleep(100);

		Assert.That(system.TryComplete(), Is.True);
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 0);
	}

	[Test]
	public void TryComplete_Is_Idempotent() {
		var state = new RecordingState();
		using var system = New(state);
		system.Start();

		Assert.Multiple(() => {
			Assert.That(system.TryComplete(), Is.True);
			Assert.That(system.TryComplete(), Is.False);
		});
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 0);
	}

	[Test]
	public void Dispatch_After_TryComplete_Is_Rejected() {
		var state = new RecordingState();
		using var system = New(state);
		system.Start();
		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 0);

		Assert.That(system.Dispatch(1, 1), Is.False);
	}

	[Test]
	public void OnCompleted_Fires_Once_Per_Worker() {
		var state = new RecordingState();
		using var system = New(state);
		system.Start();
		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 0);

		Assert.That(Volatile.Read(ref state.CompletedCalls), Is.EqualTo(1));
	}

	[Test]
	public void TryComplete_With_An_Exception_Faults_Completion() {
		var state = new RecordingState();
		using var system = New(state);
		system.Start();
		system.TryComplete(new InvalidOperationException("boom"));

		var ex = Assert.Throws<InvalidOperationException>(() =>
			WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 0));
		Assert.That(ex!.Message, Is.EqualTo("boom"));
	}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test -c Release --filter "FullyQualifiedName~ActorSystemTests"`
Expected: compile error — `TryComplete` / `Completion` do not exist.

- [ ] **Step 3: Implement termination**

In `ActorSystem.cs`, add fields:

```csharp
	private readonly TaskCompletionSource _completion =
		new(TaskCreationOptions.RunContinuationsAsynchronously);
	private readonly ManualResetEventSlim _workAvailable = new(false);
	private long _dispatchedFinal;
	private volatile bool _completed;
	private Exception? _completionError;
	private int _completeGate;
```

Replace `Dispose()` and `WaitForWork()`, and add the rest:

```csharp
	/// <summary>Resolves once the worker has drained and exited.</summary>
	public Task Completion => _completion.Task;

	/// <summary>
	///   Signal that no more messages will be dispatched: drain everything queued, then exit. A non-null
	///   <paramref name="exception" /> faults <see cref="Completion" />. Returns false if already
	///   completing. Producer thread only — and it must be the producer's LAST act (see below).
	/// </summary>
	public bool TryComplete(Exception? exception = null) {
		if (Interlocked.Exchange(ref _completeGate, 1) != 0) return false;
		_completionError = exception;

		// Publish the final count BEFORE the flag: the quiescence check reads the flag first, so this
		// ordering is what makes "observed completed ⇒ observes the full dispatched count" hold.
		Volatile.Write(ref _dispatchedFinal, _dispatched);
		_completed = true;

		// Shutdown wakes UNCONDITIONALLY — never flag-gated. Without this, a system that was already
		// drained and parked when TryComplete arrives would never be woken by anything: no further
		// message will ever be processed, so no worker will ever re-evaluate the predicate.
		WakeAll();
		return true;
	}

	private void WakeAll() => _workAvailable.Set();

	/// <summary>
	///   Park until work arrives or the system should exit. Returns false to exit the loop.
	///   <para>
	///     Arm-then-recheck: the flag is cleared, then work and quiescence are re-tested, and only then
	///     do we block. Collapsing this into check-then-park is the classic lost wakeup, and it passes
	///     every test whose worker never actually sleeps.
	///   </para>
	/// </summary>
	private bool WaitForWork() {
		for (var spin = 0; spin < 32; spin++) {
			if (HasWork() || _stopped) return !_stopped;
			Thread.SpinWait(64);
		}

		while (true) {
			_workAvailable.Reset();
			Interlocked.MemoryBarrier();
			if (HasWork() || _stopped) return !_stopped;
			if (IsQuiescent()) return false;
			_workAvailable.Wait();
			if (HasWork() || _stopped) return !_stopped;
			if (IsQuiescent()) return false;
		}
	}

	private bool HasWork() => _localCount > 0 || _scheduled.CountApprox > 0;

	/// <summary>
	///   Everything dispatched has been processed and completion was signalled.
	///   <para>
	///     <b>Read order is load-bearing.</b> <c>_completed</c> must be read BEFORE the counts, both with
	///     acquire semantics. The soundness argument is "every producer increment is sequenced-before
	///     <c>_completed = true</c>, so a worker observing the flag must observe the counts" — which only
	///     holds if the count loads are ordered after the flag load. Reversed, ARM64 will serve a stale
	///     count beside a fresh flag: false quiescence, and messages silently dropped on dispose.
	///   </para>
	///   This mirrors the family's existing soft-complete rule (re-read Tail *after* observing
	///   _completed) with the processed count as the new Tail.
	/// </summary>
	private bool IsQuiescent() {
		if (!_completed) return false;
		var processed = Volatile.Read(ref _processedPublished);
		return processed == Volatile.Read(ref _dispatchedFinal);
	}

	public void Dispose() {
		_stopped = true;
		WakeAll();
		_worker?.Join(TimeSpan.FromSeconds(1));
		// The events are deliberately NOT disposed: the producer is a caller-owned thread that may be
		// parked inside Wait() right now, and disposing under it throws ObjectDisposedException on
		// their thread. Letting the finalizer reclaim them is the correct trade here.
	}
```

Change `RunLoop` to run the completion hooks on exit:

```csharp
	private void RunLoop() {
		try {
			while (!_stopped) {
				if (TryAcquire(out var index)) {
					Activate(index);
					continue;
				}

				if (!WaitForWork()) break;
			}
		}
		finally {
			PublishProcessed();
			Exception? error = _stopped && !_completed ? new OperationCanceledException() : _completionError;
			try {
				_handler.OnCompleted(error);
			}
			catch (Exception ex) {
				SafeOnError(ex);
			}

			if (error is null) _completion.TrySetResult();
			else _completion.TrySetException(error);
		}
	}
```

In `Dispatch`, reject after completion — insert as the first line:

```csharp
		if (_completed || _stopped) return false;
```

And in `ScheduleFromProducer`, wake the worker after enqueuing:

```csharp
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void ScheduleFromProducer(int index) {
		if (!_scheduled.TryEnqueue(index))
			throw new InvalidOperationException(
				"scheduler ring full — it is sized to the map, so this indicates a key index was scheduled twice.");
		_workAvailable.Set();
	}
```

Finally, add the counter publish in `Activate`'s drain loop so a long batch is visible to the
quiescence check — replace the loop body's tail (`head++; n++; _processedLocal++;`) with:

```csharp
			head++;
			n++;
			_processedLocal++;
			if ((n & 31) == 0) {
				Volatile.Write(ref cell.Head, head);
				PublishProcessed();
			}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test -c Release --filter "FullyQualifiedName~ActorSystemTests"`
Expected: all 16 PASS on both TFMs.

- [ ] **Step 5: Run in Debug**

Run: `dotnet test -c Debug --filter "FullyQualifiedName~ActorSystemTests"`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/experiments tests/Nanov.HighPerformance.Tests
git commit -m "feat(actors): termination detection by message counts"
```

---

### Task 8: Hard stop

**Files:**
- Modify: `…/ActorSystem.cs`
- Test: `tests/…/Threading/Actors/ActorSystemTests.cs` (extend)

**Interfaces:**
- Consumes: `Dispose`, `_stopped` (Task 7)
- Produces: hard-stop semantics — drop queued, finish the current message, `Completion` faults `OperationCanceledException`

- [ ] **Step 1: Write the failing tests**

Append to `ActorSystemTests.cs`:

```csharp
	[Test]
	public void Dispose_Drops_Queued_Messages() {
		var state = new RecordingState();
		var gate = new ManualResetEventSlim(false);
		state.Throw = (_, value) => {
			if (value == 0) gate.Wait(TimeSpan.FromSeconds(5));
			return false;
		};

		var system = New(state, chunkSize: 64);
		system.Start();
		for (var i = 0; i < 50; i++)
			system.Dispatch(1, i);

		WorkerTestUtil.AwaitProgress(() => state.HandledCount, 1); // worker is inside message 0
		system.Dispose();
		gate.Set();

		Assert.That(state.HandledCount, Is.LessThan(50), "hard stop should have dropped queued messages");
	}

	[Test]
	public void Dispose_Faults_Completion_As_Cancelled() {
		var state = new RecordingState();
		var system = New(state);
		system.Start();
		system.Dispatch(1, 1);
		WorkerTestUtil.AwaitProgress(() => state.HandledCount, 1);

		system.Dispose();
		Assert.ThrowsAsync<OperationCanceledException>(async () => await system.Completion);
	}

	[Test]
	public void Dispose_Is_Idempotent() {
		var state = new RecordingState();
		var system = New(state);
		system.Start();
		system.Dispose();
		Assert.DoesNotThrow(() => system.Dispose());
	}

	[Test]
	public void Dispatch_After_Dispose_Is_Rejected() {
		var state = new RecordingState();
		var system = New(state);
		system.Start();
		system.Dispose();

		Assert.That(system.Dispatch(1, 1), Is.False);
	}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test -c Release --filter "FullyQualifiedName~ActorSystemTests"`
Expected: `Dispose_Is_Idempotent` fails (`ObjectDisposedException` from the second `_workAvailable.Dispose()`), and `Dispose_Drops_Queued_Messages` may fail.

- [ ] **Step 3: Make `Dispose` idempotent and the stop check mid-batch**

Replace `Dispose` in `ActorSystem.cs`:

```csharp
	private int _disposeGate;

	/// <summary>
	///   Hard stop: drop queued messages and exit. The message currently inside the handler still
	///   finishes — tearing down under a running handler is something the family never does.
	///   Workers are joined against one shared budget, not one per worker.
	/// </summary>
	public void Dispose() {
		if (Interlocked.Exchange(ref _disposeGate, 1) != 0) return;
		_stopped = true;
		WakeAll();
		_worker?.Join(TimeSpan.FromSeconds(1));
		// Events are deliberately not disposed — see the note in Task 7.
	}
```

The mid-batch `if (_stopped) break;` added in Task 6 already gives "finish the current message, abandon the rest of the batch".

The 1 s join is a **single shared budget**, not one second per worker. Phase 2 keeps it shared when N
workers arrive: `Join` each against the remaining time, not a fresh second each.

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test -c Release --filter "FullyQualifiedName~ActorSystemTests"`
Expected: all 20 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/experiments tests/Nanov.HighPerformance.Tests
git commit -m "feat(actors): hard stop semantics"
```

---

### Task 9: Backpressure and TryDispatch

Implements §5.4. Two sources: a key's mailbox full, and the slab exhausted.

**Files:**
- Modify: `…/ActorSystem.cs`
- Test: `tests/…/Threading/Actors/ActorSystemTests.cs` (extend)

**Interfaces:**
- Produces: `public bool TryDispatch(TKey key, T message)`; `Dispatch` blocks instead of returning false when full

- [ ] **Step 1: Write the failing tests**

Append to `ActorSystemTests.cs`:

```csharp
	[Test]
	public void TryDispatch_Returns_False_When_The_Key_Mailbox_Is_Full() {
		var state = new RecordingState();
		using var system = New(state, chunkSize: 4); // never started — nothing drains

		for (var i = 0; i < 4; i++)
			Assert.That(system.TryDispatch(1, i), Is.True, $"message {i}");

		Assert.Multiple(() => {
			Assert.That(system.TryDispatch(1, 99), Is.False);
			Assert.That(system.DispatchedCount, Is.EqualTo(4), "a rejected TryDispatch must not be counted");
		});
	}

	[Test]
	public void TryDispatch_Returns_False_When_The_Slab_Is_Exhausted() {
		var state = new RecordingState();
		using var system = New(state, keyCapacity: 64, chunkCount: 4, chunkSize: 4);

		for (var k = 0; k < 4; k++)
			Assert.That(system.TryDispatch(k, k), Is.True, $"key {k}");

		Assert.That(system.TryDispatch(99, 99), Is.False, "5th distinct key should find no chunk");
	}

	[Test]
	public void Dispatch_Blocks_Until_The_Worker_Drains_Then_Proceeds() {
		var state = new RecordingState();
		using var system = New(state, chunkSize: 4);
		const int count = 200;

		var producer = new Thread(() => {
			for (var i = 0; i < count; i++)
				system.Dispatch(1, i);
		}) { IsBackground = true };
		producer.Start();

		Thread.Sleep(50);
		system.Start(); // producer is blocked on a full mailbox until now

		Assert.That(producer.Join(TimeSpan.FromSeconds(10)), Is.True, "producer never unblocked");
		WorkerTestUtil.AwaitProgress(() => state.HandledCount, count);
		Assert.That(state.ByKey()[1], Is.EqualTo(Enumerable.Range(0, count).ToList()));
	}

	[Test]
	public void Dispatch_Blocked_On_A_Full_Mailbox_Is_Released_By_Dispose() {
		var state = new RecordingState();
		var system = New(state, chunkSize: 4);
		var finished = 0;

		var producer = new Thread(() => {
			for (var i = 0; i < 1_000; i++)
				if (!system.Dispatch(1, i)) break;
			Volatile.Write(ref finished, 1);
		}) { IsBackground = true };
		producer.Start();

		Thread.Sleep(100);
		system.Dispose();

		Assert.That(producer.Join(TimeSpan.FromSeconds(5)), Is.True, "producer stayed parked after Dispose");
		Assert.That(Volatile.Read(ref finished), Is.EqualTo(1));
	}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test -c Release --filter "FullyQualifiedName~ActorSystemTests"`
Expected: compile error — `TryDispatch` does not exist.

- [ ] **Step 3: Implement backpressure**

In `ActorSystem.cs`, add a producer-park event alongside `_workAvailable`:

```csharp
	private readonly ManualResetEventSlim _spaceAvailable = new(false);
	private volatile bool _producerParked;
```

Replace `Dispatch` and `TryBindChunk`, and add `TryDispatch`, with the shared core:

```csharp
	/// <summary>
	///   Enqueue a message for <paramref name="key" />, <b>blocking</b> if the key's mailbox is full or
	///   the slab is exhausted. Single-producer: never call concurrently. Returns false if the system is
	///   completed or disposed.
	/// </summary>
	public bool Dispatch(TKey key, T message) => DispatchCore(key, message, blocking: true);

	/// <summary>
	///   Enqueue a message, returning <c>false</c> immediately instead of blocking when the key's mailbox
	///   is full or the slab is exhausted. A rejected message is not enqueued and is not counted.
	///   <para>
	///     Because a full key blocks the single producer for <i>all</i> keys, the difference between this
	///     and <see cref="Dispatch" /> is exactly the cost of the bounded per-key mailbox — which is what
	///     makes that cost measurable.
	///   </para>
	/// </summary>
	public bool TryDispatch(TKey key, T message) => DispatchCore(key, message, blocking: false);

	private bool DispatchCore(TKey key, T message, bool blocking) {
		if (_completed || _stopped) return false;

		ref var cell = ref _map.GetOrAddRef(key, out _, out var index);
		if (cell.ChunkBase == Chunk.None && !BindChunk(ref cell, blocking)) return false;

		// Fullness is checked against the producer-private CachedHead; the real Head — which the worker
		// writes — is re-read only on the "looks full" edge. Head only advances, so CachedHead can only
		// over-estimate fullness: conservative, never overwrites a live slot.
		if (cell.Tail - cell.CachedHead >= _chunkSize) {
			cell.CachedHead = Volatile.Read(ref cell.Head);
			while (cell.Tail - cell.CachedHead >= _chunkSize) {
				if (!blocking) return false;
				if (!WaitForProgress()) return false;
				cell.CachedHead = Volatile.Read(ref cell.Head);
			}
		}

		ref var slot = ref _slab.SlotRef(cell.ChunkBase, cell.Tail & _chunkMask);
		_handler.OnSlotReused(ref slot);
		slot = message;
		Volatile.Write(ref cell.Tail, cell.Tail + 1);
		_dispatched++;

		Interlocked.MemoryBarrier();
		if (Volatile.Read(ref cell.State) == CellState.Free &&
			Interlocked.CompareExchange(ref cell.State, CellState.Scheduled, CellState.Free) == CellState.Free)
			ScheduleFromProducer(index);

		return true;
	}

	private bool BindChunk(ref KeyCell<TKey> cell, bool blocking) {
		int chunk;
		while (!_slab.TryRent(out chunk)) {
			if (Harvest()) continue;              // cold: reclaim drained keys' chunks (Task 10)
			if (!blocking) return false;
			if (!WaitForProgress()) return false; // park until a worker drains something
		}

		cell.ChunkBase = chunk;
		cell.Head = 0;
		cell.Tail = 0;
		cell.CachedHead = 0;
		return true;
	}

	/// <summary>
	///   Park until the worker makes progress — either freeing a mailbox slot, or draining a key whose
	///   chunk the producer can then harvest. One helper serves both backpressure sources (§5.4) because
	///   both are relieved by the same event: the worker advancing.
	///   <para>
	///     Returns false if the system stopped or completed while parked, so a dispatch racing shutdown
	///     is <b>rejected, never stranded</b> — that stranding is exactly bug #2 in <c>memory.md</c>.
	///   </para>
	///   <para>
	///     The bounded wait is deliberate for v1: a missed wake self-heals within 20 ms rather than
	///     hanging. Phase 2 replaces it with the family's parked-flag protocol once there are N workers
	///     to signal.
	///   </para>
	/// </summary>
	private bool WaitForProgress() {
		_producerParked = true;
		try {
			_spaceAvailable.Reset();
			Interlocked.MemoryBarrier(); // flag visible before we re-test, so the worker cannot miss us
			if (_stopped || _completed) return false;
			_spaceAvailable.Wait(TimeSpan.FromMilliseconds(20));
			return !_stopped && !_completed;
		}
		finally {
			_producerParked = false;
		}
	}
```

Add a stub `Harvest` (fully implemented in Task 10):

```csharp
	private bool Harvest() => false;
```

Wake the parked producer from the worker. In `ReleaseAndMaybeSchedule`, after
`Volatile.Write(ref cell.Head, head);` add:

```csharp
		if (_producerParked) _spaceAvailable.Set();
```

and in `Activate`'s periodic flush, after `Volatile.Write(ref cell.Head, head);` add the same line.

Finally, shutdown must release a parked producer. In `TryComplete` and `Dispose`, after `WakeAll();`
add:

```csharp
		_spaceAvailable.Set();
```

Do **not** dispose it — same reason as `_workAvailable` (Task 7): the producer is a caller-owned
thread that may be parked inside `Wait()` at that moment.

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test -c Release --filter "FullyQualifiedName~ActorSystemTests"`
Expected: all 24 PASS on both TFMs.

- [ ] **Step 5: Run in Debug**

Run: `dotnet test -c Debug --filter "FullyQualifiedName~ActorSystemTests"`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/experiments tests/Nanov.HighPerformance.Tests
git commit -m "feat(actors): blocking and non-blocking dispatch with backpressure"
```

---

### Task 10: Chunk harvest and pool hooks

Implements §4.4 and §4.6. The reclaim path is **cold and producer-side**, and it must route its
failure case through `ReleaseAndMaybeSchedule` — a plain revert strands messages.

**Files:**
- Modify: `…/ActorSystem.cs`
- Test: `tests/…/Threading/Actors/ActorSystemTests.cs` (extend)

**Interfaces:**
- Consumes: `ReleaseAndMaybeSchedule` (Task 6), `KeyCellMap.IsOccupied`/`SlotCount`/`RefAt` (Task 3)
- Produces: real `private bool Harvest()`

- [ ] **Step 1: Write the failing tests**

Append to `ActorSystemTests.cs`:

```csharp
	[Test]
	public void Harvest_Recycles_Chunks_Of_Drained_Keys() {
		var state = new RecordingState();
		using var system = New(state, keyCapacity: 64, chunkCount: 4, chunkSize: 4);
		system.Start();

		// Far more distinct keys than chunks: only recycling makes this possible.
		const int keys = 40;
		for (var k = 0; k < keys; k++) {
			Assert.That(system.Dispatch(k, k), Is.True, $"key {k}");
			WorkerTestUtil.AwaitProgress(() => state.HandledCount, k + 1);
		}

		Assert.That(state.HandledCount, Is.EqualTo(keys));
	}

	[Test]
	public void Harvest_Invokes_OnChunkReclaimed() {
		var state = new RecordingState();
		using var system = New(state, keyCapacity: 64, chunkCount: 4, chunkSize: 4);
		system.Start();

		for (var k = 0; k < 20; k++) {
			system.Dispatch(k, k);
			WorkerTestUtil.AwaitProgress(() => state.HandledCount, k + 1);
		}

		Assert.That(Volatile.Read(ref state.ChunkReclaimCalls), Is.GreaterThan(0));
	}

	[Test]
	public void OnSlotReused_Fires_When_A_Slot_Is_Overwritten() {
		var state = new RecordingState();
		using var system = New(state, chunkSize: 4);
		system.Start();

		const int count = 40; // wraps the 4-slot mailbox many times
		for (var i = 0; i < count; i++)
			system.Dispatch(1, i);

		WorkerTestUtil.AwaitProgress(() => state.HandledCount, count);
		Assert.That(Volatile.Read(ref state.SlotReuseCalls), Is.EqualTo(count));
	}

	[Test]
	public void Harvest_Leaves_A_Redirtied_Key_Scheduled() {
		var state = new RecordingState();
		using var system = New(state, keyCapacity: 64, chunkCount: 4, chunkSize: 4);
		system.Start();

		for (var round = 0; round < 200; round++) {
			for (var k = 0; k < 6; k++)
				system.Dispatch(k, round);
		}

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 200 * 6);
		Assert.That(state.HandledCount, Is.EqualTo(200 * 6));
	}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test -c Release --filter "FullyQualifiedName~ActorSystemTests"`
Expected: `Harvest_Recycles_Chunks_Of_Drained_Keys` and `Harvest_Invokes_OnChunkReclaimed` fail — `Harvest` is the stub returning false, so the 5th distinct key blocks forever (the test times out via the watchdog).

- [ ] **Step 3: Implement `Harvest`**

Replace the `Harvest` stub in `ActorSystem.cs`:

```csharp
	/// <summary>
	///   Cold, producer-side sweep: reclaim the chunks of keys that have drained. Deliberately the same
	///   shape as <c>CoalescingKeyMap.EvictFree</c> — a state-gated scan, not a queue. v1 has no
	///   drain-hint ring; that is an optimization over this scan and would be the only multi-producer
	///   structure in the system.
	///   <para>
	///     Must not run after <see cref="TryComplete" />: §5.6's quiescence argument requires every
	///     producer schedule to be sequenced-before the completion flag, and this path can schedule.
	///   </para>
	/// </summary>
	private bool Harvest() {
		if (_completed) return false; // see the remark above — never harvest after completion

		var freed = false;
		var slots = _map.SlotCount;
		for (var slot = 0; slot < slots; slot++) {
			if (!_map.IsOccupied(slot)) continue;
			ref var cell = ref _map.RefAt(slot);
			if (cell.ChunkBase == Chunk.None) continue;
			if (Volatile.Read(ref cell.State) != CellState.Free) continue;

			// Claim the cell. A worker that is mid-release loses this CAS or wins it later; either way
			// the key cannot be processed while we hold Reclaiming.
			if (Interlocked.CompareExchange(ref cell.State, CellState.Reclaiming, CellState.Free) !=
				CellState.Free)
				continue;

			var head = Volatile.Read(ref cell.Head);
			if (Volatile.Read(ref cell.Tail) != head) {
				// It went dirty between the check and the claim. Releasing this is NOT a plain revert:
				// Reclaiming is a non-Free state, so a worker's release-recheck may have lost its CAS and
				// skipped scheduling. Restoring Free without re-testing the mailbox strands the key.
				ReleaseAndMaybeSchedule(slot, ref cell, head, fromWorker: false);
				continue;
			}

			_handler.OnChunkReclaimed(_slab.ChunkSpan(cell.ChunkBase));
			_slab.Return(cell.ChunkBase);
			cell.ChunkBase = Chunk.None;
			cell.Head = 0;
			cell.Tail = 0;
			cell.CachedHead = 0;
			Volatile.Write(ref cell.State, CellState.Free);
			freed = true;
		}

		return freed;
	}
```

`ReleaseAndMaybeSchedule` writes `cell.Head` and publishes the processed count — neither is correct
from the producer. Split the publish out so the producer path does not touch worker state. Replace
`ReleaseAndMaybeSchedule`:

```csharp
	private void ReleaseAndMaybeSchedule(int index, ref KeyCell<TKey> cell, int head, bool fromWorker) {
		if (fromWorker) {
			Volatile.Write(ref cell.Head, head);
			if (_producerParked) _spaceAvailable.Set();
			PublishProcessed();
		}

		Interlocked.Exchange(ref cell.State, CellState.Free);

		if (Volatile.Read(ref cell.Tail) == head) return;

		if (Interlocked.CompareExchange(ref cell.State, CellState.Scheduled, CellState.Free) != CellState.Free)
			return;

		if (fromWorker) PushLocal(index);
		else ScheduleFromProducer(index);
	}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test -c Release --filter "FullyQualifiedName~ActorSystemTests"`
Expected: all 28 PASS on both TFMs.

- [ ] **Step 5: Run in Debug**

Run: `dotnet test -c Debug --filter "FullyQualifiedName~ActorSystemTests"`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/experiments tests/Nanov.HighPerformance.Tests
git commit -m "feat(actors): cold chunk harvest and pool hooks"
```

---

### Task 11: Stress and race guards

The tests that will actually find bugs. Each targets a specific interleaving from the spec.

**Files:**
- Create: `tests/…/Threading/Actors/ActorSystemStressTests.cs`

**Interfaces:**
- Consumes: everything from Tasks 5–10

- [ ] **Step 1: Write the stress tests**

Create `tests/…/Threading/Actors/ActorSystemStressTests.cs`:

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Actors;

using Nanov.HighPerformance.Tests.Threading.Workers;
using Nanov.HighPerformance.Threading.Actors;
using NUnit.Framework;

/// <summary>
///   Race guards. Each targets a named interleaving from the design spec; a regression here is a real
///   concurrency bug, not a flake. All run under the stall watchdogs so a lost wakeup fails fast.
/// </summary>
[TestFixture]
public class ActorSystemStressTests {
	private static ActorSystem<int, int, RecordingHandler> New(RecordingState state,
		int keyCapacity = 128, int chunkCount = 32, int chunkSize = 8, int batchBound = 0) =>
		new(new RecordingHandler(state), keyCapacity, chunkCount, chunkSize, batchBound);

	/// <summary>
	///   §5.3 — the release race. One key, the producer appending continuously while the worker keeps
	///   releasing. A stranded message shows up as a stall; a double-schedule as a duplicate or a
	///   scheduler-ring overflow.
	/// </summary>
	[Test]
	public void Release_Race_Never_Strands_Or_Double_Schedules() {
		const int count = 300_000;
		var state = new RecordingState();
		using var system = New(state, chunkSize: 8, batchBound: 1); // batchBound 1 ⇒ maximum release churn
		system.Start();

		for (var i = 0; i < count; i++)
			Assert.That(system.Dispatch(1, i), Is.True, $"message {i}");

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, count);

		Assert.That(state.HandledCount, Is.EqualTo(count));
		var values = state.ByKey()[1];
		Assert.That(values, Has.Count.EqualTo(count));
		for (var i = 0; i < count; i++)
			if (values[i] != i)
				Assert.Fail($"order broke at {i}: got {values[i]}");
	}

	/// <summary>
	///   §5.6 — quiescence must never be observed while a message is still queued. The final dispatch is
	///   raced against TryComplete repeatedly; a false-quiescence bug drops that message.
	/// </summary>
	[Test]
	public void TryComplete_Racing_A_Final_Message_Never_Loses_It() {
		for (var round = 0; round < 200; round++) {
			var state = new RecordingState();
			using var system = New(state, chunkSize: 8);
			system.Start();

			const int count = 64;
			for (var i = 0; i < count; i++)
				system.Dispatch(i % 4, i);

			system.TryComplete();
			WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, count);
			Assert.That(state.HandledCount, Is.EqualTo(count), $"round {round} lost a message");
		}
	}

	/// <summary>§4.4 — the producer's harvest running against continuous worker release churn.</summary>
	[Test]
	public void Harvest_Racing_Release_Never_Strands() {
		const int keys = 64;
		const int perKey = 2_000;
		var state = new RecordingState();
		using var system = New(state, keyCapacity: 256, chunkCount: 8, chunkSize: 4, batchBound: 2);
		system.Start();

		for (var i = 0; i < perKey; i++)
			for (var k = 0; k < keys; k++)
				Assert.That(system.Dispatch(k, i), Is.True);

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, keys * perKey);

		Assert.That(state.HandledCount, Is.EqualTo(keys * perKey));
		foreach (var (key, values) in state.ByKey())
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");
	}

	/// <summary>Sustained mixed load — order and exactly-once across many keys under churn.</summary>
	[Test]
	public void Sustained_Mixed_Load_Preserves_Order_And_Loses_Nothing() {
		const int keys = 32;
		const int perKey = 5_000;
		var state = new RecordingState();
		using var system = New(state, keyCapacity: 256, chunkCount: 16, chunkSize: 16, batchBound: 3);
		system.Start();

		for (var i = 0; i < perKey; i++)
			for (var k = 0; k < keys; k++)
				Assert.That(system.Dispatch(k, i), Is.True);

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, keys * perKey);

		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key}");
	}

	/// <summary>
	///   Steady state must not allocate. The ring, slab and cells are allocated once at construction;
	///   dispatch and drain must add nothing per message.
	/// </summary>
	[Test]
	public void Steady_State_Dispatch_Does_Not_Allocate() {
		var state = new RecordingState { Throw = null };
		using var system = New(state, keyCapacity: 64, chunkCount: 16, chunkSize: 64);
		system.Start();

		// Warm up: bind chunks, JIT the paths.
		for (var i = 0; i < 2_000; i++)
			system.Dispatch(i % 8, i);
		WorkerTestUtil.AwaitProgress(() => state.HandledCount, 2_000);

		// RecordingHandler allocates (ConcurrentQueue nodes), so measure the PRODUCER only, with the
		// worker idle: this isolates Dispatch's own allocation.
		var before = GC.GetAllocatedBytesForCurrentThread();
		for (var i = 0; i < 10_000; i++)
			system.TryDispatch(1, i);
		var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

		Assert.That(allocated, Is.EqualTo(0), $"Dispatch allocated {allocated} bytes over 10k calls");
	}
}
```

- [ ] **Step 2: Run the stress suite in Release**

Run: `dotnet test -c Release --filter "FullyQualifiedName~ActorSystemStressTests"`
Expected: all 5 PASS on both TFMs. A stall means a lost wakeup or a stranded key; a count mismatch means the release or harvest protocol is wrong.

- [ ] **Step 3: Run the stress suite in Debug**

Run: `dotnet test -c Debug --filter "FullyQualifiedName~ActorSystemStressTests"`
Expected: all PASS.

- [ ] **Step 4: Flake check — five consecutive clean runs**

Run:

```bash
for i in 1 2 3 4 5; do
  dotnet test -c Release --filter "FullyQualifiedName~Threading.Actors" || { echo "FAILED on run $i"; break; }
done
```

Expected: five clean runs. Concurrency bugs here are probabilistic; a single green run proves little.

- [ ] **Step 5: Full suite and clean build**

Run: `dotnet build -c Release`
Expected: 0 warnings.

Run: `dotnet test -c Release`
Expected: the entire repo suite passes — the existing Workers tests must be unaffected.

- [ ] **Step 6: Commit**

```bash
git add tests/Nanov.HighPerformance.Tests
git commit -m "test(actors): release, quiescence and harvest race guards"
```

---

## Phase 1 done — what comes next

At this point there is a working single-worker, key-ordered processor with the full ownership
protocol, backpressure, termination detection and storage recycling. Phase 2 (§13 steps 4–5) adds the
`IKeyScheduler` seam and the two schedulers, and phase 3 (step 6) the benchmark harness.

### Spec sections deliberately not covered by this plan

| Spec | Why deferred |
| --- | --- |
| §6 scheduler seam, §6.1 routed, §6.2 work stealing | phase 2 — the whole point of phases 4–5 |
| §8 observability (`IWorkerQueue`, steal/park counters) | the counters that matter are per-worker and per-steal; both need phase 2. Phase 1 exposes `ScheduledCountApprox` and `DispatchedCount` as internals for tests only |
| §9 tests 4 (serial consumer handoff), 7 (`batchBound` fairness across keys), 8 (steal correctness) | all require **more than one worker** |
| §10 benchmark plan and baselines | phase 3 |
| §11 v2 chained chunks, design C | out of scope by design |

### Two things phase 2 will need that phase 1 deliberately leaves alone

- **The scheduler seam does not exist yet.** `ActorSystem` calls `ScheduleFromProducer`/`PushLocal`
  directly. Phase 2 extracts `IKeyScheduler` and makes the system generic over `TScheduler`; the
  phase-1 arrangement (producer ring + worker-private local stack) becomes the routed scheduler's
  per-worker structure unchanged.
- **`_processedPublished` is a single counter.** Phase 2 makes it a padded per-worker array and the
  quiescence check a sum, exactly as §5.6 specifies. The read order (`_completed` first) does not
  change.
