# Mini Actor System — Phase 2b: Scheduler D (work stealing) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement scheduler D — a pull-side work-stealing scheduler — behind the existing
`IKeyScheduler` seam, and prove the B-vs-D behavioural asymmetry the spec asserts but has never
measured.

**Architecture:** A bounded MPMC global submission queue (Vyukov per-slot sequence stamps) plus one
fixed-size Chase-Lev deque per worker. Workers pop their own deque bottom (LIFO, affinity), fall
back to the global queue, then steal one key from a random victim's top. Idle workers park on the
same arm-then-recheck protocol B uses; schedulers wake exactly one sleeper per enqueue.

**Tech Stack:** C# / .NET, multi-target net9.0 + net10.0, NUnit 4.

## Context

Phase 1 built the cell state machine, slab, and single-worker path. Phase 2a built the
`IKeyScheduler` seam and scheduler B (`RoutedScheduler`). Both are green: 73/73 on both TFMs.

**Read before starting:**
- `docs/superpowers/specs/2026-08-12-mini-actor-system-design.md` §6 (scheduler seam), §6.2 (D),
  §8 (observability), §9 (test plan), §13 (implementation order).
- `src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/CLAUDE.md` if present,
  and the worker-tree `CLAUDE.md` invariants.
- `Schedulers/RoutedScheduler.cs` — the reference implementation of this seam. D mirrors its
  parking, wake, and reuse-guard conventions.

**The load-bearing invariant (spec §3.1):** a key index is in at most one place at any instant, and
per-key ordering comes from exclusive cell ownership, *not* from queue discipline. This is why D may
use LIFO, steal from the top, and hand keys out of order without affecting correctness. Every queue
in this plan may therefore be sized to the key map's slot count and never overflow.

## Global Constraints

- Central package management: add/upgrade deps in `Directory.Packages.props` (`<PackageVersion>`).
  **Never** add `Version=` to a `.csproj`. Never set a version property in a project.
- `TreatWarningsAsErrors=true`, `Nullable=enable`, `ImplicitUsings=enable`, `AllowUnsafeBlocks=true`,
  `EnforceCodeStyleInBuild=true`. A warning fails the build.
- **Never** silence a warning with `<NoWarn>`, an `.editorconfig` suppression, or by disabling
  `GenerateDocumentationFile`. Fix the cause. In particular: CS0414 (field assigned but never used)
  means the field is dead — delete it, do not suppress it.
- Style (`.editorconfig`): tabs, `tab_width = 2`; file-scoped namespaces; `using` directives
  **inside** the namespace; K&R braces; no braces for single-line statements; `var`; no `this.`;
  `_camelCase` private fields; `csharp_style_prefer_primary_constructors = false` (write explicit
  constructors).
- Tests are **NUnit 4**. `[Timeout]` is `[Obsolete]`. `[CancelAfter]` does **not** abort a blocking
  test — it arms a token nothing polls. Any test that could hang must protect itself with a bounded
  `Thread.Join(TimeSpan)` on a background producer, or `WorkerTestUtil.AwaitCompletionOrStall`.
- `using NUnit.Framework;` is **not** in global usings — every test file declares it.
- Do not touch `CLAUDE.md`, `.claude/`, or `docs/agents/` — unrelated pre-existing local changes.
- `[StructLayout(LayoutKind.Explicit)]` is illegal on generic types. Reuse the non-generic
  `Internal/PaddedLong.cs` for padded counters and positions.
- Run tests in the **foreground**. Do not background a test run and poll it.
- Verify with `dotnet build -c Release` (must report 0 warnings) and
  `dotnet test -c Release --filter "FullyQualifiedName~Threading.Actors"` (both TFMs).

---

## File Structure

| Path | Responsibility |
| --- | --- |
| `Internal/WorkStealingDeque.cs` | Chase-Lev deque, fixed buffer. Owner pushes/pops bottom; thieves CAS the top. |
| `Internal/GlobalKeyQueue.cs` | Bounded MPMC queue of key indices, Vyukov per-slot sequence stamps. |
| `Schedulers/WorkStealingScheduler.cs` | Scheduler D — implements `IKeyScheduler`. |
| `tests/…/Actors/WorkStealingDequeTests.cs` | Deque unit + concurrent stress. |
| `tests/…/Actors/GlobalKeyQueueTests.cs` | Global queue unit + concurrent stress. |
| `tests/…/Actors/WorkStealingSchedulerTests.cs` | D-specific: wake protocol, steal counters, overflow. |
| `tests/…/Actors/SchedulerBehaviourTests.cs` | Generic fixture — the core behavioural suite run against **both** schedulers. |
| `tests/…/Actors/StarvationAsymmetryTests.cs` | The B-vs-D asymmetry §6.1 asserts. |

Production source root for this plan:
`src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/`
Test root: `tests/Nanov.HighPerformance.Tests/Threading/Actors/`

---

### Task 1: Chase-Lev work-stealing deque

**Files:**
- Create: `src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/Internal/WorkStealingDeque.cs`
- Test: `tests/Nanov.HighPerformance.Tests/Threading/Actors/WorkStealingDequeTests.cs`

**Interfaces:**
- Consumes: `Internal/PaddedLong.cs` (existing).
- Produces: `internal sealed class WorkStealingDeque` with
  `WorkStealingDeque(int capacity)`, `int Capacity`, `int CountApprox`,
  `bool TryPushBottom(int value)`, `bool TryPopBottom(out int value)`, `bool TrySteal(out int value)`.

**Memory ordering — use the 2013 paper, not the 2005 one.** Lê / Pop / Cohen / Zappa Nardelli,
*Correct and Efficient Work-Stealing for Weak Memory Models* (PPoPP 2013). Apple Silicon will find
the difference. The two fences that matter:
- `TryPopBottom` must issue a **full StoreLoad fence** (`Interlocked.MemoryBarrier()`) between
  storing the decremented `bottom` and loading `top`. `Volatile.Write` does **not** order a store
  before a load of a *different* location on ARM64.
- `TrySteal` must issue a full fence between loading `top` and loading `bottom`.

**Why a fixed buffer is safe here.** The classic deque grows because a task queue is unbounded. Ours
is not: a key index is in at most one place at a time (§3.1), so the number in flight is bounded by
the key map. `TryPushBottom` returns `false` when full and the caller spills to the global queue —
what `ThreadPoolWorkQueue` does. This also closes the only correctness hole a fixed buffer would
otherwise have: a thief that has read `_buffer[t & mask]` but not yet won its CAS cannot have that
slot overwritten, because overwriting it requires `bottom - top >= capacity` while `top` is still
`t`, and `TryPushBottom` refuses in exactly that case.

- [ ] **Step 1: Write the failing tests**

Create `tests/Nanov.HighPerformance.Tests/Threading/Actors/WorkStealingDequeTests.cs`:

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Actors;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors.Internal;
using NUnit.Framework;

[TestFixture]
public class WorkStealingDequeTests {
	[Test]
	public void Owner_Pops_Its_Own_Pushes_In_Lifo_Order() {
		var deque = new WorkStealingDeque(16);
		for (var i = 0; i < 5; i++)
			Assert.That(deque.TryPushBottom(i), Is.True);

		var popped = new List<int>();
		while (deque.TryPopBottom(out var value))
			popped.Add(value);

		Assert.That(popped, Is.EqualTo(new List<int> { 4, 3, 2, 1, 0 }));
	}

	[Test]
	public void Thieves_Take_From_The_Oldest_End() {
		var deque = new WorkStealingDeque(16);
		for (var i = 0; i < 5; i++)
			deque.TryPushBottom(i);

		var stolen = new List<int>();
		while (deque.TrySteal(out var value))
			stolen.Add(value);

		Assert.That(stolen, Is.EqualTo(new List<int> { 0, 1, 2, 3, 4 }),
			"the owner takes the newest end so its cache stays warm; thieves take the oldest");
	}

	[Test]
	public void Push_Fails_When_Full_So_The_Caller_Can_Spill_To_The_Global_Queue() {
		var deque = new WorkStealingDeque(4);
		for (var i = 0; i < deque.Capacity; i++)
			Assert.That(deque.TryPushBottom(i), Is.True);

		Assert.That(deque.TryPushBottom(99), Is.False);
	}

	[Test]
	public void Empty_Deque_Yields_Nothing_To_Either_End() {
		var deque = new WorkStealingDeque(8);
		Assert.Multiple(() => {
			Assert.That(deque.TryPopBottom(out _), Is.False);
			Assert.That(deque.TrySteal(out _), Is.False);
			Assert.That(deque.CountApprox, Is.EqualTo(0));
		});

		// A failed pop must leave `bottom` where it found it, or the next push writes the wrong slot.
		deque.TryPushBottom(7);
		Assert.That(deque.TryPopBottom(out var value), Is.True);
		Assert.That(value, Is.EqualTo(7));
	}

	/// <summary>
	///   The single-element race: owner and thief both go for the last item. Exactly one may win.
	///   Run enough rounds that the interleaving is actually sampled — this is the case the CAS in
	///   <c>TryPopBottom</c> exists for, and a deque that gets it wrong hands the SAME key to two
	///   workers, which breaks the one-owner invariant the whole system rests on.
	/// </summary>
	[Test]
	public void Owner_And_Thief_Never_Both_Take_The_Last_Item() {
		const int rounds = 20_000;
		var deque = new WorkStealingDeque(4);
		var doubleTaken = 0;
		var lost = 0;

		for (var round = 0; round < rounds; round++) {
			deque.TryPushBottom(round);
			var start = new ManualResetEventSlim(false);
			var ownerGot = false;
			var thiefGot = false;

			var thief = new Thread(() => {
				start.Wait();
				thiefGot = deque.TrySteal(out _);
			}) { IsBackground = true };
			thief.Start();

			start.Set();
			ownerGot = deque.TryPopBottom(out _);
			Assert.That(thief.Join(TimeSpan.FromSeconds(5)), Is.True, "thief wedged");

			if (ownerGot && thiefGot) doubleTaken++;
			if (!ownerGot && !thiefGot) lost++;
			while (deque.TryPopBottom(out _)) { }
		}

		Assert.Multiple(() => {
			Assert.That(doubleTaken, Is.EqualTo(0), "the same key was handed to two workers");
			Assert.That(lost, Is.EqualTo(0), "an item vanished — neither end got it");
		});
	}

	/// <summary>
	///   Sustained concurrent stress: one owner pushing and popping while three thieves steal.
	///   The oracle is set equality — every pushed value taken exactly once, by exactly one taker.
	/// </summary>
	[Test]
	public void Concurrent_Owner_And_Thieves_Lose_And_Duplicate_Nothing() {
		const int items = 200_000;
		const int thieves = 3;
		var deque = new WorkStealingDeque(256);
		var ownerTaken = new List<int>();
		var thiefTaken = new List<int>[thieves];
		var done = false;

		var workers = new Thread[thieves];
		for (var t = 0; t < thieves; t++) {
			var id = t;
			thiefTaken[id] = new List<int>();
			workers[id] = new Thread(() => {
				while (!Volatile.Read(ref done))
					if (deque.TrySteal(out var value)) thiefTaken[id].Add(value);
				while (deque.TrySteal(out var value)) thiefTaken[id].Add(value);
			}) { IsBackground = true };
			workers[id].Start();
		}

		var pushed = 0;
		while (pushed < items) {
			if (deque.TryPushBottom(pushed)) {
				pushed++;
				continue;
			}

			// Full — drain one from our own end so the producer side makes progress.
			if (deque.TryPopBottom(out var mine)) ownerTaken.Add(mine);
		}

		while (deque.TryPopBottom(out var mine)) ownerTaken.Add(mine);
		Volatile.Write(ref done, true);
		foreach (var worker in workers)
			Assert.That(worker.Join(TimeSpan.FromSeconds(30)), Is.True, "thief wedged");

		while (deque.TryPopBottom(out var mine)) ownerTaken.Add(mine);

		var all = ownerTaken.Concat(thiefTaken.SelectMany(x => x)).ToList();
		Assert.Multiple(() => {
			Assert.That(all, Has.Count.EqualTo(items), "items lost or duplicated");
			Assert.That(all.Distinct().Count(), Is.EqualTo(items), "an item was taken twice");
		});
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test -c Release --filter "FullyQualifiedName~WorkStealingDequeTests"`
Expected: FAIL — `WorkStealingDeque` does not exist (CS0246).

- [ ] **Step 3: Implement the deque**

Create `src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/Internal/WorkStealingDeque.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Actors.Internal;

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
///   A Chase-Lev work-stealing deque of key indices, over a FIXED buffer.
///   <para>
///     The owner pushes and pops the <b>bottom</b> — LIFO, and on the uncontended path atomic-free
///     apart from one fence. Thieves take the <b>top</b> and arbitrate with a CAS. Owner and thieves
///     only contend when exactly one item remains, which is the case
///     <see cref="TryPopBottom" />'s CAS exists to settle.
///   </para>
///   <para>
///     <b>Memory ordering follows Lê / Pop / Cohen / Zappa Nardelli (2013), not the 2005 paper.</b>
///     Two full StoreLoad fences are load-bearing and neither can be a <c>Volatile</c> write: on
///     ARM64 a release store is not ordered against a subsequent load of a DIFFERENT location, and
///     both the pop and the steal do exactly that — store or load one cursor, then load the other.
///     Drop either fence and owner and thief can both take the last item, handing one key to two
///     workers and breaking the exclusive-ownership invariant the whole system rests on.
///   </para>
///   <para>
///     <b>Fixed buffer, no growth.</b> A key index is in at most one place at a time, so the number
///     in flight is bounded by the key map; <see cref="TryPushBottom" /> returns <see langword="false" />
///     when full and the caller spills to the global queue, the same escape
///     <c>ThreadPoolWorkQueue</c> uses. That refusal is also what makes the fixed buffer SAFE: a
///     thief that has read <c>_buffer[top]</c> but not yet won its CAS cannot have that slot
///     overwritten, because overwriting it needs <c>bottom - top &gt;= capacity</c> while <c>top</c>
///     is still at the value the thief read — precisely when the push refuses.
///   </para>
/// </summary>
internal sealed class WorkStealingDeque {
	private readonly int[] _buffer;
	private readonly int _mask;

	// Padded apart: the owner writes _bottom on every push and pop while thieves CAS _top. Sharing a
	// line would put a steal attempt in the owner's write path on every single push.
	private PaddedLong _top;
	private PaddedLong _bottom;

	internal WorkStealingDeque(int capacity) {
		var n = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(capacity, 2));
		_buffer = new int[n];
		_mask = n - 1;
	}

	internal int Capacity => _buffer.Length;

	/// <summary>Approximate depth. Racy by design — used to decide whether stealing is worth a scan.</summary>
	internal int CountApprox {
		get {
			var bottom = Volatile.Read(ref _bottom.Value);
			var top = Volatile.Read(ref _top.Value);
			var count = bottom - top;
			return count < 0 ? 0 : (int)count;
		}
	}

	/// <summary>
	///   OWNER ONLY. Returns <see langword="false" /> when full so the caller can spill to the global
	///   queue.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool TryPushBottom(int value) {
		var bottom = _bottom.Value; // owner-private
		var top = Volatile.Read(ref _top.Value);
		if (bottom - top >= _buffer.Length) return false;
		_buffer[(int)(bottom & _mask)] = value;
		Volatile.Write(ref _bottom.Value, bottom + 1); // release: publishes the slot write
		return true;
	}

	/// <summary>OWNER ONLY. Takes the newest item — the one whose cell lines are warmest.</summary>
	internal bool TryPopBottom(out int value) {
		var bottom = _bottom.Value - 1;
		Volatile.Write(ref _bottom.Value, bottom);

		// The load-bearing fence. Without it the load of _top below can be reordered ahead of the
		// store to _bottom above, and a thief racing for the last item sees a stale bottom while we
		// see a stale top — both take it.
		Interlocked.MemoryBarrier();

		var top = Volatile.Read(ref _top.Value);
		if (top > bottom) {
			// Empty. Restore bottom, or the next push writes the wrong slot.
			Volatile.Write(ref _bottom.Value, bottom + 1);
			value = 0;
			return false;
		}

		value = _buffer[(int)(bottom & _mask)];
		if (top != bottom) return true; // more than one left: no thief can be after this slot

		// Exactly one left, and a thief may be taking it right now. The CAS settles it.
		var won = Interlocked.CompareExchange(ref _top.Value, top + 1, top) == top;
		Volatile.Write(ref _bottom.Value, bottom + 1); // the deque is empty either way
		if (won) return true;
		value = 0;
		return false;
	}

	/// <summary>ANY THIEF. Takes the oldest item; one key per steal (see the type docs on granularity).</summary>
	internal bool TrySteal(out int value) {
		var top = Volatile.Read(ref _top.Value);

		// Second load-bearing fence: _bottom must be loaded AFTER _top, and on ARM64 two acquire
		// loads of different locations do not give that on their own.
		Interlocked.MemoryBarrier();

		var bottom = Volatile.Read(ref _bottom.Value);
		if (top >= bottom) {
			value = 0;
			return false;
		}

		value = _buffer[(int)(top & _mask)];
		if (Interlocked.CompareExchange(ref _top.Value, top + 1, top) == top) return true;

		// Lost to the owner or another thief. Report empty-handed; the caller moves to the next victim.
		value = 0;
		return false;
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dotnet build -c Release` (expect 0 warnings), then
`dotnet test -c Release --filter "FullyQualifiedName~WorkStealingDequeTests"`
Expected: PASS on both TFMs.

- [ ] **Step 5: Commit**

```bash
git add src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/Internal/WorkStealingDeque.cs \
        tests/Nanov.HighPerformance.Tests/Threading/Actors/WorkStealingDequeTests.cs
git commit -m "feat(actors): Chase-Lev work-stealing deque over a fixed buffer"
```

---

### Task 2: Bounded MPMC global submission queue

**Files:**
- Create: `src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/Internal/GlobalKeyQueue.cs`
- Test: `tests/Nanov.HighPerformance.Tests/Threading/Actors/GlobalKeyQueueTests.cs`

**Interfaces:**
- Consumes: `Internal/PaddedLong.cs`.
- Produces: `internal sealed class GlobalKeyQueue` with `GlobalKeyQueue(int capacity)`,
  `int Capacity`, `int CountApprox`, `bool TryEnqueue(int value)`, `bool TryDequeue(out int value)`.

**Why MPMC and not SPMC.** Spec §6.2 describes the global queue as producer-single. It is not: a
worker whose deque is full spills to the global queue too, so there are `workers + 1` possible
enqueuers. Making the enqueue side CAS-arbitrated costs one CAS on a path that runs once per
*activation* (not per message) and removes a sizing invariant that would otherwise be silent to
violate. **Record this deviation in Task 7's spec update.**

**Never fills.** Sized to the key map's slot count. A key index is in at most one place, so total
indices in flight is bounded by the map (§6). The `false` return is a real guard against a
mis-sizing, not a backpressure path.

- [ ] **Step 1: Write the failing tests**

Create `tests/Nanov.HighPerformance.Tests/Threading/Actors/GlobalKeyQueueTests.cs`:

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Actors;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors.Internal;
using NUnit.Framework;

[TestFixture]
public class GlobalKeyQueueTests {
	[Test]
	public void Dequeues_In_Fifo_Order() {
		var queue = new GlobalKeyQueue(16);
		for (var i = 0; i < 8; i++)
			Assert.That(queue.TryEnqueue(i), Is.True);

		var seen = new List<int>();
		while (queue.TryDequeue(out var value))
			seen.Add(value);

		Assert.That(seen, Is.EqualTo(Enumerable.Range(0, 8).ToList()));
	}

	[Test]
	public void Refuses_To_Enqueue_When_Full() {
		var queue = new GlobalKeyQueue(4);
		for (var i = 0; i < queue.Capacity; i++)
			Assert.That(queue.TryEnqueue(i), Is.True);

		Assert.That(queue.TryEnqueue(99), Is.False);
	}

	[Test]
	public void Empty_Queue_Yields_Nothing() {
		var queue = new GlobalKeyQueue(8);
		Assert.Multiple(() => {
			Assert.That(queue.TryDequeue(out _), Is.False);
			Assert.That(queue.CountApprox, Is.EqualTo(0));
		});
	}

	[Test]
	public void Slots_Are_Reusable_After_A_Full_Wrap() {
		var queue = new GlobalKeyQueue(4);
		for (var round = 0; round < 100; round++) {
			for (var i = 0; i < queue.Capacity; i++)
				Assert.That(queue.TryEnqueue(round * 10 + i), Is.True, $"enqueue failed on round {round}");
			for (var i = 0; i < queue.Capacity; i++) {
				Assert.That(queue.TryDequeue(out var value), Is.True, $"dequeue failed on round {round}");
				Assert.That(value, Is.EqualTo(round * 10 + i));
			}
		}
	}

	/// <summary>
	///   Many enqueuers, many dequeuers — the real shape, since a worker with a full deque spills
	///   here alongside the producer. Oracle is set equality: nothing lost, nothing handed out twice.
	/// </summary>
	[Test]
	public void Concurrent_Producers_And_Consumers_Lose_And_Duplicate_Nothing() {
		const int producers = 3;
		const int consumers = 4;
		const int perProducer = 50_000;
		const int total = producers * perProducer;
		var queue = new GlobalKeyQueue(1024);
		var taken = new List<int>[consumers];
		var producersDone = 0;

		var consumerThreads = new Thread[consumers];
		for (var c = 0; c < consumers; c++) {
			var id = c;
			taken[id] = new List<int>();
			consumerThreads[id] = new Thread(() => {
				while (Volatile.Read(ref producersDone) < producers)
					if (queue.TryDequeue(out var value)) taken[id].Add(value);
				while (queue.TryDequeue(out var value)) taken[id].Add(value);
			}) { IsBackground = true };
			consumerThreads[id].Start();
		}

		var producerThreads = new Thread[producers];
		for (var p = 0; p < producers; p++) {
			var id = p;
			producerThreads[id] = new Thread(() => {
				for (var i = 0; i < perProducer; i++) {
					var value = id * perProducer + i;
					while (!queue.TryEnqueue(value)) Thread.SpinWait(16); // full: consumers will drain
				}

				Interlocked.Increment(ref producersDone);
			}) { IsBackground = true };
			producerThreads[id].Start();
		}

		foreach (var thread in producerThreads)
			Assert.That(thread.Join(TimeSpan.FromSeconds(60)), Is.True, "producer wedged");
		foreach (var thread in consumerThreads)
			Assert.That(thread.Join(TimeSpan.FromSeconds(60)), Is.True, "consumer wedged");

		var all = taken.SelectMany(x => x).ToList();
		Assert.Multiple(() => {
			Assert.That(all, Has.Count.EqualTo(total), "items lost or duplicated");
			Assert.That(all.Distinct().Count(), Is.EqualTo(total), "an item was handed out twice");
		});
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test -c Release --filter "FullyQualifiedName~GlobalKeyQueueTests"`
Expected: FAIL — `GlobalKeyQueue` does not exist (CS0246).

- [ ] **Step 3: Implement the queue**

Create `src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/Internal/GlobalKeyQueue.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Actors.Internal;

using System;
using System.Numerics;
using System.Threading;

/// <summary>
///   Bounded multi-producer / multi-consumer queue of key indices, with per-slot sequence stamps
///   (Vyukov). Scheduler D's global submission queue.
///   <para>
///     <b>Why MPMC and not SPMC.</b> The spec describes this edge as producer-single, but a worker
///     whose deque is full spills here as well, so there are <c>workers + 1</c> possible enqueuers.
///     The enqueue side therefore CASes too. It costs one CAS on a path that runs once per
///     ACTIVATION rather than per message, and it removes a sizing invariant that would otherwise
///     be silent to violate.
///   </para>
///   <para>
///     <b>It cannot fill in normal operation.</b> Sized to the key map's slot count; a key index is
///     in at most one place at a time, so the number in flight is bounded by the map. The
///     <see langword="false" /> return is a guard against a mis-sized construction, not backpressure.
///   </para>
///   <para>
///     <b>The sequence stamp is the whole protocol.</b> A slot is writable when its sequence equals
///     the enqueue position, readable when it is one past the dequeue position, and is handed back
///     to the next lap by stamping it <c>pos + capacity</c>. Ownership of a slot therefore never
///     depends on the cursors alone, which is what makes a CAS on the cursor sufficient to claim the
///     slot without a lock.
///   </para>
/// </summary>
internal sealed class GlobalKeyQueue {
	private struct Cell {
		internal long Sequence;
		internal int Value;
	}

	private readonly Cell[] _cells;
	private readonly int _mask;

	// Padded apart: enqueuers and dequeuers hammer these from different cores.
	private PaddedLong _enqueuePos;
	private PaddedLong _dequeuePos;

	internal GlobalKeyQueue(int capacity) {
		var n = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(capacity, 2));
		_cells = new Cell[n];
		for (var i = 0; i < n; i++)
			_cells[i].Sequence = i;
		_mask = n - 1;
	}

	internal int Capacity => _cells.Length;

	/// <summary>Approximate depth. Racy by design — read before parking, never for correctness.</summary>
	internal int CountApprox {
		get {
			var count = Volatile.Read(ref _enqueuePos.Value) - Volatile.Read(ref _dequeuePos.Value);
			return count < 0 ? 0 : (int)count;
		}
	}

	internal bool TryEnqueue(int value) {
		while (true) {
			var pos = Volatile.Read(ref _enqueuePos.Value);
			ref var cell = ref _cells[(int)(pos & _mask)];
			var difference = Volatile.Read(ref cell.Sequence) - pos;
			if (difference == 0) {
				if (Interlocked.CompareExchange(ref _enqueuePos.Value, pos + 1, pos) != pos) continue;
				cell.Value = value;
				Volatile.Write(ref cell.Sequence, pos + 1); // release: publishes Value to the consumer
				return true;
			}

			if (difference < 0) return false; // full: the slot a lap behind has not been consumed yet
			// difference > 0: another enqueuer already took this position. Re-read and retry.
		}
	}

	internal bool TryDequeue(out int value) {
		while (true) {
			var pos = Volatile.Read(ref _dequeuePos.Value);
			ref var cell = ref _cells[(int)(pos & _mask)];
			var difference = Volatile.Read(ref cell.Sequence) - (pos + 1);
			if (difference == 0) {
				if (Interlocked.CompareExchange(ref _dequeuePos.Value, pos + 1, pos) != pos) continue;
				value = cell.Value;
				Volatile.Write(ref cell.Sequence, pos + _mask + 1); // hand the slot to the next lap
				return true;
			}

			if (difference < 0) { // empty: the producer has not published this slot yet
				value = 0;
				return false;
			}

			// difference > 0: another consumer already took this position. Re-read and retry.
		}
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dotnet build -c Release` (0 warnings), then
`dotnet test -c Release --filter "FullyQualifiedName~GlobalKeyQueueTests"`
Expected: PASS on both TFMs.

- [ ] **Step 5: Commit**

```bash
git add src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/Internal/GlobalKeyQueue.cs \
        tests/Nanov.HighPerformance.Tests/Threading/Actors/GlobalKeyQueueTests.cs
git commit -m "feat(actors): bounded MPMC global key queue with per-slot sequence stamps"
```

---

### Task 3: `WorkStealingScheduler` — scheduler D behind the seam

**Files:**
- Create: `src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/Schedulers/WorkStealingScheduler.cs`
- Test: `tests/Nanov.HighPerformance.Tests/Threading/Actors/WorkStealingSchedulerTests.cs`

**Interfaces:**
- Consumes: `IKeyScheduler` (existing — `Start(int workerCount, Action<Exception> onFault)`,
  `Schedule(int)`, `ScheduleLocal(int, int)`, `TryAcquire(int, out int)`, `HasWork(int)`,
  `WakeAll()`, `SignalStop()`, `ArmWait(int)`, `Wait(int)`); `WorkStealingDeque`; `GlobalKeyQueue`.
- Produces: `public struct WorkStealingScheduler : IKeyScheduler` with settable
  `int Capacity` (global queue; default 1024), `int DequeCapacity` (per worker; default 256), and
  read-only counters `long StealsAttempted`, `long StealsSucceeded`, `long GlobalDequeues`,
  `long DequeOverflows`, `long Parks`.

**Four things that are easy to get wrong here — get them right the first time:**

1. **`HasWork` must consider PEER deques.** `ActorSystem.WaitForWork` parks a worker when `HasWork`
   is false. If `HasWork` only looks at this worker's own deque and the global queue, then a worker
   holding ten stealable keys leaves every other worker asleep, and D degenerates into B without a
   router. This is not a hang — quiescence still resolves — but it silently deletes the entire point
   of the scheduler, and no correctness test would notice.

2. **`Schedule` and `ScheduleLocal` need a StoreLoad fence between publishing and reading the parked
   flags.** Same Dekker handshake as `RoutedScheduler.Schedule`. Without it, the enqueue can be
   reordered after the flag read: we see nobody parked and skip the wake, while the sleeper's own
   armed recheck sees a not-yet-visible queue and blocks. On the last dispatch before `TryComplete`
   that is a permanent hang.

3. **`ArmWait` must `Reset` the event BEFORE raising the parked flag.** Reversed, a `Set` landing
   between the two is erased by the `Reset` and the wake is lost.

4. **D owns no threads, so it has no `Stopping` flag and never invokes `onFault`.** The fault channel
   exists for scheduler-owned threads (B's router). Do not add a `Stopping` field "for symmetry" —
   an assigned-but-never-read field is CS0414 and fails the build. Ignore the `onFault` parameter and
   document why.

- [ ] **Step 1: Write the failing tests**

Create `tests/Nanov.HighPerformance.Tests/Threading/Actors/WorkStealingSchedulerTests.cs`:

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Actors;

using System;
using System.Linq;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using Nanov.HighPerformance.Tests.Threading.Workers;
using NUnit.Framework;

[TestFixture]
public class WorkStealingSchedulerTests {
	[Test]
	public void Rejects_A_Second_System() {
		var scheduler = new WorkStealingScheduler();
		scheduler.Start(2, _ => { });
		Assert.Throws<InvalidOperationException>(() => scheduler.Start(2, _ => { }),
			"one scheduler instance drives one system — sharing it would put two owners on a deque");
	}

	[Test]
	public void A_Worker_Takes_Its_Own_Local_Schedule_Before_The_Global_Queue() {
		var scheduler = new WorkStealingScheduler { Capacity = 64, DequeCapacity = 64 };
		scheduler.Start(1, _ => { });
		scheduler.Schedule(11);      // global
		scheduler.ScheduleLocal(0, 22); // this worker's deque

		Assert.That(scheduler.TryAcquire(0, out var first), Is.True);
		Assert.That(first, Is.EqualTo(22), "local first — that is where the affinity comes from");
		Assert.That(scheduler.TryAcquire(0, out var second), Is.True);
		Assert.That(second, Is.EqualTo(11));
		Assert.That(scheduler.TryAcquire(0, out _), Is.False);
	}

	[Test]
	public void An_Idle_Worker_Steals_From_A_Busy_Peer() {
		var scheduler = new WorkStealingScheduler { Capacity = 64, DequeCapacity = 64 };
		scheduler.Start(2, _ => { });
		scheduler.ScheduleLocal(0, 7);

		Assert.That(scheduler.HasWork(1), Is.True,
			"worker 1 must SEE a peer's stealable key, or it parks and D degenerates into B");
		Assert.That(scheduler.TryAcquire(1, out var stolen), Is.True);
		Assert.Multiple(() => {
			Assert.That(stolen, Is.EqualTo(7));
			Assert.That(scheduler.StealsSucceeded, Is.GreaterThan(0));
		});
	}

	[Test]
	public void A_Full_Deque_Spills_To_The_Global_Queue() {
		var scheduler = new WorkStealingScheduler { Capacity = 256, DequeCapacity = 4 };
		scheduler.Start(1, _ => { });

		for (var i = 0; i < 16; i++)
			scheduler.ScheduleLocal(0, i);

		Assert.That(scheduler.DequeOverflows, Is.GreaterThan(0), "the overflow path never ran");

		var taken = new System.Collections.Generic.List<int>();
		while (scheduler.TryAcquire(0, out var value))
			taken.Add(value);

		Assert.That(taken.OrderBy(x => x), Is.EqualTo(Enumerable.Range(0, 16)),
			"spilled keys must still be acquirable — order is irrelevant, loss is not");
	}

	/// <summary>
	///   The wake edge. A parked worker must be released by a schedule from another thread. This is
	///   the Dekker handshake between the enqueue and the parked-flag read; get the fence wrong and
	///   this test hangs rather than fails, which is why it is bounded by a join.
	/// </summary>
	[Test]
	public void A_Parked_Worker_Is_Woken_By_A_Schedule() {
		var scheduler = new WorkStealingScheduler { Capacity = 64, DequeCapacity = 64 };
		scheduler.Start(2, _ => { });

		var acquired = -1;
		var parked = new ManualResetEventSlim(false);
		var worker = new Thread(() => {
			scheduler.ArmWait(0);
			Interlocked.MemoryBarrier();
			parked.Set();
			scheduler.Wait(0);
			if (scheduler.TryAcquire(0, out var value)) Volatile.Write(ref acquired, value);
		}) { IsBackground = true };
		worker.Start();

		Assert.That(parked.Wait(TimeSpan.FromSeconds(5)), Is.True);
		Thread.Sleep(50); // let it actually reach Wait
		scheduler.Schedule(42);

		Assert.That(worker.Join(TimeSpan.FromSeconds(10)), Is.True,
			"the parked worker was never woken — lost wakeup on the schedule path");
		Assert.That(Volatile.Read(ref acquired), Is.EqualTo(42));
	}

	[Test]
	public void Signal_Stop_Releases_Every_Parked_Worker() {
		var scheduler = new WorkStealingScheduler { Capacity = 64, DequeCapacity = 64 };
		scheduler.Start(3, _ => { });

		var released = 0;
		var threads = new Thread[3];
		for (var i = 0; i < threads.Length; i++) {
			var id = i;
			threads[id] = new Thread(() => {
				scheduler.ArmWait(id);
				scheduler.Wait(id);
				Interlocked.Increment(ref released);
			}) { IsBackground = true };
			threads[id].Start();
		}

		Thread.Sleep(100);
		scheduler.SignalStop();
		foreach (var thread in threads)
			Assert.That(thread.Join(TimeSpan.FromSeconds(10)), Is.True, "a worker was left parked");

		Assert.That(Volatile.Read(ref released), Is.EqualTo(3));
	}

	[Test]
	[CancelAfter(60_000)]
	public void Eight_Workers_Preserve_Per_Key_Order_And_Lose_Nothing() {
		const int keys = 32;
		const int perKey = 500;
		const int total = keys * perKey;
		var state = new RecordingState();
		using var system = new ActorSystem<int, int, RecordingHandler, WorkStealingScheduler>(
			new RecordingHandler(state), keyCapacity: 128, chunkCount: 32, chunkSize: 32,
			batchBound: 4, workers: 8,
			scheduler: new WorkStealingScheduler { Capacity = 512, DequeCapacity = 256 });
		system.Start();

		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				for (var i = 0; i < perKey; i++)
					for (var k = 0; k < keys; k++) {
						if (!system.Dispatch(k, i)) return;
						dispatched++;
					}
			}
			catch (Exception ex) {
				producerError = ex;
			}
		}) { IsBackground = true };
		producer.Start();

		Assert.That(producer.Join(TimeSpan.FromSeconds(45)), Is.True,
			$"producer wedged after {Volatile.Read(ref dispatched)}/{total} dispatches");
		if (producerError is not null) Assert.Fail($"producer threw: {producerError}");

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, total);

		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test -c Release --filter "FullyQualifiedName~WorkStealingSchedulerTests"`
Expected: FAIL — `WorkStealingScheduler` does not exist (CS0246).

- [ ] **Step 3: Implement the scheduler**

Create `src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/Schedulers/WorkStealingScheduler.cs`:

```csharp
namespace Nanov.HighPerformance.Threading.Actors.Schedulers;

using System;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors.Internal;

/// <summary>
///   Scheduler D — pull-side work stealing. The producer submits newly-dirty keys to a shared global
///   queue; each worker keeps a Chase-Lev deque for its own self-rescheduled keys and, when it runs
///   dry, drains the global queue and then steals one key from a random peer.
///   <para>
///     <b>No router thread and no key→worker map.</b> Where B decides placement on the push side, D
///     lets idle workers decide on the pull side. The cell state machine already guarantees that one
///     key is processed by one worker at a time, so a key may move between workers freely — that is
///     what makes stealing correct here and what makes it a real alternative to B rather than a
///     variation on it.
///   </para>
///   <para>
///     <b>Steal granularity is one key.</b> The steal-half variant exists because fine-grained task
///     deques make a single steal too small to amortise its CAS. That does not apply: one key is
///     already a batch of up to <c>batchBound</c> messages, so a single steal moves real work.
///     Steal-half would also pile several hot keys onto one thief, which is the imbalance stealing
///     is supposed to remove.
///   </para>
///   <para>
///     <b>This scheduler owns no threads</b>, so it has no stop flag of its own and never invokes the
///     fault callback — the callback exists for a scheduler that runs its own thread and could die on
///     it silently (B's router). <see cref="SignalStop" /> only has to release parked workers so each
///     can observe the system's own exit condition.
///   </para>
/// </summary>
public struct WorkStealingScheduler : IKeyScheduler {
	private readonly State _state;

	public WorkStealingScheduler() => _state = new State();

	private sealed class State {
		public GlobalKeyQueue? Global;
		public WorkStealingDeque[]? Deques;
		public ManualResetEventSlim[]? WorkAvailable;

		// Racy by design. A stale `true` costs one wasted Set on a worker that is already awake and
		// heading for TryAcquire — which finds the work anyway. A stale `false` cannot lose a wakeup,
		// because the sleeper re-tests HasWork inside its armed window before it blocks.
		public bool[]? Parked;

		public uint[]? Rng; // per-worker xorshift; a shared Random would be a contention point
		public bool Started;

		public long StealsAttempted;
		public long StealsSucceeded;
		public long GlobalDequeues;
		public long DequeOverflows;
		public long Parks;
	}

	/// <summary>Global queue capacity; must be at least the key map's slot count so it can never fill.</summary>
	public int Capacity { get; set; } = 1024;

	/// <summary>
	///   Per-worker deque capacity. Overflow is not an error — a full deque spills to the global
	///   queue, which is what lets the deque stay small and cache-resident.
	/// </summary>
	public int DequeCapacity { get; set; } = 256;

	/// <summary>Steal attempts, including the ones that came up empty. Denominator of the steal rate.</summary>
	public readonly long StealsAttempted => Volatile.Read(ref _state.StealsAttempted);

	/// <summary>Steals that took a key. <b>The number that says which regime you are in</b> (§8).</summary>
	public readonly long StealsSucceeded => Volatile.Read(ref _state.StealsSucceeded);

	public readonly long GlobalDequeues => Volatile.Read(ref _state.GlobalDequeues);

	public readonly long DequeOverflows => Volatile.Read(ref _state.DequeOverflows);

	public readonly long Parks => Volatile.Read(ref _state.Parks);

	/// <summary>
	///   <paramref name="onFault" /> is deliberately unused: this scheduler starts no thread of its
	///   own, so there is no thread whose silent death it would have to report.
	/// </summary>
	public void Start(int workerCount, Action<Exception> onFault) {
		if (workerCount < 1) throw new ArgumentOutOfRangeException(nameof(workerCount));
		// A struct WorkStealingScheduler wraps a reference to shared State; handing the SAME value to a
		// second ActorSystem would put two owners on every deque — and the deque's whole cheap path
		// assumes exactly one. Fail loudly instead.
		if (_state.Started)
			throw new InvalidOperationException(
				"this scheduler instance is already driving a system; construct a new one per ActorSystem.");
		_state.Started = true;

		_state.Global = new GlobalKeyQueue(Capacity);
		_state.Deques = new WorkStealingDeque[workerCount];
		_state.WorkAvailable = new ManualResetEventSlim[workerCount];
		_state.Parked = new bool[workerCount];
		_state.Rng = new uint[workerCount];
		for (var i = 0; i < workerCount; i++) {
			_state.Deques[i] = new WorkStealingDeque(DequeCapacity);
			_state.WorkAvailable[i] = new ManualResetEventSlim(false);
			_state.Rng[i] = (uint)(i * 2654435761u + 1u); // any non-zero seed; xorshift needs one
		}
	}

	public readonly void Schedule(int cellIdx) {
		if (!_state.Global!.TryEnqueue(cellIdx))
			throw new InvalidOperationException(
				"global key queue full — it is sized to the map, so this most likely means Capacity is set " +
				"below the key map's slot count, not that a key index was scheduled twice.");
		WakeOne(_state);
	}

	public readonly void ScheduleLocal(int workerId, int cellIdx) {
		if (!_state.Deques![workerId].TryPushBottom(cellIdx)) {
			// Deque full: spill to the global queue rather than growing. Some other worker will pick it
			// up, which costs this key its affinity but keeps the deque bounded and cache-resident.
			Interlocked.Increment(ref _state.DequeOverflows);
			if (!_state.Global!.TryEnqueue(cellIdx))
				throw new InvalidOperationException(
					"deque overflowed into a full global queue — both are sized to the map, so Capacity is " +
					"almost certainly below the key map's slot count.");
		}

		// A self-reschedule leaves stealable work behind, so a sleeping peer should be given the chance
		// to take it. Gated on there being a sleeper at all, so a busy system never pays for this.
		WakeOne(_state);
	}

	/// <summary>
	///   Wake exactly one sleeping worker, not the herd. The fence is the Dekker handshake against
	///   <c>ActorSystem.WaitForWork</c>: the enqueue above must be visible BEFORE the parked flags are
	///   read, or we see nobody parked and skip the wake while the sleeper's own armed recheck sees a
	///   not-yet-visible queue and blocks. Mid-run that stalls one worker; on the last dispatch before
	///   <c>TryComplete</c> the key is never picked up, quiescence never arrives, and the system hangs.
	/// </summary>
	private static void WakeOne(State s) {
		Interlocked.MemoryBarrier();
		var parked = s.Parked!;
		for (var i = 0; i < parked.Length; i++) {
			if (!Volatile.Read(ref parked[i])) continue;
			s.WorkAvailable![i].Set();
			return;
		}
	}

	/// <summary>Local deque, then the global queue, then a steal scan. Non-blocking throughout.</summary>
	public readonly bool TryAcquire(int workerId, out int cellIdx) {
		// Clear the parked flag on the hot path so a worker that left WaitForWork via HasWork (rather
		// than via Wait) stops attracting wakes meant for a real sleeper.
		Volatile.Write(ref _state.Parked![workerId], false);

		if (_state.Deques![workerId].TryPopBottom(out cellIdx)) return true;

		if (_state.Global!.TryDequeue(out cellIdx)) {
			Interlocked.Increment(ref _state.GlobalDequeues);
			return true;
		}

		return TrySteal(_state, workerId, out cellIdx);
	}

	private static bool TrySteal(State s, int workerId, out int cellIdx) {
		var deques = s.Deques!;
		var count = deques.Length;
		if (count > 1) {
			// Random victim, then a full round so no peer is permanently skipped. A fixed scan order
			// would make every idle worker converge on the same victim.
			var start = (int)(NextRandom(s, workerId) % (uint)count);
			for (var i = 0; i < count; i++) {
				var victim = start + i;
				if (victim >= count) victim -= count;
				if (victim == workerId) continue;

				Interlocked.Increment(ref s.StealsAttempted);
				if (!deques[victim].TrySteal(out cellIdx)) continue;
				Interlocked.Increment(ref s.StealsSucceeded);
				return true;
			}
		}

		cellIdx = 0;
		return false;
	}

	/// <summary>Per-worker xorshift32. Only this worker touches its own slot, so no atomics.</summary>
	private static uint NextRandom(State s, int workerId) {
		var x = s.Rng![workerId];
		x ^= x << 13;
		x ^= x >> 17;
		x ^= x << 5;
		s.Rng[workerId] = x;
		return x;
	}

	/// <summary>
	///   Racy by design; read before parking.
	///   <para>
	///     <b>Peer deques are part of the answer.</b> If this only reported on the worker's own deque
	///     and the global queue, a worker holding stealable keys would leave every peer asleep and D
	///     would silently degenerate into B without a router — quiescence still resolves, so no
	///     correctness test would catch it.
	///   </para>
	/// </summary>
	public readonly bool HasWork(int workerId) {
		if (_state.Deques![workerId].CountApprox > 0) return true;
		if (_state.Global!.CountApprox > 0) return true;

		var deques = _state.Deques;
		for (var i = 0; i < deques.Length; i++) {
			if (i == workerId) continue;
			if (deques[i].CountApprox > 0) return true;
		}

		return false;
	}

	public readonly void WakeAll() {
		foreach (var evt in _state.WorkAvailable!)
			evt.Set();
	}

	/// <summary>
	///   No stop flag of its own: this scheduler owns no threads, so all stopping means is releasing
	///   parked workers to observe the system's own exit condition.
	/// </summary>
	public readonly void SignalStop() => WakeAll();

	/// <summary>
	///   Reset BEFORE raising the flag. Reversed, a <c>Set</c> that lands between the two is erased by
	///   the <c>Reset</c> and that wakeup is lost.
	/// </summary>
	public readonly void ArmWait(int workerId) {
		_state.WorkAvailable![workerId].Reset();
		Volatile.Write(ref _state.Parked![workerId], true);
	}

	public readonly void Wait(int workerId) {
		Interlocked.Increment(ref _state.Parks);
		_state.WorkAvailable![workerId].Wait();
		Volatile.Write(ref _state.Parked![workerId], false);
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dotnet build -c Release` (0 warnings), then
`dotnet test -c Release --filter "FullyQualifiedName~WorkStealingSchedulerTests"`
Expected: PASS on both TFMs.

If `readonly` on the interface members causes a compile error because the member mutates
`Capacity`/`DequeCapacity` (it must not), drop `readonly` from that member rather than making the
field mutable. `Start` is intentionally non-`readonly` because it reads the settable properties and
populates `State`.

- [ ] **Step 5: Run the whole Actors suite**

Run: `dotnet test -c Release --filter "FullyQualifiedName~Threading.Actors"`
Expected: PASS — existing tests must be unaffected.

- [ ] **Step 6: Commit**

```bash
git add src/experiments/mini-actor-system/Nanov.HighPerformance.Threading.Actors/Schedulers/WorkStealingScheduler.cs \
        tests/Nanov.HighPerformance.Tests/Threading/Actors/WorkStealingSchedulerTests.cs
git commit -m "feat(actors): scheduler D — work stealing with per-worker deques and a global queue"
```

---

### Task 4: Run the core behavioural suite against BOTH schedulers

**Files:**
- Create: `tests/Nanov.HighPerformance.Tests/Threading/Actors/SchedulerBehaviourTests.cs`

**Interfaces:**
- Consumes: `RoutedScheduler`, `WorkStealingScheduler`, `RecordingState`, `RecordingHandler`,
  `WorkerTestUtil.AwaitCompletionOrStall`.
- Produces: nothing consumed by later tasks.

Spec §9 requires every behavioural test to run against both schedulers, same body, parameterised on
`TScheduler`. NUnit's generic fixture support does this: `[TestFixture(typeof(A), typeof(B))]` on a
generic class instantiates it once per attribute.

A factory type parameter is needed because the schedulers have different settable properties, and
the constraint must be `struct` so the call devirtualises the same way production does.

- [ ] **Step 1: Write the fixture**

Create `tests/Nanov.HighPerformance.Tests/Threading/Actors/SchedulerBehaviourTests.cs`:

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Actors;

using System;
using System.Linq;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using Nanov.HighPerformance.Tests.Threading.Workers;
using NUnit.Framework;

// No `out` variance annotation: a variant type parameter cannot be instantiated with a value type,
// and every scheduler here is a struct by design.
/// <summary>Builds a scheduler of the fixture's type — the schedulers have different knobs.</summary>
internal interface ISchedulerFactory<TScheduler> where TScheduler : struct, IKeyScheduler {
	TScheduler Create(int capacity);
}

internal readonly struct RoutedFactory : ISchedulerFactory<RoutedScheduler> {
	public RoutedScheduler Create(int capacity) => new() { Capacity = capacity };
}

internal readonly struct WorkStealingFactory : ISchedulerFactory<WorkStealingScheduler> {
	public WorkStealingScheduler Create(int capacity) =>
		new() { Capacity = capacity, DequeCapacity = capacity };
}

/// <summary>
///   The behaviours that must hold no matter which scheduler is installed. Per-key order comes from
///   exclusive cell ownership, not from queue discipline (§3.1), so a scheduler that reorders,
///   steals, or hands keys out LIFO must still pass every one of these unchanged.
/// </summary>
[TestFixture(typeof(RoutedScheduler), typeof(RoutedFactory))]
[TestFixture(typeof(WorkStealingScheduler), typeof(WorkStealingFactory))]
public class SchedulerBehaviourTests<TScheduler, TFactory>
	where TScheduler : struct, IKeyScheduler
	where TFactory : struct, ISchedulerFactory<TScheduler> {
	private static ActorSystem<int, int, RecordingHandler, TScheduler> NewSystem(
		RecordingState state, int keyCapacity, int chunkCount, int chunkSize, int batchBound,
		int workers, int schedulerCapacity) =>
		new(new RecordingHandler(state), keyCapacity, chunkCount, chunkSize, batchBound, workers,
			default(TFactory).Create(schedulerCapacity));

	[Test]
	[CancelAfter(120_000)]
	public void Eight_Workers_Preserve_Per_Key_Order_And_Lose_Nothing() {
		const int keys = 64;
		const int perKey = 1_000;
		const int total = keys * perKey;
		var state = new RecordingState();
		using var system = NewSystem(state, keyCapacity: 256, chunkCount: 64, chunkSize: 16,
			batchBound: 4, workers: 8, schedulerCapacity: 1024);
		system.Start();

		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				for (var i = 0; i < perKey; i++)
					for (var k = 0; k < keys; k++) {
						if (!system.Dispatch(k, i)) return;
						dispatched++;
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

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, total);

		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");
	}

	/// <summary>
	///   Test 14. Every worker must genuinely PARK, and the system must still complete. A lost-wakeup
	///   bug passes every test whose workers never sleep, so the sleep here is the point: the dispatch
	///   is tiny and the pause is long enough that eight workers are certainly blocked before
	///   TryComplete runs.
	/// </summary>
	[Test]
	[CancelAfter(60_000)]
	public void A_Fully_Parked_System_Still_Completes() {
		var state = new RecordingState();
		using var system = NewSystem(state, keyCapacity: 64, chunkCount: 16, chunkSize: 16,
			batchBound: 0, workers: 8, schedulerCapacity: 256);
		system.Start();

		for (var i = 0; i < 4; i++)
			system.Dispatch(i, i);

		Thread.Sleep(500); // everyone drains, then parks

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 4);

		Assert.Multiple(() => {
			Assert.That(state.HandledCount, Is.EqualTo(4));
			Assert.That(state.CompletedCalls, Is.EqualTo(8), "OnCompleted fires once per worker");
		});
	}

	/// <summary>
	///   Test 15 — the F2 interleaving. TryComplete races a final in-flight message; Completion must
	///   not resolve before that message is handled.
	/// </summary>
	[Test]
	[CancelAfter(60_000)]
	public void Completion_Never_Resolves_Before_A_Racing_Final_Message_Is_Handled() {
		for (var round = 0; round < 200; round++) {
			var state = new RecordingState();
			using var system = NewSystem(state, keyCapacity: 64, chunkCount: 16, chunkSize: 16,
				batchBound: 0, workers: 4, schedulerCapacity: 256);
			system.Start();

			for (var i = 0; i < 32; i++)
				system.Dispatch(i % 8, i);

			system.TryComplete();
			WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, 32);
			Assert.That(state.HandledCount, Is.EqualTo(32), $"round {round}: completed with work outstanding");
		}
	}

	/// <summary>
	///   Both producer cold paths — chunk harvest and cell eviction — against eight concurrently
	///   releasing workers. Tiny keyCapacity and chunkCount force both paths to run constantly.
	/// </summary>
	[Test]
	[CancelAfter(120_000)]
	public void Eviction_And_Harvest_Under_Eight_Workers_Lose_Nothing() {
		const int keys = 400;
		const int perKey = 3;
		const int total = keys * perKey;
		var state = new RecordingState();
		using var system = NewSystem(state, keyCapacity: 8, chunkCount: 4, chunkSize: 4,
			batchBound: 1, workers: 8, schedulerCapacity: 256);
		system.Start();

		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				for (var k = 0; k < keys; k++)
					for (var i = 0; i < perKey; i++) {
						if (!system.Dispatch(k, i)) return;
						dispatched++;
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

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, total);

		var byKey = state.ByKey();
		Assert.That(byKey, Has.Count.EqualTo(keys));
		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()), $"key {key} out of order");
	}

	/// <summary>Test 11 — a throwing handler reports through OnError and does not break the key's order.</summary>
	[Test]
	[CancelAfter(60_000)]
	public void A_Throwing_Handler_Reports_And_The_Key_Keeps_Its_Order() {
		const int perKey = 200;
		var state = new RecordingState { Throw = (key, value) => key == 1 && value % 10 == 0 };
		using var system = NewSystem(state, keyCapacity: 64, chunkCount: 16, chunkSize: 64,
			batchBound: 4, workers: 4, schedulerCapacity: 256);
		system.Start();

		for (var i = 0; i < perKey; i++)
			for (var k = 0; k < 4; k++)
				system.Dispatch(k, i);

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, perKey * 4);

		var byKey = state.ByKey();
		Assert.Multiple(() => {
			Assert.That(state.Errors, Has.Count.EqualTo(perKey / 10), "every poison message must reach OnError");
			Assert.That(byKey[0], Is.EqualTo(Enumerable.Range(0, perKey).ToList()));
			Assert.That(byKey[1], Is.EqualTo(Enumerable.Range(0, perKey).Where(v => v % 10 != 0).ToList()),
				"the surviving messages of the poisoned key must still be in order");
		});
	}
}
```

- [ ] **Step 2: Run the fixture**

Run: `dotnet test -c Release --filter "FullyQualifiedName~SchedulerBehaviourTests"`
Expected: PASS for both fixture instantiations, on both TFMs. Each test name appears twice, once per
scheduler.

If a test fails **only** under `WorkStealingScheduler`, that is a real D defect — fix D, do not
weaken the assertion. If it fails under both, the fixture's parameters are wrong (most likely a
`chunkSize` too small for the per-key traffic, which silently overwrites unread slots).

- [ ] **Step 3: Run the whole Actors suite**

Run: `dotnet test -c Release --filter "FullyQualifiedName~Threading.Actors"`
Expected: PASS on both TFMs.

- [ ] **Step 4: Commit**

```bash
git add tests/Nanov.HighPerformance.Tests/Threading/Actors/SchedulerBehaviourTests.cs
git commit -m "test(actors): run the core behavioural suite against both schedulers"
```

---

### Task 5: Steal correctness under churn (spec test 8)

**Files:**
- Modify: `tests/Nanov.HighPerformance.Tests/Threading/Actors/WorkStealingSchedulerTests.cs`

**Interfaces:**
- Consumes: `WorkStealingScheduler` counters (`StealsAttempted`, `StealsSucceeded`, `Parks`).

Spec §9 test 8: many workers, few keys, high steal rate. The point is not just "it still works" —
it is that stealing **actually happened**, measured, not assumed. A scheduler whose steal path never
executes would pass every other test in this plan.

**The scheduler instance must be kept**, not constructed inline, so its counters can be read after
the run. `WorkStealingScheduler` is a struct wrapping a reference to shared `State`, so a copy still
observes the same counters.

- [ ] **Step 1: Add the test**

Append to `WorkStealingSchedulerTests.cs`:

```csharp
	/// <summary>
	///   Spec §9 test 8 — steal correctness under churn: many workers, few keys, so most workers are
	///   starved of local work and must steal to make progress.
	///   <para>
	///     <b>The counters are the point.</b> Order and loss assertions would pass against a scheduler
	///     whose steal path never ran even once; asserting <c>StealsSucceeded &gt; 0</c> is what makes
	///     this a test of STEALING rather than another test of the cell machine. It is a floor, not a
	///     target: the exact rate is a benchmark question, not a correctness one.
	///   </para>
	/// </summary>
	[Test]
	[CancelAfter(120_000)]
	public void Steals_Happen_Under_Churn_And_Preserve_Per_Key_Order() {
		const int keys = 4;
		const int perKey = 20_000;
		const int total = keys * perKey;
		var state = new RecordingState();
		var scheduler = new WorkStealingScheduler { Capacity = 256, DequeCapacity = 64 };
		using var system = new ActorSystem<int, int, RecordingHandler, WorkStealingScheduler>(
			new RecordingHandler(state), keyCapacity: 64, chunkCount: 16, chunkSize: 64,
			batchBound: 1, workers: 8, scheduler: scheduler);
		system.Start();

		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				for (var i = 0; i < perKey; i++)
					for (var k = 0; k < keys; k++) {
						if (!system.Dispatch(k, i)) return;
						dispatched++;
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

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, total);

		var byKey = state.ByKey();
		Assert.Multiple(() => {
			Assert.That(byKey, Has.Count.EqualTo(keys));
			Assert.That(scheduler.StealsAttempted, Is.GreaterThan(0), "the steal scan never ran");
			Assert.That(scheduler.StealsSucceeded, Is.GreaterThan(0),
				"no steal ever succeeded — D is behaving as a global-queue scheduler, not a stealing one");
		});

		foreach (var (key, values) in byKey)
			Assert.That(values, Is.EqualTo(Enumerable.Range(0, perKey).ToList()),
				$"key {key} out of order — a stolen key was processed concurrently with its owner");

		TestContext.Out.WriteLine(
			$"steals {scheduler.StealsSucceeded}/{scheduler.StealsAttempted}, " +
			$"global {scheduler.GlobalDequeues}, overflows {scheduler.DequeOverflows}, parks {scheduler.Parks}");
	}
```

- [ ] **Step 2: Run it**

Run: `dotnet test -c Release --filter "FullyQualifiedName~Steals_Happen_Under_Churn"`
Expected: PASS on both TFMs. Record the printed counters in the task report — they are the first
real data on D's behaviour and Task 7 writes them into the spec.

- [ ] **Step 3: Commit**

```bash
git add tests/Nanov.HighPerformance.Tests/Threading/Actors/WorkStealingSchedulerTests.cs
git commit -m "test(actors): prove steals actually happen under churn and preserve per-key order"
```

---

### Task 6: The B-vs-D starvation asymmetry (closes the `LocalDrainBound` debt)

**Files:**
- Create: `tests/Nanov.HighPerformance.Tests/Threading/Actors/StarvationAsymmetryTests.cs`

**Interfaces:**
- Consumes: `RoutedScheduler` (`LocalDrainBound`, `Policy`), `WorkStealingScheduler`.

**This task discharges a recorded debt.** Spec §6.1 says plainly: *"the bound exists in code, but its
necessity is **not demonstrated** by anything in the current suite."* Setting `LocalDrainBound` to
`int.MaxValue` made no measurable difference to any existing test, because they all round-robin
dispatch, which never lets one key monopolise a worker's local stack.

§6.1 names the three ingredients a test needs:
1. a single hot key dispatched **densely and consecutively**, with a small `batchBound` so every
   activation self-reschedules onto the same worker's local stack;
2. a victim key on **that same worker's ring** — which needs `RoutingPolicy.RoundRobin`, because
   `LeastLoaded` routes work away from a visibly busy worker and defeats the setup;
3. hot-key volume far exceeding the bound, and an assertion on **when** the victim is handled
   relative to the hot stream.

Use `workers: 1` for the two B cases: with one worker, "the victim is on the same worker's ring" is
guaranteed rather than arranged, which makes the demonstration deterministic instead of merely
likely. Use `workers: 4` for the D case, since a thief is the mechanism under test.

**Measure position, not time.** The oracle is the victim's index in the arrival sequence — how many
hot messages were handled before it. Wall-clock would be flaky; position is what starvation actually
means.

**If the thresholds turn out flaky, report it — do not widen them until the test asserts nothing.**
A test that passes under both bound settings is worthless here; the entire value is the gap.

- [ ] **Step 1: Write the tests**

Create `tests/Nanov.HighPerformance.Tests/Threading/Actors/StarvationAsymmetryTests.cs`:

```csharp
namespace Nanov.HighPerformance.Tests.Threading.Actors;

using System;
using System.Linq;
using System.Threading;
using Nanov.HighPerformance.Threading.Actors;
using Nanov.HighPerformance.Threading.Actors.Schedulers;
using Nanov.HighPerformance.Tests.Threading.Workers;
using NUnit.Framework;

/// <summary>
///   The one behavioural difference between schedulers B and D that the design predicts and nothing
///   in the suite had ever measured (spec §6.1).
///   <para>
///     B gives a worker a private local stack for its self-rescheduled keys and drains it first,
///     which buys affinity for free. Unbounded, that starves the worker's RING: a key refilled faster
///     than it drains is popped from local forever while a ring-queued key never runs at all — and
///     under B nothing can move that key elsewhere. <c>LocalDrainBound</c> caps the local run to fix
///     it. D needs no such bound, because a thief can take the starved key.
///   </para>
///   <para>
///     <b>Setup, per §6.1.</b> One hot key dispatched densely and consecutively with
///     <c>batchBound: 1</c>, so every activation self-reschedules onto the local stack; one victim
///     key dispatched early; <c>RoundRobin</c> routing, because <c>LeastLoaded</c> routes work away
///     from a visibly busy worker and destroys the setup. The B cases use a single worker so "the
///     victim is on the busy worker's ring" is structural rather than arranged.
///   </para>
///   <para>
///     <b>The oracle is POSITION, not time.</b> How many hot messages were handled before the victim
///     is exactly what starvation means, and it does not depend on machine speed.
///   </para>
/// </summary>
[TestFixture]
public class StarvationAsymmetryTests {
	private const int HotKey = 0;
	private const int VictimKey = 1;
	private const int HotMessages = 3_000;
	private const int VictimDispatchedAfter = 10;

	/// <summary>
	///   Runs the hot-key-plus-victim workload and returns how many messages were handled before the
	///   victim's single message.
	///   <para>
	///     <b>The hot handler is deliberately slower than the producer.</b> Starvation only occurs
	///     while the hot key's mailbox is non-empty at every release — that is what makes the worker
	///     self-reschedule onto its local stack instead of falling through to its ring. If the worker
	///     ever drains faster than the producer refills, the local stack empties, the ring gets
	///     checked, and the victim runs early: the test would then measure the race rather than the
	///     hazard. Burning a little time per hot message keeps the producer ahead until backpressure
	///     caps it, at which point the mailbox stays near-full BY CONSTRUCTION and the self-reschedule
	///     is guaranteed. The <c>Throw</c> hook is reused as a per-message callback — it runs at the
	///     top of <c>Handle</c> and returning <see langword="false" /> means "did not throw".
	///   </para>
	/// </summary>
	private static int VictimPosition<TScheduler>(TScheduler scheduler, int workers, int batchBound)
		where TScheduler : struct, IKeyScheduler {
		var state = new RecordingState {
			Throw = (key, _) => {
				if (key == HotKey) Thread.SpinWait(200);
				return false;
			},
		};
		using var system = new ActorSystem<int, int, RecordingHandler, TScheduler>(
			new RecordingHandler(state), keyCapacity: 64, chunkCount: 16, chunkSize: 256,
			batchBound: batchBound, workers: workers, scheduler: scheduler);
		system.Start();

		var dispatched = 0;
		Exception? producerError = null;
		var producer = new Thread(() => {
			try {
				for (var i = 0; i < HotMessages; i++) {
					if (i == VictimDispatchedAfter && !system.Dispatch(VictimKey, 0)) return;
					if (!system.Dispatch(HotKey, i)) return;
					dispatched++;
				}
			}
			catch (Exception ex) {
				producerError = ex;
			}
		}) { IsBackground = true };
		producer.Start();

		Assert.That(producer.Join(TimeSpan.FromSeconds(90)), Is.True,
			$"producer wedged after {Volatile.Read(ref dispatched)}/{HotMessages} dispatches");
		if (producerError is not null) Assert.Fail($"producer threw: {producerError}");

		system.TryComplete();
		WorkerTestUtil.AwaitCompletionOrStall(system.Completion, () => state.HandledCount, HotMessages + 1);

		var arrivals = state.Seen.ToList();
		var position = arrivals.FindIndex(x => x.Key == VictimKey);
		Assert.That(position, Is.GreaterThanOrEqualTo(0), "the victim key was never handled at all");
		TestContext.Out.WriteLine($"victim handled at position {position} of {arrivals.Count}");
		return position;
	}

	/// <summary>
	///   B with the bound disabled: the hazard, demonstrated. This is the test whose absence spec §6.1
	///   recorded — without it, <c>LocalDrainBound</c> is a fix for a problem nobody had reproduced.
	/// </summary>
	[Test]
	[CancelAfter(120_000)]
	public void Unbounded_Local_Draining_Starves_A_Ring_Queued_Key() {
		var position = VictimPosition(
			new RoutedScheduler {
				Capacity = 256, Policy = RoutingPolicy.RoundRobin, LocalDrainBound = int.MaxValue,
			}, workers: 1, batchBound: 1);

		Assert.That(position, Is.GreaterThan(HotMessages / 2),
			"with local draining unbounded the ring-queued key should not be serviced until the hot " +
			"stream is nearly exhausted — if this fails, the starvation hazard is not what §6.1 describes");
	}

	/// <summary>B with the shipped default: the bound rescues the victim quickly.</summary>
	[Test]
	[CancelAfter(120_000)]
	public void Bounded_Local_Draining_Rescues_A_Ring_Queued_Key() {
		const int bound = 16;
		var position = VictimPosition(
			new RoutedScheduler {
				Capacity = 256, Policy = RoutingPolicy.RoundRobin, LocalDrainBound = bound,
			}, workers: 1, batchBound: 1);

		// The bound caps consecutive local pops, so the victim must surface within roughly
		// bound x batchBound messages of being routed. Generous slack: the claim is "promptly",
		// orders of magnitude below the unbounded case, not an exact figure.
		Assert.That(position, Is.LessThan(HotMessages / 10),
			$"LocalDrainBound = {bound} did not bound the local run");
	}

	/// <summary>
	///   D with no bound of any kind: the same workload that starves under unbounded B does not starve
	///   here. This is the asymmetry §6.1 asserts, measured.
	///   <para>
	///     <b>What rescues the victim is not necessarily a steal.</b> Under D the victim is submitted
	///     to the shared global queue, so an idle worker can simply dequeue it — no theft required.
	///     The claim this test makes is only the one §6.1 makes: D needs no drain bound to avoid the
	///     hazard, because the starved key is reachable by workers other than the one pinned to the
	///     hot key. Which of the two mechanisms got there first is not asserted, and the counters are
	///     printed rather than checked for exactly that reason.
	///   </para>
	/// </summary>
	[Test]
	[CancelAfter(120_000)]
	public void Work_Stealing_Rescues_A_Starved_Key_With_No_Drain_Bound_At_All() {
		var position = VictimPosition(
			new WorkStealingScheduler { Capacity = 256, DequeCapacity = 256 },
			workers: 4, batchBound: 1);

		Assert.That(position, Is.LessThan(HotMessages / 10),
			"the victim key should have been picked up long before the hot stream ended — D is supposed " +
			"to be immune to this hazard without needing a drain bound");
	}
}
```

- [ ] **Step 2: Run the three tests**

Run: `dotnet test -c Release --filter "FullyQualifiedName~StarvationAsymmetryTests"`
Expected: PASS on both TFMs. **Record the three printed victim positions in the task report** —
those numbers are the measurement, and Task 7 writes them into the spec.

- [ ] **Step 3: Verify the tests are not vacuous**

Temporarily change `Unbounded_Local_Draining_Starves_A_Ring_Queued_Key` to use
`LocalDrainBound = 16`, re-run it, and confirm it **FAILS**. Then revert. A starvation test that
passes with the bound enabled is not measuring starvation. Report the observed position from this
check in the task report; do not commit the temporary change.

- [ ] **Step 4: Run each test 5 times to check for flakiness**

Run: `dotnet test -c Release --filter "FullyQualifiedName~StarvationAsymmetryTests"` five times.
Expected: 5/5 clean. If any run is red, report the observed positions rather than widening the
thresholds — a threshold wide enough to always pass measures nothing.

- [ ] **Step 5: Commit**

```bash
git add tests/Nanov.HighPerformance.Tests/Threading/Actors/StarvationAsymmetryTests.cs
git commit -m "test(actors): measure the B-vs-D starvation asymmetry and justify LocalDrainBound"
```

---

### Task 7: Update the spec, the defect log, and memory.md

**Files:**
- Modify: `docs/superpowers/specs/2026-08-12-mini-actor-system-design.md` (§6.1, §6.2, §8, §13)
- Modify: `memory.md`

**Interfaces:** none — documentation only. No production or test changes in this task.

Write only what the tasks above actually measured. Every number must come from a task report; if a
figure was not measured, say it was not measured rather than estimating it.

- [ ] **Step 1: Update §6.2 with what was built**

Record the deviations from what §6.2 originally specified, each with its reason:
- The global queue is **MPMC, not SPMC** — deque overflow means `workers + 1` enqueuers, so the
  enqueue side CASes (Task 2).
- `HasWork` consults **peer deques**, which §6.2 did not mention. Without it, workers park while
  stealable work sits in a peer's deque and D silently degenerates into B without a router — a
  liveness-shaped defect that no correctness test detects (Task 3).
- D owns no threads, so it has **no stop flag** and never invokes `onFault` (Task 3).
- Both fences in the deque are `Interlocked.MemoryBarrier()` per Lê et al. 2013, not `Volatile`
  writes (Task 1).

- [ ] **Step 2: Update the §6.1 starvation block**

§6.1 currently ends by saying the bound's necessity is not demonstrated and naming the test that
would demonstrate it. That test now exists. Replace the "not demonstrated" paragraph with the
measured result from Task 6 — the three victim positions, the configuration that produced them, and
the fact that the unbounded case was verified to fail when the bound was re-enabled. Keep the
description of the hazard; only the "unproven" status changes.

- [ ] **Step 3: Update §8 with the counters that exist**

List the counters `WorkStealingScheduler` actually exposes (`StealsAttempted`, `StealsSucceeded`,
`GlobalDequeues`, `DequeOverflows`, `Parks`) and note they are `Interlocked` increments on the
activation path, not the per-message path. §8 proposed a compile-time switch; that was not built —
say so.

- [ ] **Step 4: Update §13 implementation order**

Mark step 5 (D) done. Step 6 (harness + baselines, B-vs-D measurement) remains the only open step.

- [ ] **Step 5: Add a memory.md entry**

Follow the existing entry format in `memory.md`. Cover: D shipped behind the unchanged seam; the
MPMC deviation and why; the `HasWork`-must-see-peers trap and why no correctness test would catch
it; and the starvation asymmetry now being measured rather than argued. Keep it to the decisions and
the traps — the spec holds the design.

- [ ] **Step 6: Verify nothing but docs changed**

Run: `git diff --stat -- src/ tests/`
Expected: empty output.

- [ ] **Step 7: Commit**

```bash
git add docs/superpowers/specs/2026-08-12-mini-actor-system-design.md memory.md
git commit -m "docs(actors): record scheduler D as built and the measured B-vs-D asymmetry"
```

---

## Out of scope

Deliberately **not** in this plan, so a later phase can pick them up:

- **The B-vs-D benchmark** (§10, §13 step 6) — the harness, the static-hash-shard and
  `Channel<T>` baselines, and the skew/cardinality/handler-cost axes. That is Phase 3 and it is what
  finally answers the experiment's question. This plan only makes D exist and proves it correct.
- **The `Activate` mid-batch-flush StoreLoad debt** — recorded, bounded by the 20 ms poll, unchanged.
- **Design C** — parked as a bonus by explicit decision.
- **Steal-half, key merge, richer steal policies** — §6.2 leaves these deliberately unspecified.
- **Adaptive worker parking / idle worker retirement** — fixed N, parked indefinitely, per the
  original design decision.
