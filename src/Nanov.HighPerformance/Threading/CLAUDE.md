# Threading — agent notes

Scope: `Nanov.HighPerformance.Threading` and below. Today that's the **SPSC worker family** in
[`Workers/`](Workers) (`…Threading.Workers`, internal core in `…Threading.Workers.Internal`). Read
[`README.md`](README.md) for the user-facing API. This file is the stuff to not get wrong when
editing.

## The one rule: SPSC SIGNALING INVARIANT

The whole design is correct **only** under strict single-producer / single-consumer. The wake
events (`_itemEvt`, `_spaceEvt`) are edge-triggered: `Set()` fires *only* on the empty→non-empty /
full→non-full transition, paired with a non-atomic `Wait();Reset()`. This is safe because:

- exactly one producer writes `_tail`; exactly one consumer (the dedicated thread) writes `_head`;
- while the consumer is parked `_head` can't advance, so only the first item past `_head` fires the
  lone `Set`; the post-`Reset` recheck acquire-reads `_tail` (released before the `Set`), so no
  wakeup is lost; shutdown rides the separate `volatile _stop`, rechecked before every park.

**A second concurrent producer, or advancing `_head` off the worker thread, breaks this and hangs
silently.** If you touch `SpRingCore` producer/consumer/signaling code, preserve this exactly. The
full write-up lives in the `SpRingCore<T>` XML doc.

- **Single consumer is structural** (the worker owns its one thread; `_head` only moves on it —
  including `HandleScope.Release`, which is a `ref struct` confined to the worker thread, called
  *before* an async handler returns its `ValueTask`).
- **Single producer is the caller's contract** — not enforceable in Release. The DEBUG-only overlap
  guard (below) catches violations in dev/test.

## Architecture (don't regress these)

- **Composition, not inheritance.** All machinery lives in `internal sealed SpRingCore<T>`; the four
  public workers *own* one and forward to it. Do **not** reintroduce public base classes — a public
  worker can't derive from an internal base (CS0060), and we deliberately keep the core internal.
  Public surface is exactly: the 4 workers, the 4 `IWorkerHandler` interfaces, `PublishScope<T>`,
  `HandleScope<T>`, `IWorkerQueue`. Keep it that way.
- **Devirtualized struct handlers.** Workers are `…<T, THandler> where THandler : struct,
  I…Handler<T>`. Per-item `Handle`/`OnError`/`OnCompleted` go through the struct field so the JIT
  inlines them. Never store the handler as an interface reference or call it virtually.
- **Fast/slow split.** Core producer/consumer fast paths (`TryReserve`/`TryClaim`/`CommitTail`/
  `AdvanceHead`/`TryEnqueue`/`TryDequeue`) are `[AggressiveInlining]`; the parking loops
  (`WaitForSpaceSlow`/`WaitForItemSlow`) are `[NoInlining]`. Keep the hot path inlined and the cold
  park out of line.
- **Cache-line-padded indices.** `_head` (consumer) and `_tail` (producer) live in a
  `PaddedIndices` struct (`_idx.Head` / `_idx.Tail`) 128 bytes apart, so the two cores don't
  false-share a line every item. `PaddedIndices` is a **non-generic** top-level struct on purpose —
  generic types can't have `[StructLayout(LayoutKind.Explicit)]`. Don't fold the indices back into
  plain adjacent fields. (Local laptop benchmarks were too noisy — error > mean — to quantify the
  win; it's the standard Disruptor technique, correct and harmless, kept on that basis.)
- **Hard stop drops, soft complete drains.** `TryClaim` rechecks `_stop` *after* the wait loop even
  when the ring is non-empty — that's what makes cancel/dispose drop queued items. Don't remove it
  (regression we already fixed once).
- **OnError is always synchronous**, on every variant including async ones. `OnCompleted` is the
  drain hook (sync for sync workers, `…Async` for async). Handler exceptions are reported to
  `OnError` and the worker continues.
- **Sync vs async use two different core paths — don't cross them.**
  - *Sync* (`SpWorker` / `ValueSpWorker`): dedicated-thread core ctor
    (`new SpRingCore<T>(capacity, threadName, runLoop)`), blocking consumer (`TryClaim`/`TryDequeue` +
    `WaitForItemSlow` parking on `_itemEvt`).
  - *Async* (`AsyncSpWorker` / `AsyncValueSpWorker`): **ThreadPool** loop, no dedicated thread.
    `Start` launches `RunLoopAsync` (one `async Task`) via `Task.Run`, using the core's **no-thread**
    ctor (`new SpRingCore<T>(capacity)`), `TryClaimNow`/`TryDequeueNow` (non-blocking), and
    `WaitForItemAsync`. The continuation resumes on the pool — no thread parked per `await` — so it
    matches/beats `Channel<T>` for truly-async serial handlers. The scope worker reads + `Release()`s
    the `HandleScope` **synchronously before any await** (ref struct can't cross await); `Dispose`
    `SignalStop()`s, `_loopTask?.Wait()`s, then `_core.Dispose()`. (We removed the old dedicated-thread
    async workers and their `_processDoneEvt` rendezvous — don't reintroduce them.)
- **The empty-wait must stay allocation-free.** `WaitForItemAsync` is **non-`async`** on purpose: it
  returns a struct `ValueTask<bool>` (completed-true item available / completed-false stop-or-drained
  / the `AsyncAutoResetEvent`'s pending wait, result `true` = "woke" → the loop re-checks). The
  `AsyncAutoResetEvent` is a reused `IValueTaskSource<bool>` (`ManualResetValueTaskSourceCore<bool>` +
  a small lock; set on the empty→non-empty edge by `CommitTail` when `_itemReadyAsync != null`, and by
  `SignalStop`/`TryComplete`). Making `WaitForItemAsync` `async` again would box a state machine per
  empty wait — don't. The only per-loop allocation left is `RunLoopAsync`'s one-time box; per-item
  handler state-machine boxing (when the handler genuinely suspends) is inherent to async and the
  channel pays it too.

## DEBUG-only producer overlap guard

`SpRingCore.DebugBeginProduce`/`DebugEndProduce` (`[Conditional("DEBUG")]` + bodies/field under
`#if DEBUG`) detect *overlapping* producers via an `Interlocked` flag — copy path wraps
`TryEnqueue`, scope path spans `Publish()`→`PublishScope.Dispose()`. It detects concurrency/overlap
(incl. two open scopes / a forgotten `using`), NOT thread identity (serial cross-thread is legal).
Compiles to nothing in Release. If you add producer entry points, guard them the same way.

## Naming

`Sp` = single-producer (the part the caller must uphold); single-consumer is structural, so it's
left out. Don't reintroduce the `Spsc` prefix.

## Tests & conventions

- NUnit, tabs (width 2), house style. Tests live in `tests/…/Threading/Workers/`.
- Recorders are **struct handlers wrapping a shared reference-type state object** (the struct is
  copied into the worker; observe results through the reference). See `SpWorkerTests` for the
  `Rec`/`RecordingState`/`RecordingHandler` pattern; mirror it.
- `WorkerTestUtil.AwaitCompletionOrStall` / `AwaitProgress` are stall watchdogs — use them for
  signaling/churn tests so a lost wakeup fails fast instead of hanging.
- The producer-guard test is `#if DEBUG` (it only throws in Debug). CI runs `-c Release`, so it's
  absent there; run `dotnet test -c Debug` to exercise it.

## Verify

- `dotnet build -c Release` → net9.0 + net10.0, 0 warnings (warnings-as-errors).
- `dotnet test -c Release` → full suite (guard compiled out).
- `dotnet test -c Debug` → includes the overlap-guard test.
- `dotnet run -c Release --project benchmarks/Nanov.HighPerformance.Benchmarks -- --filter '*SpWorker*'`.
