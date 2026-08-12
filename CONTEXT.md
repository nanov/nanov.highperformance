# Nanov.HighPerformance

Low-allocation concurrency building blocks for .NET: bounded mailboxes with a fixed number of
threads behind them, and the scheduling machinery that decides which thread runs which work.

## Language

### Roles

**Producer**:
The side that hands work in. Throughout this repo "single producer" is a *caller contract* — the
producer may be different threads over time, but never two at once.
_Avoid_: publisher, sender, writer

**Worker**:
A thread that runs a handler. Owns its consumer position; nothing else may advance it.
_Avoid_: consumer, processor, executor

**Handler**:
The caller-supplied struct holding the per-message logic plus its error and completion hooks. It is
copied into each worker, so its state is per-worker.
_Avoid_: callback, delegate, listener

**Scheduler**:
The policy that decides which worker gets which key next. Interchangeable: routed (push-side) or
work-stealing (pull-side).
_Avoid_: dispatcher, balancer

### Message flow

**Dispatch**:
Handing one message to the system. Blocks under backpressure; the `Try` form declines instead.
_Avoid_: enqueue, post, send, publish

**Key**:
The unit of ordering and of exclusive ownership. Messages sharing a key are processed strictly in
order by one worker at a time; messages with different keys are unordered relative to each other.
_Avoid_: partition, shard, actor, entity

**Mailbox**:
The ordered messages waiting for one key.
_Avoid_: inbox, queue, backlog

**Activation**:
One worker taking exclusive ownership of a key, processing some of its mailbox, and releasing it.
The unit the scheduler traffics in — not the message.
_Avoid_: task, job, turn

**Batch**:
The messages consumed in a single activation, capped so one busy key cannot pin a worker.
_Avoid_: run, chunk (that means storage here)

### Storage

**Slab**:
The single pre-allocated array holding every in-flight message. All message storage is carved from
it; nothing else allocates per message.
_Avoid_: pool, arena, buffer

**Chunk**:
A fixed-size run of slab slots lent to one key for as long as it has messages.
_Avoid_: segment, block, page, buffer

**Cell**:
A key's durable record — its identity, its ownership state, and its mailbox position. Outlives the
chunk: storage is released as soon as a key drains, the cell is discarded lazily.
_Avoid_: entry, bucket, slot

**Drain hint**:
A worker's advisory note that a key looks empty. Never authoritative — the producer re-checks before
acting on it.
_Avoid_: signal, notification

### Keyed folding

**Conflation**:
Keeping only the newest message per key, overwriting anything still queued. Guarantees the latest is
delivered; tolerates a rare duplicate.
_Avoid_: deduplication, throttling, debouncing

**Coalescing**:
Folding messages per key into a running accumulator, exactly once — no contribution lost or double
counted.
_Avoid_: aggregation, merging, batching

### Lifecycle

**Soft complete**:
No more messages will be dispatched; everything already queued is processed before workers exit.
_Avoid_: graceful shutdown, close, finish

**Hard stop**:
Immediate teardown. Queued messages are dropped; the message currently in a handler still finishes.
_Avoid_: abort, kill, cancel

**Quiescence**:
The condition that lets workers exit after a soft complete: nothing is queued and nothing is in
flight anywhere.
_Avoid_: idle, drained, done
