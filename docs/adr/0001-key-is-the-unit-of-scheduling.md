# The key, not the message, is the unit of scheduling

**Status:** accepted

In the multi-worker keyed processor, the schedulers move **key indices**, never messages. A key's
messages live in one ordered mailbox, and a key is in exactly one place at any instant — nowhere,
one scheduler queue, or one worker — arbitrated by a per-key state machine. We chose this because it
makes per-key ordering a property of *ownership* rather than of *queue discipline*.

The consequence is the whole reason the design works: **the scheduler's own ordering discipline
becomes irrelevant to correctness.** LIFO local deques, stealing from either end, out-of-order claims
from a shared queue — all legal, none can invert two messages of the same key. That is what makes
work stealing tractable here and what lets routed and work-stealing schedulers be the same system
with one type argument swapped.

## Considered options

**Rejecting the obvious design is the point of this record.** The natural sketch is a shared queue of
*messages* that N workers pull from, claiming keys in a shared map and offloading conflicting
messages to the current owner's inbox. It does not work: workers take from the queue in order but
reach the claim map in a different order, so two workers can invert two messages of the same key.
Take order and claim order are not the same thing, and execution order follows the latter. Closing
that needs per-message sequence numbers plus a per-key reorder buffer, on top of a contended CAS and
a shared map lookup **per message** — where key-as-unit pays neither.

Static hash sharding across independent single-consumer workers is also order-safe and cheaper still,
and remains the baseline this design must beat. It is rejected only because it cannot rebalance:
a hot key or an uneven split pins one worker permanently.
