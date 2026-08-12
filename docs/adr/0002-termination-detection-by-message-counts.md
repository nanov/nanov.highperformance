# Termination detection counts messages, not in-flight keys

**Status:** accepted

Soft completion needs to know when N workers have drained everything. We detect it by comparing a
producer-private `dispatched` count against the sum of per-worker `processed` counts, rather than by
maintaining a global count of keys currently scheduled-or-owned. The message-count scheme is **exact
by construction** — the cell state machine independently guarantees every message is handled exactly
once, so the sums converge regardless of release races, batch-bound reschedules, reclaim reverts,
evictions or steals.

## Considered options

The in-flight key counter is the obvious design, and it was written, reviewed and **rejected with two
confirmed bugs**. Recorded here so nobody simplifies back to it:

- **It leaks on every batch-bound reschedule.** Producer increments; the worker drains to the bound,
  wins the release-recheck and reschedules — incrementing again; the next activation drains fully and
  decrements once. Net +1 per bounded activation, forever. Completion hangs deterministically under
  any sustained key.
- **It leaks on the release race even if only the producer increments.** When the producer wins the
  reschedule CAS and the worker's recheck CAS then *fails*, the worker takes neither branch and never
  decrements. The episode ended via a failed CAS, which no final-release rule covers.

The naive repair — decrementing on every ownership release — is balanced but strictly worse: the
decrement lands *before* the emptiness recheck, so a worker that read a stale tail can produce a
transient zero while completion is signalled, firing `Completion` with a message still queued that is
then dropped on dispose.

A correct token discipline does exist and was verified against every release-race outcome. It is
rejected anyway on two grounds: it is four counter sites keyed to CAS outcomes, each a latent
reintroduction of the above; and it puts a contended cross-core atomic on every activation from every
worker — under high key churn with small batches, activations approach messages, making it a shared
hot line of exactly the kind the ring-buffer signalling work existed to remove.

## Consequences

Two ordering rules are load-bearing and must not be "tidied":

- The exit predicate reads the completion flag **first**, then the counters, all acquire. Reversed,
  ARM64 will serve a stale zero beside a fresh completion flag — false quiescence and silent message
  loss. This mirrors the existing soft-complete rule in the worker family (re-read the tail *after*
  observing completion), with the counter sum as the new tail.
- The producer must run no cold paths (chunk reclaim, eviction) after signalling completion, since
  those can schedule work. Structurally true today; asserted explicitly because it is invisible.
