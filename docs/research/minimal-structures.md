---
title: "Can DKQ use fewer Redis structures? Explored, and no — not with these semantics"
type: research
status: current
updated: 2026-09-09
tags: [redis, data-structures, simplification, architecture, negative-result]
---

# Can DKQ use fewer Redis structures? Explored, and no — not with these semantics

An exploration (2026-09-09) of whether the store could be built on fewer structures, driven by the question
"what is the simplest mental model that keeps every guarantee". Conclusion: **the current set is minimal for
the current contract.** Each structure carries a guarantee, and the one merge that looked clean dies on a
corner of the settle contract. Recorded so the path is not re-walked.

## The mental model that came out of it

Worth keeping even though the reduction died — it explains the system in six ideas:

> A mail room: a wall of pigeonholes, one per key. Letters go into a family's hole in arrival order. A
> worker takes the longest-waiting hole off the wall, notes "back by 3" on the board, and gets a numbered
> ticket. Nobody else touches that family's mail meanwhile. Letters are handled one at a time — done is
> shredded, failed stays put. The janitor returns any hole past its board time, and the ticket number
> changes, so a worker wandering back late is politely turned away.

`ready` is the wall, the lease is the board, `fence` is the ticket, the sweep is the janitor. The one place
the metaphor is *simpler than the system* is the board: DKQ has two boards (`claimed` and `delayed`), and
that is exactly where the reduction was attempted.

## The candidate: `claimed` + `delayed` → one `parked` (key → available-at)

Both mean "off the wall until time T". Merging them would have deleted a structure, a sweep pass, one of
enqueue's three guard checks, and — best of all — the double-push subtlety in `sweep.lua`, because the
claimed-and-delayed-at-once state stops being representable.

**Why it dies:** a key can legitimately be in both at once. A claim may be settled piece by piece, and a
nack in an *early* settle can carry a backoff while the claim lives on:

```
call 1:  "message A failed — don't retry this key for 5 minutes"
call 2:  "B and C are done"          ← ends the claim; the key must now wait the 5 minutes
```

`delayed` is where the pending 5 minutes is *stored between those calls*. That is the structure's real job;
"backoff timing" is only what it looks like. A merged `parked` holds the lease while the claim lives and has
nowhere to keep the wish.

The only clean resolution is a contract change — "the settle that ends the claim is the one whose
`retry_after` counts" — and that was considered and **rejected**: the current cross-call
longest-wait-standing semantics are the product. A pending-backoff smuggled into the fence hash's values was
also rejected as two structures pretending to be one.

## The rest of the inventory, and why each is irreducible

| structure | the guarantee it carries | why it cannot fold away |
|---|---|---|
| `ready` | oldest-waiting fairness | it *is* the wall |
| `sequence` | tie-free ordering | millisecond scores collide (measured: 200 keys → 133 distinct) |
| `claimed` | crash recovery | the lease |
| `delayed` | cross-call backoff | the stored wish, above |
| `fence` | zombie rejection | must outlive any one claim, so nothing claim-scoped can carry it |
| `msgs` + `payloads` | order vs cargo | a list carries order, a hash addresses by id; Redis has no ordered map |
| `owned` | idempotent piecewise settle | without it a consumer can settle an id enqueued after its claim (ids are producer-chosen, so guessable), and double-acks lose their anchor |
| `attempts` | visible redelivery | same id-space as `payloads` but merging means encoded field names — fewer structures on paper, more concept |
| `wake` | reactive delivery | deliberate: reactive over polling, settled 2026-09-09 |

## What the exploration did pay for

- The pigeonhole model above, usable wherever DKQ needs explaining.
- The observation that `sweep.lua`'s double-push guard exists precisely because claimed∩delayed is
  representable — the comment there now has a companion explaining *why* the state exists.
- The level-triggered backstop under `QueueReadiness` (`simplify-exp-2`), which came out of the same
  conversation: correctness rests on state, the wake path stays the accelerator.
