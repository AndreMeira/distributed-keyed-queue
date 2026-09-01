---
title: "What a WorkerId is, and why it is called that"
type: learning-material
status: current
updated: 2026-08-31
tags: [redis, blmove, claiming, identity, recovery, naming]
---

# What a WorkerId is, and why it is called that

## The definition

A `WorkerId` is a **string that names two Redis objects**:

```
{q:orders}:claiming:dkq-7f3c-2-a91b     a list — keys taken from `ready` but not yet claimed
{q:orders}:workers   → member "dkq-7f3c-2-a91b" with a deadline
```

That is all it is. It identifies **one pooled Redis connection** in one instance of this service. It is
not a consumer, not a thread, not a request, not a process. Nothing outside this service ever sees it.

Read the two objects as a pair: *"a box that can hold keys, and a deadline saying whether anyone is still
coming back for them."*

## Why the box exists

Redis cannot say "block until this list has something, and let me look before committing". The blocking
primitive is `BLMOVE`, and it **moves**: the key leaves `ready` before this service has run any logic of
its own. So a claim is two calls, not one:

```
BLMOVE ready:{q} → claiming:<worker>      blocking; takes a key out of the queue
EVALSHA consume.lua                        grants it: fence, lease, ownership, payloads
```

Between those two calls the key is in neither `ready` nor a claim. If the process dies there, the key is
in the box and nothing else knows about it. `claiming:<worker>` is that box, and `<worker>` is its address.
The deadline in `workers` is how the watchdog decides the box has been abandoned and drains it back
(`watchdog.lua`, sweep 2).

## Why it is minted per borrowed connection

Two facts force it:

1. **`release` cannot name a key.** In `claim`, only `take` is interruptible — `restore(take(...))` inside
   an `uninterruptibleMask` — so the only window in which the interrupt handler can fire is *while the
   `BLMOVE` is in flight*. That is precisely when the outcome is unknown: the command may already have
   moved a key server-side and the reply was lost with the fiber. So `release` is positional by necessity,
   not by convenience: `LMOVE claiming:<worker> → ready`, meaning "hand back whatever is in my box". That
   is correct only if the box holds at most one key.
2. **A connection makes one `BLMOVE` at a time.** That is the only unit in this design guaranteed to have
   at most one blocking move in flight — so one box per connection is exactly one key per box.

Hence `Connection.pool` mints one id per claiming connection, and `provideBlocking` hands it to the
borrower: you cannot know which box you are filling until you know which connection you got.

Give two connections one id and they share a box. Trace it with connections C1 and C2 both using id `W`:

1. C1's `BLMOVE` returns key `a` → `claiming:W = [a]` (`take` pushes on the right).
2. C2's `BLMOVE` returns key `b` → `claiming:W = [a, b]`.
3. C1's caller is interrupted, so `release` runs `LMOVE claiming:W → ready`, popping from the **right** —
   which is `b`, *C2's* key.
4. C2 runs `consume.lua` for `b`. Its first line, `LREM claiming:W 1 b`, returns 0, so the claim fails and
   C2 answers "nothing found" although it had found something.
5. **`a` is stranded.** It is in no `ready` list and no claim, and a box is only drained when its worker's
   liveness expires — but `W` is still alive, because C2 is registered and `beat` keeps renewing it. So `a`
   is invisible until the process dies.

Step 4 is merely wasteful; step 5 is a message lost for the life of the instance. Both are prevented by the
same thing: one id per connection means the box holds at most one key, so a positional `release` can only
ever move the key its own caller was holding.

**This is the whole reason for the granularity**, and it is not fixable by naming the key: in the one path
where `release` runs, there is no name to use. A shared box would need per-key expiry instead of a list —
see the design space below.

## Why it is called "worker", and why that is wrong

The name comes from the Lua, where `workers` is the set of things that hold liveness, and sweep (2) talks
about "a worker that died between its `BLMOVE` and `consume.lua`". Everything downstream inherited it.

It is a bad name, for a specific reason: **the participant that actually does the work is the consumer, and
the consumer has no id at all** — it is known by its receipt (queue, key, fence token). So "worker" names
the one participant that never works. It holds a key for the instant between two Redis calls.

It also leaks. `WorkerId` is declared in `domain/types/Types.scala` and appears in the port:

```scala
final case class Swept(reclaimed: Chunk[MessageKey], recovered: Chunk[WorkerId], released: Chunk[MessageKey])
```

So the domain's vocabulary contains the identity of a Redis connection in an adapter it is not supposed to
know exists. The domain has no use for it — what a caller of `sweep` wants to know is *what work came
back*, not which connection had been holding it.

Names that would say what it is: `ClaimSlot`, `ClaimingList`, `Holder`. And `Swept.recovered` could report
keys or a count, which would remove the type from the domain altogether.

## What every model has to keep

Whatever it ends up called, these are the invariants, and they are the reason the id exists at all:

1. **Registered before its first `BLMOVE`.** A box with no deadline in `workers` is unrecoverable — nothing
   will ever look at it. `register` does this once per `(id, queue)`.
2. **Its liveness renewed while it is in use.** `beat` renews every `(id, queue)` pair the store has
   registered, on the shared connection so it cannot queue behind a `BLMOVE`.
3. **Nothing stays in a box.** Every key in a claiming list is handed to a consumer, released, or drained
   by sweep (2). A key in no list and no claim does not exist.

## The design space

- **A — one id per connection (today).** Box holds ≤ 1 key, so `release` and sweep can be positional.
  `workers` holds `DKQ_CLAIMERS` entries per instance: precise, noisy to read.
- **B — one id per instance, with a box that expires per key.** A shared box needs to distinguish "just
  moved, about to be claimed" from "orphaned", which a list cannot do and a name cannot fix — the
  interrupted caller never learned its key. Make the box a sorted set of `key → deadline` and a sweep can
  return anything that has sat longer than a claim takes to complete, whoever put it there. `workers` then
  holds one entry per instance, which is the view an operator wants, at the cost of a second expiry
  mechanism alongside the lease.
- **C — one id per queue watcher.** If a connection is given to a queue rather than to a caller
  ([`../research/dequeue-connection-model.md`](../research/dequeue-connection-model.md)), the id belongs to
  the watcher — one per `(instance, queue)`. The one-key invariant survives as long as a watcher has one
  `BLMOVE` outstanding; pipeline them and it needs B's expiring box.

## Where to look

- `Connection.identity` — where ids are minted: `host` for legibility, `index` to separate connections in
  one process, `random` so two pods or two runs never share a box
- `Connection.pool` / `Pool.provideBlocking` — where one is handed to a borrower
- `RedisQueueStore.register` / `take` / `release` — its three uses
- `lua/consume.lua` first line — the `LREM` guard that makes the box's ownership checkable
- `lua/watchdog.lua` sweep (2) — recovery, and the only reason liveness is written
