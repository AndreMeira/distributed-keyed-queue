---
title: "Dequeue without holding a connection"
type: research
status: draft
updated: 2026-09-01
tags: [dequeue, redis, streams, xread, claiming, cluster, design]
---

# Dequeue without holding a connection

Not built. This is the design that replaces `BLMOVE` — what it deletes, what it adds, and the states
everything moves through.

## The idea, in one picture

Today every waiting consumer is a phone call held open to Redis. `BLMOVE` occupies its connection for the
whole wait, so *waiting* is the scarce resource: `DKQ_CLAIMERS` of them per instance, and a wait covers one
queue, so watching more queues than that is impossible.

The proposal keeps the phone metaphor but changes who holds the line:

- **Taking work becomes one atomic step.** A consumer asks "give me a key and its messages" and either gets
  a claim or gets nothing. No holding, no waiting inside Redis.
- **Each queue gets a doorbell.** Anything that makes a key claimable rings it, in the same atomic step that
  made it claimable.
- **One listener per instance hears every doorbell** and taps the shoulder of one consumer waiting for that
  queue. Waiting is now a fiber and a promise, not a connection.

The consumer's own loop is unchanged in shape: ask, and if there is nothing, wait until told or until its
patience runs out.

## What it deletes

This is the reason to do it. The whole of the following exists only because `BLMOVE` moves a key *before*
this service can run any logic on it, leaving a half-claimed state that has to be made recoverable:

- `claiming:<worker>` — the per-connection box a key sits in between the move and the claim
- `workers` — the liveness set that says whether a box has been abandoned
- `WorkerId`, and its leak into `domain/types` and `QueueStore.Swept.recovered`
- `register` and the part of `beat` that keeps claiming connections announced
- `watchdog.lua` sweep (2) — recovery of dead workers' boxes
- `release` — handing back a key whose name the interrupted caller never learned
- `consume.lua`'s `LREM claiming` guard, which exists to detect that someone else drained the box
- `Connection.provideBlocking`, the claiming half of the pool, and `DKQ_CLAIMERS`
- the patience-as-deadline correction, which only matters because a caller can queue for a connection

A key is either in `ready` or claimed. There is no third state, so there is nothing to recover.

## Data structures

Per queue, all sharing the `{q:<queue>}` hash tag so they live in one cluster slot.

| structure | type | status | what it is |
|---|---|---|---|
| `ready` | list | kept | keys with work and nobody working them |
| `state` | hash | kept | key → `queued` \| `processing`; absent means idle |
| `claimed` | zset | kept | key → lease deadline |
| `fence` | hash | kept | key → claim counter, the token a receipt carries |
| `delayed` | zset | kept | key → when a nacked key may be worked again |
| `attempts` | hash | kept | message id → delivery count |
| `msgs:<key>` | list | kept | that key's message ids, producer order |
| `payloads:<key>` | hash | kept | message id → bytes |
| `owned:<key>` | set | kept | ids the current claim still owes |
| `claiming:<worker>` | list | **removed** | the half-claimed box |
| `workers` | zset | **removed** | liveness of the things that held boxes |
| `wake` | **stream** | **new** | one entry per key made claimable |

`wake` is per queue, not global, and that is the load-bearing choice: it shares the queue's slot, so the
script that pushes a key onto `ready` can append to it **in the same call**. A global stream would sit in a
different slot, a script may not touch two slots, and the `XADD` would have to happen after the script —
leaving a window where a crash loses the notification. Per queue, there is no window: either both happened
or neither did.

Entries carry the queue name as their only field. The stream name already identifies the queue; the field
is there so a human reading `XRANGE` output can tell what they are looking at. `MAXLEN ~ 1000` on every
append bounds it: entries are read within milliseconds or not at all.

Per instance, in memory:

- `waiters: Map[QueueName, FIFO[Promise]]` — who is waiting, oldest first
- `pending: Set[QueueName]` — a doorbell rang with nobody waiting
- `lastId: Map[QueueName, StreamId]` — where the listener has read up to

## How states move

**A key** moves exactly as it does today; what changes is that two of the arrows are now one step, and that
every arrow *into* `ready` also appends to `wake`:

```
idle ──enqueue──▶ queued (on ready, wake+1)
queued ──claim (one script)──▶ processing
processing ──settle, last message──▶ queued (wake+1) │ idle │ delayed
processing ──lease lapses, watchdog──▶ queued (wake+1)
delayed ──backoff elapses, watchdog──▶ queued (wake+1)
```

The `queued → processing` arrow is the one that used to be two moves with a box in between. It is now a
single `EVALSHA`: pop the head of `ready`, and do everything `consume.lua` already does — set `state`, add
the lease to `claimed`, bump `fence`, fill `owned`, read ids and payloads, bump `attempts`. Atomic, so no
window exists for a second consumer to see the same key.

**A consumer** moves through:

```
ask ──claim──▶ done
ask ──nothing──▶ registered ──woken──▶ ask
registered ──patience elapsed──▶ empty response
```

with one rule at registration: **if the queue's `pending` flag is set, clear it and go straight back to
`ask`** instead of waiting. That closes the race where a doorbell rings between a consumer finding nothing
and putting its name down.

**The listener** moves through:

```
read (XREAD BLOCK on every watched stream) ──entry──▶ deliver ──▶ read
read ──block expired──▶ read
read ──connection lost──▶ reconnect ──resume from lastId──▶ read
read ──watched set changed──▶ re-issue XREAD ──▶ read
```

## The pieces in detail

### The claim

One script, replacing `BLMOVE` + `consume.lua`:

1. `LPOP ready` — nothing means nothing is claimable; return nil.
2. Everything the current `consume.lua` does, unchanged.

The current script's first line — `LREM claiming 1 key`, which fails the claim if someone else drained the
box — has nothing left to guard and goes away with the box.

### The doorbell

Every script that pushes a key onto `ready` appends one entry to `wake` in the same script: `produce.lua`
when it queues a message for an idle key, `complete.lua` when a settled claim leaves messages behind, and
each of the watchdog's sweeps when it returns a key. A nacked key parked in `delayed` rings *nothing* — its
doorbell comes later, from the sweep that releases it.

**One entry per key made claimable.** The cardinality is the point: an entry means "one key is claimable",
so a listener wakes exactly one consumer for it. Waking all of them would give one claim and N−1 wasted
round trips.

### The listener

One fiber and one connection per instance:

1. `XREAD BLOCK 5000 COUNT n STREAMS <s1> <s2> … <id1> <id2> …` over the streams of every queue with
   waiters.
2. Per entry: advance that stream's `lastId`, take the first live waiter from its FIFO, complete its
   promise. A consumer that has already timed out has interrupted its promise, so it is skipped. No live
   waiter → set `pending`.
3. A nil reply is the block expiring; loop.
4. On error: reconnect and read **from `lastId`**, not from `$`. This is what a stream buys over pub/sub —
   a blip costs nothing.
5. When the set of watched queues changes, interrupt the block and re-issue with the new set, keeping the
   ids already held.

A cold instance starts every stream at `$`: it has no waiters, so there is nothing to catch up on.

### The consumer

1. Try the claim. A busy queue never waits.
2. Nothing → register, checking `pending` as above.
3. Await the promise for what is left of the patience.
4. Woken → try again. It may find nothing, because every instance woke a consumer and one of them won; if
   so, go back to 2 with the remaining patience.
5. Patience elapsed → interrupt the promise, deregister, answer empty.

## Cluster

Per-queue streams keep the atomicity in cluster, because the stream is in its queue's slot. What cluster
costs is on the listener: `XREAD` cannot span slots, so an instance needs one reader per slot it watches —
at most one per queue.

That is worth stating plainly: **in cluster you cannot have guaranteed wake-ups, atomic emission, and a
single connection at once.** The slot rule forbids it. Standalone — what this runs on — gets all three,
because one `XREAD` can name every stream.

## What it costs

- **One extra Redis write per key made claimable.** `XADD` is O(1) and rides inside a script already being
  executed, so it is an appended command rather than a round trip.
- **Fairness becomes approximate.** Redis serves blocked clients FIFO, so `BLMOVE` is fair among parked
  connections; here every instance wakes its oldest local waiter and they race. Local order is preserved
  and a loser returns to the head of its FIFO, so it wins the next entry. Note today's fairness is already
  partial: consumers beyond `DKQ_CLAIMERS` are queued in-process and are not in Redis's blocked list at
  all.
- **A trimmed stream is a memory cost per queue**, bounded by `MAXLEN`.
- **More moving parts in the instance**: a listener fiber, a waiter registry, and a set of stream ids to
  keep. Against that, the whole recovery apparatus in the deleted list goes away.

## Why not the alternatives

- **Pub/sub.** Lossy by construction: a wake published while a subscriber is reconnecting is gone, and
  nothing lets it catch up. A stream resumes from an id.
- **A list as a mailbox.** Point-to-point — `BLPOP` gives the entry to exactly one reader, which may be an
  instance with no waiter for that queue while another instance's consumer waits on. A wake-up has to
  broadcast; plain `XREAD` does, `XREADGROUP` would not.
- **A global wake stream.** One connection in cluster, but a different slot from the queue, so emission
  cannot be atomic and a crash between the two loses the wake.
- **`BLMPOP` over many queues.** Multi-key, so it needs one slot; queues deliberately have different tags.
- **Keeping `BLMOVE` but per queue** (a watcher owning the connection). Fewer moving parts than this and
  keeps Redis's FIFO fairness, but still one connection per queue and keeps every structure in the deleted
  list. Described in [`dequeue-connection-model.md`](dequeue-connection-model.md).

## Open questions

1. **Does the safety poll survive?** With per-queue streams there is no lost-wake window, so it would only
   cover a bug in the listener. A slow tick — seconds — is cheap insurance while the design is new.
2. **How does the listener learn which queues to watch?** The store already memoises the queues it has
   seen; the watched set is "queues with waiters", which is smaller and changes more often.
3. **What trims the streams of a queue that goes quiet?** `MAXLEN ~` caps length, not lifetime. A queue
   nobody touches keeps up to 1000 entries indefinitely — small, but it is state with no owner.
4. **Does `Dequeue` still need `max_wait` at all?** Waiting no longer costs a connection, so the ceiling
   exists only to bound how long a client's RPC hangs.
