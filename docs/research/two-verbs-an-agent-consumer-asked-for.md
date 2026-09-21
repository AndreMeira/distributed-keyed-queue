---
title: "Two verbs an agent consumer asked for — schedule, and per-message deferral"
type: research
status: draft
updated: 2026-09-21
tags: [api, schedule, deferral, retry-after, exclusivity, consumer, agent, exploration, not-a-decision]
---

# Two verbs an agent consumer asked for

> **Exploratory. Neither verb is a proposal yet.** The consumer described below is itself an unbuilt design
> explored in one session (`homelab-toolkit-zio/docs/research/llm-conversation-model.md`), so what it "needs"
> is provisional. This note records the pressure it put on two places in the API and the reasoning around
> them — not a request, and not a commitment to anything.

Came out of designing an agent loop against dkq in the ZIO toolkit
(`homelab-toolkit-zio/docs/research/llm-conversation-model.md`). That consumer is worth recording whether or
not either verb is ever built, because it uses dkq in a way the README does not describe and it puts
pressure on two specific places.

## How that consumer uses dkq

A conversation is a key. A pool of identical runners claims a key, runs one turn of that conversation — a
model call, some tool calls, minutes of work — and settles. Nothing else in the system is a process: an
agent is a row in a store plus, sometimes, a message on its key.

The part that matters here is **where the work lives, which is not in the queue.** The transcript is in the
consumer's own store, and so is anything that has arrived and cannot be acted on yet. Almost every message on
the queue carries no payload at all — it says "look at conversation K again", and everything needed to decide
what that means is already durable elsewhere. A duplicate costs one read; an early one finds nothing to do
and settles; a lost pod loses nothing.

So this consumer is not using dkq as a work queue. It is using it for **per-key exclusivity with a lease**,
plus durable delivery of the nudge. Which leads to the observation worth passing back:

> The obvious simplification — any durable broker for the nudges, plus `KeyedLock` for the exclusivity —
> does not work, and it fails on the interesting axis.

A nudge arriving while the lock is held has to be dropped, on the reasoning that the holder will notice the
new work before it releases. That is the classic stranding interleave: the holder checks its pending list,
sees nothing, and is releasing while a writer adds an item and a second runner drops its nudge against a lock
that is still held. Across a store and a separate lock service the check and the release cannot be one atomic
step, so the race is real.

The queue does not have it, because **a message enqueued on a held key is not refused — it waits.** "There is
pending work" and "someone holds this key" are one structure, so settling both releases the key and reveals
the work. That is the distinctive thing dkq sells, more than the queueing: *a lock that remembers*. Durable
delivery, redelivery on death and per-key order are all buyable elsewhere; that is not.

## Verb one: `schedule`

Enqueue a message that becomes claimable at a time, rather than immediately.

The consumer's need is a deadline. A turn can suspend — it has asked a subagent for something and cannot
proceed until an answer exists — and a suspended conversation that is never woken sits forever holding a
user's next question unread. The fix is a backstop nudge at the deadline: if nothing else has woken this
conversation by then, wake it anyway and let it decide that its wait expired.

Without `schedule` that is a timer somewhere — in the consumer, or a scheduler beside it, with its own
durability story and its own way of being lost on a pod rotation. With it, the backstop is an ordinary
message on the ordinary path, and the consumer gains no new concept at all: a scheduled nudge is contentless
and idempotent exactly like every other nudge it already handles.

This looks like the cheaper of the two. It is additive to the message record rather than a new claimability
state, and delayed delivery is a feature every comparable queue has, so it costs little in surface area or in
explaining what the queue is.

## Verb two: per-message deferral

Today deferral is key-level: `SettleRequest.retry_after`, *"a nack asking the KEY to wait"*. There is no way
to say "not this message, but keep serving the key".

The consumer hits it exactly. A runner claims a key and finds a message it cannot act on yet — a user's
question that cannot legally join the transcript mid-turn. Nacking it defers the whole key, and **the message
that would unblock it arrives later on that same key**, so deferring the question defers its own unblock. The
`retry_after` chosen would be racing the thing it is waiting for, which is a poll with a better name.

So that consumer writes the message into its own store instead, and settles. It works, and it is cheap. But
the gap is general: any consumer that claims something it cannot act on yet, and does not want to stall the
key it arrived on, has the same problem and the same workaround. That is the case for the verb — not this
consumer, which has an answer, but the ones that will write the same workaround again.

### It would not remove the caller's own stash

Worth being clear about, since it is the obvious argument for building it. Three things that consumer's
store does with a held message, a message parked inside dkq could not:

- **It is readable by something other than a consumer.** A UI shows the user "queued: and what do I eat
  tomorrow". A deferred queue message is invisible to everything but whoever next claims it.
- **It moves atomically.** Appending the held text to the transcript and clearing it from the holding list is
  one write in one store. Pulling from dkq and writing to the store is two systems and a crash window.
- **Its release condition is an event, not a time.** It is unblocked by a subagent finishing — which is
  another message on the same key. Expressing that would mean dkq modelling "hold until this key is next
  enqueued to", a dependency it models nowhere else, and which strands the message if that never happens.

So per-message deferral would be for dkq's *other* consumers. This one keeps its own holding list either way.

## What either costs

An instance records the layout it writes under and refuses to start against a store written under a
different one, so a change to stored structures means stopping every instance, draining, deleting
`dkq:layout:schema` and starting again. That is a real cost for a service that has only just become stable,
and it sorts the two: `schedule` plausibly touches the message record and a claimability index, while
per-message deferral introduces a state a message can be in that nothing today can express.

## Where this leaves it

`schedule` is worth taking seriously on its own merits and has a concrete consumer asking for it.
Per-message deferral is a real gap, correctly identified, and its first consumer has already routed around
it — which is the honest argument for waiting until a second one appears rather than for building it now.

What would change that: a consumer whose held messages are *not* also domain state it wants to read, which is
the only case where dkq holding them is simpler than the caller holding them.
