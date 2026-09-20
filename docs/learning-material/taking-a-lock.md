---
title: "Taking a lock with the client"
type: learning-material
status: current
updated: 2026-09-20
tags: [client, lock, lease, fence, zio]
---

# Taking a lock

The lock half of `distributed-keyed-queue-client`, from the outside. What it is for: run something while
holding a named lock, and let the lease, the receipt and the renewals be somebody else's problem.

If you want the RPCs instead — the fence, the receipt, a refresh you time yourself — that is `LockClient`
underneath, and the managed form is built on it in public.

## Depending on it

```scala
resolvers ++= Seq(
  "distributed-keyed-queue" at "https://maven.pkg.github.com/AndreMeira/distributed-keyed-queue",
  "homelab-toolkit-zio"     at "https://maven.pkg.github.com/AndreMeira/homelab-toolkit-zio",
)

libraryDependencies += "com.andremeira.homelab" %% "distributed-keyed-queue-client" % dkqVersion
```

Two resolvers because the client's messaging ports come from the toolkit's registry and a published pom
does not name where its dependencies came from. Both registries need a credential; the recipe is in
[`using-the-contract-as-a-dependency.md`](using-the-contract-as-a-dependency.md#getting-them).

## Holding one while you work

```scala
ZIO.scoped:
  for
    client <- LockClient.scoped(Endpoint("dkq", 9000))
    answer <- DistributedLock(client).acquire("the-key", ttl = 30.seconds, maxWait = 5.seconds)(theWork)
  yield answer   // None when the wait elapsed with somebody else holding it
```

**`None` is an answer, not a failure.** Somebody else held the lock for the whole wait; that is an
ordinary outcome of asking, and a caller decides whether to retry, skip or report. A failure means the
service could not be reached or refused the request.

What happens around your effect, which is the point of the layer:

| | |
|---|---|
| while it runs | the lease is renewed on a cadence taken from the grant |
| it answers | the lock goes back, and the answer comes out as `Some` |
| it fails | the lock goes back, and your error is what surfaces |
| it is interrupted | the lock goes back |

The lock is released **after** the effect, never before, so nothing else takes the key while your work is
still running.

## Not waiting

```scala
DistributedLock(client).tryAcquire("the-key", ttl = 30.seconds)(theWork)
```

Same shape, no wait: it answers about this instant. `None` means somebody holds it now — including
somebody merely queued for it, because taking it ahead of a waiter would be barging rather than locking.

## The lease you ask for is not the lease you get

`ttl` is what you would like; the service clamps it to its own ceiling and says what it actually granted.
That matters because the renewals are timed by the grant, not by the request — ask for an hour against a
ten-minute ceiling and the refreshes still fall due every five minutes rather than every thirty.

A hold that stops being renewed lapses at its lease and the lock frees itself. That is how a consumer that
dies stops blocking the key, and it is why a long `ttl` is not a substitute for renewing.

## The fence, and when you need it

The managed form hides the fencing token, because most callers never stamp a write with it. A caller
writing to a resource that checks fences drops a layer:

```scala
taken <- client.acquire("the-key", ttl = 30.seconds, maxWait = 5.seconds)
_     <- taken match
           case Acquired.Unavailable   => ZIO.unit
           case Acquired.Granted(hold) =>
             writeStampedWith(hold.fence)
               *> client.refresh(hold.receipt, 30.seconds)
               *> client.release(hold.receipt)
```

`hold.fence` rises with every grant of that lock. A resource that rejects a write carrying a stale one is
what makes a long hold safe: a holder that paused long enough to lose the lock without noticing cannot
land a write after the next holder has started. No lease alone can give you that, because nothing on the
server side can stop your process from running.

At this layer the renewals and the release are yours. `refresh` answers `Lost` when the hold is gone,
which is the signal to stop.

## What it will not do for you

**A lost hold does not interrupt your effect.** The managed form stops renewing and lets the work run to
its end. Interrupting would promise an exclusion the service cannot give — your code may already be inside
a write, and stopping the fiber does not stop what it has handed to something else. A caller that needs
more than best effort takes the fence.

**Nothing retries `None` for you.** Waiting longer is a `maxWait`; trying again later is a schedule around
the whole call, and which one you want is a property of the work rather than of the lock.

## One error type

Every call aborts with a `ServiceError`, which is an `ApplicationError.AdapterError` — so a dkq failure
reports the way every other adapter failure in the homelab does, and your effect's own errors widen with
it rather than being flattened into a wrapper. Dialling fails the same way, and every call carries a
deadline, so one the service never answers comes back rather than holding you.

Four cases, three decisions: `Rejected` is yours to fix, `Unreachable` is worth retrying, and `Unreadable`
and `Failed` are to report.
