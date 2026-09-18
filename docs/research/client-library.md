---
title: "A client library: two layers, and what each is allowed to decide"
type: research
status: draft
updated: 2026-09-18
tags: [client, library, consumer, lock, zio-schema, api, ergonomics]
---

# A client library

Today a consumer depends on `distributed-keyed-queue-protocol-zio-grpc` and gets the generated stubs: eight
RPCs, proto types, and no opinions. That is the right floor and it should stay. The question is what to put
above it.

Not hexagonal — it is a library, not a service. Interfaces and implementations, nothing else.

## Two layers, and the rule between them

**Layer 1 — the API in Scala.** Every RPC, one for one, in domain types rather than generated ones, by the
same mechanics the service already uses inbound and outbound: Chimney transformers over shapes that mirror
the wire field for field. Nothing is withheld. Per-message settle outcomes, the fence token, the batch
size, the patience — all present, because this layer's job is to be the API with better types.

**Layer 2 — the managed forms.** A consumer that heartbeats for you. A lock that refreshes for you and has
no token in its signature. Opinionated where layer 1 is complete.

The rule that makes this work: **layer 2 may only remove choices, never add capability.** Anything it
decides on your behalf is recoverable by dropping a layer. So the answer to "I need per-message outcomes"
is not a feature request, it is `layer1.settle(…)`.

That rule is what resolves the two things that would otherwise be design problems:

- **Batching.** Layer 2 acks the whole batch. One failure fails all ten, which is wrong for a poison
  message among nine healthy ones — and the answer is that such a consumer wants layer 1, where the wire's
  `repeated MessageOutcome` is exposed as it is.
- **The fence.** Layer 2 hides the token, because a managed lock refreshes and most callers never stamp a
  write. A caller with a fencing-aware resource uses layer 1, which returns the grant.

## Payloads: a typeclass, not `Chunk[Byte]`

The part that makes this a library rather than a wrapper. A caller should send an `A` and receive an `A`,
not bytes plus two strings it has to keep consistent.

`zio-schema` supplies the encode/decode half. It does **not** supply the other half: `payload_type` is
documented as *"stable schema name + version, never a class name"*, and a schema knows its structure, not
the name the organisation agreed on. So the typeclass carries both:

```scala
trait Payload[A]:
  def schema: Schema[A]
  def payloadType: String   // "order.v2" — stated, not derived
  def encoding: String      // the media type; a default per codec
```

This lands well against the encoding change made today. `encoding` became an open media-type string
precisely because the queue never reads a payload, so a JSON codec sets `application/json`, a protobuf one
sets `application/x-protobuf`, and a caller choosing Avro is no longer refused at the boundary. The client
picks the string from the codec instead of the caller remembering to.

Decoding is where it earns its keep: a message whose `payload_type` does not match the `Payload[A]` a
consumer expects is a value the consumer cannot hold, and should be refused as such rather than fed to a
decoder that may succeed by accident.

## What layer 2 manages

**The heartbeat.** A claim expires after `lease-ttl`. A handler that outlives it loses the claim, the work
is redelivered, and the original handler keeps running — two workers on one key, which is the guarantee the
service exists to provide. Layer 2 forks a renewal for the handler's lifetime. Nothing in the stubs
suggests you must, and nothing fails loudly when you do not.

**Demand against workers.** A batched dequeue claims `n` messages and starts `n` leases. A consumer running
`m < n` concurrently leaves the rest claimed and ticking. Layer 2 derives the batch from the parallelism
rather than letting them be set independently.

**The refresh.** Same for the lock, with the same shape and the same failure if forgotten.

## The structural question: where do the types live?

Layer 1 needs request and response types. The service already has them — `domain/request/queue/*`,
`domain/response/lock/*` — and they are in `modules/server`, which does not publish.

Three ways, and this is the one to settle before writing code:

1. **Publish the server's domain types.** Tempting and wrong: they are shaped by the *service's* needs —
   plain unchecked types on the way in because a validator is about to run, named types on the way out
   because the service minted them. A caller wants neither rule.
2. **Duplicate them in the client.** Two sets drifting, with the wire as the only thing keeping them
   honest. That is what the proto is for, so the drift would at least be caught — by a transformer failing
   to derive, which is the repo's existing test for shapes having drifted.
3. **Let the client define its own, caller-shaped.** Defaults where the service demands explicitness, a
   `Payload[A]` where the service has bytes, no `requester` context. Different types because different
   jobs, related only through the proto.

(3) is right for the same reason the service's DTOs are not the proto's: each layer owns shapes for its own
purpose, and the contract between them is the wire.

## What it would cost

| piece | size | what makes it that size |
|---|---|---|
| module + publish wiring | hours | a third published artifact |
| layer 1, eight RPCs | 1–2 days | mechanical once the types exist; Chimney derives, and a failure to derive is the signal shapes drifted |
| `Payload[A]` + a JSON codec | half a day | the typeclass is small; deciding what refuses a mismatched `payload_type` is the work |
| layer 2 lock | half a day | a bracket plus a refresh fiber |
| layer 2 consumer | 1–2 days | heartbeat, batch-against-parallelism, the ack boundary |
| tests | 1 day | `e2e/` already has `Deployment`, `Compose` and `Instance` to borrow |
| docs | hours | one page, and `using-the-contract-as-a-dependency.md` grows a section |

Four to six days. Layer 1 is the bulk and the least interesting; layer 2 is small and holds every decision.

## The dependency question

Layer 2's consumer is the toolkit's `Consumer` shape — *"the adapter wraps `logic` with the substrate's
commit/ack boundary, so offset and acknowledgement handling never surface here"* is this job exactly, and
implementing it gives callers `Processor`'s run loop, parallelism and batching for free.

The cost is that every dkq consumer then inherits `homelab-common`. The toolkit is public so it resolves,
but `protocol` carries `scalapb-runtime` and nothing else on purpose, and this would be the first published
artifact here with an opinion about someone's effect topology.

Layer 1 needs none of it. So the split falls naturally: layer 1 depends on the stubs and `zio-schema`,
layer 2 depends on the toolkit. Two artifacts rather than one, and a caller takes what it wants.

## What to do first

Layer 1 for the lock, then layer 2's managed lock on top of it — four RPCs, no payload codec, no consumer
machinery. It proves the two-layer rule end to end at the smallest scale, and the managed lock is useful
immediately.

The queue follows, and it is the one where `Payload[A]` and the heartbeat have to be right.

## Sketch: layer 1 for the lock

The smallest complete slice. Four RPCs, no payload codec, no managed forms.

```scala
lazy val client = project
  .in(file("modules/client"))
  .dependsOn(protocolZioGrpc)
  .settings(
    name := "distributed-keyed-queue-client",
    libraryDependencies ++= Seq(
      "dev.zio"      %% "zio"     % zioVersion,
      "io.scalaland" %% "chimney" % chimneyVersion,
      // no transport: a caller dials over netty, in-process or otherwise, as with the contract modules
    ),
  )
```

```
modules/client/src/main/scala/homelab/keyedqueue/client/
  lock/
    LockClient.scala        the interface — one method per RPC
    GrpcLockClient.scala    the implementation over the generated stub
    Acquired.scala          Granted(hold) | Unavailable
    Refreshed.scala         Renewed(leaseExpiresAt) | Lost
    Hold.scala              receipt, fence, leaseExpiresAt
    Receipt.scala           opaque String
    Fence.scala             opaque Long
  LockError.scala           what a call aborts with
  codec/LockCodecs.scala    Chimney, both directions
```

### The interface

```scala
trait LockClient:
  def acquire(name: String, ttl: Duration, maxWait: Duration): IO[LockError, Acquired]
  def tryAcquire(name: String, ttl: Duration): IO[LockError, Acquired]
  def release(receipt: Receipt): IO[LockError, Boolean]
  def refresh(receipt: Receipt, ttl: Duration): IO[LockError, Refreshed]
```

One for one with the service, and everything present: the fence is in `Hold`, the wait is the caller's, and
`tryAcquire` is a separate verb rather than a `maxWait` of zero — which the service refuses anyway.

### The types the wire cannot state

```scala
enum Acquired:
  case Granted(hold: Hold)
  case Unavailable

enum Refreshed:
  case Renewed(leaseExpiresAt: Instant)
  case Lost

final case class Hold(receipt: Receipt, fence: Fence, leaseExpiresAt: Instant)
```

This is the service's outbound codec run backwards, and it is the layer's whole point. The wire says
`acquired: bool` with three fields that are only meaningful when it is true; a caller wants a choice. So
the transform **in** is partial for the same reason the service's is: `acquired = true` with no
`lease_expires_at` is a message the client cannot hold, and is refused at the boundary rather than carried
inwards as an `Option` every call site has to re-examine.

### Errors

```scala
enum LockError:
  case Rejected(message: String)       // INVALID_ARGUMENT — the request was malformed
  case Unreachable(cause: Throwable)   // the deployment did not answer
  case Failed(cause: Throwable)        // anything else the transport raised
```

The client is the adapter edge, so `StatusException` is wrapped here and never reaches a caller's
signature — the same rule the service applies to Lettuce.

### Construction

```scala
final case class Config(host: String, port: Int, plaintext: Boolean = true)

object LockClient:
  /** Dial a deployment. */
  def scoped(config: Config): ZIO[Scope, Throwable, LockClient]

  /** Dial one over a channel the caller built, for TLS, interceptors or an in-process transport. */
  def scoped(channel: ZManagedChannel): ZIO[Scope, Throwable, LockClient]
```

The same rule as the layers: the short form decides, the long form is underneath it. `Config` rather than
loose parameters so a deadline or a TLS setting can arrive later without changing the call.

**Not a `ZLayer`.** A scoped effect composes into one if a caller wants it, and a library that hands out
layers has decided how its consumer builds an environment. The homelab's toolkit already takes this
position.

**Config is taken, not read.** No HOCON, no environment lookups inside the client — the caller's
composition root reads its own configuration and passes the result in, which is the convention the service
follows and the thing that lets a test point the client somewhere else by calling the same constructor.

**This is the one place the module takes a transport dependency.** `scoped(Config)` has to build a channel,
so the artifact carries `grpc-netty` — where the contract modules deliberately carry none, because *"whether
a consumer dials over netty, in-process or something else is the consumer's decision"*. That stance is right
for a contract and wrong for a client: dialling is what a client is for. Worth stating rather than
inheriting by accident, and the channel overload is what keeps the other transports reachable.

### What this slice proves

Nothing in it needs the toolkit, a payload codec, or a background fiber — so it can ship alone, and the
managed lock on top of it is then a bracket plus a refresh fiber over an interface that already exists.
If the two-layer rule survives contact here, it survives.
