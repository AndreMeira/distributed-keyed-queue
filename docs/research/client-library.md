---
title: "A client library: two layers, and what each is allowed to decide"
type: research
status: draft
updated: 2026-09-20
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

## Payloads: bytes on the model, typing beside it

The model carries `Chunk[Byte]`, because that is what the wire carries and the payload is the one thing
this client does not know better than the wire. Everything it *does* know better is in the envelope — an
id rather than a string, a receipt rather than a string, an instant rather than a timestamp, a non-empty
chunk rather than a head and a tail. Typing the payload is a separate job, and it sits beside the model
rather than inside it.

Two earlier drafts of this page put it inside, and both are recorded here because the reasons they failed
are the reasons this works. The first proposed a `Payload[A]` typeclass carrying the schema, the
`payload_type` and the `encoding` together — rejected by the rule at the top of this page, since binding
the schema name to the type is a policy and policy belongs a layer up. The second proposed a `Format[A]`
the caller supplies to every call, which was machinery in front of a decision that did not need the
caller's help.

### Two typeclasses, one per direction

```scala
trait MessageEncoder[A]:
  def encoding: String                      // the media type these bytes will be
  def encode(value: A): Chunk[Byte]

trait MessageDecoder[A]:
  def decode(message: Message.Incoming): Either[MessageDecoder.Failure, A]
```

Split by direction because the jobs are not symmetric in practice: a service that only consumes needs no
encoder, and one that only produces needs no decoder. Neither carries `payload_type` — the caller states
that label when it builds a message, which is what the first draft got wrong.

The encoder owns `encoding` because nothing else can know it, and it is what fills that field on an
outgoing message:

```scala
object Message.Outgoing:
  def apply[A](key: String, id: MessageId, payloadType: String, value: A)
              (using encoder: MessageEncoder[A]): Message.Outgoing
```

Which makes `encoding` unsettable by hand, the pair that must agree — bytes and media type — agree by
construction, and a build of an outgoing message impossible without an encoder for what is going in it.

The decoder is the one that reads a whole message rather than bytes, and that is deliberate: it is the only
thing left holding the `encoding` field, so refusing a message encoded as something it does not speak is
its job. That refusal is the entire reason the proto makes the field required — *"a receiver that assumes a
format will one day decode garbage successfully"*.

### Where zio-schema comes in, and only there

Both typeclasses get smart constructors from a `Schema[A]` in scope, so the common case is derived rather
than written:

```scala
object MessageDecoder:
  def json[A: Schema]: MessageDecoder[A]
  def protobuf[A: Schema]: MessageDecoder[A]
```

That is the single place zio-schema is needed. A caller with its own codec writes the four lines and never
touches it; a caller that already describes its types with schemas gets both directions free. Several
encodings coexist on one queue, because an encoder is a value rather than a property of `A` — which is the
openness the proto's media-type field was for, recovered without the caller supplying a pair to every call.

### Layer 1 hands over bytes

Decoding happens above layer 1, not inside it, and this is what makes the whole arrangement pay. A
`dequeue[A: MessageDecoder]` that decoded for you would have to answer for a batch where three of ten
messages fail, and no answer is good: refuse the call and the seven that were fine are lost — still
claimed, still leased, and now unnameable — or invent a shape for partly-readable claims.

Handing back bytes makes that question disappear rather than answering it. The caller has the receipt and
every id in hand before a single decode is attempted, so a message that will not decode is settled
`Failed` like any other message it cannot process. Nothing is lost because nothing was ever at risk.

Two more questions dissolve with it. A queue carrying several payload types needs no special support: match
on the `payloadType` each message states and pick the decoder. And a decode failure stops being a transport
concern — it is an `Either` in the caller's hands, not a `QueueError` the client has to carry a receipt
inside.

Layer 2's consumer is where a decoder is supplied and the policy chosen, which is the two-layer rule
landing where it should: layer 1 withholds nothing, layer 2 decides what to do about it.

### Still open

Which codecs ship as smart constructors. JSON has the advantage that a message sitting in Valkey can be
read by a human debugging it; protobuf is smaller and is already in the build. Both are cheap, so the
question is really whether anything beyond those two is worth providing.


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
   a decoded `A` where the service has bytes, no `requester` context. Different types because different
   jobs, related only through the proto.

(3) is right for the same reason the service's DTOs are not the proto's: each layer owns shapes for its own
purpose, and the contract between them is the wire.

## What it would cost

| piece | size | what makes it that size |
|---|---|---|
| module + publish wiring | hours | a third published artifact |
| layer 1, eight RPCs | 1–2 days | mechanical once the types exist; Chimney derives, and a failure to derive is the signal shapes drifted |
| the two codec typeclasses | hours | zio-schema does the work; the smart constructors are a few lines each |
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

The queue follows, and it is the one where the payload types and the heartbeat have to be right.

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
    Refreshed.scala         Renewed(leaseExpiresAt, leaseTtl) | Lost
    Hold.scala              receipt, fence, leaseExpiresAt, leaseTtl
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
  case Renewed(leaseExpiresAt: Instant, leaseTtl: Duration)
  case Lost

final case class Hold(receipt: Receipt, fence: Fence, leaseExpiresAt: Instant, leaseTtl: Duration)
```

This is the service's outbound codec run backwards, and it is the layer's whole point. The wire says
`acquired: bool` with three fields that are only meaningful when it is true; a caller wants a choice. So
the transform **in** is partial for the same reason the service's is: `acquired = true` with no
`lease_expires_at` is a message the client cannot hold, and is refused at the boundary rather than carried
inwards as an `Option` every call site has to re-examine.

`lease_ttl` was added to both responses ([#30](https://github.com/AndreMeira/distributed-keyed-queue/pull/30))
once the managed layer needed a cadence: the service clamps a ttl past its ceiling, so the request is not
the lease, and `lease_expires_at` is a reading of the service's clock that a holder cannot subtract its own
from. A duration is neither — it is the one quantity both ends agree on, so the renewals are timed by it.

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

## Sketch: layer 1 for the queue

The lock slice shipped, and the two-layer rule survived contact: `DistributedLock.acquire` removes the
receipt, the fence and the renewals, and every one of them is still reachable one layer down. The queue is
the same shape with two things the lock did not have — a payload that is not bytes to the caller, and a
lease the caller never asked for.

### First, the wire: a claim's lease has no span

The lock shipped with a bug worth restating, because the queue has the same one in a worse form. A managed
holder has to renew on a cadence, and a cadence is a *duration*. The lock's grant stated only
`lease_expires_at`, an instant on the service's clock, so the managed form timed its renewals by the ttl
the caller had asked for — which the service silently clamps. Ask for thirty minutes, hold a ten-minute
lease, refresh first at fifteen: the lock lapsed five minutes before anything touched it. The fix was to
state the granted span as a duration, because a duration is the one quantity both ends agree on without
comparing clocks.

The queue cannot even make the lock's mistake, because it has nothing to make it with:

- `DequeueRequest` carries no ttl. A claim's lease is the service's `lease-ttl` configuration, so the
  caller never names it and cannot infer it from what it sent.
- `DequeueResponse` states `lease_expires_at` — an instant, on the service's clock.
- `HeartbeatResponse` states `renewed_until` — likewise.

So a consumer has no way to compute a heartbeat cadence except subtracting its own clock from the
service's, which is precisely the unsound thing the lock's fix exists to avoid. **Layer 2 for the queue is
not buildable without guessing until `lease_ttl` appears on `DequeueResponse` and `HeartbeatResponse`**,
the same additive change as the lock's.

The general rule this yields, and the reason `max_batch` needs no equivalent: **a service decision must be
stated on the wire when the answer does not already reveal it.** The batch ceiling is clamped silently too,
and that is fine — a caller counts the deliveries it got. The lease is clamped silently and nothing in the
answer reveals the span, so it has to be said.

### The interface

```scala
trait QueueClient:
  def enqueue(queue: String, message: Message.Outgoing): IO[QueueError, Enqueued]
  def dequeue(queue: String, maxWait: Duration, maxBatch: Int): IO[QueueError, Dequeued]
  def settle(receipt: Receipt, verdicts: Chunk[Verdict], retryAfter: Duration): IO[QueueError, Settled]
  def heartbeat(receipts: Chunk[Receipt]): IO[QueueError, Renewed]
```

No type parameters anywhere, which is the shape of the decision in *Payloads* above: the payload is bytes
here, and an encoder built one and a decoder will read one. The typing sits beside this interface rather
than in it.

Four RPCs, nothing withheld: per-message outcomes are a `Chunk[Verdict]` rather than an all-or-nothing ack,
the retry delay is the caller's, and the batch size is stated rather than derived. `heartbeat` takes every
receipt at once because that is what the RPC does — one call renews everything a consumer holds, which is
what makes layer 2's loop one fiber per client rather than one per claim.

### The types

Each is the wire's "a flag, and fields that mean something only when it is set" restated as a choice — the
same translation `Acquired` does for the lock.

```scala
enum Dequeued:
  case Idle                                    // nothing became ready in time, which is not an error
  case Claimed(claim: Claim)

final case class Claim(
  receipt:        Receipt,
  messages:       NonEmptyChunk[Message.Incoming],  // the wire's head + tail
  leaseExpiresAt: Instant,
  leaseTtl:       Duration,                    // once the wire states it
  backlogDepth:   Int,
)

sealed trait Message:
  def key: String
  def id: MessageId
  def payloadType: String
  def encoding: String
  def payload: Chunk[Byte]

object Message:
  final case class Outgoing(
    key:         String,
    id:          MessageId,
    payloadType: String,      // the name the organisation agreed on, stated by the caller
    encoding:    String,      // filled by the encoder, never by hand
    payload:     Chunk[Byte],
  ) extends Message

  final case class Incoming(
    key:         String,
    id:          MessageId,
    payloadType: String,      // stated rather than checked; the caller decides what to make of it
    encoding:    String,
    payload:     Chunk[Byte],
    sentAt:      Instant,
    attempt:     Int,
  ) extends Message

enum Settled:  case Applied, Stale
enum Outcome:  case Done, Failed
final case class Verdict(id: MessageId, outcome: Outcome)
final case class Renewed(stale: Chunk[Receipt], renewedUntil: Instant)
```

`head` and `tail` collapsing into one `NonEmptyChunk` is the piece most worth keeping. The proto spends a
comment explaining that `head` is what a consumer tests rather than an empty list; the type makes the
alternative unstatable instead of documented, and a claim with no work in it stops being a shape anyone can
construct.

`Message.Incoming` flattens the wire's `Delivery` and the `Message` inside it: that nesting exists so the
message type can be reused by `Enqueue`, which is a contract concern rather than a reader's. One trait with
two branches is what the wire models anyway — a single `Message` travelling in both directions — with the
asymmetry where the wire puts it.

`encoding` sits on the trait rather than on `Incoming` alone: the wire requires it in both directions, and
an outgoing message that did not state it would be one no receiver could safely read. A decoder fills it
going out and reads it coming back.

`stale` comes back as `Chunk[Receipt]` even though the service's own response type keeps it as
`Chunk[String]` — the reasoning there is that a service cannot call a receipt evidence when the caller may
have sent something it never issued. A client is on the other side of that: it only ever sends receipts it
holds, so what comes back is a subset of what it already had.

### Three decisions this sketch takes

**Typed, but by a parameter rather than a typeclass.** Layer 1 hands back an `A`, because
bytes-plus-`payload_type`-plus-`encoding` is the worst-typed thing on this wire and decoding it is what the
library is for. What it does not do is bind the schema name or the media type to the type — see *Payloads*
above for why that policy belongs a layer up.

**One error type, shared.** `LockError`'s four cases — fix the request, retry later, give up and report,
and an answer this client cannot read — mean exactly the same for the queue. Conceptually one type, so one
type, which costs a rename of something already published. Worth taking while the version still says
breaking changes are expected.

**`max_batch` stays the caller's at layer 1.** Deriving the batch from a consumer's parallelism is layer
2's job, and it is a choice removed rather than a capability added, which is the rule.

### What this slice needs that the lock's did not

| piece | why it is new |
|---|---|
| `lease_ttl` on two responses | layer 2 has no cadence without it — see above |
| an encoder and a decoder | the lock moved no user data at all |
| `zio-schema` | the first dependency the client adds for its own sake |
| nothing else | handing back bytes closed the batch and multi-type questions rather than answering them |
