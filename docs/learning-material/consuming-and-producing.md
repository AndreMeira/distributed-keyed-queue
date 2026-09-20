---
title: "Consuming and producing with the queue client"
type: learning-material
status: current
updated: 2026-09-20
tags: [client, queue, consumer, producer, heartbeat, schema, zio]
---

# Consuming and producing

The queue half of `distributed-keyed-queue-client`, from the outside. What it is for: a caller sends an
`A` and receives an `A`, and the lease, the receipt, the outcome and the retry are somebody else's
problem.

If you want the RPCs instead — per-message outcomes, the backlog depth, a batch you settle by hand — that
is `QueueClient` underneath, and everything below is built on it in public. The lock half is
[`taking-a-lock.md`](taking-a-lock.md).

## Depending on it

```scala
resolvers ++= Seq(
  "distributed-keyed-queue" at "https://maven.pkg.github.com/AndreMeira/distributed-keyed-queue",
  "homelab-toolkit-zio"     at "https://maven.pkg.github.com/AndreMeira/homelab-toolkit-zio",
)

libraryDependencies += "com.andremeira.homelab" %% "distributed-keyed-queue-client" % dkqVersion
```

One artifact, and it brings the stubs, a transport, `zio-schema` for the payload codecs, and
`homelab-common` for the messaging ports a consumer and a producer are.

**Two resolvers, not one.** `homelab-common` lives in the toolkit's own registry, and a published pom does
not name where its dependencies came from — so a build that lists only the first resolver fails to resolve
the second artifact with nothing to say why. Both registries also need a credential: GitHub Packages
serves Maven only to authenticated callers, whatever a package's visibility, and one classic PAT with
`read:packages` covers every package on the account. The recipe is in
[`using-the-contract-as-a-dependency.md`](using-the-contract-as-a-dependency.md#getting-them).

**Taking the lock and nothing else still pulls all of it.** The two halves ship in one artifact; if that
matters to you, the contract modules and the generated stubs are the smaller dependency, and the same
page says what they cost.

## Sending

Three things travel with every message that are not its bytes: which key it is ordered within, what it is
called, and what kind of thing it is. A `Producer` takes a value and nothing else, so each of those has a
home.

```scala
import homelab.keyedqueue.client.Endpoint
import homelab.keyedqueue.client.queue.{ Provider, QueueClient }
import homelab.keyedqueue.client.queue.model.*
import zio.schema.{ DeriveSchema, Schema }

final case class Order(id: String, customer: String, lines: Int)

given Schema[Order]          = DeriveSchema.gen
given MessageEncoder[Order]  = MessageEncoder.deriveAs("order.v2")

ZIO.scoped:
  for
    client   <- QueueClient.scoped(Endpoint("dkq", 9000))
    producer <- Provider(client).producerWith[Order]("orders"): order =>
                  MessageId(order.id) -> MessageKey(order.customer)
    _        <- producer.emit(Order("o-1", "c-9", lines = 3))
  yield ()
```

**The id comes from the value on purpose.** It is what a settle names, and what makes a repeated send one
message rather than two — so deriving it from what is being sent is what makes a retried `emit`
idempotent. Generate a fresh one per call and a retry becomes a duplicate.

**The key is the unit of exclusivity.** Messages sharing a key are delivered in order and never worked by
two consumers at once; messages with different keys are independent. Keying by the customer means one
customer's orders are serialised and different customers proceed in parallel. An empty key is a key of its
own, so a message sent without one is ordered against nothing.

**The payload type is the encoder's.** `deriveAs` names what this encoder writes; plain `derive` writes
`MessageEncoder.unnamed` — the literal `"*"` — for a queue where nobody checks.

If a type always names itself the same way, say so once with a `Partition` and use the shorter call:

```scala
given Provider.Partition[Order] with
  override def messageId(order: Order): MessageId   = MessageId(order.id)
  override def messageKey(order: Order): MessageKey = MessageKey(order.customer)

producer <- Provider(client).producer[Order]("orders")
```

## Consuming

```scala
given MessageDecoder[Order] = MessageDecoder.deriveAs[Order]("order.v2")

ZIO.scoped:
  for
    client   <- QueueClient.scoped(Endpoint("dkq", 9000))
    consumer <- Provider(client).consumer[Order](Provider.ConsumerConfig("orders"))
    _        <- consumer.consume(handle).forever
  yield ()

def handle(order: Order): IO[MyError, Unit] = ???
```

**One `consume` call processes one message.** It blocks until work arrives or the patience elapses, and
answers with nothing when nothing became ready — so the run loop is yours, and `.forever` is the whole of
it. The toolkit's `PollConsumer` is the same `Consumer` with a loop and a parallelism policy around it, if
you want one written for you.

**Your handler must be idempotent.** A message is delivered again after a failure, after a lease lapses,
and after a consumer dies mid-work. There is no delivery count that makes this go away — `attempt` tells
you how often it has been tried, not whether the work landed.

What happens around your handler, which is the point of the layer:

| | |
|---|---|
| while it runs | the claim is renewed on a beat timed by the lease the service granted |
| it answers | the message is settled done |
| it fails | the message is settled failed, and comes back after `retryAfter` |
| it is interrupted | the same — settled failed, so the work returns rather than vanishing |
| it never reads | the handler is not called at all; see the policy below |

The beat lives as long as the scope the consumer was built in. Close that scope and anything still claimed
lapses on its own lease and is delivered again — which is also what happens when a consumer dies, so there
is one behaviour rather than two.

## Messages that will not decode

A message whose bytes are not an `A` never reaches the handler: the port hands over values, so there is
nowhere to report one through. What becomes of it is chosen where the consumer is built.

```scala
Provider.ConsumerConfig("orders", policy = Provider.DecodingPolicy.DiscardAfter(3))
```

| policy | what it does |
|---|---|
| `Retry` | settle failed, so it comes back — forever, if nobody can ever read it |
| `Discard` | settle done, so it is gone |
| `DiscardAfter(n)` | come back until it has been delivered `n` times, then go |

`DiscardAfter` is the default because it is the only one of the three that neither loses a message on a
first bad read nor blocks a key for good. The service has no dead letter, so there is no third place to
put one.

A mismatch is worth distinguishing from corruption, and the failure says which: a decoder built with
`deriveAs` refuses anything labelled as another type or written in another format, and an `Unreadable`
carries what the sender claimed it was.

## Taking a key's messages together

```scala
batch <- Provider(client).batched[Order](Provider.BatchConsumerConfig("orders", size = 10))
_     <- batch.consume(handleAll).forever

def handleAll(orders: List[Order]): IO[MyError, Unit] = ???
```

One claim, one outcome: the handler answers and every message is done, it fails and every message comes
back — including the ones it had already worked. If you need to mark three of ten done and the rest
failed, that is `QueueClient.settle`, which states an outcome per message.

Two properties worth knowing before you choose a size:

- **Reading is all or nothing.** One message nobody can read keeps the whole claim from the handler,
  because a batch handed over in part is a batch answered for in part.
- **Settling is not.** Only the message that would not read takes the policy; the ones that read come back,
  since they are owed work nobody did.

A batch is one claim over one key's messages, so it does not widen how much you work at once across keys —
it lowers how many round trips one key's backlog costs.

## Bringing your own codec

`derive` and `deriveAs` write and read protobuf over a `zio-schema`. For anything else, the typeclasses are
two methods:

```scala
given MessageEncoder[Order] with
  override def payloadType: String            = "order.v2"
  override def encoding: String               = "application/json"
  override def encode(order: Order): Chunk[Byte] = ???
```

The decoder has a matching `deriveAs[A](payloadType, encoding)` taking a `BinaryCodec[A]`, for a codec
this client did not derive. Both strings are checked on the way in, because bytes in an unexpected format
can decode into something wrong rather than fail.

For a queue where nobody names anything, `import MessageDecoder.auto.given` and
`import MessageEncoder.auto.given` summon both from a `Schema` alone.

## One error type

Every call aborts with a `ServiceError`, which is an `ApplicationError.AdapterError` — so a dkq failure
reports the way every other adapter failure in the homelab does, and a handler's own errors widen with it
rather than being flattened into a wrapper. Dialling fails the same way: `QueueClient.scoped` answers with
`Failed` rather than raising the transport's own exception at you.

Four cases, three decisions: `Rejected` is yours to fix, `Unreachable` is worth retrying, and `Unreadable`
and `Failed` are to report.

## What the client does not decide for you

- **How many consumers to run.** One `Consumer` is one loop. Concurrency across keys is more loops, or the
  toolkit's `PollConsumer`.
- **Whether your handler is idempotent.** Nothing here can make it so, and everything here assumes it is.
- **What to do when a claim is lost.** A beat that comes back `stale` stops renewing that receipt, and the
  handler keeps running — stopping it is yours, because nothing on the server side can stop your code.
  `../architecture/guarantees.md` has the full statement.
