---
title: "Open question: the encoder does not carry a payload type and the decoder does"
type: session
status: draft
updated: 2026-09-20
tags: [client, queue, payload, typeclass, encoder, decoder, open-question]
---

# The payload type sits on one side and not the other

Layer 1's payload typeclasses came out asymmetric, and the asymmetry was arrived at one decision at a
time rather than designed. It holds up under the reasons below, but nobody has looked at it as a whole.
This records it so the question can be asked properly rather than rediscovered.

## What the two look like

```scala
trait MessageEncoder[A]:
  def encoding: String                 // the media type — bound to the instance
  def encode(value: A): Chunk[Byte]
                                       // no payloadType

trait MessageDecoder[A]:
  def decode(message: Message.Incoming): Either[Failure, A]

object MessageDecoder:
  def expecting[A: Schema](payloadType: String): MessageDecoder[A]   // the name IS bound to the instance
```

So a decoder can be built to accept exactly `order.v2`, while an encoder has no idea what the thing it is
writing is called. The name is supplied to `Message.Outgoing` at each call, and to a `Provider.producer`
once per producer.

## Why it ended up that way

Three decisions, each defensible on its own:

1. **The rejected `Payload[A]` typeclass** bundled schema, `payload_type` and `encoding`. It was dropped
   because binding the organisation's name for a type *to the type* is a policy, and the same `A` may
   legitimately travel as `order.v2` from one service and something else from another.
2. **`encoding` is derivable and `payload_type` is not.** Encode with the protobuf codec and the media
   type is `application/x-protobuf`; nothing derives "order.v2" from a `Schema`. That split is what lets
   `MessageEncoder.derive` and `MessageEncoder.auto` exist at all — an instance summoned from a schema
   alone.
3. **Stating is mandatory, checking is optional.** The wire requires an outgoing message to carry a
   payload type, so something must supply it; an incoming one can be decoded without ever looking at the
   field, so holding an expected name is a capability rather than an obligation.

(3) is the one that actually explains the shape: the encoder cannot hold the name without making it
mandatory at instance construction, which kills `auto`; the decoder can hold it optionally because
`derive` exists beside `expecting`.

## Why it still looks wrong

- Two typeclasses over the same wire field, one of which knows about it and one of which does not.
- A caller writing both sides states `"order.v2"` in two unrelated places — once building the producer,
  once building the decoder — with nothing tying them together. Nothing catches a producer sending
  `order.v2` into a consumer expecting `order.v3` until the message is refused at run time.
- `MessageEncoder.protobuf` is a constant on the companion, so the *encoding* half already has a shared
  vocabulary. The *name* half has none.

## What a closer look should weigh

- An encoder with an optional name — `derive` without, `stating(payloadType)` with — which would mirror
  `derive`/`expecting` exactly and keep `auto` alive. Then `Message.Outgoing` and `producer` take the name
  only when the encoder does not carry it.
- A single value a caller declares once and both sides take, so a producer and a consumer of the same
  contract cannot drift. That is close to the rejected typeclass, but as a *value* rather than an
  instance bound to `A`, which is what made the typeclass wrong.
- Leaving it: the asymmetry is real, it has a cause, and the cost is one repeated string literal.

Not urgent. Nothing is broken; a mismatch is refused at the boundary and reported as
`MessageDecoder.Failure.WrongType`, which is exactly what that case is for.
