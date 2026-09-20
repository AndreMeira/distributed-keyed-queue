---
title: "Resolved: the encoder and the decoder both carry a payload type"
type: session
status: current
updated: 2026-09-20
tags: [client, queue, payload, typeclass, encoder, decoder, symmetry]
---

# The payload type sat on one side and now sits on both

Layer 1's payload typeclasses came out asymmetric — a decoder could be built expecting `order.v2`, an
encoder had no idea what it was writing — and the asymmetry was arrived at one decision at a time rather
than designed. It was written down as an open question and resolved the same day. Both halves are kept
here: the reasoning that produced the asymmetry was sound, and it is what made the resolution cheap.

## How it looked

```scala
trait MessageEncoder[A]:
  def encoding: String                 // the media type — bound to the instance
  def encode(value: A): Chunk[Byte]
                                       // no payload type

object MessageDecoder:
  def expecting[A: Schema](payloadType: String): MessageDecoder[A]   // the name IS bound to the instance
```

The name was supplied to `Message.Outgoing` at each call and to a producer once per producer.

## Why it ended up that way

Three decisions, each defensible on its own:

1. **The rejected `Payload[A]` typeclass** bundled schema, `payload_type` and `encoding`. It was dropped
   because binding the organisation's name for a type *to the type* is a policy, and the same `A` may
   legitimately travel as `order.v2` from one service and something else from another.
2. **`encoding` is derivable and `payload_type` is not.** Encode with the protobuf codec and the media
   type is `application/x-protobuf`; nothing derives "order.v2" from a `Schema`.
3. **Stating is mandatory, checking is optional.** The wire requires an outgoing message to carry a
   payload type, so something must supply it; an incoming one can be decoded without ever looking at the
   field.

(2) and (3) together looked decisive: an encoder holding the name would have to demand it at construction,
and then `MessageEncoder.derive` and `MessageEncoder.auto` — an instance summoned from a schema alone —
could not exist.

## What it cost

- Two typeclasses over the same wire field, one of which knew about it and one of which did not.
- A caller writing both sides stated `"order.v2"` in two unrelated places, with nothing tying them
  together and nothing catching a producer sending `order.v2` into a consumer expecting `order.v3` until
  a message was refused at run time.

## What resolved it

A **default name**. The encoder carries a payload type; when nobody gives one it writes
`MessageEncoder.unnamed`, the literal `"*"`. That keeps the summon-from-schema property, which was the
only real argument for the asymmetry — `derive` needs no name, so `auto` still works.

```scala
MessageEncoder.derive[Order]                 // writes "*"
MessageEncoder.deriveAs[Order]("order.v2")   // writes the name
MessageDecoder.derive[Order]                 // accepts anything
MessageDecoder.deriveAs[Order]("order.v2")   // accepts only that name
```

Three things follow. `Message.Outgoing(key, id, value)` takes the name from the encoder rather than from
the caller. A producer no longer states one. And decision (1) above still holds: the name is bound to the
*instance*, not to `A`, so the same type travels under two names by using two encoders.

**There is no wildcard matching anywhere.** `"*"` is a literal both sides default to, so a decoder built
with a name and an encoder that never had one simply disagree — which is the right answer, and needs no
special case in the matcher.

## Two things Scala decided rather than us

- **`deriveAs`, not an overload of `derive`.** Overloading a no-arg and a one-arg method of the same name
  breaks under explicit type parameters: `MessageDecoder.derive[Order]` is read as partially applying the
  one-arg version. Every call site writes the type parameter, so the names had to differ.
- **`producerWith` beside `producer`.** Same rule one level up: `producer(name)(parts)` and
  `producer(name)` cannot be told apart on their first parameter list, so the lambda-taking primitive and
  the `Partition`-summoning convenience carry different names.

## What is still open

The decoder's no-argument `derive` checks nothing at all — not the payload type and not the encoding —
while `deriveAs` checks both. Whether the *encoding* check should be unconditional is a separate question
from the one resolved here: it is the check that prevents bytes in an unexpected format from decoding into
something wrong, and unlike the name it is always derivable from the codec.
