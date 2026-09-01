---
title: "Redis streams, and driving them from Lettuce"
type: learning-material
status: current
updated: 2026-09-01
tags: [redis, valkey, streams, lettuce, xread, xadd]
---

# Redis streams, and driving them from Lettuce

What a stream is, the handful of commands that matter, and the Lettuce API for each — verified against
`lettuce-core` 6.7.1, the version this repo pins.

## What a stream is

An append-only log. Each entry has an **id** and a set of **field/value pairs**:

```
1756704000123-0   queue=orders
1756704000456-0   queue=emails
1756704000456-1   queue=orders
```

The id is `<unix-millis>-<sequence>`, assigned by Redis when you append with `*`. Ids only ever increase,
and the sequence disambiguates entries added in the same millisecond. Readers remember the last id they
saw and ask for what came after it.

Three things distinguish it from the structures already in this repo:

| | list (`ready`) | pub/sub | stream |
|---|---|---|---|
| an item goes to | exactly one reader | every *connected* subscriber | every reader, each at its own position |
| survives a reader being away | yes (it stays in the list) | no — it is gone | yes, until trimmed |
| reading removes it | yes (`LPOP`) | n/a | no |

That last row is the mental shift: **reading a stream doesn't consume it.** Ten readers can each read every
entry; each simply tracks where it got to. Which is why it fits a notification bus, where a list would hand
the notification to one instance and leave the others unaware.

## Writing

```
XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] <*|id> field value [field value ...]
```

- `*` lets Redis assign the id. You can pass an explicit id, but it must be greater than the stream's last.
- The stream is created by the first `XADD` unless `NOMKSTREAM` is given.
- **Trimming is part of the write**, and that is where you keep memory bounded: `MAXLEN ~ 1000` caps the
  length approximately, trimming only at internal node boundaries, which is much cheaper than the exact
  form and is what you want in a hot path. `MINID` trims by id instead, which because ids embed a
  timestamp is really "drop anything older than this".

A stream nobody trims grows forever, and one nobody reads still costs memory. `MAXLEN ~` on every append
is the simplest safe habit.

## Reading

```
XREAD [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...]
```

- It returns entries **strictly after** the ids you pass, so you pass the last id you processed.
- The special id `$` means "nothing that exists now — only what arrives after this call". It makes sense
  only with `BLOCK`, and only for a reader with no history to catch up on.
- `BLOCK ms` parks the connection until an entry arrives or the timeout expires; a timeout returns nil
  rather than an error. `BLOCK 0` waits forever.
- Multiple streams in one call, each with its own id. **In Redis Cluster every key in the call must hash to
  the same slot**, which is the constraint that decides how many reader connections a cluster deployment
  needs.

A blocked `XREAD` occupies its connection exactly like `BLMOVE` does, so it wants a connection of its own.

`XRANGE key - +` reads history without blocking (`-` and `+` are the minimum and maximum ids), and `XLEN`
gives the length — both useful when inspecting a live system.

## Consumer groups, and when not to use them

`XREADGROUP` turns the stream back into point-to-point delivery: entries are handed to one consumer per
group, tracked in a **pending entries list** until `XACK`, and recoverable with `XAUTOCLAIM` when a
consumer dies holding one.

That is the right shape for a work queue, and the wrong shape for a notification bus — with a group, a
notification can be delivered to an instance that has nobody waiting, and sit in its PEL while another
instance's consumer waits. Plain `XREAD` gives every reader every entry, which is what a doorbell needs.

## Lettuce

`RedisStreamCommands` is mixed into both `RedisCommands` and `RedisAdvancedClusterCommands`, so the same
calls work on either — including this repo's `Connection.Commands`, which is
`RedisClusterCommands[String, Array[Byte]]`.

Note the generics: the body is a `Map[K, V]`, so **field names use the key codec and values use the value
codec**. With this repo's `RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE)` that means
`Map[String, Array[Byte]]` — string field names, byte-array values.

### Appending

```scala
import io.lettuce.core.XAddArgs

val body = java.util.Map.of("queue", "orders".getBytes("UTF-8"))
val args = XAddArgs.Builder.maxlen(1000).approximateTrimming()   // MAXLEN ~ 1000

val id: String = redis.xadd(streamKey, args, body)
```

`XAddArgs.Builder` gives `maxlen`, `minId`, `nomkstream`; the instance methods add `approximateTrimming()`,
`limit(n)` and `id(...)`. There is also a varargs `xadd(key, field, value, field, value…)` if a map is
awkward.

### Reading

```scala
import io.lettuce.core.XReadArgs
import io.lettuce.core.XReadArgs.StreamOffset

val offsets = Array(
  StreamOffset.from(ordersStream, lastOrdersId),   // everything after this id
  StreamOffset.latest(emailsStream),               // "$" — only what arrives from now
)

val batch: java.util.List[StreamMessage[String, Array[Byte]]] =
  redis.xread(XReadArgs.Builder.block(java.time.Duration.ofSeconds(5)).count(64), offsets*)
```

Each `StreamMessage` carries `getStream()`, `getId()` and `getBody(): Map[K, V]`. `StreamOffset` has
`from(key, id)`, `latest(key)` (`$`), `last(key)` and `lastConsumed(key)` (`>`, for groups).

An expired block comes back as an **empty list**, not an exception — the loop just goes round again.

### The command-timeout trap

This is the same one `BLMOVE` set in this repo. Lettuce's connection-level command timeout is enforced
client-side, so a `BLOCK 5000` on a connection whose command timeout is 5 s races its own deadline and
surfaces as `RedisCommandTimeoutException` instead of an empty result. **The connection's command timeout
must exceed the longest `BLOCK` it will be asked to make** — which is why `Connection.claimingSlack` exists
for the claiming connections and why a stream reader wants the same treatment, on its own connection.

### Cluster

`RedisAdvancedClusterCommands` accepts the same calls, but a multi-stream `XREAD` across different slots
fails with `CROSSSLOT`. Group streams by slot — and remember that keys sharing a hash tag share a slot, so
`{q:orders}:wake` is in the same slot as `{q:orders}:ready` and can be written by the same Lua script.

### From Lua

`XADD` with a `*` id is allowed inside scripts on any modern Redis or Valkey, because scripts replicate by
their effects — the generated id is what propagates to replicas. Under the old verbatim replication this
was rejected as non-deterministic.

## Gotchas worth knowing before you rely on it

1. **`$` is not a bookmark.** It means "from now", evaluated at call time. Reconnect with `$` and you lose
   everything published while you were away; reconnect with your last id and you don't. That difference is
   the main reason to prefer a stream over pub/sub.
2. **Ids come from the server clock.** They are monotonic per stream — Redis will not accept an id lower
   than the last — but they are not a global ordering across streams.
3. **`XDEL` leaves a tombstone.** It removes the entry's payload; it does not renumber anything, and ids
   never get reused.
4. **Reading does not trim.** Memory is bounded by whoever writes, via `MAXLEN`/`MINID` — not by consumers
   keeping up.
5. **A blocking read holds a connection**, so it belongs on a dedicated one, exactly like `BLMOVE`.
6. **`COUNT` bounds a single reply**, not the block: `XREAD COUNT 64 BLOCK 5000` returns as soon as *one*
   entry exists, with up to 64.

## Where this is going in this repo

Streams are not used yet. The design that would introduce them — one `wake` stream per queue, appended to
inside the scripts that make a key claimable, read by one blocking `XREAD` per instance — is
[`../research/non-blocking-dequeue.md`](../research/non-blocking-dequeue.md).
