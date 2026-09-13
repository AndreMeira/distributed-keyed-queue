---
title: "What could back DKQ besides Redis — the atomic-claim filter, and a wake when the substrate has none"
type: research
status: draft
updated: 2026-09-10
tags: [substrate, redis, postgres, foundationdb, mongodb, wake, pod-to-pod, membership, architecture]
---

# What could back DKQ besides Redis — the atomic-claim filter, and a wake when the substrate has none

DKQ is a set of semantics (per-key exclusivity, per-key order, at-least-once, lease recovery) built *on* a
substrate, not tied to one. `QueueStore` is the port; `infrastructure/redis/` is the only implementation
today. This asks which other substrates could implement that port, and — since the answer separates cleanly
into two questions — treats the wake mechanism on its own, including how to build one when the substrate
offers nothing.

Draft. Nothing here is built or benchmarked; vendor specifics (TTL lag, watch limits, value ceilings) are
from memory and flagged where they decide a verdict.

## The port is five operations, and one of them is the whole filter

```
enqueue   append a message to a key's FIFO, make the key claimable if idle
claim     take the oldest claimable key, lease it, hand back a batch + a fence token
settle    remove settled ids from what the claim owns; end the claim when none remain
renew     push the lease deadline forward on the claims a caller still holds
sweep     return claims whose lease lapsed, and keys whose backoff elapsed
```

`claim` is the filter. Read the current `claim.lua` and it is: pop the oldest key from `ready`, read its
messages, write a lease, mark them owned, bump per-message attempt counts, advance a fence — **and no other
consumer may interleave between the pop and the lease**, or two consumers work one key and the core
guarantee is gone. So the substrate must offer one of:

- **single-threaded execution** — Redis/Valkey Lua runs to completion with nothing else touching the
  keyspace; or
- **serializable multi-key transactions** — the same effect by isolation rather than by being alone; or
- **row-claiming with skip-locked** — `SELECT … FOR UPDATE SKIP LOCKED LIMIT n` claims the oldest unclaimed
  row and no concurrent claimer sees it.

Anything offering only single-item compare-and-swap can *approximate* it, but the "oldest claimable key
across the queue" step wants either an ordered scan under a lock or a transaction; per-item CAS turns that
into a retry loop that degrades under contention on a hot queue. This is the axis that sorts the field.

`sweep` adds a softer requirement: the lease deadline must be **queryable** ("give me every claim past
now"). This is why a lazy TTL — DynamoDB's, MongoDB's TTL index — cannot *be* the lease: it deletes on the
substrate's own schedule (minutes to 48h late), long after the work should have been reclaimed. Those
substrates keep the deadline as an ordinary indexed field and sweep it explicitly, exactly as DKQ already
does.

## The second question is independent: the wake

The wake is an accelerator, not a correctness dependency — established in `minimal-structures.md` and the
`QueueReadiness` design: a token means "look now", the claim reads real state, and a lost token costs latency,
never safety (a retrying caller re-looks). So the wake can come from anywhere, and a substrate scores on it
*separately* from the atomic-claim axis. Native options, best to worst for our purposes:

- **Resumable log / change stream** (Redis Streams, MongoDB change streams, etcd/Consul watch-from-revision,
  DynamoDB Streams): a reader that reconnects resumes from where it was, so a blip loses nothing. This is
  what DKQ uses today.
- **Fire-and-forget pub/sub** (Redis pub/sub, Postgres `LISTEN/NOTIFY`): misses whatever arrived while
  disconnected — fine here, because the level-triggered claim is the backstop.
- **One-shot watches** (ZooKeeper): fire once, must be re-armed; edge-triggered, the fragile kind, but
  usable.
- **Nothing**: the substrate is a pure transactional store. Covered in its own section below — this is the
  part worth designing, because it frees the substrate choice from needing a messaging feature at all.

## The candidates

| substrate | atomic claim | lease sweepable | native wake | homelab fit | verdict |
|---|---|---|---|---|---|
| **Redis / Valkey** | Lua (single-threaded) | zset by deadline | Streams (resumable) | self-host, tiny | **reference** |
| **PostgreSQL** | txn + `SKIP LOCKED` | indexed column | `LISTEN/NOTIFY` | self-host, boring-good | **strongest alternative** |
| **MongoDB** | `findAndModify` + txns | indexed field | change streams (resumable) | self-host | strong |
| **FoundationDB** | serializable txns | indexed subspace | key **watches** | operationally heavy | strong, over-built for homelab |
| **etcd** | mvcc txn / CAS | lease + key | watch-from-revision | small data only | fits config-scale, not throughput |
| **ZooKeeper** | sequential znodes | **session = lease** | one-shot watches | small data only | elegant lease, weak throughput |
| **DynamoDB** | `TransactWriteItems` | field + explicit sweep | Streams | cloud-only | works, TTL trap |
| **Cassandra / Scylla** | LWT (Paxos) | column + sweep | none (CDC) | self-host | claim contends, poor fit |
| **Kafka / JetStream** | — | — | — | — | wrong model (see below) |

### PostgreSQL — the one I would reach for next

*Worked out in full in [`postgres-substrate.md`](postgres-substrate.md); the summary:*

It is the only candidate that answers *both* questions natively and well. The claim is the canonical
work-queue pattern — `SELECT id FROM msgs WHERE key = … AND claimable ORDER BY seq FOR UPDATE SKIP LOCKED
LIMIT n`, set `claimed_until` in the same transaction — which is exactly the shape the `QueueStore` doc
already anticipates. The wake is `LISTEN/NOTIFY`: `NOTIFY dkq_wake, 'orders'` on the enqueue commit, every
instance `LISTEN`ing. It is fire-and-forget (a disconnected listener misses notifications), which is fine.
One real wrinkle: `LISTEN` holds a connection per instance, and `NOTIFY` only fires on commit — but DKQ's
enqueue is a single committed transaction, so that lines up. The whole of DKQ's Lua could become a handful
of functions or plain parameterised transactions; the fence, owned-set, attempt counts are all ordinary
columns. **For a homelab that already runs Postgres, this is less operational surface than Redis, not more.**

### MongoDB — strong, one trap

`findAndModify` is an atomic single-document claim, and change streams are a first-class resumable wake
(better than `LISTEN/NOTIFY`, on par with Redis Streams). The trap is the lease: a TTL index is lazy, so the
lease must be a plain `Date` field swept explicitly. The awkward part is "oldest claimable key across the
queue" as a single atomic step — `findAndModify` is one document; spanning a key's message list plus its
lease plus its fence wants a multi-document transaction, which Mongo has but which is heavier than a Lua
call. Workable, not obviously better than Postgres.

### FoundationDB — technically the best, operationally the worst fit

Serializable transactions across arbitrary keys are exactly what the claim wants, with no single-threaded
bottleneck, and **watches** are a real wake (a transaction can register up to ~10k key-change
notifications per client — verify the current limit). It would scale DKQ far past anything the homelab
needs. The cost is running an FDB cluster, a 10MB transaction / 5-second limit to design within, and a
client model most people have never touched. Right answer for a large multi-tenant deployment; wrong amount
of machinery for a home cluster.

### etcd / Consul / ZooKeeper — coordination stores, not throughput stores

All three give a transactional claim (etcd mvcc txn, Consul txn+ModifyIndex, ZK sequential znodes) and a
native wake (etcd/Consul watch-from-revision, ZK one-shot watches). Two things recommend them at small
scale and rule them out at large: values are meant to be small (etcd ~1.5MB/key and discouraged; ZK ~1MB
znodes), and write throughput is a fraction of Redis or Postgres. **ZooKeeper has one genuinely elegant
property worth stealing regardless of substrate:** an ephemeral znode tied to the client session *is* the
lease — if the consumer's process dies, the session expires, the node vanishes, and the claim releases with
no watchdog and no sweep. DKQ pays for lease recovery with a periodic sweep precisely because Redis has no
session concept; a session-scoped substrate gets it for free.

### Kafka / NATS JetStream — the model DKQ exists because they can't provide

Named only to close the door. These are logs/brokers: ordering is per partition/subject, and "one key
worked by one consumer with a minute-long lease, thousands of keys concurrent" is the thing the README opens
by saying they can't do. They are not substrates for DKQ; DKQ is the alternative to them.

## Building a wake when the substrate has none

Suppose the substrate wins on the atomic claim and offers nothing for the wake — a plain transactional store,
or one whose change feed we would rather not depend on. Because correctness rests on the level and the wake
is pure acceleration, the wake can be built *beside* the substrate, pod to pod, and be best-effort without
risking a lost message.

**The seam already exists.** `QueueReadiness` is a pure in-process coordination primitive — it knows nothing
about Redis (its three "redis" mentions are prose). What is Redis-specific is `ReadinessListener`: it reads the
streams and calls `readiness.ready(queue)`. Replace *what calls `readiness.ready`* and the substrate's wake
is gone without touching the claim path. Extract the port:

```
trait WakeChannel:
  def announce(queue: QueueName): UIO[Unit]        // "this queue may have work" — called on enqueue/settle
  def listen(onWake: QueueName => UIO[Unit]): UIO[Unit]   // deliver announcements to readiness.ready
```

`RedisWakeChannel` is today's `ReadinessListener`. `PeerWakeChannel` is the substrate-free one.

### The peer mesh

Every DKQ instance is identical and any of them can claim any key (they all poll the same `ready`). So a
wake need not reach a *particular* instance — broadcasting "queue X may have work" to *all* peers, each
offering a local readiness token, wakes one consumer per instance to race for the claim. That is
**semantically identical to the shared wake stream today** (the README's known "instances race to claim"
behaviour), just delivered peer-to-peer instead of via the substrate.

```
enqueue commits ──▶ announce(queue) ──▶ broadcast to N-1 peers ──▶ each peer: readiness.ready(queue)
```

Best-effort is acceptable for the same reason a trimmed stream is: a dropped broadcast costs one consumer
one look, recovered by the retry loop (and, if adopted, bounded by the `simplify-exp-2` backstop). No
delivery guarantee, no persistence, no ordering — the substrate remains the source of truth, the mesh only
says "look sooner".

Fan-out is O(N) per enqueue, N = instance count. For a homelab (a handful of instances, low volume) that is
nothing; it becomes the dominant cost only at hundreds of instances, which is not this system's regime.

### Membership: who are my peers?

The mesh needs the peer set, and this is where the real complexity lands — not in the broadcast.

- **Kubernetes headless Service** — DNS returns every pod IP; watch the `Endpoints` (or `EndpointSlice`)
  via the k8s API for changes, or refresh DNS on a timer. Simplest where DKQ already runs on k8s, which the
  homelab cluster is. No extra infrastructure.
- **Gossip / SWIM** (à la `memberlist`) — self-organising membership with failure detection, no k8s
  dependency. More moving parts and its own failure modes; overkill when k8s already knows the pod set.
- **The substrate as registry** — instances write a heartbeated presence row; peers read it. Circular
  (using the store to find peers so peers can avoid using the store for wake) but requires nothing new, and
  the presence read is off the hot path.

For the homelab: **headless Service + Endpoints watch.** It reuses what k8s already tracks, and a pod
appearing or leaving is exactly an Endpoints change.

### Transport

gRPC streaming, consistent with the rest of the homelab and `homelab-schemas` — each instance opens a
long-lived stream to each peer and pushes `(queue)` announcements. A dead peer's stream breaks and is
dropped from the set on the next Endpoints change; a new peer's stream opens then. UDP multicast would be
lighter but only works on one L2 segment and is a poor fit for k8s networking.

### What this buys and what it costs

Buys: the substrate choice stops requiring a messaging feature. Any store that wins the atomic-claim
filter — FoundationDB, a bare Postgres without `LISTEN/NOTIFY`, a KV with transactions — becomes viable
with reactive (not polling) wake latency.

Costs, stated plainly: DKQ instances stop being independent. Today an instance talks only to Redis; a mesh
means discovery, N² connections, and a membership failure mode that did not exist. That is a real increase
in operational surface, and it is only worth paying when the substrate that wins on the claim happens to
lose on the wake. When the substrate offers a resumable change feed (Redis, Mongo, etcd) or even
`LISTEN/NOTIFY` (Postgres), use it and keep the instances independent.

## Recommendation

1. **Stay on Redis/Valkey** for the homelab. It wins both axes and is the smallest thing to run.
2. **PostgreSQL is the alternative to reach for** if consolidating on one datastore matters more than the
   last millisecond — it answers both questions natively, and a homelab already running Postgres shrinks
   operational surface by dropping Redis.
3. **The pod-to-pod wake is the interesting piece to prototype** regardless of substrate, because it
   converts the wake from a substrate requirement into a `WakeChannel` implementation — and the port seam is
   already there in `QueueReadiness`. The cheapest experiment: a `PeerWakeChannel` over the existing gRPC stack
   with k8s-Endpoints membership, tested by running the demo's two instances and confirming an enqueue on
   one wakes a consumer on the other with no Redis stream involved.

## Open questions

1. Does dropping the substrate wake for the mesh actually pay, or is a resumable change feed (which several
   strong candidates have) always the better answer? The mesh's value is entirely in unlocking
   *feed-less* substrates.
2. Session-as-lease (the ZooKeeper property): analysed in [`zookeeper-ideas.md`](zookeeper-ideas.md) — it
   would delete the sweep's reclaim pass but not the sweep, and DKQ already has its consumer-facing
   behaviour; a latent option, not a change. Postgres advisory locks are its one real substrate home.
3. Is a second adapter worth building at all before there is a second deployment that needs it? The port is
   validated by having been designed against two shapes (script vs `SKIP LOCKED`); building the Postgres one
   with no consumer would be speculative.
