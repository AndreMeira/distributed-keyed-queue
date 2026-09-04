# distributed-keyed-queue

A queue over gRPC where **one key is worked by one consumer at a time** — enforced by the storage layer,
not by a lock inside your process.

```
Enqueue(queue, key, message)  ──▶  ┌──────────┐  ──▶  Dequeue(queue)  ──▶  a claim on ONE key,
                                   │  Redis / │       (long poll)          its oldest messages,
                                   │  Valkey  │                            and a lease
                                   └──────────┘  ◀──  Settle(receipt, per-message outcomes)
```

## The problem it solves

Plenty of systems give you ordering per partition. Few give you *per-key serial processing with long-lived
handlers*: "everything for customer 42 happens one at a time, in order, and a handler may take a minute",
while thousands of other keys run concurrently.

The usual answers each cost something:

- **Partition and pin consumers to partitions.** Ordering is per partition, so a slow key blocks every
  other key that hashes to it, and rebalancing moves ownership underneath you.
- **Serialise in the consumer.** A lock keyed by the message key, held in process memory. Correct on one
  instance; meaningless across two, which is where the requirement usually came from.

dkq puts the exclusivity where every instance can see it. A consumer **claims a key**, gets a lease and a
fencing token, and nothing else may work that key until the claim ends or the lease lapses. Restarts,
deployments and network partitions are all covered by the same mechanism, because the claim lives in the
store rather than in a process.

## What it guarantees

- **Per-key exclusivity.** At most one consumer is authorised to work a key at a time, across every
  instance.
- **Per-key order.** A key's messages are handed out oldest first; an unacknowledged one keeps its place.
- **At-least-once delivery**, with a per-message attempt count so redelivery is visible.
- **Nothing is lost when a consumer dies.** The lease lapses, a watchdog revokes the claim, and the work
  returns.
- **Batch claims, individual settles.** One claim can cover several of a key's messages; each is settled on
  its own, and the key is released once none are outstanding.

The precise contract — including what is *not* guaranteed, and what a consumer must do to hold up its end —
is [`docs/architecture/guarantees.md`](docs/architecture/guarantees.md). Read it before writing a consumer.

## The API

Four unary RPCs, defined in
[`keyed_queue_service.proto`](modules/protocol/src/main/protobuf/homelab/keyedqueue/v1/keyed_queue_service.proto):

```proto
service KeyedQueue {
  rpc Enqueue  (EnqueueRequest)   returns (EnqueueResponse);    // accept a message for a key
  rpc Dequeue  (DequeueRequest)   returns (DequeueResponse);    // long-poll for a claim
  rpc Settle   (SettleRequest)    returns (SettleResponse);     // report what became of each message
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);  // renew the claims still held
}
```

A `Dequeue` answers with a **receipt** (the claim), a **head** delivery, any **tail** the batch included,
and the **lease expiry**. Every `Settle` names the receipt and what became of which message id. A consumer
that works longer than the lease must `Heartbeat` on a tick, and must stop the moment a heartbeat reports a
claim stale — that is the half of the contract dkq cannot enforce for you.

## Using it from a service

Two artifacts are published to GitHub Packages: `distributed-keyed-queue-protocol` (the message types) and
`distributed-keyed-queue-protocol-zio-grpc` (the ZIO client and server stubs). What to depend on, how to
authenticate to GitHub Packages, and what a consumer still has to write itself is in
[`docs/learning-material/using-the-contract-as-a-dependency.md`](docs/learning-material/using-the-contract-as-a-dependency.md).

Any gRPC client works — the contract ships as `.proto`, so a consumer in another language generates its own
stubs.

## Running it

```bash
docker compose up -d          # a Valkey to back it
sbt run                       # the service, on :9000
```

Settings are HOCON with an environment override for every key
(`modules/server/src/main/resources/config/queue.conf`):

| variable | default | what it decides |
|---|---|---|
| `DKQ_REDIS_URL` | `redis://localhost:6379` | where the substrate lives |
| `DKQ_CLUSTER` | `false` | whether that URL names a Redis Cluster |
| `DKQ_PORT` | `9000` | the gRPC port |
| `DKQ_LEASE_TTL` | `30 seconds` | how long a claim survives without a heartbeat |
| `DKQ_MAX_WAIT` | `30 seconds` | the longest `Dequeue` wait honoured |
| `DKQ_MAX_BATCH_LIMIT` | `32` | the most messages one claim may take |
| `DKQ_WAKE_BLOCK` | `1 second` | how long one read of the wake streams waits before going round again |
| `DKQ_WAKE_BUCKETS` | `1` | how many wake streams the queues are spread over — and so how many hash tags. Permanent for a deployment |
| `DKQ_SWEEP_INTERVAL` | `5 seconds` | how often each instance runs repair |
| `DKQ_SWEEP_LIMIT` | `100` | entries one sweep handles, per kind |

Every instance is identical and stateless — the queue's state is entirely in Redis — so scaling out is
running more of them against the same store. Redis Cluster is supported: every key a queue uses carries the
same hash tag, so a queue lives on one slot and sharding spreads queues rather than splitting one
([`docs/architecture/redis-cluster.md`](docs/architecture/redis-cluster.md)).

## Building from source

```bash
sbt compile
sbt test    # unit + integration, against a Testcontainers Valkey (needs Docker)
sbt e2e     # builds the image, composes two instances, drives them over the wire
```

Building requires a GitHub personal access token with `read:packages` in `~/.sbt/1.0/credentials`, because
the shared homelab toolkit this service depends on resolves from GitHub Packages, which serves Maven only
to authenticated callers:

```
realm=GitHub Package Registry
host=maven.pkg.github.com
user=<your-github-username>
password=<a-classic-pat>
```

## Performance

On a laptop, two instances against one Valkey: **1,552–4,339 msg/s** end to end, the ceiling set by how
many consumers are running rather than by keys or connections. Method and numbers:
[`docs/research/throughput-first-numbers.md`](docs/research/throughput-first-numbers.md).

## Status

**A POC, and honest about it.** The semantics are settled and tested — unit, integration against a real
Valkey, and an end-to-end suite that kills an instance mid-handler — but this is not a system anyone should
run critical work on yet. Expect breaking changes between versions; pin an exact one.

Known gaps:

- **Poison messages.** `attempt` is counted per message but nothing acts on it. A permanently failing
  message cycles rather than wedging its key, so this is not urgent — what is missing is somewhere to put
  it once the count is too high.
- **One queue per `Dequeue`.** A consumer spanning queues needs a connection each.
- **A hot key stays on one instance.** Waiting consumers are woken by a broadcast and race to claim, and
  the instance that just settled a key is already claiming while the others are being woken. Nothing is
  lost — a key is worked by one consumer at a time regardless — but "several consumers" does not mean the
  work for one key is spread across them.
- **No persistence.** The POC runs Valkey with saving off; durability is a later phase.

## Docs

- [`docs/architecture/guarantees.md`](docs/architecture/guarantees.md) — the contract, for consumers and
  test authors
- [`docs/architecture/redis-data-structures.md`](docs/architecture/redis-data-structures.md) — what is kept
  in Redis and why
- [`docs/architecture/redis-cluster.md`](docs/architecture/redis-cluster.md) — the key layout and cluster
  mode
- [`docs/learning-material/redis-state-walkthrough.md`](docs/learning-material/redis-state-walkthrough.md) —
  every request traced through the structures it touches
- [`docs/README.md`](docs/README.md) — the full index

## Licence

Apache-2.0 — see [`LICENSE`](LICENSE).
