# distributed-keyed-queue (DKQ)

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
handlers*.

> **Example**: Say you are sending commands to a fleet of machines. 
Each machine must be given its commands in order and needs to acknowledge each command 
before the next can be sent — which may take a minute. You want thousands of commands moving across 
thousands of machines at once, while any one machine is still only ever working on a single command.

The usual answers each cost something:

- **Partition and pin consumers to partitions.** Ordering is per partition, so a slow key blocks every
  other key that hashes to it, and rebalancing moves ownership underneath you.
- **Serialise in the consumer.** A lock keyed by the message key, held in process memory. Correct on one
  instance; meaningless across two, which is where the requirement usually came from.

DKQ puts the exclusivity in the queue itself. A consumer **claims a key**, gets a lease and a
fencing token, and nothing else may work that key until the claim ends or the lease lapses. Restarts,
deployments and network partitions are all covered by the same mechanism, because the claim lives in the
store rather than in a process.

## Where it fits

**Treat DKQ the way you treat a database: one per service.**

It is not a message bus, and it is not for moving data between services — that is a broker's job, and a
contract between two services belongs somewhere both of them agree on. DKQ spreads *one* service's work
across *its own* instances, serialised per key. A queue's producers and its consumers are the same service,
and its keys mean something only inside it.

Sharing one DKQ between two services couples them the way a shared database does: they inherit each other's
key space, each other's semantics, and each other's outages.

The *Redis* underneath is a different matter: "we already have a Redis running" is a supported way to
deploy. DKQ keeps to its own key prefixes and never counts, scans, or flushes anything outside them.

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
claim stale — that is the half of the contract DKQ cannot enforce for you.

### The lock

The same store also serves a **distributed lock** — the queue's distilled core, for callers that want
per-key exclusivity without messages. Three unary RPCs, defined in
[`keyed_lock_service.proto`](modules/protocol/src/main/protobuf/homelab/keyedqueue/v1/keyed_lock_service.proto):

```proto
service KeyedLock {
  rpc Acquire (AcquireRequest) returns (AcquireResponse);  // take a named lock, waiting up to max_wait
  rpc Release (ReleaseRequest) returns (ReleaseResponse);  // free a lock this caller holds
  rpc Refresh (RefreshRequest) returns (RefreshResponse);  // extend a held lock's lease
}
```

An `Acquire` answers with a **receipt** (for releasing and refreshing) and a **fence** — a number strictly
larger on every later grant of the same lock. Stamp the writes the lock protects with it, and have the
thing being written reject stale fences: a lease alone cannot stop a stalled holder's write from landing,
and the fence is what makes that harmless. Grants are **fair** — waiters are served in the order the
service saw them ask, and a newcomer cannot barge past the queue. The full contract is
[`docs/architecture/lock-guarantees.md`](docs/architecture/lock-guarantees.md).

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

The binary has one operational mode besides serving: `sbt "run layout accept"` records the code's schema
version in the store — needed only when deploying a version whose stored structures changed shape, against
a drained store with no instances running, since an instance whose schema disagrees with the store's
refuses to start ([`docs/architecture/redis-cluster.md`](docs/architecture/redis-cluster.md)).

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
| `DKQ_SWEEP_INTERVAL` | `5 seconds` | how often each instance runs repair |
| `DKQ_SWEEP_LIMIT` | `100` | entries one sweep handles, per kind |
| `DKQ_LOCK_TRIM_INTERVAL` | `120 seconds` | how often each instance removes abandoned lock holds |
| `DKQ_LOCK_TRIM_GRACE` | `10 minutes` | how long past lease expiry a lock hold survives before trim removes it |
| `DKQ_LOCK_MAX_TTL` | `10 minutes` | the longest a single lock grant's lease may run; longer requests are clamped |

Every instance is identical and stateless — the queue's state is entirely in Redis — so scaling out is
running more of them against the same store. Redis Cluster is supported and tested: every key a queue uses
carries its **bucket's** hash tag, so a queue's keys and the stream announcing them live in one slot; the
bucket count is **fixed in code at 16** — nothing to configure and nothing to get permanently wrong — and
the wake listener reads **per slot group**, one connection each, which on a single server collapses to one.
The e2e suite runs against a real three-node cluster with
`DKQ_E2E_COMPOSE=docker-compose.e2e-cluster.yml sbt e2e`
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

## Status

**A POC, and honest about it.** The semantics are settled and tested — unit, integration against a real
Valkey, and an end-to-end suite that kills an instance mid-handler — but this is not a system anyone should
run critical work on yet. Expect breaking changes between versions; pin an exact one.

Known gaps:

- **Poison messages.** `attempt` is counted per message but nothing acts on it. A permanently failing
  message cycles rather than wedging its key, so this is not urgent — what is missing is somewhere to put
  it once the count is too high.
- **One queue per `Dequeue`.** A consumer spanning queues needs a connection each.
- **A hot key stays on one instance.** Every instance reads every wake entry, so instances race to claim,
  and the one that just settled a key is already claiming while the others are being told. Nothing is
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
