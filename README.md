# distributed-keyed-queue

A POC: a **keyed queue, distributed over gRPC**, where per-key serialisation is a property of the *storage
layer* rather than of consumer memory.

The requirement, and why mainstream brokers relocate the problem instead of solving it, is in
[`../research/infrastructure/homelab-message-broker.md`](../research/infrastructure/homelab-message-broker.md):
partition ownership, per-partition order, **per-key serial processing with long-lived handlers**, and
concurrency across keys.

Built on [`homelab-toolkit-zio`](../homelab-toolkit-zio) — `homelab-common` brings the messaging, flow and
store ports (`KeyedQueue`, `KeyLock`, `Distributer`, `PollConsumer`); `homelab-postgres` brings the leased
store behind them.

Docs follow the homelab-wide taxonomy — see [`docs/README.md`](docs/README.md).

## Build

```bash
sbt compile
sbt test    # unit + integration, against a Testcontainers Valkey
sbt e2e     # the deployed thing: builds the image, composes two instances, drives them over the wire
```

The toolkit resolves from **GitHub Packages**, which serves Maven only to authenticated callers. You need a
classic PAT with `read:packages` in `~/.sbt/1.0/credentials`:

```
realm=GitHub Package Registry
host=maven.pkg.github.com
user=<your-github-username>
password=<your-classic-pat>
```

Full recipe: [`../homelab-toolkit-zio/docs/learning-material/using-modules-as-a-dependency.md`](../homelab-toolkit-zio/docs/learning-material/using-modules-as-a-dependency.md).

**Consuming dkq from another service** is the other direction, and has its own guide:
[using the contract as a dependency](docs/learning-material/using-the-contract-as-a-dependency.md).

## Layout

```
src/main/protobuf/homelab/keyedqueue/v1/   proto — ScalaPB + zio-grpc generate into target/src_managed
src/main/scala/homelab/keyedqueue/         the code
src/test/scala/homelab/keyedqueue/         zio-test
```

Generator options (`flat_package`, package remaps) belong **in the `.proto`**, not in `build.sbt`: zio-grpc's
generator reads them from the file, so a build-side `flatPackage` desynchronises the two generators and the
ZIO stub ends up referring to a package ScalaPB no longer emits.

## What exists

Phase 1 of [`docs/research/roadmap.md`](docs/research/roadmap.md): four unary gRPC calls over Redis, with
per-key exclusivity, leases and repair. No streaming, no batching — those are phase 2.

```
src/main/protobuf/homelab/keyedqueue/v1/queue.proto   the contract: Enqueue, Dequeue, Settle, Heartbeat
src/main/resources/lua/                               the five atomic transitions
src/main/scala/homelab/keyedqueue/
  domain/          types, the QueueStore port, one class per use case
  infrastructure/  the Redis adapter, the claimer pool, the repair loop, config
  application/grpc the service, and the composition root
```

Run it against the local Redis:

```bash
docker compose up -d
sbt run                    # settings: src/main/resources/config/queue.conf
```

Configuration is HOCON, and every key carries both a working default and an environment override in the
same two lines, so there is one place to read what a setting means:

```hocon
lease-ttl = 30 seconds
lease-ttl = ${?DKQ_LEASE_TTL}
```

`sbt test` starts its own Valkey container, so it needs Docker but not the compose stack. It runs on every
push ([`.github/workflows/tests.yml`](.github/workflows/tests.yml)); the end-to-end suite is only compiled
there, not run.

`sbt e2e` is the other kind of test: it builds a Docker image, brings up two instances over one Valkey
(`docker-compose.e2e.yml`) and asserts what only a deployment can be wrong about — that both instances are
one queue, that a blocked `Dequeue` on one is woken by an `Enqueue` on the other, and that a SIGKILLed
instance loses no work. Point it at an existing deployment with `DKQ_E2E_ENDPOINTS=host:port,host:port`.
Details: [`docs/architecture/end-to-end-testing.md`](docs/architecture/end-to-end-testing.md).

## Known gaps

- **Poison messages.** `attempt` is delivered and counted, but nothing acts on it. Per-key ordering makes
  this urgent: a message that always fails blocks every later message for its key, for ever. A
  `max_attempts` plus a dead-letter or park policy is the next thing this needs — see
  [`docs/research/phase-1-api.md`](docs/research/phase-1-api.md).
- **One queue per `Dequeue`.** `BLMOVE` takes a single source; a consumer spanning queues needs a connection
  each.
- **Claimers bound concurrency.** `DKQ_CLAIMERS` connections may block at once; further `Dequeue` calls wait
  for one to free rather than opening more.

## Licence

Apache-2.0 — see [`LICENSE`](LICENSE).
