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
sbt test
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

## Layout

```
src/main/protobuf/homelab/keyedqueue/v1/   proto — ScalaPB + zio-grpc generate into target/src_managed
src/main/scala/homelab/keyedqueue/         the code
src/test/scala/homelab/keyedqueue/         zio-test
```

Generator options (`flat_package`, package remaps) belong **in the `.proto`**, not in `build.sbt`: zio-grpc's
generator reads them from the file, so a build-side `flatPackage` desynchronises the two generators and the
ZIO stub ends up referring to a package ScalaPB no longer emits.

## Scaffolding to delete

Three files exist only to prove the build is whole — that the toolkit resolves, protoc runs, zio-grpc's
stubs compile under the strict flags, and zio-test executes. Delete them as the real thing appears:

- `src/main/protobuf/homelab/keyedqueue/v1/scaffold.proto`
- `src/test/scala/homelab/keyedqueue/ScaffoldSpec.scala`
- `src/main/scala/homelab/keyedqueue/Main.scala` (a placeholder `println`)

## Licence

Apache-2.0 — see [`LICENSE`](LICENSE).
