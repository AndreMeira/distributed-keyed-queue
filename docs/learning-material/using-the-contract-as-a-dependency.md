---
title: "Talking to dkq from another service"
type: learning-material
status: current
updated: 2026-09-20
tags: [sbt, dependencies, github-packages, grpc, client, lock, pat]
---

# Talking to dkq from another service

> **Early, and versioned accordingly.** The contract is settled enough to publish and not settled enough to
> promise stability: expect breaking changes between minor versions while the service is a POC, and pin an
> exact version rather than a range.

## Which version

Releases are cut as GitHub Releases tagged `vX.Y.Z`; the artifact version is the tag without the `v`. The
current one is whatever the [Releases page](https://github.com/AndreMeira/distributed-keyed-queue/releases)
shows at the top — this page does not name a version, so that it cannot go stale.

All three artifacts are released together and share a version. **Pin them to the same one**: mixing a
`protocol` with a `protocol-zio-grpc` from a different release is neither tested nor supported, and the
client is built against the contract of its own release.

## What you can depend on

Three artifacts, and which you want depends on how much you need:

| artifact | what it is | what it drags in |
|---|---|---|
| `distributed-keyed-queue-protocol` | the message types | `scalapb-runtime`, and nothing else |
| `distributed-keyed-queue-protocol-zio-grpc` | the RPC stubs, ZIO-native | the above, plus `zio-grpc-core` and `zio` |
| `distributed-keyed-queue-client` | the lock as Scala types | the above, plus `grpc-netty` |

**Take the smaller one if you only handle the data** — building a message, reading one out of somewhere
else, writing a test fixture. It has no effect system and no transport, deliberately. Only a service
actually calling dkq needs the stubs.

**Take the client if you want the lock** rather than the four RPCs it is made of. It is two layers, and the
upper one is what most callers want:

```scala
ZIO.scoped:
  for
    client <- LockClient.scoped(LockClient.Config("dkq", 9000))
    answer <- DistributedLock(client).acquire("the-key", ttl = 30.seconds, maxWait = 5.seconds):
                doTheWorkThatNeedsExclusivity
  yield answer // None when the wait elapsed with somebody else holding it
```

`acquire` keeps the lease alive while your effect runs and gives the lock back however the effect ends — an
answer, a failure, an interruption. `LockClient` underneath it is the four RPCs one for one, and hands you
the fencing token, which is what you need if you are stamping downstream writes.

The two contract artifacts carry no transport. That is not an omission: whether you dial over netty,
in-process, or something else is yours to choose. The client does carry `grpc-netty`, because
`LockClient.scoped(config)` dials for you and a channel needs a provider at runtime. To use a different
transport — or TLS, or interceptors, or an in-process channel for tests — build the channel yourself and
hand it to `LockClient.scoped(channel)`, excluding `grpc-netty` from the dependency.

## Getting them

They resolve from **GitHub Packages**, which serves Maven only to authenticated callers. There is no
anonymous fetch at any visibility — but *authorisation* is a separate gate from authentication, and these
packages are public, so any authenticated identity may read them. Which is to say: what a consumer is
missing is a way to authenticate, not a permission.

**In CI that is free.** Every Actions run has a built-in `GITHUB_TOKEN`; pass it as the sbt credential
(`GITHUB_ACTOR` / `GITHUB_TOKEN`) with `permissions: packages: read` and nothing has to be created. dkq's
own tests resolved the toolkit that way for a week before any secret existed.

**On a laptop it is not**, because there is no built-in token, and it has to be a **classic** PAT — the
Maven registry predates fine-grained tokens and rejects them. One token with `read:packages` covers every
package on the account, so if you already consume `homelab-toolkit-zio` you have what you need and can skip
the rest of this section.

```
# ~/.sbt/1.0/credentials — never in a repo
realm=GitHub Package Registry
host=maven.pkg.github.com
user=<your-github-username>
password=<your-classic-pat>
```

The realm string is fixed by GitHub. Get it wrong and sbt silently skips the credentials, which surfaces as
a 401 that reads like a bad token.

Full recipe, including the CI half: the toolkit's
[`using-modules-as-a-dependency.md`][toolkit-deps].

[toolkit-deps]: https://github.com/AndreMeira/homelab-toolkit-zio/blob/main/docs/learning-material/using-modules-as-a-dependency.md

```scala
resolvers += "distributed-keyed-queue" at
  "https://maven.pkg.github.com/AndreMeira/distributed-keyed-queue"

libraryDependencies ++= Seq(
  "com.andremeira.homelab" %% "distributed-keyed-queue-protocol-zio-grpc" % dkqVersion,
  "io.grpc"                 % "grpc-netty"                                % grpcVersion,
)
```

For the lock, one line replaces both — the client brings the stubs and a transport with it:

```scala
libraryDependencies += "com.andremeira.homelab" %% "distributed-keyed-queue-client" % dkqVersion
```

> **Pin Netty to what your `grpc-netty` was built against.** `grpc-netty` reaches into Netty's HTTP/2
> internals, and mixing versions produces corrupt HPACK header blocks once several requests are in flight —
> the client reports `Incomplete header block fragment`, the server reports a truncated request, and neither
> is the culprit. It never appears in a single-threaded test. If anything else in your build pulls a newer
> Netty (Lettuce does), override it down. `build.sbt` here does exactly that, with the reasoning.

## What the contract does not give you

**A queue client.** The lock has one — see the table above — but the queue does not: for `Dequeue`,
`Settle` and `Heartbeat` the generated stub is a stub, and everything about *using* the queue correctly is
yours to write. Two parts of it are easy to get wrong:

- **Heartbeating while you work.** A claim expires unless renewed, and a handler that outlives its lease has
  its claim revoked underneath it — its settles refused, its work possibly redone by someone else.
- **Settling on every path.** Success, failure, and interruption. A claim that is never settled holds its
  key until the lease lapses.

Read [`../architecture/guarantees.md`](../architecture/guarantees.md) before writing that loop. Its
Exclusivity section in particular says what the service will and will not do for you — including the one
obligation that is yours alone: **stop working a key the moment you can no longer renew it**, because
nothing on the server side can stop your code from running.

**Anything for a non-Scala consumer.** The `.proto` files under `modules/protocol/src/main/protobuf/` are
the contract, and any language's protobuf toolchain can generate from them. Nothing else is published — no
TypeScript package, no descriptor set. Generating from the checked-in protos is the supported route.
