---
title: "Running dkq: pulling the image, and a Kubernetes deployment"
type: learning-material
status: current
updated: 2026-09-18
tags: [docker, ghcr, kubernetes, deployment, operations, configuration]
---

# Running dkq

This page is about running the service. Calling it from another service is
[`using-the-contract-as-a-dependency.md`](using-the-contract-as-a-dependency.md) — the two are separate
jobs, and a consumer needs only that one.

## The image

`ghcr.io/andremeira/distributed-keyed-queue`, pushed by `build-deploy.yml` on every push to `main`, tagged
with the commit SHA and `latest`.

```bash
docker pull ghcr.io/andremeira/distributed-keyed-queue:latest
```

That is the whole story: the package is public, so no credential. Worth contrasting with the Maven
artifacts, which GitHub Packages serves only to authenticated callers whatever their visibility — a
consumer of the contract still needs a PAT, a consumer of the image does not.

**Pin the SHA for anything real.** `latest` moves on every merge, which is what a laptop wants and what a
deployment does not.

There is no hand-written Dockerfile: sbt-native-packager generates it, so `sbt server/Docker/publishLocal`
produces the same image locally, under the same name. That is deliberate — the image a developer tests is
the image that ships.

## Configuring it

Every setting has a default that works against a local Redis, and an environment override. The file that
says what each one means is `modules/server/src/main/resources/config/queue.conf`; the ones a deployment
usually sets:

| variable | default | why you would change it |
|---|---|---|
| `DKQ_REDIS_URL` | `redis://localhost:6379` | always, in a deployment |
| `DKQ_CLUSTER` | `false` | `true` when the URL names a Redis Cluster; Lettuce cannot tell from the URL |
| `DKQ_PORT` | `9000` | rarely — the image exposes 9000 |
| `DKQ_LEASE_TTL` | `30 seconds` | must exceed the slowest handler that does not heartbeat |
| `DKQ_MAX_WAIT` | `30 seconds` | the longest dequeue/acquire wait honoured, and the connection timeout |
| `DKQ_MAX_BATCH_LIMIT` | `32` | how much one claim may withhold from everyone else |

Telemetry is **off** in the image (`OTEL_JAVAAGENT_ENABLED=false`) and turning it on is
[`java-agents-and-telemetry.md`](java-agents-and-telemetry.md).

## A Kubernetes deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dkq
spec:
  replicas: 2
  selector:
    matchLabels: { app: dkq }
  template:
    metadata:
      labels: { app: dkq }
    spec:
      # Must exceed DKQ_MAX_WAIT — see "The two numbers that must agree" below.
      terminationGracePeriodSeconds: 45
      containers:
        - name: dkq
          image: ghcr.io/andremeira/distributed-keyed-queue:<sha>
          ports:
            - containerPort: 9000
          env:
            - name: DKQ_REDIS_URL
              value: redis://valkey:6379
          # The image carries a JRE and no gRPC tooling, so a probe can prove the port is bound and not
          # much more. The same constraint the compose healthcheck works around.
          readinessProbe:
            tcpSocket: { port: 9000 }
            periodSeconds: 5
          livenessProbe:
            tcpSocket: { port: 9000 }
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: dkq
spec:
  selector: { app: dkq }
  ports:
    - port: 9000
      targetPort: 9000
```

**More than one replica is fine, and is the point.** Every instance runs the repair sweeps, which are
idempotent — so there is no leader election, and no single instance whose death stops repair. Instances
share nothing but Redis.

**Redis is required and is not in here.** dkq stores everything in it; a `Deployment` with no persistence
is a queue that forgets. Cluster mode works — every key carries its queue's `{q:<queue>}` hash tag — but
`DKQ_CLUSTER=true` has to say so.

## Letting the cluster pull

**Nothing, as things stand.** The image is public, so the manifest above needs no `imagePullSecrets` and
the kubelet pulls it anonymously.

It came out that way on its own: a workflow in a public repository published it with `GITHUB_TOKEN`, so
GHCR linked the package to the repository and it inherited that visibility. Not a rule to lean on blindly
for other registries — GitHub's own documentation describes packages as private by default, and that is
true of ones published without a repository link.

Visibility is a package setting, so someone can change it. To see which you have:

```bash
docker logout ghcr.io
docker pull ghcr.io/andremeira/distributed-keyed-queue:latest
```

Refused means private, changeable under repo → **Packages** → the package → **Change visibility**. A private
image needs the cluster authenticated — the kubelet does the pulling, so a `docker login` on your
workstation does nothing for it, and the pods sit in `ImagePullBackOff`. That needs `imagePullSecrets:
[{name: ghcr}]` on the pod spec, and the secret itself:

```bash
kubectl create secret docker-registry ghcr \
  --docker-server=ghcr.io \
  --docker-username=<your-github-user> \
  --docker-password="$GITHUB_TOKEN"     # classic PAT with read:packages
```

The secret is per namespace, so it has to exist in whichever namespace the deployment lands in.

## The two numbers that must agree

`terminationGracePeriodSeconds` **must exceed `DKQ_MAX_WAIT`.**

A dequeue or an acquire parks for up to `max-wait` before answering. The service's own
`gracefulShutdownTimeout` is `Duration.Infinity` on purpose: the real guard is meant to be the orchestrator,
so Kubernetes decides how long a draining pod gets. Kubernetes' default grace period is 30 seconds and
`max-wait` defaults to 30 seconds — which means that with both defaults, a pod carrying an in-flight
long-poll can be `SIGKILL`ed at the exact moment it would have answered.

Set the grace period above `max-wait` with room to spare. If you raise `DKQ_MAX_WAIT`, raise this with it.

## Checking it came up

The image has no gRPC client in it, so from outside:

```bash
kubectl port-forward svc/dkq 9000:9000
grpcurl -plaintext localhost:9000 list          # needs reflection; otherwise use -proto
```

The honest readiness check is a real call — the end-to-end suite treats its first request that way rather
than trusting a healthcheck, for the same reason.
