---
title: "Writing end-to-end tests that mean something"
type: learning-material
status: current
updated: 2026-08-23
tags: [testing, e2e, zio-test, docker-compose, grpc, netty, debugging, distributed]
---

# Writing end-to-end tests that mean something

Building dkq's end-to-end suite took one afternoon, of which maybe a fifth was writing tests and the rest was
finding out why they failed. Four of those failures were real, and only one of the four was in the code the
tests were pointed at. This note is what I would tell someone starting the same job.

Everything here is grounded in `e2e/` and `docker-compose.e2e.yml`; the suite itself is described in
[`../architecture/end-to-end-testing.md`](../architecture/end-to-end-testing.md).

## 1. The first question is what only a deployment can be wrong about

There is no point in an end-to-end suite that re-proves what a cheaper test already proves. dkq already had
`GrpcSpec`: a real gRPC server, a real Valkey in a container, a real client — all in one JVM. That covers the
API. So the end-to-end suite gets exactly the claims that **need more than one process to be false**:

- two instances are *one* queue, not two;
- a call parked on instance A is woken by a call to instance B;
- an instance that is SIGKILLed loses none of the work it was holding.

If a test in the expensive suite would also pass with one instance, it belongs in the cheap suite. This is
also the argument for **no load balancer** in the compose file: put a proxy in front and a client can no
longer tell which instance served it, so the interesting claims become untestable. Address the instances
directly and let the test choose.

## 2. Real clocks, real processes, real kills

Three rules that fell out of this suite:

- **`TestAspect.withLiveClock` on everything.** A virtual clock proves nothing about a lease that a *server*
  is enforcing on its own clock. The moment a test asserts "this was still valid after 12 seconds", those
  seconds have to be real.
- **SIGKILL, not stop.** `docker compose stop` gives the process time to close its connections and hand back
  what it holds — the *opposite* of the failure being staged. `docker compose kill` is the honest one: the
  lease is the only thing standing between a dead pod and lost work, and that is precisely the claim.
- **Killing is a test operation, not a fixture detail.** The harness (`Compose`) exposes `kill` and `revive`
  because they belong in the body of a test that reads "an instance dies mid-handler", not in some setup
  block far away from the assertion they serve.

## 3. Measure windows so a failure cannot be a false positive

The exclusivity test asserts that one key is never held by two consumers at once. The consumers are clients,
so what they can observe is client-side timestamps — and the naïve choice is wrong.

Take the window as `[after the claim returned, after the settle returned]` and you have a window that
*extends past* the moment the server stopped considering the key held. Consumer 2 can legitimately claim the
key the instant consumer 1's settle applied, which is before consumer 1's settle *response* gets home. The
windows overlap; the test fails; nothing is broken.

Take it as `[after the claim returned, before the settle was sent]` and the window is strictly **inside** the
interval the server considers the key held:

```
server:  |------------- key held by consumer 1 -------------|
client:        |---- measured window ----|
                ^ claim response home     ^ settle request leaves
```

An overlap between two such windows is a real overlap. The general rule: **when a test measures an interval
across a network, shrink the measurement until it is provably contained in the thing you are asserting
about.** A test that can produce a false positive under load is worse than no test, because it trains you to
ignore it.

## 4. Record before you release, not after

The same suite asserts per-key FIFO order across four competing consumers. Consumers append what they handled
to a shared `Ref`, and the append has to happen **before the settle is sent**:

```scala
_ <- into.update(_ :+ handled)      // first
_ <- instance.settle(receipt)       // then this
```

Because a key becomes claimable again only once its settle has applied, appending first puts this handling in
the log strictly before any other consumer can claim the same key. Do it the other way round and two
consumers race to append, and an order assertion fails for reasons that have nothing to do with the queue.

This is the same idea as §3 wearing different clothes: **make the observation order derive from the
guarantee under test**, rather than hoping the two agree.

## 5. Isolate per run, because the suite outlives the run

The first "real" assertion failure was `20 was not equal to 12`, with deliveries carrying `attempt = 2`. The
cause: the previous run's messages were still in the queue. The stack is not always wiped between runs — it
survives `DKQ_E2E_KEEP=1`, and it is *somebody else's* when the suite is pointed at a real deployment.

Wiping the store is the tempting fix and the wrong one: it makes the suite unusable against anything shared.
Instead, every queue name goes through one place and carries a token unique to the run:

```scala
def queue(name: String): String = s"$name-$run"
```

Runs become disjoint without deleting anything. Worth generalising: **an end-to-end suite should be safe to
run against production-shaped infrastructure**, which means it may create state but must never assume it is
alone or destroy what it did not make.

## 6. `System.env` in ZIO Test does not read the environment

This one cost an hour and is pure trap. Under `ZIOSpecDefault`, `System.env` is served by **`TestSystem`** —
an empty map — so a variable that is set in the shell reads as absent. Meanwhile `TestAspect.ifEnvNotSet`
consults the *live* environment. The same variable was therefore simultaneously set and unset depending on
who asked, which produced a genuinely confusing symptom: the suite skipped the test that env var is supposed
to skip (proving it was set), while also composing and then tearing down the deployment it had been told not
to touch (proving it was not).

```scala
// Reads TestSystem: empty under ZIOSpecDefault.
System.env("DKQ_E2E_ENDPOINTS")

// Reads the process environment.
ZIO.withSystem(System.SystemLive)(System.env("DKQ_E2E_ENDPOINTS"))
```

The same applies to `Clock`, `Random` and `Console`: in ZIO Test they are all test doubles by default. If a
value must come from the real world — an env var, a wall-clock instant, a random suffix that must differ
between runs — say so explicitly with `ZIO.withX(XLive)`. `TestAspect.withLiveClock` covers the clock for a
whole spec; there is no such blanket for the others.

## 7. When a distributed test fails, suspect the harness first — but prove it

The failures pointed at three suspects in turn, and the discipline that mattered was **refusing to fix
anything before an experiment separated them**:

| Suspect | The experiment | Verdict |
|---|---|---|
| "The harness kills its own containers" | `docker events` for the whole daemon during a run | guilty, but of something unrelated (§6) |
| "Docker Desktop's port forwarding resets connections" | run two instances **on the host**, same Valkey, no Docker in the path | innocent — identical failures |
| "zio-grpc's client corrupts the stream" | drive the same server with the **plain Java blocking stub on plain threads** | innocent — it corrupted too |

Each experiment removes one layer. The third is the one that cracked it: once the plain Java stub reproduced
the corruption, everything this repository writes on the client side was out of scope, and the only thing
left was the server and what it is built from.

That last technique generalises. **When you suspect a library, replace it with the most boring possible
alternative and see if the symptom survives.** If it does, the library was never the problem.

## 8. Reading an HTTP/2 failure

The symptom set looked like three different bugs:

```
INTERNAL: Encountered end-of-stream mid-frame                             (client, on dequeue)
INTERNAL: Connection closed after GOAWAY … COMPRESSION_ERROR,
          debug data: Incomplete header block fragment                    (client, on enqueue)
NettyServerStream deframeFailed: Encountered end-of-stream mid-frame      (server, reading a request)
```

They are one bug. **HTTP/2 is a framed protocol with stateful header compression, so a single malformed write
desynchronises everything after it — in both directions.** The server blames a truncated request; the client
blames a corrupt response; both are downstream of the same corrupted stream, and neither error names the
culprit. Take the *earliest* failure on the connection, not the loudest, and never assume the side reporting
the error is the side that caused it.

Two more reading notes, both learned the hard way:

- `Incomplete header block fragment` means a HEADERS block was interrupted by something that was not its
  CONTINUATION. It is a **framing** error, so it points at whoever wrote the frames, not at application code.
- `HTTP/2 client preface string missing or corrupt. Hex dump: 0a` in a *server's* log is not an attack, it
  is something writing a newline into a gRPC port. In our case it was the compose healthcheck; see §10.

## 9. The dependency graph is part of the deployment, and it is testable

The bug was this, and it had been in `build.sbt` since the first commit:

```
netty-buffer / -common / -handler / -transport   4.1.118.Final   ← lettuce
netty-codec-http2 / -http / -socks               4.1.100.Final   ← grpc-netty 1.64
```

`grpc-netty` reaches into Netty's HTTP/2 internals, so an HTTP/2 codec running against another release's
internals corrupts header blocks — but **only once several requests are in flight**. Which is why every test
passed in isolation, why `GrpcSpec` had never seen it, and why it took a suite that puts concurrent traffic
on one connection to surface it.

Three transferable lessons:

- **Align to what the consumer was built against, not to the newest.** The instinct is to force everything up
  to the highest version on the graph. Here that is exactly wrong: grpc-netty is happy with the Netty it
  ships with and nothing else, while lettuce uses stable APIs and is happy anywhere in 4.1.x. Pin *down*.
- **Pin at `ThisBuild`, not per project.** Overrides in one project's settings do not reach a second project,
  which will resolve its own versions and reintroduce the split — which is exactly what the new `e2e`
  project did.
- **Keep the minimal reproduction as a test.** `WireSpec` — 200 calls on one channel, once through the Java
  stub, once through the ZIO one — takes two seconds and fails loudly if anyone bumps one library without the
  other. A comment in `build.sbt` explains *why*; the test is what enforces it.

The broader point: a version skew is invisible to the compiler and to every single-threaded test, and it
lives in the deployment rather than the code. That is precisely the category an end-to-end suite exists to
catch, and it is why this one paid for itself on its first run.

## 10. Small things that cost real time

- **Health-checking a gRPC port.** `echo > /dev/tcp/host/9000` proves the port is bound and also writes a
  newline that gRPC rejects as a corrupt HTTP/2 preface — a stack trace every two seconds, drowning the log
  you will need later. `</dev/tcp/host/9000` opens the socket and writes nothing. The general rule: a
  health check should not *speak*, or it must speak the protocol properly.
- **`--wait` only waits for the healthcheck.** A bound port is not an answering service, so the suite still
  retries a cheap real call (a heartbeat naming nothing) before the first test.
- **ZIO's `Chunk#zip` flattens tuples.** `Chunk[(A, B)].zip(Chunk[(A, B)])` gives you `Chunk[(A, B, A, B)]`
  via `Zippable`, not a chunk of pairs. Use `zipWith` when the pairing is the point.
- **`-Wnonunit-statement` and lambdas that return builders.** `ProcessLogger(line => builder.append(line))`
  fails the build; bind it to `val _` to say the discard is deliberate.
- **A fresh subproject sees a stale classpath.** The first `e2e/Test/compile` failed with "ZioKeyedQueue is
  not a member" for generated code that was on disk. Re-run; it is the incremental compiler catching up, not
  a real error. Similarly `sbt "clean; test"` in one invocation can compile tests against a just-deleted main
  output — run them as separate invocations.
- **Measure throughput to the last message handled, not to the last consumer finishing.** Every consumer ends
  by waiting out its patience on an empty queue, so including that tail measured `patience` and reported
  22 msg/s for something that actually ran at 449 msg/s. The number a test prints has to be a number about
  the system, or it should not be printed.

## What I would do differently next time

Write §5 and §6 first. Both are properties of the *harness*, both cost more debugging time than the real bug
did, and both have the same shape: **the suite must be honest about what it owns.** It does not own the
clock, it does not own the environment, it does not own the deployment's state, and every place it quietly
assumes otherwise turns into a failure that looks like a bug in the system under test.
