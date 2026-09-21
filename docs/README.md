# distributed-keyed-queue docs

How docs are organised follows one taxonomy: `architecture/` for how the system works today,
`learning-material/` for how a technology works, `research/` for why a design is the way it is — the
explorations behind it, whether or not they were built — and `sessions/` for dated notes on a piece of
work. Every page carries frontmatter — `title`, `type`, `status`, `updated`, `tags`.

What is specific to this repo:

- **This is a POC.** Expect `sessions/` and `research/` to carry most of the weight for now, and
  `architecture/` to stay thin until something is settled enough to describe as current state. A page that
  claims "this is how it works" while the design is still moving is worse than no page. So far:
  - [`architecture/guarantees.md`](architecture/guarantees.md) — what the service promises, in terms a
    caller can design against and a test can assert. The page to read first.
  - [`architecture/lock-guarantees.md`](architecture/lock-guarantees.md) — the same, for the lock API:
    exclusion, the fence, fairness, and what a holder owes.
  - [`research/client-library.md`](research/client-library.md) — whether to publish a client above the
    stubs: the queue as a `Consumer`, the lock as a scope, and what each would have to decide.
  - [`research/schema-versioned-keys.md`](research/schema-versioned-keys.md) — why every key carries its
    schema version, and the migration model that stands on it.
  - [`architecture/lock-mechanics.md`](architecture/lock-mechanics.md) — how the lock works: the three
    states, the script that performs each move, how a waiter waits, and which class does what.
  - [`architecture/redis-data-structures.md`](architecture/redis-data-structures.md) — what is kept in
    Redis, and what each structure is for.
  - [`architecture/redis-cluster.md`](architecture/redis-cluster.md) — the key layout was built for cluster
    mode from the start; the page is as much about what is *not* proven as what is.
  - [`architecture/redis-connections.md`](architecture/redis-connections.md) — two connections, a
    synchronous client, and the measurements that say that is the right call.
  - [`architecture/observability.md`](architecture/observability.md) — what the service reports about
    itself, and why `dequeue`'s latency is not comparable to the rest.
  - [`architecture/end-to-end-testing.md`](architecture/end-to-end-testing.md) — the test harness, settled
    even while the thing it tests is not.
  - [`architecture/readiness-and-wake.md`](architecture/readiness-and-wake.md) — how a consumer waits:
    what a readiness holds, why the queue's and the lock's differ, and every path that could lose a wake.
  - [`architecture/states-as-classes.md`](architecture/states-as-classes.md) — the shape the lock's wait is
    written in, named: defunctionalised recursion, absorbing terminals, trampolined. Wants abstracting.

  And in `learning-material/`:
  - [`claiming-identity.md`](learning-material/claiming-identity.md) — *superseded*: why a claim taken in
    two steps needed an identity, and what removing the second step removed with it.
  - [`lock-state-walkthrough.md`](learning-material/lock-state-walkthrough.md) — every lock call traced
    through the keys it touches, from a free lock to a trimmed one.
  - [`redis-state-walkthrough.md`](learning-material/redis-state-walkthrough.md) — every request traced
    through the structures it touches; the page for when something is stuck and you are looking at a live
    instance.
  - [`running-the-image.md`](learning-material/running-the-image.md) — pulling the image and deploying it;
    the grace period that must exceed `max-wait` is the part that bites.
  - [`interruption-and-lost-wakes.md`](learning-material/interruption-and-lost-wakes.md) — why a value
    returned to a dying fiber vanishes without any finalizer seeing it, and what a handover has to do
    instead.
  - [`using-the-contract-as-a-dependency.md`](learning-material/using-the-contract-as-a-dependency.md) —
    what another service depends on to talk to dkq, and what it still has to write itself.
  - [`taking-a-lock.md`](learning-material/taking-a-lock.md) — holding a named lock while something runs,
    what the managed form does around it, and when a caller needs the fence instead.
  - [`consuming-and-producing.md`](learning-material/consuming-and-producing.md) — sending typed messages
    and working them, what happens around a handler, and what becomes of one that will not decode.
  - [`signal-synchronisation.md`](learning-material/signal-synchronisation.md) — the other use of the
    queue: telling a consumer a key is worth looking at, without sending it anything.
  - [`proto-generation.md`](learning-material/proto-generation.md) — how two published artifacts are
    generated from one set of `.proto` files, and what breaks if that is rearranged.
  - [`querying-observability-tools.md`](learning-material/querying-observability-tools.md) — asking
    Prometheus and Jaeger a specific question from the terminal, and the traps in the answers.
  - [`java-agents-and-telemetry.md`](learning-material/java-agents-and-telemetry.md) — what a Java agent
    does to a build, why it ships only in the image, and why present is not the same as active.
  - [`writing-end-to-end-tests.md`](learning-material/writing-end-to-end-tests.md) — what the e2e suite
    taught about measuring and asserting against a real deployment.
  - [`reading-a-latency-tail.md`](learning-material/reading-a-latency-tail.md) — how to tell a GC pause
    from queueing in trace data, why generational ZGC lost to G1 here, and which lever actually moves it.
  And the most recent checkpoint is
  [`sessions/2026-09-22-signals-in-the-client.md`](sessions/2026-09-22-signals-in-the-client.md) — the
  queue's other use, what the client needed for it (almost nothing), and a flaky `HeartbeatSpec` to keep
  an eye on. Before it,
  [`sessions/2026-09-20-the-client-makes-it-a-product.md`](sessions/2026-09-20-the-client-makes-it-a-product.md)
  — the client that ships with 0.0.5, the line it draws between the RPCs and what it takes on, and what
  the wire owed it. Before it,
  [`sessions/2026-09-13-redis-package-cleanup.md`](sessions/2026-09-13-redis-package-cleanup.md) — the
  `redis` package back in shape, and why the readinesses belong in the domain next.

- **The problem statement lives outside this repo**, in `research/infrastructure/homelab-message-broker.md`
  and its transport companion: they precede this code and are not only about it. Rationale that *is* only
  about this repo goes in [`research/`](research/).
- **`architecture/` is one page per concern**, named after the package it describes — the convention the
  toolkit repos use, so a reader goes from a package to its page without a lookup.

Empty folders carry a `.gitkeep`; delete it when the folder gets its first real page.
