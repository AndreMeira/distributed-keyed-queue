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
  - [`architecture/redis-data-structures.md`](architecture/redis-data-structures.md) — what is kept in
    Redis, and what each structure is for.
  - [`architecture/redis-cluster.md`](architecture/redis-cluster.md) — the key layout was built for cluster
    mode from the start; the page is as much about what is *not* proven as what is.
  - [`architecture/end-to-end-testing.md`](architecture/end-to-end-testing.md) — the test harness, settled
    even while the thing it tests is not.

  And in `learning-material/`:
  - [`claiming-identity.md`](learning-material/claiming-identity.md) — *superseded*: why a claim taken in
    two steps needed an identity, and what removing the second step removed with it.
  - [`redis-state-walkthrough.md`](learning-material/redis-state-walkthrough.md) — every request traced
    through the structures it touches; the page for when something is stuck and you are looking at a live
    instance.
  - [`interruption-and-lost-wakes.md`](learning-material/interruption-and-lost-wakes.md) — why a value
    returned to a dying fiber vanishes without any finalizer seeing it, and what a handover has to do
    instead.
  - [`using-the-contract-as-a-dependency.md`](learning-material/using-the-contract-as-a-dependency.md) —
    what another service depends on to talk to dkq, and what it still has to write itself.
  - [`proto-generation.md`](learning-material/proto-generation.md) — how two published artifacts are
    generated from one set of `.proto` files, and what breaks if that is rearranged.
  - [`writing-end-to-end-tests.md`](learning-material/writing-end-to-end-tests.md) — what the e2e suite
    taught about measuring and asserting against a real deployment.
- **The problem statement lives outside this repo**, in `research/infrastructure/homelab-message-broker.md`
  and its transport companion: they precede this code and are not only about it. Rationale that *is* only
  about this repo goes in [`research/`](research/).
- **`architecture/` is one page per concern**, named after the package it describes — the convention the
  toolkit repos use, so a reader goes from a package to its page without a lookup.

Empty folders carry a `.gitkeep`; delete it when the folder gets its first real page.
