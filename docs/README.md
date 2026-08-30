# distributed-keyed-queue docs

How docs are organised — folders, the promotion rule, the required frontmatter — is defined once for the
whole homelab in [`../../DOCS.md`](../../DOCS.md). It is the authority; nothing here restates it.

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
  [`redis-state-walkthrough.md`](learning-material/redis-state-walkthrough.md) traces every request through
  the structures it touches — the page to read when something is stuck and you are looking at a live
  instance.
- **The problem statement lives outside this repo**, in `research/infrastructure/homelab-message-broker.md`
  and its transport companion: they precede this code and are not only about it. Rationale that *is* only
  about this repo goes in [`research/`](research/).
- **`architecture/` is one page per concern**, named after the package it describes — the convention the
  toolkit repos use, so a reader goes from a package to its page without a lookup.

Empty folders carry a `.gitkeep`; delete it when the folder gets its first real page.
