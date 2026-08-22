# distributed-keyed-queue docs

How docs are organised — folders, the promotion rule, the required frontmatter — is defined once for the
whole homelab in [`../../DOCS.md`](../../DOCS.md). It is the authority; nothing here restates it.

What is specific to this repo:

- **This is a POC.** Expect `sessions/` and `research/` to carry most of the weight for now, and
  `architecture/` to stay empty until something is settled enough to describe as current state. A page that
  claims "this is how it works" while the design is still moving is worse than no page.
- **The problem statement lives outside this repo**, in `research/infrastructure/homelab-message-broker.md`
  and its transport companion: they precede this code and are not only about it. Rationale that *is* only
  about this repo goes in [`research/`](research/).
- **`architecture/` is one page per concern**, named after the package it describes — the convention the
  toolkit repos use, so a reader goes from a package to its page without a lookup.

Empty folders carry a `.gitkeep`; delete it when the folder gets its first real page.
