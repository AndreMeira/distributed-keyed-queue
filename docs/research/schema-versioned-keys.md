---
title: "Schema-versioned keys — the principle that makes migration definable"
type: research
status: draft
updated: 2026-09-11
tags: [schema, migration, layout, keys, hash-tag, versioning]
---

# Schema-versioned keys

Every key dkq writes carries the schema version between the hash tag and the rest of the name —
`{w:3}:v1:q:jobs:ready`, `{dkq:locks}:v1:held`. This note records why, and sketches the migration model it
enables but does not yet build.

## The problem it solves

The boot fence (`KeyLayout`) refuses to run new-schema code against old-schema data, and `layout accept`
records the new version — but nothing could ever *verify* that old state was gone, or find it again later.
Two attempts failed for the same root cause:

- counting every key (`DBSIZE`) assumed the store was dkq's alone, and "we already have a Redis running" is
  a supported way to deploy;
- scanning dkq's own prefixes assumed the *current* code knows what old keys look like — but the moment
  that matters is a schema change, when the leftovers are shaped like the **old** schema the new code no
  longer knows. It would have found nothing and vouched for a drain that did not happen.

The missing principle: **the convention must outlive the shapes.** With the version in every key, "what
does schema N own" is answerable by pattern — `{w:*}:vN:*` — by any later version, knowing nothing about
N's structures. Ownership across versions becomes a fact of the store instead of a memory of the code.

## Placement is load-bearing

The version sits **after** the hash tag, never inside it. A key's incarnations under different schemas —
`{w:3}:v1:…` and `{w:3}:v2:…` — therefore share a slot, so one Lua script can read v1 and write v2
**atomically**. Version inside the tag would scatter old and new across cluster nodes and turn every
migration step into a two-phase crash window.

## The migration model this enables (not built)

A worker, run deliberately by an operator who wants to carry state across a schema change rather than
drain: it pops state from vN keys and writes it into vN+1 keys, key by key. The convolutions, named now so
the builder does not rediscover them:

- **Per-key order.** A queue key with messages in both v1 and v2 lists violates per-key ordering. A key's
  whole list must move atomically before v2 accepts work for it — possible in one script precisely because
  of the placement above.
- **Fence monotonicity.** Fence counters migrate with their values plus a safety margin, or the new
  version mints numbers below ones already seen downstream — the exact failure fencing exists to prevent.
- **Client-held receipts.** A v1 receipt names v1 state. Either tokens survive the move byte-for-byte, or
  the migration waits out live holds and claims.
- **The ongoing cost.** The vN+1 binary keeps vN-*reading* code — an N−1 compatibility window. That is a
  maintenance promise, not a one-off, and the strongest reason the worker stays unbuilt until a schema
  change is actually planned.

## What the fence means now

With versioned keys, new code would otherwise run happily beside old state it silently ignores — queued
messages nobody will ever deliver again. The boot fence turns that silent abandonment into a loud
decision, and `accept` into a statement: *I know vN state exists; proceed with vN+1* — knowing the old
state remains findable forever, migratable later, instead of stranded and unfindable. The marker itself
(`dkq:layout:schema`) is deliberately version-less: it is the one key every version must agree on how to
read.
