---
title: "When the store forgets DKQ's scripts — what a Valkey restart means for a running DKQ"
type: research
status: draft
updated: 2026-11-14
tags: [redis, valkey, lua, noscript, script-cache, restart, failover, schema-marker, operations, exploration, not-a-decision]
---

# When the store forgets DKQ's scripts

> **Not a decision.** This lays out the cases behind issue #74 (scripts are never re-registered after
> `NOSCRIPT`) so the choice can be made on how DKQ is run, not on what is quickest to code. Nothing here is
> built.

## The question

Every DKQ operation is a Lua script. DKQ registers each one at boot with `SCRIPT LOAD` (on every primary of a
cluster, `LuaScript.loadEverywhere`) and then runs it with `EVALSHA`. The store's script cache is not part of
its data. A restart, a `SCRIPT FLUSH` or a failover empties it while DKQ keeps running.

The intuition is that "if Valkey restarts, DKQ should restart". The question is whether that is a requirement
or a habit. Restarting DKQ is one way of making it re-learn what it knows about the store. What matters is
whether anything it knows has stopped being true.

## What happens today

1. Lettuce reconnects by itself (`ClientOptions.autoReconnect` is on by default, and `Connection.make` leaves it).
2. Every `EVALSHA` then fails with `NOSCRIPT`. `LuaScript.failure` maps any error to `RedisFailure.Unavailable`,
   which is transient, so callers see `UNAVAILABLE` and retry.
3. Nothing re-registers the scripts.
4. The liveness probe in [`running-the-image.md`](../learning-material/running-the-image.md) is `tcpSocket: 9000`.
   The port stays bound, so the orchestrator never restarts the process.

So DKQ **neither recovers nor restarts**. It stays up and refuses every call until someone restarts it by hand.
Whichever option below is chosen, the current behaviour is the one to leave.

## What DKQ holds that a store restart could invalidate

DKQ is stateless by design: claims, leases, fences, due times and backoffs live in the store and are measured on
the store's clock (`TIME`). Its process memory holds only these things about the store:

| Held in memory | Learned | If the store restarts |
|---|---|---|
| Script digests | at boot, from `SCRIPT LOAD` | Stale on the store's side, but recomputable locally: a digest is the SHA-1 of the source. |
| The schema marker check | at boot (`KeyLayout.verify`: `SET NX dkq:layout:schema`, then compare) | Still true if the data survived. If the data was wiped, the marker is gone and nothing re-records it. |
| Readiness tokens, `DueTimer` requests | continuously | Hints only. The store is the truth, and a stale hint costs a wasted look or a backstop wait. |

So a restart of DKQ is **never needed for correctness of the data path**. The one boot-time fact that a store
event can invalidate is the marker.

## The cases

| Event | Data | Script cache | What restarting DKQ buys |
|---|---|---|---|
| Valkey restart with persistence (RDB/AOF), the usual pod restart with a volume | survives | lost | Nothing. Every structure is valid, and outstanding claims continue under the same fences. |
| Valkey restart without persistence | lost | lost | Re-recording the schema marker. Everything else is already consistent with S4: queues are empty, scheduled messages are gone, and old receipts are answered `Stale` because their fences no longer match. |
| `SCRIPT FLUSH` by an operator | survives | lost | Nothing. |
| Failover to a replica (Sentinel, cluster, a promoted node) | survives, minus the writes not yet replicated | not to be relied on: the cache is not replicated as data (see below) | Nothing beyond the scripts. The lost tail of writes is replication's loss, which a restart does not repair. |
| A cluster node added or replaced later | survives | missing on the new node, since `loadEverywhere` ran once at boot | Nothing beyond the scripts. |
| A network blip, reconnecting to the same server | survives | survives | Nothing. This case already works. |

**The hazard in the no-persistence case.** If the store is wiped and DKQ keeps running, the marker stays
absent. A DKQ of a different schema version booting in that window finds no marker, records its own, and starts
writing its layout while the running instances keep writing the old one. That is the mixed-layout state the
marker exists to prevent. The window is narrow, since it needs a wipe and a version change together, but it is
the one real reason to re-run a boot check.

**On replication of scripts.** Valkey's documentation contrasts scripts with **functions**: functions are
persisted to the AOF and replicated from primary to replicas, while loaded scripts are only cached
([Valkey: Functions](https://valkey.io/topics/functions-intro/)). Third-party guides and Alibaba's Tair
documentation say cached scripts are not guaranteed to survive a switchover or to be present on other nodes.
I found no Valkey page that states outright that `SCRIPT LOAD` is never replicated, so the design should simply
not rely on it.

## Options

### A. Fail fast: a lost script cache stops the process

Treat `NOSCRIPT` as fatal. The process exits, the orchestrator restarts it, and boot re-registers the scripts
and re-checks the marker.

- **For:** it is the simplest model ("the store changed under me, start over"). It reuses the boot path with no
  new code path, and it re-records the marker after a wipe.
- **Against:**
  - Every instance hits `NOSCRIPT` at about the same moment, so all of them restart together. In-flight long
    polls are dropped, and a JVM boot's worth of downtime is added to Valkey's own.
  - It depends on a restart policy. Under bare `docker compose` without `restart:`, the service stays down.
  - For the most common case, a pod restart with its volume, it buys nothing.
- **Needs:** a way for "fatal" to reach the process. Either an explicit exit, or a health signal that a real
  liveness probe reads (today's probe is TCP only, see above).

### B. Transparent fallback: re-send as `EVAL`

On `NOSCRIPT`, re-send the call as `EVAL` with the source. That runs it and re-caches it on the node that
answered. Compute the digest locally, so no `SCRIPT LOAD` round trip is needed, and `loadEverywhere` becomes
unnecessary because each node caches the script the first time it is asked to run it. Keep a preload at boot
only so an invalid script still fails at startup.

- **For:** no downtime beyond Valkey's own. It is correct whenever the data survived, which is every case but
  one. It also covers nodes added or failed over later, and it is the standard client pattern (valkey-py's
  `Script` object does exactly this).
- **Against:** it leaves the marker hazard in the no-persistence case.
- **Cost:** a `NOSCRIPT` round trip once per script per node, after each cache loss.

### C. Fallback, then re-assert the marker

Do B, and when a `NOSCRIPT` is seen, also run the marker check again (`KeyLayout.verify`, the same `SET NX`
then compare that boot runs):

- **marker present and equal:** carry on. This is every case where the data survived.
- **marker absent:** the store was wiped. `SET NX` records this instance's layout again, which closes the
  window in which another version could claim it.
- **marker different:** another version recorded its layout first. Fail fast, as boot would.

This is "re-run the boot checks without restarting". It fails fast only when that is warranted.

- **For:**
  - The common cases cost nothing.
  - The one hazard is covered.
  - The restart that A forces happens only when the store really disagrees with this instance.
- **Against:**
  - There is one more code path, and it needs a trigger. `NOSCRIPT` is a good one, because a store whose
    scripts are gone has restarted or been flushed.
  - The check is per instance. Several instances re-asserting at once is harmless, though, since `SET NX` is
    atomic and they all record the same stamp.

### D. Valkey functions instead of scripts

Load the library once with `FUNCTION LOAD`, and call it with `FCALL`. Functions are persisted and replicated
([Valkey: Functions](https://valkey.io/topics/functions-intro/)), so a restart with persistence and a failover
both keep them.

- **For:** the store keeps the code with the data, so most of the cases above stop arising at all.
- **Against:**
  - It is a migration of every script.
  - It does not cover the no-persistence case: the functions are lost with the data, so the marker question is
    the same.
  - A library is replaced by name. During a rolling upgrade, two DKQ versions would overwrite each other's
    function bodies unless the library name carries the schema version.
  - On a cluster, functions are per node as well.
- **Fit:** more a direction for the toolkit's `homelab-redis` than a fix for #74.

## Where it lands with the toolkit

The toolkit's planned `homelab-redis` module (branch `redis-module`,
[`redis-lua-codecs.md`](../../../homelab-toolkit-zio/docs/research/redis-lua-codecs.md)) is meant to carry a
library `LuaScript`. Options B and C split cleanly along that line:

- the **`EVALSHA` → `EVAL` fallback** is generic, so it belongs in the library;
- **re-asserting the marker** is DKQ's own, as a hook DKQ runs when the library reports that a script was missing.

## Lean

**C.** The case that actually happens is a Valkey pod restarting with its volume, and there a DKQ restart buys
nothing but downtime. C keeps the one property a restart would have bought, the marker re-check, and fails fast
only when the store really disagrees.

If what appeals in A is that a Valkey restart becomes visible, a log line and a measured operation on the
fallback (`Monitor.measure` around the re-registration) give that without the downtime.

Whatever is chosen, the liveness probe should eventually check more than a bound port. An instance that is up
but unable to reach its store's scripts is today invisible to the orchestrator.

## To confirm before choosing

- **Reproduce today's behaviour.** On the Testcontainers Valkey: run a script, `SCRIPT FLUSH`, run it again.
  Expected: `Unavailable` with a `NOSCRIPT` message on every call.
- **Check Lettuce's error type.** Does it raise `RedisNoScriptException`, so the fallback can match on a type
  rather than on message text?
- **Check a container restart without persistence**, with and without a schema-version change in between, to
  see the marker hazard happen.
