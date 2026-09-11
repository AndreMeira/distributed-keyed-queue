---
title: "PostgreSQL as a DKQ substrate — the schema, the five operations, and where it hurts"
type: research
status: draft
updated: 2026-09-10
tags: [postgres, substrate, skip-locked, listen-notify, doobie, autovacuum, architecture]
---

# PostgreSQL as a DKQ substrate — the schema, the five operations, and where it hurts

`substrate-candidates.md` names Postgres the strongest alternative to Redis because it answers both
questions DKQ asks — the atomic claim (`FOR UPDATE SKIP LOCKED`) and the wake (`LISTEN/NOTIFY`) — natively.
This is the detail: the schema, each `QueueStore` operation as SQL, the wake wiring, and the two weaknesses
that actually decide whether it is viable.

Draft, and the SQL is illustrative — read for shape, not to paste. Nothing here has been run. A few vendor
specifics (the `NOTIFY` payload limit, pooler behaviour, `now()` semantics) are stated from knowledge and
flagged where they change a decision.

## The one idea that makes Postgres attractive: rows absorb structures

Redis spreads one key across many structures because it has no row. DKQ uses ten structure kinds
(`minimal-structures.md`) largely because a key's *ready-score, lease, backoff, fence* each need their own
zset or hash. A relational row holds all of them at once, so most of that count collapses into columns:

| Redis structure | Postgres |
|---|---|
| `ready` zset (key → seq) | `dkq_key.state='ready'` + `ready_seq` column |
| `sequence` counter | a `SEQUENCE` |
| `claimed` zset (key → deadline) | `dkq_key.claimed_until` column |
| `delayed` zset (key → retry-at) | `dkq_key.delayed_until` column |
| `fence` hash (key → generation) | `dkq_key.fence` column |
| `msgs` list (order) | `dkq_message.seq` + `ORDER BY` |
| `payloads` hash (id → bytes) | `dkq_message.payload` column |
| `owned` set (claimed ids) | `dkq_message.owned` boolean |
| `attempts` hash (id → count) | `dkq_message.attempts` column |
| `wake` stream | `LISTEN/NOTIFY` (not stored) |

Ten kinds become **two tables**. The `claimed`/`delayed` split that `minimal-structures.md` could not merge
in Redis without a semantics change is just two nullable timestamp columns here — the relational model does
not pay Redis's "extra structure" cost, so the merge question never arises.

```sql
create table dkq_key (
  queue         text        not null,
  msg_key       text        not null,
  state         smallint    not null,          -- 0 ready | 1 claimed | 2 delayed
  ready_seq     bigint,                         -- fairness order across keys; set when it becomes ready
  claimed_until timestamptz,                    -- the lease
  delayed_until timestamptz,                    -- the backoff
  fence         bigint      not null default 0, -- claim generation; the token
  primary key (queue, msg_key)
);

create table dkq_message (
  queue        text     not null,
  msg_key      text     not null,
  id           text     not null,
  seq          bigint   not null,               -- producer order within the key
  payload      bytea    not null,
  payload_type text     not null,
  encoding     smallint not null,
  sent_at      timestamptz,
  attempts     int      not null default 0,
  owned        boolean  not null default false, -- taken by the live claim, not yet settled
  primary key (queue, msg_key, id)
);

create sequence dkq_seq;

-- the ready set is a partial index, not a table: it holds only claimable keys, stays small
create index dkq_ready   on dkq_key     (queue, ready_seq)     where state = 0;
create index dkq_leases  on dkq_key     (queue, claimed_until) where state = 1;
create index dkq_backoff on dkq_key     (queue, delayed_until) where state = 2;
create index dkq_fifo    on dkq_message (queue, msg_key, seq);
```

The partial indexes are the quiet win: `dkq_ready` indexes *only* keys that are claimable, so "oldest ready
key" is an index scan over exactly the working set, and a claimed or backing-off key costs nothing in it.
That is the `ready` zset, for free, as a by-product of the state column.

## The five operations, each one transaction

Every DKQ operation is one Redis Lua script — atomic by being alone. On Postgres each is one **transaction**
— atomic by isolation. Read Committed plus explicit row locks is enough; `SERIALIZABLE` is not needed,
because the row lock serialises exactly the contended access and nothing else.

### claim — the operation the whole substrate is chosen for

```sql
-- one transaction
with taken as (
  select queue, msg_key
  from dkq_key
  where queue = $1 and state = 0
  order by ready_seq
  for update skip locked          -- the guarantee: no two claimers take the same key
  limit 1
)
update dkq_key k
   set state = 1, claimed_until = now() + $2::interval, fence = fence + 1
  from taken t
 where k.queue = t.queue and k.msg_key = t.msg_key
 returning k.msg_key, k.fence, k.claimed_until;
-- then, for that key:
update dkq_message m
   set owned = true, attempts = attempts + 1
 where (m.queue, m.msg_key, m.id) in (
   select queue, msg_key, id from dkq_message
   where queue = $1 and msg_key = $key and not owned
   order by seq limit $batch)
 returning id, payload, payload_type, encoding, attempts;
-- backlog = select count(*) from dkq_message where queue=$1 and msg_key=$key and not owned;  (after)
```

`FOR UPDATE SKIP LOCKED` is the single-threaded-Redis-equivalent, and in one respect it is **better**: Redis
serialises *every* operation globally (one thread, one keyspace), while the row lock serialises only the one
key. Concurrent claims on *different* keys proceed in parallel with no contention at all. On a workload with
many keys and many consumers — DKQ's whole point — Postgres's claim path can out-scale Redis's precisely
because its locking is per-key rather than global. That is counter to Redis's reputation and follows directly from the granularity.

`now()` is transaction-start time on the server, so leases do not depend on a caller's clock — the same
property `TIME` gives the Lua, for free.

### enqueue — append, dedup, make ready, wake

```sql
insert into dkq_message (queue, msg_key, id, seq, payload, payload_type, encoding, sent_at)
values ($1, $2, $3, nextval('dkq_seq'), $4, $5, $6, $7)
on conflict (queue, msg_key, id) do nothing
returning id;                        -- a row iff it was new (the HSETNX dedup)

-- only when the message was new: make the key ready if it is idle
insert into dkq_key (queue, msg_key, state, ready_seq, fence)
values ($1, $2, 0, nextval('dkq_seq'), 0)
on conflict (queue, msg_key) do nothing;   -- a live key keeps its state and its ready_seq

select pg_notify('dkq_wake', $1);    -- the wake; fires on commit
```

`on conflict do nothing` on the message *is* `HSETNX`: a producer's at-least-once retry of the same id
inserts nothing and is silently accepted. On the key row it *is* the "a key already in `ready` keeps its
score" rule — an existing row (ready, claimed, or delayed) is left untouched. The idle-key case works
because final settle deletes the `dkq_key` row (below), so a fresh enqueue recreates it as ready.

### settle — fence-check, clear owned, end the claim

```sql
-- lock the key and check the token in one shot
select fence from dkq_key where queue=$1 and msg_key=$2 for update;
-- if fence <> $token: the claim was revoked — return stale, touch nothing

-- per settled id: ack deletes the message, nack clears ownership (leaving order intact)
delete from dkq_message where queue=$1 and msg_key=$2 and id = any($ackIds) and owned;
update dkq_message set owned=false where queue=$1 and msg_key=$2 and id = any($nackIds) and owned;

-- if any owned remain: the claim lives on, token still valid — done.
-- else the claim ends: bump fence, then pick the next state --
--   no messages left      -> delete from dkq_key ...              (idle: the row disappears)
--   a nack asked to wait   -> state=2, delayed_until=now()+backoff
--   otherwise              -> state=0, ready_seq=nextval, pg_notify
```

More statements than the one Lua call, but one transaction, so equally atomic. `SREM` returning 0/1 — "did
this claim own it" — becomes the `and owned` predicate: an id the claim never held matches nothing, which is
what makes a retried settle harmless. Ending the claim by *deleting* the `dkq_key` row when no messages
remain is how "idle is the absence of the key" survives; the enqueue path recreates it.

### renew — fence-checked lease push, one row per key

```sql
update dkq_key set claimed_until = now() + $ttl::interval
 where queue=$1 and msg_key = any($keys) and state=1 and fence = $token   -- token per key, so: unnest pairs
 returning msg_key;                 -- returned = renewed; every held key not returned is lost
```

The `state=1 and fence=$token` predicate is renew.lua's `XX` + token guard in one clause: it never
resurrects a revoked claim and never extends one handed to someone else. As in Redis, the caller learns
which keys it *lost* by which it sent are missing from the returned set.

### sweep — the two passes, still two, and cheaper to keep apart

```sql
-- (1) reclaim lapsed leases; the "not if backing off" guard is one more predicate
update dkq_key set state=0, ready_seq=nextval('dkq_seq'), fence=fence+1, claimed_until=null
 where queue=$1 and state=1 and claimed_until < now()
 returning msg_key;                 -- reclaimed; also: update dkq_message set owned=false where key in (...)

-- (2) release elapsed backoffs
update dkq_key set state=0, ready_seq=nextval('dkq_seq'), delayed_until=null
 where queue=$1 and state=2 and delayed_until < now()
 returning msg_key;                 -- released; pg_notify each
```

`dkq_leases` and `dkq_backoff` (the partial indexes) make each pass an index range scan over just the due
rows. `LIMIT` per pass, as today, keeps a sweep from locking too much at once.

## The wake: `LISTEN/NOTIFY`, and the trap that voids it

`NOTIFY dkq_wake, 'orders'` on the enqueue commit; every instance runs `LISTEN dkq_wake` on one dedicated
connection and calls `readiness.ready(queue)` on each notification. This is the Redis listener's shape
exactly — one connection that does nothing but wait — so the connection model is symmetric, not new.

Three properties, all fine for DKQ:

- **Fires on commit, not on statement.** A consumer woken by the notification cannot arrive before the row
  it announces is visible, because the transaction that inserted it has committed. This is the same
  ordering guarantee enqueue.lua gets by doing the `XADD` inside the script.
- **Fire-and-forget.** A disconnected listener misses notifications; nothing queues them. Acceptable for the
  same reason a trimmed stream is — the claim reads real state, the retry loop is the floor.
- **Payload is small** (≤ ~8000 bytes; a queue name is nothing), and identical `(channel, payload)` pairs
  de-duplicate within a transaction — which is a small gift, since a burst of enqueues to one queue
  collapses to one wake, exactly what `Readiness` wants.

**The trap: a transaction-pooling connection pooler breaks `LISTEN`.** PgBouncer in `transaction` or
`statement` mode hands a different backend connection to each transaction, and `LISTEN` needs a connection
pinned for the session's life — so notifications are delivered to whichever backend happens to hold the
listen, which is not the one the app thinks is listening. The listener connection must bypass the pooler
(connect straight to Postgres) or use `session` pooling. This is the single most likely way a Postgres DKQ
"loses wakes" in production, and it is a deployment mistake, not a code one — worth a loud line in whatever
runs it. The claim/enqueue connections can pool normally; only the listener is special, which mirrors
Redis's one-special-connection split.

## Where it actually hurts

### Autovacuum churn — the real weakness, and it is real

A keyed queue is the highest-churn table shape there is: `enqueue` inserts, `ack` deletes, constantly. Every
deleted message and every reclaimed key leaves a dead tuple, and Postgres's MVCC means the "oldest ready
key" scan and the FIFO read wade through dead tuples until autovacuum reclaims them. If autovacuum falls
behind — and its defaults are tuned for tables that are read more than they churn — the partial indexes
bloat, scans slow, and the whole thing degrades exactly under sustained load. This is *the* reason
"don't use Postgres as a queue" is folklore, and it is not wrong; it is just beatable:

- per-table aggressive autovacuum (`autovacuum_vacuum_scale_factor` near zero, a low threshold) so it runs
  often on these two tables specifically;
- keeping rows narrow (the payload is `bytea` on `dkq_message`, so a claimed-then-acked message's whole row
  churns — consider whether large payloads belong in a side table read only on claim);
- at homelab volume (low message rate, few instances) this is a non-issue with sane settings; it becomes the
  dominant concern only at Redis-competitive throughput, which is not why one would pick Postgres here.

Honest framing: Redis's weakness is that it is another system to run; Postgres's weakness is that this
workload is adversarial to its storage engine and must be tuned for. Pick the weakness that fits the
deployment.

### The smaller costs

- **Migrations.** Postgres needs a schema and versioned migrations (the repo's `resources/migrations/`
  convention, per `CLAUDE.md`); Redis is schemaless. Minor, and the homelab already does this elsewhere.
- **A pool, not a connection.** Redis's adapter is single-threaded-friendly: one shared connection. Postgres
  wants a real pool (concurrent transactions are the point) plus the one pinned `LISTEN` connection. This is
  the `zio-conduit-doobie` template's model — Doobie over HikariCP — which `CLAUDE.md` already names as the
  ZIO reference, so it is a well-trodden path, not a new one.
- **Sequence gaps.** `dkq_seq` gaps on rollback; irrelevant, since it is only ever used for monotonic
  ordering, never as a count.
- **Advisory locks as a session-lease** are a Postgres-only option — the one substrate with ZooKeeper's
  ephemeral-on-death property natively; see [`zookeeper-ideas.md`](zookeeper-ideas.md). Latent, with the
  same pooler caveat as `LISTEN`.

## What building it would look like here

The port is ready: `QueueStore` says nothing about Redis, and `Readiness` is already substrate-agnostic
(its wake source is the only Redis-specific piece). A Postgres adapter is a new `infrastructure/postgres/`
folder parallel to `infrastructure/redis/`, the shape `CLAUDE.md` prescribes and `zio-conduit-doobie`
models:

- `PostgresQueueStore` implementing the five operations as Doobie transactions;
- a `PgWakeListener` that `LISTEN`s and drives the existing `Readiness` — the same seam the substrate note's
  `WakeChannel` port would formalise;
- `resources/migrations/` for the two tables, the sequence, the indexes;
- the same in-memory parallel adapter the Redis side lacks today would be worth building alongside, since
  two real adapters is what actually validates the port.

Nothing above the store changes — use cases, validation, the gRPC surface, `Readiness` itself.

## When to choose it

- **Consolidating datastores matters more than the last millisecond.** A homelab already running Postgres
  drops a whole system (Redis) by moving DKQ onto it — net *less* to operate, which is the opposite of the
  usual "add Postgres" story.
- **Many keys, many consumers, moderate rate.** The per-key lock granularity is a genuine advantage over
  Redis's global serialisation here, and moderate rate keeps autovacuum comfortable.
- **Not** when throughput is the goal and the box is already busy — that is where the churn weakness bites
  and Redis's single-threaded simplicity wins.

## Open questions

1. Payload placement: inline `bytea` (simple, churns the whole row) vs a side table (narrower hot rows,
   an extra read on claim). Decidable only against a real payload-size distribution.
2. Is one global `dkq_seq` a contention point at high enqueue rates, or is `nextval` caching enough? Cheap
   to measure, worth knowing before trusting the fairness ordering under load.
3. Does the in-memory adapter get built *with* the Postgres one (to validate the port against two real
   shapes), or is that a third piece of speculative work until a second deployment exists?
