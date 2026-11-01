---
title: "2026-11-02 — the old review threads, answered; lock bugs and renames parked"
type: session
status: current
updated: 2026-11-02
tags: [review, lock, trim, naming, backlog]
---

# 2026-11-02 — the old review threads, answered

Old review threads that were still unanswered (on #2–#30) were triaged against current `main`. Those that no
longer held got a reply naming the commit that fixed them, or saying why they were not valid. Those that still
held are fixed in the PR this note ships with, or listed below as open. The toolkit side is in
`homelab-toolkit-zio/docs/sessions/2026-11-02-old-review-threads.md`.

## Fixed in this PR

- **Token 0.** `release.lua` and `refresh.lua` fell back to `0` when a lock had no token entry, so a receipt
  carrying token 0 could release a lock nobody held, or refresh it into a hold that no token backs. Both scripts
  now require the entry to exist. `RedisLockStoreSpec` pins it; the spec fails without the fix.
- **The trim limit is per partition, on purpose.** A Lua script blocks the whole server, so the thing the limit
  has to bound is one script. Each partition runs its own script, so a pass may free up to 16 × `limit`. A
  global budget was tried and dropped: it made no script shorter, and it would have taken more passes to clear a
  backlog. The docs now say this: `LockStore.trim`, `RedisLockStore.trim`, and `LockCleanup`, whose repeat
  condition can run one extra pass for nothing but never misses a full one.
- **Docs:** the startup sequence of `WakeConsumer.make` (reachable, positioned, start); the current method names
  in `lock-mechanics.md`; `states-as-classes.md` marked superseded by `Recursion`; `v3` in the Lua prefix
  comments.
- **Specs:** the lock defaults in `QueueConfigSpec`; `EmptyEncoding`, alone and accumulated with the other
  errors; `GrpcSpec` waits for the waiter's ticket in Redis instead of sleeping 300 ms.

## Open — confirmed, design needed

These came from Copilot on #8 and still hold.

1. **An acquire interrupted after the grant leaks the hold.** If `store.ask` is interrupted after `grant.lua`
   has granted, `onExit` calls `withdraw`, which cannot find the ticket. The hold then blocks everyone until its
   lease expires.
2. **`abandon.lua` wakes nobody.** When the head ticket withdraws, the waiters behind it sleep until that
   ticket's old deadline. A waiter with less patience can time out on a free lock.
3. **The grant is O(n²) under contention.** Every waiter behind the head scans the list with `LRANGE`, and one
   release wakes all of them.

## Approved, not done yet

No `of` factories or parameters, and no parameter names that try to make the call read as a sentence:

- **`KeyLayout.of(cluster)`**: remove it. Callers write `if cluster then KeyLayout.cluster else KeyLayout.single`,
  as `redis.Module.layout` already does.
- **`ManagedConsumerSpec.claim(of: Message.Incoming)`** → `claim(message: …)`.
- **`ManagedBatchSpec.claim(of: Message.Incoming*)`** → `claim(messages: …)`, and **`verdicts(of: …)`** →
  `verdicts(settled: …)`.
- **`HeartbeatSpec.claim(receipt, of: String)`** → `claim(receipt, messageId: String)`.

## Still open

- Adopting the toolkit's `Shutdown`.
