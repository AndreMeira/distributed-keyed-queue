---
title: "Moving the request boundary from validation to parsing"
type: session
status: current
updated: 2026-08-31
tags: [validation, parsing, domain, ports, zio-prelude, open]
---

# Moving the request boundary from validation to parsing

The four `QueueRequest` types were checked and then used. They are now *parsed* — turned into a type the
store accepts, which nothing else can produce. Recording what changed, why the intermediate designs were
wrong, and the two loose ends left behind.

## The rule that decided every case

`parse(unsafe: A): Validated[B]`, where **A is expressible from the wire alone** and **B is unconstructible
without having passed**.

The second half was the part that had been missing. A request holding a `QueueName` or a `ClaimRef` is a
codec minting *evidence* — of a name this service can address, of a receipt it issued — for values nobody
has looked at. The domain types are the mark that something was checked, so a request must be incapable of
carrying one.

| untrusted | parse | trusted |
|---|---|---|
| `Enqueue(String, Enqueue.Message)` | `Validated[Submission]` | `Submission(QueueName, Message)` |
| `Dequeue(String, Duration, Int)` | `Validated[Demand]` | `Demand(QueueName, patience, batch)` — bounded |
| `Settle(String, Chunk[Outcome], Duration)` | `Validated[Settlement]` | `Settlement(Claim, NonEmptyChunk, Option)` |
| `Heartbeat(Chunk[String])` | `Renewal` | `Renewal(held, unreadable)` |

Every request field is now a `String`, `Int`, `Duration` or `Chunk[Byte]`. The only non-primitives left are
`Verdict` and `Encoding`, and those are not claims: the codec refuses the wire's `UNSPECIFIED`, so what
arrives is total.

## What it removed

- **Use cases can no longer reach past the parse.** `EnqueueUseCase` used to validate a request and then
  hand `request.queue` — the unvalidated one — to the watchdog and the store. That is now a type error.
- **`SettleUseCase.settlement` disappeared.** Its three narrowings (`Chunk` to `NonEmptyChunk`, raw ids to
  `MessageId`, `Duration.Zero` to `None`) were parsing done outside the parser, complete with a branch that
  re-reported a problem validation had already excluded.
- **`SyncUseCases.Config` disappeared.** The service's limits are enforced by the parse, so they live with
  it as `QueueInputValidation.Config`; `DequeueUseCase` lost two constructor parameters and all its
  arithmetic.
- **`Inbound` mints nothing for requests.** `Transformer[String, QueueName]` and `[String, ClaimRef]` are
  gone. `MessageKey` and `MessageId` remain for the *storage* path only, where reconstructing names from
  bytes this service wrote is restoring evidence rather than inventing it.

## Three designs that were wrong on the way

- **Receipt-first.** `parse(request, claim)` with the use case resolving the receipt beforehand preserved
  the old "an unreadable receipt is stale" answer — but a garbled receipt then hid an empty batch, so a
  caller with both problems heard one, fixed it, and heard the next. Reading the receipt is a parse; it
  belongs with the others. A string that was never a receipt is now `InvalidRequest`; a receipt whose claim
  was revoked is still `Stale`, and only the store can answer that.
- **A gate in front of the checks.** `batch(...).flatMap { … }` refused an empty settle before parsing the
  ids, which cost the same accumulation for the same reason. `batch` now *uses* the per-id parse instead of
  gating it, and the whole settle parse is one `validate`. The regression test — a garbled receipt beside a
  bad id — fails on the staged version and passes on the flat one.
- **`Validated[Renewal]`.** Heartbeat cannot fail: one unreadable receipt says nothing about the others,
  and refusing the call would cost a consumer the renewals that were good. Its parse returns `Renewal`
  directly, so no caller handles a case that does not exist.

## Loose ends

- **`QueueInputValidation` is misnamed.** Three of its four `parse` methods construct rather than check,
  and one cannot fail. `QueueRequestParsing` or `RequestParser` would describe it. Deliberately not done
  now — it touches every call site for no behaviour, and is worth doing when something else brings us
  through those files. Its nested config was renamed `Limits` to `Config` in passing, matching
  `Watchdog.Config` and the `configuration.Module.validation` layer that provides it.
- **`payloadType` is still unchecked.** An empty one is storable and meaningless. Refusing it is a contract
  change and deserves its own decision rather than being smuggled into a refactor.

## Still open elsewhere

The build bug found during this work —
[`2026-08-31-zio-grpc-module-not-compiled.md`](2026-08-31-zio-grpc-module-not-compiled.md) — is what to
pick up next.
