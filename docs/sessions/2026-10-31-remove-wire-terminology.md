---
title: "2026-10-31 — removing the \"wire\" terminology (carry-over todo list)"
type: session
status: current
updated: 2026-10-31
tags: [naming, refactoring, terminology, grpc, protobuf, todo]
---

# Removing the "wire" terminology

**Why:** the homelab `CLAUDE.md` rule is *no "wire" in DTO names*. A DTO-like type, its package, or a
module holding such types is named after the concrete protocol (`proto` for protobuf-generated types). The
same goes for comments and docs where "wire" would mean a DTO: name the specific type (`v1.DequeueResponse`,
"the protobuf `Message`"), or the protocol when no single type fits. **When the right name isn't obvious,
propose one and get approval before using it.**

**"Over the wire" → "over the network"**, wherever the traffic really crosses a network (a socket, loopback
included). Checked against what each test runs over before changing.

**Out of scope:** other transport uses of "wire" ("costs nothing on the wire", "the other side of a wire"),
"on the wire" where no network is involved (the in-process `GrpcClientSpec`s), and "wiring" in the DI
sense. These stay.

**State:** nothing changed in this repo yet. This page is the inventory, so the work can resume after
something else. Line numbers are as of 2026-10-31 (`main` after #56).

**Decided, not on this list:** `modules/protocol` / `protocol-zio-grpc` keep their names.

## 1. Names: approved and renamed

All six were approved and renamed on branch `rename-wire-identifiers`. The test file is now `e2e/.../e2e/GrpcChannelSpec.scala`.

| Where | Now | Proposed | What it is | Status |
|---|---|---|---|---|
| `e2e/.../e2e/WireSpec.scala` | `object WireSpec` | `GrpcChannelSpec` | "one channel, many callers": HTTP/2 corruption regression over one gRPC channel | done |
| same file, lines 32, 53 | queues `"wire-java"`, `"wire-zio"` | `"channel-java"`, `"channel-zio"` | queue names used by that spec | done |
| `modules/server/src/test/.../SpecHelper.scala:129` | `Helper.wireMessage(key, body)` | `Helper.protoMessage(key, body)` | builds a `v1.Message` (protobuf) | done |
| `modules/client/src/test/.../codec/QueueCodecsSpec.scala:29` | `wireMessage(id, body)` | `protoMessage(id, body)` | builds a `v1.Message` | done |
| `e2e/.../e2e/Instance.scala:154` | `Instance.wire(duration)` | `Instance.toProto(duration)` | `Duration` → protobuf `Duration` | done |
| `modules/server/src/test/.../codecs/grpc/v1/OutboundSpec.scala:25, 32` | local `val wire` | `val proto` | the encoded protobuf response | done |

References to update with the renames:
- **`WireSpec`:**
  - `build.sbt:78`
  - `docs/architecture/end-to-end-testing.md`
  - `docs/learning-material/writing-end-to-end-tests.md`
  - (`docs/sessions/2026-08-23-netty-hpack-corruption.md` is a dated record; see §4)
- **`wireMessage`:**
  - `GrpcSpec.scala` lines 74, 90, 106, 126, 135, 146, 147, 155
  - `QueueCodecsSpec.scala:33`
- **`Instance.wire`:** `Instance.scala` lines 113, 132

## 2. Comments where "wire" means a DTO

Name the specific type. Line numbers are as on the `rename-wire-identifiers` branch.

**Server, gRPC services:** each `@param request the wire request` / `@return the wire response` names the
method's own types.
- [x] `application/grpc/v1/QueueService.scala`
  - 42, 43 → `v1.EnqueueRequest` / `v1.EnqueueResponse`
  - 56, 57 → `v1.DequeueRequest` / `v1.DequeueResponse`
  - 70, 71 → `v1.SettleRequest` / `v1.SettleResponse`
  - 84, 85 → `v1.HeartbeatRequest` / `v1.HeartbeatResponse`
  - 97 "a wire message" → "a `v1.Message`"
- [x] `application/grpc/v1/LockService.scala`
  - 35, 36 → `v1.AcquireRequest` / `v1.AcquireResponse`
  - 46, 47 → `v1.TryAcquireRequest` / `v1.AcquireResponse`
  - 57, 58 → `v1.ReleaseRequest` / `v1.ReleaseResponse`
  - 68, 69 → `v1.RefreshRequest` / `v1.RefreshResponse`

**Server, codecs:**
- [x] `infrastructure/codecs/grpc/v1/Inbound.scala`
  - 21 "Wire to domain." → "v1 to domain." (matches `Outbound`'s "Domain to v1.")
  - 66 `@param message the wire message` → "the `v1.Message`"
  - 78 "the wire message could not be read" → "the `v1.EnqueueRequest` could not be read"
- [x] `infrastructure/codecs/grpc/v1/Outbound.scala`
  - 24 "a wire representation, because the wire is strictly the wider" → "a protobuf representation, because the proto is strictly the wider"
  - 44 "what 'no claim' looks like on the wire" → "… in a `v1.DequeueResponse`"
  - 76, 79 "the wire form of a message" → "the `v1.Message` form"
  - 92, 93 "the wire states it as fields…", "where wire concerns belong" → "`v1.DequeueResponse` states it…", "where protobuf concerns belong"
  - each `@return the wire response`, named after its extension: 85 `v1.EnqueueResponse`, 95 `v1.DequeueResponse`, 102 `v1.SettleResponse`, 106 `v1.HeartbeatResponse`, 115 `v1.AcquireResponse`, 129 `v1.ReleaseResponse`, 133 `v1.RefreshResponse`
- [x] `modules/protocol/.../v1/keyed_queue_service.proto` 4, 5 — "the wire jar", "the wire module" → "the `protocol` jar / module" (a module holding DTOs)

**Client:**
- [x] `codec/QueueCodecs.scala`
  - 19 "the wire states a claim as a receipt…" → "`v1.DequeueResponse` states a claim…"
  - 33 → "the `v1.EnqueueRequest`"
  - 51, 54 → "as `v1.MessageOutcome` states it" / "the `v1.MessageOutcome`"
  - 169, 172 → "as `v1.Outcome` names it" / "the `v1.Outcome`"
- [x] `codec/Protos.scala`
  - 14, 16 "the times and spans the wire carries", "nothing on the wire bounds these fields" → "the protobuf `Timestamp`s and `Duration`s carry", "nothing in them bounds these fields"
  - 79, 100 → "the protobuf `Timestamp`"; 89, 109 → "the protobuf `Duration`"
- [x] `lock/managed/GrpcClient.scala` 57 "the wire says a boolean" → "`v1.ReleaseResponse` says a boolean"
- [x] `queue/managed/ManagedBatch.scala` 95 "the wire states outcomes per message" → "`v1.SettleRequest` states outcomes per message"
- [x] `queue/model/Message.scala` 12, 13 "what the wire carries", "know better than the wire" → "what the `v1.Message` carries", "… than the `v1.Message`"
- [x] `lock/LockClient.scala` 17 "What the wire states as a flag and three fields" → "What `v1.AcquireResponse` states as a flag and three fields"

**Tests and build:**
- [x] `LockCodecsSpec.scala` 18, `QueueCodecsSpec.scala` 18 "Reading the wire's answers, where the wire can state more…" → "Reading the protobuf responses, which can state more…"
- [x] `LockCodecsSpec.scala` 64 "Nothing on the wire bounds these fields" → "Nothing in the proto bounds these fields"
- [x] `OutboundSpec.scala` 18 "a lease the wire is meant to state" → "a lease the protobuf response is meant to state"
- [x] `QueueInputValidationSpec.scala` 153 "reads as zero on the wire" → "reads as zero in the `v1.DequeueRequest`" (the proto3 default)
- [x] `build.sbt` 258 "Wire <-> domain mapping" → "Protobuf <-> domain mapping"

## 3. Docs where "wire" means a DTO

- [x] `docs/learning-material/proto-generation.md` 112 "The wire artifact" → "The `protocol` artifact"
- [x] `docs/README.md` 72 "what the wire owed it" → "what the protobuf contract owed it"

Bump `updated:` in the frontmatter of each doc you edit.

## "Over the wire" → "over the network": done (branch `rename-wire-log-scope`)

- [x] e2e (Docker instances, reached through host ports): `EndToEndSpec.scala` 10, `LockSpec.scala` 9,
  `build.sbt` 287, `README.md` 177, `docker-compose.yml` 80, `project/plugins.sbt` 11,
  `docs/architecture/end-to-end-testing.md` 2 (title)
- [x] server tests (a real server on port 19099, dialled over TCP): `GrpcSpec.scala` 25, 45 (test names),
  46, and `SpecHelper.scala` 184

## Left as is: not a DTO, and "network" wouldn't be accurate

- `GrpcClientSpec.scala` (lock 23, queue 22) "put the right thing on the wire": these run over gRPC's
  **in-process** transport, so there is no network
- `build.sbt` 174, 313 "reach past the wire into the implementation" (the process boundary), 193 "the wire tests", 314 "side of the wire"; `docs/architecture/end-to-end-testing.md` 118
  "after touching the substrate or the wire" (meaning the protocol)
- `MessageEncoder.scala` 70 "costs nothing on the wire" (bytes sent) and `ServiceError.scala` 13 "the other side of a wire"
- `LuaScript.scala` 25, 224 and `ClaimScript.scala` 25 — the Redis reply format, not a DTO of ours

## 4. Dated records and research drafts: decision needed

By the docs rules these are written once and not edited to stay true. **Default: leave as written**, even
where "wire" means a DTO. Confirm or override before touching them.

- `docs/research/client-library.md` (18 lines), `look-ahead-and-discard.md` (13, 56, 61), `roadmap.md` (74), `the-queue-as-a-signal.md` (12)
- `docs/sessions/2026-08-23-netty-hpack-corruption.md` (70, 82, 88), `2026-08-31-parse-dont-validate.md` (17, 33),
  `2026-09-15-spec-organisation-and-scaladoc-pollution.md` (192, 193), `2026-09-20-encoder-decoder-payload-type-asymmetry.md` (39, 49),
  `2026-09-20-the-client-makes-it-a-product.md` (11, 24, 25, 53, 63)

## 5. Done when

- [x] Every name in §1 is approved and renamed.
- [x] §2 and §3 are done (branch `rename-wire-dto-comments`).
- [ ] `sbt test` passes, and so does the e2e suite (`sbt e2e/test`, which needs Docker).
- [ ] Everything this finds is in §4 or under "Left as is":
  ```bash
  grep -rnE '\b[Ww]ires?\b|\b[Ww]ire-[a-z]|[a-z]Wire|Wire[A-Z]|wire[A-Z]' --exclude-dir={target,.git,.bloop,.metals,.bsp} .
  ```
