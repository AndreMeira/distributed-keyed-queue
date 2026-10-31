---
title: "2026-10-31 — removing the \"wire\" terminology (carry-over todo list)"
type: session
status: current
updated: 2026-10-31
tags: [naming, refactoring, terminology, grpc, protobuf, todo]
---

# Removing the "wire" terminology

**Why:** the homelab `CLAUDE.md` now says *don't say "wire"*. Name the concrete protocol instead (`proto`
for protobuf-generated types, gRPC, HTTP/JSON, the Redis reply, …), in identifiers, packages, artifacts and
prose. `Wire` is never acceptable for a DTO, a DTO package or a module holding DTOs. **When the right name
isn't obvious, propose one and get approval before using it.** "Wiring" in the dependency-injection sense
is fine.

**State:** nothing changed in this repo yet. This page is the inventory, so the work can resume after
something else. Line numbers are as of 2026-10-31 (`main` after #56).

**Decided, not on this list:** `modules/protocol` / `protocol-zio-grpc` keep their names.

## 1. Names: need approval before renaming

Proposals only. Don't rename until each one is approved.

| Where | Now | Proposed | What it is | Status |
|---|---|---|---|---|
| `e2e/.../e2e/WireSpec.scala` | `object WireSpec` | `GrpcChannelSpec` | "one channel, many callers": HTTP/2 corruption regression over one gRPC channel | awaiting approval |
| same file, lines 32, 53 | queues `"wire-java"`, `"wire-zio"` | `"channel-java"`, `"channel-zio"` | queue names used by that spec | awaiting approval |
| `modules/server/src/test/.../SpecHelper.scala:129` | `Helper.wireMessage(key, body)` | `Helper.protoMessage(key, body)` | builds a `v1.Message` (protobuf) | awaiting approval |
| `modules/client/src/test/.../codec/QueueCodecsSpec.scala:29` | `wireMessage(id, body)` | `protoMessage(id, body)` | builds a `v1.Message` | awaiting approval |
| `e2e/.../e2e/Instance.scala:154` | `Instance.wire(duration)` | `Instance.toProto(duration)` | `Duration` → protobuf `Duration` | awaiting approval |
| `modules/server/src/test/.../codecs/grpc/v1/OutboundSpec.scala:25, 32` | local `val wire` | `val proto` | the encoded protobuf response | awaiting approval |

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

## 2. Code comments and test names

Rewrite each to the concrete protocol. Suggested word in brackets.

- [ ] `modules/server/.../application/grpc/v1/LockService.scala` 35, 36, 46, 47, 57, 58, 68, 69 — "the wire request/response" [the protobuf request/response]
- [ ] `modules/server/.../application/grpc/v1/QueueService.scala` 42, 43, 56, 57, 70, 71, 84, 85, 97 — same, and "a wire message" [a protobuf message]
- [ ] `modules/server/.../infrastructure/codecs/grpc/v1/Inbound.scala` 21 ("Wire to domain." [Protobuf to domain.]), 66, 78 [protobuf message]
- [ ] `modules/server/.../infrastructure/codecs/grpc/v1/Outbound.scala` 24, 44, 76, 79, 85, 92, 93, 95, 102, 106, 115, 129, 133 [protobuf form / response]
- [ ] `modules/server/.../infrastructure/redis/script/LuaScript.scala` 25, 224 — [the Redis reply / RESP types]
- [ ] `modules/server/.../infrastructure/redis/script/queue/ClaimScript.scala` 25 — [the reply's shape in RESP]
- [ ] `modules/protocol/src/main/protobuf/homelab/keyedqueue/v1/keyed_queue_service.proto` 4, 5 — "the wire jar", "the wire module" [the `protocol` jar / module]
- [ ] `modules/client/.../ServiceError.scala` 13 [the other side of a gRPC connection]
- [ ] `modules/client/.../codec/Protos.scala` 14, 16, 79, 89, 100, 109 [protobuf timestamp / duration]
- [ ] `modules/client/.../codec/QueueCodecs.scala` 19, 33, 51, 54, 169, 172 [the protobuf request / outcome / form]
- [ ] `modules/client/.../lock/LockClient.scala` 17; `lock/managed/GrpcClient.scala` 57 [the protocol / the protobuf response]
- [ ] `modules/client/.../queue/MessageEncoder.scala` 70; `queue/managed/ManagedBatch.scala` 95; `queue/model/Message.scala` 12, 13 [protobuf]
- [ ] tests: `LockCodecsSpec.scala` 18, 64; `QueueCodecsSpec.scala` 18; `lock/managed/GrpcClientSpec.scala` 23; `queue/managed/GrpcClientSpec.scala` 22; `QueueInputValidationSpec.scala` 153; `SpecHelper.scala` 128, 184
- [ ] test **names** in `GrpcSpec.scala` 25, 45, 46 — "the lock API over the wire", "try-acquire over the wire" [over gRPC]
- [ ] e2e: `EndToEndSpec.scala` 10, `LockSpec.scala` 9, `Instance.scala` 149, `WireSpec.scala` 66 [over gRPC / protobuf duration / protobuf message]
- [ ] build: `build.sbt` 193, 258, 287, 314; `project/plugins.sbt` 11; `docker-compose.yml` 80 [gRPC / protobuf ↔ domain]

## 3. Living docs

- [ ] `README.md` 177
- [ ] `docs/README.md` 72
- [ ] `docs/architecture/end-to-end-testing.md` 2, 40, 105, 118 (includes the `WireSpec` reference)
- [ ] `docs/learning-material/proto-generation.md` 112
- [ ] `docs/learning-material/writing-end-to-end-tests.md` 191

Bump `updated:` in the frontmatter of each doc you edit.

## 4. Dated records and research drafts: decision needed

By the docs rules these are written once and not edited to stay true. **Default: leave as written.** Confirm
or override before touching them.

- `docs/research/client-library.md` (18 lines), `look-ahead-and-discard.md` (13, 56, 61), `roadmap.md` (74), `the-queue-as-a-signal.md` (12)
- `docs/sessions/2026-08-23-netty-hpack-corruption.md` (70, 82, 88), `2026-08-31-parse-dont-validate.md` (17, 33),
  `2026-09-15-spec-organisation-and-scaladoc-pollution.md` (192, 193), `2026-09-20-encoder-decoder-payload-type-asymmetry.md` (39, 49),
  `2026-09-20-the-client-makes-it-a-product.md` (11, 24, 25, 53, 63)

## 5. Done when

- [ ] Every name in §1 is approved and renamed.
- [ ] §2 and §3 are done.
- [ ] `sbt test` passes, and so does the e2e suite (`sbt e2e/test`, which needs Docker).
- [ ] This finds nothing outside §4:
  ```bash
  grep -rnE '\b[Ww]ires?\b|\b[Ww]ire-[a-z]|[a-z]Wire|Wire[A-Z]|wire[A-Z]' --exclude-dir={target,.git,.bloop,.metals,.bsp} .
  ```
  The grep also matches the verb ("wires X into Y"); leave those.
