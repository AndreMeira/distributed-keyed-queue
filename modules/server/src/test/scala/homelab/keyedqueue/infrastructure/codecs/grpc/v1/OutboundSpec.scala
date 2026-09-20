package homelab.keyedqueue.infrastructure.codecs.grpc.v1


import homelab.keyedqueue.SpecHelper.Helper
import homelab.keyedqueue.domain.response.queue.{ DequeueResponse, HeartbeatResponse }
import homelab.keyedqueue.infrastructure.codecs.grpc.v1.Outbound.toProto
import zio.*
import zio.test.*

import java.time.Instant


/**
 * What the derivation carries, for the fields it could silently drop.
 *
 * These transformers enable default values so that they stay derivable, which means a field the derivation
 * fails to match becomes the target's default rather than a compile error. For an `Option` that default is
 * `None`, so a lease the wire is meant to state would go out unset and nothing else here would say so.
 */
object OutboundSpec extends ZIOSpecDefault:

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Outbound")(
    test("a granted dequeue states the lease span, not only its deadline") {
      val granted = DequeueResponse.fromGrant(Helper.grant("orders", "key-1", "body"))
      val wire    = granted.toProto
      assertTrue(
        wire.leaseTtl.map(_.seconds).contains(30L),
        wire.leaseExpiresAt.isDefined,
      )
    },
    test("a heartbeat states the span its renewal runs for") {
      val wire = HeartbeatResponse(Chunk.empty, Instant.EPOCH, 30.seconds).toProto
      assertTrue(wire.leaseTtl.map(_.seconds).contains(30L))
    },
  )
