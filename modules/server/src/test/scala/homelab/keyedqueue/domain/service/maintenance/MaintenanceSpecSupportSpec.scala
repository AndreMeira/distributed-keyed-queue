package homelab.keyedqueue.domain.service.maintenance


import zio.*
import zio.test.*


/**
 * The bounded wait the maintenance specs rely on: it must pass on a fiber that succeeded, and fail the test on one
 * that failed or died, since a regression test that waits on a fiber is only as good as that distinction.
 */
object MaintenanceSpecSupportSpec extends ZIOSpecDefault:

  def spec = suite("MaintenanceSpecSupport.done")(
    test("passes on a fiber that succeeded") {
      for
        fiber <- ZIO.succeed(1).fork
        _     <- MaintenanceSpecSupport.done(fiber, "the work")
      yield assertCompletes
    },
    test("fails, with the cause, on a fiber that failed") {
      for
        fiber  <- ZIO.fail("boom").fork
        result <- MaintenanceSpecSupport.done(fiber, "the work").flip
      yield assertTrue(result.startsWith("the work failed"), result.contains("boom"))
    },
    test("fails, with the cause, on a fiber that died") {
      for
        fiber  <- ZIO.dieMessage("defect").fork
        result <- MaintenanceSpecSupport.done(fiber, "the work").flip
      yield assertTrue(result.startsWith("the work failed"), result.contains("defect"))
    },
    test("fails, saying what did not happen, on a fiber that never finishes") {
      for
        fiber  <- ZIO.never.fork
        result <- MaintenanceSpecSupport.done(fiber, "the work").flip
        _      <- fiber.interrupt
      yield assertTrue(result == "the work did not happen")
    },
  )
