package homelab.keyedqueue.infrastructure.configuration


import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import zio.*
import zio.test.*


/**
 * That the shipped HOCON parses into the shape the service expects.
 *
 * Worth a test because the failure mode is late and unhelpful: a renamed key or a duration the reader
 * cannot parse only shows up when a deployment refuses to start, and every value here has a default that
 * would otherwise hide the mistake until someone overrode it.
 */
object QueueConfigSpec extends ZIOSpecDefault:

  def spec: Spec[TestEnvironment & Scope, Any] = suite("QueueConfig")(
    test("the defaults in config/queue.conf load, and every field maps") {
      for config <- QueueConfig.load
      yield assertTrue(
        config.redisUrl == "redis://localhost:6379",
        !config.cluster,
        config.port == 9000,
        config.leaseTtl == 30.seconds, // "30 seconds" in HOCON, a zio.Duration here
        config.sweepInterval == 5.seconds,
        config.sweepLimit == 100,
        config.lockTrimInterval == 120.seconds,
        config.lockTrimGrace == 10.minutes,
        config.lockMaxTtl == 10.minutes,
        config.wakeBlock == 1.second,
        config.maxWait == 30.seconds,
        config.maxBatchLimit == 32,
      )
    },
    test("the lease outlasts a sweep, and a wait fits inside the lease") {
      // Not style: a lease shorter than the sweep interval would have work reclaimed before anyone could
      // renew it, and a wait longer than the lease would let a claim expire while its caller still blocks.
      for config <- QueueConfig.load
      yield assertTrue(
        config.leaseTtl > config.sweepInterval,
        config.maxWait <= config.leaseTtl,
        config.wakeBlock.toMillis > 0,
        config.sweepLimit > 0,
      )
    },
    test("the lock settings are usable: a trim runs, a late holder has a window, a grant has a lease") {
      // A zero trim interval would spin the trim, a zero grace would remove a holder the moment its lease
      // lapsed (refresh is meant to forgive lateness up to the grace), and a zero cap would clamp every
      // grant to nothing.
      for config <- QueueConfig.load
      yield assertTrue(
        config.lockTrimInterval > Duration.Zero,
        config.lockTrimGrace > Duration.Zero,
        config.lockMaxTtl > Duration.Zero,
      )
    },
  )
