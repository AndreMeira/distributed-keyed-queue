package homelab.keyedqueue.infrastructure.configuration


import zio.*


/**
 * Everything this service needs to run, from the environment.
 *
 * One lease and one heartbeat interval for every queue: per-queue tuning is a knob nobody can set correctly
 * before there is traffic to observe, so phase 1 does not offer it (`docs/research/phase-1-api.md`).
 *
 * @param redisUrl where the substrate lives
 * @param port the port the gRPC server listens on
 * @param leaseTtl how long a claim survives without a heartbeat
 * @param sweepInterval how often each instance runs the repair sweeps
 * @param sweepLimit the most entries one sweep handles per kind, so a script cannot block the server
 * @param claimers how many connections may be parked in a blocking claim at once — the ceiling on
 *                 concurrent `Dequeue` calls this instance can serve
 * @param maxWait the longest a caller may ask to wait, and the connection's command timeout
 */
final case class QueueConfig(
  redisUrl: String,
  port: Int,
  leaseTtl: Duration,
  sweepInterval: Duration,
  sweepLimit: Int,
  claimers: Int,
  maxWait: Duration,
)


object QueueConfig:

  /**
   * Read the configuration, falling back to values that work against `docker-compose.yml`.
   *
   * Defaults exist so a developer can run the service with no environment at all; production sets every one
   * of them, and `DKQ_LEASE_TTL` in particular should exceed the slowest handler a consumer will run.
   *
   * @return the configuration; never fails
   */
  val fromEnvironment: UIO[QueueConfig] =
    for
      url      <- env("DKQ_REDIS_URL", "redis://localhost:6379")
      port     <- env("DKQ_PORT", "9000").map(_.toIntOption.getOrElse(9000))
      lease    <- millis("DKQ_LEASE_TTL", 30.seconds)
      interval <- millis("DKQ_SWEEP_INTERVAL", 5.seconds)
      limit    <- env("DKQ_SWEEP_LIMIT", "100").map(_.toIntOption.getOrElse(100))
      claimers <- env("DKQ_CLAIMERS", "8").map(_.toIntOption.getOrElse(8))
      maxWait  <- millis("DKQ_MAX_WAIT", 30.seconds)
    yield QueueConfig(url, port, lease, interval, limit, claimers, maxWait)

  /**
   * One variable, or its default.
   *
   * @param name the variable
   * @param fallback what to use when it is unset
   * @return the value
   */
  private def env(name: String, fallback: String): UIO[String] =
    System.env(name).orDie.map(_.filter(_.nonEmpty).getOrElse(fallback))

  /**
   * One variable read as milliseconds.
   *
   * @param name the variable
   * @param fallback what to use when it is unset or unreadable
   * @return the duration
   */
  private def millis(name: String, fallback: Duration): UIO[Duration] =
    env(name, fallback.toMillis.toString).map(_.toLongOption.fold(fallback)(Duration.fromMillis))
