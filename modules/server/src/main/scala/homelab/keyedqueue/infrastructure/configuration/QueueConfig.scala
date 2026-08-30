package homelab.keyedqueue.infrastructure.configuration


import homelab.keyedqueue.domain.error.QueueError
import pureconfig.{ ConfigReader, ConfigSource }
import zio.*


/**
 * Everything this service needs to run, read from `resources/config/queue.conf`.
 *
 * One lease and one heartbeat interval for every queue: per-queue tuning is a knob nobody can set correctly
 * before there is traffic to observe, so phase 1 does not offer it (`docs/research/phase-1-api.md`).
 *
 * Defaults live in the HOCON rather than here, so there is one place to read what a setting means and what
 * it is when nobody sets it — each key carries a working default and a `${?DKQ_...}` override in the same
 * two lines.
 *
 * @param redisUrl where the substrate lives; a seed node when `cluster` is set
 * @param cluster whether that URL names a Redis Cluster rather than a single server
 * @param port the port the gRPC server listens on
 * @param leaseTtl how long a claim survives without a heartbeat
 * @param sweepInterval how often each instance runs the repair sweeps
 * @param sweepLimit the most entries one sweep handles per kind, so a script cannot block the server
 * @param claimers how many connections may be parked in a idle claim at once — the ceiling on
 *                 concurrent `Dequeue` calls this instance can serve
 * @param maxWait the longest a caller may ask to wait, and the connection's command timeout
 */
final case class QueueConfig(
  redisUrl: String,
  cluster: Boolean,
  port: Int,
  leaseTtl: Duration,
  sweepInterval: Duration,
  sweepLimit: Int,
  claimers: Int,
  maxWait: Duration,
) derives ConfigReader


object QueueConfig:

  /**
   * Read HOCON durations, which pureconfig otherwise cannot.
   *
   * `zio.Duration` is `java.time.Duration`, and the reader pureconfig picks for that wants ISO-8601
   * (`PT30S`). HOCON — and every other config file in the homelab — writes `30 seconds`, so read it as the
   * Scala duration, which understands that form, and convert. Defined here so it is in scope where the
   * derived reader for this class is generated.
   */
  private given ConfigReader[Duration] =
    ConfigReader[scala.concurrent.duration.FiniteDuration].map(Duration.fromScala)

  /**
   * Read the configuration.
   *
   * A malformed or missing file fails startup rather than falling back to something invented here: a
   * service that silently runs on a guessed lease is worse than one that does not start.
   *
   * @return the configuration; aborts with `Misconfigured` describing every problem pureconfig found, not just
   *         the first
   */
  val load: IO[QueueError, QueueConfig] =
    ZIO
      .attempt(ConfigSource.resources("config/queue.conf").load[QueueConfig])
      .mapError(error => QueueError.Misconfigured(s"config/queue.conf could not be read: ${error.getMessage}"))
      .flatMap:
        case Right(config)  => ZIO.succeed(config)
        case Left(failures) => ZIO.fail(QueueError.Misconfigured(s"config/queue.conf is invalid: ${failures.prettyPrint()}"))
