package homelab.keyedqueue.infrastructure.configuration


import homelab.common.error.ApplicationError
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
 * @param wakeBlock how long one read of the wake streams waits before going round again. Not a latency
 *                  bound: a read returns the moment an entry lands, and the streams it names are fixed, so
 *                  nothing waits on this. It bounds how long a half-open connection goes unnoticed
 *                  and how long a stopping instance waits out a read it cannot cancel
 * @param wakeBuckets how many wake streams the deployment is divided into, and therefore how many hash
 *                    tags its keys are spread over. Permanent for a deployment: changing it moves
 *                    queues between tags and strands what was written under the old one. One is the
 *                    single-node answer — one stream, one slot; above one, queues spread across
 *                    slots and each bucket carries its own
 * @param maxWait the longest a caller may ask to wait, and the connection's command timeout
 * @param maxBatchLimit the most messages this service will hand over in one claim
 */
final case class QueueConfig(
  redisUrl: String,
  cluster: Boolean,
  port: Int,
  leaseTtl: Duration,
  sweepInterval: Duration,
  sweepLimit: Int,
  wakeBlock: Duration,
  wakeBuckets: Int,
  maxWait: Duration,
  maxBatchLimit: Int,
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
  val load: IO[ApplicationError, QueueConfig] =
    ZIO
      .attempt(ConfigSource.resources("config/queue.conf").load[QueueConfig])
      .mapError(error => Misconfigured(s"config/queue.conf could not be read: ${error.getMessage}"))
      .flatMap:
        case Right(config)  => ZIO.succeed(config)
        case Left(failures) => ZIO.fail(Misconfigured(s"config/queue.conf is invalid: ${failures.prettyPrint()}"))
