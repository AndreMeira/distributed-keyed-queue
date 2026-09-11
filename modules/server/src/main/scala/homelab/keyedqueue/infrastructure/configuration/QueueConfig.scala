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
 * @param lockTrimInterval how often each instance removes abandoned lock holds
 * @param lockTrimGrace how long past lease expiry a lock hold survives before a trim may remove it
 * @param lockMaxTtl the longest a single lock grant's lease may run; longer requests are clamped
 * @param wakeBlock how long one read of the wake streams waits before going round again. Not a latency
 *                  bound: a read returns the moment an entry lands, and the streams it names are fixed, so
 *                  nothing waits on this. It bounds how long a half-open connection goes unnoticed
 *                  and how long a stopping instance waits out a read it cannot cancel
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
  lockTrimInterval: Duration,
  lockTrimGrace: Duration,
  lockMaxTtl: Duration,
  wakeBlock: Duration,
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
        case Right(config)  => bounded(config)
        case Left(failures) => ZIO.fail(Misconfigured(s"config/queue.conf is invalid: ${failures.prettyPrint()}"))

  /**
   * Refuse values that parse but cannot run.
   *
   * Environment overrides arrive as free text, so a well-formed file can still carry a zero sweep limit
   * (which would turn a full pass into an immediate one, for ever), a negative trim grace (which would
   * delete still-live holds), or a zero ttl ceiling (which would clamp every lease to nothing). Every
   * violation is reported, not just the first, matching the parse above.
   *
   * @param config what the file parsed to
   * @return the same configuration; aborts with `Misconfigured` naming every bound that is broken
   */
  private def bounded(config: QueueConfig): IO[ApplicationError, QueueConfig] =
    val problems = Chunk(
      // Not a bound but the same kind of refusal: the wake listener reads every wake stream in one XREAD,
      // which Redis Cluster rejects across slots — so cluster mode would silently degrade into polling.
      // Refused until the listener reads per slot, against a real cluster fixture (redis-cluster.md).
      Option.when(config.cluster)(
        "cluster = true is not yet supported: the wake listener reads all wake streams in one XREAD, " +
          "which Redis Cluster rejects across slots. See docs/architecture/redis-cluster.md"
      ),
      Option.when(config.leaseTtl.toMillis <= 0)("lease-ttl must be greater than zero"),
      Option.when(config.sweepInterval.toMillis <= 0)("sweep-interval must be greater than zero"),
      Option.when(config.sweepLimit <= 0)("sweep-limit must be greater than zero"),
      Option.when(config.lockTrimInterval.toMillis <= 0)("lock-trim-interval must be greater than zero"),
      Option.when(config.lockTrimGrace.toMillis < 0)("lock-trim-grace must not be negative"),
      Option.when(config.lockMaxTtl.toMillis <= 0)("lock-max-ttl must be greater than zero"),
      Option.when(config.wakeBlock.toMillis <= 0)("wake-block must be greater than zero"),
      Option.when(config.maxWait.toMillis <= 0)("max-wait must be greater than zero"),
      Option.when(config.maxBatchLimit <= 0)("max-batch-limit must be greater than zero"),
    ).flatten
    if problems.isEmpty then ZIO.succeed(config)
    else ZIO.fail(Misconfigured(s"config/queue.conf is invalid: ${problems.mkString("; ")}"))
