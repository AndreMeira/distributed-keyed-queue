package homelab.keyedqueue.e2e


import homelab.keyedqueue.v1.HeartbeatRequest
import homelab.keyedqueue.v1.ZioKeyedQueue.KeyedQueueClient
import io.grpc.ManagedChannelBuilder
import scalapb.zio_grpc.ZManagedChannel
import zio.*


/**
 * The deployment a test talks to: two instances, addressed one at a time.
 *
 * @param a the first instance
 * @param b the second
 * @param run a token unique to this suite run, which keeps its queues clear of every other run's
 */
final case class Deployment(a: Instance, b: Instance, run: String):

  /**
   * A queue name nothing else will use.
   *
   * '''Every test names its queues through this.''' The suite can be pointed at a deployment it does not own
   * and must not wipe, and even its own stack outlives a single run when `DKQ_E2E_KEEP` is set — so leftovers
   * from a previous run would arrive as extra messages with `attempt` already counted, failing tests for a
   * reason that has nothing to do with the code under test. Suffixing every queue per run makes each run's
   * state disjoint without deleting anybody's.
   *
   * @param name what the test calls the queue
   * @return that name, made unique to this run
   */
  def queue(name: String): String = s"$name-$run"

  /** Both instances, in the order the compose file declares them. */
  def both: Chunk[Instance] = Chunk(a, b)

  /**
   * Pick an instance by index, so a loop spreads its work over the whole deployment.
   *
   * The tests use this rather than picking one instance because "both instances see the same queue" is the
   * property under test, not a detail: an enqueue on `a` and a dequeue on `b` must behave as one system.
   *
   * @param index any number; instances are chosen round-robin
   * @return the instance for that index
   */
  def apply(index: Int): Instance = both(math.floorMod(index, both.size))


object Deployment:

  /** Where `docker-compose.e2e.yml` publishes the instances. */
  private val local = Chunk("localhost:9101", "localhost:9102")

  /** Point the suite at an existing deployment — a cluster, a colleague's laptop — instead of composing one. */
  private val endpoints = "DKQ_E2E_ENDPOINTS"

  /** Leave the stack running after the suite, for when a failure needs its logs and its Redis. */
  private val keep = "DKQ_E2E_KEEP"

  /**
   * The deployment, composed if this suite owns it and merely dialled if it does not.
   *
   * '''Two ways in, one suite.''' With `DKQ_E2E_ENDPOINTS` set, nothing is started or stopped and the same
   * tests run against whatever is already deployed — which is the point of an end-to-end suite that can also
   * be pointed at the cluster. Without it, the suite brings its own stack up and takes it down again.
   *
   * @return the deployment, ready to serve; fails when compose fails or an instance never answers
   */
  val layer: ZLayer[Any, Throwable, Deployment] = ZLayer.scoped(make)

  /**
   * Resolve the addresses, dial them, and wait until they answer.
   *
   * @return the deployment; fails when the stack cannot be started, the addresses are unusable, or an
   *         instance is still not answering after the readiness window
   */
  private def make: ZIO[Scope, Throwable, Deployment] =
    for
      external  <- live(endpoints)
      addresses <- external.fold(started.as(local))(parse)
      _         <- ZIO
                     .fail(IllegalArgumentException(s"$endpoints must name exactly two instances, got: ${addresses.mkString(", ")}"))
                     .unless(addresses.size == 2)
      clients   <- ZIO.foreach(addresses)(connect)
      run       <- ZIO.withRandom(Random.RandomLive)(Random.nextInt).map(value => f"${value & 0xffffff}%06x")
      instances  = Compose.instances.zip(addresses).zip(clients).map(Instance(_, _, _))
      deployment = Deployment(instances(0), instances(1), run)
      _         <- ZIO.foreachDiscard(deployment.both)(ready)
    yield deployment

  /**
   * Bring this suite's own stack up, and take it down when the suite ends.
   *
   * @return noop; fails when compose does — most often because the image has not been built
   */
  private def started: ZIO[Scope, Throwable, Unit] =
    ZIO.acquireRelease(Compose.up)(_ =>
      live(keep)
        .flatMap:
          case Some(_) => ZIO.logInfo(s"$keep is set: leaving the ${Compose.project} stack running")
          case None    => Compose.down
    )

  /**
   * Read a variable from the *real* environment.
   *
   * `System.env` alone would read `TestSystem`, which under `ZIOSpecDefault` is an empty map — so every run
   * would silently believe no variables were set and compose its own stack, including when pointed at a
   * deployment it must not touch. `TestAspect.ifEnvNotSet` reads the live environment, so the two would
   * disagree about the same variable.
   *
   * @param name the variable to read
   * @return its value, if it is set
   */
  private def live(name: String): UIO[Option[String]] =
    ZIO.withSystem(System.SystemLive)(System.env(name)).orDie

  /**
   * Read a `host:port,host:port` list.
   *
   * @param text the environment variable's value
   * @return the addresses; fails when an entry is not `host:port`
   */
  private def parse(text: String): Task[Chunk[String]] =
    val entries = Chunk.fromArray(text.split(",")).map(_.trim).filter(_.nonEmpty)
    ZIO
      .foreach(entries): entry =>
        ZIO
          .succeed(entry)
          .filterOrFail(_.split(":").length == 2)(IllegalArgumentException(s"$endpoints entry is not host:port: $entry"))

  /**
   * Open a channel to one address, closed when the suite's scope ends.
   *
   * Plaintext: the deployment under test speaks plaintext, and a suite that quietly did TLS would be testing
   * a different service from the one that ships.
   *
   * @param address `host:port`
   * @return the client; fails when the address cannot be parsed
   */
  private def connect(address: String): ZIO[Scope, Throwable, KeyedQueueClient] =
    val Array(host, port) = address.split(":"): @unchecked
    ZIO
      .attempt(port.toInt)
      .flatMap: number =>
        KeyedQueueClient.scoped(ZManagedChannel(ManagedChannelBuilder.forAddress(host, number).usePlaintext()))

  /**
   * Wait until an instance answers a call.
   *
   * The compose healthcheck only proves the port is bound, and a heartbeat naming nothing is the cheapest
   * call that proves the whole stack behind it answers — it touches no queue and leaves no state.
   *
   * @param instance the instance to wait for
   * @return noop; fails when it is still not answering after the retries
   */
  private def ready(instance: Instance): Task[Unit] =
    ZIO.withClock(Clock.ClockLive):
      instance
        .heartbeat(Nil)
        .retry(Schedule.spaced(500.millis) && Schedule.recurs(40))
        .unit
