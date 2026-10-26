package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.SpecHelper.Helper
import homelab.keyedqueue.domain.types.{ MessageKey, QueueName }
import homelab.keyedqueue.infrastructure.redis.keys.KeyLayout
import homelab.keyedqueue.infrastructure.redis.script.QueueScripts
import zio.*
import zio.test.*


/**
 * The shared connection answers through futures, so a call in flight holds no thread. What it must keep from
 * the synchronous client it replaced: a call to a server that stops answering fails after the command timeout
 * rather than hanging, and a call already sent is waited for even when its caller is interrupted.
 *
 * Redis is frozen with `CLIENT PAUSE`, issued on a partition connection so the shared one is free to be
 * measured.
 */
object ConnectionSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("Connection — the shared connection")(
      test("a call to a server that stops answering fails after the command timeout instead of hanging") {
        for
          connection <- ZIO.service[Connection]
          scripts    <- connection.provide(QueueScripts.make)
          _          <- pause(connection, 3.seconds)
          started    <- Clock.nanoTime
          outcome    <- connection.provide(scripts.enqueue.execute(enqueued("frozen"))).either
          ended      <- Clock.nanoTime
          _          <- ZIO.sleep(1500.millis) // let the pause run out, so the next test finds the server answering
        yield assertTrue(outcome.isLeft, Duration.fromNanos(ended - started) < 2900.millis)
      },
      test("an interrupted call still waits for its reply, and what it did lands") {
        for
          connection <- ZIO.service[Connection]
          scripts    <- connection.provide(QueueScripts.make)
          _          <- pause(connection, 1.second)
          call       <- connection.provide(scripts.enqueue.execute(enqueued("interrupted"))).fork
          _          <- ZIO.sleep(100.millis)
          started    <- Clock.nanoTime
          exit       <- call.interrupt
          ended      <- Clock.nanoTime
          ready      <- connection.provide(Connection.use(redis => ZIO.fromCompletionStage(redis.zcard(keys("interrupted").ready))))
        yield assertTrue(exit.isInterrupted, Duration.fromNanos(ended - started) >= 500.millis, ready == 1L)
      },
    ).provideShared(connection) @@ TestAspect.sequential @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)

  private object Support:

    /** One partition: the specs here need a partition connection only to pause the server from. */
    val layout: KeyLayout = KeyLayout(partitions = 1)

    /**
     * Connections to a throwaway server, with a command timeout of two seconds on the shared one.
     *
     * @return the layer
     */
    val connection: ZLayer[Any, ApplicationError, Connection] =
      RedisSpecSupport.container >>> ZLayer.scoped:
        ZIO.serviceWithZIO[Container.Type](container =>
          Connection.make(Connection.Config(2.seconds, Helper.redisUrl(container), cluster = false), layout)
        )

    /**
     * Stop the server answering any client for `span`, from a partition connection.
     *
     * @param connection the connections
     * @param span how long the server stops answering
     * @return noop once the pause is in place
     */
    def pause(connection: Connection, span: Duration): IO[Any, Unit] =
      ZIO
        .fromOption(layout.partitionIds.headOption)
        .flatMap: partition =>
          connection.provideBlocking(partition):
            Connection.useBlocking(redis => ZIO.attemptBlocking(redis.clientPause(span.toMillis)).unit)

    /**
     * The keys of a queue used by one test.
     *
     * @param queue the queue's name
     * @return its keys
     */
    def keys(queue: String) = layout.queue(QueueName(queue))

    /**
     * What enqueuing one message on `queue` sends.
     *
     * @param queue the queue's name
     * @return the script's input
     */
    def enqueued(queue: String) = (keys = keys(queue), message = Helper.message(MessageKey("k1"), "body"))
