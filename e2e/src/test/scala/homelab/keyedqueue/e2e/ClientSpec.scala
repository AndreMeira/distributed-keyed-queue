package homelab.keyedqueue.e2e


import homelab.keyedqueue.client.{ Endpoint, ServiceError }
import homelab.keyedqueue.client.lock.{ DistributedLock, LockClient }
import homelab.keyedqueue.client.queue.model.*
import homelab.keyedqueue.client.queue.{ MessageDecoder, MessageEncoder, Provider, QueueClient }
import zio.*
import zio.schema.{ DeriveSchema, Schema }
import zio.test.*


/**
 * The client as a consumer uses it, against the deployment.
 *
 * What only this suite can show: that the lease a caller never asked about is kept, and that a value sent
 * through a producer comes back through a consumer as the same value. Everything below it runs against
 * fakes and in-process channels, which cannot be wrong about the service and cannot be right about it
 * either.
 */
object ClientSpec extends ZIOSpecDefault:

  final case class Order(id: String, customer: String, lines: Int)

  private given Schema[Order]         = DeriveSchema.gen[Order]
  private given MessageEncoder[Order] = MessageEncoder.deriveAs[Order]("order.v2")
  private given MessageDecoder[Order] = MessageDecoder.deriveAs[Order]("order.v2")

  /** What the deployment is configured with, so a test can outlive it deliberately. */
  private val lease = 5.seconds

  /**
   * A client for each half, pointed at one instance and closed with the scope.
   *
   * @param at which instance to dial
   * @return the two clients
   */
  private def clients(at: Instance): ZIO[Scope, ServiceError, (QueueClient, LockClient)] =
    val endpoint = Endpoint(at.host, at.port)
    for
      queue <- QueueClient.scoped(endpoint)
      lock  <- LockClient.scoped(endpoint)
    yield (queue, lock)

  /**
   * How this consumer reads the queue: a short wait, and a beat well inside the lease.
   *
   * @param queue which queue to take from
   * @return the configuration
   */
  private def consuming(queue: String): Provider.ConsumerConfig =
    Provider.ConsumerConfig(queue, patience = 5.seconds)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("the client, against the deployment")(
    test("a value sent through a producer comes back through a consumer as the same value") {
      for
        dkq            <- ZIO.service[Deployment]
        name            = dkq.queue("client-roundtrip")
        (queue, _)     <- clients(dkq.a)
        producer       <- Provider(queue).producerWith[Order](name)(order =>
                            MessageId(order.id) -> MessageKey(order.customer)
                          )
        consumer       <- Provider(queue).consumer[Order](consuming(name))
        _              <- producer.emit(Order("o-1", "c-9", lines = 3))
        worked         <- Ref.make(List.empty[Order])
        _              <- consumer.consume(order => worked.update(_ :+ order))
        seen           <- worked.get
        // settled, so nothing is left for a second call to find
        again          <- consumer.consume(order => worked.update(_ :+ order))
        afterSettling  <- worked.get
      yield assertTrue(seen == List(Order("o-1", "c-9", lines = 3)), afterSettling == seen)
    },
    test("a handler that outlives the lease keeps its claim, and its settle is accepted") {
      // The obligation the guide used to hand to the reader: a claim expires unless renewed. Nothing here
      // renews it by hand, and the work runs for three leases.
      for
        dkq        <- ZIO.service[Deployment]
        name        = dkq.queue("client-heartbeat")
        (queue, _) <- clients(dkq.a)
        producer   <- Provider(queue).producerWith[Order](name)(order =>
                        MessageId(order.id) -> MessageKey(order.customer)
                      )
        consumer   <- Provider(queue).consumer[Order](consuming(name))
        _          <- producer.emit(Order("slow", "c-1", lines = 1))
        _          <- consumer.consume(_ => ZIO.sleep(lease.multipliedBy(3)))
        // a claim that lapsed would have been redelivered; a settle that was refused would leave it queued
        redelivered <- Ref.make(false)
        _          <- consumer.consume(_ => redelivered.set(true))
        cameBack   <- redelivered.get
      yield assertTrue(!cameBack)
    } @@ TestAspect.timeout(2.minutes),
    test("a lock held through the managed form is refused to everybody else while the effect runs") {
      for
        dkq            <- ZIO.service[Deployment]
        name            = dkq.lock("client-exclusive")
        (_, mine)      <- clients(dkq.a)
        (_, theirs)    <- clients(dkq.b)
        inside         <- Promise.make[Nothing, Unit]
        finish         <- Promise.make[Nothing, Unit]
        held           <- DistributedLock(mine)
                            .acquire(name, ttl = 30.seconds, maxWait = 5.seconds)(inside.succeed(()) *> finish.await)
                            .fork
        _              <- inside.await
        refused        <- DistributedLock(theirs).tryAcquire(name, ttl = 30.seconds)(ZIO.unit)
        _              <- finish.succeed(())
        _              <- held.join
        // and given back afterwards, so the other instance can have it
        afterwards     <- DistributedLock(theirs).tryAcquire(name, ttl = 30.seconds)(ZIO.succeed("mine now"))
      yield assertTrue(refused.isEmpty, afterwards.contains("mine now"))
    },
    test("a hold that outlives its lease is kept alive, and the lock is still ours") {
      // Two leases' worth of work under a lease the service will clamp to nothing longer. Nothing here
      // refreshes by hand.
      for
        dkq         <- ZIO.service[Deployment]
        name         = dkq.lock("client-renewed")
        (_, mine)   <- clients(dkq.a)
        (_, theirs) <- clients(dkq.b)
        inside      <- Promise.make[Nothing, Unit]
        held        <- DistributedLock(mine)
                         .acquire(name, ttl = 2.seconds, maxWait = 5.seconds)(
                           inside.succeed(()) *> ZIO.sleep(6.seconds).as("finished")
                         )
                         .fork
        _           <- inside.await
        _           <- ZIO.sleep(4.seconds) // twice the ttl asked for
        refused     <- DistributedLock(theirs).tryAcquire(name, ttl = 2.seconds)(ZIO.unit)
        answer      <- held.join
      yield assertTrue(refused.isEmpty, answer.contains("finished"))
    } @@ TestAspect.timeout(2.minutes),
  ).provideShared(Deployment.layer, Scope.default)
    @@ TestAspect.withLiveClock
    @@ TestAspect.sequential
    @@ TestAspect.timeout(5.minutes)
