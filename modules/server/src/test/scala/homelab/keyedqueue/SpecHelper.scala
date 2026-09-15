package homelab.keyedqueue


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.model.{ Acquisition, Claim, Demand, Grant, Message, Settlement }
import homelab.keyedqueue.domain.model.Message.Encoding
import homelab.keyedqueue.domain.model.Settlement.Verdict
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import org.testcontainers.containers.GenericContainer
import zio.*
import zio.test.{ TestAspect, TestAspectPoly }


object SpecHelper {

  object Aspect {

    /**
     * The conditions a suite that needs sequential execution and timeout
     */
    val common: TestAspectPoly =
      TestAspect.withLiveClock >>> TestAspect.sequential >>> TestAspect.timeout(3.minutes)
  }

  object Helper {

    /**
     *
     * @param container
     * @return
     */
    def redisUrl(container: GenericContainer[?]): String =
      s"redis://${container.getHost}:${container.getMappedPort(6379)}"

    /**
     *
     * @param redis
     * @param leaseTtl
     * @param port
     * @return
     */
    def config(redis: String, leaseTtl: Duration = 10.seconds, port: Int = 0): QueueConfig =
      QueueConfig(
        redisUrl = redis,
        cluster = false,
        port = port,
        leaseTtl = leaseTtl,
        sweepInterval = 1.second,
        sweepLimit = 100,
        lockTrimInterval = 120.seconds,
        lockTrimGrace = 10.minutes,
        lockMaxTtl = 10.minutes,
        wakeBlock = 200.millis,
        maxWait = 5.seconds,
        maxBatchLimit = 32,
      )

    /**
     * An acquisition for a name — patience defaults to none, which `tryAcquire` ignores anyway.
     */
    def acq(name: String, ttl: Duration, patience: Duration = Duration.Zero): Acquisition =
      Acquisition(LockName(name), ttl, patience)

    /**
     * A message whose cargo is `body`: these tests care about order and ownership, not about content.
     */
    def message(key: MessageKey, body: String): Message =
      Message(key, MessageId(body), "test.Text", Encoding.Json, None, Chunk.fromArray(body.getBytes("UTF-8")))

    /**
     * the payload of the message as a string
     */
    def cargo(message: Message): String =
      String(message.payload.toArray, "UTF-8")

    /**
     * What a batch is carrying, as text, in the order it was handed over.
     */
    def body(batch: Grant): Chunk[String] =
      batch.messages.map(owned => cargo(owned.message)).toChunk

    /**
     * Acknowledge everything a batch owns. Non-empty, because a batch is.
     */
    def acks(batch: Grant): NonEmptyChunk[(MessageId, Verdict)] =
      batch.messages.map(_.id -> Verdict.Done)

    /**
     * What the use case builds before it calls the port, in the spec's own vocabulary.
     *
     * @param claim the claim being settled against
     * @param outcomes what became of each message named
     * @param retryAfter how long the key should wait; zero for "as soon as it is free"
     * @return the settlement to hand the store
     */
    def settlement(
      claim: Claim,
      outcomes: NonEmptyChunk[(MessageId, Verdict)],
      retryAfter: Duration = Duration.Zero,
    ): Settlement =
      Settlement(
        claim,
        outcomes.map((id, verdict) => Settlement.Outcome(id, verdict)),
        Option.when(retryAfter.toMillis > 0)(retryAfter),
      )

    /**
     * Claim exactly one message, for the tests that are not about batching.
     */
    def one(store: QueueStore, queue: QueueName): ZIO[Any, ApplicationError, Option[Grant]] =
      store.claim(Demand(queue, 2.seconds, 1))

    /**
     * Acknowledge a single-message batch and report what it was carrying.
     */
    def ack(store: QueueStore, queue: QueueName)(batch: Option[Grant]): ZIO[Any, ApplicationError, String] =
      ZIO
        .foreach(batch): one =>
          store.settle(settlement(one.claim, acks(one))).as(body(one).mkString)
        .map(_.getOrElse(""))

    /**
     * Report a whole batch as failed.
     */
    def nack(store: QueueStore)(batch: Grant): ZIO[Any, ApplicationError, Boolean] =
      store.settle(settlement(batch.claim, batch.messages.map(_.id -> Verdict.Failed)))
  }

  object Failure {
    trait TestRuntimeError extends ApplicationError

    def apply(msg: String): TestRuntimeError = new TestRuntimeError:
      override def message: String = msg
  }
}
