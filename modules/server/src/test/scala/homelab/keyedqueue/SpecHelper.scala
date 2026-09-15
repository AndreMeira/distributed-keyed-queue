package homelab.keyedqueue


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.model.{ Acquisition, Claim, Demand, Grant, Message, Settlement }
import homelab.keyedqueue.domain.model.Message.Encoding
import homelab.keyedqueue.domain.model.Settlement.Verdict
import homelab.keyedqueue.domain.request.v1.EnqueueRequest
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.infrastructure.redis.Connection
import homelab.keyedqueue.v1
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

    /** A message as it arrives over the wire, with whatever key the test is about. */
    def wireMessage(key: String, body: String): v1.Message =
      v1.Message(
        key = key,
        messageId = s"$key-$body",
        payloadType = "test.Message/v1",
        encoding = v1.Encoding.ENCODING_JSON,
        payload = com.google.protobuf.ByteString.copyFromUtf8(body),
      )

    /** One message's outcome, by id. */
    def done(id: String): v1.MessageOutcome = v1.MessageOutcome(id, v1.Outcome.OUTCOME_DONE)

    /** Settle every message a claim handed over, with the same verdict. */
    def settle(reply: v1.DequeueResponse, outcome: v1.Outcome): v1.SettleRequest =
      v1.SettleRequest(reply.receipt, outcomes = claimed(reply).map(d => v1.MessageOutcome(d.messageId, outcome)))

    /** Everything a claim handed over, head first — the shape a consumer actually iterates. */
    def claimed(reply: v1.DequeueResponse): Seq[v1.Delivery] = reply.head.toSeq ++ reply.tail

    /** What a claim is carrying, as text, in the order it was handed over. */
    def bodies(reply: v1.DequeueResponse): Seq[String] =
      claimed(reply).flatMap(_.message).map(_.payload.toStringUtf8)

    /** A message as a request carries it, before anything has checked it. */
    def requestMessage(key: String, messageId: String = "m1"): EnqueueRequest.Message =
      EnqueueRequest.Message(key, messageId, payloadType = "test.Text/v1", Encoding.Json, None, Chunk.empty)

    /**
     * Run a layout effect the way boot does: on the suite's connection.
     *
     * @param effect what to run against the store
     * @return what the effect returns
     */
    def boot[A](effect: ZIO[Connection.Commands, Any, A]): ZIO[Connection, Any, A] =
      ZIO.serviceWithZIO[Connection](_.provide(effect))
  }

  object Failure {
    trait TestRuntimeError extends ApplicationError

    def apply(msg: String): TestRuntimeError = new TestRuntimeError:
      override def message: String = msg
  }
}
