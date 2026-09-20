package homelab.keyedqueue.client.queue


import homelab.common.messaging.Consumer
import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.Provider.DecodingPolicy
import zio.*


/**
 * One queue's messages, handed to a caller's logic one at a time.
 *
 * A call claims one message, reads it, runs the logic and settles the outcome: done when the logic
 * answered, failed when it did not, and failed when the call was interrupted, so nothing a consumer took
 * is left owed. The claim is kept alive by the beat while that happens.
 *
 * A message that does not read never reaches the logic — the port has no channel to report one through —
 * so what becomes of it is [[Provider.DecodingPolicy]], chosen where the consumer was built.
 *
 * @param client what the calls are made through
 * @param heartbeat what keeps the claim alive while the logic runs
 * @param decoder what turns a message into a value
 * @param conf which queue it reads, and how it waits, retries and refuses
 * @tparam A what the logic is given
 */
final private[queue] class ManagedConsumer[A: MessageDecoder as decoder](
  client: QueueClient,
  heartbeat: Heartbeat,
  conf: Provider.ConsumerConfig,
) extends Consumer[ServiceError, A]:

  /**
   * Claim one message and work it, or answer with nothing when none became ready.
   *
   * @param logic what to run on the value
   * @tparam E2 what the logic aborts with, alongside this client's own failures
   * @return noop once the message is settled, or once the wait elapsed with nothing to take; aborts with
   *         what the logic aborted with, or with a [[ServiceError]] when a call does not land
   */
  override def consume[E2 >: ServiceError](logic: A => IO[E2, Unit]): IO[E2, Unit] =
    client.dequeue(conf.queue, conf.patience, maxBatch = 1).flatMap {
      case Dequeued.Idle           => ZIO.unit
      case Dequeued.Claimed(claim) => holding(claim, logic)
    }

  /**
   * Hold a claim for as long as its message is being worked.
   *
   * Taking the claim and settling it are both outside the caller's interruption, and the settle runs on
   * every exit, so a consumer stopped mid-message still says what became of it and stops renewing it.
   *
   * @param claim what was granted
   * @param logic what to run on the value
   * @tparam E2 what the logic aborts with
   * @return noop once the message is settled
   */
  private def holding[E2 >: ServiceError](claim: Claim, logic: A => IO[E2, Unit]): IO[E2, Unit] =
    ZIO.uninterruptibleMask: restore =>
      heartbeat.hold(claim) *> {
        val message = claim.messages.head
        decoder.decode(message) match
          case Left(_)      => settling(claim, message, outcome(message))
          case Right(value) => restore(logic(value)).onExit(exit => settling(claim, message, outcome(exit)))
      }

  /**
   * What became of a message the logic was given.
   *
   * Anything but an answer is failed, so work that was interrupted comes back rather than counting as
   * done.
   *
   * @param exit how the logic ended
   * @return the outcome to settle it as
   */
  private def outcome(exit: Exit[?, Unit]): Verdict.Outcome =
    if exit.isSuccess
    then Verdict.Outcome.Done
    else Verdict.Outcome.Failed

  /**
   * What becomes of a message that did not read.
   *
   * @param message what arrived, which carries how often it has been tried
   * @return the outcome to settle it as
   */
  private def outcome(message: Message.Incoming): Verdict.Outcome =
    conf.policy match
      case DecodingPolicy.Retry                                           => Verdict.Outcome.Failed
      case DecodingPolicy.Discard                                         => Verdict.Outcome.Done
      case DecodingPolicy.DiscardAfter(tries) if message.attempt >= tries => Verdict.Outcome.Done
      case DecodingPolicy.DiscardAfter(_)                                 => Verdict.Outcome.Failed

  /**
   * Report one message's outcome and stop holding its claim.
   *
   * A settle that does not land is left alone: the claim lapses on its own lease and the message is
   * delivered again, which is the path a claim takes when its consumer dies.
   *
   * @param claim what owns the message
   * @param message which message
   * @param outcome what became of it
   * @return noop once it has been reported, or once reporting failed
   */
  private def settling(claim: Claim, message: Message.Incoming, outcome: Verdict.Outcome): UIO[Unit] =
    client.settle(claim.receipt, Chunk(Verdict(message.id, outcome)), conf.retryAfter).ignore
      *> heartbeat.release(claim.receipt)
