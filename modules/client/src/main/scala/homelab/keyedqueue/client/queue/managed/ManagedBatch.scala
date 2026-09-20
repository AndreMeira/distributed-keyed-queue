package homelab.keyedqueue.client.queue.managed


import homelab.common.messaging.Consumer
import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.model.*
import homelab.keyedqueue.client.queue.{ Provider, QueueClient }
import zio.*
import zio.prelude.*


/**
 * One key's messages, handed to a caller's logic together.
 *
 * They share the logic's outcome: it answered, so every message is done, or it did not, so every message
 * comes back — including the ones it had already worked. That is the trade a batch makes for a single
 * round trip, and a caller that needs to mark three of ten done and the rest failed wants [[QueueClient]]
 * underneath, where an outcome is stated per message.
 *
 * Messages arrive as they travelled. Reading one is the caller's, through `map` or `mapZIO`, which is what
 * leaves the choice about a message that will not read where the caller can make it.
 *
 * @param client what the calls are made through
 * @param heartbeat what keeps the claim alive while the logic runs
 * @param conf which queue it reads, how many at once, and how it waits and retries
 */
final private[queue] class ManagedBatch(
  client: QueueClient,
  heartbeat: Heartbeat,
  conf: Provider.BatchConsumerConfig,
) extends Consumer.Batched[ServiceError, Message.Incoming]:

  /**
   * Claim a key's messages and work them together, or answer with nothing when none became ready.
   *
   * @param logic what to run on the messages
   * @tparam E2 what the logic aborts with, alongside this client's own failures
   * @return noop once the batch is settled, or once the wait elapsed with nothing to take; aborts with
   *         what the logic aborted with, or with a [[ServiceError]] when a call does not land
   */
  override def consume[E2 >: ServiceError](logic: List[Message.Incoming] => IO[E2, Unit]): IO[E2, Unit] =
    client.dequeue(conf.queue, conf.patience, conf.size).flatMap {
      case Dequeued.Idle           => ZIO.unit
      case Dequeued.Claimed(claim) => holding(claim, logic)
    }

  /**
   * Hold a claim for as long as its messages are being worked.
   *
   * Taking the claim and settling it are both outside the caller's interruption, and the settle runs on
   * every exit, so a consumer stopped mid-batch still says what became of it and stops renewing it.
   *
   * @param claim what was granted
   * @param logic what to run on the messages
   * @tparam E2 what the logic aborts with
   * @return noop once the batch is settled
   */
  private def holding[E2 >: ServiceError](
    claim: Claim,
    logic: List[Message.Incoming] => IO[E2, Unit],
  ): IO[E2, Unit] =
    ZIO.uninterruptibleMask: restore =>
      val messages = claim.messages.toList
      heartbeat.hold(claim) *> restore(logic(messages)).onExit(exit => settle(claim, worked(exit, messages)))

  /**
   * What became of the messages the logic was given.
   *
   * @param exit how the logic ended
   * @param messages the claim's messages
   * @return a verdict for each, all of them the same
   */
  private def worked(exit: Exit[?, Unit], messages: List[Message.Incoming]): List[Verdict] =
    messages.map(message => Verdict(message.id, outcome(exit)))

  /**
   * The outcome an ending amounts to.
   *
   * Anything but an answer is failed, so work that was interrupted comes back rather than counting as
   * done.
   *
   * @param exit how the logic ended
   * @return the outcome to settle a message as
   */
  private def outcome(exit: Exit[?, Unit]): Verdict.Outcome =
    if exit.isSuccess
    then Verdict.Outcome.Done
    else Verdict.Outcome.Failed

  /**
   * Report every outcome at once and stop holding the claim.
   *
   * One call, because the wire states outcomes per message and a claim ends when nothing it owns is left
   * owed. A settle that does not land is left alone: the claim lapses on its own lease and the messages
   * are delivered again, which is the path a claim takes when its consumer dies.
   *
   * @param claim what owns the messages
   * @param verdicts what became of each of them
   * @return noop once they have been reported, or once reporting failed
   */
  private def settle(claim: Claim, verdicts: List[Verdict]): UIO[Unit] =
    client.settle(claim.receipt, verdicts.toChunk, conf.retryAfter).ignore *> heartbeat.release(claim.receipt)
