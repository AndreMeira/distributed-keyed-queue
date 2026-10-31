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
   * Only the wait for the claim and the logic are interruptible. A claim that has reached this consumer is
   * therefore always held and settled, even when an interrupt arrives with it: the logic is interrupted at
   * once and its messages come back. The settle runs on every exit, so a consumer stopped mid-batch still
   * says what became of it and stops renewing it.
   *
   * @param logic what to run on the messages
   * @tparam E2 what the logic aborts with, alongside this client's own failures
   * @return noop once the batch is settled, or once the wait elapsed with nothing to take; aborts with
   *         what the logic aborted with, or with a [[ServiceError]] when a call does not land
   */
  override def consume[E2 >: ServiceError](logic: List[Message.Incoming] => IO[E2, Unit]): IO[E2, Unit] =
    ZIO.uninterruptibleMask: restore =>
      restore(client.dequeue(conf.queue, conf.patience, conf.size)).flatMap {
        case Dequeued.Idle           => ZIO.unit
        case Dequeued.Claimed(claim) =>
          val messages = claim.messages.toList
          heartbeat.hold(claim) *> restore(logic(messages))
            .onExit(exit => settle(claim, worked(exit, messages), backoff(exit)))
      }

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
   * How long the key waits before what the logic did not finish is delivered again.
   *
   * Work that was interrupted did not fail, it was stopped, so it is handed back at once. Only a logic that
   * ended on its own without answering waits out the configured backoff.
   *
   * @param exit how the logic ended
   * @return no wait when it was interrupted, else the configured backoff
   */
  private def backoff(exit: Exit[?, Unit]): Duration =
    if exit.isInterrupted then Duration.Zero else conf.retryAfter

  /**
   * Report every outcome at once and stop holding the claim.
   *
   * One call, because `v1.SettleRequest` states outcomes per message and a claim ends when nothing it owns is left
   * owed. A settle that does not land is left alone: the claim lapses on its own lease and the messages
   * are delivered again, which is the path a claim takes when its consumer dies.
   *
   * @param claim what owns the messages
   * @param verdicts what became of each of them
   * @param retryAfter how long the key waits before anything failed is delivered again
   * @return noop once they have been reported, or once reporting failed
   */
  private def settle(claim: Claim, verdicts: List[Verdict], retryAfter: Duration): UIO[Unit] =
    client.settle(claim.receipt, verdicts.toChunk, retryAfter).ignore *> heartbeat.release(claim.receipt)
