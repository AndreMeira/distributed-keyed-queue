package homelab.keyedqueue.client.queue


import homelab.common.messaging.Consumer
import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.Provider.DecodingPolicy
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
 * One message nobody can read keeps the whole claim from the logic: the batch is answered for together,
 * so it is read together. Only that message is settled as the policy says — the ones that read are owed
 * work nobody did, so they come back.
 *
 * @param client what the calls are made through
 * @param heartbeat what keeps the claim alive while the logic runs
 * @param decoder what turns a message into a value
 * @param conf which queue it reads, how many at once, and how it waits, retries and refuses
 * @tparam A what the logic is given
 */
final private[queue] class ManagedBatch[A: MessageDecoder as decoder](
  client: QueueClient,
  heartbeat: Heartbeat,
  conf: Provider.BatchConsumerConfig,
) extends Consumer.Batched[ServiceError, A]:

  /**
   * Claim a key's messages and work them together, or answer with nothing when none became ready.
   *
   * @param logic what to run on the values
   * @tparam E2 what the logic aborts with, alongside this client's own failures
   * @return noop once the batch is settled, or once the wait elapsed with nothing to take; aborts with
   *         what the logic aborted with, or with a [[ServiceError]] when a call does not land
   */
  override def consume[E2 >: ServiceError](logic: List[A] => IO[E2, Unit]): IO[E2, Unit] =
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
   * @param logic what to run on the values
   * @tparam E2 what the logic aborts with
   * @return noop once the batch is settled
   */
  private def holding[E2 >: ServiceError](claim: Claim, logic: List[A] => IO[E2, Unit]): IO[E2, Unit] =
    ZIO.uninterruptibleMask: restore =>
      heartbeat.hold(claim) *> {
        val messages = claim.messages.toList
        decoded(messages) match
          case Left(unreadable) => settling(claim, refused(unreadable.toSet, messages))
          case Right(values)    => restore(logic(values)).onExit(exit => settling(claim, processed(exit, messages)))
      }

  /**
   * The values a claim's messages read as, when every one of them does.
   *
   * All or nothing, like the outcome: a batch is answered for together, so one message nobody can read
   * keeps the whole claim from the logic rather than handing over the part of it that read.
   *
   * @param messages the claim's messages, in the order they were handed over
   * @return the values in that order, or the name of every message that did not read
   */
  private def decoded(messages: List[Message.Incoming]): Either[List[MessageId], List[A]] =
    val (left, right) = messages.partitionMap: message =>
      decoder.decode(message) match
        case Right(value) => Right(value)
        case Left(_)      => Left(message.id)
    if left.nonEmpty then Left(left) else Right(right)

  /**
   * What became of the messages the logic was given.
   *
   * @param exit how the logic ended, which every message of the batch shares
   * @param messages the claim's messages
   * @return a verdict for each, all of them the same
   */
  private def processed(exit: Exit[?, Unit], messages: List[Message.Incoming]): List[Verdict] =
    messages.map: message =>
      Verdict(message.id, outcome(exit))

  /**
   * What became of the messages in a batch the logic was never given.
   *
   * The policy answers for the ones that could not be read; the rest were readable and are owed work
   * nobody did, so they come back whatever the policy says about their neighbours.
   *
   * @param unreadable the names of this claim's messages that did not read
   * @param messages the claim's messages, each carrying how often it has been tried
   * @return a verdict for each
   */
  private def refused(unreadable: Set[MessageId], messages: List[Message.Incoming]): List[Verdict] =
    messages.map: message =>
      if unreadable.contains(message.id)
      then Verdict(message.id, outcome(message))
      else Verdict(message.id, Verdict.Outcome.Failed)

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
   * What becomes of a message in a claim that did not read.
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
  private def settling(claim: Claim, verdicts: List[Verdict]): UIO[Unit] =
    client.settle(claim.receipt, verdicts.toChunk, conf.retryAfter).ignore
      *> heartbeat.release(claim.receipt)
