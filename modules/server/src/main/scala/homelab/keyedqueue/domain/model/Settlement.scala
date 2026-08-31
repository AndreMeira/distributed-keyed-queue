package homelab.keyedqueue.domain.model


import homelab.keyedqueue.domain.model.Settlement.Outcome
import homelab.keyedqueue.domain.types.{ MessageId, Verdict }
import zio.{ Duration, NonEmptyChunk }


/**
 * What a consumer reports back about the messages it took: which claim, what became of which, and whether
 * the key should wait before anyone works it again.
 *
 * '''One type rather than three arguments.''' The three only mean anything together — outcomes name
 * messages that belong to *this* claim, and the backoff applies to the key *that claim holds* — so a
 * signature that took them separately could be called with three unrelated values.
 *
 * '''Non-empty by construction.''' A settle that names nothing is a round trip that decides nothing: the
 * claim stays exactly as owed as it was. Making it unrepresentable here is what lets the store treat every
 * settle as an event that moves something.
 *
 * @param claimed the claim being settled against, and the token that authorises it
 * @param outcomes what became of each message named. A claim may be settled piece by piece: what is not
 *                 named here stays owed, and the claim ends — releasing the key — once nothing is
 * @param retryAfter how long the key should wait before anyone works it again, when a failure asked for it;
 *                   `None` is "as soon as it is free", and several waits in one claim leave the longest
 *                   standing
 */
final case class Settlement(
  claimed: Claim,
  outcomes: NonEmptyChunk[Outcome],
  retryAfter: Option[Duration],
)


object Settlement:

  /**
   * What became of one message of a batch.
   *
   * @param messageId which message, as the delivery named it
   * @param verdict whether it is done with, or is to be tried again
   */
  final case class Outcome(messageId: MessageId, verdict: Verdict)
