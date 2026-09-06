package homelab.keyedqueue.domain.request.v1


import homelab.keyedqueue.domain.model.Settlement.Verdict
import homelab.keyedqueue.domain.types.MessageId
import zio.{ Chunk, Duration }


/**
 * Report what happened to some of what a claim owns.
 *
 * '''Nothing here is a domain type, and that is the point.''' A `ClaimRef` or a `MessageId` here would
 * assert that someone checked a value nobody has looked at. Raw strings make no such claim, so the only way
 * to obtain the evidence is [[homelab.keyedqueue.domain.service.validation.QueueInputValidation]], which is
 * what turns this into a `Settlement`. `Verdict` is the exception: it is total on arrival, so it asserts
 * nothing.
 *
 * @param receipt the handle from the delivery, as it arrived
 * @param outcomes what became of each message named. What is not named stays owed, and the claim ends
 *                 once nothing is
 * @param retryAfter how long the key should wait before anyone works it again, asked for by a nack; zero
 *                   is what an absent duration decodes to
 */
final case class SettleRequest(
  receipt: String,
  outcomes: Chunk[SettleRequest.MessageOutcome],
  retryAfter: Duration,
)


object SettleRequest:

  /**
   * What a consumer did with one message of its batch.
   *
   * Keeps its full name: it is a part of a request rather than one of them, so there is no suffix to drop
   * and nothing to gain from calling it `Outcome`, which is already what the verdict enum answers.
   *
   * @param messageId which message, as the delivery named it — a name, not yet a [[MessageId]]
   * @param outcome what became of it
   */
  final case class MessageOutcome(messageId: String, outcome: Verdict)
