package homelab.keyedqueue.domain.request.v1


import homelab.keyedqueue.domain.model.Settlement.Verdict
import homelab.keyedqueue.domain.types.MessageId
import zio.{ Chunk, Duration }


/**
 * A consumer's report of what became of some of what a claim owns.
 *
 * @param receipt the handle from the delivery, as it arrived
 * @param outcomes what became of each message named. What is not named stays owed, and the claim ends
 *                 once nothing is
 * @param retryAfter how long the key should wait before anyone works it again, asked for by a nack; zero
 *                   is no wait
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
