package homelab.keyedqueue.domain.model


import homelab.keyedqueue.domain.types.*

import java.nio.charset.StandardCharsets
import java.util.Base64


/**
 * What a consumer must hand back to settle or renew: which key, in which queue, under which claim.
 *
 * Travels over the wire as an opaque *receipt* so a consumer cannot reason about the fencing scheme, or do
 * arithmetic on it. Forgery is not a threat model here: the store validates the token against the key's
 * current generation, so a made-up receipt buys nothing that a guessed one would not.
 *
 * @param queue the queue the message was taken from
 * @param key the key being held
 * @param token the generation of this particular claim
 */
final case class ClaimRef(queue: QueueName, key: MessageKey, token: Token):

  /**
   * Encode as the opaque string a consumer carries.
   *
   * Base64url over a space-separated triple: queue names and keys may contain anything, and base64 of the
   * whole thing keeps the separator out of reach of their content.
   *
   * @return the receipt
   */
  def receipt: String =
    val raw = queue + " " + key + " " + token.toString
    Base64.getUrlEncoder.withoutPadding.encodeToString(raw.getBytes(StandardCharsets.UTF_8))


object ClaimRef:

  /**
   * Read back a receipt handed out by [[ClaimRef.receipt]].
   *
   * @param receipt the opaque string from the consumer
   * @return the claim it names, or `None` when it is not one we issued
   */
  def fromReceipt(receipt: String): Option[ClaimRef] =
    scala.util
      .Try(String(Base64.getUrlDecoder.decode(receipt), StandardCharsets.UTF_8))
      .toOption
      .map(_.split(' '))
      .collect:
        case Array(queue, key, token) if token.toLongOption.isDefined =>
          ClaimRef(QueueName(queue), MessageKey(key), Token(token.toLong))
