package homelab.keyedqueue.domain.model.queue

import homelab.keyedqueue.domain.types.*


/**
 * What a consumer must hand back to settle or renew: which key, in which queue, under which claim.
 *
 * Handed to a consumer as an opaque [[Claim.Ref]] so it cannot reason about the fencing scheme or do
 * arithmetic on it. Forgery is not a threat model: the store validates the token against the key's current
 * generation, so a made-up reference buys nothing a guessed one would not.
 *
 * @param queue the queue the message was taken from
 * @param key the key being held
 * @param token the generation of this particular claim
 */
final case class Claim(queue: QueueName, key: MessageKey, token: Token):

  /**
   * Encode as the opaque string a consumer carries.
   *
   * The fields go through [[Obfuscated]], so a queue name or a key may hold anything.
   *
   * @return the reference
   */
  def reference: Claim.Ref =
    Claim.Ref(Obfuscated.encode(queue, key, token.toString))


object Claim:

  /** The opaque handle a consumer holds while it works a message: a [[Claim]] it cannot read. */
  opaque type Ref <: String = String

  object Ref:

    /**
     * A handle, trusted.
     *
     * @param value the encoded handle
     * @return the handle
     */
    def apply(value: String): Ref = value

  /**
   * Read back a reference handed out by [[Claim.reference]].
   *
   * Takes a `String` rather than a [[Claim.Ref]]: what a consumer hands back is an unchecked value, and this
   * is the mechanics of deciding whether it is a receipt. `Claim.Ref` is what this service *hands out* —
   * evidence of a claim it granted — so requiring one here would mean minting it before the check.
   *
   * Answers with an `Option` rather than refusing, because its two callers disagree about what a failure
   * means: settle's validator turns `None` into an input problem, while a heartbeat lists an unreadable
   * receipt among the claims the consumer has lost — refusing the whole call there would cost it the
   * renewals that were good.
   *
   * @param reference the opaque string from the consumer
   * @return the claim it names, or `None` when it is not one we issued
   */
  def decode(reference: String): Option[Claim] =
    Obfuscated(reference).decoded match
      case Some(Seq(queue, key, token)) =>
        for generation <- token.toLongOption
        yield Claim(QueueName(queue), MessageKey(key), Token(generation))
      case _                            => None
