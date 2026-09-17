package homelab.keyedqueue.domain.model.lock

import homelab.keyedqueue.domain.types.*


/**
 * What a consumer hands back to release or refresh a lock: which lock, under which fence generation.
 *
 * The lock's counterpart to [[homelab.keyedqueue.domain.model.queue.Claim]] — same opaque-handle idea,
 * without the queue/key split a lock has no use for. The receipt is handed out so a caller carries a token it cannot reason about; the fence token
 * itself is exposed separately, on purpose, for stamping downstream writes (see the lock service).
 *
 * @param name the lock held
 * @param token the generation this hold was granted under
 */
final case class Claim(name: LockName, token: Token):

  /**
   * Encode as the opaque handle a caller carries.
   *
   * The fields go through [[Obfuscated]], so a lock name may hold anything.
   *
   * @return the handle
   */
  def reference: Claim.Ref = Claim.Ref {
    Obfuscated.encode(name, token.toString)
  }


object Claim:

  /** The opaque handle a caller holds while it holds the lock: a [[Claim]] it cannot read. */
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
   * Read a handle back to the claim it names.
   *
   * Takes a `String` rather than a [[Claim.Ref]]: what a consumer hands back is an unchecked value, and this
   * is the mechanics of deciding whether it is one this service issued.
   *
   * @param reference the opaque handle from an acquire
   * @return the claim it names, or `None` when it is not one we issued
   */
  def decode(reference: String): Option[Claim] =
    Obfuscated(reference).decoded match
      case Some(Seq(name, token)) =>
        for generation <- token.toLongOption
        yield Claim(LockName(name), Token(generation))
      case _                      => None
