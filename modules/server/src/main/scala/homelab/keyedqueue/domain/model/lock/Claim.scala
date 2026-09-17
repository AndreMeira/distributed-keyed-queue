package homelab.keyedqueue.domain.model.lock


import homelab.keyedqueue.domain.types.*

import java.nio.charset.StandardCharsets
import java.util.Base64


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
   * Base64url per field, joined with `.`: the alphabet cannot produce a dot, so the separator stays clear
   * of a lock name whatever it contains.
   *
   * @return the handle
   */
  def reference: Claim.Ref =
    Claim.Ref(Seq(name, token.toString).map(Claim.encoded).mkString("."))


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
    reference.split('.') match
      case Array(name, token) =>
        for
          lock       <- decoded(name)
          generation <- decoded(token).flatMap(_.toLongOption)
        yield Claim(LockName(lock), Token(generation))
      case _                  => None

  /**
   * One field, base64url encoded.
   *
   * @param part the field
   * @return its encoding, which holds no dot
   */
  private def encoded(part: String): String =
    Base64.getUrlEncoder.withoutPadding.encodeToString(part.getBytes(StandardCharsets.UTF_8))

  /**
   * One field, read back.
   *
   * @param part the encoded field
   * @return what it encodes, or `None` when it is not base64url
   */
  private def decoded(part: String): Option[String] =
    scala.util.Try(String(Base64.getUrlDecoder.decode(part), StandardCharsets.UTF_8)).toOption
