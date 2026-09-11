package homelab.keyedqueue.domain.model


import homelab.keyedqueue.domain.types.*

import java.nio.charset.StandardCharsets
import java.util.Base64


/**
 * What a consumer hands back to release or refresh a lock: which lock, under which fence generation.
 *
 * The lock's counterpart to [[Claim]] — same opaque-receipt idea, without the queue/key split a lock has no
 * use for. The receipt is handed out so a caller carries a token it cannot reason about; the fence token
 * itself is exposed separately, on purpose, for stamping downstream writes (see the lock service).
 *
 * @param name the lock held
 * @param token the generation this hold was granted under
 */
final case class LockClaim(name: LockName, token: Token):

  /**
   * Encode as the opaque handle a caller carries.
   *
   * Base64url over a space-separated `name token`, so a name may contain anything and the separator stays
   * clear of its content.
   *
   * @return the receipt
   */
  def receipt: String =
    Base64.getUrlEncoder.withoutPadding.encodeToString(s"$name $token".getBytes(StandardCharsets.UTF_8))


object LockClaim:

  /**
   * Read a receipt back to the claim it names.
   *
   * Takes a `String` rather than a receipt type: what a consumer hands back is unchecked, and this is the
   * mechanics of deciding whether it is one this service issued.
   *
   * @param receipt the opaque handle from an acquire
   * @return the claim it names, or `None` when it is not one we issued
   */
  def fromReceipt(receipt: String): Option[LockClaim] =
    scala.util
      .Try(String(Base64.getUrlDecoder.decode(receipt), StandardCharsets.UTF_8))
      .toOption
      .map(_.split(' '))
      .collect:
        case Array(name, token) if token.toLongOption.isDefined => LockClaim(LockName(name), Token(token.toLong))
