package homelab.keyedqueue.domain.types


import java.nio.charset.StandardCharsets
import java.util.Base64
import zio.prelude.*


/**
 * Several parts carried as one string a reader is not meant to take apart: base64url per part, joined with
 * a dot.
 *
 * The alphabet cannot produce a dot, so the separator stays clear of the parts whatever they contain.
 */
type Obfuscated = Obfuscated.Type


object Obfuscated:
  opaque type Type <: String = String

  /**
   * One, trusted.
   *
   * @param value the encoded string
   * @return it, as the type
   */
  def apply(value: String): Obfuscated = value

  /**
   * Carry these parts as one.
   *
   * @param parts the parts, in the order a reader expects them back
   * @return their encoding
   */
  def encode(parts: String*): Obfuscated =
    parts.map(encodedPart).mkString(".")

  extension (value: Obfuscated)

    /**
     * Take it apart again.
     *
     * @return the parts, in the order they were encoded, or `None` when any of them is not base64url
     */
    def decoded: Option[Seq[String]] =
      value.split('.').toList.forEach(decodedPart)

  /**
   * One part, encoded.
   *
   * @param part the part
   * @return its encoding, which holds no dot
   */
  private def encodedPart(part: String): String =
    Base64.getUrlEncoder.withoutPadding.encodeToString(part.getBytes(StandardCharsets.UTF_8))

  /**
   * One part, read back.
   *
   * @param part the encoded part
   * @return what it encodes, or `None` when it is not base64url
   */
  private def decodedPart(part: String): Option[String] =
    scala.util.Try(String(Base64.getUrlDecoder.decode(part), StandardCharsets.UTF_8)).toOption
