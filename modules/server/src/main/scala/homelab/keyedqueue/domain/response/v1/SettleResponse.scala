package homelab.keyedqueue.domain.response.v1

/**
 * The outcome of a settle.
 *
 * Note what is *not* an error here: a `Stale` outcome is an ordinary result of an at-least-once queue,
 * carried where a caller has to look at it rather than raised as a status.
 *
 * @param applied whether it landed, or the claim had already been revoked
 */
final case class SettleResponse(applied: SettleResponse.Applied)


object SettleResponse:

  /** Whether a call applied, or found the caller's claim already revoked. */
  enum Applied:
    case Ok, Stale
