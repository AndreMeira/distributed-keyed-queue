package homelab.keyedqueue.domain.response.queue

/**
 * The outcome of a settle. `Stale` is an ordinary result of an at-least-once queue, not an error: a caller
 * has to look at it rather than be raised at.
 *
 * @param applied whether it landed, or the claim had already been revoked
 */
final case class SettleResponse(applied: SettleResponse.Applied)


object SettleResponse:

  /** Whether a call applied, or found the caller's claim already revoked. */
  enum Applied:
    case Ok, Stale
