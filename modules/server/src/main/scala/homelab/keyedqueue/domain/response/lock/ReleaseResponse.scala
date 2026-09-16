package homelab.keyedqueue.domain.response.queue

/**
 * The outcome of a release. `false` is an ordinary result — the hold had already been revoked — not an
 * error.
 *
 * @param released whether it applied
 */
final case class ReleaseResponse(released: Boolean)
