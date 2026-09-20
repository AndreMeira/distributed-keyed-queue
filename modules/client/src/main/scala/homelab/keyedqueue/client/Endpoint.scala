package homelab.keyedqueue.client

import zio.*


/**
 * Where a deployment answers, and how to reach it.
 *
 * One deployment serves both halves of the service, so one endpoint dials either.
 *
 * @param host where it listens
 * @param port the port it serves on
 * @param plaintext whether to dial without TLS, which is the homelab's arrangement inside a cluster
 * @param patience how long a call may take beyond what it was asked to wait for, after which the
 *                 transport gives up on it; a call that blocks by design, like waiting for work, is
 *                 allowed its own wait on top of this
 */
final case class Endpoint(
  host: String,
  port: Int,
  plaintext: Boolean = true,
  patience: Duration = 10.seconds,
)
