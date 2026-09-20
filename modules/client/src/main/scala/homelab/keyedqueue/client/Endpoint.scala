package homelab.keyedqueue.client

/**
 * Where a deployment answers, and how to reach it.
 *
 * One deployment serves both halves of the service, so one endpoint dials either.
 *
 * @param host where it listens
 * @param port the port it serves on
 * @param plaintext whether to dial without TLS, which is the homelab's arrangement inside a cluster
 */
final case class Endpoint(host: String, port: Int, plaintext: Boolean = true)
