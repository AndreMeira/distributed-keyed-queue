package homelab.keyedqueue.client.queue.model

/**
 * A signal: the key named here has something worth looking at, and nothing else travels with it.
 *
 * @param id the key a consumer should read the state of
 */
case class Ready(id: String)
