package homelab.keyedqueue.infrastructure.configuration

import homelab.common.error.ApplicationError


/**
 * The service's own configuration is unusable.
 *
 * '''Deliberately not a validation failure''', though both are "something is invalid": that one is a
 * caller's request and this one is nobody's, so no caller can fix it. Conflating them once meant a
 * malformed `queue.conf` was reported to callers as `INVALID_ARGUMENT`, blaming them for our file.
 *
 * Lives here, beside the loader that raises it: it can only happen at startup, and nothing on a request
 * path can produce it.
 *
 * @param reason what could not be read, or what was wrong with it
 */
final case class Misconfigured(reason: String) extends ApplicationError, ApplicationError.AdapterError, ApplicationError.UnrecoverableError:

  /** @return the message */
  override def message: String = s"The service is misconfigured: $reason"
