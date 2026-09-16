package homelab.keyedqueue.infrastructure.configuration

import homelab.common.error.ApplicationError


/**
 * The service's own configuration is unusable.
 *
 * Not a validation failure: what is invalid is nobody's request, so no caller can act on it. It can only
 * happen at startup, and nothing on a request path can produce it.
 *
 * @param reason what could not be read, or what was wrong with it
 */
final case class Misconfigured(reason: String) extends ApplicationError, ApplicationError.AdapterError, ApplicationError.UnrecoverableError:

  /** @return the message */
  override def message: String = s"The service is misconfigured: $reason"
