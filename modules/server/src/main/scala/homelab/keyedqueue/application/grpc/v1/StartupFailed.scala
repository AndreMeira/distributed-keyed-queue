package homelab.keyedqueue.application.grpc.v1

import homelab.common.error.ApplicationError


/**
 * The gRPC server could not start.
 *
 * Nothing is running, so there is nothing to retry against and no caller to tell — this reaches a log and
 * an exit code. It lives beside the module that starts the server for the same reason [[Misconfigured]]
 * lives beside the config loader: it can only happen there.
 *
 * @param reason what the server reported
 */
final case class StartupFailed(reason: String) extends ApplicationError, ApplicationError.AdapterError, ApplicationError.UnrecoverableError:

  /** @return the message */
  override def message: String = s"The service could not start: $reason"
