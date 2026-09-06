package homelab.keyedqueue.infrastructure.tracing


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.telemetry.OtelMonitor
import io.opentelemetry.api.GlobalOpenTelemetry
import zio.*


/**
 * Where the `Monitor` port is wired to OpenTelemetry.
 *
 * The toolkit hands back an adapter and leaves this here, because which SDK backs it is a deployment
 * question: the global one is whatever registered itself, which is the Java agent when the image runs with
 * `OTEL_JAVAAGENT_ENABLED=true`, and a no-op the rest of the time. Nothing branches on that — an
 * unconfigured SDK answers with non-recording spans and no-op instruments, so the same wiring is correct in
 * every environment. See `docs/learning-material/java-agents-and-telemetry.md`.
 *
 * How the current span is stored is deliberately not decided here. It is a correctness question rather than
 * a deployment one — a thread-local loses the span whenever a fiber parks, which a long-polling claim does
 * by design — so `OtelMonitor` settles it and offers no way to get it wrong.
 */
object Module:
  case class OtelLoadError(cause: Throwable) extends ApplicationError.AdapterError:
    override def message: String = s"OpenTelemetry SDK could not be loaded: ${cause.getMessage}"

  /** The scope every span and metric this service opens is attributed to — us, not the libraries. */
  private val scope = "homelab.keyedqueue"

  /**
   * The `Monitor` the service observes itself with.
   *
   * Scoped because the tracer and meter behind it are: they are released when the service stops. The
   * failure is narrowed to `AdapterError` so the whole graph fails with one type — reading the global SDK
   * is an effect that can fail, and a service that cannot build its monitor has not started.
   */
  val monitor: ZLayer[Any, ApplicationError.AdapterError, Monitor] =
    ZLayer.scoped:
      for
        otel    <- ZIO.attempt(GlobalOpenTelemetry.get()).mapError(OtelLoadError(_))
        monitor <- OtelMonitor.make(otel, scope)
      yield Monitor.WithLogging(monitor)
