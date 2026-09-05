package homelab.keyedqueue.infrastructure.tracing


import homelab.common.monitor.Monitor
import homelab.telemetry.OtelMonitor
import zio.*
import zio.telemetry.opentelemetry.OpenTelemetry
import zio.telemetry.opentelemetry.metrics.Meter
import zio.telemetry.opentelemetry.tracing.Tracing


/**
 * Where the `Monitor` port is wired to OpenTelemetry.
 *
 * The toolkit hands back an adapter and leaves the wiring here, because which SDK backs it is a deployment
 * question: [[OpenTelemetry.global]] is whatever registered itself as global, which is the Java agent when
 * the image runs with `OTEL_JAVAAGENT_ENABLED=true`, and a no-op the rest of the time. Nothing branches on
 * that — an unconfigured SDK answers with non-recording spans and no-op instruments, so the same wiring is
 * correct in every environment. See `docs/learning-material/java-agents-and-telemetry.md`.
 *
 * '''`contextJVM`, not the default fiber-local storage.''' The agent keeps the current span in a
 * thread-local; zio-telemetry defaults to a `FiberRef`. With the default, spans opened here would not nest
 * under the agent's — a request would produce two disconnected traces instead of one.
 */
object Module:

  /** The scope every span and metric this service opens is attributed to — us, not the libraries. */
  private val scope = "homelab.keyedqueue"

  private val otel    = OpenTelemetry.global ++ OpenTelemetry.contextJVM
  private val tracing = otel >>> OpenTelemetry.tracing(scope)
  private val metrics = otel >>> OpenTelemetry.metrics(scope)

  /**
   * The `Monitor` the service observes itself with.
   *
   * Fails with `Throwable` because [[OpenTelemetry.global]] does: reading the global SDK is an effect that
   * can fail, and there is nothing useful to translate it into — a service that cannot build its monitor
   * has not started.
   */
  val monitor: ZLayer[Any, Throwable, Monitor] =
    (tracing ++ metrics) >>> ZLayer:
      for
        tracer  <- ZIO.service[Tracing]
        meter   <- ZIO.service[Meter]
        monitor <- OtelMonitor.make(tracer, meter)
      yield Monitor.WithLogging(monitor)
