#!/usr/bin/env bash
#
# Everything needed to watch dkq's telemetry locally: bring up the observability stack, build the image,
# and run the service against it with the OpenTelemetry agent on.
#
#   bin/run.sh              start it all, and follow the service's logs
#   bin/run.sh --no-build   skip the image build, when nothing has changed
#   bin/run.sh down         stop the service and the stack
#
#   Traces      http://localhost:16686   (Jaeger — service `distributed-keyed-queue`)
#   Metrics     http://localhost:9090    (Prometheus — try `dkq_jvm_cpu_time_seconds_total`)
#   Dashboards  http://localhost:3000    (Grafana — both datasources provisioned)
#
# Nothing appears until you send a request: the agent reports what happens, not what could.
#
# Why the image and not `sbt run`: the agent is scoped `dist` in build.sbt, so it exists only in the
# packaged application — which is what keeps it out of the dev loop. See
# docs/learning-material/java-agents-and-telemetry.md.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

case "${1:-up}" in
  down)
    docker rm -f dkq-app >/dev/null 2>&1 || true
    docker compose --profile telemetry down
    exit 0
    ;;
  up|--no-build) ;;
  *)
    echo "usage: $(basename "$0") [up | --no-build | down]" >&2
    exit 2
    ;;
esac

echo "==> observability stack"
docker compose --profile telemetry up -d --wait

if [ "${1:-up}" != "--no-build" ]; then
  echo "==> building the image"
  sbt -batch server/Docker/publishLocal
fi

echo "==> dkq, with the agent on"
echo "    traces http://localhost:16686 · metrics http://localhost:9090 · dashboards http://localhost:3000"
exec bin/run-with-telemetry.sh
