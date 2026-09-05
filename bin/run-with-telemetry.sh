#!/usr/bin/env bash
#
# Run the dkq image with the OpenTelemetry agent on, against the local observability stack.
#
# The agent only ships in the image (`javaAgents ... % "dist"` in build.sbt), so this is the only way to
# see dkq's telemetry locally — `sbt run` has no agent to enable. The image keeps the agent inert by
# default; the two variables below are what wake it.
#
#   docker compose --profile telemetry up -d      # Jaeger, Prometheus, Grafana
#   sbt server/Docker/publishLocal                # (re)build the image
#   bin/run-with-telemetry.sh
#
#   Traces      http://localhost:16686
#   Metrics     http://localhost:9090
#   Dashboards  http://localhost:3000
#
# Anything after `--` is passed to the service, so a run mode can be selected once there is more than one.
set -euo pipefail

NETWORK="${DKQ_NETWORK:-distributed-keyed-queue_default}"
IMAGE="${DKQ_IMAGE:-distributed-keyed-queue:latest}"

if ! docker network inspect "$NETWORK" >/dev/null 2>&1; then
  echo "network '$NETWORK' not found — start the stack first:" >&2
  echo "  docker compose --profile telemetry up -d" >&2
  exit 1
fi

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "image '$IMAGE' not found — build it first:" >&2
  echo "  sbt server/Docker/publishLocal" >&2
  exit 1
fi

# Interactive only when there is a terminal to be interactive with, so this also works from a script.
# A plain string rather than an array: an empty array under `set -u` is an error in bash 3.2, which is
# what macOS ships.
TTY=""
if [ -t 0 ] && [ -t 1 ]; then TTY="-it"; fi

exec docker run --rm ${TTY} \
  --name dkq-app \
  --network "$NETWORK" \
  -p 9000:9000 \
  -e OTEL_JAVAAGENT_ENABLED=true \
  -e OTEL_SERVICE_NAME=distributed-keyed-queue \
  -e OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318 \
  -e OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf \
  -e OTEL_LOGS_EXPORTER=none \
  -e DKQ_REDIS_URL=redis://redis:6379 \
  "$IMAGE" "$@"
