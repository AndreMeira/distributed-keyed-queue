#!/usr/bin/env bash
#
# Run a demo scenario against a dkq that is already up, so you can watch a dashboard while known traffic
# goes through it.
#
#   bin/run.sh              # the stack and the service, with telemetry on
#   bin/demo.sh             # what scenarios there are
#   bin/demo.sh steady      # run one
#
# DKQ_ADDRESS may name several instances, comma-separated, and every scenario spreads its producers and
# consumers over them — which is how to see whether the service scales out. Start a second one with
# `DKQ_NAME=dkq-app-2 DKQ_PORT=9001 bin/run-with-telemetry.sh`, then:
#
#   DKQ_ADDRESS=localhost:9000,localhost:9001 bin/demo.sh flood
#
# Nothing here asserts. If a scenario turns up a bug, reproduce it in the e2e suite — the demo finds
# things, the suite keeps them found.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

ADDRESS="${DKQ_ADDRESS:-localhost:9000}"

# Every named instance has to answer: a demo that silently drove one of two would report half the
# throughput as the whole of it.
for one in ${ADDRESS//,/ }; do
  if ! nc -z "${one%%:*}" "${one##*:}" 2>/dev/null; then
    echo "nothing answering on $one — start it first:" >&2
    echo "  bin/run.sh                                                   # the first" >&2
    echo "  DKQ_NAME=dkq-app-2 DKQ_PORT=9001 bin/run-with-telemetry.sh   # a second" >&2
    exit 1
  fi
done

exec sbt -batch "demo/run ${*:-}"
