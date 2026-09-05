#!/usr/bin/env bash
#
# Run a demo scenario against a dkq that is already up, so you can watch a dashboard while known traffic
# goes through it.
#
#   bin/run.sh              # the stack and the service, with telemetry on
#   bin/demo.sh             # what scenarios there are
#   bin/demo.sh steady      # run one
#
# Nothing here asserts. If a scenario turns up a bug, reproduce it in the e2e suite — the demo finds
# things, the suite keeps them found.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

ADDRESS="${DKQ_ADDRESS:-localhost:9000}"

if ! nc -z "${ADDRESS%%:*}" "${ADDRESS##*:}" 2>/dev/null; then
  echo "nothing answering on $ADDRESS — start it first:" >&2
  echo "  bin/run.sh" >&2
  exit 1
fi

exec sbt -batch "demo/run ${*:-}"
