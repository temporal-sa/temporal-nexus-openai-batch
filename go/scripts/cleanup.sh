#!/usr/bin/env bash
set -euo pipefail
if [[ $# -ne 1 ]]; then echo 'Usage: bash go/scripts/cleanup.sh RUN_ID' >&2; exit 2; fi
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
"$root/bin/batchctl" cleanup --run-id "$1" --address "${TEMPORAL_ADDRESS:-127.0.0.1:7234}" --namespace "${TEMPORAL_NAMESPACE:-default}"
