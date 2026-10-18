#!/usr/bin/env bash
set -euo pipefail
mode=${1:-fake}
case "$mode" in
  fake) endpoint=llm-batch; queue=nexus-batch-service ;;
  live) endpoint=llm-batch-live; queue=nexus-batch-service-live ;;
  *) echo 'Usage: bash go/scripts/setup.sh [fake|live]' >&2; exit 2 ;;
esac
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
temporal() { bash "$root/scripts/temporal-runtime.sh" "$@"; }
address=${TEMPORAL_ADDRESS:-127.0.0.1:7234}
namespace=${TEMPORAL_NAMESPACE:-default}
temporal --address "$address" --namespace "$namespace" operator nexus endpoint list --output json > /dev/null
if endpoint_json=$(temporal --address "$address" operator nexus endpoint get --name "$endpoint" --output json 2>/dev/null); then
  python3 -c 'import json,sys; d=json.loads(sys.argv[1]); w=d.get("spec",{}).get("target",{}).get("worker",{}); expected={"namespace":sys.argv[2],"taskQueue":sys.argv[3]};
if w != expected: raise SystemExit("Existing endpoint target differs from requested namespace/queue; choose another endpoint or correct its target explicitly.")
print("Existing endpoint target verified.")' "$endpoint_json" "$namespace" "$queue"
else
  temporal --address "$address" operator nexus endpoint create --name "$endpoint" --target-namespace "$namespace" --target-task-queue "$queue"
fi
