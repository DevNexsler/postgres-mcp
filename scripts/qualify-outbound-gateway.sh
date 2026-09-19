#!/usr/bin/env bash
# Production-shaped gateway qualification, including enforced traffic control.
set -Eeuo pipefail
: "${COMPOSE_PROJECT_NAME:?manager-owned COMPOSE_PROJECT_NAME is required}"
: "${MAINT_DOCKER_RUN_ID:?manager-owned MAINT_DOCKER_RUN_ID is required}"
: "${CDS_REPO:?path to Comm-Data-Store checkout is required}"
: "${CDS_REVISION:?pin the Comm-Data-Store harness commit}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_sha="$(git -C "$repo_root" rev-parse HEAD)"
cds_sha="$(git -C "$CDS_REPO" rev-parse "${CDS_REVISION}^{commit}")"
export OUTBOUND_QUALIFICATION_IMAGE="comm-data-store-mcp:qual-${MAINT_DOCKER_RUN_ID}"
scratch="$(mktemp -d)"
mkdir "$scratch/gateway" "$scratch/cds"
compose=(docker compose --project-directory "$scratch/cds" -f "$scratch/cds/docker-compose.outbound-qualification.yml" -f "$scratch/enforce.yml")
cleanup() {
  local result=$?
  trap - EXIT
  if (( result != 0 )); then
    "${compose[@]}" logs --tail 100 gateway || true
  fi
  "${compose[@]}" down --remove-orphans --volumes || result=1
  if [[ -n "$(docker ps -aq --filter "label=com.docker.compose.project=${COMPOSE_PROJECT_NAME}")$(docker network ls -q --filter "label=com.docker.compose.project=${COMPOSE_PROJECT_NAME}")$(docker volume ls -q --filter "label=com.docker.compose.project=${COMPOSE_PROJECT_NAME}")" ]]; then
    echo 'qualification cleanup failed: project resources remain' >&2
    result=1
  fi
  docker image rm "$OUTBOUND_QUALIFICATION_IMAGE" || result=1
  rm -rf "$scratch"
  echo "qualification cleanup exit=$result"
  exit "$result"
}
# Export commits: never bake uncommitted code, operator .env files, or host venvs.
git -C "$repo_root" archive "$source_sha" | tar -x -C "$scratch/gateway"
git -C "$CDS_REPO" archive "$cds_sha" | tar -x -C "$scratch/cds"
printf 'services:\n  gateway:\n    environment:\n      OUTBOUND_TRAFFIC_CONTROL: enforce\n' > "$scratch/enforce.yml"
trap cleanup EXIT
uv run --directory "$scratch/gateway" pytest -q -ra
docker build --label "org.opencontainers.image.revision=$source_sha" -t "$OUTBOUND_QUALIFICATION_IMAGE" "$scratch/gateway"
docker compose --project-directory "$scratch/cds" -f "$scratch/cds/docker-compose.outbound-qualification.yml" up -d --wait
exec_env=(-e COMM_DATA_STORE_CANDIDATE_QUALIFICATION=1 -e "MAINT_DOCKER_RUN_ID=$MAINT_DOCKER_RUN_ID" -e COMM_DATA_STORE_CANDIDATE_GATEWAY_URL=http://127.0.0.1:8094/mcp -e COMM_DATA_STORE_QUALIFICATION_ADMIN_DSN=postgresql://qualification_admin:qualification_admin_password@postgres:5432/qualification)
"${compose[@]}" exec -T "${exec_env[@]}" gateway python tests/system/test_outbound_gateway_qualification_candidate.py
"${compose[@]}" up -d --no-deps --force-recreate --wait gateway
"${compose[@]}" exec -T "${exec_env[@]}" gateway python /app/tests/live/gateway_enforce_candidate.py
"${compose[@]}" logs gateway > "$scratch/gateway.log"
if grep -E 'traffic control (check failed|fail-open)' "$scratch/gateway.log"; then
  echo 'traffic probe failed open; enforce qualification invalid' >&2
  exit 1
fi
python3 - "$source_sha" "$cds_sha" "$OUTBOUND_QUALIFICATION_IMAGE" "$("${compose[@]}" ps -q gateway)" "$("${compose[@]}" ps -q postgres)" <<'PY'
import json
import subprocess
import sys

source, cds, image, *containers = sys.argv[1:]
def inspect(*args):
    return json.loads(subprocess.check_output(["docker", "inspect", *args], text=True))

states = []
for container in inspect(*containers):
    state = container["State"]
    summary = {"name": container["Name"], "status": state["Status"], "health": state.get("Health", {}).get("Status"),
               "oom_killed": state["OOMKilled"], "restarts": container["RestartCount"]}
    assert summary["status"] == "running" and summary["health"] == "healthy", summary
    assert not summary["oom_killed"] and summary["restarts"] == 0, summary
    states.append(summary)
print(json.dumps({"source_sha": source, "cds_sha": cds, "image_id": inspect(image)[0]["Id"],
                  "containers": states, "traffic_mode": "enforce", "qualification": "passed"}, sort_keys=True))
PY
