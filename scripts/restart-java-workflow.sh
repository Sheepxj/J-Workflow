#!/usr/bin/env bash
set -euo pipefail

workflow_repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
workflow_mode="${1:---container}"
case "$workflow_mode" in
    --verify-only|--container) ;;
    *) printf 'Usage: %s [--verify-only|--container]\n' "$0" >&2; exit 2 ;;
esac

mvn -B -f "$workflow_repo_dir/core-workflow-java/pom.xml" verify

if [[ "$workflow_mode" == '--verify-only' ]]; then
    exit 0
fi

docker info >/dev/null
workflow_compose_file="${WORKFLOW_COMPOSE_FILE:-$workflow_repo_dir/docker/PaiFlow/docker-compose.yaml}"
workflow_java_service="${WORKFLOW_JAVA_SERVICE:-core-workflow-java}"
docker compose -f "$workflow_compose_file" up -d --build --no-deps --force-recreate --wait --wait-timeout 180 "$workflow_java_service"
