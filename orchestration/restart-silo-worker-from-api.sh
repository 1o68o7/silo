#!/usr/bin/env bash
# LEGACY — Exécuté *dans* le conteneur API (socket Docker).
# Préférer l’orchestrateur hôte : log8ot/scripts/silo-host-orchestrator.py.
# Nécessite : socket Docker + compose log8ot monté sur /work (docker-compose.silo-vps-a.orchestration.yml).
set -euo pipefail

MODE="${1:-}"
case "$MODE" in true|false) ;; *)
  echo "usage: $0 true|false" >&2
  exit 1
  ;;
esac

ROOT="${SILO_ORCHESTRATION_HOST_DIR:-/work}"
COMPOSE_FILE="${SILO_ORCHESTRATION_COMPOSE_FILE:-docker-compose.yml}"

cd "$ROOT"
if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi

if [[ "${SILO_DUAL_CRAWL_WORKERS:-}" == "true" ]] || [[ "${SILO_DUAL_CRAWL_WORKERS:-}" == "1" ]]; then
  if [[ "$MODE" == "true" ]]; then
    exec docker compose -f "$COMPOSE_FILE" up -d --no-deps --force-recreate silo-worker-crawl-stealthy
  else
    exec docker compose -f "$COMPOSE_FILE" up -d --no-deps --force-recreate silo-worker-crawl-http
  fi
fi

export SILO_USE_STEALTHY_FETCHER="$MODE"
exec docker compose -f "$COMPOSE_FILE" up -d --no-deps --force-recreate silo-worker
