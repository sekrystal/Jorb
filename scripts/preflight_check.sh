#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ -n "${VIRTUAL_ENV:-}" ] && [ -x "${VIRTUAL_ENV}/bin/python" ]; then
  PYTHON_BIN="${VIRTUAL_ENV}/bin/python"
elif [ -x "$ROOT/.venv_validation/bin/python" ]; then
  PYTHON_BIN="$ROOT/.venv_validation/bin/python"
elif [ -x "$ROOT/.venv/bin/python" ]; then
  PYTHON_BIN="$ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "Python interpreter not found." >&2
  exit 1
fi

echo "Using Python: $PYTHON_BIN"

TMP_DB="$(mktemp "${TMPDIR:-/tmp}/opportunity-scout-preflight-XXXXXX.sqlite")"
TMP_PYCACHE="$(mktemp -d "${TMPDIR:-/tmp}/opportunity-scout-pyc-XXXXXX")"
cleanup() {
  rm -f "$TMP_DB" "${TMP_DB}-wal" "${TMP_DB}-shm"
  rm -rf "$TMP_PYCACHE"
}
trap cleanup EXIT

export DATABASE_URL="sqlite:///$TMP_DB"
export ENABLE_SCHEDULER="false"
export OPENAI_ENABLED="false"
export PYTHONDONTWRITEBYTECODE="1"
export PYTHONPYCACHEPREFIX="$TMP_PYCACHE"

echo "== pwd =="
pwd

echo
echo "== git status --short =="
git status --short || true

echo
echo "== compileall =="
"$PYTHON_BIN" -m compileall api core services scripts tests

echo
echo "== init_db smoke =="
"$PYTHON_BIN" scripts/init_db.py

echo
echo "== api import and route smoke =="
"$PYTHON_BIN" - <<'PY'
from api.main import app
from core.db import SessionLocal
from services.autonomy import build_autonomy_health
from services.company_discovery import build_discovery_status
from services.runtime_control import get_runtime_control, runtime_control_payload
from services.sync import list_leads

route_paths = {route.path for route in app.router.routes}
for required in ["/health", "/runtime-control", "/autonomy-status", "/discovery-status", "/opportunities", "/search-runs/latest", "/search-runs/manual"]:
    if required not in route_paths:
        raise SystemExit(f"Missing expected route: {required}")
    print(required, "registered")

with SessionLocal() as session:
    runtime_control_payload(get_runtime_control(session))
    build_autonomy_health(session)
    build_discovery_status(session)
    list_leads(session, freshness_window_days=14)

print("service-backed route smoke ok")
PY

echo
echo "== worker import smoke =="
"$PYTHON_BIN" - <<'PY'
from scripts.run_worker import main, run_worker_loop

assert callable(main)
assert callable(run_worker_loop)
print("worker import ok")
PY

echo
echo "== pytest =="
"$PYTHON_BIN" -m pytest tests

echo
echo "Preflight checks passed."
echo "This script proves local imports, DB init, and test coverage only."
echo "It is not live runtime proof. For acceptance-critical product work, run ./scripts/runtime_self_check.sh against a running API, worker, and UI stack."
