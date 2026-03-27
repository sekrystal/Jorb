#!/usr/bin/env bash

set -euo pipefail

API_URL="${API_URL:-http://127.0.0.1:8000}"
UI_URL="${UI_URL:-http://127.0.0.1:8500}"
PROJECT_DIR="$(pwd)"

print_header() {
  printf '\n== %s ==\n' "$1"
}

fail() {
  printf 'ERROR: %s\n' "$1" >&2
  exit 1
}

safe_git() {
  if command -v git >/dev/null 2>&1; then
    "$@" 2>/dev/null || true
  else
    printf 'git unavailable\n'
  fi
}

show_env_matches() {
  local env_file=""
  if [ -f ".env" ]; then
    env_file=".env"
  elif [ -f ".env.production" ]; then
    env_file=".env.production"
  elif [ -f ".env.local" ]; then
    env_file=".env.local"
  fi

  if [ -z "$env_file" ]; then
    printf 'no env file found in %s\n' "$PROJECT_DIR"
    return
  fi

  printf 'env_file=%s\n' "$env_file"
  grep -E '^(OPENAI_ENABLED|OPENAI_MODEL|DEMO_MODE|GREENHOUSE_ENABLED|SEARCH_DISCOVERY_ENABLED|ENABLE_SCHEDULER|DISCOVERY_MAX_SEARCH_QUERIES_PER_CYCLE|DISCOVERY_MAX_NEW_COMPANIES_PER_CYCLE|DISCOVERY_MAX_EXPANSIONS_PER_CYCLE|ALLOWED_LOCATION_SCOPES|ALLOW_REMOTE_GLOBAL|ALLOW_AMBIGUOUS_LOCATIONS)=' "$env_file" || true
  if grep -q '^OPENAI_API_KEY=' "$env_file"; then
    if grep -Eq '^OPENAI_API_KEY=.+$' "$env_file"; then
      printf 'OPENAI_API_KEY=<set>\n'
    else
      printf 'OPENAI_API_KEY=<missing>\n'
    fi
  else
    printf 'OPENAI_API_KEY=<missing>\n'
  fi
}

require_process() {
  local label="$1"
  local pattern="$2"

  if command -v pgrep >/dev/null 2>&1; then
    pgrep -af "$pattern" >/tmp/opportunity-scout-process-check.$$ || true
  else
    ps aux | grep -E "$pattern" | grep -v grep >/tmp/opportunity-scout-process-check.$$ || true
  fi

  if [ ! -s /tmp/opportunity-scout-process-check.$$ ]; then
    rm -f /tmp/opportunity-scout-process-check.$$
    fail "missing required process: ${label} (${pattern})"
  fi

  cat /tmp/opportunity-scout-process-check.$$
  rm -f /tmp/opportunity-scout-process-check.$$
}

require_http_json() {
  local label="$1"
  local url="$2"

  print_header "$label"
  local body
  body="$(curl -fsS "$url")" || fail "${label} failed at ${url}"
  printf '%s\n' "$body"
}

require_http_post_json() {
  local label="$1"
  local url="$2"
  local payload="$3"

  print_header "$label"
  local body
  body="$(curl -fsS -X POST "$url" -H 'Content-Type: application/json' -d "$payload")" || fail "${label} failed at ${url}"
  printf '%s\n' "$body"
}

require_ui() {
  print_header "curl primary UI"
  local body
  body="$(curl -fsS "$UI_URL")" || fail "primary UI check failed at ${UI_URL}"
  printf '%s\n' "$body" | head -n 20
  printf '%s\n' "$body" | grep -Eiq 'streamlit|Opportunity Scout|Jobs|Saved|Applied|Profile' \
    || fail "primary UI response did not look like Opportunity Scout"
}

show_processes() {
  if command -v pgrep >/dev/null 2>&1; then
    pgrep -af 'uvicorn api.main:app|scripts/run_worker.py|streamlit run ui/app.py' || printf 'no matching processes found\n'
  else
    ps aux | grep -E 'uvicorn api.main:app|scripts/run_worker.py|streamlit run ui/app.py' | grep -v grep || printf 'no matching processes found\n'
  fi
}

show_logs() {
  if [ -d "logs" ]; then
    find logs -maxdepth 1 -type f | sort | while read -r log_file; do
      print_header "tail ${log_file}"
      tail -n 100 "$log_file" || true
    done
  else
    printf 'logs directory not found\n'
  fi
}

print_header "pwd"
pwd

print_header "git rev-parse --short HEAD"
safe_git git rev-parse --short HEAD

print_header "git status --short"
safe_git git status --short

print_header "process list"
show_processes

print_header "required process proof"
require_process "api" 'uvicorn api.main:app'
require_process "worker" 'scripts/run_worker.py'
require_process "ui" 'streamlit run ui/app.py'

print_header "env flags"
show_env_matches

require_http_json "curl /autonomy-status" "${API_URL}/autonomy-status"
require_http_json "curl /health" "${API_URL}/health"
require_http_json "curl /runtime-control" "${API_URL}/runtime-control"
require_http_json "curl /discovery-status" "${API_URL}/discovery-status"
require_http_post_json "curl POST /runtime-control action=run_once" "${API_URL}/runtime-control" '{"action":"run_once"}'
require_http_json "curl /opportunities" "${API_URL}/opportunities?freshness_window_days=14"
require_ui

print_header "recent logs"
show_logs

print_header "runtime verdict"
printf '%s\n' 'Live runtime smoke passed: API, worker, and primary UI path were directly reachable.'
printf '%s\n' 'Local tests and preflight checks are still not live product proof on their own.'
