#!/usr/bin/env bash
# common.sh — shared helpers for camellia-sync data-sync test scripts
#
# Endpoints are overridable via env: CLOUD_URL, ONPREM_URL, WEBUI_URL.
set -uo pipefail

# ---- endpoints ----
CLOUD_URL="${CLOUD_URL:-http://localhost:8080}"
ONPREM_URL="${ONPREM_URL:-http://localhost:8086}"
WEBUI_URL="${WEBUI_URL:-http://localhost:3000}"

# ---- colors ----
C_GREEN=$'\033[0;32m'
C_RED=$'\033[0;31m'
C_YELLOW=$'\033[0;33m'
C_CYAN=$'\033[0;36m'
C_NC=$'\033[0m'

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

say()  { printf '%s\n' "$*"; }
info() { printf '%s[INFO]%s %s\n' "$C_CYAN" "$C_NC" "$*"; }
ok()   { PASS_COUNT=$((PASS_COUNT + 1)); printf '%s[PASS]%s %s\n' "$C_GREEN" "$C_NC" "$*"; }
bad()  { FAIL_COUNT=$((FAIL_COUNT + 1)); printf '%s[FAIL]%s %s\n' "$C_RED" "$C_NC" "$*"; }
warn() { SKIP_COUNT=$((SKIP_COUNT + 1)); printf '%s[SKIP]%s %s\n' "$C_YELLOW" "$C_NC" "$*"; }

# http_get <url> -> response body (empty string on connection failure)
http_get() { curl -sf -m 10 "$1" 2>/dev/null || true; }

# http_code <method> <url> [json-body] -> HTTP status code (000 on connection failure)
http_code() {
  local method="$1" url="$2" body="${3:-}"
  if [ -n "$body" ]; then
    curl -s -o /dev/null -w '%{http_code}' -m 10 -X "$method" \
      -H 'Content-Type: application/json' -d "$body" "$url" 2>/dev/null || echo 000
  else
    curl -s -o /dev/null -w '%{http_code}' -m 10 -X "$method" "$url" 2>/dev/null || echo 000
  fi
}

# json_field <json> <key> -> value of top-level key
#   scalars -> plain text ("true"/"false"/number/string)
#   nested object/array -> serialized JSON text (feed back into json_field)
#   absent or null -> empty string
json_field() {
  local json="$1" key="$2"
  if command -v jq >/dev/null 2>&1; then
    printf '%s' "$json" | jq -r --arg k "$key" \
      'if (has($k) and .[$k] != null) then .[$k] else "" end' 2>/dev/null
  else
    printf '%s' "$json" | python3 -c '
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    d = {}
v = d.get(sys.argv[1])
if v is None:
    print("")
elif isinstance(v, (dict, list)):
    print(json.dumps(v))
else:
    print(v)' "$key" 2>/dev/null
  fi
}

# wait_until <timeout_sec> <condition-string>
#   evals <condition-string> every 1s; returns 0 on first success, 1 on timeout
wait_until() {
  local timeout="$1" cond="$2" waited=0
  while [ "$waited" -lt "$timeout" ]; do
    if eval "$cond"; then return 0; fi
    sleep 1
    waited=$((waited + 1))
  done
  return 1
}

# summary <label> — prints counts; exits 1 when any check failed
summary() {
  local label="$1"
  printf '\n%s────────────────────────────────────────────%s\n' "$C_CYAN" "$C_NC"
  printf '%s[%s]%s passed=%d failed=%d skipped=%d\n' "$C_CYAN" "$label" "$C_NC" \
    "$PASS_COUNT" "$FAIL_COUNT" "$SKIP_COUNT"
  printf '%s────────────────────────────────────────────%s\n' "$C_CYAN" "$C_NC"
  [ "$FAIL_COUNT" -eq 0 ]
}
