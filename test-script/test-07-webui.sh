#!/usr/bin/env bash
# test-07-webui.sh — Web UI reachable
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

run() {
  info "web-ui = $WEBUI_URL"
  local code
  code="$(http_code GET "$WEBUI_URL/")"
  [ "$code" = "200" ] && ok "web-ui HTTP 200" || bad "web-ui HTTP $code (want 200)"
  summary "test-07 web-ui"
}

run
