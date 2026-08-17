#!/usr/bin/env bash
# test-05-switch-debounce.sh — 大小網切換防抖: first switch succeeds (200),
# an immediate second switch must be rejected with 429
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

run() {
  # read current primary + debounce window
  local status primary min_interval last_switch now
  status="$(http_get "$CLOUD_URL/api/switch")"
  [ -n "$status" ] && ok "GET /api/switch responds" || bad "GET /api/switch unreachable"

  primary="$(json_field "$status" primary)"
  min_interval="$(json_field "$status" minIntervalSeconds)"
  last_switch="$(json_field "$status" lastSwitchAt)"
  [ -n "$primary" ] && ok "current primary=$primary" || bad "no primary in response"

  # ensure we are outside the debounce window before the first switch
  now="$(($(date +%s) * 1000))"
  if [ -n "$last_switch" ] && [ "$last_switch" -gt 0 ] && [ $((now - last_switch)) -lt $((min_interval * 1000)) ]; then
    local remain=$(( (min_interval * 1000 - (now - last_switch)) / 1000 + 1 ))
    info "inside debounce window, sleeping ${remain}s before first switch"
    sleep "$remain"
  fi

  local target="OnPrem"
  [ "$primary" = "OnPrem" ] && target="Cloud"

  # first switch -> 200
  local c1
  c1="$(http_code POST "$CLOUD_URL/api/switch?to=$target")"
  [ "$c1" = "200" ] && ok "1st switch ($primary -> $target) HTTP 200" || bad "1st switch HTTP $c1 (want 200)"

  # immediate second switch -> 429 (debounce)
  local c2
  c2="$(http_code POST "$CLOUD_URL/api/switch?to=$primary")"
  [ "$c2" = "429" ] && ok "2nd switch within window HTTP 429 (debounce)" || bad "2nd switch HTTP $c2 (want 429)"

  summary "test-05 switch debounce"
}

run
