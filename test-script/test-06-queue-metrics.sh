#!/usr/bin/env bash
# test-06-queue-metrics.sh — queue health: no send failures, no replay failures on either side
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

run() {
  local c o
  c="$(http_get "$CLOUD_URL/api/queue-status")"
  o="$(http_get "$ONPREM_URL/api/queue-status")"
  [ -n "$c" ] && ok "cloud queue-status responds" || bad "cloud queue-status unreachable"
  [ -n "$o" ] && ok "onprem queue-status responds" || bad "onprem queue-status unreachable"

  local cm om c_rf o_rf c_sf o_sf c_sent o_sent
  cm="$(json_field "$c" metrics)"
  om="$(json_field "$o" metrics)"

  c_rf="$(json_field "$cm" replayFail)"
  o_rf="$(json_field "$om" replayFail)"
  [ "$c_rf" = "0" ] && ok "cloud replayFail=0" || bad "cloud replayFail='$c_rf' (want 0)"
  [ "$o_rf" = "0" ] && ok "onprem replayFail=0" || bad "onprem replayFail='$o_rf' (want 0)"

  c_sf="$(json_field "$cm" sendFail)"
  o_sf="$(json_field "$om" sendFail)"
  [ "$c_sf" = "0" ] && ok "cloud sendFail=0" || bad "cloud sendFail='$c_sf' (want 0)"
  [ "$o_sf" = "0" ] && ok "onprem sendFail=0" || bad "onprem sendFail='$o_sf' (want 0)"

  # informational: activity observed during earlier data tests
  c_sent="$(json_field "$cm" sent)"
  o_sent="$(json_field "$om" sent)"
  info "cloud metrics: sent=$c_sent consumed=$(json_field "$cm" consumed) replaySuccess=$(json_field "$cm" replaySuccess) inFlight=$(json_field "$cm" inFlight)"
  info "onprem metrics: sent=$o_sent consumed=$(json_field "$om" consumed) replaySuccess=$(json_field "$om" replaySuccess) inFlight=$(json_field "$om" inFlight)"
  if [ "${c_sent:-0}" -gt 0 ] && [ "${o_sent:-0}" -gt 0 ]; then
    ok "both sides have sent>0 (cloud=$c_sent onprem=$o_sent)"
  else
    warn "sent counters zero — run data tests (02/03) first"
  fi

  summary "test-06 queue metrics"
}

run
