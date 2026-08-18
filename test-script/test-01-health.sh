#!/usr/bin/env bash
# test-01-health.sh — both regions report UP with the expected topology
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

run() {
  info "cloud  = $CLOUD_URL"
  info "onprem = $ONPREM_URL"

  local cloud onprem
  cloud="$(http_get "$CLOUD_URL/api/health")"
  onprem="$(http_get "$ONPREM_URL/api/health")"

  [ -n "$cloud" ] && ok "cloud /api/health responds" || bad "cloud /api/health unreachable"
  [ -n "$onprem" ] && ok "onprem /api/health responds" || bad "onprem /api/health unreachable"

  # --- cloud expectations (role=All, location=Cloud, ReadWrite, GcpPubSub) ---
  [ "$(json_field "$cloud" status)" = "UP" ] && ok "cloud status=UP" || bad "cloud status=$(json_field "$cloud" status)"
  [ "$(json_field "$cloud" location)" = "Cloud" ] && ok "cloud location=Cloud" || bad "cloud location=$(json_field "$cloud" location)"
  [ "$(json_field "$cloud" redisMode)" = "ReadWrite" ] && ok "cloud redisMode=ReadWrite" || bad "cloud redisMode=$(json_field "$cloud" redisMode)"
  [ "$(json_field "$cloud" queueType)" = "GcpPubSub" ] && ok "cloud queueType=GcpPubSub" || bad "cloud queueType=$(json_field "$cloud" queueType)"

  # --- onprem expectations (role=All, location=OnPrem, Sentinel, Kafka) ---
  [ "$(json_field "$onprem" status)" = "UP" ] && ok "onprem status=UP" || bad "onprem status=$(json_field "$onprem" status)"
  [ "$(json_field "$onprem" location)" = "OnPrem" ] && ok "onprem location=OnPrem" || bad "onprem location=$(json_field "$onprem" location)"
  [ "$(json_field "$onprem" redisMode)" = "Sentinel" ] && ok "onprem redisMode=Sentinel" || bad "onprem redisMode=$(json_field "$onprem" redisMode)"
  [ "$(json_field "$onprem" queueType)" = "Kafka" ] && ok "onprem queueType=Kafka" || bad "onprem queueType=$(json_field "$onprem" queueType)"

  summary "test-01 health"
}

run
