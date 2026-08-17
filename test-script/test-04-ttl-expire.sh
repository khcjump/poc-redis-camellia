#!/usr/bin/env bash
# test-04-ttl-expire.sh — TTL/setex forwarding: key written with TTL must
# (a) arrive on the other side with a bounded TTL, (b) expire on both sides
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

run() {
  local key="test.ttl.$(date +%s)"
  local value="expiring"
  local ttl=8
  info "key=$key ttl=${ttl}s"

  # 1. write on Cloud with TTL
  local code
  code="$(http_code POST "$CLOUD_URL/api/session" "{\"key\":\"$key\",\"value\":\"$value\",\"ttlSeconds\":$ttl}")"
  [ "$code" = "200" ] && ok "cloud write TTL HTTP 200" || bad "cloud write TTL HTTP $code"

  # 2. Cloud sees key with TTL > 0 and <= requested
  wait_until 10 '[ "$(json_field "$(http_get "$CLOUD_URL/api/compare?key='"$key"'")" exists)" = "true" ]' \
    && ok "cloud sees key locally" || bad "cloud never saw own write"
  local c_ttl
  c_ttl="$(json_field "$(http_get "$CLOUD_URL/api/compare?key=$key")" ttlSeconds)"
  if [ -n "$c_ttl" ] && [ "$c_ttl" -gt 0 ] && [ "$c_ttl" -le "$ttl" ]; then
    ok "cloud ttlSeconds=$c_ttl (within 1..$ttl)"
  else
    bad "cloud ttlSeconds='$c_ttl' (want 1..$ttl)"
  fi

  # 3. OnPrem receives it (sync) with a TTL still > 0
  wait_until 30 '[ "$(json_field "$(http_get "$ONPREM_URL/api/compare?key='"$key"'")" exists)" = "true" ]' \
    && ok "onprem synced key before expiry" || bad "onprem never saw key (or expired too fast)"
  local o_ttl
  o_ttl="$(json_field "$(http_get "$ONPREM_URL/api/compare?key=$key")" ttlSeconds)"
  if [ -n "$o_ttl" ] && [ "$o_ttl" -gt 0 ]; then
    ok "onprem ttlSeconds=$o_ttl (still counting down)"
  else
    bad "onprem ttlSeconds='$o_ttl' (want > 0)"
  fi

  # 4. key expires on both sides
  wait_until 25 '[ "$(json_field "$(http_get "$ONPREM_URL/api/compare?key='"$key"'")" exists)" = "false" ]' \
    && ok "onprem key expired (exists=false)" || bad "onprem key still present after TTL"
  wait_until 15 '[ "$(json_field "$(http_get "$CLOUD_URL/api/compare?key='"$key"'")" exists)" = "false" ]' \
    && ok "cloud key expired (exists=false)" || bad "cloud key still present after TTL"

  summary "test-04 TTL/EXPIRE forwarding"
}

run
