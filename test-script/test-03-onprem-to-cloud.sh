#!/usr/bin/env bash
# test-03-onprem-to-cloud.sh — write on OnPrem, verify it lands on Cloud with origin=OnPrem
# (outbound queue of OnPrem = Kafka, consumed by Cloud)
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

run() {
  local key="test.o2c.$(date +%s)"
  local value="hello-onprem"
  info "key=$key value=$value"

  # 1. write on OnPrem
  local code
  code="$(http_code POST "$ONPREM_URL/api/session" "{\"key\":\"$key\",\"value\":\"$value\",\"ttlSeconds\":120}")"
  [ "$code" = "200" ] && ok "onprem write HTTP 200" || bad "onprem write HTTP $code"

  # 2. OnPrem sees it locally with origin=OnPrem
  wait_until 10 '[ "$(json_field "$(http_get "$ONPREM_URL/api/compare?key='"$key"'")" exists)" = "true" ]' \
    && ok "onprem sees key locally" || bad "onprem never saw own write"
  local o_origin
  o_origin="$(json_field "$(http_get "$ONPREM_URL/api/compare?key=$key")" origin)"
  [ "$o_origin" = "OnPrem" ] && ok "onprem origin=OnPrem" || bad "onprem origin=$o_origin"

  # 3. Cloud eventually sees the same key with origin=OnPrem (cross-region sync via Kafka)
  wait_until 30 '[ "$(json_field "$(http_get "$CLOUD_URL/api/compare?key='"$key"'")" origin)" = "OnPrem" ]' \
    && ok "cloud synced key, origin=OnPrem" || bad "cloud never synced key (Kafka→GcpPubSub path)"

  # 4. value intact on Cloud
  local v
  v="$(json_field "$(http_get "$CLOUD_URL/api/compare?key=$key")" value)"
  [ "$v" = "$value" ] && ok "value intact on cloud" || bad "cloud value='$v' (want '$value')"

  summary "test-03 OnPrem→Cloud (Kafka)"
}

run
