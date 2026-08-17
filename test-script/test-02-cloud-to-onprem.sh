#!/usr/bin/env bash
# test-02-cloud-to-onprem.sh — write on Cloud, verify it lands on OnPrem with origin=Cloud
# (outbound queue of Cloud = GcpPubSub, consumed by OnPrem)
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

run() {
  local key="test.c2o.$(date +%s)"
  local value="hello-cloud"
  info "key=$key value=$value"

  # 1. write on Cloud
  local code
  code="$(http_code POST "$CLOUD_URL/api/session" "{\"key\":\"$key\",\"value\":\"$value\",\"ttlSeconds\":120}")"
  [ "$code" = "200" ] && ok "cloud write HTTP 200" || bad "cloud write HTTP $code"

  # 2. Cloud sees it locally with origin=Cloud
  wait_until 10 '[ "$(json_field "$(http_get "$CLOUD_URL/api/compare?key='"$key"'")" exists)" = "true" ]' \
    && ok "cloud sees key locally" || bad "cloud never saw own write"
  local c_origin
  c_origin="$(json_field "$(http_get "$CLOUD_URL/api/compare?key=$key")" origin)"
  [ "$c_origin" = "Cloud" ] && ok "cloud origin=Cloud" || bad "cloud origin=$c_origin"

  # 3. OnPrem eventually sees the same key with origin=Cloud (cross-region sync via GcpPubSub)
  wait_until 30 '[ "$(json_field "$(http_get "$ONPREM_URL/api/compare?key='"$key"'")" origin)" = "Cloud" ]' \
    && ok "onprem synced key, origin=Cloud" || bad "onprem never synced key (GcpPubSub→Kafka path)"

  # 4. value intact on OnPrem
  local v
  v="$(json_field "$(http_get "$ONPREM_URL/api/compare?key=$key")" value)"
  [ "$v" = "$value" ] && ok "value intact on onprem" || bad "onprem value='$v' (want '$value')"

  summary "test-02 Cloud→OnPrem (GcpPubSub)"
}

run
