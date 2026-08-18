#!/usr/bin/env bash
# test-08-hash-zset.sh — Hash and Sorted Set (ZSet) cross-region sync & operations test
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

run() {
  local hkey="test.hash.$(date +%s)"
  local hfield="user_1"
  local hval="alice_cloud"
  info "Hash test: key=$hkey field=$hfield val=$hval"

  # 1. Write Hash on Cloud
  local code
  code="$(http_code POST "$CLOUD_URL/api/hash" "{\"key\":\"$hkey\",\"field\":\"$hfield\",\"value\":\"$hval\"}")"
  [ "$code" = "200" ] && ok "cloud write Hash HTTP 200" || bad "cloud write Hash HTTP $code"

  # 2. OnPrem syncs Hash key and field value
  wait_until 30 '[ "$(json_field "$(http_get "$ONPREM_URL/api/hash?key='"$hkey"'")" exists)" = "true" ]' \
    && ok "onprem synced Hash key" || bad "onprem never synced Hash key"

  local fields_json
  fields_json="$(json_field "$(http_get "$ONPREM_URL/api/hash?key=$hkey")" fields)"
  local field_val
  field_val="$(json_field "$(json_field "$fields_json" "$hfield")" value)"
  [ "$field_val" = "$hval" ] && ok "onprem Hash field value intact ($field_val)" || bad "onprem Hash field value '$field_val' (want '$hval')"

  # 3. Write Hash on OnPrem
  local hkey2="test.hash.o2c.$(date +%s)"
  local hval2="bob_onprem"
  code="$(http_code POST "$ONPREM_URL/api/hash" "{\"key\":\"$hkey2\",\"field\":\"user_2\",\"value\":\"$hval2\"}")"
  [ "$code" = "200" ] && ok "onprem write Hash HTTP 200" || bad "onprem write Hash HTTP $code"

  wait_until 30 '[ "$(json_field "$(http_get "$CLOUD_URL/api/hash?key='"$hkey2"'")" exists)" = "true" ]' \
    && ok "cloud synced Hash key" || bad "cloud never synced Hash key"

  # 4. Write ZSet on Cloud
  local zkey="test.zset.$(date +%s)"
  local zmember="player_1"
  local zscore=150
  info "ZSet test: key=$zkey member=$zmember score=$zscore"

  code="$(http_code POST "$CLOUD_URL/api/zset" "{\"key\":\"$zkey\",\"member\":\"$zmember\",\"score\":$zscore}")"
  [ "$code" = "200" ] && ok "cloud write ZSet HTTP 200" || bad "cloud write ZSet HTTP $code"

  wait_until 30 '[ "$(json_field "$(http_get "$ONPREM_URL/api/zset?key='"$zkey"'")" exists)" = "true" ]' \
    && ok "onprem synced ZSet key" || bad "onprem never synced ZSet key"

  # 5. Write ZSet on OnPrem
  local zkey2="test.zset.o2c.$(date +%s)"
  code="$(http_code POST "$ONPREM_URL/api/zset" "{\"key\":\"$zkey2\",\"member\":\"player_2\",\"score\":200}")"
  [ "$code" = "200" ] && ok "onprem write ZSet HTTP 200" || bad "onprem write ZSet HTTP $code"

  wait_until 30 '[ "$(json_field "$(http_get "$CLOUD_URL/api/zset?key='"$zkey2"'")" exists)" = "true" ]' \
    && ok "cloud synced ZSet key" || bad "cloud never synced ZSet key"

  summary "test-08 Hash & ZSet sync"
}

run
