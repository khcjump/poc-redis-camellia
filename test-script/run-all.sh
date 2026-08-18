#!/usr/bin/env bash
# run-all.sh — run every test-*.sh in dependency order and aggregate results
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

TESTS=(
  test-01-health
  test-02-cloud-to-onprem
  test-03-onprem-to-cloud
  test-04-ttl-expire
  test-05-switch-debounce
  test-06-queue-metrics
  test-07-webui
  test-08-hash-zset
)

passed=0
failed=0
declare -a failures=()

for t in "${TESTS[@]}"; do
  printf '\n%s[ RUN ]%s %s\n' "$C_CYAN" "$C_NC" "$t"
  if "$SCRIPT_DIR/$t.sh"; then
    passed=$((passed + 1))
  else
    failed=$((failed + 1))
    failures+=("$t")
  fi
done

printf '\n%s══════════════════════════════════════════════════════════%s\n' "$C_CYAN" "$C_NC"
printf '%s[SUMMARY]%s tests-passed=%d tests-failed=%d\n' "$C_CYAN" "$C_NC" "$passed" "$failed"
if [ "$failed" -gt 0 ]; then
  printf '%s[FAILED]%s %s\n' "$C_RED" "$C_NC" "${failures[*]}"
  exit 1
fi
exit 0
