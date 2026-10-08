#!/usr/bin/env bash
set -euo pipefail
API_URL="${API_URL:-http://localhost:8080}"
curl -fsS "$API_URL/health/ready"
printf '\n'
body='{"type":"text","payload":{"text":"Hello distributed world"},"max_attempts":3}'
res="$(curl -fsS -X POST "$API_URL/v1/jobs" -H 'Content-Type: application/json' -d "$body")"
echo "$res"
id="$(echo "$res" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"
test -n "$id"
for i in $(seq 1 50); do
  result="$(curl -fsS "$API_URL/v1/jobs/$id")"
  case "$result" in
    *'"status":"succeeded"'*) echo "$result"; echo 'SMOKE TEST PASSED'; exit 0 ;;
    *'"status":"failed"'*|*'"status":"canceled"'*) echo "$result"; exit 1 ;;
  esac
  sleep 0.2
done
echo "Timed out checking $id" >&2
exit 1
