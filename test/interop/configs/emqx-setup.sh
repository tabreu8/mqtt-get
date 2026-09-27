#!/bin/sh
# Creates the SCRAM user for the v5-tcp-scram endpoint once EMQX is up
# (EMQX can't bootstrap SCRAM users from a file). Idempotent.
set -e
API=${EMQX_API:-http://127.0.0.1:21081}
for i in $(seq 1 60); do
  TOKEN=$(curl -sf -X POST "$API/api/v5/login" -H 'content-type: application/json' \
    -d '{"username":"admin","password":"public"}' | sed -n 's/.*"token":"\([^"]*\)".*/\1/p') && [ -n "$TOKEN" ] && break
  sleep 2
done
[ -n "$TOKEN" ] || { echo "EMQX API not reachable at $API" >&2; exit 1; }
curl -s -X POST "$API/api/v5/authentication/scram:built_in_database/users" \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"user_id":"scram-user","password":"scram-pass"}' >/dev/null
echo "EMQX SCRAM user ready"
