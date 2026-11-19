#!/usr/bin/env bash
# Re-run retrieval + scoring on PROD for every canonical uid the remediation merged into.
#
#   analysisRefreshFlag=true&retrievalRefreshFlag=ALL_PUBLICATIONS, one uid at a time,
#   through `kubectl port-forward svc/reciter-prod`. Prod only: dev and prod share DynamoDB.
#
# Usage:
#   ./rerun_canonical.sh --from snapshots/rerun_uids_<UTC>.txt  # the uids the --apply run merged
#   ./rerun_canonical.sh djl2008 shp2010                       # just these
#
# There is no default list: only canonical uids whose merge actually applied should be
# rescored, and dynamo_remediation.py --apply writes exactly those to rerun_uids_<UTC>.txt.
#
# Writes rerun_<UTC>.tsv (uid, HTTP status, seconds). Re-running is safe.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
PORT="${PORT:-9082}"
NS=reciter
OUT="$HERE/rerun_$(date -u +%Y%m%dT%H%M%SZ).tsv"

unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

usage() { echo "usage: $0 --from <rerun_uids_UTC.txt> | <uid>..." >&2; exit 2; }
UIDS=()
if [ "${1:-}" = "--from" ]; then
  [ "$#" -eq 2 ] && [ -f "$2" ] || usage
  while IFS= read -r u; do [ -n "$u" ] && UIDS+=("$u"); done < "$2"
elif [ "$#" -gt 0 ]; then
  UIDS=("$@")
else
  usage
fi
[ "${#UIDS[@]}" -gt 0 ] || { echo "no uids to re-run" >&2; exit 1; }
echo "${#UIDS[@]} uid(s): ${UIDS[*]}"

# api key straight into a variable; never printed
APIKEY="$(kubectl -n "$NS" get secret reciter-secrets -o jsonpath='{.data.ADMIN_API_KEY}' | base64 -d)"
[ -n "$APIKEY" ] || { echo "could not read ADMIN_API_KEY from reciter-secrets" >&2; exit 1; }

kubectl -n "$NS" port-forward svc/reciter-prod "$PORT:80" >/dev/null 2>&1 &
PF=$!
trap 'kill "$PF" 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do
  curl -s -o /dev/null "http://localhost:$PORT/reciter/ping" && break
  sleep 1
done
curl -sf -o /dev/null "http://localhost:$PORT/reciter/ping" || { echo "port-forward to svc/reciter-prod not answering" >&2; exit 1; }

printf 'uid\thttp\tseconds\n' > "$OUT"
for uid in "${UIDS[@]}"; do
  start=$(date +%s)
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 900 -G \
    -H "api-key: $APIKEY" \
    --data-urlencode "uid=$uid" \
    --data-urlencode "analysisRefreshFlag=true" \
    --data-urlencode "retrievalRefreshFlag=ALL_PUBLICATIONS" \
    --data-urlencode "useGoldStandard=AS_EVIDENCE" \
    "http://localhost:$PORT/reciter/feature-generator/by/uid" || true)
  code="${code:-000}"
  secs=$(( $(date +%s) - start ))
  printf '%s\t%s\t%s\n' "$uid" "$code" "$secs" | tee -a "$OUT"
done

echo "results: $OUT"
fails=$(awk -F'\t' 'NR > 1 && $2 != "200"' "$OUT" | wc -l | tr -d ' ')
echo "non-200: $fails"
[ "$fails" = "0" ]
