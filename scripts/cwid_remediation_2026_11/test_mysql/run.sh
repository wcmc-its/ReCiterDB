#!/bin/bash
# Fixture test for mysql_remediation.sql on a throwaway MariaDB container.
#   docker run -d --name cwidfix2-mariadb -e MARIADB_ROOT_PASSWORD=pw -e MARIADB_DATABASE=reciterdb mariadb:11.4
#   CONTAINER=cwidfix2-mariadb ./run.sh --load-schema     # first run
#   CONTAINER=cwidfix2-mariadb ./run.sh                   # later runs
# Never point this at a real database: it truncates the admin tables.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
SQL=$HERE/../mysql_remediation.sql
CONTAINER=${CONTAINER:-cwidfix2-mariadb}
PW=${MARIADB_PW:-pw}
D=$(mktemp -d)
M(){ docker exec -i "$CONTAINER" mariadb -uroot -p"$PW" -t "$@"; }
case "$(docker exec "$CONTAINER" mariadb -uroot -p"$PW" -N -e 'SELECT @@hostname')" in
  "$(docker inspect -f '{{.Config.Hostname}}' "$CONTAINER")") ;;
  *) echo "refusing: $CONTAINER is not a local docker container"; exit 2 ;;
esac
if [ "${1:-}" = "--load-schema" ]; then
  M < "$REPO/setup/createDatabaseTableReciterDb.sql" >/dev/null 2>&1 || true
  for f in alter_add_reporter_fields_v1.2.sql table_authorship_review.sql table_external_record.sql            table_external_rejection.sql table_person_author_id_import_harvest_v3.8.sql; do
    M reciterdb < "$REPO/setup/$f" >/dev/null || { echo "schema load failed: $f"; exit 2; }
  done
  M < "$HERE/extra_schema.sql" || exit 2
fi
sed -e 's/^ROLLBACK;$/-- ROLLBACK;/' -e 's/^-- COMMIT;$/COMMIT;/' "$SQL" > "$D/commit.sql"
grep -q '^COMMIT;$' "$D/commit.sql" || { echo "commit switch failed"; exit 2; }
CK="CHECKSUM TABLE admin_users, admin_users_roles, admin_feedback_log, admin_orcid, grant_provenance, authorship_review, reporting_ad_hoc_feature_generator_execution, reporting_ad_hoc_identity_creation EXTENDED"
ck(){ docker exec -i "$CONTAINER" mariadb -uroot -p"$PW" -N reciterdb -e "$CK" | cksum; }
pass=0; fail=0
ok(){ echo "PASS: $1"; pass=$((pass+1)); }; bad(){ echo "FAIL: $1"; fail=$((fail+1)); }

echo "== 1. ROLLBACK mode leaves data unchanged"
M < $HERE/fixture.sql || { echo 'fixture load failed'; exit 2; }; before=$(ck)
M reciterdb < $SQL > $D/out_rollback.log 2>&1; rc=$?
[ $rc -eq 0 ] && ok "rollback run exit 0" || { bad "rollback run exit $rc"; tail -5 $D/out_rollback.log; }
[ "$(ck)" = "$before" ] && ok "rollback: checksums unchanged" || bad "rollback changed data"
grep -q "checks not 0" $D/out_rollback.log && bad "rollback: a check fired on a clean fixture" || ok "rollback: no check fired on the clean fixture"

echo "== 2. COMMIT mode applies every fix"
M reciterdb < $D/commit.sql > $D/out_commit.log 2>&1; rc=$?
[ $rc -eq 0 ] && ok "commit run exit 0" || { bad "commit run exit $rc"; tail -5 $D/out_commit.log; }
M reciterdb < $HERE/expect.sql > $D/out_expect.log 2>&1
if grep -q FAIL $D/out_expect.log; then bad "expectations"; cat $D/out_expect.log; else ok "all fixture expectations hold (d3,d4,d5,d6)"; fi
grep "keeper_status" $D/out_commit.log | grep -q "merged_away_status" && grep -E "account merges +\| mrj4001 +\| +13 +\| +0 +\| +40078 +\| +1 " $D/out_commit.log >/dev/null && ok "account-merges output shows status" || bad "account-merges lacks status"

echo "== 3. second COMMIT run is idempotent"
before=$(ck)
M reciterdb < $D/commit.sql > $D/out_commit2.log 2>&1; rc=$?
[ $rc -eq 0 ] && ok "second commit exit 0" || { bad "second commit exit $rc"; tail -5 $D/out_commit2.log; }
[ "$(ck)" = "$before" ] && ok "second commit changed nothing" || bad "second commit changed data"

echo "== 4. non-zero GUARD aborts COMMIT, nothing committed"
M < $HERE/fixture.sql || { echo "fixture load failed"; exit 2; }
M reciterdb -e "INSERT INTO admin_orcid (personIdentifier, orcid) VALUES ('Abl9002', '0000-0001-0000-0000')"
before=$(ck)
M reciterdb < $D/commit.sql > $D/out_guard.log 2>&1; rc=$?
[ $rc -ne 0 ] && grep -q "stop_here" $D/out_guard.log && ok "guard: stopped with stop_here" || { bad "guard did not stop (rc=$rc)"; tail -5 $D/out_guard.log; }
grep -q "GUARD.*admin_orcid" $D/out_guard.log && ok "guard: listed in STOP output" || bad "guard not listed"
[ "$(ck)" = "$before" ] && ok "guard: nothing committed" || bad "guard: data changed"

echo "== 5. non-zero after-check aborts COMMIT, nothing committed"
M < $HERE/fixture.sql || { echo "fixture load failed"; exit 2; }
M reciterdb -e "INSERT INTO admin_users (userID, personIdentifier, status, proxy_person_ids) VALUES (31, 'pxy1002', 1, '{\"NA\": 1}')"
before=$(ck)
M reciterdb < $D/commit.sql > $D/out_after.log 2>&1; rc=$?
[ $rc -ne 0 ] && grep -q "stop_here" $D/out_after.log && ok "after-check: stopped with stop_here" || { bad "after-check did not stop (rc=$rc)"; tail -5 $D/out_after.log; }
grep -q "after.*proxy_person_ids mapped/deleted" $D/out_after.log && ok "after-check: listed in STOP output" || bad "after-check not listed"
[ "$(ck)" = "$before" ] && ok "after-check: nothing committed" || bad "after-check: data changed"

echo "mysql tests: $pass passed, $fail failed (logs in $D)"
[ $fail -eq 0 ]
