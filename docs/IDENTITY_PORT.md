# reciterdb.identity: Splunk to cron

`update/buildIdentity.py` replaces the Splunk saved search *"reciter identity
update"* (1 `dbxquery` against ASMS + 16 `ldapsearch` subsearches, joined with
`append` and a terminal `stats ... by weillCornellEduCWID`, written to the
`reciterIdentity` CSV lookup and upserted by DB Connect).

## Why

`append` subsearches are silently truncated at `maxresultrows`/`maxtime`, and
`list()` silently caps at 100 values per group. Both drop rows with no error.
The port logs a row count per source and aborts if any source returns zero.

## What is deliberately unchanged

The table is **cumulative**. Rows are upserted on `cwid` and never deleted, so
department and division survive after someone drops out of ED's `ou=canonical`
— a business requirement. This table must not join the shadow-build/atomic-swap
pattern in `setup/person_table_swap.sql`.

`notes` (1,979 rows) and `alumniResidentNYP` (778 rows) are written by something
outside this job and are absent from `UPSERT_COLUMNS`, so the upsert cannot
clobber them. **Their writer is still unidentified.**

`max(surname)` / `max(givenName)` and the 43 hardcoded excluded cwids are ported
verbatim so the first diff is empty. Both are follow-ups, not fixes for round one.

## Cutover

1. ~~Confirm the five LDAP base DNs.~~ **Done 2026-09-05** — all five resolve
   against live ED. Four came from the institutional client's own config
   (`application.properties` `ldap.base.dn`, and the `ldapSources` block in
   `k8-scheduling-default.yaml`); `ed-organizations` was inferred and the spike
   confirmed it. Splunk's `ldap.conf` was never needed.

   To re-run the spike (the deployed image carries neither the script nor
   `ldap3`, so the file is copied in and the dep installed at runtime):
   ```bash
   IMG=$(kubectl -n reciter get cronjob reciterdb \
     -o jsonpath='{.spec.jobTemplate.spec.template.spec.containers[0].image}')
   kubectl -n reciter run identity-spike --restart=Never --image="$IMG" \
     --overrides='{"spec":{"containers":[{"name":"identity-spike","image":"'"$IMG"'",
       "command":["sleep","1800"],"envFrom":[{"secretRef":{"name":"reciter-inst-secrets"}},
       {"configMapRef":{"name":"reciter-inst-client-configmap"}}]}]}}'
   kubectl -n reciter wait --for=condition=Ready pod/identity-spike --timeout=180s
   kubectl -n reciter cp update/buildIdentity.py identity-spike:/usr/src/app/buildIdentity.py
   kubectl -n reciter exec identity-spike -- pip install --quiet ldap3
   kubectl -n reciter exec identity-spike -- python3 /usr/src/app/buildIdentity.py --spike
   kubectl -n reciter delete pod identity-spike
   ```
2. Apply `setup/table_identity.sql`, then the `uq_identity_cwid` ALTER it
   documents. The upsert cannot work without that unique key.
3. `--dry-run` — builds and stages, writes nothing to `identity`.
4. Diff (below). Iterate until empty.
5. Apply `kubernetes/k8-cronjob-identity.yaml` by hand. Never `kubectl apply`
   `k8-cronjob.yaml` — it has drifted from the live object.
6. Run both for a week, then disable the Splunk search.

## Diff

Splunk writes `identity`; the port writes `identity_staging`. The diff is a
query, not a program.

```sql
-- Population delta
SELECT 'only in port' side, COUNT(*) n FROM identity_staging s
  LEFT JOIN identity i ON i.cwid = s.cwid WHERE i.cwid IS NULL
UNION ALL
SELECT 'only in splunk', COUNT(*) FROM identity i
  LEFT JOIN identity_staging s ON s.cwid = i.cwid WHERE s.cwid IS NULL;
```

```sql
-- Per-column disagreement across the 37 owned columns. Add columns as needed;
-- start with the ones the merge rules actually decide.
SELECT SUM(NOT (s.surname                  <=> i.surname))                  surname,
       SUM(NOT (s.givenName                <=> i.givenName))                givenName,
       SUM(NOT (s.primaryAcademicDepartment<=> i.primaryAcademicDepartment)) dept,
       SUM(NOT (s.primaryAcademicDivision  <=> i.primaryAcademicDivision))  division,
       SUM(NOT (s.primaryProgram           <=> i.primaryProgram))           program,
       SUM(NOT (s.primaryOrg               <=> i.primaryOrg))               org,
       SUM(NOT (s.facultyRank              <=> i.facultyRank))              rank_,
       SUM(NOT (s.fullTimeFaculty          <=> i.fullTimeFaculty))          ftf
FROM identity_staging s JOIN identity i ON i.cwid = s.cwid;
```

"Only in splunk" is expected to be large on the first run and is **not** a bug:
those are the cumulative residue rows the SPL's final `where` no longer emits.
"Only in port" should be near zero — investigate any row there.

## ED returns attribute names lowercased

Confirmed live: ED answers with `weillcornelleducwid`, `labeleduri;onlinedirectory`
and so on, regardless of the casing requested. LDAP attribute descriptions are
case-insensitive by RFC 4512, so this is correct server behaviour, but it means a
plain `dict.get("weillCornellEduCWID")` misses every time. `_Row` stores and looks
up keys lowercased. Do not replace it with a plain dict.

`labeledURI;pops` and `labeledURI;vivo` are present on roughly a third of
full-time faculty; that is real sparsity, not a query fault.

## Open

- [ ] What writes `notes` and `alumniResidentNYP`?
- [ ] `varchar(128)` truncation on profile URLs and `primaryTitle` — widen after
      the diff is clean; `--dry-run` logs each truncation
- [ ] Stale residue rows keep `fullTimeFaculty=yes` after an appointment ends.
      Pre-existing behaviour, deliberately not addressed here.
