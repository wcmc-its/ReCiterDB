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
6. Run both for a week, then disable the Splunk search. Keep the cron job on
   `--dry-run` for the whole parallel period -- it writes only
   `identity_staging`, leaving Splunk the sole writer to `identity` while the
   diff is reconciled. Drop `--dry-run` only at the switch.

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

## The two columns this job does not write

`notes` is written by hand (Drew). It is preserved because the table is never
deleted from and the column is absent from `UPSERT_COLUMNS`.

`alumniResidentNYP` is **dead**. Measured 2026-09-05: 778 rows carry it, the
newest created 2025-01-22, and only 18 are currently `residentNYP`. New rows have
been inserted continuously since (1-3/day) and none is ever flagged, so whatever
set it stopped by early 2025. It appears nowhere in the Splunk job.

Nine candidate definitions were probed against live ED looking for a rule that
yields 778 -- current NYP residents (3,080), NYP-resident type in ed-people
(1,146), in ed-sors but not active in ed-people (1,935), affiliate-alumni
(5,232), and the intersections (218 / 110 / 108). None matches. The rule is not
recoverable from ED; if the flag is wanted again it needs defining, not guessing.

## Open

- [x] ~~Is there a second Splunk writer?~~ **No.** Confirmed 2026-09-05 via
      `| rest`: exactly one saved search contains `dbxoutput` ("ReCiter - output
      identity to db", the two known lines), and `conf-db_outputs` holds one
      stanza -- `ReCiter-Identity`, connection `reciter`, table
      `` `reciterdb`.`identity` ``. The Splunk side is fully accounted for:
      one search builds the lookup, one output writes it.
- [ ] DB Connect's write mode was never read from config -- the field name in the
      `| rest` query was a guess and came back empty. It is inferred from
      behaviour instead: 35,448 rows, zero duplicate cwids, createTimestamps
      surviving from 2021, 1-3 new rows a day. That reads as update-in-place on
      cwid, which is what this port implements. Confirm with
      `| rest ... conf-db_outputs | search title="ReCiter-Identity" | transpose`
      if it ever matters.
- [ ] `uq_identity_cwid` was applied 2026-09-05 to a table with a live writer
      whose mode was never confirmed. Zero duplicates across ~229 nightly runs
      says DB Connect has never attempted a duplicate insert, so the constraint
      should be inert -- but the first Splunk run under it had not yet happened
      when this was written. If that job errors, this is why; rollback is
      `ALTER TABLE identity DROP INDEX uq_identity_cwid;`
- [ ] ed-sors lists 3,080 current NYP residents; ed-people shows 1,146. The
      `residentNYP` flag comes from ed-people, so ~1,900 people appear in the
      name and department sources but never get flagged and fall out at the
      final filter. Faithful to the SPL -- a pre-existing population gap, not
      introduced here.
- [ ] `varchar(128)` truncation on profile URLs and `primaryTitle` — widen after
      the diff is clean; `--dry-run` logs each truncation
- [ ] Stale residue rows keep `fullTimeFaculty=yes` after an appointment ends.
      Pre-existing behaviour, deliberately not addressed here.

## The original

`docs/reciter_identity_update.spl` is the Splunk saved search this job replaces,
exported 2026-09-05, verbatim and unedited. It is the reference any diff argues
against and the only record of the rules being reproduced — it existed nowhere
but a laptop and the Splunk UI. Do not edit it; it is provenance, not source.

Its final two lines are the write path:

```
| eval _key = cwid
| outputlookup reciterIdentity
```

with a second saved search doing `| inputlookup reciterIdentity | dbxoutput
output=ReCiter-Identity`. The DB Connect output stanza behind that name is UI-only
config and is **not** captured here — if it can be exported, it belongs beside
this file.

## ED's department to orgUnit migration

ED is moving `department`/`departmentCode` to `orgUnit`/`orgUnitCode`, tagged by
level (`;level1`, `;level2`, ...). This job still reads the old model. Measured
against live ED 2026-09-05:

| comparison | both | agree | differ | only old | only new |
|---|---:|---:|---:|---:|---:|
| `PrimaryDepartment` vs `PrimaryOrgUnit;level1` | 1,426 | 1,383 (97%) | 43 | 74 | **0** |
| `Department` vs `OrgUnit;level1` | 1,495 | 1,356 (91%) | 139 (9%) | 5 | **0** |

Three reasons the port did not switch:

1. **The old attributes are strictly more complete.** `PrimaryDepartment` 100% vs
   `PrimaryOrgUnit;level1` 93%; `Department` 100% vs `OrgUnit;level1` 91%. Zero
   records carry a new attribute without the old one, so nothing is currently
   reachable only through the new model and there is no data-loss pressure.
2. **It is not a rename.** `OrgUnit;level1` is an org-chart reporting line, not a
   department: `Orthopaedic Surgery` becomes `Hospital for Special Surgery`,
   `Library` becomes `Information Technologies and Services`. Which hierarchy the
   reporting table should express is a business decision.
3. **It would land inside the one clean diff.** The Splunk comparison is the only
   correctness oracle for 37 columns of undocumented business rules. Changing
   3-9% of values during the port spends it for nothing.

Division stays on ASMS. `OrgUnit;level2` covers ~7% of faculty records, against
ASMS's full coverage, so sourcing division from ED would be a large regression.
Dropping the ASMS dependency is the biggest durability win available here and is
worth revisiting once level2 fills in — but not yet.

`--dry-run` prints old and new attribute coverage each run
(`ORGUNIT_MIGRATION_WATCH`). When old coverage starts falling, that is the signal
to plan the switch, with time in hand rather than after a breakage.

Sampling caveat: the figures above come from the first 400-1,500 entries LDAP
returned, not a random sample. Rerun over the full population before acting on
the 9%.
