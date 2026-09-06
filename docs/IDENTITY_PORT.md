# reciterdb.identity: Splunk to cron

## STATUS: LIVE since 2026-09-05. Splunk retired.

| | |
|---|---|
| job | CronJob `reciterdb-identity`, namespace `reciter`, `0 12 * * *` |
| entry point | `python3 buildIdentity.py` (no flags = live upsert) |
| image | tracks the pipeline; CodeBuild sets it alongside `reciterdb` |
| PRs | #211 (the port), #212 (image tracking, nodeSelector, upsert SQL) |
| Splunk | `ReCiter - output identity to db` DISABLED by Paul, 2026-09-05 |

**`identity` now has exactly one writer, and a dead job raises nothing.** The
table is cumulative, so every consumer keeps returning plausible rows while the
data silently ages. There is no staleness alert. Until one exists, this is the
check:

```sql
SELECT id, run_at, staged_rows, upserted
FROM identity_build_log ORDER BY id DESC LIMIT 5;
```

Expect a daily row near 33,388 with `upserted = 1`. A gap of more than ~26 hours
means the job has stopped and nothing else will tell you.

### If you have to roll back

Splunk is one click from being re-enabled and its lookup-building search was
never disabled, so it can resume writing without any rebuild. The job's own
history lives in `identity_build_log`. `uq_identity_cwid` on `identity.cwid` is
required by the upsert; dropping it disables the new job but does not affect
Splunk.

### What shipped beyond a like-for-like port

- **12,912 people gained a department** they never had. Splunk's `append`
  subsearches were being silently truncated, so the data was always in ED and
  simply never landed.
- **417 people gained rows entirely.**
- ASMS divisions matched Splunk's exactly: 1 row differing across 32,971.

## What the port replaces

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

## PREFER_ORGUNIT

Ships `False`, so the first Splunk diff is empty. Flipping it to `True` moves
departments and student programs onto ED's orgUnit model. Verified against live
ED 2026-09-05 by running each source with the flag both ways:

| column | cwids | changed | blanked |
|---|---:|---:|---:|
| `primaryAcademicDepartment` | 11,213 | 523 | **0** |
| `inactiveDepartment` | 24,631 | 761 | **0** |
| `primaryAcademicDivision` (backfill) | 24,631 | +2,090 gained | **0** |
| `primaryProgram` | 1,309 | 169 | **0** |
| `program` | 5,638 | 428 | **0** |

**Deepest level wins** -- `coalesce(L2, L1, department)`. L1 is the parent org and
L2 the actual unit: Library sits at L2 under an L1 of "Information Technologies
and Services", so taking L1 would file every librarian under ITS. Where L2
exists, the old department matched L1 in 0 of 299 records.

**The inactive path SPLITS the levels instead of flattening them.** Role records
carry the hierarchy that SOR records mostly lack -- L2 on ~37% of expired role
records against 3.4% of faculty SOR records -- so L1 becomes the department and
L2 the division, which is the same shape ASMS gives an active colleague:

    L1 -> inactiveDepartment       "Medicine"
    L2 -> primaryAcademicDivision  "Infectious Diseases"

Flattening to deepest would have put a division in the department column and
made expired faculty the only cohort whose `primaryAcademicDepartment` means
something different from everyone else's.

The 761 department changes that remain are genuine historical renames --
`Healthcare Policy and Research` -> `Population Health Sciences`, `Biochemistry`
-> `Biochemistry and Biophysics`, `Physiology and Biophysics` -> `Systems and
Computational Biomedicine`. `Psychiatry` -> `Hospital Programs`, which looked
like a downgrade under flattening, is now correctly a division.

The division half is a real backfill: 2,090 expired faculty gain a division they
have never had, in ASMS's own vocabulary (`General Internal Medicine` 309,
`Hematology and Medical Oncology` 214, `Cardiology` 109, `Infectious Diseases`
101). ASMS cannot supply these -- its query requires a live appointment, which is
why 72% of the table has no division. ASMS still wins wherever it has a value:
`asms_division` is registered first in `SOURCES` and `merge()` keeps the first
non-empty.

Students read `orgUnit;level2` and normalise through the existing
`PROGRAM_OVERRIDE` table; ED's L2 spellings were added to it rather than a
second mechanism being built. Note ED's quirks: `TriI` without the hyphen,
`System Biology` without the plural.

Two behaviour changes beyond the source swap, both deliberate:

- `primaryProgram` was never normalised before. Under the flag it goes through
  the same override table as `program`, so the two columns cannot disagree.
  That is why 87 `Tri-I Program in Computational Biology & Medicine` become
  `Computational Biology & Medicine`.
- 62 MD-PhD students move from the generic `MD-PhD Program` to their specific
  field (`Neuroscience`, `Immunology & Microbial Pathogenesis`, ...) because L2
  names the field where the old attribute did not. This regroups them, it does
  not merely rename them, and `program` feeds the
  `primaryAcademicDepartment` fallback chain.

## First full dry-run, 2026-09-05

All 16 sources against live ED and ASMS, in-cluster. `asms_division` returned
9,752 cwids -- its first execution ever, against 9,737 divisions in the live
table. Build: 50,033 cwids merged, 33,388 after filters, staged.

| | rows |
|---|---:|
| in both | 32,971 |
| only in port | 417 |
| only in Splunk (cumulative residue, untouched by the upsert) | 2,477 |

Column differences across the 32,971 shared rows:

| column | differing |
|---|---:|
| `givenName` | 0 |
| `surname` | 1 |
| `primaryAcademicDivision` | 1 |
| `primaryProgram` | 244 |
| `primaryTitle` | 266 |
| `primaryOrg` | 263 |
| `primaryAcademicDepartment` | 12,495 gained, 0 lost, 343 changed |

Six defects the dry run caught that inspection had not:

1. **Staging silently rolled back.** pymysql opens an implicit transaction; a
   stage-only run returned before any commit and `conn.close()` discarded the
   rows. `DROP`/`CREATE` implicitly commit, so the empty table looked real.
2. **Every person-type flag empty.** `weillCornellEduPersonTypeCode` is
   multi-valued (up to 17 values); `get()` returns the first, always the least
   specific `academic`. Fixed with `_Row.all()`. This alone moved 8,916 people
   through the final filter.
3. **`""` written where the live table uses `NULL`**, inflating every column
   diff to ~90%.
4. **`program` normalisation skipped when `PREFER_ORGUNIT` was off** -- a
   regression from refactoring onto `_program_value`. The SPL's `case()` ran
   unconditionally.
5. **Priority ranking broken by (4)**: `PROGRAM_PRIORITY` is keyed on normalised
   names, so `MD-PhD WGS Neuroscience` scored 999 instead of 6 and lost to
   `MD-PhD Program`.
6. **The floor gate baselined on a cumulative table.** A healthy build stages
   33,388 rows against 35,448 live -- 93%, already under a naive 95% floor, and
   the residue only grows. Now baselined on prior builds via
   `identity_build_log`.

(2), (4) and (5) are pinned by assertions in `--demo`.

### Historical flags survive the upsert

`residentNYP` is `yes` on 2,194 live rows and 1,146 staged. **1,040 of the
difference are absent from staging entirely**, so the upsert never touches those
rows and the historical value persists; only 8 would change, and those are people
in the current build who are no longer residents. `fullTimeFaculty` and
`inactiveFaculty`: zero would be cleared. Upserting only what you rebuild is what
makes the cumulative table safe.

## What the upsert actually changes

Comparing raw `identity_staging` against `identity` overstates the change: the
upsert is `COALESCE(VALUES(col), col)`, so a NULL never overwrites. Measure with
COALESCE semantics to see the real change set:

```sql
SELECT SUM(NOT (COALESCE(s.primaryProgram, i.primaryProgram) <=> i.primaryProgram))
FROM identity i JOIN identity_staging s ON s.cwid = i.cwid;
```

| column | raw diff | actually changes |
|---|---:|---:|
| `givenName` | 0 | 0 |
| `fullTimeFaculty` | 8,543 | **0** |
| `surname` | 1 | 1 |
| `primaryAcademicDivision` | 1 | 1 (a gain) |
| `primaryOrg` | 263 | **2** |
| `primaryProgram` | 244 | **5** |
| `primaryTitle` | 266 | **5** |
| `primaryAcademicDepartment` | 13,176 | 12,680 (12,495 gains, 185 changes) |

Everything that collapsed was a NULL-overwrite artifact. The port reaches 8,916
people Splunk's truncated output never did, and for those people the columns they
do not qualify for come back empty; without the COALESCE guard the first run
would have erased 237 `primaryProgram` and 261 `primaryOrg` values.

The 185 department changes are ED updates Splunk has gone stale on --
`Healthcare Policy and Research` -> `Population Health Sciences` (26),
`Physiology and Biophysics` -> `Systems and Computational Biomedicine` (9),
`Institute for Computational Biomedicine` -> `Systems and Computational
Biomedicine` (4). The 12 remaining program/title/org changes are two rows gaining
values and three MD students who moved into the PhD phase (`Medical Student` ->
`Graduate Student`). All corrections, none regressions.

## Bugs that only appeared on the live path

Three defects survived every dry run, because `--dry-run` returns before the
upsert and my hand-built test pods differed from the manifest. Recorded because
each is a class of mistake, not a one-off:

1. **`nodeSelector: lifecycle: Ec2Spot` matches no node.** Copied from
   `k8-cronjob.yaml`, which has drifted from the live object. The first run sat
   `Pending` for eleven minutes with no error and no log line --
   `FailedScheduling: 0/13 nodes are available`. No node has carried that label
   since the nodegroups were recreated during the subnet-`0d35` recovery on
   2026-09-02. The live `reciterdb` cronjob has no nodeSelector at all, so
   neither does this one. **`k8-cronjob.yaml` still contains it.**
2. **`(1052, "Column 'surname' in UPDATE is ambiguous")`.** The upsert runs as
   `INSERT INTO identity ... SELECT ... FROM identity_staging`, so both tables
   are in scope and bare column names in `ON DUPLICATE KEY UPDATE` are rejected.
   Targets must be written `` `identity`.`col` ``.
3. **The staging load was silently rolled back.** pymysql opens an implicit
   transaction and does not autocommit; a stage-only run returned before any
   commit and `conn.close()` discarded every row. `DROP`/`CREATE` implicitly
   commit, so the empty table looked real and the diff read as total failure.

The lesson worth carrying: **a safety mode that stops short of the dangerous
operation gives false confidence about exactly the part you most want confidence
in.** `--demo` now asserts on the generated SQL, which catches (2) offline.
Better still would be for `--dry-run` to execute the upsert against a scratch
copy and roll back, so the statement is genuinely exercised. Not done.

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
