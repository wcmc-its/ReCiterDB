# CWID remediation, 2026-11

One-time cleanup of the non-canonical person ids (`Abl9002`, `'ggi9001 '`, `kims7@mskcc.org`, `DLAMB` and the rest) found on 11-18. This is Phase 2 of the CWID normalization plan (`Projects/Roadmap/cwid-normalization-plan.md`).

None of these scripts has been run against prod. Each one defaults to a dry run.

## Files

| File | What it does |
|---|---|
| `mapping.json` | The single list of decisions: bad uid → canonical uid, and the action for each. Everything else reads it. |
| `mysql_remediation.sql` | reciterdb (MariaDB), in one transaction that ends in `ROLLBACK` until you switch it. |
| `dynamo_remediation.py` | DynamoDB and S3. Dry run by default; `--apply` writes; `--restore` rolls back. |
| `rerun_canonical.sh` | Runs each canonical uid on prod with `analysisRefreshFlag=true&retrievalRefreshFlag=ALL_PUBLICATIONS`. |

## Decisions in `mapping.json`

- **MAP (26).** Merge the bad uid into the canonical CWID, then delete the bad one:
  - `Ana2037`, `Kym9003`, `'ggi9001 '`, `Abl9002`, `Jod2033`, `Arn4002`, `EMH2012`, `SHP2010`, `'xoprotop '`, `'jih9026 '`, `Anc2032`, `Hlloyd`, `Res2011`, `SMB9009`, `' Isk9008'`, `'ars2027 '`, `HAF9018`, `Nes9050`: trimmed and lowercased.
  - `ldn9002@nyp.org`→`ldn9002`, `tfm9003@nyp.org`→`tfm9003`, `kims7@mskcc.org`→`ski4009`, `Minkowitz`→`bam9012`, `DLAMB` and `dlamb`→`djl2008`, `jason.carmel`→`jbc7001`, `str2025-mi`→`str2025`.
  - `kym9003` is Kyle Merandy in ED. Its Identity name (Annie Rohan, who is really `ajr7002`) is corrected to Kyle Merandy.
- **DELETE (5).** No real CWID: `NA` and `na` (Shama Farooq, no ED CWID), `Hello, world!`, `margarita.todor`, `rodman.john`.
- **UNDERSCORE (17).** Legacy PM external logins. Each is deleted if its GoldStandard holds no assertions, and otherwise left alone as NEEDS-DECISION.

### Change from the 11-18 instructions: the four underscore "merges" are not done

The instructions were to merge `_jfr9006`, `_joc9251`, `_pab4001` and `_pah4001` into the ED CWID without the underscore. The dry run showed that each one is a **different person** whose external id only looks like a CWID:

| Underscore id | Its Identity record | ED owner of the CWID without the underscore | Its GoldStandard |
|---|---|---|---|
| `_pah4001` | Paul A. Harris, Vanderbilt | `pah4001` = Parviz Daniel Hejazi Pastor (MD-PhD student) | 111 known, 25 rejected |
| `_joc9251` | Jörn Coers, Duke | `joc9251` = Jordan Chanler-Berat | 82 known |
| `_pab4001` | Michael Albert Bales | `pab4001` = Paige Brabant | 16 known, 3 rejected |
| `_jfr9006` | "Friedman Friedman" | `jfr9006` = Hayley Costas | none |

Merging `_pah4001` alone would have added 111 of Paul Harris's papers to a WCM student's accepted list. All four are now UNDERSCORE:
- `_jfr9006` has no assertions, so it is deleted.
- The other three are NEEDS-DECISION.

`dynamo_remediation.py` now also refuses any MAP where the Identity's surname doesn't match ED's (`ed_last` in `mapping.json`). It reports the uid as `ABORT` and writes nothing for it.
- `ars2027 ` needed an alias for this check: its CTSC record spells the surname "Shakrokni".

### NEEDS-DECISION (not touched by any script)

These hold GoldStandard assertions under an id with no ED CWID:

| Id | Name on Identity | GoldStandard (known/rejected) |
|---|---|---|
| `_joc9251` | Jörn Coers | 82/0 |
| `_pab4001` | Michael Albert Bales | 16/3 |
| `_pah4001` | Paul A. Harris | 111/25 |
| `_dar7342` | none | 36/5 |
| `_djulius` | David Julius | 75/89 |
| `_huangfud` | none | 47/0 |
| `_jpp2757` | none | 15/12 |
| `_lal2055` | none | 122/6 |
| `_lsw8099` | Lakshmi Swamy | 7/4 |
| `_olo2760` | none | 62/0 |
| `_pac2224` | none | 82/15 |
| `_tap2707` | none | 1/21 |
| `_tem2089` | none | 0/2 |
| `_uk28` | Ursula Krotscheck | 43/0 |

They look like external-cohort people, and they are probably best kept as they are. The new uid validation already allows `_` (`[a-z0-9._-]`), so keeping them doesn't block anything. If they are kept, nothing more is needed.

`_pan9087`, `_xyz1234` and `_jfr9006` have no assertions and are deleted.

## PM accounts (`admin_users`)

- **Junk accounts.** These are deleted, together with their roles, departments, notification preferences and notification log:
  - the five accounts that aren't CWIDs: `WCMCWID`, `Mahender Jangari`, `NewName123`, `ReCiterBot`, `Sbackovic`;
  - any account under a DELETE-class id (`NA`).

  **Exception:** an account that authored `admin_feedback_log` rows is kept with `status = 0` and no roles, so the curator history keeps its `userID`. The dry-run section lists `n_feedback_authored` for each junk account.
- **ReCiterBot.** I searched for anything that logs in as or names `ReCiterBot`:
  - No code references it: not PM, ReCiterDB, ReCiter, the Institutional Client, ReciterAI, SPS, or the scripts in ReCiter Research.
  - It appears once in data: one GoldStandard `auditLog` entry, `uid: "ReCiterBot"`, `userVerbose: "ReCiter Bot"`, ACCEPTED pmid 21927451 on 2025-06-10, found in the `ReCiter Research/analysis/goldstandard_cleanup_2026-08-20/` backups.
  - So something did write as that account once in 2025. Nothing in our code does now.
- **Duplicate and bad-case accounts.**
  - Each account's canonical id is its mapped id, or else its trimmed, lowercased id. External-cohort ids (`usc_`, `fh_`, `fredhutch_`, `ucsf_`, `uc`) are only trimmed.
  - Each group of accounts with the same canonical id keeps its lowest `userID`.
  - The other accounts' roles, departments, notification log, feedback-log `userID`/`impersonatedByUserID` and (when the keeper has none) notification preferences move to the keeper. Then those accounts are deleted.
  - The keeper's `personIdentifier` is set to the canonical id. This covers the 15 duplicate pairs (for example `mrj4001`: 13 and 40078 → keep 13) and the 11 bad-case accounts.
  - Example: `dlamb` (5949) merges into `djl2008` if that account exists, and is otherwise renamed.
- **Roles and departments.** Duplicate `(userID, roleID)` and `(userID, departmentID)` rows are removed everywhere. This includes userID 46 (blank `personIdentifier`, 286 copies each of roles 3 and 4), which ends with one row of each. The script prints the row.

## Run order

Window: **after the inst-client run has finished and before the 14:30 UTC ReCiterDB nightly.**

0. **Preconditions.**
   - The IC CTSC fix is deployed, or the CTSC merge is off, or the cronjob is suspended:
     `kubectl -n reciter patch cronjob reciter-inst-client -p '{"spec":{"suspend":true}}'`
     Otherwise `'ggi9001 '`/`'jih9026 '` and the uppercase forms come back that night.
   - Hold PM curation for the uids in `mapping.json` during the window.
   - The read-side normalization in ReCiter and PM ships **after** this run (plan, Phase 1 ordering).
   - `unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY` before every aws/kubectl/python step.
1. **Self-check.** `python3 dynamo_remediation.py --selftest`. This also checks that the SQL's `rm_map` rows match `mapping.json`.
2. **MySQL backup.** Take a backup of the tables the SQL touches (they are small):
   ```
   mysqldump --defaults-file=<prod.cnf> --single-transaction reciterdb admin_users admin_users_roles \
     admin_users_departments admin_notification_preferences admin_notification_log admin_feedback_log \
     grant_provenance authorship_review reporting_ad_hoc_feature_generator_execution \
     reporting_ad_hoc_identity_creation identity_grad_school_faculty > pre_cwid_remediation.sql
   ```
3. **MySQL dry run.**
   ```
   mysql --defaults-file=<prod.cnf> -t reciterdb < mysql_remediation.sql | tee mysql_dryrun.log
   ```
   - Check that `@@hostname` is the database you mean.
   - Check that every `GUARD` count is 0.
   - Check that the FK list names only the five admin child tables.
   - Check that every `after` row says 0.
   - Never pass `--force`: any error stops the run and the open transaction rolls back.
4. **MySQL commit.**
   - In `mysql_remediation.sql`, comment out `ROLLBACK;` and uncomment `COMMIT;` (the last two lines).
   - Run it again and keep the log.
   - A third run changes 0 rows.
5. **DynamoDB dry run.** `python3 dynamo_remediation.py`.
   - Read the per-uid plan. Expect `ABORT=0`, `MAP=26`, `DELETE=8`, `NEEDS-DECISION=14`.
   - It writes `snapshots/snapshot_<UTC>.json` and the S3 objects it would delete.
6. **DynamoDB apply.** `python3 dynamo_remediation.py --apply` (you confirm by typing `apply`).
   - The run takes a fresh snapshot first.
   - A uid that fails is reported and the rest continue. A re-run is idempotent: merges become no-ops and the copies skip rows that already exist.
7. **Rerun.** `./rerun_canonical.sh` re-runs the 25 canonical uids on prod (`djl2008` is inactive, so nothing else would rescore it). Every line should be `200`.
8. **Force a re-login** for every account in the SQL's last result set ("accounts that changed"), so the old case leaves their JWT.
9. **Let the 14:30 UTC nightly run**, then run the verification below.
10. **Resume the IC** only once its fix is live:
    `kubectl -n reciter patch cronjob reciter-inst-client -p '{"spec":{"suspend":false}}'`

## Rollback

- **MySQL.**
  - Before COMMIT, nothing is changed.
  - After COMMIT, restore the step-2 dump of the affected tables. Only the tables in the dump are touched.
- **DynamoDB and S3.** Run `python3 dynamo_remediation.py --restore snapshots/snapshot_<UTC>.json --apply`, or add `--only <bad uid>` to restore one person. This:
  - puts back every bad-form item and row;
  - returns the canonical GoldStandard and Identity to their snapshot state (deleting them if they didn't exist), which loses any curation done on them since;
  - removes the FeedbackLog and ExternalArticle rows the remediation copied;
  - re-uploads the AnalysisOutput objects from their local copies. `reciter-dynamodb` is versioned too, but `AnalysisOutput/ldn9002@nyp.org` predates versioning (VersionId `null`), which is why the bytes are kept.

## Dry-run result (prod, read-only, 2026-11-18 19:29 UTC)

`MAP=26, DELETE=8, NEEDS-DECISION=14, ABORT=0`. The snapshot covered 73 uids and 27,897 rows.

The GoldStandard merges that add assertions match the plan: `Kym9003` +2, `Jod2033` +1, `SHP2010` +4, `SMB9009` +2, `Hlloyd` +2, `'xoprotop '` +3. Every other bad GoldStandard is empty or already contained in the canonical one, and no merge has a known-vs-rejected conflict.

The other writes:
- **New Identity records:** `bam9012` (from `Minkowitz`) and `xoprotop`. `kym9003` gets its name fixed.
- **FeedbackLog copies:** 8 rows to `ski4009` and 3 rows to `xoprotop`.
- **S3 deletes:** `AnalysisOutput/DLAMB`, `AnalysisOutput/dlamb` and `AnalysisOutput/ldn9002@nyp.org`.

## Verification (after one clean nightly)

```sql
-- person: no case/space duplicates, no non-canonical ids outside the external cohorts
SELECT LOWER(TRIM(personIdentifier)) c, COUNT(*) n FROM person GROUP BY c HAVING n > 1;
SELECT personIdentifier FROM person
WHERE BINARY personIdentifier <> BINARY LOWER(TRIM(personIdentifier))
  AND personIdentifier NOT REGEXP '^(usc_|fh_|fredhutch_|ucsf_|uc)';
-- PM accounts and roles
SELECT LOWER(TRIM(personIdentifier)) c, COUNT(*) FROM admin_users
WHERE TRIM(COALESCE(personIdentifier,'')) <> '' GROUP BY c HAVING COUNT(*) > 1;
SELECT userID, roleID, COUNT(*) FROM admin_users_roles GROUP BY userID, roleID HAVING COUNT(*) > 1;
-- the merged people now carry their accepted papers
SELECT personIdentifier, COUNT(*) FROM person_article
WHERE personIdentifier IN ('djl2008','shp2010','hlloyd','smb9009','jod2033','kym9003','xoprotop','bam9012')
  AND userAssertion = 'ACCEPTED' GROUP BY personIdentifier;
```

- **DynamoDB.** A dry run of `dynamo_remediation.py` should now show only NEEDS-DECISION entries and steps with nothing left to delete. A full rescan of Identity, GoldStandard and Analysis for uids that fail `^[a-z0-9._-]+$` (external cohorts excepted) should list only the NEEDS-DECISION ids.
- **PM.** Spot-check `djl2008`, `shp2010` and `hlloyd` on /curate.
- **After the next IC run.** `'ggi9001 '` and `'jih9026 '` have not come back.

## Left in place on purpose

- **FeedbackLog rows under the bad keys.** They are audit history, so they are copied, never deleted.
- **`feedbackscoring` S3 files under bad prefixes** (for example `SHP2010-feedbackIdentityScoringInput.json`). The bucket is versioned and nothing reads them by person.
- **`reciterdb.identity` rows `kims7@mskcc.org` and `tfm9003@nyp.org`.** That table is rebuilt from ED each night.
- **`authorship_review.candidate_cwids_json`.** A row that already listed the canonical CWID can now list it twice. The producer recomputes open rows on its next sweep.
- **Mixed-case values not in the map in `authorship_review` and `admin_feedback_log`.** Only mapped ids are rewritten, and these are compared with BINARY.
