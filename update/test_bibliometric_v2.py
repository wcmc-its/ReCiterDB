#!/usr/bin/env python3
"""v3.11 part 2 (plan PR R2, PM #1105 5h) consistency checks, no database needed.

1. Both copies of the reporting SP (setup/populateAnalysisSummaryTables_v2.sql and
   setup/createEventsProceduresReciterDb.sql) carry the same R2 blocks, in the plan's order
   (2b < 2v2 < 2c, 3 < 3v2 < 4, 5 < 5v2 < 6, 6b < 6v2 < 7).
2. The R2 blocks are additive: they only write the v3.11 columns, the cell table and their own
   temp tables, and the original 4-table RENAME is still there for a DB without v3.11.
3. Exact arithmetic: pctExact is DECIMAL(5,2), avgTop DECIMAL(6,3); top-N articles are picked
   with ROW_NUMBER(), the cohort rank is RANK(); a cohort under 20 has no rank.
4. The migration, the fresh-build DDL and the SP agree on the column names; the migration is
   guarded by information_schema and drops / truncates nothing.
5. The swap, cleanup_staging_tables_v2 and restore_from_backup_v2 (both SP copies and the
   standalone helper files) handle analysis_summary_person_peer.

Run: python3 update/test_bibliometric_v2.py
"""
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SETUP = os.path.join(ROOT, "setup")


def read(name):
    with open(os.path.join(SETUP, name)) as f:
        return f.read()


ARTICLE_COLS = ["provisionalNIH", "hasPercentileNIH", "pctExact", "researchArticleV2", "ratedV2", "recentV2"]
AUTHOR_COLS = ["authorRoleV2", "soleAuthor"]
PEER_COLS = ["personIdentifier", "facultyRank", "cohortEligible", "roleSet", "topN", "ratedCount",
             "recentCount", "avgTop", "cohortRank", "cohortM", "runDate", "iciteLoadedAt"]
V2_COLS = set(ARTICLE_COLS + AUTHOR_COLS + PEER_COLS)
TEMP_TABLES = {"temp_v2_sole_pmid", "temp_v2_rank", "temp_v2_works", "temp_v2_roleset", "temp_v2_cohort"}
checks = 0


def ok(cond, msg):
    global checks
    assert cond, msg
    checks += 1


def proc(sql, name, end):
    i = sql.index(f"CREATE PROCEDURE `{name}`()")
    return sql[i:sql.index(end, i)]


def r2_blocks(text):
    """Every '-- >>> R2 (v3.11' ... '-- <<< R2 (v3.11' segment, in file order."""
    return re.findall(r"-- >>> R2 \(v3\.11.*?-- <<< R2 \(v3\.11[^\n]*\n", text, re.S)


standalone = read("populateAnalysisSummaryTables_v2.sql")
combined = read("createEventsProceduresReciterDb.sql")
main_s = proc(standalone, "populateAnalysisSummaryTables_v2", "END proc_main")
main_c = proc(combined, "populateAnalysisSummaryTables_v2", "END proc_main")

# 1. same blocks in both copies, in order
blocks = r2_blocks(main_s)
ok(len(blocks) == 8, f"expected 8 R2 blocks in the main SP, found {len(blocks)}")
ok(blocks == r2_blocks(main_c), "R2 blocks differ between populateAnalysisSummaryTables_v2.sql and createEventsProceduresReciterDb.sql")
for helper, end in (("cleanup_staging_tables_v2", "END"), ("restore_from_backup_v2", "\nEND")):
    ok(r2_blocks(proc(standalone, helper, end)) == r2_blocks(proc(combined, helper, end)) != [],
       f"{helper}: R2 blocks differ between the two SP copies")

order = ["SET v_step = '2b. Equal contribution authors'", "SET v_step = '2v2. Bibliometric v2 author role'",
         "SET v_step = '2c. Populate author_list'", "SET v_step = '3. Populate analysis_summary_article'",
         "SET v_step = '3v2. Bibliometric v2 article flags'", "SET v_step = '4. Populate analysis_summary_person'",
         "SET v_step = '5. Compute percentile rankings'", "SET v_step = '5v2. Bibliometric v2 cohort cells'",
         "SET v_step = '6. Compute h-index (set-based)'", "SET v_step = '6b. Union external-source publications'",
         "SET v_step = '6v2. Bibliometric v2 external rows'", "SET v_step = '7. Atomic table swap'"]
for body in (main_s, main_c):
    pos = [body.index(o) for o in order]
    ok(pos == sorted(pos), "steps out of order: " + str(pos))

# 2. additive: writes only v2 columns / cell table / own temp tables
r2 = "".join(blocks)
for m in re.finditer(r"\bUPDATE\s+(\w+)(?:\s+(?!SET\b)\w+)?\s.*?\bSET\b(.*?);", r2, re.S):
    table, sets = m.group(1), m.group(2)
    for col in re.findall(r"(?:^|,)\s*(?:\w+\.)?(\w+)\s*=", sets.split(" WHERE ")[0], re.M):
        ok(col in V2_COLS or table in TEMP_TABLES,
           f"R2 block writes {table}.{col}, which is not a v3.11 column")
for t in re.findall(r"\bINSERT\s+(?:IGNORE\s+)?INTO\s+(\w+)", r2):
    ok(t == "analysis_summary_person_peer_new" or t in TEMP_TABLES, f"R2 block inserts into {t}")
for t in re.findall(r"\bDROP\s+TABLE\s+IF\s+EXISTS\s+(\w+)", r2):
    ok(t in {"analysis_summary_person_peer_new", "analysis_summary_person_peer_backup"} | TEMP_TABLES,
       f"R2 block drops {t}")
ok("TRUNCATE" not in r2.upper(), "R2 block truncates")
v1_rename = ("    RENAME TABLE\n"
             "        analysis_summary_author TO analysis_summary_author_backup,\n"
             "        analysis_summary_author_new TO analysis_summary_author,\n"
             "        analysis_summary_article TO analysis_summary_article_backup,\n"
             "        analysis_summary_article_new TO analysis_summary_article,\n"
             "        analysis_summary_person TO analysis_summary_person_backup,\n"
             "        analysis_summary_person_new TO analysis_summary_person,\n"
             "        analysis_summary_author_list TO analysis_summary_author_list_backup,\n"
             "        analysis_summary_author_list_new TO analysis_summary_author_list;\n")
for body in (main_s, main_c):
    ok(body.count(v1_rename) == 1, "the original 4-table RENAME must stay (DB without v3.11)")
    ok(body.count("analysis_summary_person_peer_new TO analysis_summary_person_peer") == 1,
       "STEP 7 must swap the cell table in the same RENAME")
    # every v2 step is isolated: it clears its own error so it can't abort the swap
    for step in ("2v2", "3v2", "5v2", "6v2"):
        seg = body[body.index(f"SET v_step = '{step}."):]
        seg = seg[:seg.index("-- <<< R2 (v3.11)")]
        ok("SET v_error_occurred = 0;" in seg, f"STEP {step} must clear its own error")
        ok("LEAVE proc_main" not in seg, f"STEP {step} must not leave the procedure")

# 3. exact arithmetic and ranking
s5 = main_s[main_s.index("SET v_step = '5v2."):main_s.index("-- <<< R2 (v3.11): STEP 5v2")]
ok("CAST(ROUND(n.nih_percentile, 2) AS DECIMAL(5,2))" in main_s, "pctExact must be DECIMAL(5,2)")
ok("AS DECIMAL(6,3)) AS avgTop" in s5, "avgTop must be DECIMAL(6,3)")
ok(re.search(r"ROW_NUMBER\(\) OVER \(PARTITION BY w\.personIdentifier, s\.roleSet\s+ORDER BY w\.pct DESC, w\.rcr DESC, w\.pmid ASC\)", s5),
   "top-N article selection must be ROW_NUMBER() over pct, RCR, pmid")
ok(re.search(r"RANK\(\) OVER \(PARTITION BY facultyRank, roleSet, topN ORDER BY avgTop DESC\)", s5),
   "cohort rank must be RANK() over avgTop within rank x role set x N")
ok(s5.count("RANK() OVER") == 1 and "DENSE_RANK" not in s5, "RANK() only for the cohort rank")
ok("IF(c.cohortM >= 20, c.cohortRank, NULL)" in s5, "a cohort under 20 has no rank")
ok("HAVING COUNT(*) = k.topN" in s5, "avgTop only when N rated articles exist")
ok("percentileNIH" not in r2, "v2 reads pctExact, never the FLOAT percentileNIH")
s3 = main_s[main_s.index("SET v_step = '3v2."):main_s.index("-- <<< R2 (v3.11): STEP 3v2")]
for t in ("Preprint", "Review", "Letter", "Comment", "Editorial Article", "Conference Paper", "Erratum", "Retraction"):
    ok(f"'{t}'" in s3, f"research-article exclusion list lacks {t}")
ok("COALESCE(a.provisionalNIH, 0) <> 1" in s3, "provisional is never rated")
ok(">= 1980" in s3 and "CURDATE() - INTERVAL 24 MONTH" in s3, "year floor / 24-month recent window")
s2 = main_s[main_s.index("SET v_step = '2v2."):main_s.index("-- <<< R2 (v3.11): STEP 2v2")]
ok("HAVING MAX(`rank`) = 1" in s2 and "THEN 'senior'" in s2, "a sole author is senior")

# 4. migration / fresh-build DDL / SP agree
mig = read("alter_analysis_summary_bibliometric_v2_v3.11.sql")
ddl = read("createDatabaseTableReciterDb.sql")
ddl_article = re.search(r"CREATE TABLE IF NOT EXISTS `analysis_summary_article` \((.*?)\) ENGINE", ddl, re.S).group(1)
ddl_author = re.search(r"CREATE TABLE IF NOT EXISTS `analysis_summary_author` \((.*?)\) ENGINE", ddl, re.S).group(1)
peer_mig = re.search(r"CREATE TABLE IF NOT EXISTS `analysis_summary_person_peer` \(.*?\) ENGINE[^;]*;", mig, re.S).group(0)
peer_ddl = re.search(r"CREATE TABLE IF NOT EXISTS `analysis_summary_person_peer` \(.*?\) ENGINE[^;]*;", ddl, re.S).group(0)
ok(peer_mig == peer_ddl, "analysis_summary_person_peer differs between the migration and the fresh-build DDL")
for c in ARTICLE_COLS:
    ok(f"ADD COLUMN `{c}`" in mig and f"`{c}`" in ddl_article and c in r2, f"article column {c} not in migration/DDL/SP")
for c in AUTHOR_COLS:
    ok(f"ADD COLUMN `{c}`" in mig and f"`{c}`" in ddl_author and c in r2, f"author column {c} not in migration/DDL/SP")
for c in PEER_COLS:
    ok(f"`{c}`" in peer_mig, f"cell table lacks {c}")
ok("`pctExact` DECIMAL(5,2)" in mig and "`avgTop` decimal(6,3)" in peer_mig, "exact types in the migration")
ok("PRIMARY KEY (`personIdentifier`, `roleSet`, `topN`)" in peer_mig, "cell table primary key")
ok("KEY `idx_peer_cohort` (`facultyRank`, `roleSet`, `topN`, `avgTop`)" in peer_mig, "cell table cohort index")
ok(mig.count("information_schema.columns") >= 2, "migration ALTERs must be guarded by information_schema")
code = "\n".join(l for l in mig.splitlines() if not l.lstrip().startswith("--"))
ok(not re.search(r"\bTRUNCATE\b|\bDROP\s+(TABLE|COLUMN)\b(?!.*TEMPORARY)", code.replace("DROP TEMPORARY TABLE", "")),
   "migration must not drop or truncate anything but its own temporary tables")

# 5. helpers: both SP copies and the standalone dumps
for name, text in (("standalone", standalone), ("combined", combined)):
    ok("DROP TABLE IF EXISTS analysis_summary_person_peer_new;" in proc(text, "cleanup_staging_tables_v2", "END"),
       f"{name} cleanup_staging_tables_v2 must drop the staged cell table")
    ok("analysis_summary_person_peer_backup TO analysis_summary_person_peer" in proc(text, "restore_from_backup_v2", "\nEND"),
       f"{name} restore_from_backup_v2 must restore the cell table")
ok("DROP TABLE IF EXISTS analysis_summary_person_peer_new;" in read("cleanup_staging_tables_v2.sql"),
   "setup/cleanup_staging_tables_v2.sql lacks the cell table")
ok("analysis_summary_person_peer_backup TO analysis_summary_person_peer" in read("restore_from_backup_v2.sql"),
   "setup/restore_from_backup_v2.sql lacks the cell table")

print(f"PASS test_bibliometric_v2 ({checks} checks)")
