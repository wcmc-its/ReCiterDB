#!/usr/bin/env python3
"""v3.10 (Reports peer review and role, PM #1105 5d-1) consistency checks, no database needed.

1. The STEP 6c block is identical in both copies of the reporting SP
   (setup/populateAnalysisSummaryTables_v2.sql and setup/createEventsProceduresReciterDb.sql),
   sits between STEP 6b and STEP 7, and guards on information_schema before touching a column.
2. The v3.10 migration, the fresh-build DDL and the SP agree on the column names, and the
   migration is idempotent and non-destructive (information_schema guards, no DROP/TRUNCATE of
   a real table).
3. The SQL rules equal PM's pubTypes.ts: the types peer reviewed by default, the MedEdPORTAL
   case for Educational Material, and the two Editor types. Checked against the copy below, and
   against the live file when PM_PUBTYPES_TS=<PM>/src/lib/pubTypes.ts is set.
4. A Python model of the SQL CASE agrees with defaultPeerReviewed / defaultRole on all 18 types.

Run: python3 update/test_peer_review_role.py
"""
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SETUP = os.path.join(ROOT, "setup")


def read(name):
    with open(os.path.join(SETUP, name)) as f:
        return f.read()


# pubTypes.ts (PM origin/master, Nov 11 2026): PEER_REVIEWED_BY_DEFAULT, canBePeerReviewed,
# defaultPeerReviewed's Educational Material case, defaultRole.
PEER_REVIEWED = {"Academic Article", "Review", "Conference Paper"}
EDITOR_TYPES = {"Edited Book", "Conference Proceedings"}
PUB_TYPES = ["Academic Article", "Review", "Letter", "Editorial", "Conference Paper", "Conference Abstract",
             "Conference Proceedings", "Book", "Edited Book", "Book Chapter", "Reference Entry", "Preprint",
             "Report", "Educational Material", "Non-Peer-Reviewed Writing", "Dataset", "Dissertation", "Software"]


def block(sql):
    m = re.search(r"\n    -- =+\n    -- STEP 6c: PEER REVIEW AND ROLE DEFAULTS.*?\n(?=    -- =+\n    -- STEP 7: ATOMIC TABLE SWAP)", sql, re.S)
    assert m, "STEP 6c block not found right before STEP 7"
    return m.group(0)


standalone = read("populateAnalysisSummaryTables_v2.sql")
combined = read("createEventsProceduresReciterDb.sql")
b = block(standalone)

# 1. identical copies, placed after 6b, guarded
assert b == block(combined), \
    "STEP 6c differs between populateAnalysisSummaryTables_v2.sql and createEventsProceduresReciterDb.sql"
for sql in (standalone, combined):
    assert sql.index("STEP 6b: UNION EXTERNAL-SOURCE") < sql.index("STEP 6c: PEER REVIEW") < sql.index("STEP 7: ATOMIC TABLE SWAP")
for tbl, col in [("analysis_summary_article_new", "peerReviewed"), ("analysis_summary_article_new", "pubType"),
                 ("analysis_summary_author_new", "role")]:
    assert re.search(rf"table_name = '{tbl}'\s+AND column_name = '{col}'", b), f"6c lacks a guard for {tbl}.{col}"
assert "IF v_error_occurred = 0 THEN" in b and "SET v_error_occurred = 0;" in b, "6c must isolate its own errors"
assert "publicationTypeCanonical" not in b and "citationCountScopus" not in b, "SPS columns must stay untouched"

# 2. column names agree; migration is guarded and non-destructive
migration = read("alter_analysis_summary_peer_review_role_v3.10.sql")
ddl = read("createDatabaseTableReciterDb.sql")
ddl_article = re.search(r"CREATE TABLE IF NOT EXISTS `analysis_summary_article` \((.*?)\) ENGINE", ddl, re.S).group(1)
ddl_author = re.search(r"CREATE TABLE IF NOT EXISTS `analysis_summary_author` \((.*?)\) ENGINE", ddl, re.S).group(1)
assert "`peerReviewed` tinyint" in ddl_article
assert "`peerReviewedSource` enum('type-default','curator')" in ddl_article
assert "`role` varchar(20)" in ddl_author
assert "ADD COLUMN `peerReviewed` TINYINT" in migration
assert "ADD COLUMN `peerReviewedSource` ENUM(''type-default'',''curator'')" in migration
assert "ADD COLUMN `role` VARCHAR(20)" in migration
assert migration.count("information_schema.columns") >= 2, "each ALTER must be guarded by information_schema"
code = re.sub(r"--[^\n]*", "", migration)
assert not re.search(r"\bTRUNCATE\b", code, re.I), "no TRUNCATE in a migration"
assert not re.search(r"\bDROP\s+(TABLE|COLUMN|INDEX)\b", code, re.I), "only temporary tables may be dropped"
assert re.search(r"a\.peerReviewed\s+=", b) and re.search(r"a\.peerReviewedSource\s+=\s+'type-default'", b)
assert re.search(r"au\.role\s+=", b)

# 3. SQL rule lists == pubTypes.ts
pr_in = re.search(r"WHEN a\.pubType IN \(([^)]*)\) THEN 1", b).group(1)
sql_pr = set(re.findall(r"'([^']+)'", pr_in))
assert sql_pr == PEER_REVIEWED, sql_pr
assert re.search(r"WHEN a\.pubType = 'Educational Material'\s+AND LOWER\(COALESCE\(a\.journalTitleVerbose, ''\)\) LIKE '%mededportal%' THEN 1", b)
ed_in = re.search(r"IF\(a\.pubType IN \(([^)]*)\), 'Editor', 'Author'\)", b).group(1)
sql_ed = set(re.findall(r"'([^']+)'", ed_in))
assert sql_ed == EDITOR_TYPES, sql_ed
assert sql_pr <= set(PUB_TYPES) and sql_ed <= set(PUB_TYPES)

ts_path = os.environ.get("PM_PUBTYPES_TS")
if ts_path:
    ts = open(ts_path).read()
    ts_pr = set(re.findall(r'"([^"]+)"', re.search(r"PEER_REVIEWED_BY_DEFAULT[^=]*= new Set\(\[([^\]]*)\]", ts).group(1)))
    assert ts_pr == sql_pr, f"pubTypes.ts PEER_REVIEWED_BY_DEFAULT {ts_pr} != SQL {sql_pr}"
    role = re.search(r"export const defaultRole = \(type: string\): PubRole =>\s*(.*?);", ts, re.S).group(1)
    assert set(re.findall(r'type === "([^"]+)"', role)) == sql_ed and '? "Editor" : "Author"' in role
    assert 'type === "Educational Material") return /mededportal/i.test' in ts
    assert 'canBePeerReviewed = (type: string): boolean => type !== "Non-Peer-Reviewed Writing"' in ts
    groups = re.search(r"export const PUB_TYPE_GROUPS = \[(.*?)\] as const;", ts, re.S).group(1)
    assert sorted(re.findall(r'"([^"]+)"', "".join(re.findall(r"types: \[([^\]]*)\]", groups)))) == sorted(PUB_TYPES)
    print("checked rules against", ts_path)


# 4. a Python model of the SQL agrees with pubTypes.ts on every type (and NULL)
def sql_peer(pub_type, venue):
    if pub_type in sql_pr:
        return 1
    if pub_type == "Educational Material" and "mededportal" in (venue or "").lower():
        return 1
    return 0


def ts_peer(pub_type, venue):  # defaultPeerReviewed
    if pub_type == "Non-Peer-Reviewed Writing":
        return 0
    if pub_type == "Educational Material":
        return int(bool(re.search("mededportal", venue or "", re.I)))
    return int(pub_type in PEER_REVIEWED)


for t in PUB_TYPES + [None]:
    for venue in [None, "", "MedEdPORTAL", "mededportal : the journal", "JAMA"]:
        assert sql_peer(t, venue) == ts_peer(t or "", venue), (t, venue)
    assert ("Editor" if t in sql_ed else "Author") == ("Editor" if t in EDITOR_TYPES else "Author")

print("PASS test_peer_review_role")
