#!/usr/bin/env python3
"""R3 (PM #1104 plan 3b-3e): STEP 6b reads external_article by status. No database needed.

1. The STEP 6b block is identical in both copies of the reporting SP
   (setup/populateAnalysisSummaryTables_v2.sql and setup/createEventsProceduresReciterDb.sql).
2. Which links count is decided once, in temp_external_live: status = 'ACCEPTED' when the
   projection has the v3.6 status column (a NULL status falls back to suppressed = 0), else
   suppressed = 0. No other statement in 6b filters external_article on suppressed itself.
3. external_record rows with a type_notice (erratum / retraction) stay out (plan decision 4),
   guarded on information_schema.
4. The supersede guard drops links whose normalized DOI is an ACCEPTED person_article row for
   the same person, normalized with external_record.doi_norm's own expression.
5. authorPosition takes target_author_index / author_count before the authorship_review label.

Run: python3 update/test_external_6b_status.py
"""
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SETUP = os.path.join(ROOT, "setup")


def read(name):
    with open(os.path.join(SETUP, name)) as f:
        return f.read()


def block(sql):
    m = re.search(r"\n    -- =+\n    -- STEP 6b: UNION EXTERNAL-SOURCE.*?\n(?=    -- =+\n    -- STEP 6c:)", sql, re.S)
    assert m, "STEP 6b block not found right before STEP 6c"
    return m.group(0)


standalone = read("populateAnalysisSummaryTables_v2.sql")
combined = read("createEventsProceduresReciterDb.sql")
b = block(standalone)
code = "\n".join(l for l in b.splitlines() if not l.strip().startswith("--"))

# 1. identical copies
assert b == block(combined), \
    "STEP 6b differs between populateAnalysisSummaryTables_v2.sql and createEventsProceduresReciterDb.sql"

# 2. one counted set, status first
assert re.search(r"table_name = 'external_article'\s+AND column_name = 'status'", code), "no guard on the status column"
assert "WHERE status = 'ACCEPTED' OR (status IS NULL AND suppressed = 0);" in code, "status branch missing"
assert code.count("suppressed = 0") == 2, "only the counted-set build may read suppressed (status NULL + pre-v3.6)"
assert "e.suppressed" not in code, "a 6b statement still filters external_article on suppressed"
assert "COLLATE=utf8mb4_unicode_ci" in code.split("CREATE TABLE temp_external_live", 1)[1].split(";", 1)[0], \
    "temp_external_live must be utf8mb4_unicode_ci (v1.8: a collation mismatch made 6b inject zero rows)"
# every later read of external_article joins to the counted set
reads = re.findall(r"FROM external_article e\n(\s+JOIN [^\n]+)", code)
assert reads and all("temp_external_live l" in r for r in reads), f"external_article read without the counted set: {reads}"
assert "SELECT DISTINCT article_id FROM temp_external_live" in code, "synthetic pmids must come from the counted set"
assert code.rstrip().count("DROP TABLE IF EXISTS temp_external_live;") == 2, "temp_external_live must be dropped at the end"

# 3. decision 4, guarded
for col in ("target_author_index", "type_notice"):
    assert re.search(rf"table_name = 'external_record'\s+AND column_name = '{col}'", code), f"no guard on external_record.{col}"
assert "WHERE r.type_notice IS NOT NULL;" in code, "erratum / retraction rows must stay out"

# 4. supersede guard, same normalization as the generated column
ddl = read("table_external_record.sql")
ddl_re = re.search(r"REGEXP_REPLACE\(LOWER\(TRIM\(`doi`\)\), ('[^']+')", ddl).group(1)
assert code.count(f"REGEXP_REPLACE(LOWER(TRIM(pa.doi)), {ddl_re}") == 1, "person_article DOI not normalized like doi_norm"
assert code.count(f"REGEXP_REPLACE(LOWER(TRIM(e.doi)), {ddl_re}") == 1, "external DOI not normalized like doi_norm"
assert "pa.userAssertion = 'ACCEPTED'" in code, "guard must match ACCEPTED PubMed articles only"
assert "pa.personIdentifier = u.uid" in code and "d.uid = l.uid" in code, "guard must match the same person"

# 5. position precedence
pos = re.search(r"CASE WHEN l\.target_author_index IS NOT NULL(.*?)ELSE p\.author_position END", code, re.S)
assert pos, "authorPosition must try target_author_index before the review label"
assert "WHEN l.target_author_index = 1 THEN 'first'" in pos.group(1)
assert "WHEN l.target_author_index = l.author_count THEN 'last'" in pos.group(1)
assert pos.group(1).index("target_author_index") < pos.group(1).index("WHEN p.uid IS NULL THEN e.author_position")
assert "IF(p.uid IS NULL OR l.pm_touched = 1, e.byline, p.authors)" in code, "a PM-written row must use its own byline"


# A Python model of the authorPosition CASE, checked against the ETL's _position rule
# (retrieveExternalArticles._position) for every index/count pair it can see.
def sql_position(index, count, review_label, has_review, etl_position):
    if index is not None:
        return "first" if index == 1 else "last" if index == count else None
    return etl_position if not has_review else review_label


def etl_position(index, count):
    if not index:
        return None
    return "first" if index == 1 else "last" if count and index == count else None


for count in (None, 1, 2, 3, 7):
    for index in range(1, (count or 3) + 1):
        assert sql_position(index, count, "x", True, "y") == etl_position(index, count), (index, count)
assert sql_position(None, None, "last", True, "first") == "last"
assert sql_position(None, None, None, False, "first") == "first"

print("PASS test_external_6b_status")
