#!/usr/bin/env python3
"""v3.7 (Reports pubType) consistency checks, no database needed.

1. setup/seed_pub_type_map.sql: every pub_type is one of PM's 18 types with the right group,
   keys are normalized like pubTypes.ts normSourceType, and every ReCiter type on PubMed rows
   has a RECITER row.
2. The STEP 3 and STEP 6b changes are identical in both copies of the reporting SP
   (setup/populateAnalysisSummaryTables_v2.sql and setup/createEventsProceduresReciterDb.sql).
3. The v3.7 migration, the fresh-build DDL and the SP agree on the column names.
4. With PM_PUBTYPES_TS=<PM>/src/lib/pubTypes.ts set, the seed equals a fresh generation.

Run: python3 update/test_pub_type_map.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pub_type_map_seed import RECITER, SEED_PATH, build_rows, norm, render  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SETUP = os.path.join(ROOT, "setup")


def read(name):
    with open(os.path.join(SETUP, name)) as f:
        return f.read()


GROUPS = {
    "journal": ["Academic Article", "Review", "Letter", "Editorial"],
    "conf": ["Conference Paper", "Conference Abstract", "Conference Proceedings"],
    "book": ["Book", "Edited Book", "Book Chapter", "Reference Entry"],
    "other": ["Preprint", "Report", "Educational Material", "Non-Peer-Reviewed Writing", "Dataset",
              "Dissertation", "Software"],
}
GROUP_OF = {t: g for g, ts in GROUPS.items() for t in ts}
NEW_COLS = ["pubType", "pubTypeGroup", "pubTypeCounted", "pubDate", "pubDatePrecision", "dateAdded",
            "publisher", "isbn", "editors"]

# 1. seed rows
seed = read("seed_pub_type_map.sql")
rows = re.findall(r"^  \('(\w+)', '([^']*)', (NULL|'[^']*'), (NULL|'[^']*'), ([01])\)[,;]$", seed, re.M)
assert len(rows) == seed.count("\n  ("), "every seed VALUES line must parse"
keys = set()
for src, key, typ, grp, counted in rows:
    typ = None if typ == "NULL" else typ.strip("'")
    grp = None if grp == "NULL" else grp.strip("'")
    assert (src, key) not in keys, f"duplicate key {src}:{key}"
    keys.add((src, key))
    assert norm(key) == key, f"{src}:{key} not normalized"
    if typ is None:
        assert grp is None and counted == "0", f"{src}:{key}: untyped rows are not counted"
    else:
        assert GROUP_OF.get(typ) == grp and counted == "1", f"{src}:{key} -> {typ}/{grp}"
for t in RECITER:
    assert ("RECITER", norm(t)) in keys, f"RECITER row missing for {t!r}"
assert {s for s, *_ in rows} == {"RECITER", "SCOPUS", "OPENALEX", "CROSSREF"}

# 2. both SP copies carry the same v3.7 blocks
def v37_blocks(sql):
    s3 = re.search(r"\n    -- 5c \(v3\.7, PM #1105\): canonical type.*?\n    END IF;\n", sql, re.S)
    b6 = re.search(r"\n        -- 5c \(v3\.7, PM #1105\): type, dates.*?CALL log_progress\(v_job_id, v_step, 'Set external Scimago SJR'.*?\n        END IF;\n", sql, re.S)
    assert s3 and b6, "v3.7 STEP 3 / 6b block not found"
    return s3.group(0), b6.group(0)

standalone = read("populateAnalysisSummaryTables_v2.sql")
combined = read("createEventsProceduresReciterDb.sql")
assert v37_blocks(standalone) == v37_blocks(combined), \
    "STEP 3 / 6b v3.7 blocks differ between populateAnalysisSummaryTables_v2.sql and createEventsProceduresReciterDb.sql"
assert standalone.count("pub_type_map") == combined.count("pub_type_map") > 0

# 3. column names agree across migration, fresh-build DDL and SP
migration = read("alter_analysis_summary_article_pubtype_v3.7.sql")
ddl = read("createDatabaseTableReciterDb.sql")
ddl_article = re.search(r"CREATE TABLE IF NOT EXISTS `analysis_summary_article` \((.*?)\) ENGINE", ddl, re.S).group(1)
for c in NEW_COLS:
    assert f"ADD COLUMN `{c}`" in migration, f"migration lacks {c}"
    assert f"`{c}`" in ddl_article, f"fresh-build DDL lacks {c}"
for c in NEW_COLS[:6]:
    assert re.search(rf"a\.{c}\s+=", v37_blocks(standalone)[1]), f"6b doesn't set {c}"
    if c not in ("publisher", "isbn", "editors"):
        assert re.search(rf"a\.{c}\s+=", v37_blocks(standalone)[0]), f"STEP 3 doesn't set {c}"
assert "CREATE TABLE IF NOT EXISTS `pub_type_map`" in migration and "CREATE TABLE IF NOT EXISTS `pub_type_map`" in ddl
assert "publicationTypeCanonical =" not in "".join(v37_blocks(standalone)), "publicationTypeCanonical must stay as is (SPS)"

# 4. seed == generation from pubTypes.ts, when PM is at hand
ts_path = os.environ.get("PM_PUBTYPES_TS")
if ts_path:
    assert render(build_rows(open(ts_path).read())) == open(SEED_PATH).read(), \
        "seed drifted from pubTypes.ts; run update/pub_type_map_seed.py --pubtypes ... --write"
    print("checked seed against", ts_path)

print(f"PASS test_pub_type_map ({len(rows)} seed rows)")
