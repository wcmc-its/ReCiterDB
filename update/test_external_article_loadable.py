#!/usr/bin/env python3
"""retrieveExternalArticles must skip rows the table can't hold, not fail the whole load.

The load TRUNCATEs first (an implicit commit), so one row whose sourceType the live
ENUM lacks (CROSSREF before the v3.4 DDL) or whose id is longer than VARCHAR(96) used to
leave external_article empty under strict SQL mode, and STEP 6b then injected nothing.

Run: python3 update/test_external_article_loadable.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from retrieveExternalArticles import loadable, to_row  # noqa: E402

OLD = {"SCOPUS", "WOS", "OPENALEX"}
NEW = OLD | {"CROSSREF"}

crossref = to_row({"uid": "abc1234", "articleId": "CROSSREF:10.1016/j.surg.2026.110815",
                   "sourceType": "CROSSREF", "doi": "10.1016/j.surg.2026.110815"})
openalex = to_row({"uid": "abc1234", "articleId": "OPENALEX:W4417001897", "sourceType": "OPENALEX"})
too_long = to_row({"uid": "abc1234", "articleId": "CROSSREF:10.1000/" + "x" * 90,
                   "sourceType": "CROSSREF"})
no_id = to_row({"uid": "abc1234", "sourceType": "SCOPUS"})

assert crossref["source_type"] == "CROSSREF" and crossref["article_id"].startswith("CROSSREF:10.")
assert loadable([crossref, openalex], OLD) == [openalex], "pre-DDL: CROSSREF skipped, rest kept"
assert loadable([crossref, openalex], NEW) == [crossref, openalex], "post-DDL: CROSSREF passes"
assert loadable([too_long, no_id, openalex], NEW) == [openalex], "over-long / missing ids skipped"
print("PASS test_external_article_loadable")
