#!/usr/bin/env python3
"""retrieveExternalArticles: DynamoDB -> external_record sync and the projection rebuilt from it.

Offline (no DB, no AWS). Checks the R2 invariants (PM #1104, plan 3b-3e):
  - an untouched synced row projects back to exactly the DynamoDB-only row (identical
    projection), for Scopus, Crossref books, editor-only books and suppressed rows;
  - status / provenance / target_author_index mapping;
  - the sync inserts absent rows, updates only untouched DYNAMO rows, never touches a
    PM-owned row, and marks a vanished item REMOVED only on a full, sane scan;
  - REMOVED stays out of the projection; a PM-edited row projects from its columns.

Run: python3 update/test_external_record_sync.py
"""
import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import retrieveExternalArticles as etl  # noqa: E402

fails = []


def check(cond, msg):
    if not cond:
        fails.append(msg)


SCOPUS = {"uid": "abc1234", "articleId": "SCOPUS:105004692561", "sourceType": "SCOPUS",
          "doi": "10.1590/1982-0224-2024-0076", "title": "Molecular phylogenetics &amp; a <i>new</i> genus",
          "journalOrVenue": "Neotropical Ichthyology", "pubDate": "2025-01-01",
          "publicationType": "Article", "addedBy": "drw2004", "method": "scopus-authorships-tab",
          "dateAdded": "2026-10-20T01:34:24.368715376Z", "suppressed": False,
          "authors": ["Brian L. Sidlauskas", "Bruno F. Melo", "José L.O. Birindelli", "Anna Smith"]}
CROSSREF_BOOK = {"uid": "gak2008", "articleId": "CROSSREF:10.1007/978-3-031-00001-1",
                 "sourceType": "CROSSREF", "doi": "10.1007/978-3-031-00001-1", "title": "A Book",
                 "pubDate": "2024", "publicationType": "book", "addedBy": "paa2013", "method": "api-isbn",
                 "dateAdded": "2026-09-01T10:00:00Z", "suppressed": False, "authors": ["G A Koretzky"],
                 "rawRecord": json.dumps({"author": [{"given": "Gary A.", "family": "Koretzky"},
                                                     {"given": "Lee", "family": "Park"}],
                                          "editor": [{"given": "Ed", "family": "Itor"}],
                                          "ISBN": ["9783031000011"], "publisher": "Springer"})}
EDITED = {"uid": "xyz9999", "articleId": "CROSSREF:10.1/ed", "sourceType": "CROSSREF",
          "title": "Edited volume", "pubDate": "2023-05", "addedBy": "xyz9999", "method": "dropdown-search",
          "dateAdded": "2026-08-01T00:00:00.5Z", "suppressed": False,
          "rawRecord": json.dumps({"editor": [{"given": "Xavier", "family": "Zed"}]})}
SUPERSEDED = dict(SCOPUS, articleId="SCOPUS:2", suppressed=True, supersededByPmid=41234567)
REJECTED = dict(SCOPUS, articleId="SCOPUS:3", suppressed=True)
ODD = {"uid": "abc1234", "articleId": "OPENALEX:W4232659898", "sourceType": "OPENALEX",
       "method": "openalex-verification-test", "title": None, "authors": None}
ITEMS = [SCOPUS, CROSSREF_BOOK, EDITED, SUPERSEDED, REJECTED, ODD]
LAST = {"abc1234": "Smith", "gak2008": "Koretzky", "xyz9999": "Zed"}
POSITIONS = {("abc1234", "scopus:105004692561"): (2, 4)}   # curator put the person 2nd


def legacy(it):
    row = etl.to_row(it, LAST, {})
    row["status"] = etl.legacy_status(row)
    return row


def as_db(rec):
    """What a row looks like read back from MySQL: JSON columns round-tripped as text."""
    out = dict(rec)
    for c in etl.JSON_COLS:
        if out.get(c) is not None:
            out[c] = json.loads(json.dumps(out[c], ensure_ascii=False))
    return out


records = [etl.to_record(it, legacy(it), POSITIONS, LAST) for it in ITEMS]

# 1. Identical projection: an untouched synced row projects to the DynamoDB-only row.
for it, rec in zip(ITEMS, records):
    want, got = legacy(it), etl.to_projection(as_db(rec))
    for c in etl.COLS:
        check(etl._canon(want.get(c)) == etl._canon(got.get(c)),
              f"{it['articleId']}: projection {c} {got.get(c)!r} != DynamoDB-only {want.get(c)!r}")
legacy_rows = [legacy(it) for it in ITEMS]
check(etl.checksum(legacy_rows) == etl.checksum(etl.projection_rows([as_db(r) for r in records])),
      "projection checksum differs from the DynamoDB-only load")
ok, touched, diffs = etl.self_check(legacy_rows, etl.projection_rows(records), records)
check(ok and touched == 0, f"self-check should match: {diffs}")

# 2. Status, provenance, identifiers.
by_id = {r["article_id"]: r for r in records}
check(by_id["SCOPUS:105004692561"]["status"] == "ACCEPTED", "unsuppressed -> ACCEPTED")
check(by_id["SCOPUS:2"]["status"] == "SUPERSEDED", "suppressed + supersededByPmid -> SUPERSEDED")
check(by_id["SCOPUS:3"]["status"] == "REJECTED", "suppressed alone -> REJECTED")
check(by_id["SCOPUS:105004692561"]["provenance"] == "AUTHORSHIPS", "authorships-tab -> AUTHORSHIPS")
check(by_id["CROSSREF:10.1007/978-3-031-00001-1"]["provenance"] == "ADD_SEARCH", "api-isbn -> ADD_SEARCH")
check(by_id["CROSSREF:10.1/ed"]["provenance"] == "ADD_SEARCH", "dropdown-search -> ADD_SEARCH")
odd = by_id["OPENALEX:W4232659898"]
check(odd["provenance"] is None and odd["method"] == "openalex-verification-test",
      "unknown method: provenance NULL, method verbatim")
check(odd["openalex_id"] == "W4232659898" and odd["scopus_eid"] is None, "openalex_id from article_id")
check(by_id["SCOPUS:105004692561"]["scopus_eid"] == "105004692561", "scopus_eid from article_id")
s = by_id["SCOPUS:105004692561"]
check(s["added_at"] == datetime(2026, 10, 20, 1, 34, 24, 368000), f"dateAdded -> ms datetime, got {s['added_at']}")
check(s["fields_json"]["dynamo"]["date_added"] == SCOPUS["dateAdded"], "verbatim dateAdded kept")
check(s["status_by"] == "drw2004" and s["status_at"] == s["added_at"], "status_by/at from addedBy/dateAdded")
check(s["sync_origin"] == "DYNAMO" and s["pm_touched"] == 0, "synced rows are DYNAMO, untouched")
check(s["title"] == "Molecular phylogenetics & a new genus", "title cleaned as the projection")
check(s["date_precision"] == "DAY" and by_id["CROSSREF:10.1/ed"]["date_precision"] == "MONTH"
      and by_id["CROSSREF:10.1007/978-3-031-00001-1"]["date_precision"] == "YEAR", "date_precision")

# 3. target_author_index: the review row wins, else the byline's surname match, else NULL.
check((s["target_author_index"], s["author_count"]) == (2, 4), "review position beats surname match")
check(s["fields_json"]["dynamo"]["author_position"] == "last", "byline position kept for the projection")
book = by_id["CROSSREF:10.1007/978-3-031-00001-1"]
check((book["target_author_index"], book["author_count"]) == (1, 2), "surname fallback, 1-based")
check(book["authors_json"] == [{"given": "Gary A.", "surname": "Koretzky"}, {"given": "Lee", "surname": "Park"}],
      f"authors_json from Crossref structure, got {book['authors_json']}")
check(book["fields_json"]["editors"] == ["Ed Itor"], "editors in fields_json")
check(book["isbn"] == "9783031000011" and book["publisher"] == "Springer", "book fields")
check(by_id["CROSSREF:10.1/ed"]["target_author_index"] is None, "editor-only: no author index")
check(odd["target_author_index"] is None and odd["authors_json"] is None, "no authors: NULLs")

# 4. title_year_key mirrors PM normTitle + year.
check(etl.norm_title("  The  Café: A--Study! ") == "the caf a study", etl.norm_title("  The  Café: A--Study! "))
check(etl.title_year_key("A Title", "2024-01-01") == etl.title_year_key("a   title!", "2024"), "key normalises")
check(etl.title_year_key("A Title", None) is None and etl.title_year_key("", "2024") is None, "key needs both")
check(len(etl.title_year_key("A Title", "2024")) == 40, "SHA-1 hex")

# 5. Sync planning.
existing = []
for i, r in enumerate(records):
    e = as_db(r)
    e["id"] = i + 1
    existing.append(e)
existing[0] = dict(existing[0], title="PM edited title", pm_touched=1)                 # PM-owned
existing[1] = dict(existing[1], sync_origin="PM")                                          # PM-written
existing[2] = dict(existing[2], byline="stale byline")                                     # needs update
existing[3] = dict(existing[3], status="ACCEPTED", superseded_by_pmid=None)               # supersede flip
gone = dict(existing[4], uid="old0001", article_id="SCOPUS:999", id=99)                   # not in scan
existing.append(gone)
touched_gone = dict(gone, article_id="SCOPUS:998", id=98, pm_touched=1)
existing.append(touched_gone)
removed_back = dict(existing[5], status="REMOVED")
existing[5] = removed_back                                                                 # re-added item

plan = etl.plan_sync(existing, records + [dict(records[0], uid="new0001")], full_scan=True)
check(len(plan["inserts"]) == 1 and plan["inserts"][0]["uid"] == "new0001", "absent row inserted")
check(plan["skipped_touched"] == 2, f"pm_touched and sync_origin=PM rows skipped, got {plan['skipped_touched']}")
upd = {o["article_id"]: (o, n) for o, n in plan["updates"]}
check(set(upd) == {"CROSSREF:10.1/ed", "SCOPUS:2", "OPENALEX:W4232659898"}, f"updates: {sorted(upd)}")
check(plan["unchanged"] == 1, f"one row unchanged, got {plan['unchanged']}")
check([r["id"] for r in plan["removals"]] == [99], "only the untouched vanished row is REMOVED")
check(etl.plan_sync(existing, records, full_scan=False)["removals"] == [], "--since: no REMOVED pass")
check(etl.plan_sync(existing, records + records[:1], full_scan=True)["dup_keys"] == 1, "dup keys counted")

now = datetime(2026, 11, 11, 12, 0, 0)
final = {r["article_id"]: r for r in etl.apply_plan(existing, plan, now) if r["uid"] != "new0001"}
check(final["SCOPUS:105004692561"]["title"] == "PM edited title", "PM-owned row never overwritten")
check(final["SCOPUS:2"]["status"] == "SUPERSEDED" and final["SCOPUS:2"]["prev_status"] == "ACCEPTED"
      and final["SCOPUS:2"]["status_by"] == "system" and final["SCOPUS:2"]["status_at"] == now,
      "a sync status change records prev_status and system actor")
check(final["CROSSREF:10.1/ed"]["byline"] == records[2]["byline"]
      and final["CROSSREF:10.1/ed"]["status_by"] == "xyz9999", "a content-only update keeps status_by")
check(final["OPENALEX:W4232659898"]["status"] == "ACCEPTED", "a re-added REMOVED item comes back")
check(final["SCOPUS:999"]["status"] == "REMOVED" and final["SCOPUS:999"]["prev_status"] == "REJECTED",
      "vanished item -> REMOVED")
check(final["SCOPUS:998"]["status"] == "REJECTED", "touched vanished row left alone")

# 6. Projection: REMOVED stays out; PM-owned rows project from their columns.
proj = {r["article_id"]: r for r in etl.projection_rows(list(final.values()))}
check("SCOPUS:999" not in proj, "REMOVED is not projected")
check(proj["SCOPUS:105004692561"]["title"] == "PM edited title", "PM edit reaches the projection")
check(proj["SCOPUS:105004692561"]["author_position"] is None, "touched row: position from index 2 of 4")
check(json.loads(proj["SCOPUS:105004692561"]["authors"])[0] == "Brian L. Sidlauskas", "touched row authors")
check(proj["SCOPUS:105004692561"]["date_added"] == "2026-10-20T01:34:24.368Z", "touched row date_added")
check(proj["SCOPUS:2"]["suppressed"] == 1 and proj["SCOPUS:2"]["status"] == "SUPERSEDED", "suppressed = not ACCEPTED")
manual = {"uid": "abc1234", "article_id": "MANUAL:6f1c", "source_type": "MANUAL", "status": "ACCEPTED",
          "sync_origin": "PM", "pm_touched": 1, "authors_json": [{"given": "Ann", "surname": "Smith"}],
          "target_author_index": 1, "author_count": 1, "added_at": datetime(2026, 11, 12, 9, 0),
          "fields_json": {"editors": ["E D"]}, "title": "Manual"}
mp = etl.to_projection(manual)
check(mp["author_position"] == "first" and mp["authors"] == '["Ann Smith"]' and mp["editors"] == '["E D"]'
      and mp["suppressed"] == 0, f"MANUAL row projects: {mp}")
rows_after = list(final.values())
ok, touched, diffs = etl.self_check(legacy_rows, etl.projection_rows(rows_after), rows_after)
check(touched == 3 and ok, f"self-check excludes the 3 PM-owned rows, rest match: {diffs}")
drift = [dict(r, byline="drifted") if r["article_id"] == "SCOPUS:3" else r for r in rows_after]
ok, _, diffs = etl.self_check(legacy_rows, etl.projection_rows(drift), drift)
check(not ok and any("byline" in d for d in diffs), f"self-check reports a real difference: {diffs}")

# 7. The REMOVED-pass guard.
check(etl.removal_guard({"removals": [gone]}, existing, 0) is not None, "empty scan blocks REMOVED")
many = {"removals": [gone] * 60}
check(etl.removal_guard(many, existing, 100) is not None, "too many removals blocked")
check(etl.removal_guard({"removals": [gone]}, existing, 6) is None, "a few removals allowed")

# 8. Before the v3.6 DDL (no external_record) main() runs the DynamoDB-only load.
class _Cur:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Conn:
    def cursor(self):
        return _Cur()

    def close(self):
        pass


calls = []
saved = (etl._conn, etl._table_exists, etl.legacy_main)
etl._conn, etl._table_exists = (lambda: _Conn()), (lambda c, t: False)
etl.legacy_main = lambda dry_run: calls.append(dry_run)
try:
    etl.main(["--dry-run"])
    etl.main([])
    try:
        etl.main(["--sync-only"])
        fails.append("--sync-only without external_record should stop")
    except SystemExit:
        pass
finally:
    etl._conn, etl._table_exists, etl.legacy_main = saved
check(calls == [True, False], f"pre-DDL fallback to the DynamoDB-only load, got {calls}")

# 9. EXTERNAL_DYNAMO_SYNC=off with an empty external_record never empties external_article.
loads = []
saved = (etl._conn, etl._table_exists, etl.read_records, etl._enum_members, etl.load,
         etl.scan_external_articles, os.environ.get("EXTERNAL_DYNAMO_SYNC"))
etl._conn, etl._table_exists = (lambda: _Conn()), (lambda c, t: True)
etl.read_records, etl._enum_members = (lambda c: []), (lambda c, t=None: {"SCOPUS"})
etl.load = lambda rows: loads.append(rows)
etl.scan_external_articles = lambda: fails.append("sync off must not scan DynamoDB") or []
os.environ["EXTERNAL_DYNAMO_SYNC"] = "off"
try:
    etl.main([])
finally:
    (etl._conn, etl._table_exists, etl.read_records, etl._enum_members, etl.load,
     etl.scan_external_articles, env) = saved
    if env is None:
        os.environ.pop("EXTERNAL_DYNAMO_SYNC", None)
    else:
        os.environ["EXTERNAL_DYNAMO_SYNC"] = env
check(loads == [], f"sync off + empty external_record must not reload external_article, got {loads}")

for f in fails:
    print("FAIL", f)
print("FAIL test_external_record_sync" if fails else "PASS test_external_record_sync")
sys.exit(1 if fails else 0)
