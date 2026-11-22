"""export_external_record_to_dynamo.py: the plan it makes, and that it inverts the sync.

Run: python3 update/test_export_external_record_to_dynamo.py
"""
import os
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import export_external_record_to_dynamo as ex  # noqa: E402
import retrieveExternalArticles as sync  # noqa: E402

T = datetime(2026, 11, 23, 17, 5, 1, 234000)


def rec(**kw):
    base = {"uid": "abc1234", "article_id": "CROSSREF:10.9/x", "source_type": "CROSSREF", "status": "ACCEPTED",
            "superseded_by_pmid": None, "pmid": None, "doi": "10.9/x", "title": "An invented paper",
            "container_title": "Journal of Invented Things", "pub_date": "2024-05", "source_pub_type": "journal-article",
            "method": "dropdown-search", "added_by": "xyz9999", "added_at": T, "raw_record": '{"DOI": "10.9/x"}',
            "authors_json": '[{"given": "Ann", "surname": "Example"}, {"given": "Bo", "surname": "Placeholder"}]',
            "sync_origin": "PM", "pm_touched": 1, "updated_at": T}
    base.update(kw)
    return base


def test_status_inverts_the_sync():
    # legacy_status(suppressed, superseded) must give back the record's status.
    for status, sup_pmid in [("ACCEPTED", None), ("REJECTED", None), ("SUPERSEDED", 42626061)]:
        suppressed, superseded = ex.status_attrs(rec(status=status, superseded_by_pmid=sup_pmid))
        assert sync.legacy_status({"suppressed": suppressed, "superseded_by_pmid": superseded}) == status, status
    # REMOVED exports as suppressed (never a delete); the sync reads that back as REJECTED.
    assert ex.status_attrs(rec(status="REMOVED")) == (True, None)


def test_new_item_round_trips_through_the_sync():
    item = ex.new_item(rec())
    assert item["dateAdded"] == "2026-11-23T17:05:01.234Z"
    assert item["authors"] == ["Ann Example", "Bo Placeholder"]
    assert item["suppressed"] is False and "supersededByPmid" not in item and "pmid" not in item
    row = sync.to_row(item)
    assert (row["uid"], row["article_id"], row["source_type"], row["doi"]) == ("abc1234", "CROSSREF:10.9/x", "CROSSREF", "10.9/x")
    assert row["title"] == "An invented paper" and row["journal_or_venue"] == "Journal of Invented Things"
    assert row["pub_date"] == "2024-05" and row["publication_type"] == "journal-article"
    assert sync.legacy_status(row) == "ACCEPTED"
    assert sync._parse_dt(item["dateAdded"]) == T


def test_plan_actions():
    existing_ok = {"uid": "abc1234", "articleId": "SCOPUS:1", "suppressed": False}
    existing_sup = {"uid": "abc1234", "articleId": "SCOPUS:2", "suppressed": True, "supersededByPmid": 5}
    records = [
        rec(),                                                                    # PM add, not in DynamoDB -> put
        rec(article_id="SCOPUS:1", source_type="SCOPUS", status="REJECTED"),      # reject of an existing item -> update
        rec(article_id="SCOPUS:2", source_type="SCOPUS", status="SUPERSEDED", superseded_by_pmid=5),  # same -> unchanged
        rec(article_id="SCOPUS:3", source_type="SCOPUS", status="ACCEPTED"),      # un-reject: existing suppressed -> update
        rec(article_id="MANUAL:1b2c", source_type="MANUAL"),                      # manual -> skip
        rec(article_id="OPENALEX:W9", source_type="OPENALEX", status="PENDING"),  # pending -> skip
        rec(article_id="OPENALEX:W8", source_type="OPENALEX", status="REMOVED"),  # PM add then remove -> skip
        rec(article_id="SCOPUS:4", source_type="SCOPUS", status="REMOVED"),       # removed existing -> suppressed
    ]
    existing = {("abc1234", "SCOPUS:1"): existing_ok, ("abc1234", "SCOPUS:2"): existing_sup,
                ("abc1234", "SCOPUS:3"): {"suppressed": True, "supersededByPmid": 7},
                ("abc1234", "SCOPUS:4"): {"suppressed": False}}
    plan = ex.plan_export(records, existing)
    kinds = [a[0] for a in plan]
    assert kinds == ["put", "update", "unchanged", "update", "skip", "skip", "skip", "update"], kinds
    assert plan[1][2:] == (True, None)
    assert plan[3][2:] == (False, None), "un-reject clears suppressed and supersededByPmid"
    assert plan[7][2:] == (True, None)
    assert "MANUAL" in plan[4][2] and "PENDING" in plan[5][2] and "never in DynamoDB" in plan[6][2]


def test_select_sql_scoping():
    sql, args = ex.select_sql()
    assert "(sync_origin = 'PM' OR pm_touched = 1)" in sql and args == []
    sql, args = ex.select_sql("2026-11-23T17:00", "abc1234")
    assert "updated_at >= %s" in sql and "uid = %s" in sql and args == ["2026-11-23T17:00", "abc1234"]


class FakeTable:
    def __init__(self):
        self.calls = []

    def put_item(self, **kw):
        self.calls.append(("put", kw))

    def update_item(self, **kw):
        self.calls.append(("update", kw))


def test_apply_is_conditional_and_never_deletes():
    t = FakeTable()
    plan = ex.plan_export([rec(), rec(article_id="SCOPUS:1", source_type="SCOPUS", status="ACCEPTED")],
                          {("abc1234", "SCOPUS:1"): {"suppressed": True}})
    done, failed = ex.apply(t, plan)
    assert dict(done) == {"put": 1, "update": 1} and failed == []
    put, upd = t.calls
    assert put[1]["ConditionExpression"] == "attribute_not_exists(uid)"
    assert upd[1]["ConditionExpression"] == "attribute_exists(uid)"
    assert "REMOVE supersededByPmid" in upd[1]["UpdateExpression"]
    assert not any("delete" in c[0] for c in t.calls)


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in tests:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"{len(tests)} passed")
