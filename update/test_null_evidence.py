#!/usr/bin/env python3
"""Regression test: articles with NULL evidence blocks must not be dropped.

DynamoDBMapper serializes an absent @DynamoDBDocument evidence block as a NULL
attribute, so the DynamoDB->CSV transform sees evidence sub-objects as None
instead of {}. Before the `or {}` fix, chained `.get()` raised AttributeError
and the per-article try/except silently dropped the whole article -- e.g. ccole
lost 46 of 47 articles, leaving 0 ACCEPTED in person_article.

Run: python3 test_null_evidence.py
"""
import csv, os, tempfile
from dataTransformer import process_person_article


def _rows(record):
    with tempfile.TemporaryDirectory() as d:
        process_person_article([record], d + "/")
        with open(os.path.join(d, "person_article2.csv")) as f:
            return list(csv.DictReader(f))


def test_null_evidence_blocks_do_not_drop_article():
    # Every evidence sub-block that we observed as NULL in prod, plus a null
    # sub-sub-object (authorNameEvidence.articleAuthorName).
    record = {
        "personIdentifier": "test0001",
        "reCiterArticleFeatures": [{
            "pmid": 12345678,
            "userAssertion": "ACCEPTED",
            "evidence": {
                "emailEvidence": None,
                "journalCategoryEvidence": None,
                "genderEvidence": None,
                "relationshipEvidence": None,
                "authorNameEvidence": {"articleAuthorName": None,
                                       "institutionalAuthorName": None},
            },
        }],
    }
    rows = _rows(record)
    assert len(rows) == 1, f"article dropped: expected 1 row, got {len(rows)}"
    assert rows[0]["userAssertion"] == "ACCEPTED"


def test_evidence_itself_null_does_not_crash():
    record = {
        "personIdentifier": "test0002",
        "reCiterArticleFeatures": [{"pmid": 99, "userAssertion": "REJECTED",
                                    "evidence": None}],
    }
    rows = _rows(record)
    assert len(rows) == 1, f"article dropped when evidence is None: got {len(rows)}"


if __name__ == "__main__":
    test_null_evidence_blocks_do_not_drop_article()
    test_evidence_itself_null_does_not_crash()
    print("OK: null evidence blocks no longer drop articles")
