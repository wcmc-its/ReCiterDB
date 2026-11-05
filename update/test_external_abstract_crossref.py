#!/usr/bin/env python3
"""CROSSREF: external articles get their abstract from Crossref by the DOI in the id.

PM issue #1060 Phase 2: a curator can add a DOI only Crossref has, stored as
"CROSSREF:" + the lowercased DOI. The OpenAlex pending query never selects those ids,
so run_crossref must pick them up itself, call Crossref with the DOI (the id minus the
prefix, slashes kept), and write a source='crossref' row keyed by the full id. A NULL
row written by OpenAlex keeps going through the existing #251 fallback alongside.

Run: python3 update/test_external_abstract_crossref.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import externalAbstractImport as ex  # noqa: E402

CROSSREF_ROWS = [("CROSSREF:10.1016/j.surg.2026.110815",), ("CROSSREF:garbage",)]
OPENALEX_NULL_ROWS = [("OPENALEX:W4417001897", " 10.2139/ssrn.123 ")]


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, written):
        self.written = written

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, stmt, params=None):
        if params is not None:          # the upsert
            self.written.extend(params)
            return FakeResult([])
        if "LIKE 'CROSSREF:%'" in stmt:
            return FakeResult(CROSSREF_ROWS)
        if "a.source = 'openalex'" in stmt:
            return FakeResult(OPENALEX_NULL_ROWS)
        raise AssertionError(f"unexpected query: {stmt}")


class FakeEngine:
    def __init__(self):
        self.written = []

    def connect(self):
        return FakeConn(self.written)

    begin = connect


def main():
    asked = []
    ex.crossref_abstract = lambda doi: asked.append(doi) or ("Text for " + doi
                                                              if doi.startswith("10.1016") else None)
    ex.SLEEP = 0
    eng = FakeEngine()

    # dry run: counts, writes nothing, calls nothing
    ex.run_crossref(eng, lambda s: s, "UPSERT", apply=False, limit=100)
    assert eng.written == [] and asked == [], (eng.written, asked)

    ex.run_crossref(eng, lambda s: s, "UPSERT", apply=True, limit=100)
    assert asked == ["10.1016/j.surg.2026.110815", "10.2139/ssrn.123"], asked
    by_id = {r["article_id"]: r for r in eng.written}
    assert set(by_id) == {"CROSSREF:10.1016/j.surg.2026.110815", "OPENALEX:W4417001897"}, by_id
    hit = by_id["CROSSREF:10.1016/j.surg.2026.110815"]
    assert hit["source"] == "crossref" and hit["abstract"] == "Text for 10.1016/j.surg.2026.110815"
    assert hit["abstract_len"] == len(hit["abstract"])
    miss = by_id["OPENALEX:W4417001897"]
    assert miss["source"] == "crossref" and miss["abstract"] is None and miss["abstract_len"] is None

    # the limit applies across both kinds of pending row
    eng2, asked[:] = FakeEngine(), []
    ex.run_crossref(eng2, lambda s: s, "UPSERT", apply=True, limit=1)
    assert asked == ["10.1016/j.surg.2026.110815"], asked

    assert ex.doi_from_crossref_id("CROSSREF:10.1000/a/b") == "10.1000/a/b"
    assert ex.doi_from_crossref_id("SCOPUS:85012345678") is None
    print("PASS test_external_abstract_crossref")


if __name__ == "__main__":
    main()
