#!/usr/bin/env python3
"""Regression test for conflictsImport.py -- see issue #127.

Two things this locks down:

1. The CSV + LOAD DATA LOCAL INFILE pairing must stay gone. csv.writer escapes an
   embedded quote by doubling it; LOAD DATA with no ESCAPED BY clause defaults to
   ESCAPED BY '\\'. The two disagree on exactly the content COI statements carry,
   and LOAD DATA reports a desynced parse as a *warning*, so rows are dropped or
   concatenated with a zero exit code. That is what cost ~99.9% of reporting_abstracts
   in May 2026 (#78/#80, #87/#89/#88).

2. get_conflicts() must return "" -- never None, never raise -- for every degenerate
   DynamoDB item shape. An article with no COI statement still has to produce a row,
   otherwise `LEFT JOIN reporting_conflicts ... WHERE a.pmid IS NULL` returns that
   PMID forever and the importer never converges.

Run: python3 test_conflicts_import.py
"""
import os

from conflictsImport import get_conflicts

HERE = os.path.dirname(os.path.abspath(__file__))


def _citation(**kw):
    return {"pubmedarticle": {"medlinecitation": kw}}


def test_no_csv_bulk_load_pattern():
    # Applied to both importers: they load free text from the same DynamoDB table
    # and were both written against the same broken template.
    # Code-only tokens: each of these is unreachable in a comment describing the
    # old bug, so the guard does not fire on the prose that explains itself.
    for name in ("conflictsImport.py", "abstractImport.py"):
        with open(os.path.join(HERE, name), encoding="utf-8") as f:
            src = f.read()
        for banned in ("LOAD DATA LOCAL INFILE", "csv.writer", "local_infile"):
            assert banned not in src, f"{name} reintroduced the banned pattern: {banned!r}"
        # Repo-wide invariant, and the one the rewrite actually got wrong first
        # time: the SP injects synthetic NEGATIVE pmids for external_article rows,
        # which have no PubMedArticle record and would be "missing" forever.
        assert "p.pmid > 0" in src, f"{name} dropped the `p.pmid > 0` guard"


def test_get_conflicts_returns_statement_verbatim():
    # The exact content class the old LOAD DATA path could not parse.
    poison = 'The authors declare "no" competing interests.\tPath: C:\\temp\r\nSigned.'
    assert get_conflicts(_citation(coiStatement=poison)) == poison


def test_get_conflicts_never_returns_none():
    # Each of these was observed or is trivially reachable in the PubMedArticle table.
    for item in (
        {},                                        # empty item
        {"pubmedarticle": {}},                     # no medlinecitation key
        {"pubmedarticle": {"medlinecitation": None}},   # NULL-serialized sub-object
        _citation(),                               # citation present, no coiStatement
        _citation(coiStatement=None),              # coiStatement explicitly NULL
        _citation(coiStatement=""),                # present but empty
    ):
        assert get_conflicts(item) == "", f"expected '' for {item!r}, got {get_conflicts(item)!r}"


if __name__ == "__main__":
    test_no_csv_bulk_load_pattern()
    test_get_conflicts_returns_statement_verbatim()
    test_get_conflicts_never_returns_none()
    print("OK: no CSV bulk-load pattern; COI statements extracted verbatim, never None")
