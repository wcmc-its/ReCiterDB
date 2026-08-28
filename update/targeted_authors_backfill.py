#!/usr/bin/env python3
"""Targeted per-document backfill for authorship_review producer-owned columns
(reference tool; the filename is historical -- authors_json was the first column it
carried, and prior handoffs cite it by that name).

WHAT IT DOES
  For every authorship_review row (source='scopus') still NULL in any column of
  COLUMNS below, looks the document up directly by its stored doi/external_id via a
  single-document Scopus Search API query, then UPDATEs just those columns on the
  matching row(s). There is no discovery/matching step, no upsert, and no column
  outside COLUMNS is touched -- we already know exactly which documents we need.

  Every column is filled with COALESCE(existing, new), so a value the producer has
  already written is never overwritten, and a re-run is safe.

  Reuses aar_universe_scopus._scopus_get (same retry/backoff) and the producer's own
  field extractors (_authors_json / _normalize_issn / _extract_isbns, all covered by
  that module's selftest) so the backfilled values are byte-identical to what a
  producer upsert of the same document would have written.

WHEN TO USE THIS PATTERN
  A new derived column gets added to authorship_review and needs to be backfilled on
  documents that were already discovered and written by a prior run of
  aar_universe_scopus.py. Add it to COLUMNS -- the select/update/report all key off
  that dict. Keep it to columns derivable from the single Scopus document already
  being fetched; anything needing a different source deserves its own script.

WHY `aar_universe_scopus.py --mode initial` CANNOT DO THIS
  The initial/rolling/recurring sweep is a discovery pass, not an idempotent column
  refresh: per its own module docstring, step 2 drops any document carrying a Scopus
  pubmed-id and step 3 drops any remaining document that resolves to PubMed via DOI
  or title/ISSN/ISBN fallback -- both BEFORE the upsert in step 5. So a document that
  has since become PubMed-indexed is dropped from the sweep before it ever reaches
  authorship_review again; re-running the sweep can never revisit -- let alone
  backfill a new column on -- the existing row for that document. This script
  bypasses that logic entirely by going straight to each row's already-stored
  doi/external_id, with no PubMed re-resolution step at all.

RUNTIME REQUIREMENT
  Must run inside the reciterdb container -- it imports update/aar_universe_scopus.py
  and update/aar_db.py via sys.path.insert(0, "/usr/src/app"), the container's app
  root, and needs the container's Scopus API credentials and DB connection.

PROVENANCE
  Run 2026-08-25 (authors_json column): 2,799 distinct documents considered,
  2,797 found via Scopus lookup (2 not_found), 4,155 rows updated, 0 errors.
  issn/isbn (issue #157): NOT YET RUN. Sizing measured on prod 2026-08-28 --
  7,902 scopus rows have issn NULL; of the 729 open no-DOI rows, a stratified
  sample put "source doc actually has a prism:issn" at 19/20 Article, 6/8 Note,
  5/12 Conference Paper, 4/4 Review and 0/6 Editorial, and "has a prism:isbn" at
  15/15 for each of Book, Book Chapter and Editorial. Run it AFTER
  setup/alter_authorship_review_add_isbn_v2.3.sql is applied to that instance --
  without the column the isbn UPDATE errors out.

Usage:
  python targeted_authors_backfill.py --limit 10   # dry-run sanity check first (default)
  python targeted_authors_backfill.py --apply      # full run, writes
"""
import argparse, sys, time
sys.path.insert(0, "/usr/src/app")
import aar_universe_scopus as scopus
import aar_db
from sqlalchemy import text

# column -> how the producer derives it from a Scopus entry. Must stay byte-identical
# to aar_universe_scopus._build_row; these are the same callables it uses.
COLUMNS = {
    "authors_json": scopus._authors_json,
    "issn": lambda d: scopus._normalize_issn(d.get("prism:issn") or d.get("prism:eIssn")),
    "isbn": lambda d: scopus._trunc(",".join(scopus._extract_isbns(d)), 128) or None,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="perform the UPDATE; default is dry-run (fetch + parse only)")
    ap.add_argument("--limit", type=int, default=None, help="cap number of documents (testing)")
    args = ap.parse_args()
    dry_run = not args.apply

    engine = aar_db.engine()
    # Documents needing at least one column. Rows whose document can never supply a
    # column (a book has no ISSN) keep matching here and get re-fetched on every run;
    # harmless for a manual one-off, and cheaper than a "already checked" marker column.
    null_any = " OR ".join(f"{c} IS NULL" for c in COLUMNS)
    with engine.connect() as c:
        rows = c.execute(text(
            "SELECT DISTINCT doi, external_id FROM authorship_review "
            f"WHERE source='scopus' AND ({null_any})"
        )).mappings().all()

    if args.limit:
        rows = rows[:args.limit]

    print(f"{len(rows)} distinct documents to backfill" + ("  [DRY-RUN]" if dry_run else ""), flush=True)

    found_docs = 0
    updated_rows = 0
    not_found = 0
    errors = 0

    for i, r in enumerate(rows, 1):
        doi, ext_id = r["doi"], r["external_id"]
        query = f"DOI({doi})" if doi else f"EID(2-s2.0-{ext_id})"
        try:
            res = scopus._scopus_get(query, 0).get("search-results", {})
            entries = res.get("entry", []) or []
            if not entries or "error" in entries[0]:
                not_found += 1
                print(f"  [{i}/{len(rows)}] NOT FOUND: {query}", flush=True)
                time.sleep(0.5)
                continue
            entry = entries[0]
            vals = {col: fn(entry) for col, fn in COLUMNS.items()}
            found_docs += 1
            if dry_run:
                shown = ", ".join(f"{k}={str(v)[:40]}" for k, v in vals.items())
                print(f"  [{i}/{len(rows)}] OK (dry-run): {query} -> {shown}", flush=True)
            else:
                # Only touch columns this document can actually fill: a book carries no
                # prism:issn, so its rows keep issn=NULL forever. Including such a column
                # would make the UPDATE match those rows on every run and report them as
                # updated when nothing changed (SQLAlchemy's MySQL dialect enables
                # CLIENT_FOUND_ROWS, so rowcount counts MATCHED rows, not changed ones).
                # COALESCE so a value the producer already wrote is never overwritten.
                # guarded rather than `continue`d so the Scopus throttle at the
                # bottom of the loop always runs (authors_json is never None, so
                # today this is always true -- the guard is for the next column).
                fillable = [c for c, v in vals.items() if v is not None]
                if fillable:
                    sets = ", ".join(f"{c}=COALESCE({c}, :{c})" for c in fillable)
                    any_null = " OR ".join(f"{c} IS NULL" for c in fillable)
                    key = "doi=:_key" if doi else "external_id=:_key"
                    with engine.begin() as c2:
                        result = c2.execute(
                            text(f"UPDATE authorship_review SET {sets} "
                                 f"WHERE source='scopus' AND {key} AND ({any_null})"),
                            {**{c: vals[c] for c in fillable}, "_key": doi or ext_id})
                        updated_rows += result.rowcount
        except Exception as e:
            errors += 1
            print(f"  [{i}/{len(rows)}] ERROR {query}: {e}", flush=True)
        if i % 100 == 0:
            print(f"  progress: {i}/{len(rows)}  found={found_docs} not_found={not_found} "
                  f"errors={errors} rows_updated={updated_rows}", flush=True)
        time.sleep(0.5)

    print(f"\n==== TARGETED BACKFILL SUMMARY ====\n"
          f"documents: {len(rows)}, found: {found_docs}, not_found: {not_found}, "
          f"errors: {errors}, rows_updated: {updated_rows}", flush=True)


if __name__ == "__main__":
    main()
