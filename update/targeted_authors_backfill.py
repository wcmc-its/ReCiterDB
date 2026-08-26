#!/usr/bin/env python3
"""Targeted per-document backfill for authorship_review.authors_json (reference tool).

WHAT IT DOES
  For every still-NULL authorship_review row (source='scopus'), looks the document
  up directly by its stored doi/external_id via a single-document Scopus Search API
  query, then UPDATEs just the authors_json column on the matching row(s). There is
  no discovery/matching step, no upsert, and no other column is touched -- we already
  know exactly which documents we need.

  Reuses aar_universe_scopus._scopus_get (same retry/backoff) and _authors_json (same
  parser, already unit-tested) for byte-identical output to the real producer's field.

WHEN TO USE THIS PATTERN
  A new derived column gets added to authorship_review and needs to be backfilled on
  documents that were already discovered and written by a prior run of
  aar_universe_scopus.py. Copy-adapt this script for the new column rather than
  generalizing this one -- it is a documented one-off pattern, not a framework.

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

Usage:
  python targeted_authors_backfill.py --limit 10   # dry-run sanity check first (default)
  python targeted_authors_backfill.py --apply      # full run, writes
"""
import argparse, sys, time
sys.path.insert(0, "/usr/src/app")
import aar_universe_scopus as scopus
import aar_db
from sqlalchemy import text


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="perform the UPDATE; default is dry-run (fetch + parse only)")
    ap.add_argument("--limit", type=int, default=None, help="cap number of documents (testing)")
    args = ap.parse_args()
    dry_run = not args.apply

    engine = aar_db.engine()
    with engine.connect() as c:
        rows = c.execute(text(
            "SELECT DISTINCT doi, external_id FROM authorship_review "
            "WHERE source='scopus' AND authors_json IS NULL"
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
            authors_json = scopus._authors_json(entry)
            found_docs += 1
            if dry_run:
                print(f"  [{i}/{len(rows)}] OK (dry-run): {query} -> {authors_json[:100]}", flush=True)
            else:
                with engine.begin() as c2:
                    if doi:
                        result = c2.execute(text(
                            "UPDATE authorship_review SET authors_json=:aj "
                            "WHERE source='scopus' AND doi=:doi AND authors_json IS NULL"),
                            {"aj": authors_json, "doi": doi})
                    else:
                        result = c2.execute(text(
                            "UPDATE authorship_review SET authors_json=:aj "
                            "WHERE source='scopus' AND external_id=:eid AND authors_json IS NULL"),
                            {"aj": authors_json, "eid": ext_id})
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
