#!/usr/bin/env python3
"""
Cornell Ithaca web-page seeds, step 6: seeds -> cornell_pub_* (author-asserted pubs).

Every live seed (pubsource_web_seed: a DOI listed on the person's own Cornell page, with
their name on its author list) becomes a cornell_pub_researcher_publication row with
retrieval_source='web_seed', review_status='auto_accepted', next to the Feb 2026 retrieval's
rows (orcid_api, openalex_orcid, europepmc_orcid, scopus_name_search), which it never touches.

  * Researcher key: identity_cornell.id by NetID, the key the Feb retrieval used. A NetID
    not in identity_cornell (hired after it was loaded) is counted and skipped.
  * Publication: the cornell_pub_publication row with that DOI, else a new one from OpenAlex
    metadata, else from the citation's own parse (title / year / venue / type).
  * This pass owns its source: links whose seed is gone are deleted (the seed row keeps
    its removed_at history), so the table always mirrors the live seeds.

Run by webPageSeeds.py after the ORCID pass when WEB_SEEDS_CORNELL_PUB=on, or alone:
  python3 webPageCornellPub.py [--dry-run]
"""

import argparse
import collections
import datetime
import json
import logging
import sys

import webPageCitations as wc
import webPageOrcid as wo

log = logging.getLogger("webPageCornellPub")

SOURCE = "web_seed"
CONFIDENCE = 95.0  # author-asserted: on the person's own page, their name on the work
OPENALEX_SELECT = "id,doi,title,publication_year,type,primary_location,ids,cited_by_count"


def pub_row(doi, work, parsed):
    """cornell_pub_publication values for a DOI from an OpenAlex work, else the citation
    parse. Pure."""
    if work:
        src = ((work.get("primary_location") or {}).get("source") or {})
        issns = src.get("issn") or []
        pmid = ((work.get("ids") or {}).get("pmid") or "").rsplit("/", 1)[-1] or None
        return {"doi": doi, "pmid": pmid, "openalex_id": (work.get("id") or "").rsplit("/", 1)[-1] or None,
                "title": work.get("title"), "journal": (src.get("display_name") or "")[:500] or None,
                "issn": src.get("issn_l"), "eissn": next((i for i in issns if i != src.get("issn_l")), None),
                "year": work.get("publication_year"), "type": work.get("type"),
                "cited_by": work.get("cited_by_count")}
    p = parsed or {}
    return {"doi": doi, "pmid": None, "openalex_id": None, "title": p.get("title"),
            "journal": (p.get("venue") or "")[:500] or None, "issn": None, "eissn": None,
            "year": p.get("year") if isinstance(p.get("year"), int) else None, "type": p.get("type"),
            "cited_by": None}


def plan_links(seeds, researcher, pubs, existing):
    """seeds {netid: {doi}}, researcher {netid: id}, pubs {doi: publication id},
    existing {(rid, pid): link id} for SOURCE -> (to_insert [(rid, pid)], to_delete [link id],
    skipped_no_researcher). Pure."""
    want, skipped = set(), 0
    for netid, dois in seeds.items():
        rid = researcher.get(netid)
        if rid is None:
            skipped += 1
            continue
        want.update((rid, pubs[d]) for d in dois if d in pubs)
    return sorted(want - set(existing)), sorted(existing[k] for k in set(existing) - want), skipped


def run_pass(db, now, dry_run=False):
    s = collections.Counter()
    with db.cursor() as c:
        c.execute("SELECT netid, doi, citation_hashes_json FROM pubsource_web_seed WHERE removed_at IS NULL")
        rows = c.fetchall()
    seeds, hash_of = collections.defaultdict(set), {}
    for r in rows:
        d = r["doi"].lower()
        seeds[r["netid"].lower()].add(d)
        hash_of.setdefault(d, (json.loads(r["citation_hashes_json"]) or [None])[0])
    dois = sorted(hash_of)
    with db.cursor() as c:
        researcher = {r["netid"].lower(): r["id"] for r in wc._load(
            c, "SELECT id, netid FROM identity_cornell WHERE netid IN ({})", sorted(seeds))}
        pubs = {r["doi"].lower(): r["id"] for r in wc._load(
            c, "SELECT id, doi FROM cornell_pub_publication WHERE doi IN ({})", dois)}
    missing = [d for d in dois if d not in pubs]
    works, _ = wo.fetch_openalex(missing, select=OPENALEX_SELECT) if missing else ({}, set())
    with db.cursor() as c:
        parsed = {r["citation_hash"]: json.loads(r["parsed_json"] or "{}") for r in wc._load(
            c, "SELECT citation_hash, parsed_json FROM pubsource_web_citation_parse WHERE citation_hash IN ({})",
            sorted({hash_of[d] for d in missing if hash_of[d]}))}
    new_pubs = [pub_row(d, works.get(d), parsed.get(hash_of[d])) for d in missing]
    s.update(seed_dois=len(dois), pubs_existing=len(pubs), pubs_new=len(new_pubs),
             pubs_new_from_openalex=sum(1 for d in missing if d in works))
    if dry_run:
        pubs.update({d: -i - 1 for i, d in enumerate(missing)})  # placeholder ids
        ins, dele, skipped = plan_links(seeds, researcher, pubs, {})
        s.update(links_to_insert=len(ins), people_skipped_no_researcher=skipped)
        return dict(s)
    with db.cursor() as c:
        c.executemany("""INSERT IGNORE INTO cornell_pub_publication
                             (doi, pmid, openalex_id, title, journal, issn, eissn, publication_year, publication_type, citation_count)
                         VALUES (%(doi)s, %(pmid)s, %(openalex_id)s, %(title)s, %(journal)s, %(issn)s, %(eissn)s,
                                 %(year)s, %(type)s, %(cited_by)s)""", new_pubs)
        pubs.update({r["doi"].lower(): r["id"] for r in wc._load(
            c, "SELECT id, doi FROM cornell_pub_publication WHERE doi IN ({})", missing)})
        c.execute("SELECT id, researcher_id, publication_id FROM cornell_pub_researcher_publication WHERE retrieval_source = %s",
                  (SOURCE,))
        existing = {(r["researcher_id"], r["publication_id"]): r["id"] for r in c.fetchall()}
        ins, dele, skipped = plan_links(seeds, researcher, pubs, existing)
        c.executemany("""INSERT INTO cornell_pub_researcher_publication
                             (researcher_id, publication_id, retrieval_source, confidence_score, review_status)
                         VALUES (%s, %s, %s, %s, 'auto_accepted')""", [(r, p, SOURCE, CONFIDENCE) for r, p in ins])
        if dele:
            c.execute("DELETE FROM cornell_pub_researcher_publication WHERE retrieval_source = %s AND id IN ({})".format(
                wc._in(len(dele))), [SOURCE, *dele])
    db.commit()
    s.update(links_inserted=len(ins), links_deleted=len(dele), people_skipped_no_researcher=skipped,
             links_total=len(existing) + len(ins) - len(dele))
    return dict(s)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="count what would change, write nothing")
    a = ap.parse_args()
    import webPageSeeds
    db = webPageSeeds.db_conn()
    try:
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        log.info("cornell_pub pass: %s", run_pass(db, now, dry_run=a.dry_run))
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
