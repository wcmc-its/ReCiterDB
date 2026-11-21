#!/usr/bin/env python3
"""
Cornell Ithaca web-page seeds, step 5: author-confirmed seeds -> ORCID inference.

For each NetID with live seeds (pubsource_web_seed), every seed DOI votes for the ORCIDs on
its name-matched authors: the Crossref author list already cached by the citation pass, plus
the OpenAlex authorships (cached in pubsource_web_openalex_work, re-asked after a TTL because
ORCIDs get backfilled on works). A DOI votes at most once per ORCID. An ORCID is inferred at
MIN_VOTES or more, only when it strictly leads the runner-up (a tie infers nothing), and only
with at least MIN_CROSSREF_VOTES publisher-deposited (Crossref) votes. On the 11-21 dry run,
OpenAlex-only leaders were contradicted by their own ORCID record (another institution, no
overlap with the seeds) about 1 in 5 times, mostly namesakes merged by OpenAlex author
disambiguation; Crossref-backed ones about 0 in 370.

Writes pubsource_web_orcid_inference (one live row per NetID; a changed or vanished inference
gets superseded_at, never deleted) with the full evidence record: each voting DOI, its page
URLs with the page content hash and fetch time, its citation hashes, and the author name that
matched. am_orcid says how it compares with the Cornell Affiliation Manager map
(pubsource_orcid_person NetID rows): agrees / conflicts / none.

Run by webPageSeeds.py after the citation pass when WEB_SEEDS_ORCID=on, or alone:
  python3 webPageOrcid.py              recompute + write
  python3 webPageOrcid.py --dry-run    recompute, print a TSV, write nothing (OpenAlex cache
                                       is not written either)
"""

import argparse
import collections
import csv
import datetime
import json
import logging
import os
import re
import sys
import time
import urllib.parse
import urllib.request

import webPageCitations as wc

log = logging.getLogger("webPageOrcid")

METHOD_VERSION = "votes-v1"   # bump when matching, sources or thresholds change
MIN_VOTES = 2
# ponytail: OpenAlex-only leaders are held, not inferred; rescue them by corroborating against
# the ORCID record (Cornell affiliation or works overlapping the seeds) if the yield matters.
MIN_CROSSREF_VOTES = 1
OPENALEX_TTL = datetime.timedelta(days=30)
OPENALEX_BATCH = 50
UA = wc.UA
ORCID_RE = r"(\d{4}-\d{4}-\d{4}-\d{3}[\dX])"

DDL = [
    """CREATE TABLE IF NOT EXISTS pubsource_web_openalex_work (
  doi VARCHAR(255) NOT NULL PRIMARY KEY,
  authorships_json MEDIUMTEXT,
  fetched_at DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci""",
    """CREATE TABLE IF NOT EXISTS pubsource_web_orcid_inference (
  netid VARCHAR(100) NOT NULL,
  orcid VARCHAR(19) NOT NULL,
  votes INT NOT NULL,
  runner_up_votes INT NOT NULL,
  seed_count INT NOT NULL,
  evidence_json MEDIUMTEXT NOT NULL,
  method_version VARCHAR(40) NOT NULL,
  crossref_votes INT NOT NULL,
  am_orcid VARCHAR(10) NOT NULL,
  inferred_at DATETIME NOT NULL,
  superseded_at DATETIME,
  PRIMARY KEY (netid, orcid),
  KEY idx_live (superseded_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci""",
]


def bare_orcid(v):
    """'https://orcid.org/0000-0002-1825-0097' / 'http://...' / bare -> bare, else None."""
    m = re.search(ORCID_RE, v or "")
    return m.group(1) if m else None


def matched_orcids(first, last, authors):
    """[(orcid, author name)] for the authors on this list whose name matches the person.
    authors: Crossref-shaped dicts (family/given/name, ORCID)."""
    out = []
    for a in authors or []:
        o = bare_orcid(a.get("ORCID") or a.get("orcid"))
        if o and wc.name_on_authors(first, last, [a]):
            out.append((o, a.get("name") or " ".join(x for x in (a.get("given"), a.get("family")) if x)))
    return out


def openalex_authors(authorships):
    """OpenAlex authorships -> Crossref-shaped author dicts for the shared name matcher."""
    return [{"name": (a.get("author") or {}).get("display_name") or a.get("raw_author_name"),
             "orcid": (a.get("author") or {}).get("orcid")} for a in authorships or []]


def infer(first, last, seeds):
    """seeds: {doi: {"crossref": [authors], "openalex": [authors]}}.
    -> (orcid | None, votes, runner_up, crossref_votes, {orcid: {doi: [(source, name)]}}). Pure."""
    votes = collections.defaultdict(dict)  # orcid -> {doi: [(source, name)]}
    for doi, src in seeds.items():
        for source in ("crossref", "openalex"):
            for o, name in matched_orcids(first, last, src.get(source)):
                votes[o].setdefault(doi, []).append((source, name))
    ranked = sorted(((len(d), o) for o, d in votes.items()), reverse=True)
    top_n, top = ranked[0] if ranked else (0, None)
    runner = ranked[1][0] if len(ranked) > 1 else 0
    cr = sum(1 for v in votes.get(top, {}).values() if any(src == "crossref" for src, _ in v))
    if top_n < MIN_VOTES or top_n == runner or cr < MIN_CROSSREF_VOTES:
        return None, top_n, runner, cr, votes
    return top, top_n, runner, cr, votes


def am_status(orcid, am):
    if not am:
        return "none"
    return "agrees" if orcid in am else "conflicts"


def fetch_openalex(dois):
    """({doi: authorships} for the DOIs OpenAlex knows, set of DOIs whose batch answered).
    A DOI in an unanswered batch is neither found nor a miss: it is asked again next run.
    Keyless, page= only."""
    out, answered = {}, set()
    for i in range(0, len(dois), OPENALEX_BATCH):
        part = dois[i:i + OPENALEX_BATCH]
        q = urllib.parse.urlencode({"filter": "doi:" + "|".join(part), "select": "doi,authorships",
                                    "per-page": OPENALEX_BATCH, "page": 1})
        for attempt in range(4):
            try:
                req = urllib.request.Request("https://api.openalex.org/works?" + q, headers={"User-Agent": UA})
                with urllib.request.urlopen(req, timeout=60) as r:
                    for w in json.load(r).get("results", []):
                        d = (w.get("doi") or "").lower().removeprefix("https://doi.org/")
                        if d:
                            out[d] = w.get("authorships") or []
                answered.update(part)
                break
            except Exception as e:  # transient: back off; a batch that never answers is retried next run
                log.info("openalex batch %d: %s", i, e)
                time.sleep(2 ** attempt)
        time.sleep(0.2)
    return out, answered


def run_pass(db, now, dry_run=False, out=None):
    """Recompute every NetID with seeds (live or removed). Returns stats."""
    s = collections.Counter()
    with db.cursor() as c:
        if not dry_run:
            for ddl in DDL:
                c.execute(ddl)
        c.execute("SELECT netid, doi, urls_json, citation_hashes_json, removed_at FROM pubsource_web_seed")
        rows = c.fetchall()
    live = collections.defaultdict(dict)
    all_netids = set()
    for r in rows:
        n = r["netid"].lower()
        all_netids.add(n)
        if r["removed_at"] is None:
            live[n][r["doi"].lower()] = (json.loads(r["urls_json"]), json.loads(r["citation_hashes_json"]))
    dois = sorted({d for p in live.values() for d in p})
    hashes = sorted({h for p in live.values() for _, hs in p.values() for h in hs})
    urls = sorted({u for p in live.values() for us, _ in p.values() for u in us})
    netids = sorted(all_netids)

    with db.cursor() as c:
        crossref = {r["citation_hash"]: json.loads(r["authors_json"] or "[]") for r in wc._load(
            c, "SELECT citation_hash, authors_json FROM pubsource_web_citation_resolve WHERE citation_hash IN ({})", hashes)}
        try:
            cached = {r["doi"].lower(): r for r in wc._load(
                c, "SELECT doi, authorships_json, fetched_at FROM pubsource_web_openalex_work WHERE doi IN ({})", dois)}
        except Exception:  # dry run before the table exists
            cached = {}
        pages = {r["url"]: (r["content_hash"], r["last_fetched"]) for r in wc._load(
            c, "SELECT url, content_hash, last_fetched FROM pubsource_web_page WHERE url IN ({})", urls)}
        names = {r["netid"].lower(): (r["name_first"], r["name_last"]) for r in wc._load(
            c, "SELECT netid, name_first, name_last FROM identity_cornell_current WHERE netid IN ({})", netids)}
        am = collections.defaultdict(set)
        for r in wc._load(c, """SELECT personIdentifier, orcid FROM pubsource_orcid_person
                                WHERE personIdentifierType = 'NetID' AND personIdentifier IN ({})""", netids):
            o = bare_orcid(r["orcid"])
            if o:
                am[r["personIdentifier"].lower()].add(o)
        prev = {}
        if not dry_run:
            c.execute("SELECT netid, orcid FROM pubsource_web_orcid_inference WHERE superseded_at IS NULL")
            prev = {r["netid"].lower(): r["orcid"] for r in c.fetchall()}

    stale = [d for d in dois if d not in cached or now - cached[d]["fetched_at"] > OPENALEX_TTL]
    fresh, answered = fetch_openalex(stale) if stale else ({}, set())
    s["openalex_asked"], s["openalex_found"] = len(stale), len(fresh)
    s["openalex_unanswered"] = len(stale) - len(answered)
    oa = {d: json.loads(r["authorships_json"] or "[]") for d, r in cached.items()}
    oa.update(fresh)

    writes, supersede = [], []
    w = csv.writer(out, delimiter="\t") if out else None
    if w:
        w.writerow(["netid", "first", "last", "inferred_orcid", "votes", "runner_up", "seeds",
                    "seeds_with_any_orcid_vote", "am", "am_orcids", "top_candidates", "crossref_votes"])
    for n in netids:
        first, last = names.get(n, ("", ""))
        seeds = {d: {"crossref": [a for h in hs for a in crossref.get(h, [])], "openalex": openalex_authors(oa.get(d))}
                 for d, (_, hs) in live.get(n, {}).items()}
        orcid, votes, runner, cr_votes, by = infer(first, last, seeds) if seeds else (None, 0, 0, 0, {})
        s["people"] += 1
        if orcid:
            s["inferred"] += 1
            st = am_status(orcid, am.get(n))
            s["am_" + st] += 1
            ev = [{"doi": d, "votes_from": sorted({src for src, _ in v}), "author_names": sorted({nm for _, nm in v if nm}),
                   "pages": [{"url": u, "content_hash": pages.get(u, (None, None))[0],
                              "fetched_at": str(pages.get(u, (None, None))[1])} for u in sorted(live[n][d][0])],
                   "citation_hashes": sorted(live[n][d][1])}
                  for d, v in sorted(by[orcid].items())]
            writes.append((n, orcid, votes, runner, len(seeds), json.dumps(ev), METHOD_VERSION, cr_votes, st, now))
        elif seeds:
            s["below_min" if votes < MIN_VOTES else "tie" if votes == runner else "held_openalex_only"] += 1
        if prev.get(n) and prev[n] != orcid:
            supersede.append((now, n, prev[n]))
            s["superseded"] += 1
        if orcid and prev.get(n) != orcid:
            s["new_or_changed"] += 1
        if w and seeds:
            top = sorted(((len(v), o) for o, v in by.items()), reverse=True)[:3]
            w.writerow([n, first, last, orcid or "", votes, runner, len(seeds),
                        len({d for v in by.values() for d in v}), am_status(orcid, am.get(n)) if orcid else "",
                        ",".join(sorted(am.get(n, []))), " ".join(f"{o}:{k}" for k, o in top),
                        cr_votes if orcid else ""])
    if dry_run:
        return dict(s)
    with db.cursor() as c:
        if answered:
            c.executemany("""INSERT INTO pubsource_web_openalex_work (doi, authorships_json, fetched_at) VALUES (%s,%s,%s)
                             ON DUPLICATE KEY UPDATE authorships_json=VALUES(authorships_json), fetched_at=VALUES(fetched_at)""",
                          # misses are cached as NULL too, so they wait the TTL instead of being asked every run
                          [(d, json.dumps(fresh[d]) if d in fresh else None, now) for d in sorted(answered)])
        c.executemany("UPDATE pubsource_web_orcid_inference SET superseded_at=%s WHERE netid=%s AND orcid=%s AND superseded_at IS NULL",
                      supersede)
        c.executemany("""INSERT INTO pubsource_web_orcid_inference (netid, orcid, votes, runner_up_votes, seed_count, evidence_json,
                             method_version, crossref_votes, am_orcid, inferred_at, superseded_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL)
                         ON DUPLICATE KEY UPDATE votes=VALUES(votes), runner_up_votes=VALUES(runner_up_votes),
                             seed_count=VALUES(seed_count), evidence_json=VALUES(evidence_json), method_version=VALUES(method_version),
                             crossref_votes=VALUES(crossref_votes), am_orcid=VALUES(am_orcid), inferred_at=IF(superseded_at IS NULL, inferred_at, VALUES(inferred_at)),
                             superseded_at=NULL""", writes)
    db.commit()
    return dict(s)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="print a TSV to stdout, write nothing")
    a = ap.parse_args()
    import webPageSeeds
    db = webPageSeeds.db_conn()
    try:
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        log.info("orcid pass: %s", run_pass(db, now, dry_run=a.dry_run, out=sys.stdout if a.dry_run else None))
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
