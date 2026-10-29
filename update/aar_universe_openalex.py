#!/usr/bin/env python3
"""
Adversarial Attribution Review — OpenAlex lane detector (conference abstracts + preprints).

A sibling of aar_universe_scopus.py for the scholarly output neither PubMed nor the Scopus
lane finds: WCM conference abstracts and preprints. It reuses that lane by import (PubMed
resolution, roster, candidate call, row helpers, recheck) and adds only the OpenAlex fetch
and parse. Same self-contained contract: identity_index + aar_db + the Scopus lane module,
no xgboost/S3/models.

Per-work pipeline:
  1. Fetch OpenAlex works with a WCM institution id, type conference-abstract|preprint,
     publication date in the window. Keyless `page=` paging (cursor paging 401s without an
     API key); OpenAlex caps page paging at 10,000 results, which is logged if hit.
  2. Drop works OpenAlex already links to a PubMed record (ids.pmid), and works whose DOI
     the Scopus lane already holds in authorship_review (that lane owns them, and its rows
     carry different author positions, so a curator would see the authorship twice).
  3. Resolve the rest against PubMed: a DOI hit drops the work; a no-DOI title hit keeps it
     with matched_pmid flagged for curator adjudication (v2.7 semantics, unchanged).
  4. Per WCM authorship (an institution id in WCM_OPENALEX_IDS) match the byline against
     identity_index via the Scopus lane's wcm_lane_candidates.
  5. Upsert via aar_db (source='openalex', external_id=W id, pub_type=OpenAlex type,
     pmid=NULL); then dismiss open openalex rows whose DOI the Scopus lane has since
     picked up, then the Scopus lane's recheck, run over source='openalex' open rows.

Step 2 alone only works one way. The OpenAlex window keys on publication date and the
Scopus window on Scopus load date, so a work can reach this lane first and the Scopus
lane weeks later, leaving one authorship open twice (openalex:{doi}:{i} and
scopus:{doi}:{i}; the prefixed keys never collide). dismiss_scopus_held() closes the
openalex side once the Scopus row exists. Scopus wins because its rows carry Scopus
author ids and the afid-aware campus split. The Scopus lane stays unaware of this lane,
which keeps it unchanged. The nightly runs this lane after the Scopus lanes, so the
dismissal lands the same night the Scopus row appears.

Hardcoded source='scopus' sites elsewhere (handoff item 4), decided per site:
  * aar_universe_scopus.recheck_open_scopus: takes a `source` argument now (default
    'scopus'). This lane calls it with source='openalex'. Openalex rows have the same
    pmid-less shape, so the same DOI/title recheck applies.
  * setup/fix_authorship_review_suggested_open_v2.6.sql and
    fix_authorship_review_reopen_title_dismissed_v2.8.sql: left alone. Both are one-off
    repairs of Scopus rows written before those fixes. No openalex row predates them, and
    this lane writes the post-fix shape from the start.
  * targeted_authors_backfill.py and backfill_external_article_authors_932.py: left
    alone. Both re-fetch author data from the Scopus API by Scopus id. An openalex row
    has no Scopus id, and this lane writes authors_json and every author column at
    insert.
  * aar_reconcile_open / aar_report_changed_picks (CLASS B drift refresh): replay only
    'pubmed' and 'scopus'. Openalex rows get no drift refresh yet. A replay would need an
    OpenAlex re-fetch per row. That is a follow-up once the lane is on and has rows.
  * aar_close_attributed / byline-owner sweeps key on pmid IS NOT NULL, so they never
    touch openalex rows (pmid is always NULL here).

Refuses --apply until setup/alter_authorship_review_add_openalex_v3.2.sql is applied: a
source='openalex' insert into the old ENUM stores '' under non-strict SQL mode.

Window: rolling publication-date window [today-span, today-lag], 365 days by default. The
Scopus lane's 90-day window keys on Scopus LOAD date; this one keys on PUBLICATION date
(OpenAlex's filter), so a work OpenAlex indexes after its publication date has scrolled
out of a short window is never fetched. A wider window only costs pages: July 2026 was 59
works at the WCM filter (live, 2026-10-29), so a year is a handful of 200-work pages.
Overlapping runs are idempotent via the author_key upsert, as on the Scopus lane.

Env (never printed): PUBMED_API_KEY (DOI resolution), DB_* (identity roster + sink),
OPENALEX_MAILTO (optional polite-pool address, same env name as Scholars-Profile-System).

Usage:
  python aar_universe_openalex.py --selftest                     # offline checks, fixtures only
  python aar_universe_openalex.py --mode rolling                 # dry-run the default window
  python aar_universe_openalex.py --mode rolling --apply         # write (cron, once enabled)
  python aar_universe_openalex.py --since 2026-01-01 --until 2026-03-31   # dry-run a range
"""
import argparse, json, os, sys, time
from datetime import date, datetime

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from identity_index import IdentityIndex, CAMPUS_WCM   # noqa: E402
import aar_db                                          # noqa: E402
from aar_universe_scopus import (                      # noqa: E402
    doi_in_pubmed, resolve_no_doi, wcm_lane_candidates, _roster, rolling_window,
    recheck_open_scopus, _compact, _normalize_issn, _position_label, _trunc, _PM_SLEEP)

OPENALEX_WORKS = "https://api.openalex.org/works"
# Weill Cornell Medicine + Weill Cornell Medical Center. NOT WCM-Qatar (I4210152471).
# Same set as Scholars-Profile-System etl/fundable-work/work.ts WCM_OPENALEX_IDS.
WCM_OPENALEX_IDS = ("I4387153466", "I4405272987")
WORK_TYPES = ("conference-abstract", "preprint")
SELECT = ("id,doi,ids,title,display_name,type,publication_date,publication_year,"
          "primary_location,authorships")
PER_PAGE = 200            # OpenAlex maximum
PAGE_CAP = 10_000         # keyless page= paging stops here; cursor= needs an API key
SLEEP = 0.2               # polite throttle between pages


# ---- OpenAlex fetch ---------------------------------------------------------
def _openalex_get(params):
    params = dict(params)
    mailto = (os.environ.get("OPENALEX_MAILTO") or "").strip()
    if mailto:
        params["mailto"] = mailto
    for attempt in range(5):
        try:
            r = requests.get(OPENALEX_WORKS, params=params, timeout=60,
                             headers={"Accept": "application/json"})
        except (requests.ConnectionError, requests.Timeout):
            if attempt == 4:
                raise
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(2 * (attempt + 1))
            continue
        r.raise_for_status()   # any other 4xx/5xx is not transient: raise now, no retry
    raise RuntimeError("OpenAlex works fetch failed after retries")


def build_filter(since, until):
    return ",".join([f"institutions.id:{'|'.join(WCM_OPENALEX_IDS)}",
                     f"type:{'|'.join(WORK_TYPES)}",
                     f"from_publication_date:{since}", f"to_publication_date:{until}"])


def sweep(since, until, progress=True):
    """Every WCM conference abstract / preprint published in [since, until] (YYYY-MM-DD),
    deduped by work id. Stops at the 10,000-result page cap and says so: past it, narrow
    the window or get an API key and switch to cursor paging."""
    flt, by_id, total = build_filter(since, until), {}, None
    for page in range(1, PAGE_CAP // PER_PAGE + 1):
        res = _openalex_get({"filter": flt, "select": SELECT, "per-page": PER_PAGE,
                             "page": page})
        if total is None:
            total = int((res.get("meta") or {}).get("count") or 0)
            if total > PAGE_CAP:
                print(f"  WARNING: {total} works match but keyless page= paging stops at "
                      f"{PAGE_CAP}; the rest are NOT fetched. Narrow the window.", flush=True)
        results = res.get("results") or []
        for w in results:
            if w.get("id"):
                by_id[_work_id(w)] = w
        if progress:
            print(f"  page {page}: {len(by_id)} works so far (of {total})", flush=True)
        if len(results) < PER_PAGE:
            break
        time.sleep(SLEEP)
    else:
        print(f"  WARNING: hit the {PAGE_CAP}-result page cap; later works NOT fetched.",
              flush=True)
    return list(by_id.values())


# ---- parsing ----------------------------------------------------------------
def _short_id(url):
    """'https://openalex.org/W7167238871' -> 'W7167238871' (bare ids pass through)."""
    return url.rstrip("/").rsplit("/", 1)[-1] if url else None


def _work_id(w):
    return _short_id(w.get("id"))


def _doi(w):
    """Bare DOI ('10.1016/…') from OpenAlex's 'https://doi.org/…' form, or None."""
    d = (w.get("doi") or (w.get("ids") or {}).get("doi") or "").strip()
    for p in ("https://doi.org/", "http://doi.org/", "doi:"):
        if d.lower().startswith(p):
            d = d[len(p):]
    return d or None


def _openalex_pmid(w):
    """The PubMed id OpenAlex links this work to, or None. ids.pmid is a pubmed URL."""
    p = _short_id((w.get("ids") or {}).get("pmid"))
    return p if p and p.isdigit() else None


def _source(w):
    return ((w.get("primary_location") or {}).get("source")) or {}


_PARTICLES = {"van", "von", "de", "del", "della", "der", "den", "da", "di", "du", "dos",
              "das", "le", "la", "ter", "ten", "bin", "al", "el"}
_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "md", "phd", "mph", "msc", "mba", "rn"}


def _split_name(name):
    """Byline string -> (fore, last). OpenAlex gives one string per author, not Scopus's
    given-name/surname pair. 'Last, First' when a comma separates two name parts,
    otherwise the last token is the surname, keeping lowercase particles ('van der Berg')
    and dropping trailing suffixes ('Jr.', 'MD')."""
    parts = [p.strip() for p in " ".join((name or "").split()).split(",")]
    parts = [p for p in parts if p and p.lower().strip(".") not in _SUFFIXES]
    if len(parts) == 2:
        return parts[1] or None, parts[0] or None
    toks = " ".join(parts).split(" ")
    while len(toks) > 1 and toks[-1].lower().strip(".") in _SUFFIXES:
        toks.pop()
    if not toks or not toks[0]:
        return None, None
    if len(toks) == 1:
        return None, toks[0]
    j = len(toks) - 1
    while j > 1 and toks[j - 1].lower() in _PARTICLES:
        j -= 1
    return " ".join(toks[:j]), " ".join(toks[j:])


def _byline(au):
    """The name as printed on the work, not OpenAlex's author-entity display name, which
    can belong to a different person when OpenAlex mis-clusters an author."""
    return au.get("raw_author_name") or (au.get("author") or {}).get("display_name")


def wcm_authorships(w):
    """Yield (i, n, author-for-matcher) for authorships carrying a WCM institution id.
    `afids` is empty: those are Scopus afids, and the campus set comes from the byline's
    own affiliation text instead (aar_universe_scopus._campuses)."""
    authorships = w.get("authorships") or []
    n = len(authorships)
    for i, au in enumerate(authorships):
        if not any(_short_id(inst.get("id")) in WCM_OPENALEX_IDS
                   for inst in au.get("institutions") or []):
            continue
        fore, last = _split_name(_byline(au))
        if not last:
            continue
        yield i, n, {
            "last": last, "fore": fore,
            "initials": "".join(f"{t[0]}." for t in (fore or "").split() if t) or None,
            "affiliations": list(au.get("raw_affiliation_strings") or []),
            "afids": [],
        }


def _authors_json(w):
    out = []
    for au in w.get("authorships") or []:
        fore, last = _split_name(_byline(au))
        out.append({"given": fore, "surname": last})
    return json.dumps(out)


def _first_author_last(w):
    authorships = w.get("authorships") or []
    return _split_name(_byline(authorships[0]))[1] if authorships else None


def _build_row(w, i, n, author, top, cands, run_ts, dup_map=None):
    wid, doi = _work_id(w), _doi(w)
    cohort = top.get("cohort_size") if top else None
    dup_uid = aar_db.dup_uid_for_authorship(dup_map, doi, top, cands)
    return {
        "source": "openalex", "pmid": None,
        # bare work id -> the PM Accept builds articleId "OPENALEX:<external_id>".
        "external_id": _trunc(wid, 96),
        # DOI-first like the Scopus lane: OpenAlex merges duplicate works and the losing W
        # id redirects, so the DOI is the steadier identity where there is one.
        "author_key": _trunc(f"openalex:{doi or wid}:{i}", 160),
        "pub_type": _trunc(w.get("type"), 40),
        "container_id": None,
        "author_position": i + 1, "author_position_label": _position_label(i, n),
        "wcm_author": _trunc(" ".join(x for x in (author.get("fore"), author.get("last")) if x),
                             255),
        "author_affiliation": " | ".join(author.get("affiliations") or []),
        "entrez_date": w.get("publication_date"),     # no entrez date; publication date
        "title": w.get("title") or w.get("display_name"),
        "journal": _trunc(_source(w).get("display_name"), 512),
        "issn": _normalize_issn(_source(w).get("issn_l")),
        "isbn": None,
        "doi": _trunc(doi, 255),
        "authors_json": _authors_json(w),
        "classification": "absent",                    # no PMID -> production never scored it
        "top_cwid": top["cwid"] if top else None,
        "top_name": _trunc(top["name"], 255) if top else None,
        "top_person_type": _trunc(top["person_type"], 64) if top else None,
        "top_dept": _trunc(top["dept"], 255) if top else None,
        "top_fg_score": None, "top_io_score": None,
        "top_confidence": top["confidence"] if top else None,
        "top_years_after_wcm": top.get("years_after_wcm") if top else None,
        "top_cohort_size": cohort,
        "top_given_match": top["given_match"] if top else None,
        "top_affil_match": int(bool(top["affil_dept_match"])) if top else None,
        "n_candidates": len(cands), "single_candidate": int(cohort == 1) if cohort else None,
        "candidate_cwids_json": json.dumps(_compact(cands)),
        "dup_flag": int(bool(dup_uid)),
        "dup_reason": (f"Already added as ExternalArticle for {dup_uid} (DOI match)"
                       if dup_uid else None),
        "matched_pmid": w.get("_matched_pmid"),
        "matched_pmid_source": "title" if w.get("_matched_pmid") else None,
        "matched_pmid_at": run_ts if w.get("_matched_pmid") else None,
        "status": "open", "first_seen": run_ts,
        "last_checked": run_ts, "last_refreshed": run_ts,
    }


# ---- reciterdb reads ----------------------------------------------------------
def scopus_lane_dois(dois):
    """Lowercased DOIs (of `dois`) the Scopus lane already holds in authorship_review, any
    status. Read-only, batched like aar_db.dup_flags_by_doi."""
    from sqlalchemy import bindparam, text
    dois = sorted({d for d in dois if d})
    out = set()
    if not dois:
        return out
    stmt = text("SELECT doi FROM authorship_review WHERE source='scopus' AND doi IN :ds") \
        .bindparams(bindparam("ds", expanding=True))
    with aar_db.engine().connect() as c:
        for i in range(0, len(dois), 500):
            out.update(d.lower() for (d,) in c.execute(stmt, {"ds": dois[i:i + 500]}) if d)
    return out


def dismiss_scopus_held(run_ts):
    """Dismiss open openalex rows whose DOI the Scopus lane now holds (any status) --
    the other direction of step 2's drop; see the module docstring. Returns the count."""
    from sqlalchemy import text
    with aar_db.engine().begin() as c:
        return c.execute(text(
            "UPDATE authorship_review oa JOIN authorship_review sc "
            "ON sc.source='scopus' AND sc.doi IS NOT NULL AND LOWER(sc.doi)=LOWER(oa.doi) "
            "SET oa.status='dismissed', oa.resolved_at=:ts, "
            "oa.note=CONCAT('auto: Scopus lane holds DOI ', oa.doi) "
            "WHERE oa.source='openalex' AND oa.status='open' AND oa.doi IS NOT NULL"),
            {"ts": run_ts}).rowcount


def source_enum_ready():
    """Does authorship_review.source accept 'openalex' (migration v3.2 applied)?"""
    from sqlalchemy import text
    with aar_db.engine().connect() as c:
        col = c.execute(text(
            "SELECT column_type FROM information_schema.columns WHERE table_schema=DATABASE() "
            "AND table_name='authorship_review' AND column_name='source'")).scalar()
    return "'openalex'" in (col or "")


# ---- driver -----------------------------------------------------------------
def run(since, until, apply_writes=False, recheck=True, idx=None):
    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if apply_writes and not source_enum_ready():
        raise RuntimeError("authorship_review.source has no 'openalex' value: apply "
                           "setup/alter_authorship_review_add_openalex_v3.2.sql first")

    print(f"[1/5] OpenAlex fetch: {'|'.join(WORK_TYPES)}, publication date {since}..{until}",
          flush=True)
    works = sweep(since, until)
    print(f"      {len(works)} WCM works", flush=True)

    no_pmid = [w for w in works if not _openalex_pmid(w)]
    in_scopus = scopus_lane_dois({_doi(w) for w in no_pmid if _doi(w)})
    candidates = [w for w in no_pmid if not (_doi(w) and _doi(w).lower() in in_scopus)]
    print(f"[2/5] {len(works) - len(no_pmid)} carry an OpenAlex PMID (dropped); "
          f"{len(no_pmid) - len(candidates)} already in the Scopus lane by DOI (dropped); "
          f"{len(candidates)} left", flush=True)

    print("[3/5] Resolving against PubMed (DOI, else title) ...", flush=True)
    openalex_only, resolved, flagged_by_title = [], 0, 0
    for w in candidates:
        doi = _doi(w)
        if doi:
            hit = doi_in_pubmed(doi)
            time.sleep(_PM_SLEEP)
            if hit:
                resolved += 1
                continue
        else:
            hit = resolve_no_doi(w.get("title") or w.get("display_name"), _first_author_last(w),
                                 w.get("type"), _source(w).get("issn_l"), None)
            time.sleep(_PM_SLEEP)
            if hit:
                # Same v2.7 rule as the Scopus lane: a title hit is a candidate for a
                # curator to adjudicate, not grounds to drop the work.
                flagged_by_title += 1
                w["_matched_pmid"] = hit
        openalex_only.append(w)
    print(f"      {resolved} resolved to PubMed by DOI (dropped); {flagged_by_title} title-matched "
          f"(kept, flagged); {len(openalex_only)} OPENALEX-ONLY", flush=True)

    print("[4/5] Matching WCM authorships against the identity roster ...", flush=True)
    if idx is None:
        idx = _roster(CAMPUS_WCM)
    dois = {_doi(w) for w in openalex_only if _doi(w)}
    dup_map = aar_db.dup_flags_by_doi(dois) if dois else {}
    rows, unmatched = [], 0
    for w in openalex_only:
        for i, n, au in wcm_authorships(w):
            # frozenset(): no Scopus afids on this lane, so no Ithaca afid list to load.
            cands, _ = wcm_lane_candidates(idx, au, w.get("publication_year"), frozenset())
            top = cands[0] if cands else None
            if top is None:
                unmatched += 1
                continue
            rows.append(_build_row(w, i, n, au, top, cands, run_ts, dup_map=dup_map))
    print(f"      {len(rows)} matched authorships; {unmatched} unmatched (skipped)", flush=True)

    if apply_writes:
        n = aar_db.upsert(rows)
        print(f"[5/5] upserted {n} openalex authorships -> reciterdb.authorship_review", flush=True)
        n_scopus = dismiss_scopus_held(run_ts)
        print(f"      {n_scopus} open openalex rows dismissed: the Scopus lane now holds "
              f"their DOI", flush=True)
        if recheck:
            n_dismissed, n_flagged = recheck_open_scopus(run_ts, source="openalex")
            print(f"      re-check: {n_dismissed} open openalex rows resolved out (DOI match), "
                  f"{n_flagged} flagged with a candidate PMID (title match)", flush=True)
    else:
        print(f"[5/5] DRY-RUN: {len(rows)} rows NOT written (pass --apply to upsert)", flush=True)

    by_type = {}
    for w in openalex_only:
        by_type[w.get("type")] = by_type.get(w.get("type"), 0) + 1
    return {"window": {"since": since, "until": until}, "works": len(works),
            "openalex_pmid": len(works) - len(no_pmid),
            "in_scopus_lane": len(no_pmid) - len(candidates),
            "doi_resolved_pubmed": resolved, "flagged_by_title": flagged_by_title,
            "openalex_only": len(openalex_only), "openalex_only_by_type": by_type,
            "matched_rows": len(rows), "unmatched": unmatched}


def _iso(yyyymmdd):
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"


def openalex_window(today=None, lag_days=14, span_days=365):
    """rolling_window's [today-span, today-lag], as the YYYY-MM-DD dates OpenAlex takes."""
    aft, bef = rolling_window(today, lag_days=lag_days, span_days=span_days)
    return _iso(aft), _iso(bef)


# ---- self-test (offline: no network / DB) -----------------------------------
def _fixture():
    wcm = {"id": "https://openalex.org/I4387153466", "display_name": "Weill Cornell Medicine"}
    other = {"id": "https://openalex.org/I48633490", "display_name": "Mass General Brigham"}
    return {
        "id": "https://openalex.org/W7167238871",
        "doi": "https://doi.org/10.1016/j.jtho.2026.104007",
        "ids": {"openalex": "https://openalex.org/W7167238871",
                "doi": "https://doi.org/10.1016/j.jtho.2026.104007"},
        "title": "PP01.08: Landscape of Resistance", "type": "conference-abstract",
        "publication_date": "2026-07-01", "publication_year": 2026,
        "primary_location": {"source": {"display_name": "Journal of Thoracic Oncology",
                                        "issn_l": "1556-0864"}},
        "authorships": [
            {"author_position": "first", "author": {"display_name": "Sarah Waliany"},
             "institutions": [other], "raw_author_name": "Sarah Waliany",
             "raw_affiliation_strings": ["Mass General Brigham, Boston, MA"]},
            {"author_position": "middle", "author": {"display_name": "Alissa Jamie Cooper"},
             "institutions": [other, wcm], "raw_author_name": "Alissa J. Cooper",
             "raw_affiliation_strings": ["Weill Cornell Medicine, New York, NY"]},
            {"author_position": "last", "author": {"display_name": "Ann Dual"},
             "institutions": [wcm], "raw_author_name": "Dual, Ann",
             "raw_affiliation_strings": ["Weill Cornell Medicine, New York, NY"]},
        ],
    }


def _selftest():
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  [{'OK' if cond else '** FAIL'}] {label}")

    w = _fixture()
    check("work id is the bare W id", _work_id(w) == "W7167238871")
    check("DOI loses the https://doi.org/ prefix", _doi(w) == "10.1016/j.jtho.2026.104007")
    check("no ids.pmid -> no OpenAlex PMID", _openalex_pmid(w) is None)
    check("ids.pmid URL -> bare PMID",
          _openalex_pmid({"ids": {"pmid": "https://pubmed.ncbi.nlm.nih.gov/41234567"}})
          == "41234567")
    check("filter names both WCM ids, both types and the date window",
          build_filter("2026-01-01", "2026-03-31")
          == "institutions.id:I4387153466|I4405272987,type:conference-abstract|preprint,"
             "from_publication_date:2026-01-01,to_publication_date:2026-03-31")
    check("WCM-Qatar is not in the WCM set", "I4210152471" not in WCM_OPENALEX_IDS)

    check("split: 'First M. Last'", _split_name("Alissa J. Cooper") == ("Alissa J.", "Cooper"))
    check("split: 'Last, First'", _split_name("Dual, Ann") == ("Ann", "Dual"))
    check("split: particles stay on the surname",
          _split_name("Maria van der Berg") == ("Maria", "van der Berg"))
    check("split: trailing suffixes dropped",
          _split_name("John Smith Jr.") == ("John", "Smith")
          and _split_name("John Smith, MD") == ("John", "Smith"))
    check("split: one token is a surname only, empty is nothing",
          _split_name("Cher") == (None, "Cher") and _split_name("") == (None, None))

    wcm = list(wcm_authorships(w))
    check("two WCM authorships (positions 1 and 2), the MGB-only author skipped",
          [(i, n) for i, n, _ in wcm] == [(1, 3), (2, 3)])
    check("matcher author from the printed byline, affiliations kept, no afids",
          wcm[0][2] == {"last": "Cooper", "fore": "Alissa J.", "initials": "A.J.",
                        "affiliations": ["Weill Cornell Medicine, New York, NY"], "afids": []})

    top = {"cwid": "ac1", "name": "Alissa Cooper", "person_type": "Faculty", "dept": "Med",
           "confidence": 0.9, "cohort_size": 1, "given_match": "full", "affil_dept_match": False}
    i, n, au = wcm[0]
    row = _build_row(w, i, n, au, top, [top], "2026-10-29 00:00:00")
    check("row carries every aar_db insert column", set(aar_db._INSERT_COLS) <= set(row))
    check("row source=openalex, pmid None, external_id = W id",
          row["source"] == "openalex" and row["pmid"] is None
          and row["external_id"] == "W7167238871")
    check("row author_key = openalex:{doi}:{index}",
          row["author_key"] == "openalex:10.1016/j.jtho.2026.104007:1")
    check("row pub_type = OpenAlex type, journal + issn from the primary source",
          row["pub_type"] == "conference-abstract"
          and row["journal"] == "Journal of Thoracic Oncology" and row["issn"] == "1556-0864")
    check("row position 2 of 3 is 'middle'",
          row["author_position"] == 2 and row["author_position_label"] == "middle")
    check("row classification absent, no FG/IO, single candidate",
          row["classification"] == "absent" and row["top_fg_score"] is None
          and row["single_candidate"] == 1)
    check("row authors_json lists every author",
          [a["surname"] for a in json.loads(row["authors_json"])] == ["Waliany", "Cooper", "Dual"])
    check("row dup_flag set only for a candidate the DOI was added for",
          _build_row(w, i, n, au, top, [top], "t",
                     dup_map={"10.1016/j.jtho.2026.104007": {"ac1"}})["dup_flag"] == 1
          and _build_row(w, i, n, au, top, [top], "t",
                         dup_map={"10.1016/j.jtho.2026.104007": {"zz9"}})["dup_flag"] == 0)
    nodoi = dict(w, doi=None, ids={"openalex": w["id"]}, _matched_pmid=42)
    row2 = _build_row(nodoi, i, n, au, top, [top], "2026-10-29 00:00:00")
    check("no-DOI row keys on the W id and carries a title-match flag",
          row2["author_key"] == "openalex:W7167238871:1" and row2["doi"] is None
          and row2["matched_pmid"] == 42 and row2["matched_pmid_source"] == "title")

    check("default window is [today-365d, today-14d] as YYYY-MM-DD",
          openalex_window(date(2026, 10, 29)) == ("2025-10-29", "2026-10-15"))

    # ---- offline end-to-end: run() over canned pages, with PubMed, reciterdb and the
    # roster replaced. Everything patched is restored in `finally`.
    from identity_index import _norm
    g = globals()
    saved = {k: g[k] for k in ("_openalex_get", "doi_in_pubmed", "resolve_no_doi",
                               "scopus_lane_dois", "source_enum_ready", "time",
                               "dismiss_scopus_held", "recheck_open_scopus", "requests")}
    saved_upsert = aar_db.upsert
    saved_dup = aar_db.dup_flags_by_doi
    pmid_work = dict(_fixture(), id="https://openalex.org/W2",
                     ids={"pmid": "https://pubmed.ncbi.nlm.nih.gov/41234567"})
    pubmed_doi = dict(_fixture(), id="https://openalex.org/W3", doi="https://doi.org/10.1/inpm")
    scopus_doi = dict(_fixture(), id="https://openalex.org/W4", doi="https://doi.org/10.1/SC")
    preprint = dict(_fixture(), id="https://openalex.org/W5", doi=None, ids={},
                    type="preprint", title="A preprint")
    pages = {1: [_fixture(), pmid_work, pubmed_doi, scopus_doi] * 50, 2: [preprint]}
    calls = []

    class _NoSleep:
        @staticmethod
        def sleep(_):
            pass

    try:
        g["_openalex_get"] = lambda p: (calls.append(p) or
                                        {"meta": {"count": 201}, "results": pages[p["page"]]})
        g["doi_in_pubmed"] = lambda d: 999 if d == "10.1/inpm" else None
        g["resolve_no_doi"] = lambda *a: 777 if a[0] == "A preprint" else None
        g["scopus_lane_dois"] = lambda ds: {"10.1/sc"} & {d.lower() for d in ds}
        g["time"] = _NoSleep
        aar_db.dup_flags_by_doi = lambda ds: {}

        def rec(given, surname, cwid):
            return {"cwid": cwid, "given": given, "middle": "", "surname": surname,
                    "given_norm": _norm(given), "surname_norm": _norm(surname),
                    "pref_norm": "", "dept": "", "division": "", "program": "", "title": "",
                    "person_type": "Faculty", "historical": False, "end_year": None,
                    "campus": CAMPUS_WCM}

        s = run("2026-01-01", "2026-12-31", idx=IdentityIndex([rec("Alissa", "Cooper", "ac1")]))
        check("paging stops on the first short page", [c["page"] for c in calls] == [1, 2])
        check("works deduped by W id across pages", s["works"] == 5)
        check("ids.pmid work and DOI-in-PubMed work dropped; Scopus-lane DOI dropped "
              "case-insensitively",
              s["openalex_pmid"] == 1 and s["doi_resolved_pubmed"] == 1
              and s["in_scopus_lane"] == 1)
        check("the no-DOI preprint is kept and title-flagged",
              s["flagged_by_title"] == 1 and s["openalex_only"] == 2
              and s["openalex_only_by_type"] == {"conference-abstract": 1, "preprint": 1})
        check("one matched authorship per kept work (Cooper); Dual is unmatched",
              s["matched_rows"] == 2 and s["unmatched"] == 2)

        import contextlib, io
        calls.clear()
        full = [dict(_fixture(), id=f"https://openalex.org/W{k}") for k in range(PER_PAGE)]
        g["_openalex_get"] = lambda p: calls.append(p) or {"meta": {"count": 25000},
                                                           "results": full}
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            sweep("2026-01-01", "2026-12-31")
        check("sweep stops at the 10,000-result page cap and logs it",
              len(calls) == PAGE_CAP // PER_PAGE
              and buf.getvalue().count("WARNING") == 2)

        # --apply path: upsert, then the Scopus-held dismissal, then the recheck on
        # source='openalex' -- in that order.
        order = []
        g["_openalex_get"] = lambda p: {"meta": {"count": 1}, "results": [_fixture()]}
        g["source_enum_ready"] = lambda: True
        aar_db.upsert = lambda rows: order.append(("upsert", len(rows))) or len(rows)
        g["dismiss_scopus_held"] = lambda ts: order.append(("scopus_held",)) or 0
        g["recheck_open_scopus"] = lambda ts, source="scopus": order.append(("recheck", source)) or (0, 0)
        with contextlib.redirect_stdout(io.StringIO()):
            run("2026-01-01", "2026-12-31", apply_writes=True,
                idx=IdentityIndex([rec("Alissa", "Cooper", "ac1")]))
        check("--apply: upsert, then dismiss Scopus-held DOIs, then recheck source=openalex",
              order == [("upsert", 1), ("scopus_held",), ("recheck", "openalex")])

        # a non-retryable 4xx raises on the first response, no backoff retries
        import requests as _rq
        gets = []

        class _Resp:
            status_code = 400

            def raise_for_status(self):
                raise _rq.HTTPError("400")

        class _FakeRequests:
            ConnectionError, Timeout, HTTPError = _rq.ConnectionError, _rq.Timeout, _rq.HTTPError

            @staticmethod
            def get(*a, **k):
                gets.append(1)
                return _Resp()

        g["_openalex_get"] = saved["_openalex_get"]
        g["requests"] = _FakeRequests
        try:
            _openalex_get({"page": 51})
            raised = False
        except _rq.HTTPError:
            raised = True
        check("a 400 from OpenAlex raises after one request, not five", raised and gets == [1])

        g["source_enum_ready"] = lambda: False
        try:
            run("2026-01-01", "2026-12-31", apply_writes=True)
            refused = False
        except RuntimeError:
            refused = True
        check("--apply refused before migration v3.2 adds 'openalex' to the ENUM", refused)
    finally:
        g.update(saved)
        aar_db.dup_flags_by_doi = saved_dup
        aar_db.upsert = saved_upsert

    print("\nSELFTEST", "PASS" if ok else "FAIL")
    return ok


def main():
    ap = argparse.ArgumentParser(description="OpenAlex not-in-PubMed WCM abstract/preprint detector")
    ap.add_argument("--mode", choices=["rolling"],
                    help="rolling = publication date in [today-span, today-lag]")
    ap.add_argument("--lag-days", type=int, default=14)
    ap.add_argument("--span-days", type=int, default=365)
    ap.add_argument("--since", help="YYYY-MM-DD publication-date start (with --until)")
    ap.add_argument("--until", help="YYYY-MM-DD publication-date end (with --since)")
    ap.add_argument("--apply", action="store_true", help="write rows (default: dry-run)")
    ap.add_argument("--no-recheck", action="store_true",
                    help="skip resolving open openalex rows out (only meaningful with --apply)")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()

    if args.selftest:
        sys.exit(0 if _selftest() else 1)
    if args.mode == "rolling":
        since, until = openalex_window(lag_days=args.lag_days, span_days=args.span_days)
    elif args.since and args.until:
        since, until = args.since, args.until
    else:
        ap.error("provide --mode rolling, --since/--until YYYY-MM-DD, or --selftest")

    print(f"OpenAlex lane: publication date {since}..{until}"
          + ("  [APPLY]" if args.apply else "  [DRY-RUN]"), flush=True)
    summary = run(since, until, apply_writes=args.apply, recheck=not args.no_recheck)
    print("\n==== OPENALEX RUN SUMMARY ====")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
