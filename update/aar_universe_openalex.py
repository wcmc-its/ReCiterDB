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
  3b. A preprint whose PUBLISHED version resolves in PubMed is dropped too (counted as
     preprint_published_pubmed): its own DOI never matches, because the published
     article has a different DOI. See published_in_pubmed() for where the link comes from.
  5. Upsert via aar_db (source='openalex', external_id=W id, pub_type=OpenAlex type,
     pmid=NULL); then dismiss open openalex rows whose DOI the Scopus lane has since
     picked up, and open rows for preprints step 3b dropped; then the Scopus lane's
     recheck, run over source='openalex' open rows.

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
OPENALEX_MAILTO (optional polite-pool address, same env name as Scholars-Profile-System),
CROSSREF_MAILTO (optional Crossref polite-pool address; falls back to OPENALEX_MAILTO),
OPENALEX_PREPRINT_LINK_SECONDS (wall-clock cap on step 3b, default 600).

Usage:
  python aar_universe_openalex.py --selftest                     # offline checks, fixtures only
  python aar_universe_openalex.py --mode rolling                 # dry-run the default window
  python aar_universe_openalex.py --mode rolling --apply         # write (cron, once enabled)
  python aar_universe_openalex.py --since 2026-01-01 --until 2026-03-31   # dry-run a range
"""
import argparse, json, os, re, sys, time
from datetime import date, datetime

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from identity_index import IdentityIndex, CAMPUS_WCM   # noqa: E402
import aar_db                                          # noqa: E402
from aar_universe_scopus import (                      # noqa: E402
    doi_in_pubmed, resolve_no_doi, wcm_lane_candidates, _roster, rolling_window,
    recheck_open_scopus, _compact, _normalize_issn, _position_label, _trunc, _PM_SLEEP,
    PUBMED_STATS, reset_pubmed_stats, pubmed_unreliable)

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

CROSSREF_WORKS = "https://api.crossref.org/works"
CROSSREF_BATCH = 40       # DOIs per filter=doi:a,doi:b call
CROSSREF_SLEEP = 0.4      # Crossref's public list-query limit is 3 req/s (x-rate-limit-limit)
RXIV_DETAILS = "https://api.biorxiv.org/details/{server}/{doi}"
RXIV_PREFIXES = ("10.1101/", "10.64898/")   # bioRxiv + medRxiv (10.64898 since late 2025)
RXIV_SLEEP = 0.2


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


def _bare_doi(d):
    """'https://doi.org/10.1/x', 'http://doi.org/…', 'doi:…' or '10.1/x' -> '10.1/x', or None."""
    d = (d or "").strip()
    for p in ("https://doi.org/", "http://doi.org/", "https://dx.doi.org/",
              "http://dx.doi.org/", "doi:"):
        if d.lower().startswith(p):
            d = d[len(p):].strip()
    return d or None


def _doi(w):
    """Bare DOI ('10.1016/…') from OpenAlex's 'https://doi.org/…' form, or None."""
    return _bare_doi(w.get("doi") or (w.get("ids") or {}).get("doi"))


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
    dup_how = "DOI match"
    if not dup_uid and wid:
        dup_uid = aar_db.dup_uid_for_authorship(dup_map, f"OPENALEX:{wid}", top, cands)
        dup_how = "OpenAlex id match"
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
        "dup_reason": (f"Already added as ExternalArticle for {dup_uid} ({dup_how})"
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


def external_article_uids_by_wid(wids):
    """'openalex:w…' -> {uid, ...} for works already hand-added as OPENALEX:<W id>
    ExternalArticles. Catches DOI-less works dup_flags_by_doi can't see; keyed lowercase
    so it merges into the same dup_map. Read-only, batched."""
    from sqlalchemy import bindparam, text
    keys = sorted({f"OPENALEX:{w}" for w in wids if w})
    out = {}
    if not keys:
        return out
    stmt = text("SELECT article_id, uid FROM external_article WHERE article_id IN :ks") \
        .bindparams(bindparam("ks", expanding=True))
    with aar_db.engine().connect() as c:
        for i in range(0, len(keys), 500):
            for aid, uid in c.execute(stmt, {"ks": keys[i:i + 500]}):
                out.setdefault(aid.lower(), set()).add(uid)
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


# ---- preprint -> published version (step 3b) --------------------------------
def _contact_ua():
    mailto = (os.environ.get("CROSSREF_MAILTO") or os.environ.get("OPENALEX_MAILTO") or "").strip()
    return "reciterdb-aar-openalex" + (f" (mailto:{mailto})" if mailto else "")


def _http_json(url, params=None):
    r = None
    for attempt in range(3):
        r = requests.get(url, params=params, timeout=30,
                         headers={"Accept": "application/json", "User-Agent": _contact_ua()})
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(2 * (attempt + 1))
            continue
        r.raise_for_status()
    raise RuntimeError(f"{url}: HTTP {r.status_code} after retries")


def _elife_vor(doi):
    """eLife reviewed preprint 10.7554/eLife.N.v -> its version of record 10.7554/eLife.N."""
    m = re.fullmatch(r"(10\.7554/elife\.\d+)\.\d+", doi or "", re.I)
    return m.group(1) if m else None


def _rxiv_server(w):
    doi = (_doi(w) or "").lower()
    if not doi.startswith(RXIV_PREFIXES):
        return None
    return "medrxiv" if "medrxiv" in (_source(w).get("display_name") or "").lower() else "biorxiv"


def _crossref_preprint_of(items):
    """Crossref work items -> {doi_lower: [published DOI, ...]} from relation is-preprint-of."""
    out = {}
    for it in items or []:
        rel = [x.get("id") for x in ((it.get("relation") or {}).get("is-preprint-of") or [])
               if x.get("id-type") == "doi" and x.get("id")]
        if rel and it.get("DOI"):
            out[it["DOI"].lower()] = rel
    return out


class PartialLookupError(Exception):
    """A batched lookup stopped early. `.partial` holds what the earlier batches found."""
    def __init__(self, msg, partial):
        super().__init__(msg)
        self.partial = partial


def crossref_preprint_of(dois, deadline):
    """Batched Crossref lookup, CROSSREF_BATCH DOIs per call. On a failed batch or past
    `deadline` it raises PartialLookupError carrying the links the earlier batches found,
    so one bad batch does not throw away the good ones."""
    dois = sorted({d.lower() for d in dois if d and "," not in d})
    out = {}
    for i in range(0, len(dois), CROSSREF_BATCH):
        if time.monotonic() > deadline:
            raise PartialLookupError("hit OPENALEX_PREPRINT_LINK_SECONDS", out)
        chunk = dois[i:i + CROSSREF_BATCH]
        try:
            j = _http_json(CROSSREF_WORKS, {"filter": ",".join(f"doi:{d}" for d in chunk),
                                            "select": "DOI,relation", "rows": len(chunk)})
        except Exception as e:   # noqa: BLE001
            raise PartialLookupError(f"batch {i // CROSSREF_BATCH + 1} failed "
                                     f"({type(e).__name__}: {e})", out) from e
        out.update(_crossref_preprint_of((j.get("message") or {}).get("items")))
        time.sleep(CROSSREF_SLEEP)
    return out


def _rxiv_published(j):
    """bioRxiv/medRxiv details response -> the published DOI, or None ('NA' = unpublished)."""
    pubs = [c.get("published") for c in (j or {}).get("collection") or []]
    pubs = [p for p in pubs if p and p != "NA"]
    return pubs[-1] if pubs else None


def rxiv_published(server, doi):
    """Published DOI from the bioRxiv/medRxiv details API. A 10.1101 DOI sent to 'biorxiv'
    only because OpenAlex named no medRxiv venue is retried on medRxiv when bioRxiv has
    no record of it (pre-2025 medRxiv shares the 10.1101 prefix)."""
    j = _http_json(RXIV_DETAILS.format(server=server, doi=doi))
    if server == "biorxiv" and not (j or {}).get("collection"):
        j = _http_json(RXIV_DETAILS.format(server="medrxiv", doi=doi))
    return _rxiv_published(j)


def published_in_pubmed(preprints, budget_seconds=None):
    """{W id: (published DOI, PMID)} for preprints whose published version resolves in
    PubMed via the lane's quoted "<doi>"[DOI] search. Never raises: a failed or
    out-of-budget lookup leaves the work kept, and is logged.

    Where the link comes from, live-measured 2026-10-29 over the 301 OpenAlex-only
    preprints in the 365-day window:
      * bioRxiv/medRxiv details API `published` (108 bioRxiv + 45 medRxiv looked up):
        2 linked, 1 in PubMed. One call per DOI, the only per-work cost here.
      * eLife reviewed preprints (10.7554/eLife.N.v): the version of record is
        10.7554/eLife.N by eLife's DOI scheme, no call needed: 5 in PubMed.
      * Crossref relation is-preprint-of: 0 of 242 Crossref-registered preprint DOIs
        carry it (bioRxiv deposits none, even for an old bioRxiv->Nature pair).
        Kept because it is cheap (CROSSREF_BATCH DOIs per call) and generic.
      * OpenAlex carries no link: no journal publishedVersion location on any of them,
        and related_works is a similarity list."""
    if budget_seconds is None:
        raw = os.environ.get("OPENALEX_PREPRINT_LINK_SECONDS", "600")
        try:
            budget_seconds = int(raw)
        except ValueError:
            budget_seconds = 600
            print(f"      WARNING: OPENALEX_PREPRINT_LINK_SECONDS={raw!r} is not an "
                  f"integer; using 600", flush=True)
    start = time.monotonic()
    deadline = start + budget_seconds
    # The per-DOI bioRxiv/medRxiv calls stop at 80% of the budget so the PubMed checks
    # (which cover the call-free eLife links too) always get a share of it.
    rxiv_deadline = start + 0.8 * budget_seconds
    links, errors = {}, 0

    def add(wid, d):
        d = (_bare_doi(d) or "").lower()
        if d and d not in links.setdefault(wid, []):
            links[wid].append(d)

    by_doi = {}
    for w in preprints:
        if _doi(w):
            by_doi.setdefault(_doi(w).lower(), []).append(_work_id(w))
            add(_work_id(w), _elife_vor(_doi(w)))
    try:
        xref = crossref_preprint_of(by_doi, deadline)
    except Exception as e:   # noqa: BLE001 -- a Crossref failure must never fail the run
        errors += 1
        xref = getattr(e, "partial", None) or {}
        print(f"      WARNING: Crossref is-preprint-of lookup stopped ({type(e).__name__}: "
              f"{e}); keeping {len(xref)} links from earlier batches", flush=True)
    for d, pubs in xref.items():
        for wid in by_doi.get(d, []):
            for p in pubs:
                add(wid, p)
    rxiv_cache = {}
    for w in preprints:
        server = _rxiv_server(w)
        key = (server, (_doi(w) or "").lower())
        if not server or key in rxiv_cache:
            add(_work_id(w), rxiv_cache.get(key))
            continue
        if time.monotonic() > rxiv_deadline:
            print("      WARNING: bioRxiv/medRxiv share of OPENALEX_PREPRINT_LINK_SECONDS "
                  "reached; remaining bioRxiv/medRxiv preprints kept unchecked", flush=True)
            break
        try:
            rxiv_cache[key] = rxiv_published(server, key[1])
        except Exception as e:   # noqa: BLE001
            errors += 1
            rxiv_cache[key] = None
            print(f"      WARNING: {server} lookup failed for {key[1]} ({type(e).__name__})",
                  flush=True)
        add(_work_id(w), rxiv_cache[key])
        time.sleep(RXIV_SLEEP)

    out, pm_cache, pm_skipped = {}, {}, 0
    for wid, cands in links.items():
        for d in cands:
            if d not in pm_cache:
                if time.monotonic() > deadline:
                    pm_skipped += 1
                    break
                try:
                    pm_cache[d] = doi_in_pubmed(d)
                except Exception as e:   # noqa: BLE001
                    errors += 1
                    pm_cache[d] = None
                    print(f"      WARNING: PubMed lookup failed for {d} ({type(e).__name__})",
                          flush=True)
                time.sleep(_PM_SLEEP)
            if pm_cache[d]:
                out[wid] = (d, pm_cache[d])
                break
    if pm_skipped:
        print(f"      WARNING: OPENALEX_PREPRINT_LINK_SECONDS reached; {pm_skipped} linked "
              f"preprints kept without a PubMed check", flush=True)
    n_linked = sum(1 for c in links.values() if c)
    print(f"      preprint links: {n_linked} preprints carry a published DOI, {len(out)} "
          f"resolve in PubMed; {errors} lookup errors (kept)", flush=True)
    return out


def dismiss_published(published, run_ts):
    """Dismiss open openalex rows for preprints step 3b dropped. `published` maps
    W id -> (preprint DOI or None, published DOI, PMID). Returns the count."""
    from sqlalchemy import text
    n = 0
    stmt = text("UPDATE authorship_review SET status='dismissed', resolved_at=:ts, note=:note "
                "WHERE source='openalex' AND status='open' AND (external_id=:wid OR "
                "(doi IS NOT NULL AND :doi IS NOT NULL AND LOWER(doi)=LOWER(:doi)))")
    with aar_db.engine().begin() as c:
        for wid, (doi, pub_doi, pmid) in published.items():
            n += c.execute(stmt, {"ts": run_ts, "wid": wid, "doi": doi,
                                  "note": f"auto: published version {pub_doi} is PMID {pmid}"}
                           ).rowcount
    return n


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
    reset_pubmed_stats()
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
          f"(kept, flagged)", flush=True)
    pubs = published_in_pubmed([w for w in openalex_only if w.get("type") == "preprint"])
    published = {_work_id(w): (_doi(w),) + pubs[_work_id(w)]
                 for w in openalex_only if _work_id(w) in pubs}
    openalex_only = [w for w in openalex_only if _work_id(w) not in published]
    print(f"      {len(published)} preprints' published version is in PubMed (dropped); "
          f"{len(openalex_only)} OPENALEX-ONLY", flush=True)

    print("[4/5] Matching WCM authorships against the identity roster ...", flush=True)
    if idx is None:
        idx = _roster(CAMPUS_WCM)
    dois = {_doi(w) for w in openalex_only if _doi(w)}
    dup_map = aar_db.dup_flags_by_doi(dois) if dois else {}
    dup_map.update(external_article_uids_by_wid({_work_id(w) for w in openalex_only}))
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

    unreliable = pubmed_unreliable()
    print(f"      PubMed: {PUBMED_STATS['failures']} of {PUBMED_STATS['calls']} esearch calls "
          f"failed", flush=True)
    if apply_writes and unreliable:
        print(f"[5/5] SKIPPED: PubMed failed too often to trust the drops; {len(rows)} rows "
              f"NOT written (PUBMED_MAX_FAILURE_RATE)", flush=True)
    elif apply_writes:
        n = aar_db.upsert(rows)
        print(f"[5/5] upserted {n} openalex authorships -> reciterdb.authorship_review", flush=True)
        n_scopus = dismiss_scopus_held(run_ts)
        print(f"      {n_scopus} open openalex rows dismissed: the Scopus lane now holds "
              f"their DOI", flush=True)
        n_pub = dismiss_published(published, run_ts) if published else 0
        print(f"      {n_pub} open openalex rows dismissed: published version is in PubMed",
              flush=True)
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
            "preprint_published_pubmed": len(published),
            "openalex_only": len(openalex_only), "openalex_only_by_type": by_type,
            "matched_rows": len(rows), "unmatched": unmatched,
            "pubmed_calls": PUBMED_STATS["calls"], "pubmed_failures": PUBMED_STATS["failures"],
            "writes_skipped": bool(apply_writes and unreliable)}


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
    check("row dup_flag set by an OPENALEX:<W id> ExternalArticle when the DOI misses",
          "OpenAlex id match" in (_build_row(w, i, n, au, top, [top], "t",
                                  dup_map={"openalex:w7167238871": {"ac1"}})["dup_reason"] or ""))
    nodoi = dict(w, doi=None, ids={"openalex": w["id"]}, _matched_pmid=42)
    row2 = _build_row(nodoi, i, n, au, top, [top], "2026-10-29 00:00:00")
    check("no-DOI row keys on the W id and carries a title-match flag",
          row2["author_key"] == "openalex:W7167238871:1" and row2["doi"] is None
          and row2["matched_pmid"] == 42 and row2["matched_pmid_source"] == "title")

    check("default window is [today-365d, today-14d] as YYYY-MM-DD",
          openalex_window(date(2026, 10, 29)) == ("2025-10-29", "2026-10-15"))

    # ---- preprint -> published version: pure parsers
    check("eLife reviewed preprint .v -> version of record; VOR and others -> None",
          _elife_vor("10.7554/eLife.108190.2") == "10.7554/eLife.108190"
          and _elife_vor("10.7554/elife.108190") is None and _elife_vor("10.1101/x.1") is None)
    check("bioRxiv/medRxiv server from DOI prefix + venue; other prefixes skipped",
          _rxiv_server({"doi": "https://doi.org/10.64898/2026.06.19.733041",
                        "primary_location": {"source": {"display_name": "medRxiv"}}}) == "medrxiv"
          and _rxiv_server({"doi": "10.1101/2025.06.20.660826"}) == "biorxiv"
          and _rxiv_server({"doi": "10.48550/arxiv.2511.05887"}) is None)
    check("bioRxiv details: last non-NA `published`, all-NA -> None",
          _rxiv_published({"collection": [{"published": "NA"}, {"published": "10.1016/j.x.1"}]})
          == "10.1016/j.x.1"
          and _rxiv_published({"collection": [{"published": "NA"}]}) is None
          and _rxiv_published({"collection": []}) is None)
    check("Crossref is-preprint-of: DOI ids only, keyed lowercase",
          _crossref_preprint_of([
              {"DOI": "10.21203/RS.3.rs-1/v1", "relation": {"is-preprint-of": [
                  {"id-type": "doi", "id": "10.1038/s1"}, {"id-type": "uri", "id": "http://x"}]}},
              {"DOI": "10.64898/a", "relation": {"is-version-of": [{"id-type": "doi", "id": "q"}]}},
          ]) == {"10.21203/rs.3.rs-1/v1": ["10.1038/s1"]})

    # ---- offline end-to-end: run() over canned pages, with PubMed, reciterdb and the
    # roster replaced. Everything patched is restored in `finally`.
    from identity_index import _norm
    g = globals()
    saved = {k: g[k] for k in ("_openalex_get", "doi_in_pubmed", "resolve_no_doi",
                               "scopus_lane_dois", "source_enum_ready", "time",
                               "dismiss_scopus_held", "recheck_open_scopus", "requests",
                               "external_article_uids_by_wid", "crossref_preprint_of",
                               "rxiv_published", "dismiss_published", "pubmed_unreliable")}
    _http_json_saved = _http_json
    saved_upsert = aar_db.upsert
    saved_dup = aar_db.dup_flags_by_doi
    pmid_work = dict(_fixture(), id="https://openalex.org/W2",
                     ids={"pmid": "https://pubmed.ncbi.nlm.nih.gov/41234567"})
    pubmed_doi = dict(_fixture(), id="https://openalex.org/W3", doi="https://doi.org/10.1/inpm")
    scopus_doi = dict(_fixture(), id="https://openalex.org/W4", doi="https://doi.org/10.1/SC")
    preprint = dict(_fixture(), id="https://openalex.org/W5", doi=None, ids={},
                    type="preprint", title="A preprint")
    published_pp = dict(_fixture(), id="https://openalex.org/W6", type="preprint",
                        doi="https://doi.org/10.64898/2026.01.01.000001",
                        primary_location={"source": {"display_name": "bioRxiv"}})
    pages = {1: [_fixture(), pmid_work, pubmed_doi, scopus_doi] * 50, 2: [preprint, published_pp]}
    calls = []

    class _NoSleep:
        @staticmethod
        def sleep(_):
            pass

        monotonic = staticmethod(time.monotonic)

    try:
        g["_openalex_get"] = lambda p: (calls.append(p) or
                                        {"meta": {"count": 201}, "results": pages[p["page"]]})
        g["doi_in_pubmed"] = lambda d: {"10.1/inpm": 999, "10.1016/pub": 555}.get(d)
        g["crossref_preprint_of"] = lambda ds, dl: {}
        g["rxiv_published"] = lambda srv, d: "10.1016/PUB" if d.endswith(".000001") else None
        g["resolve_no_doi"] = lambda *a: 777 if a[0] == "A preprint" else None
        g["scopus_lane_dois"] = lambda ds: {"10.1/sc"} & {d.lower() for d in ds}
        g["time"] = _NoSleep
        aar_db.dup_flags_by_doi = lambda ds: {}
        g["external_article_uids_by_wid"] = lambda ws: {}

        def rec(given, surname, cwid):
            return {"cwid": cwid, "given": given, "middle": "", "surname": surname,
                    "given_norm": _norm(given), "surname_norm": _norm(surname),
                    "pref_norm": "", "dept": "", "division": "", "program": "", "title": "",
                    "person_type": "Faculty", "historical": False, "end_year": None,
                    "campus": CAMPUS_WCM}

        s = run("2026-01-01", "2026-12-31", idx=IdentityIndex([rec("Alissa", "Cooper", "ac1")]))
        check("paging stops on the first short page", [c["page"] for c in calls] == [1, 2])
        check("works deduped by W id across pages", s["works"] == 6)
        check("bioRxiv preprint whose published DOI is in PubMed dropped and counted",
              s["preprint_published_pubmed"] == 1)
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
        g["_openalex_get"] = lambda p: {"meta": {"count": 2}, "results": [_fixture(), published_pp]}
        g["dismiss_published"] = lambda pubs, ts: order.append(("published", pubs)) or 0
        g["source_enum_ready"] = lambda: True
        aar_db.upsert = lambda rows: order.append(("upsert", len(rows))) or len(rows)
        g["dismiss_scopus_held"] = lambda ts: order.append(("scopus_held",)) or 0
        g["recheck_open_scopus"] = lambda ts, source="scopus": order.append(("recheck", source)) or (0, 0)
        with contextlib.redirect_stdout(io.StringIO()):
            run("2026-01-01", "2026-12-31", apply_writes=True,
                idx=IdentityIndex([rec("Alissa", "Cooper", "ac1")]))
        check("--apply: upsert, dismiss Scopus-held DOIs, dismiss published-in-PubMed "
              "preprints (W id, preprint DOI, published DOI, PMID), then recheck",
              order == [("upsert", 1), ("scopus_held",),
                        ("published", {"W6": ("10.64898/2026.01.01.000001", "10.1016/pub", 555)}),
                        ("recheck", "openalex")])

        # PubMed too unreliable to trust the drops: --apply writes and dismisses nothing.
        order.clear()
        g["pubmed_unreliable"] = lambda: True
        with contextlib.redirect_stdout(io.StringIO()):
            skipped = run("2026-01-01", "2026-12-31", apply_writes=True,
                          idx=IdentityIndex([rec("Alissa", "Cooper", "ac1")]))
        g["pubmed_unreliable"] = saved["pubmed_unreliable"]
        check("--apply skips upsert, dismissals and recheck when PubMed is unreliable",
              order == [] and skipped["writes_skipped"] is True)

        # step 3b never fails the run: Crossref / bioRxiv / PubMed errors keep the work
        def boom(*a):
            raise RuntimeError("down")
        elife = dict(_fixture(), id="https://openalex.org/W7", type="preprint",
                     doi="https://doi.org/10.7554/eLife.108190.2")
        g["crossref_preprint_of"] = boom
        g["rxiv_published"] = boom
        with contextlib.redirect_stdout(io.StringIO()) as out:
            got = published_in_pubmed([published_pp, elife], budget_seconds=60)
        check("Crossref + bioRxiv failures are logged, not raised, and the works are kept",
              got == {} and out.getvalue().count("WARNING") == 2)
        g["doi_in_pubmed"] = lambda d: 4242 if d == "10.7554/elife.108190" else None
        with contextlib.redirect_stdout(io.StringIO()):
            got = published_in_pubmed([published_pp, elife], budget_seconds=60)
        check("eLife .v preprint resolves through its version of record",
              got == {"W7": ("10.7554/elife.108190", 4242)})
        g["doi_in_pubmed"] = boom
        with contextlib.redirect_stdout(io.StringIO()):
            got = published_in_pubmed([elife], budget_seconds=60)
        check("a PubMed failure on the published DOI keeps the work", got == {})
        rx = []
        g["rxiv_published"] = lambda srv, d: rx.append(d)
        with contextlib.redirect_stdout(io.StringIO()):
            published_in_pubmed([published_pp] * 3, budget_seconds=60)
        check("bioRxiv lookups cached per DOI within a run", len(rx) == 1)
        rx.clear()
        with contextlib.redirect_stdout(io.StringIO()):
            published_in_pubmed([published_pp], budget_seconds=-1)
        check("past OPENALEX_PREPRINT_LINK_SECONDS no bioRxiv call is made", rx == [])
        g["crossref_preprint_of"] = saved["crossref_preprint_of"]
        xc = []
        g["_http_json"] = lambda u, p=None: xc.append(p) or {"message": {"items": []}}
        try:
            crossref_preprint_of([f"10.1/{k}" for k in range(81)] + ["10.1/a,b"],
                                 time.monotonic() + 60)
        finally:
            g["_http_json"] = _http_json_saved
        check("Crossref batched 40 DOIs per call (81 -> 3 calls), comma DOIs skipped",
              len(xc) == 3 and all("10.1/a,b" not in c["filter"] for c in xc))

        # a failed Crossref batch keeps the links earlier batches found
        def _xc_flaky(u, p=None):
            if "doi:10.1/k0" in p["filter"]:
                return {"message": {"items": [{"DOI": "10.1/K0", "relation": {"is-preprint-of": [
                    {"id-type": "doi", "id": "https://doi.org/10.1016/PUB.K0"}]}}]}}
            raise RuntimeError("crossref down")
        g["_http_json"] = _xc_flaky
        try:
            crossref_preprint_of([f"10.1/k{k}" for k in range(41)], time.monotonic() + 60)
            partial = None
        except PartialLookupError as e:
            partial = e.partial
        finally:
            g["_http_json"] = _http_json_saved
        check("a failed Crossref batch raises PartialLookupError with batch 1's links",
              partial == {"10.1/k0": ["https://doi.org/10.1016/PUB.K0"]})
        xpp = dict(_fixture(), id="https://openalex.org/W8", type="preprint",
                   doi="https://doi.org/10.1/k0")
        g["crossref_preprint_of"] = lambda ds, dl: (_ for _ in ()).throw(
            PartialLookupError("batch 2 failed", {"10.1/k0": ["https://doi.org/10.1016/PUB.K0"]}))
        pm_seen = []
        g["doi_in_pubmed"] = lambda d: pm_seen.append(d) or (777 if d == "10.1016/pub.k0" else None)
        with contextlib.redirect_stdout(io.StringIO()) as out:
            got = published_in_pubmed([xpp], budget_seconds=60)
        check("partial Crossref links are used, and a doi.org-prefixed relation id is "
              "stripped before the PubMed search",
              got == {"W8": ("10.1016/pub.k0", 777)} and pm_seen == ["10.1016/pub.k0"]
              and out.getvalue().count("WARNING") == 1)
        g["crossref_preprint_of"] = lambda ds, dl: {}
        os.environ["OPENALEX_PREPRINT_LINK_SECONDS"] = "ten"
        try:
            with contextlib.redirect_stdout(io.StringIO()) as out:
                published_in_pubmed([], budget_seconds=None)
        finally:
            del os.environ["OPENALEX_PREPRINT_LINK_SECONDS"]
        check("a non-integer OPENALEX_PREPRINT_LINK_SECONDS falls back to 600, logged",
              "not an integer; using 600" in out.getvalue())
        g["doi_in_pubmed"] = lambda d: 4242
        with contextlib.redirect_stdout(io.StringIO()) as out:
            got = published_in_pubmed([elife], budget_seconds=-1)
        check("PubMed checks skipped at the deadline are logged, not silent",
              got == {} and "1 linked preprints kept without a PubMed check" in out.getvalue())
        g["crossref_preprint_of"] = saved["crossref_preprint_of"]
        rq = []
        g["_http_json"] = lambda u, p=None: rq.append(u) or (
            {"collection": [{"published": "10.1016/MED"}]} if "/medrxiv/" in u else {"collection": []})
        try:
            got = saved["rxiv_published"]("biorxiv", "10.1101/2021.01.01.21249999")
            got_m = saved["rxiv_published"]("medrxiv", "10.1101/2021.01.01.21249999")
        finally:
            g["_http_json"] = _http_json_saved
        check("a 10.1101 DOI bioRxiv does not know is retried on medRxiv; medRxiv is "
              "not retried", got == "10.1016/MED" and got_m == "10.1016/MED" and len(rq) == 3)
        check("_bare_doi strips doi.org / dx.doi.org / doi: prefixes",
              _bare_doi("https://doi.org/10.1/a") == "10.1/a"
              and _bare_doi("http://dx.doi.org/10.1/a") == "10.1/a"
              and _bare_doi("doi:10.1/a") == "10.1/a" and _bare_doi("  ") is None)

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
