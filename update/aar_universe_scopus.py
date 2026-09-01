#!/usr/bin/env python3
"""
Adversarial Attribution Review — Scopus lane detector (in-cluster copy).

VENDORED from the ReCiter Research producer into the reciterdb CronJob image so the
Scopus not-in-PubMed sweep runs in the cluster (weekly, gated in run_all.py) instead
of the Mac cron. Self-contained by design: it imports only `identity_index` (the
roster) and `aar_db` (the sink) — NOT the PubMed-lane modules (aar_universe /
aar_matcher / adversarial_attribution_review), so the image needs no xgboost/S3/models.
DOI->PubMed resolution is inlined below (throttled E-utilities esearch).

Finds WCM-affiliated authorships on documents that are NOT in PubMed via an
institution-wide Scopus AF-ID sweep, matches each against the identity roster, and
upserts them into reciterdb `authorship_review` with source='scopus'. The PM
Authorships tab (WP3) lets curators Accept them as ExternalArticle records (no PMID
-> not gold standard). DB-only sink: no CSV ledger for this lane.

Per-doc pipeline (probe-verified — analysis/.../2026-07-03-scopus-probe/):
  1. Sweep Scopus COMPLETE view for the family AF-IDs over an ORIG-LOAD-DATE window.
  2. Drop docs carrying a Scopus pubmed-id (the PubMed lane owns them).
  3. Resolve remaining docs against PubMed: [DOI] esearch when a DOI exists, else an
     ISSN pre-filter (Book: ISBN instead — no ISSN) / unquoted title+author esearch
     fallback for the 13.7% with no DOI (live-measured 2026-08-14, not the ~1%
     originally assumed — see resolve_no_doi). A DOI hit -> drop (reliable); a no-DOI
     (title) hit -> KEEP, flagged with the candidate matched_pmid for curator
     adjudication instead of auto-dismissed (v2.7, #951 Layer 1).
  4. Per-author WCM tagging (afid in the family set) -> match via identity_index.
  5. Upsert via aar_db (source='scopus', external_id=numeric-id|DOI, pmid=NULL).
  6. Re-check: open scopus rows now in PubMed by DOI are resolved out; a no-DOI
     (title) hit instead persists matched_pmid and stays open for adjudication
     (v2.7, #951 Layer 1).

Windowing (in-cluster default): rolling short-lag window (Decision A) — each weekly
run sweeps ORIG-LOAD-DATE over [today-90d, today-14d]. New Scopus authorships surface
within ~1-2 weeks of load; the lag lets Scopus<->PubMed links begin settling before
we write; the re-check resolves out any that later become PubMed-reachable.

Env (never printed): SCOPUS_API_KEY, SCOPUS_INST_TOKEN (inst token REQUIRED — key
alone -> 401), PUBMED_API_KEY (DOI resolution), DB_* (identity roster + sink).

Usage:
  python aar_universe_scopus.py --selftest                 # offline pure-function checks
  python aar_universe_scopus.py --mode rolling --apply     # weekly cron: [today-90d,today-14d]
  python aar_universe_scopus.py --month 2026-05            # dry-run one month (vs probe ranges)
"""
import argparse, calendar, csv, json, os, re, sys, time
from datetime import date, datetime, timedelta

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from identity_index import IdentityIndex   # source-agnostic identity roster
import aar_db                              # shared authorship_review upsert sink

SCOPUS_SEARCH = "https://api.elsevier.com/content/search/scopus"
SCOPUS_KEY = os.environ.get("SCOPUS_API_KEY")
SCOPUS_TOKEN = os.environ.get("SCOPUS_INST_TOKEN")

# family AF-ID set ships alongside this script in the image (Dockerfile COPY).
DEFAULT_AFID_LIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scopus_afids.csv")

# afid 119027669 "Weill Institute for Neurosciences" is a UCSF entity (no city, added by a
# "Weill" affil-search rather than the home list). Including it would tag UCSF authors as WCM.
EXCLUDE_AFIDS = {"119027669"}

AFID_CHUNK = 40      # >40 AF-ID() OR-clauses per query -> HTTP 400
PAGE = 25            # COMPLETE view caps count at 25/page (26+ -> 400 INVALID_INPUT)
SLEEP = 0.3          # polite Scopus throttle between pages

# ---- inline PubMed E-utilities (avoid importing the PubMed-lane aar_universe) ----
EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
_PUBMED_API_KEY = os.environ.get("PUBMED_API_KEY")
_PM_SLEEP = 0.12 if _PUBMED_API_KEY else 0.34   # 10 req/s with key, 3/s without


def _pubmed_esearch(term):
    """esearch result dict for a PubMed term (throttled, retrying), or None on
    persistent failure — the safe direction: an unresolved DOI/title stays a scopus
    row and is re-checked next run rather than being silently dropped."""
    params = {"db": "pubmed", "term": term, "retmax": 1, "retmode": "json",
              "tool": "reciterdb-aar-scopus", "email": os.environ.get("NCBI_EMAIL", "")}
    if _PUBMED_API_KEY:
        params["api_key"] = _PUBMED_API_KEY
    for attempt in range(5):
        try:
            r = requests.get(f"{EUTILS}/esearch.fcgi", params=params, timeout=60)
            if r.status_code == 200:
                return r.json().get("esearchresult")
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.5 * (attempt + 1))
                continue
            r.raise_for_status()
        except (requests.RequestException, KeyError, ValueError, TypeError):
            if attempt == 4:
                return None
            time.sleep(1.5 * (attempt + 1))
    return None


def _pubmed_count(term):
    """esearch hit-count for a PubMed term. Returns 0 on any persistent failure or
    malformed response — same safe direction as _pubmed_esearch."""
    j = _pubmed_esearch(term)
    try:
        return int(j["count"]) if j else 0
    except (KeyError, ValueError, TypeError):
        return 0


def _pubmed_pmid(term):
    """esearch matched PMID (int) for a PubMed term, or None whenever there is nothing
    safe to hand back — zero hits, or a malformed/unexpected response (any KeyError/
    ValueError/TypeError) — same failure-safe direction as _pubmed_count: an
    unresolved term stays a scopus row and is re-checked next run rather than being
    matched to something wrong."""
    j = _pubmed_esearch(term)
    try:
        if int(j["count"]) <= 0:
            return None
        idlist = j["idlist"]
        if isinstance(idlist, list) and idlist and str(idlist[0]).isdigit():
            return int(idlist[0])
        return None
    except (KeyError, ValueError, TypeError):
        return None


def doi_in_pubmed(doi):
    """The PubMed PMID this Scopus DOI resolves to via exact "<doi>"[DOI] esearch, or
    None when the DOI is missing or PubMed has no match. Truthiness is unchanged from
    the old bool return — every `if hit:` call site keeps working unmodified — but a
    caller that wants the PMID itself (recheck_open_scopus) now gets it directly
    instead of it being discarded."""
    return _pubmed_pmid(f'"{doi}"[DOI]') if doi else None


def title_in_pubmed(title, author_last=None):
    """Fallback for no-DOI Scopus docs (13.7% of the corpus, not the ~1% originally
    assumed — live-measured 2026-08-14). Deliberately UNQUOTED: NCBI's exact
    quoted-phrase [Title] search is unreliable on long, punctuation-heavy titles
    (live-verified — it reports the phrase 'not found in the phrase index' even with
    punctuation matching byte-for-byte against PubMed's own stored title). Unquoted
    `{title}[ti]` instead ANDs every title word via Automatic Term Mapping
    (live-verified against the case that motivated this: 0 hits quoted, 1 exact hit
    unquoted). Titles alone are not always unique, so AND in the author's surname
    when known — cuts collision risk for shorter or more generic titles at
    negligible cost.

    Returns the matched PMID (int), or None when the title is empty or PubMed has no
    match — same truthiness as the old bool return. This heuristic is NOT reliable
    enough to auto-resolve a row on its own (see run() step 3 / recheck_open_scopus):
    it only ever proposes a candidate matched_pmid for a curator to confirm or
    reject via matched_pmid_verdict."""
    title = (title or "").strip()
    if not title:
        return None
    term = f"{title}[ti]"
    if author_last:
        term += f" AND {author_last}[au]"
    return _pubmed_pmid(term)


def issn_in_pubmed(issn):
    """Does this journal have ANY MEDLINE/PubMed presence at all? Scopus gives ISSNs
    unhyphenated (e.g. '0732183X'); PubMed's [issn] field tag needs NNNN-NNNC (live-
    verified: unhyphenated silently falls through to a useless All-Fields text match,
    hyphenated correctly resolves to the journal). NOT proof a *specific* article is
    indexed — MEDLINE indexing is selective within a journal — only that the venue is
    indexable at all.

    GOTCHA caught live: an unrecognized ISSN doesn't error, it silently falls back to
    an All-Fields free-text search and can return a coincidental non-zero count — a
    fake '9999-9999[issn]' matched 6 unrelated articles via translation
    '"9999-9999"[All Fields]'. So a non-zero count alone isn't enough;
    querytranslation must actually show [Journal] or the "hit" is meaningless.
    Returns None (not False) whenever the ISSN is missing/malformed/unrecognized, so
    callers don't mistake "we can't check" for "confirmed absent".

    CONSEQUENCE, measured 2026-08-28 (issue #157) and left deliberately in place: on
    the open no-DOI backlog this returns True or None but essentially never False, so
    the pre-filter almost never short-circuits. All 21 distinct ISSNs among those rows
    came back either [Journal]-recognized with a non-zero count (14, -> True) or with
    no [Journal] translation at all (7, -> None). Only a venue PubMed recognizes as a
    journal AND holds zero articles for yields False, which is close to an empty set.
    Making "well-formed but unrecognized" mean False WOULD fire on that second group
    and is the safe direction (a False keeps the row open rather than dismissing it),
    but it trades away the guard that this docstring's GOTCHA paragraph exists for.
    That is a deliberate call for a human, not a silent change: don't "fix" this
    without re-reading the gotcha above."""
    hyphenated = _normalize_issn(issn)
    if not hyphenated:
        return None
    j = _pubmed_esearch(f"{hyphenated}[issn]")
    if not j or "[Journal]" not in (j.get("querytranslation") or ""):
        return None  # not recognized as a journal ISSN — don't trust the count
    try:
        return int(j["count"]) > 0
    except (KeyError, ValueError, TypeError):
        return None


def isbn_in_pubmed(isbns):
    """Does PubMed/NLM have a catalog record for this book, by any of its ISBNs (Scopus
    gives several — print/ebook editions)? Unlike [issn], PubMed's [ISBN] tag doesn't
    silently fall back to an All-Fields text match on an unrecognized value (live-
    verified: a garbage 10-digit ISBN correctly comes back with an empty
    querytranslation match and phrasesnotfound, not a coincidental hit) — still checked
    defensively here to match the [issn] guard's spirit, in case that holds only for
    the specific cases tested. Tries each ISBN in turn, first hit wins; returns False
    (not None) when nothing hits or none were supplied — resolve_no_doi falls through
    to the title search either way, same as an unconfirmable ISSN."""
    for isbn in isbns or []:
        isbn = (isbn or "").strip()
        if not isbn:
            continue
        j = _pubmed_esearch(f"{isbn}[ISBN]")
        if j and "[ISBN]" in (j.get("querytranslation") or ""):
            try:
                if int(j["count"]) > 0:
                    return True
            except (KeyError, ValueError, TypeError):
                pass
        time.sleep(_PM_SLEEP)
    return False


def resolve_no_doi(title, author_last, pub_type, issn, isbns=None):
    """Best-effort PubMed resolution for a DOI-less doc. Returns the matched PMID
    (int) or None — truthiness unchanged from the old bool return, so every
    `if hit:` call site keeps working unmodified.
    - Book: isbn_in_pubmed is no longer consulted here (v2.7) — it can only confirm
      presence, not hand back a PMID, and a bare True is not a valid value for the new
      matched_pmid BIGINT column. A live probe (2026-08-14, n=26) found ISBN alone used
      to resolve 3.8% of books that title missed (together 11.5%); that 3.8% is the
      accepted cost of requiring an actual PMID to persist. `isbns` is still accepted
      (both call sites pass it) but unused, for signature stability. Book now falls
      through to the title search exactly like every other pub_type: both of
      resolve_no_doi's hit-producing return statements are title_in_pubmed(...) — the
      ISSN pre-filter below is the only other branch, and it only ever short-circuits
      to None.
    - Non-Book: an ISSN-has-any-presence check is a cheap, high-confidence early exit
      when the whole journal has zero PubMed footprint — a title search would almost
      certainly miss too. Only short-circuits (to None) on a *confirmed* False;
      unknown/malformed ISSN (None) falls through same as no ISSN at all.
    - Falls through to the title+author search otherwise (MEDLINE indexing is
      selective even within an indexed journal, so journal-level presence alone
      isn't sufficient)."""
    if pub_type == "Book":
        return title_in_pubmed(title, author_last)
    if issn and issn_in_pubmed(issn) is False:
        return None
    return title_in_pubmed(title, author_last)


# ---- tiny local helpers -----------------------------------------------------
def _normalize_issn(issn):
    """Scopus gives ISSN unhyphenated (e.g. '0732183X'); PubMed's [issn] tag and the
    authorship_review.issn column both want hyphenated NNNN-NNNC. Returns None for
    anything that is neither (missing/malformed — stored as NULL rather than guessed
    at).

    IDEMPOTENT, and that is load-bearing: recheck_open_scopus() feeds the ALREADY-
    hyphenated stored column value back through resolve_no_doi -> issn_in_pubmed ->
    here. Normalizing only the 8-char form made that second pass fail the length test
    and return None, so the ISSN pre-filter silently no-op'd for EVERY stored row —
    the column shipped by #137/#138 could never once have changed a recheck decision.
    Live-verified 2026-08-28 (issue #157): issn_in_pubmed('0890-9091') returned None
    with no network call, while the raw '08909091' returned True."""
    issn = (issn or "").strip().upper()
    if len(issn) == 9 and issn[4] == "-":
        return issn
    return f"{issn[:4]}-{issn[4:]}" if len(issn) == 8 else None


def _extract_isbns(entry):
    """Scopus packs multiple ISBNs (print/ebook editions) into prism:isbn as a list
    holding ONE dict whose '$' is itself a bracketed comma-separated string —
    e.g. '[9781609136826, 9781469879918]' — not a real JSON array of separate
    values. Live-verified against a real Book doc, 2026-08-14; unpacked here
    rather than trusted as-is.

    _build_row stores the result comma-joined in authorship_review.isbn; recheck_open_
    scopus splits it back on ',' — hence no comma inside an individual ISBN."""
    out = []
    for item in _as_list(entry.get("prism:isbn")):
        s = (item.get("$", "") if isinstance(item, dict) else str(item or "")).strip()
        if s.startswith("[") and s.endswith("]"):
            s = s[1:-1]
        out.extend(p.strip() for p in s.split(",") if p.strip())
    return out


def _position_label(i, n):
    return "first" if i == 0 else "last" if i == n - 1 else "middle"


def _trunc(s, n):
    return s[:n] if isinstance(s, str) and len(s) > n else s


def _compact(cands):
    keep = ("cwid", "name", "person_type", "dept", "given_match", "affil_dept_match",
            "cohort_size", "confidence", "years_after_wcm")
    return [{k: c.get(k) for k in keep} for c in cands]


def _pub_year(entry):
    """Publication year from Scopus `prism:coverDate` ('2026-05-10'), for the matcher's
    temporal-plausibility penalty (issue #159). This IS the paper's own date on this
    lane — unlike the PubMed lane, where the stored `entrez_date` is the NCBI index
    date and the publication year comes from the record's Journal PubDate instead."""
    cover = entry.get("prism:coverDate") or ""
    return int(cover[:4]) if cover[:4].isdigit() else None


def _as_list(v):
    return v if isinstance(v, list) else [] if v is None else [v]


# ---- family AF-ID set -------------------------------------------------------
def load_family_afids(path=DEFAULT_AFID_LIST):
    fam = set()
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            a = (row.get("afid") or "").strip()
            if a and a not in EXCLUDE_AFIDS:
                fam.add(a)
    if not fam:
        raise RuntimeError(f"no afids loaded from {path}")
    return fam


# ---- Scopus search ----------------------------------------------------------
def _scopus_get(query, start):
    headers = {"X-ELS-APIKey": SCOPUS_KEY, "X-ELS-Insttoken": SCOPUS_TOKEN,
               "Accept": "application/json"}
    params = {"query": query, "view": "COMPLETE", "count": PAGE, "start": start}
    for attempt in range(5):
        try:
            r = requests.get(SCOPUS_SEARCH, headers=headers, params=params, timeout=60)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(2 * (attempt + 1))
                continue
            r.raise_for_status()
        except requests.RequestException:
            if attempt == 4:
                raise
            time.sleep(2 * (attempt + 1))
    raise RuntimeError("Scopus search failed after retries")


def _chunks(seq, n):
    seq = list(seq)
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def build_query(afids, aft, bef):
    clause = " OR ".join(f"AF-ID({a})" for a in afids)
    return f"({clause}) AND ORIG-LOAD-DATE AFT {aft} AND ORIG-LOAD-DATE BEF {bef}"


def sweep(family, aft, bef, progress=True):
    """All family docs in the ORIG-LOAD-DATE window, deduped by EID across AF-ID chunks."""
    by_eid = {}
    for ci, chunk in enumerate(_chunks(sorted(family), AFID_CHUNK), 1):
        query = build_query(chunk, aft, bef)
        start = 0
        while True:
            res = _scopus_get(query, start).get("search-results", {})
            entries = res.get("entry", []) or []
            if len(entries) == 1 and "error" in entries[0]:
                break                                    # empty result set
            for e in entries:
                if e.get("eid"):
                    by_eid[e["eid"]] = e
            total = int(res.get("opensearch:totalResults", 0) or 0)
            start += PAGE
            if start >= total or not entries:
                break
            time.sleep(SLEEP)
        if progress:
            print(f"  chunk {ci}: {len(chunk)} afids -> {len(by_eid)} docs so far", flush=True)
    return list(by_eid.values())


# ---- parsing ----------------------------------------------------------------
def _numeric_scopus_id(eid):
    """'2-s2.0-105037523511' -> '105037523511' (EID minus the '2-s2.0-' prefix)."""
    return eid.split("-s2.0-")[-1] if eid else None


def _container_doi(doi, pub_type):
    """Book-chapter DOIs look like '10.1201/9781003735878-24' -> base '...9781003735878'."""
    if not doi or not pub_type or "chapter" not in pub_type.lower():
        return None
    m = re.match(r"^(.*)-\d+$", doi)
    return m.group(1) if m else None


def _affil_names(entry):
    """afid -> 'AffilName, City' from the doc-level affiliation array (COMPLETE view)."""
    out = {}
    for af in _as_list(entry.get("affiliation")):
        aid = af.get("afid")
        if aid:
            out[aid] = ", ".join(x for x in (af.get("affilname"),
                                             af.get("affiliation-city")) if x)
    return out


def wcm_authorships(entry, family):
    """Yield (i, n, author-for-matcher, family_hits) for authors whose afid is in `family`."""
    authors = _as_list(entry.get("author"))
    affil = _affil_names(entry)
    n = len(authors)
    for i, au in enumerate(authors):
        afids = [d.get("$") for d in _as_list(au.get("afid")) if d.get("$")]
        hits = [a for a in afids if a in family]
        if not hits:
            continue
        yield i, n, {
            "last": au.get("surname"), "fore": au.get("given-name"),
            "initials": au.get("initials"),
            "affiliations": [affil[a] for a in afids if a in affil],
        }, hits


def _authors_json(entry):
    """Simplified {given,surname} list of EVERY author on the document (not just the
    WCM-matched one) — the Scopus Search API COMPLETE view already returns this full
    array (entry.get("author")); _build_row() previously discarded everyone but the
    one WCM author, leaving curators no way to see the paper's byline the way a PubMed
    row's PMID link shows it. Scopus-internal fields (@seq/afid) are dropped — this is
    a curator-facing display list, not a re-parseable structure. Always a JSON array
    string, never null, even when the document has no author field."""
    return json.dumps([
        {"given": a.get("given-name"), "surname": a.get("surname")}
        for a in _as_list(entry.get("author"))
    ])


def _build_row(entry, i, n, author, top, cands, run_ts, dup_map=None):
    doi = entry.get("prism:doi")
    sid = _numeric_scopus_id(entry.get("eid"))
    dedup = doi or sid          # author_key identity: DOI-first, stable across Scopus record merges
    pub_type = entry.get("subtypeDescription")
    cohort = top.get("cohort_size") if top else None
    # dup_flag/dup_reason: heads-up "already added as ExternalArticle" signal, joined
    # by DOI against reciterdb.external_article (aar_db.dup_flags_by_doi, batched once
    # per run() call, not per row — see that function's docstring) and then narrowed to
    # THIS authorship's own candidates, so a co-author's already-added authorship never
    # flags this one (issue #158). Live 409 at Accept/Assign time (Publication Manager)
    # remains the final same-day-race check.
    dup_uid = aar_db.dup_uid_for_authorship(dup_map, doi, top, cands)
    return {
        "source": "scopus", "pmid": None,
        # numeric Scopus record id -> the PM Accept builds articleId "SCOPUS:<external_id>"
        # and the "Scopus record" link. The DOI lives in `doi`; author_key stays DOI-first.
        "external_id": _trunc(sid or doi, 96),
        "author_key": _trunc(f"scopus:{dedup}:{i}", 160),
        "pub_type": _trunc(pub_type, 40),
        "container_id": _container_doi(doi, pub_type),
        "author_position": i + 1, "author_position_label": _position_label(i, n),
        "wcm_author": _trunc(" ".join(x for x in (author.get("fore"), author.get("last")) if x)
                             or author.get("initials") or author.get("last"), 255),
        "author_affiliation": " | ".join(author.get("affiliations") or []),
        "entrez_date": entry.get("prism:coverDate"),   # Scopus has no entrez date; coverDate ~ recency
        "title": entry.get("dc:title"),
        "journal": _trunc(entry.get("prism:publicationName"), 512),
        "issn": _normalize_issn(entry.get("prism:issn") or entry.get("prism:eIssn")),
        # comma-joined (books carry several — print/ebook editions); NULL when absent.
        # Book-like types are ISBN-keyed and carry no ISSN at all, so this is the only
        # venue identifier they ever have — see recheck_open_scopus.
        "isbn": _trunc(",".join(_extract_isbns(entry)), 128) or None,
        "doi": _trunc(doi, 255),
        "authors_json": _authors_json(entry),
        "classification": "absent",                    # no PMID -> production never scored it
        "top_cwid": top["cwid"] if top else None,
        "top_name": _trunc(top["name"], 255) if top else None,
        "top_person_type": _trunc(top["person_type"], 64) if top else None,
        "top_dept": _trunc(top["dept"], 255) if top else None,
        "top_fg_score": None, "top_io_score": None,    # scopus rail shows neither
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
        # v2.7 (#951 Layer 1): a title-heuristic hit at ingest (run() step 3) stashes
        # the candidate PMID on the doc as "_matched_pmid" instead of dropping it;
        # source is always 'title' here -- a DOI hit never reaches _build_row at all
        # (dropped before a row is built), and 'scopus' is reserved for a future
        # producer-native Scopus pubmed-id write.
        "matched_pmid": entry.get("_matched_pmid"),
        "matched_pmid_source": "title" if entry.get("_matched_pmid") else None,
        "matched_pmid_at": run_ts if entry.get("_matched_pmid") else None,
        "status": "open", "first_seen": run_ts,
        "last_checked": run_ts, "last_refreshed": run_ts,
    }


# ---- re-check open scopus rows ----------------------------------------------
def recheck_open_scopus(run_ts):
    """Open scopus rows now PubMed-reachable are handled two different ways, by DOI
    when present, else delegated to resolve_no_doi (ISSN dead-journal pre-filter /
    title fallback — same logic ingest uses). A DOI hit is a reliable, unambiguous
    match: dismissed exactly as before this migration, plus matched_pmid/
    matched_pmid_source='doi' now persisted alongside the note. A no-DOI (title) hit is
    NOT dismissed — the title heuristic is not reliable enough to auto-resolve a row on
    its own (the whole point of #951 Layer 1: persist the candidate PMID and let a
    curator adjudicate matched_pmid_verdict, instead of the row silently vanishing) —
    only matched_pmid/matched_pmid_source='title'/matched_pmid_at are set; status and
    note are left untouched. The PubMed lane will surface a DOI-hit row's canonical
    version if it is still orphaned there. Guarded on status='open' so a curator's
    accept/reject is never clobbered. (No dedicated 'resolved' ENUM value; dismissed +
    an auto note is the removal-from-queue state for the DOI path — the title path
    instead stays open with matched_pmid set, awaiting a verdict.)

    `matched_pmid IS NULL` in the SELECT excludes two kinds of row on purpose: one
    already flagged by a prior recheck and awaiting a curator's verdict (re-running the
    title heuristic on it would waste an esearch call and could overwrite a value
    mid-review), and one a curator has already set matched_pmid_verdict='distinct' on
    (a verdict only ever sits on a non-NULL matched_pmid) — the producer must never
    re-flag either kind.

    `issn`/`isbn` reach the backlog two ways: rows upserted after each column shipped
    already have it; older rows stay NULL until their next producer refresh re-upserts
    them. The recurring cron's INGEST sweep (sweep()/run(), NOT this function) only
    covers a recent ORIG-LOAD-DATE window, so rows from the 2026-07-05 --mode initial
    five-year backfill are never revisited by it — they need
    update/targeted_authors_backfill.py, which looks each document up by its stored
    doi/external_id and fills the columns in place (see issue #157). This function has
    no such gap: its own SELECT is source='scopus' AND status='open' (now also
    matched_pmid IS NULL) with no date filter, so it sweeps every open row regardless
    of ingest vintage.

    ISBN is passed for every pub_type, exactly as at ingest; resolve_no_doi no longer
    consults it at all (v2.7 — see that function's docstring)."""
    from sqlalchemy import text
    eng = aar_db.engine()
    with eng.connect() as c:
        rows = c.execute(text(
            "SELECT author_key, doi, title, wcm_author, pub_type, issn, isbn FROM authorship_review "
            "WHERE source='scopus' AND status='open' AND matched_pmid IS NULL")).mappings().all()
    dismissed, flagged = 0, 0
    for r in rows:
        if r["doi"]:
            hit = doi_in_pubmed(r["doi"])
            if hit:
                with eng.begin() as c:
                    c.execute(text(
                        "UPDATE authorship_review SET status='dismissed', resolved_at=:ts, "
                        "matched_pmid=:p, matched_pmid_source='doi', matched_pmid_at=:ts, "
                        "note=CONCAT('auto: now in PubMed (', :d, ') PMID ', :p) "
                        "WHERE author_key=:k AND status='open'"),
                        {"ts": run_ts, "d": r["doi"], "p": hit, "k": r["author_key"]})
                dismissed += 1
        else:
            author_last = (r["wcm_author"] or "").split()[-1] if r["wcm_author"] else None
            hit = resolve_no_doi(r["title"], author_last, r["pub_type"], r["issn"],
                                 (r["isbn"] or "").split(","))
            if hit:
                with eng.begin() as c:
                    c.execute(text(
                        "UPDATE authorship_review SET matched_pmid=:p, "
                        "matched_pmid_source='title', matched_pmid_at=:ts "
                        "WHERE author_key=:k AND status='open' AND matched_pmid IS NULL"),
                        {"ts": run_ts, "p": hit, "k": r["author_key"]})
                flagged += 1
        time.sleep(_PM_SLEEP)
    return dismissed, flagged


# ---- driver -----------------------------------------------------------------
def run(aft, bef, apply_writes=False, afid_list=DEFAULT_AFID_LIST, recheck=True, idx=None):
    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    family = load_family_afids(afid_list)

    print(f"[1/5] Scopus sweep: {len(family)} family afids, ORIG-LOAD-DATE {aft}..{bef}",
          flush=True)
    docs = sweep(family, aft, bef)
    print(f"      {len(docs)} family docs", flush=True)

    no_pmid = [d for d in docs if not str(d.get("pubmed-id") or "").strip()]
    print(f"[2/5] {len(docs) - len(no_pmid)} carry a Scopus PMID (dropped); "
          f"{len(no_pmid)} no-PMID", flush=True)

    print("[3/5] Resolving no-PMID docs against PubMed (DOI, else title) ...", flush=True)
    scopus_only, resolved, flagged_by_title = [], 0, 0
    for d in no_pmid:
        doi = d.get("prism:doi")
        if doi:
            hit = doi_in_pubmed(doi)
            time.sleep(_PM_SLEEP)
            if hit:
                resolved += 1
                continue
        else:
            authors = _as_list(d.get("author"))
            author_last = authors[0].get("surname") if authors else None
            issn = d.get("prism:issn") or d.get("prism:eIssn")
            isbns = _extract_isbns(d)
            hit = resolve_no_doi(d.get("dc:title"), author_last, d.get("subtypeDescription"), issn, isbns)
            time.sleep(_PM_SLEEP)
            if hit:
                # v2.7 (#951 Layer 1): a title-heuristic hit is too unreliable to
                # auto-drop the doc -- KEEP it and stash the candidate PMID so
                # _build_row can persist matched_pmid for curator adjudication,
                # instead of the row silently vanishing (unchanged: a DOI hit above
                # is reliable enough to still drop outright).
                flagged_by_title += 1
                d["_matched_pmid"] = hit
        scopus_only.append(d)
    print(f"      {resolved} resolved to PubMed by DOI (dropped); {flagged_by_title} title-matched "
          f"(kept, flagged with a candidate PMID for adjudication); {len(scopus_only)} SCOPUS-ONLY",
          flush=True)

    print("[4/5] Matching WCM authorships against the identity roster ...", flush=True)
    if idx is None:
        idx = IdentityIndex.load()
    # one batched dup-check join over every DOI in this month/window, not a query per
    # row (aar_db.dup_flags_by_doi docstring) — mirrors the "load the roster once"
    # convention already used for idx above.
    dois = {d.get("prism:doi") for d in scopus_only if d.get("prism:doi")}
    dup_map = aar_db.dup_flags_by_doi(dois) if dois else {}
    rows, unmatched = [], 0
    for d in scopus_only:
        for i, n, au, _ in wcm_authorships(d, family):
            cands, _ = idx.candidates(au["last"], au["fore"], au["initials"],
                                      au["affiliations"], top_k=5,
                                      pub_year=_pub_year(d))
            top = cands[0] if cands else None
            if top is None:
                unmatched += 1                         # nothing to assign (v1 skips unmatched)
                continue
            rows.append(_build_row(d, i, n, au, top, cands, run_ts, dup_map=dup_map))
    print(f"      {len(rows)} matched authorships; {unmatched} unmatched (skipped)", flush=True)

    if apply_writes:
        n = aar_db.upsert(rows)
        print(f"[5/5] upserted {n} scopus authorships -> reciterdb.authorship_review", flush=True)
        if recheck:
            n_dismissed, n_flagged = recheck_open_scopus(run_ts)
            print(f"      re-check: {n_dismissed} open scopus rows resolved out (DOI match), "
                  f"{n_flagged} flagged with a candidate PMID for adjudication (title match)",
                  flush=True)
    else:
        print(f"[5/5] DRY-RUN: {len(rows)} rows NOT written (pass --apply to upsert)", flush=True)

    return {"window": {"aft": aft, "bef": bef}, "family_docs": len(docs),
            "no_pmid": len(no_pmid), "doi_resolved_pubmed": resolved,
            "flagged_by_title": flagged_by_title,
            "scopus_only": len(scopus_only), "matched_rows": len(rows),
            "unmatched": unmatched}


# ---- windows ----------------------------------------------------------------
def rolling_window(today=None, lag_days=14, span_days=90):
    """Decision A (in-cluster default): sweep ORIG-LOAD-DATE over [today-span_days,
    today-lag_days] (~76-day rolling window, lagged so Scopus<->PubMed links begin
    settling). Weekly cadence; overlapping windows are idempotent via the author_key
    upsert, so a re-run just refreshes the same rows."""
    today = today or date.today()
    aft = (today - timedelta(days=span_days)).strftime("%Y%m%d")
    bef = (today - timedelta(days=lag_days)).strftime("%Y%m%d")
    return aft, bef


def month_window(year, month):
    """(AFT, BEF) YYYYMMDD exclusive bounds spanning the given calendar month."""
    first = date(year, month, 1)
    last = date(year, month, calendar.monthrange(year, month)[1])
    return ((first - timedelta(days=1)).strftime("%Y%m%d"),
            (last + timedelta(days=1)).strftime("%Y%m%d"))


def recurring_month(today=None):
    """One-window-lag: a run in month M sweeps loads of month M-2."""
    today = today or date.today()
    y, m = today.year, today.month - 2
    while m <= 0:
        m += 12
        y -= 1
    return y, m


def month_range(start, end):
    """Inclusive list of (year, month) tuples from start=(y,m) to end=(y,m)."""
    (sy, sm), (ey, em) = start, end
    out, y, m = [], sy, sm
    while (y, m) <= (ey, em):
        out.append((y, m))
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def initial_months(years=5, today=None):
    """Backfill window: monthly slices spanning `years` back to the M-2 lag boundary."""
    ey, em = recurring_month(today)          # newest month we sweep (one-window-lag)
    return month_range((ey - years, em), (ey, em))


def run_backfill(months, apply_writes=False, afid_list=DEFAULT_AFID_LIST):
    """Backfill driver: sweep each calendar month once, sharing one identity index (the
    35k-row roster loads once, not per month). The open-row re-check runs ONCE at the end
    (only when applying). Idempotent per month via the author_key upsert — a run that dies
    partway resumes with `--from <next-month> --to <end-month>`."""
    idx = IdentityIndex.load()
    print(f"Backfill: {len(months)} months, {months[0][0]}-{months[0][1]:02d} .. "
          f"{months[-1][0]}-{months[-1][1]:02d}"
          + ("  [APPLY]" if apply_writes else "  [DRY-RUN]"), flush=True)
    agg = {"months": len(months), "family_docs": 0, "scopus_only": 0,
           "matched_rows": 0, "unmatched": 0, "per_month": []}
    for k, (y, m) in enumerate(months):
        aft, bef = month_window(y, m)
        print(f"\n########## {y}-{m:02d}  ({k + 1}/{len(months)}) ##########", flush=True)
        s = run(aft, bef, apply_writes=apply_writes, afid_list=afid_list,
                recheck=apply_writes and k == len(months) - 1, idx=idx)
        for key in ("family_docs", "scopus_only", "matched_rows", "unmatched"):
            agg[key] += s[key]
        agg["per_month"].append({"month": f"{y}-{m:02d}", "scopus_only": s["scopus_only"],
                                 "matched_rows": s["matched_rows"]})
    return agg


# ---- self-test (offline: no network / DB) -----------------------------------
def _selftest():
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  [{'OK' if cond else '** FAIL'}] {label}")

    check("numeric scopus id strips '2-s2.0-' prefix",
          _numeric_scopus_id("2-s2.0-105037523511") == "105037523511")
    check("container doi derived from book chapter",
          _container_doi("10.1201/9781003735878-24", "Book Chapter") == "10.1201/9781003735878")
    check("container doi is None for an article",
          _container_doi("10.1007/s11940-026-00868-8", "Article") is None)
    check("119027669 (UCSF) excluded from family set",
          "119027669" not in load_family_afids())
    check("isbn_in_pubmed makes no network call for an empty/missing isbn list",
          isbn_in_pubmed([]) is False and isbn_in_pubmed(None) is False)
    check("_extract_isbns unpacks Scopus's bracketed-string-in-a-dict-in-a-list form",
          _extract_isbns({"prism:isbn": [{"$": "[9781609136826, 9781469879918]"}]})
          == ["9781609136826", "9781469879918"])
    check("_extract_isbns returns [] when the doc has no isbn field",
          _extract_isbns({}) == [])
    check("issn_in_pubmed rejects malformed length without a network call",
          issn_in_pubmed("bad") is None)
    check("_normalize_issn hyphenates Scopus's unhyphenated form",
          _normalize_issn("0732183X") == "0732-183X" and _normalize_issn("bad") is None)
    # regression guard for #157: recheck_open_scopus feeds the STORED (already
    # hyphenated) value back in. Re-normalizing used to return None, silently
    # disabling the ISSN pre-filter for every row that had the column populated.
    check("_normalize_issn is idempotent on the stored hyphenated form",
          _normalize_issn("0890-9091") == "0890-9091"
          and _normalize_issn(_normalize_issn("0732183X")) == "0732-183X"
          and _normalize_issn("08909-091") is None)
    check("rolling window spans [today-90d, today-14d]",
          rolling_window(date(2026, 7, 4)) == ("20260405", "20260620"))
    check("rolling window honours custom lag/span",
          rolling_window(date(2026, 7, 4), lag_days=7, span_days=30) == ("20260604", "20260627"))
    check("recurring window is month M-2",
          recurring_month(date(2026, 7, 3)) == (2026, 5)
          and recurring_month(date(2026, 1, 15)) == (2025, 11))
    check("month_window brackets the calendar month exclusively",
          month_window(2026, 5) == ("20260430", "20260601"))
    check("month_range wraps the year boundary",
          month_range((2025, 11), (2026, 2)) == [(2025, 11), (2025, 12), (2026, 1), (2026, 2)])
    check("initial 5y backfill = 61 months, 2021-05 .. 2026-05 (M-2 boundary)",
          initial_months(5, date(2026, 7, 3))[0] == (2021, 5)
          and initial_months(5, date(2026, 7, 3))[-1] == (2026, 5)
          and len(initial_months(5, date(2026, 7, 3))) == 61)

    family = {"60007997"}
    entry = {
        "eid": "2-s2.0-105037523511", "prism:doi": "10.1/x", "subtypeDescription": "Article",
        "dc:title": "T", "prism:publicationName": "J", "prism:coverDate": "2026-05-10",
        "affiliation": [{"afid": "60007997", "affilname": "Weill Cornell Medicine",
                         "affiliation-city": "New York"},
                        {"afid": "99999999", "affilname": "Other U"}],
        "author": [
            {"@seq": "1", "surname": "Smith", "given-name": "Jane", "initials": "J.",
             "afid": [{"$": "60007997"}]},
            {"@seq": "2", "surname": "Doe", "given-name": "John", "afid": [{"$": "99999999"}]},
        ],
    }
    wcm = list(wcm_authorships(entry, family))
    check("exactly one WCM authorship tagged", len(wcm) == 1)
    i, n, au, _ = wcm[0]
    check("WCM author is position 0 (Smith)", i == 0 and au["last"] == "Smith")
    check("WCM affiliation resolved to family affilname",
          any("Weill Cornell" in a for a in au["affiliations"]))

    top = {"cwid": "js1", "name": "Jane Smith", "person_type": "Faculty", "dept": "Peds",
           "confidence": 0.9, "cohort_size": 1, "given_match": "full", "affil_dept_match": False}
    row = _build_row(entry, i, n, au, top, [top], "2026-07-03 00:00:00")
    check("row source=scopus, pmid None", row["source"] == "scopus" and row["pmid"] is None)
    check("row external_id = numeric Scopus id (for Accept), doi kept separately",
          row["external_id"] == "105037523511" and row["doi"] == "10.1/x")
    check("row author_key = scopus:{doi}:0 (DOI-first dedup)", row["author_key"] == "scopus:10.1/x:0")
    check("row classification=absent, no FG/IO",
          row["classification"] == "absent"
          and row["top_fg_score"] is None and row["top_io_score"] is None)
    check("row single_candidate=1", row["single_candidate"] == 1)
    check("row authors_json lists both authors on the document, not just the WCM one",
          json.loads(row["authors_json"]) == [
              {"given": "Jane", "surname": "Smith"}, {"given": "John", "surname": "Doe"}])
    check("row dup_flag=0/dup_reason=None with no dup_map (default)",
          row["dup_flag"] == 0 and row["dup_reason"] is None)
    check("row isbn is None when the document carries no prism:isbn (an Article)",
          row["isbn"] is None)

    # a real Book entry: Scopus's bracketed-string-in-a-dict-in-a-list, stored
    # comma-joined so recheck_open_scopus can split it straight back into a list
    book = dict(entry, **{"subtypeDescription": "Book",
                          "prism:isbn": [{"$": "[9781609136826, 9781469879918]"}]})
    row_book = _build_row(book, i, n, au, top, [top], "2026-07-03 00:00:00")
    check("row isbn stores the Scopus ISBN list comma-joined",
          row_book["isbn"] == "9781609136826,9781469879918")
    check("stored isbn splits back into exactly what _extract_isbns produced",
          row_book["isbn"].split(",") == _extract_isbns(book))

    # issue #158: dup_flag is per-AUTHORSHIP. A DOI already added for someone who is
    # not a candidate for this authorship (a co-author on the same paper) is not a
    # duplicate of this row.
    row_other = _build_row(entry, i, n, au, top, [top], "2026-07-03 00:00:00",
                           dup_map={"10.1/x": {"him4004"}})
    check("row dup_flag=0 when the DOI was added for a NON-candidate (co-author)",
          row_other["dup_flag"] == 0 and row_other["dup_reason"] is None)

    row_dup = _build_row(entry, i, n, au, top, [top], "2026-07-03 00:00:00",
                         dup_map={"10.1/x": {"js1"}})
    check("row dup_flag=1 and dup_reason names the candidate the DOI was added for",
          row_dup["dup_flag"] == 1 and row_dup["dup_reason"]
          == "Already added as ExternalArticle for js1 (DOI match)")

    alt = dict(top, cwid="jd2", name="Jane Doe")
    row_alt = _build_row(entry, i, n, au, top, [top, alt], "2026-07-03 00:00:00",
                         dup_map={"10.1/x": {"him4004", "jd2"}})
    check("row dup_flag=1 off an alternate candidate, and dup_reason names that "
          "candidate rather than the unrelated co-author",
          row_alt["dup_flag"] == 1 and row_alt["dup_reason"]
          == "Already added as ExternalArticle for jd2 (DOI match)")

    row_both = _build_row(entry, i, n, au, top, [top, alt], "2026-07-03 00:00:00",
                          dup_map={"10.1/x": {"js1", "jd2"}})
    check("dup_reason names the top candidate when several candidates match",
          row_both["dup_reason"] == "Already added as ExternalArticle for js1 (DOI match)")

    check("_authors_json returns '[]' (not null) when the document has no author field",
          _authors_json({}) == "[]")

    check("_pub_year reads the year off prism:coverDate, None when absent/malformed",
          _pub_year(entry) == 2026 and _pub_year({}) is None
          and _pub_year({"prism:coverDate": "n/a"}) is None)

    entry2 = dict(entry, **{"prism:doi": None})
    au2 = {"last": "Smith", "fore": "Jane", "initials": "J.", "affiliations": []}
    row2 = _build_row(entry2, 0, 1, au2, top, [top], "2026-07-03 00:00:00")
    check("no-DOI row falls back to numeric scopus id",
          row2["external_id"] == "105037523511"
          and row2["author_key"] == "scopus:105037523511:0")

    # ---- v2.7 (#951 Layer 1): matched-PMID resolution, offline via a monkeypatched
    # _pubmed_esearch (canned esearchresult dicts keyed by exact term). Restored in
    # `finally` no matter what fails, so a later selftest run is never left patched.
    global _pubmed_esearch
    _real_esearch = _pubmed_esearch
    canned = {}
    _pubmed_esearch = lambda term: canned.get(term)
    try:
        canned['"10.1/hit"[DOI]'] = {"count": "1", "idlist": ["12345678"]}
        check("doi_in_pubmed returns the int PMID on a hit",
              doi_in_pubmed("10.1/hit") == 12345678)
        canned['"10.1/miss"[DOI]'] = {"count": "0", "idlist": []}
        check("doi_in_pubmed returns None when count is 0",
              doi_in_pubmed("10.1/miss") is None)
        check("doi_in_pubmed returns None for a missing DOI (no lookup)",
              doi_in_pubmed(None) is None and doi_in_pubmed("") is None)

        canned["empty idlist"] = {"count": "3", "idlist": []}
        check("_pubmed_pmid returns None when count > 0 but idlist is empty",
              _pubmed_pmid("empty idlist") is None)
        canned["malformed idlist"] = {"count": "2", "idlist": ["not-a-pmid"]}
        check("_pubmed_pmid returns None when count > 0 but idlist is malformed",
              _pubmed_pmid("malformed idlist") is None)
        canned["clean hit"] = {"count": "1", "idlist": ["999"]}
        check("_pubmed_pmid returns the int PMID on a clean hit",
              _pubmed_pmid("clean hit") == 999)

        canned["T[ti] AND Smith[au]"] = {"count": "1", "idlist": ["42"]}
        check("title_in_pubmed returns the int PMID on a hit",
              title_in_pubmed("T", "Smith") == 42)
        check("title_in_pubmed returns None for an empty title (no lookup)",
              title_in_pubmed("") is None and title_in_pubmed(None) is None)
    finally:
        _pubmed_esearch = _real_esearch

    entry_matched = dict(entry, _matched_pmid=555)
    row_matched = _build_row(entry_matched, i, n, au, top, [top], "2026-07-03 00:00:00")
    check("row emits matched_pmid/matched_pmid_source/matched_pmid_at when the entry "
          "carries a title-heuristic match",
          row_matched["matched_pmid"] == 555
          and row_matched["matched_pmid_source"] == "title"
          and row_matched["matched_pmid_at"] == "2026-07-03 00:00:00")
    check("row emits None for all three matched_* keys with no title-heuristic match",
          row["matched_pmid"] is None and row["matched_pmid_source"] is None
          and row["matched_pmid_at"] is None)

    check("aar_db._INSERT_COLS carries the matched_pmid trio for first-insert only",
          all(c in aar_db._INSERT_COLS for c in
              ("matched_pmid", "matched_pmid_source", "matched_pmid_at")))
    check("aar_db._REFRESH_COLS does NOT carry the matched_pmid trio "
          "(a re-sweep must never clobber them)",
          not any(c in aar_db._REFRESH_COLS for c in
                  ("matched_pmid", "matched_pmid_source", "matched_pmid_at")))
    check("matched_pmid_verdict is curator-owned: in NEITHER column list",
          "matched_pmid_verdict" not in aar_db._INSERT_COLS
          and "matched_pmid_verdict" not in aar_db._REFRESH_COLS)

    print("\nSELFTEST", "PASS" if ok else "FAIL")
    return ok


def main():
    ap = argparse.ArgumentParser(description="Scopus not-in-PubMed WCM authorship detector")
    ap.add_argument("--mode", choices=["rolling", "recurring", "initial"],
                    help="rolling = [today-90d,today-14d] (weekly cron, Decision A); "
                         "recurring = month M-2; initial = backfill --years back")
    ap.add_argument("--lag-days", type=int, default=14, help="rolling mode: settle-lag before window end")
    ap.add_argument("--span-days", type=int, default=90, help="rolling mode: window width in days")
    ap.add_argument("--month", help="YYYY-MM single calendar month (testing / spot backfill)")
    ap.add_argument("--from", dest="from_month", help="YYYY-MM backfill range start (with --to)")
    ap.add_argument("--to", dest="to_month", help="YYYY-MM backfill range end (with --from)")
    ap.add_argument("--years", type=int, default=5, help="--mode initial backfill span (default 5)")
    ap.add_argument("--afid-list", default=DEFAULT_AFID_LIST)
    ap.add_argument("--apply", action="store_true", help="write rows (default: dry-run)")
    ap.add_argument("--no-recheck", action="store_true",
                    help="skip resolving open scopus rows out (only meaningful with --apply)")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()

    if args.selftest:
        sys.exit(0 if _selftest() else 1)
    if not (SCOPUS_KEY and SCOPUS_TOKEN):
        ap.error("SCOPUS_API_KEY and SCOPUS_INST_TOKEN must be set (never printed)")

    def one_month(y, m):
        aft, bef = month_window(y, m)
        print(f"Scopus lane: loads of {y}-{m:02d}  (ORIG-LOAD-DATE {aft}..{bef})"
              + ("  [APPLY]" if args.apply else "  [DRY-RUN]"), flush=True)
        return run(aft, bef, apply_writes=args.apply, afid_list=args.afid_list,
                   recheck=not args.no_recheck)

    if args.mode == "rolling":
        aft, bef = rolling_window(lag_days=args.lag_days, span_days=args.span_days)
        print(f"Scopus lane: rolling ORIG-LOAD-DATE {aft}..{bef}"
              + ("  [APPLY]" if args.apply else "  [DRY-RUN]"), flush=True)
        summary = run(aft, bef, apply_writes=args.apply, afid_list=args.afid_list,
                      recheck=not args.no_recheck)
    elif args.mode == "recurring":
        summary = one_month(*recurring_month())
    elif args.mode == "initial":
        summary = run_backfill(initial_months(args.years), apply_writes=args.apply,
                               afid_list=args.afid_list)
    elif args.from_month and args.to_month:
        f = tuple(int(x) for x in args.from_month.split("-"))
        t = tuple(int(x) for x in args.to_month.split("-"))
        summary = run_backfill(month_range(f, t), apply_writes=args.apply, afid_list=args.afid_list)
    elif args.month:
        summary = one_month(*(int(x) for x in args.month.split("-")))
    else:
        ap.error("provide --mode rolling|recurring|initial, --month YYYY-MM, "
                 "--from/--to YYYY-MM, or --selftest")

    print("\n==== SCOPUS RUN SUMMARY ====")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
