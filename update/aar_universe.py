#!/usr/bin/env python3
"""
Adversarial Attribution Review — Step 1: WCM-affiliation universe pull.

Produces the set of WCM-affiliated articles in a window, the raw material the
attribution gate + identity matcher consume downstream. Runs standalone in cron
via NCBI E-utilities (not the interactive MCP tool).

What it does:
  1. Loads ReCiter's canonical home-institution keywords from application.properties
     and builds the same affiliation OR-query AffiliationRetrievalStrategy uses.
  2. ESearch (db=pubmed, datetype=edat, usehistory=y) over the entrez-date window.
  3. EFetch the records and parse: pmid, entrez date (PubStatus="entrez" history date,
     identical to ArticleTranslator.java:329-351), title, journal, doi, authors+affils,
     and which authors carry a home-institution affiliation.
  4. Filters to the precise parsed-entrez-date window and caches universe.json.

Windows (entrez date):
  initial   = [today-2y, today-40d]      (one-time backlog clear)
  recurring = [today-71d, today-40d]     (rolling 31-day slice, monthly cadence)

Env: PUBMED_API_KEY (read from environment, never logged), NCBI_EMAIL (optional).

Usage:
  python aar_universe.py --mode recurring
  python aar_universe.py --mode initial
  python aar_universe.py --from 2026/05/05 --to 2026/05/20 --out /tmp/u.json   # custom/test
"""
import argparse, json, os, sys, time, xml.etree.ElementTree as ET
from datetime import date, timedelta

import requests

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
APP_PROPS = os.path.expanduser(
    "~/Dropbox/GitHub/ReCiter/src/main/resources/application.properties")
KEYWORDS_KEY = "strategy.authorAffiliationScoringStrategy.homeInstitution-keywords"
# In-cluster there is no application.properties on disk. Baked snapshot of ReCiter's
# canonical home-institution keywords (from application.properties, 2026-07-05); override
# with env HOME_INSTITUTION_KEYWORDS. Keep in sync if the ReCiter property changes.
DEFAULT_HOME_INSTITUTION_KEYWORDS = (
    "weil|cornell, weill|cornell, weill|medicine, cornell|medicine, cornell|medical, "
    "weill|medical, weill|bugando, weill|graduate, cornell|presbyterian, "
    "weill|presbyterian, 10065|cornell, 10065|presbyterian, 10021|cornell, "
    "10021|presbyterian, weill|qatar, cornell|qatar, @med.cornell.edu, "
    "@qatar-med.cornell.edu, tri-institutional|md-phd, memorial|sloan|kettering, "
    "rockefeller|university, hospital|special|surgery"
)
TOOL = "reciter-aar"
API_KEY = os.environ.get("PUBMED_API_KEY")
EMAIL = os.environ.get("NCBI_EMAIL", "palbert1@gmail.com")
EFETCH_BATCH = 200
# polite throttle: 10 req/s with key, 3/s without
SLEEP = 0.12 if API_KEY else 0.34


# ---- affiliation query (mirrors AffiliationRetrievalStrategy) --------------
def load_home_institution_groups(props_path=APP_PROPS):
    """Return list of token-groups; '|' = AND within a group, ',' = OR between.

    Source order: env HOME_INSTITUTION_KEYWORDS -> application.properties file (Mac)
    -> baked DEFAULT (in-cluster)."""
    value = os.environ.get("HOME_INSTITUTION_KEYWORDS")
    if value is None and os.path.exists(props_path):
        with open(props_path) as fh:
            for line in fh:
                if line.startswith(KEYWORDS_KEY + "="):
                    value = line.split("=", 1)[1].strip()
                    break
    if value is None:
        value = DEFAULT_HOME_INSTITUTION_KEYWORDS
    groups = []
    for grp in value.split(","):
        tokens = [t.strip().lower() for t in grp.strip().split("|") if t.strip()]
        if tokens:
            groups.append(tokens)
    return groups


def build_affiliation_query(groups):
    parts = []
    for tokens in groups:
        if len(tokens) == 1:
            parts.append(f'"{tokens[0]}"[Affiliation]')
        else:
            parts.append("(" + " AND ".join(f'"{t}"[Affiliation]' for t in tokens) + ")")
    return "(" + " OR ".join(parts) + ")"


def match_home_institution(affil_text, groups):
    """True if any group's tokens are ALL substrings of the affiliation text.

    Same substring/AND/OR semantics as the PubMed [Affiliation] query, reusable by
    the downstream matcher to mark WCM-affiliated authors."""
    if not affil_text:
        return False
    t = affil_text.lower()
    return any(all(tok in t for tok in tokens) for tokens in groups)


# ---- E-utilities -----------------------------------------------------------
def _req(method, path, **params):
    params.update(tool=TOOL, email=EMAIL)
    if API_KEY:
        params["api_key"] = API_KEY
    url = f"{EUTILS}/{path}"
    for attempt in range(5):
        try:
            r = (requests.post(url, data=params, timeout=60) if method == "POST"
                 else requests.get(url, params=params, timeout=60))
            if r.status_code == 200:
                return r
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.5 * (attempt + 1))
                continue
            r.raise_for_status()
        except requests.RequestException:
            if attempt == 4:
                raise
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"E-utilities {path} failed after retries")


def esearch_history(term, date_from, date_to):
    r = _req("POST", "esearch.fcgi", db="pubmed", term=term, datetype="edat",
             mindate=date_from, maxdate=date_to, usehistory="y", retmax=0, retmode="json")
    res = r.json()["esearchresult"]
    return res["webenv"], res["querykey"], int(res["count"])


def efetch_batch(webenv, query_key, retstart, retmax):
    r = _req("POST", "efetch.fcgi", db="pubmed", WebEnv=webenv, query_key=query_key,
             retstart=retstart, retmax=retmax, retmode="xml")
    return r.content


# ---- XML parsing -----------------------------------------------------------
def _entrez_date(article_el):
    """PubStatus='entrez' history date -> yyyy-mm-dd (month/day default 01)."""
    for pd in article_el.findall("./PubmedData/History/PubMedPubDate"):
        if pd.get("PubStatus") == "entrez":
            y = pd.findtext("Year")
            if not y:
                return None
            m = pd.findtext("Month") or "01"
            d = pd.findtext("Day") or "01"
            try:
                return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
            except ValueError:
                return None
    return None


def _doi(article_el):
    for aid in article_el.findall("./PubmedData/ArticleIdList/ArticleId"):
        if aid.get("IdType") == "doi":
            return aid.text
    for el in article_el.findall("./MedlineCitation/Article/ELocationID"):
        if el.get("EIdType") == "doi":
            return el.text
    return None


def _text(el):
    return "".join(el.itertext()).strip() if el is not None else None


def parse_articles(xml_bytes, groups):
    root = ET.fromstring(xml_bytes)
    out = []
    for art in root.findall("./PubmedArticle"):
        pmid = art.findtext("./MedlineCitation/PMID")
        if not pmid:
            continue
        authors = []
        for au in art.findall("./MedlineCitation/Article/AuthorList/Author"):
            last = au.findtext("LastName")
            fore = au.findtext("ForeName")
            if not (last or fore):
                continue  # skip CollectiveName authors
            affs = [a.text for a in au.findall("AffiliationInfo/Affiliation") if a.text]
            authors.append({
                "last": last, "fore": fore, "initials": au.findtext("Initials"),
                "affiliations": affs,
                "home_inst": any(match_home_institution(a, groups) for a in affs),
            })
        out.append({
            "pmid": int(pmid),
            "entrez_date": _entrez_date(art),
            "title": _text(art.find("./MedlineCitation/Article/ArticleTitle")),
            "journal": art.findtext("./MedlineCitation/Article/Journal/Title"),
            "doi": _doi(art),
            "authors": authors,
            "wcm_author_count": sum(1 for a in authors if a["home_inst"]),
        })
    return out


# ---- driver ----------------------------------------------------------------
def window_for_mode(mode):
    today = date.today()
    if mode == "initial":
        return today - timedelta(days=730), today - timedelta(days=40)
    if mode == "recurring":
        return today - timedelta(days=71), today - timedelta(days=40)
    raise ValueError(mode)


def _fmt(d):
    return d.strftime("%Y/%m/%d") if isinstance(d, date) else d


def pull_universe(date_from, date_to, max_records=None, progress=True):
    groups = load_home_institution_groups()
    term = build_affiliation_query(groups)
    webenv, query_key, count = esearch_history(term, date_from, date_to)
    if progress:
        print(f"  ESearch: {count} WCM articles in entrez window {date_from}..{date_to}",
              flush=True)
    total = min(count, max_records) if max_records else count
    articles = []
    for start in range(0, total, EFETCH_BATCH):
        retmax = min(EFETCH_BATCH, total - start)
        xml = efetch_batch(webenv, query_key, start, retmax)
        articles.extend(parse_articles(xml, groups))
        if progress:
            print(f"  fetched {min(start + retmax, total)}/{total}", flush=True)
        time.sleep(SLEEP)
    # precise re-window on the parsed entrez date (belt-and-suspenders vs ESearch EDAT)
    lo = date_from.replace("/", "-") if isinstance(date_from, str) else date_from.isoformat()
    hi = date_to.replace("/", "-") if isinstance(date_to, str) else date_to.isoformat()
    in_window = [a for a in articles if a["entrez_date"] and lo <= a["entrez_date"] <= hi]
    return {
        "term": term,
        "date_from": lo, "date_to": hi,
        "esearch_count": count,
        "fetched": len(articles),
        "in_window": len(in_window),
        "with_wcm_author": sum(1 for a in in_window if a["wcm_author_count"] > 0),
        "articles": in_window,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["initial", "recurring"])
    ap.add_argument("--from", dest="date_from", help="YYYY/MM/DD (custom window)")
    ap.add_argument("--to", dest="date_to", help="YYYY/MM/DD (custom window)")
    ap.add_argument("--max", type=int, default=None, help="cap fetched records (testing)")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    if args.mode:
        d_from, d_to = window_for_mode(args.mode)
        d_from, d_to = _fmt(d_from), _fmt(d_to)
    elif args.date_from and args.date_to:
        d_from, d_to = args.date_from, args.date_to
    else:
        ap.error("provide --mode or both --from and --to")

    print(f"Universe pull: entrez {d_from} .. {d_to}"
          + (f" (api_key set)" if API_KEY else " (NO api_key — slow)"), flush=True)
    result = pull_universe(d_from, d_to, max_records=args.max)

    outdir_base = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                               "analysis", "adversarial_attribution_review")
    out = args.out or os.path.join(
        outdir_base, f"universe_{result['date_from']}_{result['date_to']}.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as fh:
        json.dump(result, fh, indent=2)

    print(f"\n  esearch_count={result['esearch_count']}  fetched={result['fetched']}"
          f"  in_window={result['in_window']}  with_wcm_author={result['with_wcm_author']}")
    print(f"  -> {out}")


if __name__ == "__main__":
    main()
