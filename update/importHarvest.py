"""Nightly import harvest (3g), DRY-RUN ONLY in this release: pull each person's works from
their ORCID record, their CONFIRMED Scopus Author IDs and their CONFIRMED OpenAlex author ids,
run every work through the import pipeline, and record what WOULD be written.

Sources, in run order (plan decision 7: the first source to produce a work holds the live row):
  SCOPUS_AUID      Scopus Search AU-ID(<id>), COMPLETE view, 25 per page.
  OPENALEX_AUTHOR  /works?filter=authorships.author.id:<A...>, 200 per page (keyless paging).
  ORCID            pub.orcid.org/v3.0/<iD>/works (anonymous public API), one row per work
                   group; DOI-bearing works are enriched with authors and affiliations from
                   OpenAlex (filter=doi:a|b, 50 per call), falling back to Crossref.
  Only CONFIRMED person_author_id rows are harvested. ORCID iDs are trust-tiered (plan
  decision 2): T1 = scholar_active.has_confirmed_orcid = 1 with the iD from DynamoDB Identity,
  T2 = admin_orcid for a non-scholar. --orcid-tier any (dry-run measurement only) takes any
  Identity iD instead.

Per-work pipeline:
  1. scope       ORCID patents/inventions/disclosures/licenses/peer reviews, Crossref and
                 OpenAlex peer reviews and grants, OpenAlex paratext and supplementary
                 materials are skipped (skipped_scope). Talks, posters, errata and retractions
                 are imported but hidden by PM (counted in hidden_type, a subset of new_pending).
  2. PubMed      a work with a PMID (its own, the enrichment's, the person's person_article
                 DOI, doi_pmid_cache, else PubMed "<doi>"[DOI]) is never written
                 (skipped_pubmed), split by the person's person_article assertion into
                 accepted / rejected / candidate / not_retrieved.
  3. rejection   external_rejection for the uid, compared on the highest-precedence key both
                 carry (DOI, ORCID put-code, source id, title+year): skipped_rejected.
  4. existing    (uid, article_id) already in external_record: existing.
  5. automerge   matches one of the person's external_record rows, or a work an earlier source
                 produced tonight, by DOI, source-id crosswalk or title_year_key: automerged.
  6. otherwise   new_pending, with an affiliation flag (KNOWN / UNFAMILIAR / NONE) and a
                 possible-duplicate flag (>= 0.85 title overlap with an accepted PubMed paper
                 of the same year).
  A (uid, source, source id) whose new_pending passes IMPORT_HARVEST_MAX_NEW_PER_PERSON is
  capped (nothing would be written for it, and its works do not seed later automerges).

Writes (dry run): import_harvest_run (one row), import_harvest_person (counts, last_status,
  capped, sample_json with up to 20 would-be rows), doi_pmid_cache. Never external_record,
  external_record_event or external_rejection. The incremental-state columns (last_ok_at,
  last_full_at, item_set_hash) are left alone, so the first apply run does a full pull.
  --no-stats writes nothing at all.

--apply HARD-REFUSES (exit 2) in this release: the write path, the cutover check and
  --rollback-run ship in plan PR H3, after 3b's cutover.

  python3 importHarvest.py [--dry-run]           (default)
  python3 importHarvest.py --selftest            (fixtures; no network, no DB)
  options: --uids a,b | --cohort s3://b/k|path|all-confirmed, --all (ignore the weekday shard),
           --limit N, --sources scopus,openalex,orcid, --orcid-tier trusted|any, --no-stats,
           --max-seconds N, --openalex-max-calls N, --scopus-max-calls N, --pubmed-max-calls N
Env: IMPORT_HARVEST_SOURCES, IMPORT_HARVEST_COHORT, IMPORT_HARVEST_MAX_NEW_PER_PERSON (100),
  IMPORT_HARVEST_MAX_NEW_PER_RUN (5000), IMPORT_HARVEST_MAX_FAILURE_RATE (0.05),
  SCOPUS_HARVEST_MAX_CALLS (2000), IMPORT_HARVEST_OPENALEX_MAX_CALLS (500),
  IMPORT_HARVEST_PUBMED_MAX_CALLS (6000), SCOPUS_API_KEY, SCOPUS_INST_TOKEN, PUBMED_API_KEY,
  OPENALEX_MAILTO, OPENALEX_API_KEY, CROSSREF_MAILTO, DB_* (never printed).
"""
import argparse, datetime, hashlib, json, os, re, statistics, sys, time, unicodedata, zlib
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

SOURCES = ("SCOPUS_AUID", "OPENALEX_AUTHOR", "ORCID")      # run order (decision 7)
SOURCE_ARG = {"scopus": "SCOPUS_AUID", "openalex": "OPENALEX_AUTHOR", "orcid": "ORCID"}
COUNTERS = ("fetched", "skipped_scope", "skipped_pubmed", "skipped_pubmed_accepted",
            "skipped_pubmed_rejected", "skipped_pubmed_candidate", "skipped_pubmed_not_retrieved",
            "skipped_rejected", "existing", "automerged", "new_pending", "hidden_type",
            "affil_known", "affil_unfamiliar", "affil_none")
SAMPLE_ROWS = 20
POSSIBLE_DUP_SIMILARITY = 0.85        # PM pubTypes.ts POSSIBLE_DUPLICATE_TITLE_SIMILARITY
ORCID_RE = re.compile(r"\d{4}-\d{4}-\d{4}-\d{3}[\dX]")
VECTORS = os.path.join(HERE, "data", "title_year_key_vectors.json")

OPENALEX = "https://api.openalex.org"
OPENALEX_SLEEP = 0.2
OPENALEX_PER_PAGE = 200
OPENALEX_DOI_BATCH = 50
SCOPUS_SEARCH = "https://api.elsevier.com/content/search/scopus"
SCOPUS_PAGE = 25                      # COMPLETE view maximum
SCOPUS_SLEEP = 0.3
ORCID_API = "https://pub.orcid.org/v3.0"
ORCID_SLEEP = 0.2                     # 5 req/s
CROSSREF = "https://api.crossref.org/works"
CROSSREF_BATCH = 40
CROSSREF_SLEEP = 0.4
WCM_OPENALEX_IDS = ("I4387153466", "I4405272987")   # as aar_universe_openalex.WCM_OPENALEX_IDS

# Source type dispositions, copied from PM src/lib/pubTypes.ts (keys normalized as its
# normSourceType). Only the two dispositions the harvest acts on are needed here: the type
# itself is stored raw and mapped by PM (plan decision 4). OpenAlex paratext and
# supplementary-materials are out of scope at harvest only (plan decision 5).
OUT_OF_SCOPE = {
    "ORCID": {"patent", "invention", "disclosure", "license", "peer-review"},
    "OPENALEX": {"peer-review", "grant", "paratext", "supplementary-materials"},
    "CROSSREF": {"peer-review", "grant"},
    "SCOPUS": set(),
}
HIDDEN = {   # imported, but hidden from Pending and its counts by PM
    "ORCID": {"lecture-speech", "conference-poster", "conference-presentation", "public-speech"},
    "OPENALEX": {"erratum", "retraction"},
    "CROSSREF": set(),
    "SCOPUS": {"erratum", "er", "retracted", "tb"},
}


def log(msg):
    print(msg, flush=True)


# ---- keys shared with Publication Manager --------------------------------------------
# Byte-for-byte ports of PM src/lib/pubTypes.ts normTitle and src/lib/externalRecordShape.ts
# normDoi / yearOf / titleYearKey / sourceIdsOf / rejectionKeysOf. update/data/
# title_year_key_vectors.json is generated from PM's own code and pins them.
def norm_title(x):
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", str(x or "").lower())).strip()


def norm_doi(d):
    s = re.sub(r"^(https?://(dx\.)?doi\.org/|doi:\s*)", "", str(d if d is not None else "").strip().lower()).strip()
    return s or None


def year_of(date):
    m = re.search(r"[0-9]{4}", str(date if date is not None else ""))
    return m.group(0) if m else None


def title_year_key(title, date):
    t, y = norm_title(title), year_of(date)
    return hashlib.sha1(f"{t}|{y}".encode("utf-8")).hexdigest() if t and y else None


def bare_openalex(i):
    s = re.sub(r"^https?://openalex\.org/", "", str(i if i is not None else "").strip(), flags=re.I)
    return s or None


def source_ids_of(r):
    out = []
    aid = str(r.get("article_id") or "")
    prefix, _, rest = aid.partition(":")
    prefix = prefix.upper()
    if prefix in ("SCOPUS", "WOS"):
        out.append(f"{prefix}:{rest.strip()}")
    if prefix == "OPENALEX" and bare_openalex(rest):
        out.append(f"OPENALEX:{bare_openalex(rest)}")
    if r.get("scopus_eid"):
        out.append(f"SCOPUS:{str(r['scopus_eid']).strip()}")
    if bare_openalex(r.get("openalex_id")):
        out.append(f"OPENALEX:{bare_openalex(r.get('openalex_id'))}")
    return list(dict.fromkeys(out))


REJECTION_KEY_TYPES = ("DOI", "ORCID_PUTCODE", "SOURCE_ID", "TITLE_YEAR")


def rejection_keys_of(r):
    keys = []
    doi = norm_doi(r.get("doi"))
    if doi:
        keys.append(("DOI", doi[:255]))
    put = str(r.get("orcid_put_code") if r.get("orcid_put_code") is not None else "").strip()
    if put:
        keys.append(("ORCID_PUTCODE", put))
    keys += [("SOURCE_ID", s[:255]) for s in source_ids_of(r)]
    ty = r.get("title_year_key") or title_year_key(r.get("title"), r.get("pub_date"))
    if ty:
        keys.append(("TITLE_YEAR", ty))
    return keys


def match_rejection_memory(cand_keys, rows):
    """PM matchRejectionMemory: per remembered record, the highest-precedence key type both
    carry decides, match or not. rows: [{"key_type", "key_value", "record_id"}]."""
    groups = defaultdict(list)
    for i, r in enumerate(rows):
        groups[f"r{r['record_id']}" if r.get("record_id") is not None else f"x{i}"].append(r)
    hits = []
    for g in groups.values():
        for t in REJECTION_KEY_TYPES:
            mine = {v for k, v in cand_keys if k == t}
            theirs = [r for r in g if r["key_type"] == t]
            if not mine or not theirs:
                continue
            hit = next((r for r in theirs if r["key_value"] in mine), None)
            if hit:
                hits.append(hit)
            break
    return hits


# ---- small helpers -------------------------------------------------------------------
def norm_type(t):
    return re.sub(r"[\s_]+", "-", str(t or "").strip().lower())


def norm_name(s):
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode().lower()
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", s)).strip()


def surname_key(s):
    return re.sub(r"[^a-z]", "", norm_name(s))


def shard_of(uid):
    return zlib.crc32(uid.encode()) % 7


def digits(v):
    m = re.search(r"\d+", str(v or ""))
    return m.group(0) if m else None


def title_similarity(a, b):
    """PM titleSimilarity: shared words over all distinct words (Jaccard)."""
    wa, wb = set(norm_title(a).split()), set(norm_title(b).split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / len(wa | wb)


def date_str(y, m=None, d=None):
    if not y:
        return None
    out = str(y)
    if m:
        out += f"-{int(m):02d}"
        if d:
            out += f"-{int(d):02d}"
    return out


def as_list(v):
    return v if isinstance(v, list) else [] if v is None else [v]


# ---- normalizers: one candidate shape per work -----------------------------------------
# {source, source_type, article_id, provenance, source_detail, doi, pmid, isbn, scopus_eid,
#  openalex_id, orcid_put_code, title, pub_date, source_pub_type, container_title,
#  authors [{name, surname, scopus, openalex, orcid, affils [(id, name)]}],
#  target_author_index, slot_affiliations [(id, name)]}
def _slot(authors, pred):
    hits = [i for i, a in enumerate(authors, 1) if pred(a)]
    return hits[0] if len(hits) == 1 else None


def set_slot(c, surname=None, orcid=None, scopus=None, openalex=None):
    """target_author_index, in plan order: ORCID iD on the byline, else the AU-ID / OpenAlex id,
    else a unique surname match; slot_affiliations from that author."""
    au, idx = c.get("authors") or [], None
    if orcid:
        idx = _slot(au, lambda a: a.get("orcid") == orcid)
    if idx is None and scopus:
        idx = _slot(au, lambda a: a.get("scopus") == scopus)
    if idx is None and openalex:
        idx = _slot(au, lambda a: a.get("openalex") == openalex)
    if idx is None and surname_key(surname):
        idx = _slot(au, lambda a: surname_key(a.get("surname")) == surname_key(surname))
    c["target_author_index"] = idx
    c["slot_affiliations"] = list(au[idx - 1].get("affils") or []) if idx else []
    return c


def scopus_work(e, auid, surname=None):
    eid = digits(str(e.get("dc:identifier") or "").split(":")[-1]) or digits(str(e.get("eid") or "").replace("2-s2.0-", ""))
    aff = {str(a.get("afid")): a.get("affilname") for a in as_list(e.get("affiliation")) if a.get("afid")}
    authors = []
    for a in sorted(as_list(e.get("author")), key=lambda a: int(a.get("@seq") or 0)):
        afids = [str(d.get("$")) for d in as_list(a.get("afid")) if d.get("$")]
        authors.append({"name": a.get("authname"), "surname": a.get("surname"), "given": a.get("given-name"),
                        "scopus": str(a.get("authid") or "") or None, "orcid": a.get("orcid"),
                        "affils": [(f"scopus:{x}", aff.get(x)) for x in afids]})
    isbn = ",".join(str(i.get("$") if isinstance(i, dict) else i) for i in as_list(e.get("prism:isbn"))) or None
    c = {"source": "SCOPUS_AUID", "source_type": "SCOPUS", "article_id": f"SCOPUS:{eid}",
         "provenance": "SCOPUS_AUID", "source_detail": None, "doi": norm_doi(e.get("prism:doi")),
         "pmid": digits(e.get("pubmed-id")), "isbn": isbn, "scopus_eid": eid, "openalex_id": None,
         "orcid_put_code": None, "title": e.get("dc:title"), "pub_date": e.get("prism:coverDate"),
         "source_pub_type": e.get("subtypeDescription") or e.get("subtype"),
         "container_title": e.get("prism:publicationName"), "authors": authors}
    return set_slot(c, surname=surname, scopus=str(auid))


def _openalex_authors(w):
    out = []
    for a in w.get("authorships") or []:
        au = a.get("author") or {}
        name = au.get("display_name") or a.get("raw_author_name") or ""
        affs = [(f"openalex:{bare_openalex(i.get('id'))}", i.get("display_name")) for i in a.get("institutions") or [] if i.get("id")]
        affs += [(None, s) for s in a.get("raw_affiliation_strings") or []]
        out.append({"name": name, "surname": name.split()[-1] if name.split() else None,
                    "openalex": bare_openalex(au.get("id")),
                    "orcid": (ORCID_RE.search(au.get("orcid") or "") or [None])[0] if au.get("orcid") else None,
                    "affils": affs})
    return out


def openalex_work(w, aid, surname=None):
    wid = bare_openalex(w.get("id"))
    src = ((w.get("primary_location") or {}).get("source") or {})
    c = {"source": "OPENALEX_AUTHOR", "source_type": "OPENALEX", "article_id": f"OPENALEX:{wid}",
         "provenance": "OPENALEX_AUTHOR", "source_detail": None, "doi": norm_doi(w.get("doi")),
         "pmid": digits(str((w.get("ids") or {}).get("pmid") or "").rstrip("/").rsplit("/", 1)[-1]),
         "isbn": None, "scopus_eid": None, "openalex_id": wid, "orcid_put_code": None,
         "title": w.get("title") or w.get("display_name"),
         "pub_date": w.get("publication_date") or (str(w["publication_year"]) if w.get("publication_year") else None),
         "source_pub_type": w.get("type"), "container_title": src.get("display_name"),
         "authors": _openalex_authors(w)}
    return set_slot(c, surname=surname, openalex=aid)


def _orcid_ids(ext):
    out = defaultdict(list)
    for x in (ext or {}).get("external-id") or []:
        if (x.get("external-id-relationship") or "self") != "self":
            continue
        t = (x.get("external-id-type") or "").lower()
        v = ((x.get("external-id-normalized") or {}).get("value")) or x.get("external-id-value")
        if t and v:
            out[t].append(str(v).strip())
    return out


def orcid_provenance(summaries, orcid):
    """(provenance, source_detail). Self-asserted when the researcher's own iD is the source of
    any summary in the group; else by the preferred summary's member source (decision 8)."""
    for s in summaries:
        if (((s.get("source") or {}).get("source-orcid") or {}).get("path")) == orcid:
            return "ORCID_SELF", None
    src = summaries[0].get("source") or {}
    name = ((src.get("source-name") or {}).get("value")) or ""
    low = name.lower()
    if "crossref" in low:
        return "ORCID_CROSSREF", None
    if "scopus" in low:
        return "ORCID_SCOPUS", None
    return "ORCID_OTHER", (name or None) and name[:200]


def orcid_works(j, orcid):
    """One candidate per ORCID work group: the preferred (first) summary's fields, the group's
    identifiers, and every put-code in the group."""
    out = []
    for g in (j or {}).get("group") or []:
        sums = g.get("work-summary") or []
        if not sums:
            continue
        s = sums[0]
        ids = _orcid_ids(g.get("external-ids"))
        for x in sums:
            for k, v in _orcid_ids(x.get("external-ids")).items():
                ids[k] += [i for i in v if i not in ids[k]]
        pd = s.get("publication-date") or {}
        val = lambda k: ((pd.get(k) or {}).get("value"))
        prov, detail = orcid_provenance(sums, orcid)
        put = str(s.get("put-code"))
        out.append({"source": "ORCID", "source_type": "ORCID", "article_id": f"ORCID:{put}",
                    "provenance": prov, "source_detail": detail,
                    "doi": norm_doi((ids.get("doi") or [None])[0]),
                    "pmid": digits((ids.get("pmid") or [None])[0]),
                    "isbn": ",".join(ids.get("isbn") or []) or None,
                    "scopus_eid": digits(str((ids.get("eid") or [""])[0]).replace("2-s2.0-", "")),
                    "openalex_id": None, "orcid_put_code": put,
                    "put_codes": [str(x.get("put-code")) for x in sums],
                    "title": (((s.get("title") or {}).get("title") or {}).get("value")),
                    "pub_date": date_str(val("year"), val("month"), val("day")),
                    "source_pub_type": s.get("type"),
                    "container_title": ((s.get("journal-title") or {}).get("value")),
                    "authors": [], "target_author_index": None, "slot_affiliations": []})
    return out


def enrich_from_openalex(c, w, orcid, surname):
    """ORCID summaries carry no byline: take authors, PMID and the OpenAlex id from the work."""
    c["authors"] = _openalex_authors(w)
    c["openalex_id"] = c.get("openalex_id") or bare_openalex(w.get("id"))
    if not c.get("pmid"):
        c["pmid"] = digits(str((w.get("ids") or {}).get("pmid") or "").rstrip("/").rsplit("/", 1)[-1])
    c["enriched"] = "openalex"
    return set_slot(c, surname=surname, orcid=orcid)


def enrich_from_crossref(c, m, orcid, surname):
    au = []
    for a in m.get("author") or []:
        fam = a.get("family") or a.get("name") or ""
        au.append({"name": " ".join(x for x in (a.get("given"), fam) if x), "surname": fam,
                   "orcid": (ORCID_RE.search(a.get("ORCID") or "") or [None])[0] if a.get("ORCID") else None,
                   "affils": [(None, x.get("name")) for x in a.get("affiliation") or [] if x.get("name")]})
    c["authors"] = au
    c["enriched"] = "crossref"
    return set_slot(c, surname=surname, orcid=orcid)


def orcid_redirect_target(requested, final_url, status, body=""):
    """The primary iD when ORCID answered for a deprecated iD (a 301 to the primary record, or
    a 409 naming it), else None."""
    for text in (final_url or "", body or "") if status in (200, 301, 302, 409) else ():
        for m in ORCID_RE.findall(text):
            if m != requested:
                return m
    return None


# ---- affiliation flag ----------------------------------------------------------------
def known_affiliations(afid_names, accepted_affils, identity_institutions, wcm_openalex=WCM_OPENALEX_IDS):
    """(ids, names): the known set. ids are namespaced 'scopus:<afid>' / 'openalex:<I...>';
    names are normalized. Organizational units are left out: a department name ('Radiology')
    would make every radiology department anywhere look known."""
    ids, names = set(), set()
    for afid, name in afid_names:
        ids.add(f"scopus:{afid}")
        if name:
            names.add(norm_name(name))
    for afid, label in accepted_affils:
        if afid:
            ids.add(f"scopus:{afid}")
        if label:
            names.add(norm_name(label))
    ids.update(f"openalex:{i}" for i in wcm_openalex)
    names.update(norm_name(n) for n in identity_institutions if n)
    names.discard("")
    return ids, names


def affiliation_flag(slot_affs, known):
    ids, names = known
    if not slot_affs:
        return "NONE"
    for i, name in slot_affs:
        if i and i in ids:
            return "KNOWN"
        n = norm_name(name)
        if n and (n in names or any(len(k) >= 8 and k in n for k in names)):
            return "KNOWN"
    return "UNFAMILIAR"


# ---- the pipeline (pure) --------------------------------------------------------------
def same_work(c, r):
    """DOI, then source-id crosswalk, then title_year_key (plan step 7)."""
    if c.get("doi") and r.get("doi") and norm_doi(c["doi"]) == norm_doi(r["doi"]):
        return "doi"
    if set(source_ids_of(c)) & set(source_ids_of(r)):
        return "source_id"
    ck = title_year_key(c.get("title"), c.get("pub_date"))
    rk = r.get("title_year_key") or title_year_key(r.get("title"), r.get("pub_date"))
    if ck and ck == rk:
        return "title_year"
    return None


def pubmed_bucket(pmid, assertions):
    a = assertions.get(int(pmid)) if str(pmid).isdigit() else None
    if a is None:
        return "skipped_pubmed_not_retrieved"
    return {"ACCEPTED": "skipped_pubmed_accepted", "REJECTED": "skipped_pubmed_rejected"}.get(a, "skipped_pubmed_candidate")


def process_batch(cands, person, live, resolve_pmid, cap):
    """Run one (uid, source, source id) batch of candidates through the pipeline.

    person: {"records": external_record rows [dict], "rejections": [rows],
             "assertions": {pmid: userAssertion}, "known": (ids, names),
             "accepted_titles": [(title, year)]}
    live:   works earlier batches produced tonight (mutated only when this batch is not capped)
    resolve_pmid(doi) -> pmid | None.
    Returns (counts Counter, would-be rows, capped)."""
    n, rows, seen = Counter(), [], {r["article_id"] for r in person["records"]}
    new_live = []
    for c in cands:
        n["fetched"] += 1
        src = c["source_type"]
        t = norm_type(c.get("source_pub_type"))
        if t in OUT_OF_SCOPE.get(src, ()):
            n["skipped_scope"] += 1
            continue
        pmid = c.get("pmid") or (resolve_pmid(c["doi"]) if c.get("doi") else None)
        if pmid:
            n["skipped_pubmed"] += 1
            n[pubmed_bucket(pmid, person["assertions"])] += 1
            continue
        if match_rejection_memory(rejection_keys_of(c), person["rejections"]):
            n["skipped_rejected"] += 1
            continue
        if c["article_id"] in seen:
            n["existing"] += 1
            continue
        parent = next(((r, how) for r in person["records"] + live + new_live
                       if (how := same_work(c, r))), None)
        if parent:
            n["automerged"] += 1
            continue
        n["new_pending"] += 1
        hidden = t in HIDDEN.get(src, ())
        if hidden:
            n["hidden_type"] += 1
        flag = affiliation_flag(c.get("slot_affiliations"), person["known"])
        n["affil_" + flag.lower()] += 1
        y = year_of(c.get("pub_date"))
        dup = any(ty == y and title_similarity(c.get("title"), tt) >= POSSIBLE_DUP_SIMILARITY
                  for tt, ty in person["accepted_titles"]) if y else False
        if dup:
            n["possible_dup"] += 1
        c["title_year_key"] = title_year_key(c.get("title"), c.get("pub_date"))
        new_live.append(c)
        seen.add(c["article_id"])
        rows.append({"article_id": c["article_id"], "provenance": c["provenance"],
                     "source_detail": c.get("source_detail"), "doi": c.get("doi"),
                     "title": (c.get("title") or "")[:200], "pub_date": c.get("pub_date"),
                     "source_pub_type": c.get("source_pub_type"), "hidden": hidden,
                     "target_author_index": c.get("target_author_index"),
                     "author_count": len(c.get("authors") or []) or None,
                     "affiliation_flag": flag, "possible_dup": dup,
                     "slot_affiliation": next((nm for _, nm in c.get("slot_affiliations") or [] if nm), None)})
    capped = n["new_pending"] > cap
    if not capped:
        live.extend(new_live)
    return n, rows, capped


# ---- HTTP with per-source budgets and circuit breakers ------------------------------------
class SourceGuard:
    """Calls and failures for one source this run. Trips when the budget is spent or when
    failures pass max_rate of calls (at least 3 failures), like aar_universe_scopus
    pubmed_unreliable. A tripped source stops for the night."""

    def __init__(self, name, budget, max_rate):
        self.name, self.budget, self.max_rate = name, budget, max_rate
        self.calls = self.failures = 0
        self.tripped = None

    def ok(self):
        if self.tripped:
            return False
        if self.calls >= self.budget:
            self.tripped = f"call budget {self.budget} spent"
        elif self.failures >= 3 and self.failures > self.max_rate * max(self.calls, 1):
            self.tripped = f"failure rate {self.failures}/{self.calls}"
        return not self.tripped


class Budget(Exception):
    pass


def http_get(guard, url, params=None, headers=None, sleep=0.0, tries=4, meta=None):
    """GET -> (status, json|None, final_url). 404 -> (404, None). Counts one call per request
    and one failure per request that never succeeded; raises on failure."""
    import requests
    if not guard.ok():
        raise Budget(guard.tripped)
    guard.calls += 1
    last = None
    for attempt in range(tries):
        try:
            r = requests.get(url, params=params, headers=headers or {"Accept": "application/json"}, timeout=60)
        except (requests.ConnectionError, requests.Timeout) as e:
            last = type(e).__name__
            time.sleep(2 * (attempt + 1))
            continue
        if meta is not None:
            for k, v in r.headers.items():
                if k.lower().startswith("x-ratelimit"):
                    meta[k] = v
        time.sleep(sleep)
        if r.status_code == 200:
            return 200, r.json(), r.url
        if r.status_code in (404, 409, 410):
            return r.status_code, None, r.url if r.status_code == 404 else (r.url + " " + r.text[:500])
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            time.sleep(2 * (attempt + 1))
            continue
        last = f"HTTP {r.status_code}"
        break
    guard.failures += 1
    raise RuntimeError(f"{guard.name}: {last}")   # never the URL: it can carry an api key


def openalex_params(p):
    p = dict(p)
    for env, key in (("OPENALEX_MAILTO", "mailto"), ("OPENALEX_API_KEY", "api_key")):
        v = (os.environ.get(env) or "").strip()
        if v:
            p[key] = v
    return p


OA_SELECT = "id,doi,ids,title,display_name,type,publication_date,publication_year,primary_location,authorships"


def fetch_openalex_author(guard, aid, surname):
    out, page = [], 1
    while True:
        _, j, _ = http_get(guard, f"{OPENALEX}/works", openalex_params(
            {"filter": f"authorships.author.id:{aid}", "per_page": OPENALEX_PER_PAGE, "page": page,
             "select": OA_SELECT}), sleep=OPENALEX_SLEEP)
        res = (j or {}).get("results") or []
        out += [openalex_work(w, aid, surname) for w in res]
        count = ((j or {}).get("meta") or {}).get("count") or 0
        if not res or page * OPENALEX_PER_PAGE >= min(count, 10_000):
            return out
        page += 1


def fetch_scopus_auid(guard, auid, surname, meta):
    headers = {"X-ELS-APIKey": os.environ["SCOPUS_API_KEY"], "X-ELS-Insttoken": os.environ["SCOPUS_INST_TOKEN"],
               "Accept": "application/json"}
    out, start = [], 0
    while True:
        _, j, _ = http_get(guard, SCOPUS_SEARCH, {"query": f"AU-ID({auid})", "view": "COMPLETE",
                                                  "count": SCOPUS_PAGE, "start": start},
                           headers=headers, sleep=SCOPUS_SLEEP, meta=meta)
        sr = (j or {}).get("search-results") or {}
        ents = [e for e in sr.get("entry") or [] if not e.get("error")]
        out += [scopus_work(e, auid, surname) for e in ents]
        total = int(sr.get("opensearch:totalResults") or 0)
        start += SCOPUS_PAGE
        if not ents or start >= min(total, 5000):
            return out


def fetch_orcid(guard, orcid):
    """(works, primary iD when the requested one is deprecated)."""
    st, j, final = http_get(guard, f"{ORCID_API}/{orcid}/works", headers={"Accept": "application/json"}, sleep=ORCID_SLEEP)
    moved = orcid_redirect_target(orcid, final, st, final)
    if st != 200 and moved:
        st, j, final = http_get(guard, f"{ORCID_API}/{moved}/works", headers={"Accept": "application/json"}, sleep=ORCID_SLEEP)
    if st != 200:
        return [], moved
    return orcid_works(j, moved or orcid), moved


def enrich_orcid(cands, orcid, surname, oa_guard, cr_guard, stats):
    """OpenAlex by DOI (50 per call), Crossref for what OpenAlex lacks. Enrichment failures leave
    the ORCID fields (empty byline); they never drop a work."""
    by_doi = defaultdict(list)
    for c in cands:
        if c.get("doi"):
            by_doi[c["doi"]].append(c)
    dois, found = list(by_doi), set()
    for i in range(0, len(dois), OPENALEX_DOI_BATCH):
        chunk = dois[i:i + OPENALEX_DOI_BATCH]
        try:
            _, j, _ = http_get(oa_guard, f"{OPENALEX}/works", openalex_params(
                {"filter": "doi:" + "|".join(chunk), "per_page": OPENALEX_DOI_BATCH, "select": OA_SELECT}),
                sleep=OPENALEX_SLEEP)
        except Exception as e:
            stats["enrich_openalex_error"] += 1
            log(f"importHarvest: OpenAlex enrichment failed: {e}")
            break
        for w in (j or {}).get("results") or []:
            d = norm_doi(w.get("doi"))
            for c in by_doi.get(d, []):
                enrich_from_openalex(c, w, orcid, surname)
                found.add(d)
    rest = [d for d in dois if d not in found]
    for i in range(0, len(rest), CROSSREF_BATCH):
        chunk = rest[i:i + CROSSREF_BATCH]
        p = {"filter": ",".join(f"doi:{d}" for d in chunk), "rows": len(chunk), "select": "DOI,author"}
        if os.environ.get("CROSSREF_MAILTO"):
            p["mailto"] = os.environ["CROSSREF_MAILTO"]
        try:
            _, j, _ = http_get(cr_guard, CROSSREF, p, sleep=CROSSREF_SLEEP)
        except Exception as e:
            stats["enrich_crossref_error"] += 1
            log(f"importHarvest: Crossref enrichment failed: {e}")
            break
        for m in ((j or {}).get("message") or {}).get("items") or []:
            for c in by_doi.get(norm_doi(m.get("DOI")), []):
                enrich_from_crossref(c, m, orcid, surname)
    stats["enriched_openalex"] += sum(1 for c in cands if c.get("enriched") == "openalex")
    stats["enriched_crossref"] += sum(1 for c in cands if c.get("enriched") == "crossref")


class PmidResolver:
    """DOI -> PMID: the person's person_article DOIs, then doi_pmid_cache (30 days), then PubMed
    "<doi>"[DOI] via aar_universe_scopus.doi_in_pubmed. Lookups that failed are not cached.
    New answers are queued for doi_pmid_cache."""

    def __init__(self, cache, guard):
        self.cache, self.guard, self.local, self.new = cache, guard, {}, {}
        self.hits = Counter()

    def __call__(self, doi):
        d = norm_doi(doi)
        if not d:
            return None
        if d in self.local:
            self.hits["person_article"] += 1
            return self.local[d]
        if d in self.cache:
            self.hits["cache"] += 1
            return self.cache[d]
        if not self.guard.ok():
            self.hits["unresolved_budget"] += 1
            return None
        import aar_universe_scopus as pub
        before = pub.PUBMED_STATS["failures"]
        self.guard.calls += 1
        pmid = pub.doi_in_pubmed(d)
        if pub.PUBMED_STATS["failures"] > before:
            self.guard.failures += 1
            self.hits["pubmed_error"] += 1
            return None
        self.hits["pubmed"] += 1
        self.cache[d] = self.new[d] = int(pmid) if pmid else None
        return self.cache[d]


# ---- DB -------------------------------------------------------------------------------
def db():
    import pymysql
    return pymysql.connect(host=os.environ["DB_HOST"], user=os.environ["DB_USERNAME"],
                           password=os.environ["DB_PASSWORD"], database=os.environ["DB_NAME"],
                           connect_timeout=15, autocommit=False, charset="utf8mb4")


def table_exists(cur, name):
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = %s", (name,))
    return cur.fetchone()[0] > 0


def load_confirmed_ids(cur, uids=None):
    """{uid: {"SCOPUS_AUID": [ids], "OPENALEX_AUTHOR": [ids]}} for CONFIRMED rows."""
    out = defaultdict(lambda: defaultdict(list))
    if not table_exists(cur, "person_author_id"):
        return out
    cur.execute("SELECT LOWER(uid), id_type, id_value FROM person_author_id WHERE status = 'CONFIRMED' ORDER BY votes DESC, id")
    for u, t, v in cur.fetchall():
        if uids is None or u in uids:
            out[u][t].append(v)
    return out


def read_identity(uids):
    """{uid: {"orcid", "institutions"}} from DynamoDB Identity (BatchGetItem, read-only)."""
    import boto3
    from authorIdSuggestions import batch_get
    ddb = boto3.resource("dynamodb", region_name=os.getenv("AWS_REGION", "us-east-1"))
    out = {}
    for it in batch_get(ddb, "Identity", "uid", sorted(uids), "#u, #i.orcid, #i.institutions",
                        {"#u": "uid", "#i": "identity"}):
        i = it.get("identity") or {}
        o = ORCID_RE.search(str(i.get("orcid") or "").upper())
        out[it["uid"].lower()] = {"orcid": o.group(0) if o else None,
                                  "institutions": [str(x) for x in i.get("institutions") or []]}
    return out


def orcid_tiers(cur, identity, uids):
    """{uid: (iD, tier)}: T1 Scholars-confirmed with the Identity iD, T2 admin_orcid for a
    non-scholar (the same rule as authorIdSuggestions.trusted_orcids)."""
    out, scholars = {}, set()
    if table_exists(cur, "scholar_active"):
        cur.execute("SELECT LOWER(cwid), has_confirmed_orcid FROM scholar_active")
        for u, c in cur.fetchall():
            scholars.add(u)
            if c and u in uids and (identity.get(u) or {}).get("orcid"):
                out[u] = (identity[u]["orcid"], "T1")
    if table_exists(cur, "admin_orcid"):
        cur.execute("SELECT LOWER(personIdentifier), orcid FROM admin_orcid")
        for u, o in cur.fetchall():
            m = ORCID_RE.search(str(o or "").upper())
            if u in uids and u not in scholars and m:
                out[u] = (m.group(0), "T2")
    return out


def load_person(cur, uid, have_er, have_rej):
    cur.execute("""SELECT pmid, userAssertion, doi, articleTitle, articleYear FROM person_article
                   WHERE personIdentifier = %s AND pmid > 0""", (uid,))
    assertions, dois, titles = {}, {}, []
    for pmid, a, doi, title, year in cur.fetchall():
        assertions[int(pmid)] = a or ""
        d = norm_doi(doi)
        if d and d.lower() not in ("null", "'null'"):
            dois[d] = int(pmid)
        if a == "ACCEPTED" and title:
            titles.append((title, str(year) if year else None))
    cur.execute("""SELECT DISTINCT targetAuthorInstitutionalAffiliationArticleScopusAffiliationId,
                          targetAuthorInstitutionalAffiliationArticleScopusLabel
                   FROM person_article_scopus_target_author_affiliation s
                   JOIN person_article p ON p.personIdentifier = s.personIdentifier AND p.pmid = s.pmid
                   WHERE s.personIdentifier = %s AND p.userAssertion = 'ACCEPTED'""", (uid,))
    affs = [(digits(a), l) for a, l in cur.fetchall() if (digits(a) or (l and l not in ("NULL", "'NULL'")))]
    records, rejections = [], []
    if have_er:
        cur.execute("""SELECT id, article_id, status, doi, scopus_eid, openalex_id, orcid_put_code,
                              title_year_key, title, pub_date FROM external_record WHERE uid = %s""", (uid,))
        cols = [d[0] for d in cur.description]
        records = [dict(zip(cols, r)) for r in cur.fetchall()]
    if have_rej:
        cur.execute("SELECT key_type, key_value, record_id FROM external_rejection WHERE uid = %s", (uid,))
        rejections = [{"key_type": t, "key_value": v, "record_id": r} for t, v, r in cur.fetchall()]
    cur.execute("SELECT surname FROM identity WHERE LOWER(cwid) = %s LIMIT 1", (uid,))
    row = cur.fetchone()
    return {"assertions": assertions, "dois": dois, "accepted_titles": titles, "accepted_affils": affs,
            "records": records, "rejections": rejections, "surname": row[0] if row else None}


def load_cohort(spec, cur):
    """uids from 's3://bucket/key', a local file, or 'all-confirmed' (None = everyone with a
    CONFIRMED author id or a trusted ORCID iD)."""
    if not spec or spec == "all-confirmed":
        return None
    if spec.startswith("s3://"):
        import boto3
        b, _, k = spec[5:].partition("/")
        text = boto3.client("s3").get_object(Bucket=b, Key=k)["Body"].read().decode()
    else:
        text = open(spec).read()
    return {u.strip().lower() for u in re.split(r"[\s,]+", text) if u.strip() and not u.startswith("#")}


# ---- stats writes -------------------------------------------------------------------
def start_run(cur, sources, cohort, shard, now):
    cur.execute("""INSERT INTO import_harvest_run (mode, status, sources, cohort, shard, started_at)
                   VALUES ('DRY_RUN', 'running', %s, %s, %s, %s)""", (",".join(sources), (cohort or "all-confirmed")[:255], shard, now))
    return cur.lastrowid


def finish_run(cur, run_id, status, totals, note):
    cur.execute("""UPDATE import_harvest_run SET status = %s, finished_at = UTC_TIMESTAMP(), totals = %s, note = %s
                   WHERE id = %s""", (status, json.dumps(totals, sort_keys=True, default=str), note, run_id))


def write_person(cur, run_id, uid, source, source_id, n, status, capped, sample, error, now):
    cols = list(COUNTERS)
    vals = [int(n.get(c, 0)) for c in cols]
    cur.execute(f"""INSERT INTO import_harvest_person
        (uid, source, source_id, last_run_id, last_visit_at, last_status, {", ".join(cols)}, capped, sample_json, error)
        VALUES (%s,%s,%s,%s,%s,%s,{",".join(["%s"] * len(cols))},%s,%s,%s)
        ON DUPLICATE KEY UPDATE last_run_id=VALUES(last_run_id), last_visit_at=VALUES(last_visit_at),
          last_status=VALUES(last_status), {", ".join(f"{c}=VALUES({c})" for c in cols)},
          capped=VALUES(capped), sample_json=VALUES(sample_json), error=VALUES(error)""",
                [uid, source, source_id[:64], run_id, now, status] + vals +
                [1 if capped else 0, json.dumps(sample[:SAMPLE_ROWS], default=str), (error or None) and error[:500]])


def write_cache(cur, new, now):
    if new:
        cur.executemany("""INSERT INTO doi_pmid_cache (doi, pmid, checked_at) VALUES (%s,%s,%s)
                           ON DUPLICATE KEY UPDATE pmid=VALUES(pmid), checked_at=VALUES(checked_at)""",
                        [(d[:255], p, now) for d, p in new.items()])


# ---- main -----------------------------------------------------------------------------
def dist(xs):
    xs = sorted(xs)
    if not xs:
        return {"n": 0}
    return {"n": len(xs), "median": statistics.median(xs), "p90": xs[min(len(xs) - 1, int(0.9 * len(xs)))],
            "max": xs[-1], "sum": sum(xs)}


def run(a):
    t0 = time.time()
    now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0, tzinfo=None)
    sources = [SOURCE_ARG[s.strip().lower()] for s in (a.sources or os.getenv("IMPORT_HARVEST_SOURCES") or "scopus,openalex,orcid").split(",")
               if s.strip().lower() in SOURCE_ARG]
    sources = [s for s in SOURCES if s in sources]
    cap = int(os.getenv("IMPORT_HARVEST_MAX_NEW_PER_PERSON", "100"))
    run_cap = int(os.getenv("IMPORT_HARVEST_MAX_NEW_PER_RUN", "5000"))
    rate = float(os.getenv("IMPORT_HARVEST_MAX_FAILURE_RATE", "0.05"))
    guards = {
        "SCOPUS_AUID": SourceGuard("scopus", a.scopus_max_calls or int(os.getenv("SCOPUS_HARVEST_MAX_CALLS", "2000")), rate),
        "OPENALEX": SourceGuard("openalex", a.openalex_max_calls or int(os.getenv("IMPORT_HARVEST_OPENALEX_MAX_CALLS", "500")), rate),
        "ORCID": SourceGuard("orcid", int(os.getenv("IMPORT_HARVEST_ORCID_MAX_CALLS", "2000")), rate),
        "CROSSREF": SourceGuard("crossref", int(os.getenv("IMPORT_HARVEST_CROSSREF_MAX_CALLS", "1000")), rate),
        "PUBMED": SourceGuard("pubmed", a.pubmed_max_calls or int(os.getenv("IMPORT_HARVEST_PUBMED_MAX_CALLS", "6000")),
                              float(os.getenv("PUBMED_MAX_FAILURE_RATE", "0.02"))),
    }
    if "SCOPUS_AUID" in sources and not (os.getenv("SCOPUS_API_KEY") and os.getenv("SCOPUS_INST_TOKEN")):
        log("importHarvest: SCOPUS_API_KEY / SCOPUS_INST_TOKEN unset; Scopus source skipped")
        sources.remove("SCOPUS_AUID")
    stats_on = not a.no_stats
    cx = db()
    cur = cx.cursor()
    cur.execute("SELECT GET_LOCK('import_harvest', 0)")
    if cur.fetchone()[0] != 1:
        log("importHarvest: another run holds GET_LOCK('import_harvest'); exiting")
        return 0
    tables = {t: table_exists(cur, t) for t in ("external_record", "external_rejection", "import_harvest_run",
                                                "import_harvest_person", "doi_pmid_cache")}
    if stats_on and not (tables["import_harvest_run"] and tables["import_harvest_person"]):
        log("importHarvest: import_harvest_run / import_harvest_person missing (setup/table_person_author_id_"
            "import_harvest_v3.8.sql); running with --no-stats")
        stats_on = False
    log(f"importHarvest: mode=dry-run sources={','.join(sources)} stats={'on' if stats_on else 'off'} "
        f"external_record={'yes' if tables['external_record'] else 'absent (treated as empty)'} cap={cap}")

    explicit = {u.strip().lower() for u in a.uids.split(",") if u.strip()} if a.uids else None
    cohort_spec = None if explicit else (a.cohort or os.getenv("IMPORT_HARVEST_COHORT") or "all-confirmed")
    cohort = explicit or load_cohort(cohort_spec, cur)
    confirmed = load_confirmed_ids(cur, cohort)
    pool = set(cohort) if cohort is not None else set(confirmed)
    identity = {}
    try:
        if cohort is None and table_exists(cur, "scholar_active"):
            cur.execute("SELECT LOWER(cwid) FROM scholar_active WHERE has_confirmed_orcid = 1")
            pool |= {u for (u,) in cur.fetchall()}
        if cohort is None and table_exists(cur, "admin_orcid"):
            cur.execute("SELECT LOWER(personIdentifier) FROM admin_orcid")
            pool |= {u for (u,) in cur.fetchall()}
    except Exception as e:
        log(f"importHarvest: ORCID pool read failed: {e}")
    pool = {u for u in pool if "_" not in u}
    weekday = now.weekday()
    everyone = bool(a.all or explicit)
    due = sorted(u for u in pool if everyone or shard_of(u) == weekday)
    if a.limit:
        due = due[:a.limit]
    try:
        identity = read_identity(due) if due else {}
    except Exception as e:
        log(f"importHarvest: DynamoDB Identity read failed ({type(e).__name__}); no ORCID iDs, no Identity institutions")
    if a.orcid_tier == "any":
        orcids = {u: (v["orcid"], "ANY") for u, v in identity.items() if v.get("orcid")}
    else:
        orcids = orcid_tiers(cur, identity, set(due))
    afid_names = []
    try:
        import csv
        with open(os.path.join(HERE, "scopus_afids.csv"), newline="") as fh:
            afid_names = [(r["afid"].strip(), r.get("name")) for r in csv.DictReader(fh) if (r.get("afid") or "").strip()]
    except OSError as e:
        log(f"importHarvest: scopus_afids.csv unreadable ({e}); known set without the WCM AF-ID family")
    cache = {}
    if tables["doi_pmid_cache"]:
        cur.execute("SELECT doi, pmid FROM doi_pmid_cache WHERE checked_at > UTC_TIMESTAMP() - INTERVAL 30 DAY")
        cache = {d: p for d, p in cur.fetchall()}
    resolver = PmidResolver(cache, guards["PUBMED"])

    run_id = start_run(cur, sources, cohort_spec or "uids", None if everyone else weekday, now) if stats_on else None
    if stats_on:
        cx.commit()
    log(f"importHarvest: run_id={run_id} pool={len(pool)} weekday={weekday} due={len(due)} "
        f"with_confirmed_ids={sum(1 for u in due if u in confirmed)} with_orcid={sum(1 for u in due if u in orcids)} "
        f"orcid_tier={a.orcid_tier}")

    totals = {s: Counter() for s in SOURCES}
    per_visible = defaultdict(list)         # source -> [visible new_pending per (uid, source id)]
    stats, meta, notes = Counter(), {}, []
    status, deadline = "ok", (t0 + a.max_seconds) if a.max_seconds else None
    try:
        for k, uid in enumerate(due, 1):
            if deadline and time.time() > deadline:
                notes.append(f"stopped at the --max-seconds deadline after {k - 1}/{len(due)} people")
                status = "degraded"
                break
            person = load_person(cur, uid, tables["external_record"], tables["external_rejection"])
            resolver.local = person["dois"]
            person["known"] = known_affiliations(afid_names, person["accepted_affils"],
                                                 (identity.get(uid) or {}).get("institutions") or [])
            live, line = [], []
            batches = [(s, i) for s in ("SCOPUS_AUID", "OPENALEX_AUTHOR") if s in sources for i in confirmed[uid][s]]
            if "ORCID" in sources and uid in orcids:
                batches.append(("ORCID", orcids[uid][0]))
            for source, sid in batches:
                guard = guards["OPENALEX" if source == "OPENALEX_AUTHOR" else source]
                err, cands = None, None
                try:
                    if source == "SCOPUS_AUID":
                        cands = fetch_scopus_auid(guard, sid, person["surname"], meta)
                    elif source == "OPENALEX_AUTHOR":
                        cands = fetch_openalex_author(guard, sid, person["surname"])
                    else:
                        cands, moved = fetch_orcid(guard, sid)
                        if moved:
                            stats["orcid_deprecated_followed"] += 1
                            log(f"importHarvest: {uid} ORCID iD deprecated, primary record used")
                        enrich_orcid(cands, moved or sid, person["surname"], guards["OPENALEX"], guards["CROSSREF"], stats)
                except Budget as e:
                    err = f"skipped: {e}"
                except Exception as e:
                    err = str(e)[:300]
                if cands is None:
                    stats[f"{source}_skipped" if err.startswith("skipped") else f"{source}_failed"] += 1
                    if stats_on:
                        write_person(cur, run_id, uid, source, sid, Counter(), "skipped" if err.startswith("skipped") else "failed",
                                     False, [], err, now)
                    line.append(f"{source}={'skipped' if err.startswith('skipped') else 'failed'}")
                    continue
                n, rows, capped = process_batch(cands, person, live, resolver, cap)
                totals[source].update(n)
                totals[source]["visits"] += 1
                totals[source]["capped"] += int(capped)
                per_visible[source].append(n["new_pending"] - n["hidden_type"])
                if stats_on:
                    write_person(cur, run_id, uid, source, sid, n, "capped" if capped else "ok", capped, rows, None, now)
                line.append(f"{source}={n['fetched']}f/{n['new_pending']}p/{n['skipped_pubmed']}pm/{n['automerged']}am"
                            + ("/CAPPED" if capped else ""))
            if stats_on:
                write_cache(cur, resolver.new, now)
                resolver.new = {}
                cx.commit()
            log(f"importHarvest: {k}/{len(due)} {uid} " + (" ".join(line) or "no sources"))
    except Exception as e:
        status = "failed"
        notes.append(f"{type(e).__name__}: {str(e)[:300]}")
        log(f"importHarvest: run failed: {type(e).__name__}: {e}")

    pending = sum(t["new_pending"] for t in totals.values())
    if pending > run_cap:
        status = "degraded" if status == "ok" else status
        notes.append(f"new_pending {pending} > IMPORT_HARVEST_MAX_NEW_PER_RUN {run_cap}: an apply run would degrade to dry-run")
    for name, g in guards.items():
        if g.tripped:
            notes.append(f"{name} stopped: {g.tripped}")
            status = "degraded" if status == "ok" else status
    out = {s: dict(totals[s]) for s in sources}
    out["calls"] = {k: {"calls": g.calls, "failures": g.failures} for k, g in guards.items()}
    out["pmid_resolution"] = dict(resolver.hits)
    out["visible_new_pending"] = {s: dist(per_visible[s]) for s in sources}
    out["scopus_ratelimit"] = meta
    out["run"] = dict(stats, people=len(due), elapsed_seconds=round(time.time() - t0, 1))
    if stats_on:
        try:
            finish_run(cur, run_id, status, out, "; ".join(notes) or None)
            cx.commit()
        except Exception as e:
            log(f"importHarvest: could not finish run row: {e}")
    for s in sources:
        log(f"importHarvest: {s} {dict(sorted(totals[s].items()))} visible_new_pending={out['visible_new_pending'][s]}")
    log(f"importHarvest: calls {out['calls']} pmid_resolution={out['pmid_resolution']} scopus_ratelimit={meta}")
    log(f"importHarvest: run_id={run_id} status={status} people={len(due)} pending={pending} "
        f"automerged={sum(t['automerged'] for t in totals.values())} "
        f"skipped_pubmed={sum(t['skipped_pubmed'] for t in totals.values())} "
        f"capped={sum(t['capped'] for t in totals.values())} elapsed={out['run']['elapsed_seconds']}s"
        + (f" note={'; '.join(notes)}" if notes else ""))
    try:
        cur.execute("SELECT RELEASE_LOCK('import_harvest')")
    except Exception:
        pass
    cx.close()
    return 0


# ---- selftest (fixtures; no network, no DB) -----------------------------------------------
def selftest():
    # 1. keys shared with PM, against the vectors generated from PM's own code
    v = json.load(open(VECTORS))
    for x in v["vectors"]:
        assert norm_title(x["title"]) == x["norm"], x
        assert year_of(x["date"]) == x["year"], x
        assert title_year_key(x["title"], x["date"]) == x["key"], x
    for x in v["doi_vectors"]:
        assert norm_doi(x["doi"]) == x["norm"], x
    for x in v["rejection_key_vectors"]:
        assert [f"{k}={val}" for k, val in rejection_keys_of(x["record"])] == x["keys"], x

    # 2. rejection memory precedence (PM: different DOIs are different works, same title or not)
    ty = title_year_key("Same title", "2020")
    mem = [{"key_type": "DOI", "key_value": "10.1/a", "record_id": 1},
           {"key_type": "TITLE_YEAR", "key_value": ty, "record_id": 1}]
    assert not match_rejection_memory(rejection_keys_of({"doi": "10.1/b", "title": "Same title", "pub_date": "2020"}), mem)
    assert len(match_rejection_memory(rejection_keys_of({"doi": "https://doi.org/10.1/A"}), mem)) == 1
    assert len(match_rejection_memory(rejection_keys_of({"title": "Same  TITLE!", "pub_date": "2020-05"}), mem)) == 1

    # 3. normalizers on response-shaped fixtures
    sc_entry = lambda eid, doi, pmid, title, auths, typ="Article", date="2024-02-01": {
        "dc:identifier": f"SCOPUS_ID:{eid}", "eid": f"2-s2.0-{eid}", "dc:title": title, "prism:doi": doi,
        "pubmed-id": pmid, "prism:coverDate": date, "subtypeDescription": typ, "prism:publicationName": "J",
        "affiliation": [{"afid": "60007997", "affilname": "Weill Cornell Medicine"},
                        {"afid": "60000001", "affilname": "Elsewhere University"}],
        "author": [{"@seq": str(i), "authid": a, "surname": s, "given-name": "G", "authname": f"{s} G.",
                    "afid": [{"$": af}]} for i, (a, s, af) in enumerate(auths, 1)]}
    w = scopus_work(sc_entry("105000000001", "10.1/X", None, "Abstract one",
                             [("111", "Smith", "60000001"), ("36472973800", "Chiang", "60007997")]), "36472973800")
    assert w["article_id"] == "SCOPUS:105000000001" and w["scopus_eid"] == "105000000001" and w["doi"] == "10.1/x"
    assert w["target_author_index"] == 2 and w["slot_affiliations"] == [("scopus:60007997", "Weill Cornell Medicine")]
    assert w["pmid"] is None and scopus_work(sc_entry("2", None, "123", "T", []), "1")["pmid"] == "123"
    oa = {"id": "https://openalex.org/W9", "doi": "https://doi.org/10.2/Y", "ids": {"pmid": "https://pubmed.ncbi.nlm.nih.gov/555"},
          "title": "OA work", "type": "conference-abstract", "publication_date": "2023-05-01",
          "primary_location": {"source": {"display_name": "Blood"}},
          "authorships": [{"author": {"id": "https://openalex.org/A1", "display_name": "Gloria C. Chiang",
                                      "orcid": "https://orcid.org/0000-0001-9925-0816"},
                           "institutions": [{"id": "https://openalex.org/I1", "display_name": "Elsewhere U"}],
                           "raw_affiliation_strings": ["Dept of Radiology, Elsewhere U"]}]}
    o = openalex_work(oa, "A1")
    assert o["article_id"] == "OPENALEX:W9" and o["pmid"] == "555" and o["doi"] == "10.2/y" and o["target_author_index"] == 1
    orc = {"group": [
        {"external-ids": {"external-id": [{"external-id-type": "doi", "external-id-value": "10.3/Z", "external-id-relationship": "self"},
                                          {"external-id-type": "issn", "external-id-value": "1", "external-id-relationship": "part-of"}]},
         "work-summary": [
             {"put-code": 11, "source": {"source-client-id": {"path": "x"}, "source-name": {"value": "Scopus - Elsevier"}},
              "title": {"title": {"value": "Talk"}}, "type": "lecture-speech",
              "publication-date": {"year": {"value": "2022"}, "month": {"value": "3"}}},
             {"put-code": 12, "source": {"source-orcid": {"path": "0000-0001-9925-0816"}}}]},
        {"external-ids": {"external-id": []},
         "work-summary": [{"put-code": 13, "source": {"source-name": {"value": "Europe PubMed Central"}},
                           "title": {"title": {"value": "No DOI"}}, "type": "journal-article",
                           "publication-date": {"year": {"value": "2021"}}}]},
        {"external-ids": {"external-id": [{"external-id-type": "doi", "external-id-value": "10.4/w"}]},
         "work-summary": [{"put-code": 14, "source": {"source-name": {"value": "Crossref Metadata Search"}},
                           "title": {"title": {"value": "Crossref one"}}, "type": "journal-article"}]}]}
    ow = orcid_works(orc, "0000-0001-9925-0816")
    assert [x["article_id"] for x in ow] == ["ORCID:11", "ORCID:13", "ORCID:14"]
    assert ow[0]["provenance"] == "ORCID_SELF" and ow[0]["doi"] == "10.3/z" and ow[0]["pub_date"] == "2022-03"
    assert ow[0]["put_codes"] == ["11", "12"]
    assert (ow[1]["provenance"], ow[1]["source_detail"]) == ("ORCID_OTHER", "Europe PubMed Central")
    assert ow[2]["provenance"] == "ORCID_CROSSREF"
    enrich_from_openalex(ow[0], oa, "0000-0001-9925-0816", "Chiang")
    assert ow[0]["target_author_index"] == 1 and ow[0]["pmid"] == "555" and ow[0]["openalex_id"] == "W9"
    cr = enrich_from_crossref(dict(ow[2]), {"DOI": "10.4/w", "author": [{"given": "A", "family": "Lee"},
                                                                         {"given": "G", "family": "Chiang", "affiliation": [{"name": "Weill Cornell Medicine"}]}]},
                              "0000-0001-9925-0816", "Chiang")
    assert cr["target_author_index"] == 2 and cr["slot_affiliations"] == [(None, "Weill Cornell Medicine")]

    # 4. ORCID deprecated iD: a 301 to the primary record (final URL) or a 409 naming it
    assert orcid_redirect_target("0000-0002-0000-0001", "https://pub.orcid.org/v3.0/0000-0002-0000-0009/works", 200) == "0000-0002-0000-0009"
    assert orcid_redirect_target("0000-0002-0000-0001", "https://pub.orcid.org/v3.0/0000-0002-0000-0001/works", 200) is None
    assert orcid_redirect_target("0000-0002-0000-0001", "u", 409, "0000-0002-0000-0001 is deprecated; primary 0000-0003-1111-222X") == "0000-0003-1111-222X"
    assert orcid_redirect_target("0000-0002-0000-0001", "u", 500, "0000-0003-1111-222X") is None

    # 5. affiliation flag
    known = known_affiliations([("60007997", "Weill Cornell Medicine")], [("60000002", "NewYork-Presbyterian Hospital")],
                               ["Princeton University", "MSKCC"])
    assert affiliation_flag([("scopus:60007997", "x")], known) == "KNOWN"
    assert affiliation_flag([(None, "Department of Radiology, Weill Cornell Medicine, New York, NY")], known) == "KNOWN"
    assert affiliation_flag([("openalex:I4387153466", None)], known) == "KNOWN"
    assert affiliation_flag([(None, "Dept of Radiology, Elsewhere U")], known) == "UNFAMILIAR"
    assert affiliation_flag([(None, "MSKCC")], known) == "KNOWN"                 # short names: equality only
    assert affiliation_flag([(None, "MSKCC Brooklyn Annex")], known) == "UNFAMILIAR"
    assert affiliation_flag([], known) == "NONE"

    # 6. the pipeline: Chiang's split profile (two CONFIRMED AU-IDs), a PMID-bearing work, a
    #    rejection-memory hit, an existing source record, AUTOMERGED matches, out of scope,
    #    a hidden talk, and a capped person
    person = {"records": [{"id": 7, "article_id": "SCOPUS:105000000009", "status": "ACCEPTED", "doi": "10.9/acc",
                           "scopus_eid": "105000000009", "openalex_id": None, "orcid_put_code": None,
                           "title_year_key": None, "title": "Accepted abstract", "pub_date": "2020"}],
              "rejections": [{"key_type": "DOI", "key_value": "10.5/rej", "record_id": 3}],
              "assertions": {42: "ACCEPTED", 43: "REJECTED", 44: ""}, "known": known,
              "accepted_titles": [("Imaging of amyloid in aging brains", "2024")]}
    resolved = {"10.6/inpubmed": 43, "10.6/notmine": 99}
    chiang = lambda eid, doi, title, typ="Article", pmid=None, date="2024-02-01", au="36472973800": sc_entry(
        eid, doi, pmid, title, [("111", "Smith", "60000001"), (au, "Chiang", "60007997")], typ, date)
    main_id = [scopus_work(e, "36472973800") for e in [
        chiang("1", "10.1/new", "A new abstract"),
        chiang("2", None, "PubMed paper", pmid="42"),               # own PMID: accepted
        chiang("3", "10.6/inpubmed", "Resolves by DOI"),           # DOI -> PMID 43: rejected
        chiang("4", "10.6/notmine", "Not retrieved"),              # DOI -> PMID 99: not retrieved
        chiang("5", "10.5/REJ", "Rejected before"),                # rejection memory
        chiang("105000000009", "10.9/acc", "Accepted abstract"),   # same (uid, article_id): existing
        chiang("6", "10.9/ACC", "Accepted abstract (Scopus dup)"), # DOI match: automerged
        chiang("7", None, "Erratum to X", typ="Erratum"),          # hidden, still pending
        chiang("8", None, "Imaging of amyloid in aging brains.", date="2024-06-01"),   # possible duplicate
    ]]
    split_id = [scopus_work(chiang("10", None, "A new abstract", au="56663200300"), "56663200300"),  # title+year: automerged
                scopus_work(chiang("11", "10.1/other", "Chaing misspelt byline", au="56663200300"), "56663200300")]
    live = []
    res = lambda d: resolved.get(norm_doi(d))
    n, rows, capped = process_batch(main_id, person, live, res, cap=100)
    assert not capped
    assert n["fetched"] == 9 and n["skipped_pubmed"] == 3, n
    assert (n["skipped_pubmed_accepted"], n["skipped_pubmed_rejected"], n["skipped_pubmed_not_retrieved"]) == (1, 1, 1), n
    assert n["skipped_rejected"] == 1 and n["existing"] == 1 and n["automerged"] == 1, n
    assert n["new_pending"] == 3 and n["hidden_type"] == 1 and n["possible_dup"] == 1, n
    assert n["affil_known"] == 3, n
    assert [r["article_id"] for r in rows] == ["SCOPUS:1", "SCOPUS:7", "SCOPUS:8"], rows
    n2, rows2, _ = process_batch(split_id, person, live, res, cap=100)
    assert n2["automerged"] == 1 and n2["new_pending"] == 1 and rows2[0]["article_id"] == "SCOPUS:11", (n2, rows2)
    # OpenAlex then ORCID on the same works: crosswalked onto tonight's Scopus rows
    oa_same = openalex_work(dict(oa, doi="https://doi.org/10.1/NEW", ids={}, type="article"), "A1")
    oa_out = openalex_work(dict(oa, id="W10", doi=None, ids={}, type="paratext"), "A1")
    n3, _, _ = process_batch([oa_same, oa_out], person, live, res, cap=100)
    assert n3["automerged"] == 1 and n3["skipped_scope"] == 1 and n3["new_pending"] == 0, n3
    talk = orcid_works(orc, "0000-0001-9925-0816")[:1]
    n4, rows4, _ = process_batch(talk, person, live, res, cap=100)
    assert n4["new_pending"] == 1 and n4["hidden_type"] == 1 and rows4[0]["provenance"] == "ORCID_SELF", n4
    # capped: nothing would be written and nothing seeds later automerges
    many = [scopus_work(chiang(str(1000 + i), f"10.7/{i}", f"Work number {i}"), "36472973800") for i in range(5)]
    before = len(live)
    n5, _, capped5 = process_batch(many, person, live, res, cap=4)
    assert capped5 and n5["new_pending"] == 5 and len(live) == before
    n6, _, capped6 = process_batch(many[:1], person, live, res, cap=4)
    assert not capped6 and n6["new_pending"] == 1      # not automerged onto a capped batch

    # 7. the breaker
    g = SourceGuard("t", 100, 0.05)
    assert g.ok()
    g.calls, g.failures = 20, 2
    assert g.ok()
    g.failures = 3
    assert not g.ok() and "failure rate" in g.tripped
    g = SourceGuard("t", 2, 0.05)
    g.calls = 2
    assert not g.ok() and "budget" in g.tripped
    assert dist([0, 1, 2, 10])["median"] == 1.5 and dist([])["n"] == 0
    assert pubmed_bucket("42", {42: "ACCEPTED"}) == "skipped_pubmed_accepted"
    assert pubmed_bucket(44, {44: ""}) == "skipped_pubmed_candidate"
    print("ok")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", help="default: fetch and measure, write stats tables only")
    g.add_argument("--apply", action="store_true", help="REFUSED in this release (plan PR H3)")
    g.add_argument("--selftest", action="store_true")
    ap.add_argument("--uids", help="comma-separated uids (ignores the weekday shard)")
    ap.add_argument("--cohort", help="s3://bucket/key, a local file, or all-confirmed (default)")
    ap.add_argument("--all", action="store_true", help="ignore the weekday shard")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--sources", help="comma list of scopus,openalex,orcid")
    ap.add_argument("--orcid-tier", choices=("trusted", "any"), default="trusted",
                    help="any = every Identity iD (dry-run measurement only)")
    ap.add_argument("--no-stats", action="store_true", help="write nothing, not even the stats tables")
    ap.add_argument("--max-seconds", type=int, default=0, help="stop starting people after this many seconds")
    ap.add_argument("--openalex-max-calls", type=int)
    ap.add_argument("--scopus-max-calls", type=int)
    ap.add_argument("--pubmed-max-calls", type=int)
    a = ap.parse_args()
    if a.selftest:
        return selftest()
    if a.apply:
        log("importHarvest: REFUSING --apply: this release is dry-run only. The write path, the 3b "
            "cutover check (external_record present and EXTERNAL_DYNAMO_SYNC=off) and --rollback-run "
            "ship in plan PR H3. Set IMPORT_HARVEST=dry-run.")
        return 2
    return run(a)


if __name__ == "__main__":
    sys.exit(main())
