#!/usr/bin/env python3
"""
Replay ONE authorship_review row through today's matcher and print EVERY candidate.

WHY THIS EXISTS. "The right person is missing" reports need two things the queue never
stores: the candidates below the fifth, and why somebody was no candidate at all. No
other tool takes an author_key, prints the whole list or handles OpenAlex and the Cornell
Ithaca Scopus lane.

HOW. The row's document is re-fetched from its source (PubMed, Scopus, OpenAlex), the
author at the row's position is parsed by the lane's own parser, and the lane's own
matcher call ranks them, once with the top-5 cut the producer uses and once without it.
Nothing about matching is re-implemented here. The row's STORED byline_orcid is used, as
aar_report_changed_picks does (BYLINE ORCID there), and its guards are called first.

`--person CWID` explains that person. A rule is named only when it can be SEEN: the
person is on no roster, the byline's campus was not theirs, the byline surname reaches no
record of theirs, or calling the matcher again with only the given name changed brings
them back. Anything else is reported as "not determinable"; the matcher discards its
reasons and this does not guess them.

A REPLAY IS TODAY'S ANSWER. The rosters, the S3 scoring inputs and the upstream records
have no as-of view, so a difference from what the row stores says that data, code or
switches changed, not which.

SWITCHES. The matcher's AAR_* switches live only on the CronJob, and a replay run with
other values reports differences that are not there. --cronjob-env FILE is the CronJob's
environment as KEY=VALUE lines. Every AAR_* variable must be the same there and in this
process, unset counting as a difference, or the script stops before it loads anything.
Other lines in FILE are ignored and never printed.

NO WRITES. Every database connection this process opens, through any module's engine, is
put in a read-only session transaction and checked before use (`_read_only`), so a write
is refused by the server whatever the code does. DynamoDB, S3 and the three APIs are only
read; that half is by inspection, not enforced. Run it with the read-only login of
setup/grants_authorship_feedback_reader_v3.20.sql.template.

Output: one JSON object on stdout; progress goes to stderr. `status` is
  ok                  replayed
  ok_with_warnings    replayed, but an input degraded (see `warnings`)
  unresolvable        the row cannot be replayed (see `reason`); exit 3
A refusal (switches, missing inputs, read-only not in force) is one line on stderr, exit 2.

Env (never printed): DB_USERNAME/DB_PASSWORD/DB_HOST/DB_NAME; AWS credentials (DynamoDB
Identity, S3 scoring inputs); SCOPUS_API_KEY + SCOPUS_INST_TOKEN for a Scopus row.

Usage:
  python3 aar_replay_row.py --cronjob-env FILE AUTHOR_KEY
  python3 aar_replay_row.py --cronjob-env FILE AUTHOR_KEY --person CWID
"""
import argparse, contextlib, io, json, logging, os, sys, threading
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import event, text
from sqlalchemy.engine import Engine

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)      # this directory wins (sys.path fork trap, aar_report_changed_picks)

NOT_DETERMINABLE = "not determinable"
PINNED_XGBOOST = "3.2.0"      # requirements.txt; another version drifts the PubMed io scores


class Refused(Exception):
    """The replay would not be faithful, or not read-only. Nothing was ranked."""


class Unresolvable(Exception):
    """The row's author could not be recovered from its source."""


# ---- switches -------------------------------------------------------------------
def read_switches(path):
    """The AAR_* lines of a KEY=VALUE file. Every other line is dropped unread."""
    out = {}
    with open(path) as fh:
        for line in fh:
            key, eq, value = line.strip().partition("=")
            key = key.strip().removeprefix("export ").strip()
            if eq and key.startswith("AAR_"):
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
                    value = value[1:-1]
                out[key] = value
    return out


def switch_differences(cronjob, env):
    """One line per AAR_* variable that is not the same in both, [] when all are."""
    here = {k: v for k, v in env.items() if k.startswith("AAR_")}
    return [f"{k}: CronJob {cronjob.get(k, '(unset)')!r}, here {here.get(k, '(unset)')!r}"
            for k in sorted(set(cronjob) | set(here)) if cronjob.get(k) != here.get(k)]


# ---- read-only --------------------------------------------------------------------
def _read_only(dbapi_conn, _record=None):
    """Put a new connection in a read-only session transaction, or refuse to use it."""
    cur = dbapi_conn.cursor()
    try:
        cur.execute("SET SESSION TRANSACTION READ ONLY")
        # SET SESSION applies from the NEXT transaction: end whatever is open.
        dbapi_conn.rollback()
        cur.execute("SHOW SESSION VARIABLES WHERE Variable_name IN "
                    "('tx_read_only', 'transaction_read_only')")
        values = [str(r[1]).upper() for r in cur.fetchall()]
    finally:
        cur.close()
    if not values or any(v not in ("ON", "1") for v in values):
        raise Refused("the database session is not read-only; nothing was read")


def force_read_only():
    """`_read_only` on every connection of every engine in this process. On the Engine
    CLASS, because aar_db, aar_gate and IdentityIndex.load each create their own."""
    if not event.contains(Engine, "connect", _read_only):
        event.listen(Engine, "connect", _read_only)


# ---- the row and its author ---------------------------------------------------------
def load_row(author_key):
    import aar_db
    import aar_report_changed_picks as rcp
    with aar_db.engine().connect() as c:
        cols = ", ".join(aar_db.present_cols(c, rcp._SELECT_COLS + ["last_refreshed"]))
        row = c.execute(text(f"SELECT {cols} FROM authorship_review WHERE author_key = :k"),
                        {"k": author_key}).mappings().first()
    if row is None:
        raise Unresolvable("no authorship_review row has this author_key")
    return dict(row)


def _scopus_author(row):
    import aar_universe_scopus as scop
    doi, ext = row["doi"], row["external_id"]
    query = f"DOI({doi})" if doi else f"EID(2-s2.0-{ext})" if ext else None
    if not query:
        raise Unresolvable("the row has neither a DOI nor a Scopus id")
    entries = scop._scopus_get(query, 0).get("search-results", {}).get("entry") or []
    if not entries or "error" in entries[0]:
        raise Unresolvable("Scopus no longer returns this document")
    wcm, ithaca = scop.load_family_afids(), scop.load_family_afids(scop.ITHACA_AFID_LIST)
    # The lane is not stored. The two sweeps decide it by afid, WCM first
    # (wcm_authorships yield_to), so the same two calls recover it.
    for lane, family, yield_to in (("scopus", wcm, frozenset()), ("scopus-ithaca", ithaca, wcm)):
        for i, _n, au, _hits in scop.wcm_authorships(entries[0], family, yield_to):
            if i == row["author_position"] - 1:
                return lane, au, scop._pub_year(entries[0])
    raise Unresolvable("the author at this position carries no WCM or Ithaca afid in Scopus now")


def _openalex_author(row):
    import aar_universe_openalex as oa
    import aar_universe_scopus as scop
    doi, wid = row["doi"], row["external_id"]
    works = []
    # DOI first, as author_key is: OpenAlex merges works and the losing W id redirects.
    for flt in ([f"doi:{doi.lower()}"] if doi and not scop._DOI_UNSAFE.search(doi) else []) \
            + ([f"ids.openalex:{wid}"] if wid else []):
        works = oa._openalex_get({"filter": flt, "select": oa.SELECT,
                                  "per-page": 25}).get("results") or []
        if works:
            break
    if not works:
        raise Unresolvable("OpenAlex no longer returns this work")
    work = next((w for w in works if oa._work_id(w) == wid), works[0])
    for i, _n, au in oa.wcm_authorships(work):
        if i == row["author_position"] - 1:
            return "openalex", au, work.get("publication_year")
    raise Unresolvable("the author at this position carries no WCM institution in OpenAlex now")


def _pubmed_author(row):
    import aar_universe as uni
    arts = uni.efetch_by_ids([row["pmid"]], uni.load_home_institution_groups())
    if not arts:
        raise Unresolvable("PubMed no longer returns this record")
    authors = arts[0]["authors"]
    if not 1 <= row["author_position"] <= len(authors):
        raise Unresolvable("the record has fewer authors than the row's position")
    # The stored ORCID, never the re-fetch's own: match_authorship reads it off the block.
    return ("pubmed", dict(authors[row["author_position"] - 1],
                           byline_orcid=row.get("byline_orcid")), arts[0].get("pub_year"))


def fetch_author(row):
    """(lane, author block as the lane parses it, publication year)."""
    import aar_universe_scopus as scop
    if not row["author_position"]:
        raise Unresolvable("the row has no author position")
    lane, au, pub_year = {"pubmed": _pubmed_author, "scopus": _scopus_author,
                          "openalex": _openalex_author}[row["source"]](row)
    now = scop._trunc(scop._byline_name(au), 255)
    if now != row["wcm_author"]:
        # backfill_byline_orcid.pubmed_fills' rule. Ranking whoever sits there now would
        # answer about somebody else.
        raise Unresolvable(f"byline moved: the row says {row['wcm_author']!r}, the author at "
                           f"position {row['author_position']} is now {now!r}")
    return lane, au, pub_year


# ---- ranking ----------------------------------------------------------------------
def rank(row, lane, au, pub_year, roster, io_scorer=None):
    """(all candidates, the producer's own list of at most five, campuses consulted)."""
    import aar_report_changed_picks as rcp
    import aar_universe_scopus as scop
    from identity_index import CAMPUS_ITHACA, byline_campuses
    idx = roster.ithaca if lane == "scopus-ithaca" else roster
    rcp._replay_orcid_guard([row], idx, lane)
    if lane == "pubmed":
        import aar_matcher as matcher
        if io_scorer:
            everyone, _n = roster.candidates(au["last"], au["fore"], au["initials"],
                                             au["affiliations"], None, pub_year,
                                             **rcp._orcid_kw(row))
            with ThreadPoolExecutor(max_workers=16) as ex:       # as _replay_pubmed warms it
                list(ex.map(io_scorer.scores,
                            sorted({c["cwid"] for c in everyone if not c.get("campus")})))

        def call(top_k):
            return matcher.match_authorship(au, row["pmid"], roster, io_scorer, top_k=top_k,
                                            pub_year=pub_year)
        campuses = byline_campuses(au["affiliations"])
    else:
        # frozenset(): the OpenAlex lane has no Scopus afids (aar_universe_openalex.run).
        family = (frozenset() if lane == "openalex"
                  else scop.load_family_afids(scop.ITHACA_AFID_LIST))
        subfield = rcp._venue_kw(rcp._replay_venue_lookup([row], idx), row).get("venue_subfield")

        def call(top_k):
            return scop.wcm_lane_candidates(idx, au, pub_year, family, subfield,
                                            row.get("byline_orcid"), top_k=top_k)[0]
        campuses = ({CAMPUS_ITHACA} if lane == "scopus-ithaca" else scop._campuses(au, family))
    return call(None), call(5), campuses


def _same(a, b):
    return str(a or "").strip().lower() == str(b or "").strip().lower()


def _position(cands, cwid):
    return next((k for k, c in enumerate(cands, 1) if _same(c["cwid"], cwid)), None)


def explain(person, au, pub_year, roster, lane, campuses, everyone, five):
    """Why `person` is, or is not, among the candidates. Every rule named here was
    observed on the roster or through the matcher's own answer; see the module docstring."""
    from identity_index import CAMPUS_ITHACA, CAMPUS_WCM, _norm
    pos = _position(everyone, person)
    if pos:
        kept = _position(five, person)
        return {"cwid": person, "candidate": True, "position_in_all": pos,
                "position_in_producer_list": kept,
                "rules": [] if kept else [{"rule": "top_5_cut", "detail":
                          "matched, but outside the five the producer keeps"}]}
    last, fore = au.get("last"), au.get("fore")
    rules, found = [], False
    for campus, idx in ((CAMPUS_WCM, roster.wcm), (CAMPUS_ITHACA, roster.ithaca)):
        recs = [r for pool in idx.by_surname.values() for r in pool if _same(r["cwid"], person)]
        if not recs:
            continue
        found = True

        def on_this_roster(last_, fore_, initials):
            return _position(idx.candidates(last_, fore_, initials, au.get("affiliations"),
                                            None, pub_year)[0], person)
        pool = [r for r in idx.by_surname.get(_norm(last), []) if _same(r["cwid"], person)]
        reached = on_this_roster(last, fore, au.get("initials"))
        # On an Ithaca-only byline a CampusRoster still keeps the WCM people with a Cornell
        # appointment; the Ithaca lane's bare index never sees the WCM roster.
        linked = lane != "scopus-ithaca" and any(_same(c, person) for c in idx.cornell_linked)
        if campus not in campuses and not linked:
            rules.append({"rule": "campus_not_consulted", "detail":
                          f"on the {campus} roster; this byline was matched against "
                          f"{', '.join(sorted(campuses))}. The {campus} roster alone would "
                          f"{'' if reached else 'not '}have matched them"})
        if reached:
            continue
        if not pool:
            rules.append({"rule": "surname_not_indexed", "detail":
                          f"no record of theirs is under the byline surname {last!r}; theirs: "
                          + ", ".join(sorted({repr(r['surname']) for r in recs}))})
        elif on_this_roster(last, None, None):
            rules.append({"rule": "given_name", "detail":
                          f"the surname reaches them and the byline given name "
                          f"{fore or au.get('initials')!r} removes them; on record: "
                          + ", ".join(sorted({repr(' '.join(x for x in (
                              r['given'], r['middle']) if x)) for r in pool}))})
        elif all(r.get("alt_name") for r in pool) and any(
                on_this_roster(r["surname"], r["given"], None) for r in pool):
            rules.append({"rule": "alternate_name_needs_full_match", "detail":
                          "the surname reaches them only through an alternate name, which "
                          "counts only when the byline spells it out: "
                          + ", ".join(sorted({repr(r['given'] + ' ' + r['surname'])
                                              for r in pool}))})
    if not found:
        rules.append({"rule": "not_on_roster", "detail": "neither the WCM nor the Cornell "
                      "Ithaca roster (reciterdb identity) holds this cwid"})
    return {"cwid": person, "candidate": False,
            "rules": rules or [{"rule": NOT_DETERMINABLE}]}


def replay(row, roster, io_scorer=None, person=None):
    """The result object for one row, warnings aside."""
    lane, au, pub_year = fetch_author(row)
    everyone, five, campuses = rank(row, lane, au, pub_year, roster, io_scorer)
    try:
        stored = [c.get("cwid") for c in json.loads(row.get("candidate_cwids_json") or "[]")]
    except (ValueError, AttributeError):
        stored = None
    out = {"status": "ok", "author_key": row["author_key"], "lane": lane,
           "row": {k: row.get(k) for k in ("id", "status", "wcm_author", "author_position",
                                           "byline_orcid", "last_refreshed")},
           "author": {"last": au["last"], "fore": au["fore"], "initials": au["initials"],
                      "affiliations": au["affiliations"], "pub_year": pub_year,
                      "home_inst": au.get("home_inst")},      # PubMed only: would be swept today
           "campuses_consulted": sorted(campuses),
           "stored_top_cwid": row.get("top_cwid"), "stored_cwids": stored,
           "producer_cwids_today": [c["cwid"] for c in five],
           "n_candidates_all": len(everyone), "candidates_all": everyone}
    if person:
        out["person"] = explain(person, au, pub_year, roster, lane, campuses, everyone, five)
    return out


# ---- CLI ----------------------------------------------------------------------------
class _Warnings(logging.Handler):
    """identity_index degrades by logging and carrying on; here that must be seen."""
    def __init__(self):
        super().__init__(logging.WARNING)
        self.seen = []

    def emit(self, record):
        self.seen.append(record.getMessage())


def _io_scorer(warnings):
    """The producer's scorer. It reads a failed S3 fetch as "never scored", so failures
    are collected, on both of its inputs. A missing or archived input is not one: the
    producer cannot read it either."""
    import xgboost
    import aar_matcher as matcher
    det = matcher.det
    if xgboost.__version__ != PINNED_XGBOOST:
        warnings.append(f"xgboost {xgboost.__version__} here, {PINNED_XGBOOST} in production: "
                        "io scores can differ")
    score_user, get_object, score = det.score_user, det._s3.get_object, det._score
    here = threading.local()       # the cwid whose identity-only input this thread is on

    def recorded(uid):
        res = score_user(uid)
        if res[0] == "error" and "InvalidObjectState" not in str(res[2]):
            warnings.append(f"identity-only score unavailable for {uid}: "
                            f"{str(res[2][:1])[:120]}")
        return res

    def failed(stage, e):
        warnings.append(f"identity-only score unavailable for {here.cwid}: "
                        f"{stage}: {type(e).__name__}: {str(e)[:90]}")

    # _identity_only_input swallows every exception, so each step is watched from inside.
    def fetched(**kw):
        if not getattr(here, "cwid", None):
            return get_object(**kw)
        try:
            obj = get_object(**kw)
            body = obj["Body"].read()
            json.loads(body)
            return dict(obj, Body=io.BytesIO(body))
        except det._s3.exceptions.NoSuchKey:
            raise
        except Exception as e:      # noqa: BLE001  (re-raised)
            if "InvalidObjectState" not in str(e):
                failed("fetch", e)
            raise

    def scored(*a, **kw):
        try:
            return score(*a, **kw)
        except Exception as e:      # noqa: BLE001  (re-raised)
            if getattr(here, "cwid", None):
                failed("score", e)
            raise

    class Scorer(matcher.IdentityOnlyScorer):
        def _identity_only_input(self, cwid):
            here.cwid = cwid
            try:
                return super()._identity_only_input(cwid)
            finally:
                here.cwid = None

    det.score_user, det._s3.get_object, det._score = recorded, fetched, scored
    return Scorer()


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("author_key")
    ap.add_argument("--cronjob-env", required=True, metavar="FILE",
                    help="the CronJob's environment, KEY=VALUE per line")
    ap.add_argument("--person", metavar="CWID", help="say why this person is or is not listed")
    args = ap.parse_args()

    seen = _Warnings()
    try:
        cronjob = read_switches(args.cronjob_env)
        if not cronjob:
            raise Refused(f"{args.cronjob_env} has no AAR_* line; is it the CronJob's environment?")
        diffs = switch_differences(cronjob, os.environ)
        if diffs:
            raise Refused("AAR_* switches differ from the CronJob's:\n  " + "\n  ".join(diffs))
        force_read_only()
        with contextlib.redirect_stdout(sys.stderr):      # the lanes print their progress
            import requests
            from identity_index import CampusRoster, load_alternate_names
            logging.getLogger("identity_index").addHandler(seen)
            try:
                row = load_row(args.author_key)
                names = load_alternate_names()
                if not names:
                    raise Refused("DynamoDB alternate names could not be read: people known by "
                                  "another name would look excluded")
                roster = CampusRoster.load(alternate_names=names)
                scorer = _io_scorer(seen.seen) if row["source"] == "pubmed" else None
                out = replay(row, roster, scorer, args.person)
            except Unresolvable as e:
                out = {"status": "unresolvable", "author_key": args.author_key, "reason": str(e)}
            except requests.RequestException as e:
                out = {"status": "unresolvable", "author_key": args.author_key,
                       "reason": f"source fetch failed: {type(e).__name__}"}
    except (Refused, RuntimeError, OSError) as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        sys.exit(2)
    if seen.seen:
        out["warnings"] = seen.seen
        if out["status"] == "ok":
            out["status"] = "ok_with_warnings"
    print(json.dumps(out, indent=2, default=str))
    sys.exit(3 if out["status"] == "unresolvable" else 0)


if __name__ == "__main__":
    main()
