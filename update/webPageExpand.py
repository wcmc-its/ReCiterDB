#!/usr/bin/env python3
"""
Cornell Ithaca web-page seeds, step 7a: seed-scored OpenAlex-by-ORCID expansion.

For each person with live seeds and a trusted ORCID (the Affiliation Manager map, else a live
web-page inference from webPageOrcid.py), pull every OpenAlex work under the ORCID, keep the
publication types faculty list, and score each non-seed work against the person's seeds with
rule R6 (measurement M4, ithaca-web-page-seed-probe-2026-11-19/m4: 0 false accepts in 52
labeled works, 77% recall):

  auto-accept  <=>  the person's authorship name has the roster surname + first initial
               AND it shows no middle initial the seed authorships never show
               AND (it shares an OpenAlex coauthor with a seed work
                    OR it carries an institution from the person's own seed authorships
                       and a field the seeds cover)

A profile where under PROFILE_GATE of the scored works pass R6 is held whole (all pending):
that is what an over-merged common-name profile looks like.

Writes, IN PLACE in cornell_pub_researcher_publication (Paul, 2026-11-21), promotions only:
  * a work already linked as openalex_orcid + pending that passes R6 -> auto_accepted;
  * a work not linked yet -> a new openalex_orcid row, auto_accepted (R6) or pending;
  * any pending row for a (person, publication) that a web_seed row confirms -> auto_accepted.
Every row this pass writes or changes has reviewed_by = 'web_seeds_r6' / 'web_seed_confirm',
reviewed_at, and its features in review_notes, so it can be found and reverted.

Run by webPageSeeds.py after the cornell_pub pass when WEB_SEEDS_EXPAND=on, or alone:
  python3 webPageExpand.py [--dry-run] [--netid X]
"""

import argparse
import collections
import datetime
import json
import logging
import re
import sys
import time
import unicodedata
import urllib.parse
import urllib.request

import webPageCitations as wc
import webPageCornellPub as cp
import webPageOrcid as wo

log = logging.getLogger("webPageExpand")

RULE = "R6-v1"
KEEP_TYPES = {"article", "review", "book", "book-chapter", "conference-paper"}
PENDING_ONLY_TYPES = {"editorial", "letter"}
PROFILE_GATE = 0.30  # ponytail: unvalidated beyond the M4 probe; logged per person to tune
CONF_ACCEPT, CONF_PENDING = 90.0, 70.0  # 70 = the Feb retrieval's CONFIDENCE_OPENALEX_BY_ORCID
SELECT = "id,doi,title,publication_year,type,primary_location,ids,cited_by_count,authorships,primary_topic"
SUFFIXES = {"jr", "sr", "ii", "iii", "iv"}


# ---------------------------------------------------------------- features (ported from M4)

def norm_doi(d):
    """OpenAlex 'https://doi.org/10.X/Y' -> '10.x/y'; None for no DOI. Pure."""
    d = re.sub(r"^https?://(dx\.)?doi\.org/", "", (d or "").strip().lower())
    return d or None


def toks(s):
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return [t for t in re.split(r"[^a-z]+", s) if t]


def name_match(raw, first, last):
    """Surname token present (or the joined compound surname) + a shared first initial."""
    rt, lt = toks(raw), [t for t in toks(last) if len(t) > 1]
    if not rt or not lt:
        return False
    if not any(t in rt for t in lt) and "".join(toks(last)) not in "".join(rt):
        return False
    others = list(rt)
    for t in lt:
        if t in others:
            others.remove(t)  # once, so "Li Li" keeps its given-name token
    fi = {t[0] for t in toks(first)}
    return bool(fi & {t[0] for t in others}) if fi else True


def given_initials(raw, last):
    rt = [t for t in toks(raw) if t not in SUFFIXES]
    for t in [t for t in toks(last) if len(t) > 1]:
        if t in rt:
            rt.remove(t)
    return [t[0] for t in rt]


def middle_conflict(raw, seed_givens, last):
    """True if this authorship shows a middle initial never seen on the seed authorships."""
    g = given_initials(raw, last)
    seen = {x[1] for x in seed_givens if len(x) >= 2}
    return len(g) >= 2 and bool(seen) and g[1] not in seen and g[1] != g[0]


def own_authorship(work, orcid):
    for a in work.get("authorships") or []:
        if ((a.get("author") or {}).get("orcid") or "").endswith(orcid):
            return a
    return None


def _aid(a):
    return (a.get("author") or {}).get("id")


def _field(w):
    return ((w.get("primary_topic") or {}).get("field") or {}).get("id")


def seed_profile(seed_works, orcid, first, last):
    """What the seeds say about the person: coauthor ids, own author ids, institutions,
    fields, given-name initials. Pure."""
    p = {"coauthors": set(), "aids": set(), "insts": set(), "fields": set(), "givens": []}
    for w in seed_works:
        me = own_authorship(w, orcid)
        if me is None:
            cands = [a for a in w.get("authorships") or []
                     if name_match(a.get("raw_author_name") or (a.get("author") or {}).get("display_name"), first, last)]
            me = cands[0] if len(cands) == 1 else None
        if me is None:
            continue
        p["aids"].add(_aid(me))
        p["givens"].append(given_initials(me.get("raw_author_name") or "", last))
        p["coauthors"].update(_aid(a) for a in w.get("authorships") or [] if a is not me and _aid(a))
        if own_authorship(w, orcid):  # institutions only from authorships that carry the ORCID
            p["insts"].update(i.get("id") for i in me.get("institutions") or [] if i.get("id"))
        if _field(w):
            p["fields"].add(_field(w))
    p["coauthors"] -= p["aids"]
    return p


def score(work, orcid, first, last, prof):
    """(passes R6, features) for one non-seed work. Pure."""
    me = own_authorship(work, orcid)
    raw = (me or {}).get("raw_author_name") or ""
    co = {_aid(a) for a in work.get("authorships") or [] if a is not me}
    inst = {i.get("id") for i in (me or {}).get("institutions") or []}
    f = {"coauthor": bool(co & prof["coauthors"]), "inst_seed": bool(inst & prof["insts"]),
         "field": bool(_field(work) and _field(work) in prof["fields"]),
         "name": name_match(raw, first, last), "midconf": middle_conflict(raw, prof["givens"], last)}
    return (f["coauthor"] or (f["inst_seed"] and f["field"])) and f["name"] and not f["midconf"], f


def plan_person(works, seed_dois, orcid, first, last, seed_extra=()):
    """-> (decisions [(work, 'accept'|'pending', features)], stats). Seeds themselves are
    skipped (webPageCornellPub links them). Pure."""
    by_doi = {norm_doi(w.get("doi")) or None: w for w in works}
    seed_works = [w for d, w in by_doi.items() if d in seed_dois] + list(seed_extra)
    prof = seed_profile(seed_works, orcid, first, last)
    out, st = [], collections.Counter()
    for w in works:
        d = norm_doi(w.get("doi")) or None
        t = w.get("type")
        if d in seed_dois:
            st["seed"] += 1
            continue
        if t not in KEEP_TYPES and t not in PENDING_ONLY_TYPES:
            st["type_dropped"] += 1
            continue
        ok, f = score(w, orcid, first, last, prof)
        out.append([w, "accept" if ok and t in KEEP_TYPES else "pending", f])
    passed = sum(1 for _, v, _ in out if v == "accept")
    st.update(kept=len(out), r6_pass=passed)
    if out and passed / len(out) < PROFILE_GATE:
        st["gated"] = 1
        for o in out:
            o[1] = "pending"
    return out, st


# ---------------------------------------------------------------- I/O

def works_by_orcid(orcid):
    """Every OpenAlex work under the ORCID. Keyless: page= paging only (cursor= 401s)."""
    out, page = [], 1
    while True:
        q = urllib.parse.urlencode({"filter": f"author.orcid:{orcid}", "select": SELECT, "per-page": 200, "page": page})
        for attempt in range(4):
            try:
                req = urllib.request.Request("https://api.openalex.org/works?" + q, headers={"User-Agent": wo.UA})
                with urllib.request.urlopen(req, timeout=60) as r:
                    d = json.load(r)
                break
            except Exception as e:
                if attempt == 3:
                    raise
                log.info("openalex %s page %d: %s", orcid, page, e)
                time.sleep(2 ** attempt)
        out.extend(d.get("results") or [])
        if page * 200 >= min(d.get("meta", {}).get("count", 0), 10000) or not d.get("results"):
            return out
        page += 1
        time.sleep(0.2)


def population(c):
    """[(netid, orcid, source)]: AM ORCID for people with live seeds (one AM ORCID only),
    else a live Crossref-backed web inference."""
    c.execute("SELECT DISTINCT LOWER(netid) n FROM pubsource_web_seed WHERE removed_at IS NULL")
    seeded = {r["n"] for r in c.fetchall()}
    am = collections.defaultdict(set)
    c.execute("SELECT personIdentifier, orcid FROM pubsource_orcid_person WHERE personIdentifierType = 'NetID'")
    for r in c.fetchall():
        o = wo.bare_orcid(r["orcid"])
        if o and r["personIdentifier"].lower() in seeded:
            am[r["personIdentifier"].lower()].add(o)
    try:
        c.execute("""SELECT netid, orcid FROM pubsource_web_orcid_inference
                     WHERE superseded_at IS NULL AND crossref_votes >= 1 AND am_orcid = 'none'""")
        inferred = {r["netid"].lower(): r["orcid"] for r in c.fetchall()}
    except Exception as e:  # before the first ORCID pass: AM only. Safe, this pass never deletes.
        log.info("no web inferences yet (%s); AM ORCIDs only", e)
        inferred = {}
    out = [(n, next(iter(o)), "am") for n, o in sorted(am.items()) if len(o) == 1]
    out += [(n, o, "web_inference") for n, o in sorted(inferred.items()) if n not in am]
    return out


def run_pass(db, now, dry_run=False, only=None):
    s = collections.Counter()
    with db.cursor() as c:
        people = population(c)
        if only:
            people = [p for p in people if p[0] == only]
        netids = [p[0] for p in people]
        rid = {r["netid"].lower(): r["id"] for r in wc._load(c, "SELECT id, netid FROM identity_cornell WHERE netid IN ({})", netids)}
        names = {r["netid"].lower(): (r["name_first"], r["name_last"]) for r in wc._load(
            c, "SELECT netid, name_first, name_last FROM identity_cornell_current WHERE netid IN ({})", netids)}
        seeds = collections.defaultdict(set)
        for r in wc._load(c, "SELECT netid, doi FROM pubsource_web_seed WHERE removed_at IS NULL AND netid IN ({})", netids):
            seeds[r["netid"].lower()].add(r["doi"].lower())
    s["people"] = len(people)
    for netid, orcid, source in people:
        if netid not in rid:
            s["skipped_no_researcher"] += 1
            continue
        first, last = names.get(netid, ("", ""))
        try:
            works = works_by_orcid(orcid)
        except Exception as e:
            log.info("%s: openalex failed, skipped this run: %s", netid, e)
            s["skipped_openalex_error"] += 1
            continue
        have = {norm_doi(w.get("doi")) for w in works}
        missing_seeds = sorted(seeds[netid] - have)
        extra = list(wo.fetch_openalex(missing_seeds, select=SELECT)[0].values()) if missing_seeds else []
        decisions, st = plan_person(works, seeds[netid], orcid, first, last, extra)
        s.update({k: v for k, v in st.items()})
        s["people_" + source] += 1
        if decisions:
            s.update(write_person(db, rid[netid], decisions, now, dry_run))
    if dry_run:
        with db.cursor() as c:
            c.execute(SEED_CONFIRM_FROM.replace("{set}", "SELECT COUNT(*) n FROM cornell_pub_researcher_publication r"))
            s["would_seed_confirm"] = c.fetchone()["n"]
    else:
        s.update(seed_confirm(db, now))
    return dict(s)


def write_person(db, researcher_id, decisions, now, dry_run=False):
    """Writes (or, dry, only counts) one person's decisions. Dry counts a missing publication
    as 'pubs_new' and its link as an insert."""
    s = collections.Counter()
    with db.cursor() as c:
        pid_of = {}
        dois = sorted({norm_doi(w.get("doi")) for w, _, _ in decisions} - {None})
        for r in wc._load(c, "SELECT id, doi FROM cornell_pub_publication WHERE doi IN ({})", dois):
            pid_of[r["doi"].lower()] = r["id"]
        oaids = sorted({(w.get("id") or "").rsplit("/", 1)[-1] for w, _, _ in decisions} - {""})
        by_oa = {r["openalex_id"]: r["id"] for r in wc._load(
            c, "SELECT id, openalex_id FROM cornell_pub_publication WHERE openalex_id IN ({})", oaids)}
        for w, _, _ in decisions:
            d = norm_doi(w.get("doi")) or None
            oa = (w.get("id") or "").rsplit("/", 1)[-1]
            if (d and d in pid_of) or oa in by_oa:
                continue
            if dry_run:
                s["pubs_new"] += 1
                continue
            row = cp.pub_row(d, w, None)
            c.execute("""INSERT IGNORE INTO cornell_pub_publication (doi, pmid, openalex_id, title, journal, issn, eissn,
                             publication_year, publication_type, citation_count)
                         VALUES (%(doi)s, %(pmid)s, %(openalex_id)s, %(title)s, %(journal)s, %(issn)s, %(eissn)s,
                                 %(year)s, %(type)s, %(cited_by)s)""", row)
            s["pubs_new"] += c.rowcount
            c.execute("SELECT id FROM cornell_pub_publication WHERE " + ("doi = %s" if d else "openalex_id = %s"), (d or oa,))
            got = c.fetchone()
            if got and d:
                pid_of[d] = got["id"]
            elif got:
                by_oa[oa] = got["id"]
        c.execute("""SELECT id, publication_id, review_status FROM cornell_pub_researcher_publication
                     WHERE researcher_id = %s AND retrieval_source = 'openalex_orcid'""", (researcher_id,))
        linked = collections.defaultdict(list)
        for r in c.fetchall():
            linked[r["publication_id"]].append(r)
        for w, verdict, f in decisions:
            d = norm_doi(w.get("doi")) or None
            pid = pid_of.get(d) if d else None
            pid = pid or by_oa.get((w.get("id") or "").rsplit("/", 1)[-1])
            if not pid:
                s["inserted_" + verdict if dry_run else "no_publication_row"] += 1
                continue
            notes = json.dumps({"rule": RULE, "verdict": verdict, **f})
            rows = linked.get(pid)
            if rows:
                if verdict == "accept":
                    for r in rows:
                        if r["review_status"] == "pending" and dry_run:
                            s["promoted"] += 1
                        elif r["review_status"] == "pending":
                            c.execute("""UPDATE cornell_pub_researcher_publication SET review_status = 'auto_accepted',
                                             confidence_score = %s, reviewed_by = 'web_seeds_r6', reviewed_at = %s, review_notes = %s
                                         WHERE id = %s AND review_status = 'pending'""", (CONF_ACCEPT, now, notes, r["id"]))
                            s["promoted"] += c.rowcount
                continue
            if dry_run:
                s["inserted_" + verdict] += 1
                continue
            c.execute("""INSERT INTO cornell_pub_researcher_publication (researcher_id, publication_id, retrieval_source,
                             confidence_score, review_status, reviewed_by, reviewed_at, review_notes)
                         VALUES (%s, %s, 'openalex_orcid', %s, %s, 'web_seeds_r6', %s, %s)""",
                      (researcher_id, pid, CONF_ACCEPT if verdict == "accept" else CONF_PENDING,
                       "auto_accepted" if verdict == "accept" else "pending", now, notes))
            linked[pid].append({"id": c.lastrowid, "review_status": "x"})
            s["inserted_" + verdict] += 1
    if not dry_run:
        db.commit()
    return s


SEED_CONFIRM_FROM = """{set}
    JOIN cornell_pub_researcher_publication s
      ON s.researcher_id = r.researcher_id AND s.publication_id = r.publication_id AND s.retrieval_source = 'web_seed'
    WHERE r.review_status = 'pending' AND r.retrieval_source <> 'web_seed'"""


def seed_confirm(db, now):
    """Pending rows (any source) for a (person, publication) a web_seed row confirms -> auto_accepted."""
    upd = SEED_CONFIRM_FROM.replace("{set}", "UPDATE cornell_pub_researcher_publication r").replace(
        "    WHERE", """    SET r.review_status = 'auto_accepted', r.reviewed_by = 'web_seed_confirm', r.reviewed_at = %s,
        r.review_notes = '{"rule": "web_seed_confirm"}'
    WHERE""")
    with db.cursor() as c:
        c.execute(upd, (now,))
        n = c.rowcount
    db.commit()
    return {"seed_confirmed": n}


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="count what would change, write nothing")
    ap.add_argument("--netid", help="one person only")
    a = ap.parse_args()
    import webPageSeeds
    db = webPageSeeds.db_conn()
    try:
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        log.info("expand pass: %s", run_pass(db, now, dry_run=a.dry_run, only=a.netid))
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
