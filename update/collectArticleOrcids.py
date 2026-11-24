"""collectArticleOrcids.py — the ORCID iD Crossref prints next to a person's own name.

Fills pubsource_article_orcid (setup/table_article_orcid_v3.18.sql) for active scholars'
accepted AND rejected papers where PubMed prints no iD on their byline entry. A rejected
paper that carries an iD contests it, so it matters as much as an accepted one.

Asks Crossref for author lists in batches (filter=doi:a,doi:b) and takes the iD of the
author aar_universe_scopus.pick_byline_orcid matches to the byline name. One row per slot
asked, orcid NULL when Crossref prints none; a failed batch writes nothing and is asked
again next run. A slot is asked again only when its DOI or its byline position changes.
ponytail: an iD Crossref adds after we asked is not picked up; re-ask old NULL rows if
that turns out to matter.

Reader: Scholars Profile System etl/orcid-evidence (see the read rules in the setup file).

  python3 collectArticleOrcids.py --dry-run [--max-batches 2]   # calls Crossref, writes nothing
  python3 collectArticleOrcids.py --apply [--max-batches 250] [--max-seconds 1740]
  python3 collectArticleOrcids.py --selftest                    # no network, no DB

Env: DB_HOST/DB_NAME/DB_USERNAME/DB_PASSWORD; CROSSREF_MAILTO or OPENALEX_MAILTO (optional).
"""
import argparse, os, sys, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

TABLE = "pubsource_article_orcid"
MAX_FAILED = 3        # batches in a row that may fail before the run stops

# targetAuthor is stored '1' or '1\r' (the importer's CR).
SLOTS_SQL = """
SELECT LOWER(a.personIdentifier), a.pmid, a.`rank`, a.authorFirstName, a.authorLastName, p.doi{have}
  FROM person_article_author a
  JOIN scholar_active s ON s.cwid = a.personIdentifier
  JOIN person_article p ON p.personIdentifier = a.personIdentifier AND p.pmid = a.pmid
  {join}
 WHERE a.targetAuthor LIKE '1%%' AND COALESCE(a.orcid, '') = ''
   AND p.userAssertion IN ('ACCEPTED', 'REJECTED') AND COALESCE(p.targetAuthorCount, 1) <= 1
   AND p.doi LIKE '10.%%' AND a.personIdentifier NOT LIKE '%%\\_%%'"""


def due_slots(rows):
    """Slots to ask for, from SLOTS_SQL rows: {doi: [(uid, pmid, rank, first, last)]}, plus
    how many slots were left out (no usable DOI; two target entries on one paper)."""
    from enrichArticles import norm_doi
    by_slot, dup = {}, set()
    for r in rows:
        dup.add(r[:2]) if r[:2] in by_slot else by_slot.__setitem__(r[:2], r)
    out, bad = {}, 0
    for key, (uid, pmid, rank, first, last, raw, had_doi, had_rank) in by_slot.items():
        doi = norm_doi(raw)                 # None for a DOI with a comma: it would split the filter
        if not doi or key in dup:
            bad += 1
        elif (had_doi, had_rank) != (doi, rank):
            out.setdefault(doi, []).append((uid, pmid, rank, first, last))
    return out, bad


def crossref_authors(dois, get, pause=0.0):
    """{doi: Crossref author list} for the DOIs Crossref has a record for. Raises when the
    request fails, the answer is not a result list, or a full batch has no record at all. One
    DOI Crossref calls invalid would fail its whole batch every night, so that batch is split
    until the DOI is alone; alone, it is a DOI with no record. Any other HTTP 400 (a renamed
    parameter, a gateway) is a failure: 'asked, no record' is never asked again."""
    from aar_universe_openalex import CROSSREF_BATCH, CROSSREF_WORKS
    try:
        j = get(CROSSREF_WORKS, {"filter": ",".join(f"doi:{d}" for d in dois),
                                 "select": "DOI,author", "rows": len(dois)})
    except Exception as e:                                               # noqa: BLE001
        resp = getattr(e, "response", None)
        if getattr(resp, "status_code", None) != 400 or "doi-not-valid" not in (resp.text or ""):
            raise
        if len(dois) == 1:
            return {}
        time.sleep(pause)
        half = len(dois) // 2
        return {**crossref_authors(dois[:half], get, pause), **crossref_authors(dois[half:], get, pause)}
    items = (j.get("message") or {}).get("items")
    if not isinstance(items, list):
        raise ValueError("Crossref answered without a result list")
    out = {i["DOI"].lower(): i.get("author") or [] for i in items if i.get("DOI")}
    if len(dois) >= CROSSREF_BATCH and not out.keys() & set(dois):
        raise ValueError(f"Crossref has none of {len(dois)} DOIs")
    return out


def match(authors, n_pubmed, rank, first, last):
    """(iD, authenticated 0|1) Crossref prints for the byline entry (rank, first, last), else
    None. `n_pubmed`: how many authors PubMed lists. `rank` counts those, and a collective
    author on one side only shifts every later position, so the position is offered to the
    matcher only when both lists are the same length."""
    from aar_universe_scopus import pick_byline_orcid
    from identity_index import bare_orcid
    if not (first or "").strip() or not (last or "").strip():
        return None
    byline = []
    for a in authors:
        family = (a.get("family") or "").replace(",", " ").replace("\u2019", "'").strip()
        given = (a.get("given") or "").strip()
        o = bare_orcid(a.get("ORCID"))
        byline.append((f"{family}, {given}", (o, int(a.get("authenticated-orcid") is True)) if o else None))
    index = rank - 1 if rank and len(byline) == n_pubmed else None
    return pick_byline_orcid(byline, index, f"{first} {last}".replace("\u2019", "'"))


def rows_for(dois, due, found, n_pubmed):
    """The rows one answered batch writes, one per slot."""
    out = []
    for doi in dois:
        for uid, pmid, rank, first, last in due[doi]:
            o, auth = match(found.get(doi, []), n_pubmed.get(pmid), rank, first, last) or (None, None)
            out.append((uid, pmid, "crossref", rank, doi, o, auth, int(doi in found)))
    return out


def run(due, get, author_counts, write, max_batches, deadline, pause):
    """Newest papers first. Returns (stats, whether the run stopped on failures)."""
    from aar_universe_openalex import CROSSREF_BATCH
    order = sorted(due, key=lambda d: (-max(s[1] for s in due[d]), d))
    st = dict.fromkeys(("batches", "failed", "slots", "found", "with_orcid", "authenticated"), 0)
    in_a_row = 0
    for i in range(0, min(len(order), max_batches * CROSSREF_BATCH), CROSSREF_BATCH):
        if time.monotonic() > deadline:
            print("time is up; the rest waits for the next run", flush=True)
            break
        dois = order[i:i + CROSSREF_BATCH]
        st["batches"] += 1
        try:
            found = crossref_authors(dois, get, pause)
        except Exception as e:                                           # noqa: BLE001
            st["failed"] += 1
            in_a_row += 1
            print(f"batch {st['batches']} failed, nothing written: {type(e).__name__}: {e}", flush=True)
            if in_a_row >= MAX_FAILED:
                return st, True
        else:
            in_a_row = 0
            rows = rows_for(dois, due, found, author_counts({s[1] for d in dois for s in due[d]}))
            write(rows)
            st["slots"] += len(rows)
            st["found"] += sum(r[7] for r in rows)
            st["with_orcid"] += sum(r[5] is not None for r in rows)
            st["authenticated"] += sum(r[6] or 0 for r in rows)
            if st["batches"] % 100 == 0:
                print(st, flush=True)
        time.sleep(pause)
    return st, False


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    for f in ("--dry-run", "--apply", "--selftest"):
        g.add_argument(f, action="store_true")
    ap.add_argument("--max-batches", type=int, help="Crossref calls this run (default 250; dry run 2)")
    ap.add_argument("--max-seconds", type=int, default=0, help="start no batch after this long")
    a = ap.parse_args()
    if a.selftest:
        return 0 if _selftest() else 1
    from aar_universe_openalex import _http_json, CROSSREF_SLEEP
    from updateOrcidSuggestions import db
    cx = db(); cur = cx.cursor()
    cur.execute("SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s", (TABLE,))
    have = cur.fetchone()[0] == 1
    if a.apply and not have:
        print(f"{TABLE} does not exist: apply setup/table_article_orcid_v3.18.sql first")
        return 1
    cur.execute(SLOTS_SQL.format(
        have=", c.doi, c.author_rank" if have else ", NULL, NULL",
        join=f"LEFT JOIN {TABLE} c ON c.personIdentifier = a.personIdentifier AND c.pmid = a.pmid "
             "AND c.source = 'crossref'" if have else ""), ())
    rows = cur.fetchall()
    due, bad = due_slots(rows)
    print(f"slots: {len(rows)}; due {sum(map(len, due.values()))} on {len(due)} DOIs; "
          f"left out {bad} (no usable DOI, or two target entries)", flush=True)

    def author_counts(pmids):
        cx.ping(reconnect=True)
        cur.execute("SELECT pmid, MAX(`rank`) FROM person_article_author WHERE pmid IN (%s) GROUP BY pmid"
                    % ",".join(["%s"] * len(pmids)), sorted(pmids))
        return dict(cur.fetchall())

    def write(rs):
        if a.apply and rs:
            cur.executemany(f"""INSERT INTO {TABLE} (personIdentifier, pmid, source, author_rank, doi, orcid,
                authenticated, found, fetched_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP())
                ON DUPLICATE KEY UPDATE author_rank=VALUES(author_rank), doi=VALUES(doi), orcid=VALUES(orcid),
                authenticated=VALUES(authenticated), found=VALUES(found), fetched_at=VALUES(fetched_at)""", rs)
            cx.commit()

    cap = a.max_batches if a.max_batches is not None else (250 if a.apply else 2)
    st, stopped = run(due, _http_json, author_counts, write, cap,
                      time.monotonic() + a.max_seconds if a.max_seconds > 0 else float("inf"), CROSSREF_SLEEP)
    print(f"{'applied' if a.apply else 'dry run, nothing written'}: {st}", flush=True)
    if stopped:
        print(f"stopped after {MAX_FAILED} failed batches in a row", flush=True)
    cx.close()
    return 1 if stopped else 0


def _selftest():
    ok = True

    def check(what, got, want):
        nonlocal ok
        if got != want:
            ok = False
            print(f"FAIL {what}: got {got!r}, want {want!r}")

    A, B = "0000-0002-1825-0097", "0000-0002-1825-009X"

    def au(family, given, orcid=None, auth=None):
        return {"family": family, "given": given, **({"ORCID": f"https://orcid.org/{orcid}"} if orcid else {}),
                **({"authenticated-orcid": auth} if auth is not None else {})}

    # ---- matching
    two = [au("Wang", "Yi", A), au("Wang", "Yan", B)]
    check("same surname and initial, lists the same length: the position decides", match(two, 2, 2, "Yan", "Wang"), (B, 0))
    check("same surname and initial, a collective author on one side: nothing", match(two, 3, 2, "Yan", "Wang"), None)
    check("a collective author on one side, one fit: the one it fits",
          match([{"name": "The Example Study Group"}, au("Doe", "Jane", A)], 1, 1, "J", "Doe"), (A, 0))
    check("a group is never a person", match([{"name": "Jane Doe", "ORCID": A}], 1, 1, "Jane", "Doe"), None)
    check("accents", match([au("Müller", "José", A)], 1, 1, "Jose", "Muller"), (A, 0))
    check("curly and straight apostrophes are the same", match([au("O\u2019Doe", "Jane", A)], 1, 1, "Jane", "O'Doe"), (A, 0))
    check("the other way round", match([au("O'Doe", "Jane", A)], 1, 1, "Jane", "O\u2019Doe"), (A, 0))
    check("hyphenated surname", match([au("Doe-Smith", "Jane", A)], 1, 1, "Jane", "Smith"), (A, 0))
    check("the initial must agree", match([au("Doe", "Karl", A)], 1, 1, "Jane", "Doe"), None)
    check("another surname at the position", match([au("Roe", "Jane", A)], 1, 1, "Jane", "Doe"), None)
    check("no given name stored: no match, even for a two-word surname",
          match([au("Cruz", "David", A)], 1, 1, "", "de la Cruz"), None)
    check("matched author prints no iD", match([au("Doe", "Jane"), au("Roe", "Ann", A)], 2, 1, "Jane", "Doe"), None)
    check("a comma in the family name", match([au("Doe, MD", "Jane", A)], 1, 1, "Jane", "Doe"), (A, 0))
    check("check character upper-cased, authenticated kept",
          match([au("Doe", "Jane", B.lower(), True)], 1, 1, "Jane", "Doe"), (B, 1))
    check("a malformed iD is not an iD", match([au("Doe", "Jane", "0000-0002-1825")], 1, 1, "Jane", "Doe"), None)

    # ---- what is due
    s = lambda uid, pmid, doi, hd=None, hr=None, rank=1: (uid, pmid, rank, "Jane", "Doe", doi, hd, hr)
    due, bad = due_slots([
        s("a", 1, "10.1000/New"), s("a", 2, "10.1000/asked", "10.1000/asked", 1),
        s("a", 3, "10.1000/moved", "10.1000/old", 1), s("a", 4, "10.1000/rank", "10.1000/rank", 2),
        s("a", 5, "10.1000/a,b"), s("a", 6, "10.1000/dup"), s("a", 6, "10.1000/dup", rank=3),
        s("b", 2, "10.1000/asked")])
    check("due: new, DOI changed, position changed, and a new person on a DOI already asked",
          {d: [x[:2] for x in v] for d, v in due.items()},
          {"10.1000/new": [("a", 1)], "10.1000/moved": [("a", 3)], "10.1000/rank": [("a", 4)],
           "10.1000/asked": [("b", 2)]})
    check("left out: a comma DOI, two target entries", bad, 2)

    # ---- batches
    class Refused(Exception):
        def __init__(self, code, text=""):
            self.response = type("R", (), {"status_code": code, "text": text})()

    def dois_of(p):
        return [x[4:] for x in p["filter"].split(",")]

    def answer(url, p):
        ds = dois_of(p)
        if "10.1000/bad" in ds:
            raise Refused(400, '{"message":[{"type":"doi-not-valid"}]}')
        return {"message": {"items": [{"DOI": d.upper(), "author": [au("Doe", "Jane", A)]}
                                      for d in ds if d != "10.1000/absent"]}}

    check("one invalid DOI does not lose its batch", set(crossref_authors(
        ["10.1000/x", "10.1000/bad", "10.1000/y"], answer)), {"10.1000/x", "10.1000/y"})
    for what, get in (("a 400 that is not about a DOI",
                       lambda u, p: (_ for _ in ()).throw(Refused(400, "select-not-available"))),
                      ("an outage", lambda u, p: (_ for _ in ()).throw(Refused(503))),
                      ("an answer without a result list", lambda u, p: {"message": {"items": {}}})):
        try:
            crossref_authors(["10.1000/x"], get)
            check(f"{what} raises", False, True)
        except Exception:                                                # noqa: BLE001
            pass

    def go(get, n, **kw):
        wrote = []
        st, stopped = run({f"10.1000/{k}" if isinstance(k, int) else k: [("a", p, 1, "Jane", "Doe")]
                           for p, k in enumerate(n)}, get, lambda pm: dict.fromkeys(pm, 1), wrote.extend,
                          kw.get("max_batches", 99), kw.get("deadline", float("inf")), 0)
        return st, stopped, wrote

    st, stopped, wrote = go(answer, ["10.1000/absent", "10.1000/x"])
    check("a row per slot: the iD, and 'asked, no record' for a DOI Crossref left out", wrote,
          [("a", 1, "crossref", 1, "10.1000/x", A, 0, 1), ("a", 0, "crossref", 1, "10.1000/absent", None, None, 0)])
    check("newest PMID first", [w[1] for w in go(answer, range(90))[2]], list(range(89, -1, -1)))
    check("--max-batches", len(go(answer, range(90), max_batches=1)[2]), 40)
    check("past the deadline nothing is asked", go(answer, range(3), deadline=0)[2], [])

    calls = []

    def flaky(url, p):
        calls.append(1)
        if len(calls) == 1:
            raise Refused(503)
        return answer(url, p)
    st, stopped, wrote = go(flaky, range(90))
    check("a failed batch writes nothing and the run goes on", (stopped, st["failed"], len(wrote)), (False, 1, 50))
    calls.clear()

    def every_other(url, p):
        calls.append(1)
        if len(calls) % 2:
            raise Refused(503)
        return answer(url, p)
    st, stopped, wrote = go(every_other, range(400))
    check("failures that are not in a row do not stop it", (stopped, st["failed"], len(wrote)), (False, 5, 200))
    st, stopped, wrote = go(lambda u, p: (_ for _ in ()).throw(Refused(503)), range(400))
    check("a full batch with no record at all is a failure", go(
        lambda u, p: {"message": {"items": []}}, range(40))[2], [])
    check("failures in a row stop the run", (stopped, st["batches"], wrote), (True, MAX_FAILED, []))
    print("selftest ok" if ok else "selftest FAILED")
    return ok


if __name__ == "__main__":
    sys.exit(main())
