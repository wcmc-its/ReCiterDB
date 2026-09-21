"""Nightly ORCID suggestions: refresh `pubsource_orcid_person` (CWID, inferred) from a
person's ACCEPTED articles, using ReCiter's own target-author label.

Source 1 (free): person_article_author rows with targetAuthor=1 carry the ORCID PubMed
    supplied for that byline entry. Same ORCID on the person's accepted papers => theirs.
Source 2 (gap-fill): people whose accepted papers carry no PubMed ORCID at the target
    position get Crossref's author list for their newest DOIs, read at the same rank.
Negative evidence: an ORCID at the target position of a REJECTED paper is written with
    its rejected count, which keeps it out of the "strong" tier downstream.

Consumer: Scholars Profile System `etl/orcid-candidates` mirrors CWID inferred rows nightly
into its ORCID coverage dashboard and the self-edit "Is this your ORCID iD?" row.
Not written: external cohorts (uid containing "_"), ORCIDs seen only on rejected papers
(nothing to suggest, and an extra row would break the sole-iD rule downstream), rows a
human has `verified`.

  python3 updateOrcidSuggestions.py --dry-run [--cache FILE | --s3-cache]
  python3 updateOrcidSuggestions.py --apply   [--cache FILE | --s3-cache]
  python3 updateOrcidSuggestions.py --check   (no network, no DB)
"""
import argparse, json, os, sys, unicodedata, urllib.request
from collections import defaultdict

CROSSREF_MAX_PER_PERSON = 5
S3_CACHE_KEY = "orcid-suggestions/crossref_cache.json"
OWNED_SOURCES = ("pubmed_author", "crossref_author")


def norm(s):
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return "".join(c for c in s if c.isalpha())


def orcid_at_position(authors, rank, last):
    """ORCID of the author at 1-based `rank` if the surname agrees, else of a unique surname hit."""
    # ponytail: positional + surname check only; a reordered byline falls back to a unique surname
    cands = [a for a in authors if norm(a.get("family")) == norm(last)]
    if 0 < rank <= len(authors) and norm(authors[rank - 1].get("family")) == norm(last):
        cands = [authors[rank - 1]]
    if len(cands) != 1 or not cands[0].get("ORCID"):
        return None
    return cands[0]["ORCID"].rsplit("/", 1)[-1]


def crossref_authors(doi, cache):
    if doi not in cache:
        try:
            req = urllib.request.Request(f"https://api.crossref.org/works/{doi}",
                                         headers={"User-Agent": "reciterdb-orcid-suggestions (mailto:paa2013@med.cornell.edu)"})
            cache[doi] = json.load(urllib.request.urlopen(req, timeout=20))["message"].get("author", [])
        except Exception:
            cache[doi] = []
    return cache[doi]


def fold(rows, cache, crossref_persons=None, log=lambda m: None):
    """rows: (uid, pmid, rank, last, orcid|None, assertion, doi|None) for target-author rows.
    Returns {uid: {orcid: {"acc": n, "pend": n, "rej": n, "src": mapping_source}}} — the
    rows to write. Pure given `cache`; Crossref is consulted only for DOIs not in it."""
    counts = defaultdict(lambda: defaultdict(lambda: {"acc": 0, "pend": 0, "rej": 0, "src": "pubmed_author"}))
    gap = defaultdict(list)  # uid -> accepted (pmid, rank, last, doi) lacking a PubMed ORCID
    for uid, pmid, rank, last, orcid, assertion, doi in rows:
        if "_" in uid:
            continue  # external cohorts are not ours to suggest for
        key = {"ACCEPTED": "acc", "REJECTED": "rej"}.get(assertion, "pend")
        if orcid:
            counts[uid][orcid][key] += 1
        elif key == "acc" and doi:
            gap[uid].append((pmid, rank or 0, last, doi))
    todo = sorted(u for u in gap if not any(v["acc"] for v in counts[u].values()))
    if crossref_persons is not None:
        todo = todo[:crossref_persons]
    for i, uid in enumerate(todo):
        for pmid, rank, last, doi in sorted(gap[uid], reverse=True)[:CROSSREF_MAX_PER_PERSON]:
            o = orcid_at_position(crossref_authors(doi, cache), rank, last)
            if o:
                counts[uid][o]["acc"] += 1
                counts[uid][o]["src"] = "crossref_author"
        if i and i % 500 == 0:
            log(f"crossref {i}/{len(todo)}")
    # Drop ORCIDs never seen on an accepted paper: nothing to suggest, and an extra inferred
    # row would defeat the consumer's sole-iD rule for the real one.
    return {u: {o: c for o, c in d.items() if c["acc"] > 0} for u, d in counts.items()
            if any(c["acc"] > 0 for c in d.values())}


def db():
    import pymysql
    return pymysql.connect(host=os.environ["DB_HOST"], user=os.environ["DB_USERNAME"],
                           password=os.environ["DB_PASSWORD"], database=os.environ["DB_NAME"],
                           connect_timeout=15, autocommit=False)


def load_rows(cur):
    # LOWER(): reciterdb holds mixed-case cwids (Isk9008 / isk9008) and the target table's
    # unique key is case-insensitive, so the fold must be too.
    cur.execute("""SELECT LOWER(a.personIdentifier), a.pmid, a.`rank`, a.authorLastName, NULLIF(a.orcid,''),
                          p.userAssertion, NULLIF(p.doi,'')
                   FROM person_article_author a
                   JOIN person_article p ON p.personIdentifier=a.personIdentifier AND p.pmid=a.pmid
                   WHERE a.targetAuthor LIKE '1%%' AND a.personIdentifier NOT LIKE '%%\\_%%'""")
    return cur.fetchall()


def plan(cur, wanted):
    """Compare `wanted` with the rows this job owns; return (inserts, updates, deletes)."""
    cur.execute("""SELECT LOWER(personIdentifier), orcid, articles_accepted, articles_pending, articles_rejected, mapping_source, verified
                   FROM pubsource_orcid_person WHERE personIdentifierType='CWID' AND confidence_type='inferred'""")
    have = {(u, o): (a or 0, p or 0, r or 0, src, v) for u, o, a, p, r, src, v in cur.fetchall()}
    inserts, updates, deletes = [], [], []
    for u, d in wanted.items():
        for o, c in d.items():
            row = (u, o, c["acc"], c["pend"], c["rej"], c["src"])
            if (u, o) not in have:
                inserts.append(row)
            elif have[(u, o)][:4] != (c["acc"], c["pend"], c["rej"], c["src"]):
                updates.append(row)
    for (u, o), (a, p, r, src, v) in have.items():
        if src in OWNED_SOURCES and not v and o not in wanted.get(u, {}):
            deletes.append((u, o))
    return inserts, updates, deletes


def apply(cur, inserts, updates, deletes):
    cur.executemany("""INSERT INTO pubsource_orcid_person
        (personIdentifier, personIdentifierType, orcid, confidence_type, mapping_source, articles_accepted, articles_pending, articles_rejected)
        VALUES (%s, 'CWID', %s, 'inferred', %s, %s, %s, %s)""",
        [(u, o, src, a, p, r) for u, o, a, p, r, src in inserts])
    cur.executemany("""UPDATE pubsource_orcid_person SET articles_accepted=%s, articles_pending=%s, articles_rejected=%s, mapping_source=%s
        WHERE personIdentifier=%s AND personIdentifierType='CWID' AND orcid=%s AND verified=0""",
        [(a, p, r, src, u, o) for u, o, a, p, r, src in updates])
    cur.executemany("""DELETE FROM pubsource_orcid_person
        WHERE personIdentifier=%s AND personIdentifierType='CWID' AND orcid=%s AND confidence_type='inferred' AND verified=0""",
        deletes)


def s3_cache(action, cache=None):
    import boto3, botocore
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = os.environ["S3_BUCKET"]
    if action == "pull":
        try:
            return json.load(s3.get_object(Bucket=bucket, Key=S3_CACHE_KEY)["Body"])
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return {}
            raise
    s3.put_object(Bucket=bucket, Key=S3_CACHE_KEY, Body=json.dumps(cache).encode())


def check():
    Z = {"family": "Zhang", "ORCID": "https://orcid.org/0009-0005-6615-367X"}
    C = {"family": "Chau", "ORCID": "https://orcid.org/0000-0002-9930-2193"}
    B = {"family": "Banerjee"}
    assert orcid_at_position([Z, C, B], 2, "Chau") == "0000-0002-9930-2193"
    assert orcid_at_position([Z, C, B], 3, "Chau") == "0000-0002-9930-2193"   # wrong rank, unique surname wins
    assert orcid_at_position([Z, C, B], 3, "Banerjee") is None                # right author, no ORCID
    assert orcid_at_position([C, C], 1, "Chau") == "0000-0002-9930-2193"      # rank agrees despite duplicate surname
    assert norm("Müller-Lyer") == "mullerlyer"
    rows = [
        ("a", 1, 2, "Chau", "X", "ACCEPTED", None), ("a", 2, 2, "Chau", "X", "ACCEPTED", None),
        ("a", 3, 1, "Chau", "H", "REJECTED", None),                       # homonym: rejected-only, dropped
        ("b", 4, 2, "Chau", None, "ACCEPTED", "10.1/gap"),                 # crossref gap-fill
        ("c", 5, 1, "Lee", "Y", "ACCEPTED", None), ("c", 6, 1, "Lee", "Y", "REJECTED", None),  # contradiction kept with counts
        ("ucsf_d", 7, 1, "Doe", "Q", "ACCEPTED", None),                     # external cohort, skipped
        ("e", 8, 1, "Kim", "K", "PENDING", None),                           # pending only: nothing to suggest
    ]
    cache = {"10.1/gap": [Z, C]}
    out = fold(rows, cache)
    assert out == {
        "a": {"X": {"acc": 2, "pend": 0, "rej": 0, "src": "pubmed_author"}},
        "b": {"0000-0002-9930-2193": {"acc": 1, "pend": 0, "rej": 0, "src": "crossref_author"}},
        "c": {"Y": {"acc": 1, "pend": 0, "rej": 1, "src": "pubmed_author"}},
    }, out
    print("ok")


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true"); g.add_argument("--apply", action="store_true"); g.add_argument("--check", action="store_true")
    ap.add_argument("--cache", help="local Crossref cache JSON"); ap.add_argument("--s3-cache", action="store_true")
    ap.add_argument("--crossref-persons", type=int, help="cap the Crossref gap-fill (sampling)")
    a = ap.parse_args()
    if a.check:
        return check()
    cache = s3_cache("pull") if a.s3_cache else (json.load(open(a.cache)) if a.cache and os.path.exists(a.cache) else {})
    n0 = len(cache)
    cx = db(); cur = cx.cursor()
    rows = load_rows(cur)
    print(f"target-author rows: {len(rows)}; crossref cache: {n0} DOIs")
    wanted = fold(rows, cache, a.crossref_persons, log=print)
    if a.s3_cache and len(cache) > n0:
        s3_cache("push", cache)
    elif a.cache and len(cache) > n0:
        json.dump(cache, open(a.cache, "w"))
    inserts, updates, deletes = plan(cur, wanted)
    by_src = defaultdict(int)
    for d in wanted.values():
        for c in d.values():
            by_src[c["src"]] += 1
    print(f"people: {len(wanted)}; rows: {sum(len(d) for d in wanted.values())} ({dict(by_src)}); "
          f"insert {len(inserts)}, update {len(updates)}, delete {len(deletes)}; new DOIs fetched: {len(cache) - n0}")
    for label, xs in (("insert", inserts), ("update", updates), ("delete", deletes)):
        for x in xs[:3]:
            print(f"  {label}: {x}")
    if a.apply:
        apply(cur, inserts, updates, deletes)
        cx.commit()
        print("applied")
    cx.close()


if __name__ == "__main__":
    main()
