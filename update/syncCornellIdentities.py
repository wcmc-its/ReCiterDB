"""Cornell University (Ithaca) people -> ReCiter Identity (DynamoDB), monthly.

Reads the roster (identity_cornell_current: the spreadsheet with Cornell's monthly HR
file on top) and PUTs each person to /reciter/save/identities/ with their `cornell-*`
person types. That marker is what lets buildIdentity.cornell_ithaca() admit a netid, so
a new hire is matchable on /authorships two nights after this runs. These people are
never retrieved and never scored: do not call the feature generator from here.

Moved from scripts/sync_cornell_ithaca_identities.py (ReCiter Research), which was run by
hand. Dry run unless --apply. Own CronJob: kubernetes/k8-cronjob-cornell-sync.yaml.

Nobody reads the output of a scheduled run, so it stops instead of guessing when
  - Cornell's directory answers partially, or with few CWIDs (a missing netid <-> CWID pair
    mints a second person),
  - a netid already names somebody else in Identity (the PUT replaces the whole item),
  - it would create more than MAX_NEW people.
Only records that would change are written, each batch is re-read just before its PUT,
and the previous version of every record written is saved first (S3 in the cluster).

Not touched: WCM people. A dual-appointed person keeps their CWID. If that CWID is on the
WCM roster, or carries any WCM person type, it is skipped. Otherwise it gets the cornell-*
types, plus the campus marker once it already holds only cornell-* types, i.e. once a
previous run's types survived the Institutional Client, which overwrites personTypes for
the people it manages. A marker it wiped would leave the CWID in the WCM pool.

  python3 syncCornellIdentities.py [--apply] [--netids a1,b2] | --demo
  (--netids takes the ids to write: a dual person's is their CWID)

Environment: DB_*, CORNELL_ITHACA_ED_DN/_PASS, AWS read on Identity; for --apply also
RECITER_API_KEY, RECITER_BASE_URL, S3_BUCKET.
"""
import argparse
import datetime
import json
import logging
import os
import sys

import buildIdentity as bi
from cornell_hr_feed import canonical_dept
from cwid import canonical_uid

logger = logging.getLogger("syncCornellIdentities")

INSTITUTION = "Cornell University"
CAMPUS = bi.CORNELL_CAMPUS_TYPE
BATCH = 100
MAX_NEW = 1000                      # the first HR file (2026-11) added 353
MIN_PAIRS = 200                     # 271 of the roster on 2026-11-24

# role flag -> person type: Cornell's vocabulary. `cornell-` is a prefix other code tests
# for, so WCM's own `affiliate-cornell` is not caught by it.
FLAG_TYPES = [
    ("is_faculty", "cornell-faculty"),
    ("is_rte_faculty", "cornell-faculty-rte"),
    ("is_new_faculty", "cornell-faculty-new"),
    ("is_academic", "cornell-academic"),
    ("is_rsrch_teach_title", "cornell-research-teaching-title"),
    ("is_researcher", "cornell-researcher"),
    ("is_proj_researcher", "cornell-researcher-project"),
    ("is_postdoc", "cornell-postdoc"),
    ("is_student_grad_all", "cornell-student-grad"),
    ("is_student_undergrad_all", "cornell-student-undergrad"),
    ("is_incubator_assoc", "cornell-incubator-associate"),
    ("is_staff", "cornell-staff"),
    ("is_rsrch_center_empl", "cornell-research-center-employee"),
]
# affiliations_all has no delimiter ("emeritusretired facultyretiree"): longest first,
# and each hit is cut out so "retired faculty" is not read again as "faculty".
AFFILIATIONS = ["retired faculty", "former postdoc", "email list", "temporary", "affiliate",
                "exception", "emeritus", "academic", "student", "retiree", "faculty",
                "alumni", "staff"]

# The fields this source owns. The PUT replaces the whole item, so every other field on
# the live record (grants, knownRelationships, degreeYear, ...) is carried forward.
OWNED = {"uid", "primaryName", "alternateNames", "primaryInstitution", "institutions",
         "personTypes", "primaryEmail", "emails", "title", "primaryOrganizationalUnit",
         "organizationalUnits"}

# DynamoDB holds RelationshipType's display label; the PUT accepts only the constant, and
# one label it cannot read rejects the whole batch. Transcribed from the enum: HR's label
# shares no text with its name.
RELATIONSHIPS = {
    "Co-investigator": "CO_INVESTIGATOR", "Mentor": "MENTOR", "Mentee": "MENTEE",
    "Manager": "MANAGER", "Report": "REPORT", "Shared organizational unit": "HR",
    "CTSC Grant Associate": "CTSC_GRANT_ASSOCIATE",
    "CTSC Protocol Associate": "CTSC_PROTOCOL_ASSOCIATE",
}

SELECT_SQL = """
SELECT i.netid, i.name, i.name_first, i.name_last, i.email, i.email_netid, i.email_alt_cu,
       i.primary_department, i.primary_job_title, i.primary_business_title,
       i.other_departments, i.affiliations_all, i.is_active_employee, {flags}, m.orcid
FROM identity_cornell_current i
LEFT JOIN cornell_pub_orcid_mapping m ON m.netid = i.netid
WHERE {population}
  AND i.name_first IS NOT NULL AND i.name_first <> ''
  AND i.name_last REGEXP '[[:alpha:]]'
ORDER BY i.netid, m.orcid
"""


def person_types(row):
    types = [CAMPUS] + [label for col, label in FLAG_TYPES if row.get(col)]
    s, found = (row.get("affiliations_all") or "").lower(), []
    for value in AFFILIATIONS:
        if value in s:
            s = s.replace(value, "|")
            found.append("cornell-" + value.replace(" ", "-"))
    types += sorted(found)
    if not row.get("is_active_employee"):
        types.append("cornell-inactive")
    return list(dict.fromkeys(types))


def build_identity(row):
    first, last = row["name_first"].strip(), row["name_last"].strip()
    name = {"firstName": first, "firstInitial": first[:1], "lastName": last}
    middle = bi._cornell_middle_name(row["name"], first, last)
    if middle:
        name.update(middleName=middle, middleInitial=middle[:1])
    # No alternateNames: the roster has one name per person.
    ident = {"uid": row["netid"], "primaryName": name, "primaryInstitution": INSTITUTION,
             "institutions": [INSTITUTION], "personTypes": person_types(row)}
    emails = list(dict.fromkeys(
        e.strip().lower() for e in (row["email_netid"], row["email"], row["email_alt_cu"])
        if e and "@" in e))
    if emails:
        ident.update(primaryEmail=emails[0], emails=emails)
    title = row["primary_business_title"] or row["primary_job_title"]
    if title:
        ident["title"] = title

    def unit(u):        # only Cornell Tech's generic HR units are renamed here
        return "Cornell Tech" if canonical_dept(u) == "Cornell Tech" else u
    if row["primary_department"]:
        units = [unit(u) for u in [row["primary_department"]]
                 + (row.get("other_departments") or "").split("|") if u]
        ident["primaryOrganizationalUnit"] = units[0]
        ident["organizationalUnits"] = [
            {"organizationalUnitLabel": u, "organizationalUnitType": "DEPARTMENT"}
            for u in dict.fromkeys(units)]
    if row.get("orcid"):
        ident["orcid"] = row["orcid"]
    return ident


def dual_identity(live, types, on_wcm_roster, has_netid_identity):
    """A dual person's CWID record with their cornell-* types added, or None to leave it
    alone. See the module docstring for who is skipped and when the marker is added."""
    have = list(live.get("personTypes") or [])
    if any(not t.startswith("cornell-") for t in have):
        return None
    if on_wcm_roster and CAMPUS not in have:
        return None
    if not have or has_netid_identity:
        types = [t for t in types if t != CAMPUS]
    return dict(live, personTypes=list(dict.fromkeys(have + types)))


def carry_forward(ident, live):
    """`ident` plus every field on the live record this source does not own."""
    return {**{k: v for k, v in (live or {}).items() if k not in OWNED}, **ident}


def relationships(ident):
    """(record with relationship types the PUT accepts, labels it does not know)."""
    krs = ident.get("knownRelationships")
    if not krs:
        return ident, set()
    known = set(RELATIONSHIPS.values())
    bad = {k.get("type") for k in krs if k.get("type") not in known | set(RELATIONSHIPS)}
    return dict(ident, knownRelationships=[
        dict(k, type=RELATIONSHIPS.get(k.get("type"), k.get("type"))) for k in krs]), bad


def same(a, b):
    """Equal once empty values are dropped (a saved item stores them as null). Person
    types are a set; nothing else is reordered."""
    def norm(v):
        if isinstance(v, dict):
            return {k: n for k, n in ((k, norm(x)) for k, x in v.items())
                    if n not in (None, "", [], {})}
        return [norm(x) for x in v] if isinstance(v, list) else v

    def key(r):
        r = norm(relationships(r)[0])
        return dict(r, personTypes=sorted(r.get("personTypes") or []))
    return key(a) == key(b)


def collisions(uids, live, allowed):
    """uids whose live record is somebody else's: not one of ours, not adjudicated."""
    return sorted(u for u in uids if u in live and u not in allowed
                  and CAMPUS not in (live[u].get("personTypes") or []))


def fetch_live(uids):
    """uid -> live identity map, for uids that exist. Present-but-empty is {}."""
    import boto3
    from boto3.dynamodb.types import TypeDeserializer
    from decimal import Decimal

    def plain(o):
        if isinstance(o, Decimal):
            return int(o) if o == o.to_integral_value() else float(o)
        if isinstance(o, dict):
            return {k: plain(v) for k, v in o.items()}
        if isinstance(o, (list, set)):
            return [plain(v) for v in o]
        return o

    dyn = boto3.client("dynamodb", region_name="us-east-1")
    des, out, uids = TypeDeserializer(), {}, sorted(set(uids))
    for i in range(0, len(uids), BATCH):
        req = {"Identity": {"Keys": [{"uid": {"S": u}} for u in uids[i:i + BATCH]]}}
        while req:
            resp = dyn.batch_get_item(RequestItems=req)
            for item in resp["Responses"].get("Identity", []):
                row = plain({k: des.deserialize(v) for k, v in item.items()})
                out[row["uid"]] = row.get("identity") or {}
            req = resp.get("UnprocessedKeys") or None
    return out


def cwid_pairs(entries, bridge):
    """netid -> {cwid}: what Cornell's directory says today, plus the dated bridge CSV.

    ED's cornell-ithaca SOR has the same list, but the cluster's ED bind cannot read it.
    On 2026-11-24 these two held 62 of its 63 pairs whose CWID has an identity; the other
    (erg54) has left the directory, so it is never created here."""
    pairs = {}
    for n, e in entries.items():
        c = str(e.get("cornellEduCWID") or "").strip().lower()
        if c:
            pairs.setdefault(n, set()).add(c)
    if len(pairs) < MIN_PAIRS:
        raise SystemExit(f"Cornell's directory gave a CWID for {len(pairs)} netids, under "
                         f"{MIN_PAIRS}. A short list creates duplicate people.")
    for n, (c, _) in bridge.items():
        if c:
            pairs.setdefault(n.strip().lower(), set()).add(c.strip().lower())
    return pairs


def load_roster():
    conn = bi.db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(SELECT_SQL.format(
                flags=", ".join(f"i.{c}" for c, _ in FLAG_TYPES),
                population=bi.CORNELL_POPULATION))
            cols = [d[0] for d in cur.description]
            rows = {}
            for rec in cur.fetchall():
                row = dict(zip(cols, rec))
                uid, why = canonical_uid(row["netid"])
                if uid is None:
                    logger.warning("unusable netid %r skipped (%s)", row["netid"], why)
                    continue
                rows.setdefault(uid, dict(row, netid=uid))
            cur.execute("SELECT LOWER(cwid) FROM identity")
            return list(rows.values()), {c for (c,) in cur.fetchall()}
    finally:
        conn.close()


def plan(rows, wcm_roster):
    """(uid -> identity to write, before merging; uid -> live record)."""
    entries = bi.cornell_directory_entries([r["netid"] for r in rows],
                                           ("uid", "cornellEduCWID"))
    pairs = cwid_pairs(entries, bi.load_cwid_bridge())
    cwids = {r["netid"]: pairs[r["netid"]] for r in rows if r["netid"] in pairs}
    live_cwids = fetch_live({c for cs in cwids.values() for c in cs})
    dual = {}
    for netid, cs in cwids.items():
        held = sorted(c for c in cs if c in live_cwids and c != netid)
        if len(held) > 1:           # whose record it is needs a person to decide
            logger.error("%s: more than one CWID with an identity (%s) - skipped",
                         netid, ", ".join(held))
        if held:
            dual[netid] = held
    mine = [r for r in rows if r["netid"] not in dual and r["netid"] in entries]
    live = fetch_live([r["netid"] for r in mine] + list(dual))
    logger.info("%d rostered, %d dual, %d absent from the live directory, %d to check",
                len(rows), len(dual), len(rows) - len(dual) - len(mine), len(mine))

    bad = collisions([r["netid"] for r in mine], live, bi.CORNELL_KNOWN_COLLISIONS)
    if bad:
        raise SystemExit(f"{len(bad)} netid(s) already name somebody else in Identity: "
                         f"{', '.join(bad)}. Adjudicate each, then add it to "
                         f"buildIdentity.CORNELL_KNOWN_COLLISIONS.")
    todo = {r["netid"]: build_identity(r) for r in mine}
    marked = 0
    for r in (r for r in rows if len(dual.get(r["netid"], ())) == 1):
        cwid = dual[r["netid"]][0]
        rec = dual_identity(live_cwids[cwid], person_types(r),
                            cwid in wcm_roster, r["netid"] in live)
        if rec is None:
            continue
        rec["uid"] = cwid
        marked += (CAMPUS in rec["personTypes"]
                   and CAMPUS not in (live_cwids[cwid].get("personTypes") or []))
        todo[cwid] = {"uid": cwid, "personTypes": rec["personTypes"], "_dual": True}
    logger.info("%d dual people on a Cornell-only CWID, %d gain the campus marker",
                sum(1 for t in todo.values() if t.get("_dual")), marked)
    return todo, {**live, **live_cwids}


def merge(todo, live):
    """What to PUT for one planned record, given its live record now."""
    if todo.get("_dual"):           # person types only; the rest of the record is theirs
        have = live.get("personTypes") or []
        if any(not t.startswith("cornell-") for t in have):
            return None             # gained a WCM type since it was planned
        return dict(live, uid=todo["uid"],
                    personTypes=list(dict.fromkeys(list(have) + todo["personTypes"])))
    return carry_forward(todo, live)


def put(batch):
    import requests
    r = requests.put(os.environ["RECITER_BASE_URL"].rstrip("/") + "/reciter/save/identities/",
                     json=batch, headers={"api-key": os.environ["RECITER_API_KEY"]}, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")


def save_previous(stamp, n, records):
    """The records a batch is about to replace (None = created here), before its PUT."""
    import boto3
    boto3.client("s3").put_object(
        Bucket=os.environ["S3_BUCKET"], Key=f"cornell-identity-sync/{stamp}/{n:04d}.json",
        Body=json.dumps(records).encode(), ContentType="application/json")


def main(apply, netids=None):
    if apply:
        missing = [v for v in ("RECITER_API_KEY", "RECITER_BASE_URL", "S3_BUCKET")
                   if not os.environ.get(v)]
        if missing:
            raise SystemExit(f"--apply needs {', '.join(missing)}")
    rows, wcm_roster = load_roster()
    todo, live = plan(rows, wcm_roster)
    if netids:
        todo = {u: t for u, t in todo.items() if u in netids}
    new = [u for u in todo if u not in live]
    if len(new) > MAX_NEW:
        raise SystemExit(f"{len(new)} new people, over {MAX_NEW}: check the roster first.")

    changed = [u for u in todo
               if u not in live or ((m := merge(todo[u], live[u])) and not same(m, live[u]))]
    logger.info("%d planned: %d new, %d changed, %d unchanged",
                len(todo), len(new), len(changed) - len(new), len(todo) - len(changed))
    fields = {}
    for u in changed:
        if u in live:
            m = merge(todo[u], live[u])
            for f in set(m) | set(live[u]):
                if not same({f: m.get(f)}, {f: live[u].get(f)}):
                    fields.setdefault(f, []).append(u)
    for f, us in sorted(fields.items(), key=lambda kv: -len(kv[1])):
        logger.info("  %s differs on %d: %s", f, len(us), ", ".join(us[:5]))
    for u in new[:10]:
        logger.info("  new %s %s", u, todo[u]["primaryName"])
    if not apply:
        logger.info("dry run: nothing written")
        return

    stamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    written = failed = 0
    for n in range(0, len(changed), BATCH):
        uids = changed[n:n + BATCH]
        now = fetch_live(uids)      # fresh: the PUT would undo an edit made since the plan
        clash = collisions([u for u in uids if u not in live], now, ())
        batch = []
        for u in uids:
            if u in clash or (todo[u].get("_dual") and u not in now):
                logger.error("%s appeared or vanished since it was planned - skipped", u)
                continue
            rec = merge(todo[u], now.get(u, {}))
            if rec is None:
                continue
            rec, unknown = relationships(rec)
            if unknown:             # would reject the whole batch; dropping it loses data
                logger.error("%s: relationship type(s) %s the API cannot read - skipped",
                             u, unknown)
                continue
            batch.append(rec)
        if not batch:
            continue
        try:
            save_previous(stamp, n // BATCH, {r["uid"]: now.get(r["uid"]) for r in batch})
            put(batch)
            # 200 does not mean saved: the endpoint skips records it finds invalid.
            after = fetch_live([r["uid"] for r in batch])
            lost = [r["uid"] for r in batch if not same(r, after.get(r["uid"], {}))]
            if lost:
                logger.error("sent but not stored as sent: %s", ", ".join(lost))
            failed += len(lost)
            written += len(batch) - len(lost)
        except Exception as exc:    # noqa: BLE001 - next batch is independent
            failed += len(batch)
            logger.error("batch %d failed: %s: %s", n // BATCH, type(exc).__name__, exc)
    logger.info("written %d, failed %d; previous records under s3://%s/cornell-identity-sync/%s/",
                written, failed, os.environ["S3_BUCKET"], stamp)
    if failed:
        raise SystemExit(1)


def demo():
    row = dict.fromkeys([c for c, _ in FLAG_TYPES], 0)
    row.update(netid="mtw1", name="Martin Timothy Wells", name_first="Martin", name_last="Wells",
               email=None, email_netid="MTW1@cornell.edu", email_alt_cu=None,
               primary_department="Faculty and Research", primary_job_title="Professor",
               primary_business_title=None, other_departments="Computer Science|Tech Instruction",
               affiliations_all="emeritusretired facultyretiree", is_active_employee=0,
               is_faculty=1, orcid=None)
    i = build_identity(row)
    assert i["primaryName"]["middleName"] == "Timothy" and i["primaryEmail"] == "mtw1@cornell.edu"
    assert [u["organizationalUnitLabel"] for u in i["organizationalUnits"]] == \
        ["Cornell Tech", "Computer Science"]
    assert i["personTypes"] == [CAMPUS, "cornell-faculty", "cornell-emeritus",
                                "cornell-retired-faculty", "cornell-retiree", "cornell-inactive"]
    assert "alternateNames" not in i and "orcid" not in i

    # the PUT replaces: fields this source does not own survive, stale owned ones do not
    live = dict(i, grants=["x"], orcid="0", alternateNames=["old"], title="Old")
    m = carry_forward(i, live)
    assert (m["grants"], m["orcid"], m["title"]) == (["x"], "0", "Professor")
    assert "alternateNames" not in m
    assert not same(m, live) and same(m, dict(m, degreeYear=None, emails=list(m["emails"])))
    assert not same({"emails": ["a", "b"]}, {"emails": ["b", "a"]}), "order carries meaning"
    kr = {"knownRelationships": [{"uid": "a", "type": "Shared organizational unit"}]}
    assert relationships(kr)[0]["knownRelationships"][0]["type"] == "HR"
    assert relationships({"knownRelationships": [{"type": "Nemesis"}]})[1] == {"Nemesis"}
    assert same(kr, relationships(kr)[0]), "label vs constant is not a change"

    # dual people
    cu = [CAMPUS, "cornell-faculty"]
    d = lambda types, roster=False, netid=False: dual_identity(
        {"personTypes": types}, cu, roster, netid)
    assert CAMPUS in d(["cornell-faculty"])["personTypes"]
    assert d([])["personTypes"] == ["cornell-faculty"], "no marker until types have survived"
    assert d(None)["personTypes"] == ["cornell-faculty"]
    assert d(["affiliate", "cornell-staff"]) is None, "holds a WCM type"
    assert d(["cornell-faculty"], roster=True) is None, "on the WCM roster"
    assert d([CAMPUS], roster=True)["personTypes"] == cu, "ours already: kept current"
    assert CAMPUS not in d(["cornell-faculty"], netid=True)["personTypes"]
    t = {"uid": "c1", "personTypes": cu, "_dual": True}
    assert merge(t, {"personTypes": ["cornell-x"], "title": "T"}) == \
        {"uid": "c1", "title": "T", "personTypes": ["cornell-x"] + cu}
    assert merge(t, {"personTypes": ["academic"]}) is None

    assert collisions(["a", "b", "c", "d"], {
        "a": {"personTypes": [CAMPUS]}, "b": {"personTypes": ["affiliate-cornell"]},
        "c": {}, "mtw1": {}}, {"c"}) == ["b"]
    e = {f"n{i}": {"cornellEduCWID": f"C{i}"} for i in range(MIN_PAIRS)}
    p = cwid_pairs(dict(e, x1={"cornellEduCWID": None}), {"N1": ("c1", True), "y1": ("z9", False)})
    assert p["n1"] == {"c1"} and p["y1"] == {"z9"} and "x1" not in p
    try:
        cwid_pairs({"n1": e["n1"]}, {})
    except SystemExit:
        pass
    else:
        raise AssertionError("a directory without CWIDs must stop the run")
    print("demo ok")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--netids", type=lambda s: {n.strip().lower() for n in s.split(",")})
    ap.add_argument("--demo", action="store_true")
    a = ap.parse_args()
    if a.demo:
        demo()
    else:
        main(a.apply, a.netids)
