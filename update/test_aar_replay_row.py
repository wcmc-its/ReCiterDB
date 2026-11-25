"""aar_replay_row.py: the switch check, the read-only connection, each lane's author, the
uncut list and the rule each excluded person hits. Offline: real matcher, fake roster and
fake PubMed / Scopus / OpenAlex answers. All people and ids are made up.

Run: python3 update/test_aar_replay_row.py
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
for _k in [k for k in os.environ if k.startswith("AAR_")]:
    del os.environ[_k]                      # the venue and ORCID terms off, whatever the shell

from sqlalchemy import event                # noqa: E402
from sqlalchemy.engine import Engine        # noqa: E402
import aar_replay_row as rr                 # noqa: E402
import aar_universe as uni                  # noqa: E402
import aar_universe_openalex as oa          # noqa: E402
import aar_universe_scopus as scop          # noqa: E402
from identity_index import (CAMPUS_ITHACA, CAMPUS_WCM, CampusRoster, IdentityIndex,  # noqa: E402
                            _norm)

n = 0


def check(label, got, want=True):
    global n
    assert got == want, f"{label}: got {got!r}, want {want!r}"
    n += 1


# ---- switches ----
with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fh:
    fh.write("DB_PASSWORD=x\n\nAAR_VENUE_FIT=on\nexport AAR_BYLINE_ORCID=\"on\"\n"
             "# AAR_COMMENT=1\nAAR_EMPTY=\n")
want = {"AAR_VENUE_FIT": "on", "AAR_BYLINE_ORCID": "on", "AAR_EMPTY": ""}
check("only AAR_ lines are read", rr.read_switches(fh.name), want)
os.unlink(fh.name)
check("equal", rr.switch_differences(want, dict(want, HOME="/x")), [])
check("missing here", len(rr.switch_differences(want, {"AAR_VENUE_FIT": "on"})), 2)
check("extra here", len(rr.switch_differences({}, {"AAR_X": ""})), 1)
check("other value", len(rr.switch_differences({"AAR_X": "on"}, {"AAR_X": "off"})), 1)


# ---- read-only ----
class Conn:
    def __init__(self, values):
        self.values, self.log = values, []

    def cursor(self):
        return self

    def execute(self, sql):
        self.log.append(sql)

    def rollback(self):
        self.log.append("rollback")

    def fetchall(self):
        return [("tx_read_only", v) for v in self.values]

    def close(self):
        pass


conn = Conn(["ON"])
rr._read_only(conn)
check("read-only is set, then the open transaction ended, then checked",
      conn.log[:2], ["SET SESSION TRANSACTION READ ONLY", "rollback"])
for bad in ([], ["OFF"], ["ON", "OFF"]):
    try:
        rr._read_only(Conn(bad))
        check(f"refuses {bad}", False)
    except rr.Refused:
        n += 1
rr.force_read_only()
rr.force_read_only()
check("on every engine in the process", event.contains(Engine, "connect", rr._read_only))
event.remove(Engine, "connect", rr._read_only)


# ---- rosters ----
def rec(cwid, given, surname, campus=CAMPUS_WCM, linked=False):
    r = {"cwid": cwid, "given": given, "middle": "", "surname": surname,
         "given_norm": _norm(given), "surname_norm": _norm(surname), "pref_norm": "",
         "pref_full": "", "dept": "", "dept_names": (), "division": "", "program": "",
         "title": "", "person_type": "Full-Time Faculty", "historical": False,
         "end_year": None, "campus": campus}
    return dict(r, cornell_linked=True) if linked else r


people = ([rec(f"abc100{k}", f"Ann{'abcdefg'[k]}", "Exampleson") for k in range(7)]
          + [rec("bcd1001", "Zed", "Exampleson"), rec("cde1001", "Ann", "Otherson"),
             rec("def1001", "Quentin", "Otherson"), rec("lnk1001", "Ann", "Exampleson", linked=True),
             rec("ith1001", "Ann", "Exampleson", CAMPUS_ITHACA),
             rec("ith1002", "Bob", "Testcase", CAMPUS_ITHACA)])
alts = {"def1001": [{"firstName": "Annabel", "lastName": "Exampleson"}]}
roster = CampusRoster(IdentityIndex(people, alternate_names=alts),
                      ithaca=IdentityIndex(people, campus=CAMPUS_ITHACA))
WCM = "Department of Medicine, Weill Cornell Medicine, New York, NY"
ITH = "Department of Physics, Cornell University, Ithaca, NY"


def row(source, who, pos=1, **kw):
    return dict({"id": 1, "source": source, "pmid": None, "external_id": None, "doi": None,
                 "author_key": f"{source}:fake:{pos - 1}", "author_position": pos,
                 "wcm_author": who, "status": "open", "issn": None, "journal": None,
                 "byline_orcid": None, "top_cwid": None, "candidate_cwids_json": None}, **kw)


# ---- PubMed ----
def pubmed(affils, fore="A", last="Exampleson"):
    uni.efetch_by_ids = lambda pmids, groups: [{
        "pmid": 1, "pub_year": 2020, "doi": None, "authors": [
            {"last": last, "fore": fore, "initials": fore[:1], "affiliations": affils,
             "home_inst": True, "orcid": None, "list_index": 0}]}]


pubmed([WCM])
pm = row("pubmed", "A Exampleson", pmid=1)
out = rr.replay(pm, roster, None, "lnk1001")
check("PubMed: every candidate, not five", out["n_candidates_all"], 8)
check("...and the producer's own five", len(out["producer_cwids_today"]), 5)
check("...on the WCM roster", (out["lane"], out["campuses_consulted"]), ("pubmed", [CAMPUS_WCM]))
cut = out["candidates_all"][-1]["cwid"]


def rules(person, r=pm):
    return [x["rule"] for x in rr.replay(r, roster, None, person)["person"]["rules"]]


check("below the fifth", rules(cut), ["top_5_cut"])
check("listed, any case", rules(out["candidates_all"][0]["cwid"].upper()), [])
check("other initial", rules("bcd1001"), ["given_name"])
check("other surname", rules("cde1001"), ["surname_not_indexed"])
check("alternate name, not spelled out", rules("def1001"), ["alternate_name_needs_full_match"])
check("on the other campus", rules("ith1001"), ["campus_not_consulted"])
check("...which would not have matched either", rules("ith1002"),
      ["campus_not_consulted", "surname_not_indexed"])
check("nobody", rules("xxx9999"), ["not_on_roster"])
drop, five = [c for c in out["candidates_all"] if c["cwid"] != "abc1000"], []
check("absent with no visible rule is not guessed at",
      rr.explain("abc1000", out["author"], 2020, roster, "pubmed", {CAMPUS_WCM}, drop, five)["rules"],
      [{"rule": rr.NOT_DETERMINABLE}])
pubmed([ITH])
check("Ithaca byline: Cornell people, and the WCM person with a Cornell appointment",
      {c["cwid"] for c in rr.replay(pm, roster)["candidates_all"]}, {"ith1001", "lnk1001"})
check("...others of WCM: campus", rules("abc1000"), ["campus_not_consulted"])
pubmed([WCM], fore="B")
try:
    rr.replay(pm, roster)
    check("a moved byline is not replayed", False)
except rr.Unresolvable as e:
    check("a moved byline is not replayed", "byline moved" in str(e))

# ---- Scopus, both lanes ----
W_AFID = sorted(scop.load_family_afids())[0]
I_AFID = sorted(scop.load_family_afids(scop.ITHACA_AFID_LIST))[0]


def scopus(*afids):
    scop._scopus_get = lambda q, start: {"search-results": {"entry": [{
        "eid": "2-s2.0-1", "prism:coverDate": "2020-01-01",
        "affiliation": [{"afid": a, "affilname": "X"} for a in afids],
        "author": [{"surname": "Exampleson", "given-name": "A.", "initials": "A.",
                    "afid": [{"$": a} for a in afids]}]}]}}


sc = row("scopus", "A. Exampleson", external_id="1")
for afids, lane, campuses, size in (
        ([W_AFID], "scopus", [CAMPUS_ITHACA, CAMPUS_WCM][1:], 8),
        ([W_AFID, I_AFID], "scopus", sorted([CAMPUS_ITHACA, CAMPUS_WCM]), 9),
        ([I_AFID], "scopus-ithaca", [CAMPUS_ITHACA], 1)):
    scopus(*afids)
    o = rr.replay(sc, roster)
    check(f"Scopus {lane} {len(afids)}", (o["lane"], o["campuses_consulted"], o["n_candidates_all"]),
          (lane, campuses, size))
check("the Ithaca lane never sees a linked WCM person", rules("lnk1001", sc), ["campus_not_consulted"])
scopus("0")
try:
    rr.replay(sc, roster)
    check("no family afid is unresolvable", False)
except rr.Unresolvable:
    n += 1

# ---- OpenAlex ----
asked = []


def openalex(params):
    asked.append(params["filter"])
    return {"results": [] if params["filter"].startswith("doi:") else [{
        "id": "https://openalex.org/W1", "publication_year": 2020, "authorships": [{
            "raw_author_name": "A Exampleson", "raw_affiliation_strings": [WCM],
            "institutions": [{"id": "https://openalex.org/" + oa.WCM_OPENALEX_IDS[0]}]}]}]}


oa._openalex_get = openalex
o = rr.replay(row("openalex", "A Exampleson", doi="10.1/X", external_id="W1"), roster)
check("OpenAlex: DOI, then the work id", asked, ["doi:10.1/x", "ids.openalex:W1"])
check("...every candidate", (o["lane"], o["n_candidates_all"], len(o["producer_cwids_today"])),
      ("openalex", 8, 5))

print(f"\n{n}/{n} passed\n")
