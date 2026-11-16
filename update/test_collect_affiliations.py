"""collectAffiliations.py: mode gating (off does nothing, dry-run writes nothing), the
source parsers, the scope key, matching, the run_all.py hook, and the migration's place.
No network and no database: the engine and the three fetchers are fakes.

Run: python3 -m pytest update/test_collect_affiliations.py -q
"""
import os
import re
import sys
import tempfile
import types
from datetime import datetime, timedelta
from decimal import Decimal

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

# run_all imports boto3/psutil at module scope; stubbed so this runs on a bare checkout
# (same approach as test_article_enrichment.py).
for _name in ("psutil", "boto3", "botocore"):
    try:
        __import__(_name)
    except ImportError:
        sys.modules[_name] = types.ModuleType(_name)
try:
    from botocore.config import Config  # noqa: F401
except ImportError:
    _cfg = types.ModuleType("botocore.config")
    _cfg.Config = object
    sys.modules["botocore.config"] = _cfg
    sys.modules["botocore"].config = _cfg

os.environ.setdefault("LOG_FILE", os.path.join(tempfile.gettempdir(), "test_collect_affiliations.log"))
os.environ.setdefault("S3_BUCKET", "unused-in-this-test")
os.environ.setdefault("S3_KEY_PREFIX", "unused-in-this-test")

import collectAffiliations as ca   # noqa: E402
import run_all                     # noqa: E402
import watchlist_patterns as wp    # noqa: E402

WRITE_VERBS = re.compile(r"^\s*(INSERT|UPDATE|DELETE|REPLACE|CREATE|ALTER|DROP|TRUNCATE|RENAME)\b", re.I)


# ---- fakes ---------------------------------------------------------------------------
class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return iter(self.rows)

    def first(self):
        return self.rows[0] if self.rows else None

    def scalar(self):
        return self.rows[0][0] if self.rows else None

    def __iter__(self):
        return iter(self.rows)


class FakeConn:
    def __init__(self, eng):
        self.eng = eng

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execution_options(self, **kw):
        return self

    def execute(self, sql, params=None):
        self.eng.log.append((sql, params))
        if self.eng.fail and self.eng.fail(sql, params):
            raise RuntimeError("simulated database error")
        return FakeResult(self.eng.answer(sql, params))


class FakeEngine:
    """Answers the script's reads from fixtures and records every statement."""

    def __init__(self, tables=ca.TABLES, done=(), stored_aff=(), stored_funders=(), state=(), fail=None):
        self.tables = set(tables) | {"authorship_review", "identity"}
        self.done, self.stored_aff, self.stored_funders = done, list(stored_aff), list(stored_funders)
        self.state = list(state)          # extra article_affiliation_state rows, as dicts
        self.fail = fail                  # fail(sql, params) -> True makes that statement raise
        self.log, self.begins = [], 0

    def connect(self):
        return FakeConn(self)

    def begin(self):
        self.begins += 1
        return FakeConn(self)

    def answer(self, sql, params):
        if "@@hostname" in sql:
            return [("testhost",)]
        if "information_schema.tables" in sql:
            return [(1,)] if params["t"] in self.tables else []
        if "FROM analysis_summary_author" in sql:
            assert params == {"ext": ca.EXT_COHORTS}
            return [{"cwid": "abc2001", "pmid": 111, "article_id": None, "doi": None},
                    {"cwid": "abc2001", "pmid": 222, "article_id": None, "doi": "10.1000/two"},
                    {"cwid": "ABC2001", "pmid": -7, "article_id": "SCOPUS:85100000001", "doi": "10.1000/Ext"},
                    {"cwid": "abc2001", "pmid": -8, "article_id": "SCOPUS:85100000002", "doi": None}]
        if "FROM person_article" in sql:
            return [{"cwid": "xyz9001", "pmid": 333, "doi": None}]
        if "FROM authorship_review" in sql:
            return [{"cwid": None, "pmid": 444, "doi": None, "source": "pubmed", "external_id": None},
                    {"cwid": "xyz9001", "pmid": None, "doi": "https://doi.org/10.1000/Review",
                     "source": "scopus", "external_id": "85100000003"}]
        if "FROM person" in sql:
            return [{"cwid": "abc2001", "firstName": "Yi-Ming", "lastName": "Wang"},
                    {"cwid": "xyz9001", "firstName": "Jane", "lastName": "Doe"}]
        if "FROM identity" in sql:
            return [{"cwid": "abc2001", "givenName": "Yiming", "surname": "Wang"}]
        if sql == ca.SEL_STATE:
            return [{"article_id": a, "source": "dynamodb", "attempts": 1, "fetched_at": datetime(2026, 1, 1)}
                    for a in self.done] + self.state
        if sql == ca.SEL_AFF:
            return self.stored_aff
        if sql == ca.SEL_FUNDER:
            return self.stored_funders
        return []

    def writes(self):
        return [sql for sql, _ in self.log if WRITE_VERBS.match(sql)]


def ddb_item(pmid, authors, grants=None):
    art = {"authorlist": authors}
    if grants is not None:
        art["grantlist"] = grants
    return {"pmid": Decimal(pmid), "pubmedarticle": {"medlinecitation": {"article": art}}}


ITEM_222 = ddb_item(222, [
    {"forename": "Yi Ming", "lastname": "Wang", "affiliation": "Weill Cornell Medicine, New York, NY, USA."},
    {"forename": "Li", "lastname": "Zhang", "affiliation": "Peking University Third Hospital, Beijing, China."},
], [{"agency": "China Scholarship Council", "grantid": "2019", "country": "China", "acronym": None}])


def fake_ddb(pmids):
    """222 is in DynamoDB; 444 is there with no author list; 333 is absent."""
    items = [ITEM_222] if 222 in pmids else []
    if 444 in pmids:
        items.append({"pmid": Decimal(444)})
    return items, set()


def no_call(*a, **k):
    raise AssertionError("a dry run must not call efetch or OpenAlex")


def text(sql):
    return sql


# ---- mode gating -------------------------------------------------------------------------
def test_resolve_mode_typo_is_off():
    assert [ca.resolve_mode(v) for v in (None, "", "off", "aply", "on", "true", "dryrun")] == ["off"] * 7
    assert ca.resolve_mode(" Apply ") == "apply" and ca.resolve_mode("dry-run") == "dry-run"


@pytest.mark.parametrize("value", [None, "off", "aply", "on"])
def test_off_does_nothing(monkeypatch, capsys, value):
    if value is None:
        monkeypatch.delenv("RESEARCH_SECURITY_MODE", raising=False)
    else:
        monkeypatch.setenv("RESEARCH_SECURITY_MODE", value)
    monkeypatch.setattr(ca, "run", no_call)
    # reaching the database would need these; make any attempt fail loudly
    monkeypatch.setitem(sys.modules, "aar_db", None)
    assert ca.main([]) == 0
    assert "nothing done" in capsys.readouterr().out


def test_dry_run_writes_nothing(capsys):
    eng = FakeEngine(done=["111"], stored_aff=[
        ("111", 1, "Yiming Wang", "Tsinghua University, Beijing, China"),
        ("111", 2, "Ann Lee", "Weill Cornell Medicine, New York, USA")],
        stored_funders=[("111", "Shanghai Pujiang Program", "21PJ")])
    stats = ca.run("dry-run", eng, text, ddb_fetch=fake_ddb, efetch=no_call, openalex=no_call)
    assert eng.begins == 0 and eng.writes() == []
    assert stats["by_source"] == {"dynamodb": 1} and stats["http_calls"] == 0
    # stored (111: Tsinghua + China, Pujiang) and read tonight (222: Peking + China, CSC)
    assert stats["matches"] == {wp.HOUSE: 2, wp.COUNTRY: 2, wp.FUNDING: 2}
    out = capsys.readouterr().out
    assert "would fetch 2 PMIDs by efetch and 2 external articles from OpenAlex; no call made" in out
    assert "1 external articles have no DOI or OpenAlex id" in out
    assert f"{wp.HOUSE}: 2 rows on 2 articles, 1 where our author lists the entity" in out


def test_dry_run_works_before_the_migration():
    eng = FakeEngine(tables=())
    stats = ca.run("dry-run", eng, text, ddb_fetch=fake_ddb, efetch=no_call, openalex=no_call)
    assert eng.begins == 0 and eng.writes() == []
    assert stats["todo"] == 7 and stats["matches"][wp.HOUSE] == 1


def test_apply_refuses_before_the_migration(capsys):
    eng = FakeEngine(tables=())
    assert ca.run("apply", eng, text, ddb_fetch=no_call, efetch=no_call, openalex=no_call) is None
    assert eng.begins == 0 and eng.writes() == []
    assert "table_research_security_v3.13.sql" in capsys.readouterr().out


def test_apply_collects_stamps_and_swaps(capsys):
    eng = FakeEngine(done=["111"])
    efetched, oa_calls = [], []

    def efetch(pmids):
        efetched.append(list(pmids))
        return ca.parse_efetch(EFETCH_XML)            # has 333, not 444

    def openalex(kind, values):
        oa_calls.append((kind, list(values)))
        return {"10.1000/ext": OA_WORK}               # 10.1000/review is a miss

    stats = ca.run("apply", eng, text, ddb_fetch=fake_ddb, efetch=efetch, openalex=openalex)
    assert efetched == [[444, 333]]                   # newest first; 111 already collected
    assert oa_calls == [("doi", ["10.1000/ext", "10.1000/review"])]
    assert stats["by_source"] == {"dynamodb": 1, "efetch": 1, "openalex": 1, "none": 2}
    assert stats["truncated"] == 0

    state = {r["article_id"]: r for sql, p in eng.log if sql == ca.INS_STATE for r in p}
    assert {k: v["source"] for k, v in state.items()} == {
        "222": "dynamodb", "333": "efetch", "444": "none",
        "SCOPUS:85100000001": "openalex", "SCOPUS:85100000003": "none"}   # the review row keeps its accept-time id
    assert state["222"]["n_authors"] == 2 and state["222"]["n_affiliations"] == 2 and state["222"]["n_funders"] == 1
    assert "SCOPUS:85100000002" not in state          # no DOI: not retrievable, not stamped
    aff = [r for sql, p in eng.log if sql == ca.INS_AFF for r in p]
    assert all(not str(r["article_id"]).startswith("-") for r in aff)
    assert {(r["article_id"], r["rank"], r["seq"]) for r in aff} >= {("222", 1, 0), ("222", 2, 0), ("333", 1, 0), ("333", 1, 1)}

    writes = eng.writes()
    for t in (ca.T_MATCH, ca.T_FLAG):
        assert f"CREATE TABLE IF NOT EXISTS {t}_new LIKE {t}" in writes
        assert f"TRUNCATE TABLE {t}_new" in writes
        assert not any(w.startswith(f"TRUNCATE TABLE {t}") and not w.endswith("_new") for w in writes)
    # both tables change hands in ONE statement, after the staging tables are filled
    renames = [w for w in writes if w.startswith("RENAME")]
    assert renames == [ca.SQL_SWAP] == [
        "RENAME TABLE watchlist_match TO watchlist_match_old, watchlist_match_new TO watchlist_match, "
        "watchlist_funding_flag TO watchlist_funding_flag_old, watchlist_funding_flag_new TO watchlist_funding_flag"]
    assert writes.index(f"TRUNCATE TABLE {ca.T_FLAG}_new") < writes.index(ca.SQL_SWAP)
    assert not any(re.match(rf"DROP TABLE (IF EXISTS )?({ca.T_MATCH}|{ca.T_FLAG})\b(?!_old)", w) for w in writes)
    assert stats["swapped"] is True and stats["write_failed"] == 0
    assert "rebuilt" in capsys.readouterr().out


def test_apply_rebuilds_matches_from_stored_strings():
    eng = FakeEngine(done=["111", "222", "333", "444", "SCOPUS:85100000001", "SCOPUS:85100000003"],
                     stored_aff=[("111", 1, "Yiming Wang", "Tsinghua University, Beijing, China"),
                                 ("111", 1, "Yiming Wang", "Tsinghua University Hospital, Beijing, China"),
                                 ("111", 3, "Bo Li", "Harbin Institute of Technology, Harbin, China")],
                     stored_funders=[("111", "China Scholarship Council", "1"),
                                     ("111", "China Scholarship Council", "2")])
    stats = ca.run("apply", eng, text, ddb_fetch=no_call, efetch=no_call, openalex=no_call)
    assert stats["todo"] == 1 and stats["http_calls"] == 0     # the one external with no DOI
    rows = [r for sql, p in eng.log if sql == ca.INS_MATCH for r in p]
    assert {(r["list_code"], r["entity"], r["rank"], r["is_institution_author"]) for r in rows} == {
        (wp.HOUSE, "Tsinghua University", 1, 1), (wp.COUNTRY, "China", 1, 1),
        (wp.USUAL, "Harbin Institute of Technology", 3, 0), (wp.COUNTRY, "China", 3, 0)}
    tsinghua = [r for r in rows if r["entity"] == "Tsinghua University"]
    assert len(tsinghua) == 1 and tsinghua[0]["affiliation"] == "Tsinghua University, Beijing, China"
    assert tsinghua[0]["category"] == "SASTIND university" and tsinghua[0]["list_no"] == 45
    flags = [r for sql, p in eng.log if sql == ca.INS_FLAG for r in p]
    assert flags == [{"article_id": "111", "program": "China Scholarship Council (CSC)",
                      "agency": "China Scholarship Council", "grant_id": "1"}]


def test_http_cap_truncates_and_says_so(capsys):
    eng = FakeEngine(done=["111"])
    calls = []
    stats = ca.run("apply", eng, text, ddb_fetch=fake_ddb, max_http_calls=1,
                   efetch=lambda pmids: calls.append("efetch") or {},
                   openalex=lambda kind, values: calls.append("openalex") or {})
    assert calls == ["efetch"] and stats["http_calls"] == 1 and stats["truncated"] == 2
    assert "TRUNCATED: 2 articles left for the next run (RESEARCH_SECURITY_MAX_HTTP_CALLS=1" in capsys.readouterr().out
    state = {r["article_id"] for sql, p in eng.log if sql == ca.INS_STATE for r in p}
    assert not any(a.startswith(("SCOPUS:", "DOI:")) for a in state)   # left unstamped: retried


def test_failed_fetch_stamps_nothing():
    eng = FakeEngine(done=["111"])

    def boom(*a, **k):
        raise RuntimeError("simulated outage")

    stats = ca.run("apply", eng, text, ddb_fetch=boom, efetch=boom, openalex=boom)
    assert stats["by_source"] == {}
    assert not any(sql in (ca.INS_STATE, ca.INS_AFF, ca.INS_FUNDER) for sql, _ in eng.log)


def test_unprocessed_dynamo_keys_are_not_sent_to_efetch():
    eng = FakeEngine(done=["111", "SCOPUS:85100000001", "SCOPUS:85100000003"])
    seen = []
    ca.run("apply", eng, text, ddb_fetch=lambda pmids: ([], {444}),
           efetch=lambda pmids: seen.extend(pmids) or {}, openalex=no_call)
    assert sorted(seen) == [222, 333]


def test_every_statement_is_parameterized():
    for sql in (ca.SQL_ACCEPTED, ca.SQL_PENDING, ca.SQL_REVIEW):
        assert ":ext" in sql and "REGEXP '" not in sql
    for sql in (ca.INS_AFF, ca.INS_FUNDER, ca.INS_STATE, ca.INS_MATCH, ca.INS_FLAG):
        assert "'" not in sql and "%s" not in sql
        cols = re.search(r"\((.*?)\) VALUES", sql).group(1).replace("`", "").split(", ")
        assert all(f":{c}" in sql for c in cols)


# ---- DynamoDB record parser -----------------------------------------------------------------
def test_parse_dynamo_null_entries_and_collective_names():
    rec = ca.parse_dynamo_item(ddb_item(42527780, [
        {"forename": "Ana", "lastname": "García", "affiliation": " Weill Cornell Medicine, New York. "},
        None,
        {"forename": None, "lastname": None, "affiliation": None, "collectivename": None},
        {"collectivename": "COVID-19 Genomics Consortium", "forename": None, "lastname": None, "affiliation": None},
        {"forename": "Bo", "lastname": "Li", "affiliation": None},
        {"forename": "Wei", "lastname": "Chen", "affiliation": "Jilin University, Changchun, China"},
    ]))
    assert rec["article_id"] == "42527780" and rec["source"] == "dynamodb"
    assert rec["n_authors"] == 4                       # two all-null entries are not authors
    assert rec["authors"] == [(1, "Ana García", ["Weill Cornell Medicine, New York."]),
                              (6, "Wei Chen", ["Jilin University, Changchun, China"])]
    assert rec["funders"] == []                        # no grantlist key at all


def test_parse_dynamo_collective_name_with_affiliation_and_grants():
    rec = ca.parse_dynamo_item(ddb_item(5, [
        {"collectivename": "Beijing Study Group", "affiliation": "Peking University, Beijing, China"}],
        [{"agency": "NCI NIH HHS", "grantid": "R01 CA1", "country": "United States", "acronym": "CA"},
         {"agency": None, "grantid": None, "country": None, "acronym": None},
         None,
         {"agency": "China Scholarship Council", "grantid": None, "country": None}]))
    assert rec["authors"] == [(1, "Beijing Study Group", ["Peking University, Beijing, China"])]
    assert rec["funders"] == [("NCI NIH HHS", "R01 CA1", "United States"), ("China Scholarship Council", "", "")]


@pytest.mark.parametrize("item", [
    {"pmid": 9},                                                      # stored without its body
    {"pmid": 9, "pubmedarticle": None},
    {"pmid": 9, "pubmedarticle": {"medlinecitation": {"article": {}}}},
    ddb_item(9, None),
    ddb_item(9, []),
    ddb_item(9, [None, {"forename": None, "lastname": None, "affiliation": None}]),
])
def test_parse_dynamo_without_an_author_list_falls_back(item):
    assert ca.parse_dynamo_item(item) is None


def test_rows_for_counts_and_clips():
    rec = ca._record(7, "efetch", [(2, "A B", ["x" * 20000, "second"])], [("ag", "id", "")], 5)
    aff, fund, state = ca.rows_for([rec, ca.empty_record("SCOPUS:1")], "NOW")
    assert [(r["rank"], r["seq"], len(r["affiliation"])) for r in aff] == [(2, 0, ca.AFF_MAX), (2, 1, 6)]
    assert fund == [{"article_id": "7", "seq": 0, "agency": "ag", "grant_id": "id", "country": None, "source": "efetch"}]
    assert state == [
        {"article_id": "7", "source": "efetch", "n_authors": 5, "n_affiliations": 2, "n_funders": 1, "fetched_at": "NOW"},
        {"article_id": "SCOPUS:1", "source": "none", "n_authors": 0, "n_affiliations": 0, "n_funders": 0, "fetched_at": "NOW"}]


# ---- efetch and OpenAlex parsers ---------------------------------------------------------------
EFETCH_XML = b"""<PubmedArticleSet><PubmedArticle><MedlineCitation><PMID Version="1">333</PMID>
<Article><AuthorList>
<Author><LastName>Doe</LastName><ForeName>Jane</ForeName>
  <AffiliationInfo><Affiliation>Weill Cornell Medicine, New York, USA.</Affiliation></AffiliationInfo>
  <AffiliationInfo><Affiliation>Fudan University, Shanghai, China.</Affiliation></AffiliationInfo></Author>
<Author><CollectiveName>Study Group</CollectiveName></Author>
<Author><LastName>Roe</LastName><ForeName>R</ForeName></Author>
</AuthorList>
<GrantList><Grant><GrantID>R01</GrantID><Agency>NIH HHS</Agency><Country>United States</Country></Grant></GrantList>
</Article><CommentsCorrectionsList><CommentsCorrections><PMID>999</PMID></CommentsCorrections></CommentsCorrectionsList>
</MedlineCitation></PubmedArticle></PubmedArticleSet>"""

OA_WORK = {"id": "https://openalex.org/W1", "doi": "https://doi.org/10.1000/EXT", "authorships": [
    {"author": {"display_name": "Yiming Wang"}, "raw_affiliation_strings": ["Cornell University, Ithaca, NY"],
     "institutions": [{"display_name": "Cornell University"}, {"display_name": None}]},
    {"author": None, "raw_affiliation_strings": None, "institutions": None},
    {"author": {"display_name": "Li Zhang"}, "raw_affiliation_strings": [], "institutions": [{"display_name": "Xidian University"}]}],
    "awards": [{"funder_display_name": "China Scholarship Council", "funder_award_id": None}]}


def test_parse_efetch():
    recs = ca.parse_efetch(EFETCH_XML)
    assert list(recs) == [333]
    r = recs[333]
    assert r["n_authors"] == 3 and r["source"] == "efetch"
    assert r["authors"] == [(1, "Jane Doe", ["Weill Cornell Medicine, New York, USA.", "Fudan University, Shanghai, China."])]
    assert r["funders"] == [("NIH HHS", "R01", "United States")]


def test_parse_openalex_work():
    r = ca.parse_openalex_work("SCOPUS:85100000001", OA_WORK)
    assert r["article_id"] == "SCOPUS:85100000001" and r["n_authors"] == 3
    assert r["authors"] == [(1, "Yiming Wang", ["Cornell University, Ithaca, NY", "Cornell University"]),
                            (3, "Li Zhang", ["Xidian University"])]
    assert r["funders"] == [("China Scholarship Council", "", "")]
    assert ca.parse_openalex_work("X", {})["n_authors"] == 0


# ---- scope and matching -------------------------------------------------------------------------
def test_article_key_is_never_a_negative_pmid():
    assert ca.article_key(123) == "123" and ca.article_key(Decimal(123), "SCOPUS:1") == "123"
    assert ca.article_key(-7, "SCOPUS:85100000001", "10.1/x") == "SCOPUS:85100000001"
    assert ca.article_key(-7, None, "https://doi.org/10.1000/ABC") == "DOI:10.1000/abc"
    assert ca.article_key(None, None, "10.1000/abc") == "DOI:10.1000/abc"
    assert ca.article_key(-7) is None and ca.article_key(0, "", "not a doi") is None


def test_build_scope():
    articles, people, skipped = ca.build_scope(
        [{"cwid": "ABC2001", "pmid": 1, "article_id": None, "doi": None},
         {"cwid": "def2002", "pmid": 1, "article_id": None, "doi": "10.1000/One"},
         {"cwid": "abc2001", "pmid": -3, "article_id": "OPENALEX:W123456", "doi": None},
         {"cwid": "abc2001", "pmid": -4, "article_id": None, "doi": None}],
        [{"cwid": "ghi2003", "pmid": 2, "doi": None}],
        [{"cwid": None, "pmid": 1, "doi": None}, {"cwid": None, "pmid": None, "doi": None}])
    assert articles == {"1": "10.1000/one", "OPENALEX:W123456": None, "2": None}
    assert people == {"1": {"abc2001", "def2002"}, "OPENALEX:W123456": {"abc2001"}, "2": {"ghi2003"}}
    assert skipped == {"no_key": 2}                     # the row with only a negative pmid, the empty review row


def test_review_row_gets_the_id_it_will_have_once_accepted():
    # the Publication Manager accept builds UPPER(source):external_id
    assert ca.article_key(None, None, "10.1000/x", "scopus", "85100000001") == "SCOPUS:85100000001"
    assert ca.article_key(None, None, None, "openalex", "W4400000001") == "OPENALEX:W4400000001"
    assert ca.article_key(None, None, "10.1000/x", "scopus", None) == "DOI:10.1000/x"
    assert ca.article_key(555, None, "10.1000/x", "pubmed", None) == "555"
    accepted = [{"cwid": "abc1", "pmid": -5, "article_id": "SCOPUS:85100000001", "doi": "10.1000/x"}]
    # same paper, accepted for one author and still open in review for another: ONE article
    articles, people, skipped = ca.build_scope(accepted, [], [
        {"cwid": "xyz1", "pmid": None, "doi": "10.1000/X", "source": "scopus", "external_id": "85100000001"}])
    assert articles == {"SCOPUS:85100000001": "10.1000/x"}
    assert people == {"SCOPUS:85100000001": {"abc1", "xyz1"}} and not skipped
    # a review row with no external id falls back to DOI:, and is folded onto the same DOI
    articles, people, skipped = ca.build_scope(accepted, [], [
        {"cwid": "xyz1", "pmid": None, "doi": "https://doi.org/10.1000/X", "source": "scopus", "external_id": None},
        {"cwid": "xyz2", "pmid": None, "doi": "10.1000/other", "source": "scopus", "external_id": None},
        {"cwid": "xyz3", "pmid": None, "doi": None, "source": "openalex", "external_id": "W4400000001"},
        {"cwid": "xyz4", "pmid": None, "doi": None, "source": "scopus", "external_id": None}])
    assert articles == {"SCOPUS:85100000001": "10.1000/x", "DOI:10.1000/other": "10.1000/other",
                        "OPENALEX:W4400000001": None}
    assert people["SCOPUS:85100000001"] == {"abc1", "xyz1"}
    assert skipped == {"folded": 1, "no_key": 1}        # the keyless row is counted, not silently dropped


def test_scope_leaves_out_ids_the_column_cannot_hold_and_keeps_one_spelling():
    long_doi = "10.1000/" + "x" * 100                  # authorship_review.doi is VARCHAR(255)
    articles, people, skipped = ca.build_scope(
        [{"cwid": "abc1", "pmid": -1, "article_id": "OPENALEX:W123", "doi": None},
         {"cwid": "abc2", "pmid": -2, "article_id": "openalex:w123", "doi": None}], [],
        [{"cwid": "xyz1", "pmid": None, "doi": long_doi, "source": "scopus", "external_id": None}])
    assert list(articles) == ["OPENALEX:W123"] and people == {"OPENALEX:W123": {"abc1", "abc2"}}
    assert skipped == {"too_long": 1}
    assert ca._ci("OPENALEX:W123 ") == ca._ci("openalex:w123") and ca._ci("É") == ca._ci("e")


def test_external_cohort_filter():
    rx = re.compile(ca.EXT_COHORTS)
    assert all(rx.search(c) for c in ("fh_123", "fredhutch_ab", "ucsf_1", "ucla_2", "uc_3", "usc_4"))
    assert not any(rx.search(c) for c in ("abc2001", "uca2001", "usc2001", "cu_ab12"))


def test_is_institution_author_is_a_name_match():
    keys = ca.name_keys([("ABC2001", "Yi-Ming", "Wang"), ("abc2001", "Yiming", "Wang"),
                         ("nofirst", "", "Smith"), ("nolast", "Ann", None)])
    assert keys == {"abc2001": {("wang", "yiming")}}
    rows = ca.match_article("1", [
        (1, "Yi Ming Wang", "Peking University, Beijing, China"),
        (2, "Y Wang", "Peking University, Beijing, China"),            # initials only: never
        (3, "Yiming Wang-Smith", "Peking University, Beijing, China"),
        (4, "Peking University Study Group", "Peking University, Beijing, China"),
        (5, "No Affiliation", None)], keys["abc2001"])
    got = {r["rank"]: r["is_institution_author"] for r in rows if r["list_code"] == wp.HOUSE}
    assert got == {1: 1, 2: 0, 3: 0, 4: 0}


@pytest.mark.parametrize("byline, ours", [
    ("Bruce R Schackman", True),            # the normal PubMed form: ForeName carries the middle initial
    ("Bruce R. Schackman", True),
    ("Bruce Schackman", True),
    ("Bruce R J Schackman", True),
    ("B R Schackman", False),               # initials only: never
    ("Bruce Robert Schackman", False),      # a second full given name is not assumed to be a middle name
    ("Bruce R Schackmann", False),
    ("Jan van der Berg", True),             # multi-word surname
    ("Jan H van der Berg", True),
    ("Jan Berg", False),
    ("Yi Ming Wang", False),                # not our "Yi Wang"
    ("Yi Wang", True),
    ("", False), (None, False),
])
def test_is_our_author_middle_initials_and_multi_word_surnames(byline, ours):
    keys = ca.name_keys([("brs2001", "Bruce", "Schackman"), ("jvb2001", "Jan", "van der Berg"),
                         ("yiw2001", "Yi", "Wang")])
    all_keys = {k for v in keys.values() for k in v}
    assert ca.is_our_author(byline, all_keys) is ours


def test_match_article_flags_our_author_with_a_middle_initial():
    keys = ca.name_keys([("brs2001", "Bruce", "Schackman")])["brs2001"]
    rows = ca.match_article("1", [(3, "Bruce R Schackman", "Peking University, Beijing, China")], keys)
    assert [r["is_institution_author"] for r in rows] == [1, 1]         # the house row and the country row


# ---- shared affiliation blob ("A is with X. B is with Y." on every author) ---------------------
BLOB = ("Li Zhang is with Peking University, Beijing, China. "
        "Bruce R. Schackman is with Weill Cornell Medicine, New York, NY.")


def test_shared_blob_is_reduced_to_each_authors_own_clause():
    rec = ca.parse_dynamo_item(ddb_item(42753209, [
        {"forename": "Li", "lastname": "Zhang", "affiliation": BLOB},
        {"forename": "Bruce R", "lastname": "Schackman", "affiliation": BLOB}]))
    assert rec["authors"] == [(1, "Li Zhang", ["Peking University, Beijing, China."]),
                              (2, "Bruce R Schackman", ["Weill Cornell Medicine, New York, NY."])]
    keys = ca.name_keys([("brs2001", "Bruce", "Schackman")])["brs2001"]
    rows = ca.match_article("42753209", ((rank, name, a) for rank, name, affs in rec["authors"] for a in affs), keys)
    house = [r for r in rows if r["list_code"] == wp.HOUSE]
    assert [(r["entity"], r["rank"], r["is_institution_author"]) for r in house] == [("Peking University", 1, 0)]
    assert not [r for r in rows if r["rank"] == 2]      # nothing is pinned on our author

    xml = ("<PubmedArticleSet><PubmedArticle><MedlineCitation><PMID>42753209</PMID><Article><AuthorList>"
           f"<Author><LastName>Zhang</LastName><ForeName>Li</ForeName><AffiliationInfo><Affiliation>{BLOB}"
           "</Affiliation></AffiliationInfo></Author>"
           f"<Author><LastName>Schackman</LastName><ForeName>Bruce R</ForeName><AffiliationInfo><Affiliation>{BLOB}"
           "</Affiliation></AffiliationInfo></Author>"
           "</AuthorList></Article></MedlineCitation></PubmedArticle></PubmedArticleSet>").encode()
    assert ca.parse_efetch(xml)[42753209]["authors"] == rec["authors"]


def test_a_string_on_one_author_or_without_the_idiom_is_left_alone():
    one = ca.parse_dynamo_item(ddb_item(1, [{"forename": "Li", "lastname": "Zhang", "affiliation": BLOB}]))
    assert one["authors"] == [(1, "Li Zhang", [BLOB])]
    same = "Peking University, Beijing, China"
    two = ca.parse_dynamo_item(ddb_item(1, [{"forename": "Li", "lastname": "Zhang", "affiliation": same},
                                            {"forename": "Bo", "lastname": "Li", "affiliation": same}]))
    assert [a[2] for a in two["authors"]] == [[same], [same]]


# ---- DynamoDB joins an author's affiliations into one string ------------------------------------
JOINED_ITEM = ddb_item(555, [{"forename": "Wei", "lastname": "Chen", "affiliation":
                              "Soochow University, Suzhou, China, Kaohsiung Medical University, Kaohsiung, Taiwan"}])
SPLIT_XML = b"""<PubmedArticleSet><PubmedArticle><MedlineCitation><PMID>555</PMID><Article><AuthorList>
<Author><LastName>Chen</LastName><ForeName>Wei</ForeName>
<AffiliationInfo><Affiliation>Soochow University, Suzhou, China</Affiliation></AffiliationInfo>
<AffiliationInfo><Affiliation>Kaohsiung Medical University, Kaohsiung, Taiwan</Affiliation></AffiliationInfo>
</Author></AuthorList></Article></MedlineCitation></PubmedArticle></PubmedArticleSet>"""


class OnePmid(FakeEngine):
    """Scope is the single PMID 555."""

    def answer(self, sql, params):
        if "FROM analysis_summary_author" in sql:
            return [{"cwid": "abc2001", "pmid": 555, "article_id": None, "doi": None}]
        if "FROM person_article" in sql or "FROM authorship_review" in sql:
            return []
        return super().answer(sql, params)


def test_guard_sensitive_dynamo_record_is_refetched_as_separate_affiliations():
    joined = ca.parse_dynamo_item(JOINED_ITEM)["authors"][0][2]
    assert len(joined) == 1 and wp.match_affiliation(joined[0]) == []   # the joined string loses both hits
    eng, asked = OnePmid(), []

    def efetch(pmids):
        asked.extend(pmids)
        return ca.parse_efetch(SPLIT_XML)

    stats = ca.run("apply", eng, text, ddb_fetch=lambda pmids: ([JOINED_ITEM], set()), efetch=efetch, openalex=no_call)
    assert asked == [555] and stats["by_source"] == {"efetch": 1}
    aff = [r for sql, p in eng.log if sql == ca.INS_AFF for r in p]
    assert [(r["seq"], r["affiliation"], r["source"]) for r in aff] == [
        (0, "Soochow University, Suzhou, China", "efetch"),
        (1, "Kaohsiung Medical University, Kaohsiung, Taiwan", "efetch")]
    hits = {(c, e) for a in aff for c, _, _, e in wp.match_affiliation(a["affiliation"])}
    assert hits == {(wp.HOUSE, "Soochow University"), (wp.COUNTRY, "China")}


def test_guard_sensitive_dynamo_record_is_kept_when_efetch_lacks_the_pmid():
    eng = OnePmid()
    stats = ca.run("apply", eng, text, ddb_fetch=lambda pmids: ([JOINED_ITEM], set()),
                   efetch=lambda pmids: {}, openalex=no_call)
    assert stats["by_source"] == {"dynamodb": 1}
    state = [r for sql, p in eng.log if sql == ca.INS_STATE for r in p]
    assert [(r["article_id"], r["source"], r["n_affiliations"]) for r in state] == [("555", "dynamodb", 1)]


def test_plain_dynamo_record_is_not_refetched():
    eng = OnePmid()
    plain = ddb_item(555, [{"forename": "Wei", "lastname": "Chen", "affiliation": "Peking University, Beijing, China"}])
    stats = ca.run("apply", eng, text, ddb_fetch=lambda pmids: ([plain], set()), efetch=no_call, openalex=no_call)
    assert stats["by_source"] == {"dynamodb": 1} and stats["http_calls"] == 0


# ---- a failed write, a stuck swap, a miss retried ------------------------------------------------
def test_one_bad_row_costs_one_article_and_the_rebuild_still_runs(capsys):
    def fail(sql, params):
        return sql == ca.INS_STATE and any(r["article_id"] == "333" for r in params)

    eng = FakeEngine(done=["111"], fail=fail)
    stats = ca.run("apply", eng, text, ddb_fetch=fake_ddb,
                   efetch=lambda pmids: ca.parse_efetch(EFETCH_XML),
                   openalex=lambda kind, values: {"10.1000/ext": OA_WORK})
    assert stats["write_failed"] == 1 and stats["swapped"] is True
    assert stats["by_source"] == {"dynamodb": 1, "openalex": 1, "none": 2}     # 333 (efetch) is not counted
    ok = [r["article_id"] for sql, p in eng.log if sql == ca.INS_STATE for r in p
          if not any(x["article_id"] == "333" for x in p)]
    assert sorted(ok) == ["222", "444", "SCOPUS:85100000001", "SCOPUS:85100000003"]
    assert ca.SQL_SWAP in eng.writes()                                         # the screen was still reached
    out = capsys.readouterr().out
    assert "could not write article 333" in out and "WRITE FAILED for 1 articles" in out


def test_swap_waits_a_bounded_time_and_keeps_yesterdays_tables_when_it_cannot_lock(capsys):
    eng = FakeEngine(done=["111", "222", "333", "444", "SCOPUS:85100000001", "SCOPUS:85100000003"])
    ca.run("apply", eng, text, ddb_fetch=no_call, efetch=no_call, openalex=no_call)
    stmts = [sql for sql, _ in eng.log]
    lock = f"SET SESSION lock_wait_timeout = {ca.SWAP_LOCK_WAIT}"
    assert ca.SWAP_LOCK_WAIT <= 120 and lock in stmts and stmts.index(lock) < stmts.index(ca.SQL_SWAP)

    eng = FakeEngine(done=["111", "222", "333", "444", "SCOPUS:85100000001", "SCOPUS:85100000003"],
                     fail=lambda sql, params: sql == ca.SQL_SWAP)       # Lock wait timeout exceeded
    stats = ca.run("apply", eng, text, ddb_fetch=no_call, efetch=no_call, openalex=no_call)
    assert stats["swapped"] is False
    after = [sql for sql, _ in eng.log][[sql for sql, _ in eng.log].index(ca.SQL_SWAP) + 1:]
    assert not any(WRITE_VERBS.match(sql) for sql in after)             # nothing dropped after the failure
    assert "SWAP FAILED" in capsys.readouterr().out


def test_main_exits_1_when_the_swap_failed(monkeypatch):
    monkeypatch.setenv("RESEARCH_SECURITY_MODE", "apply")
    monkeypatch.setattr(ca, "make_engine", lambda: None)
    monkeypatch.setattr(ca, "DEADLINE", None)
    monkeypatch.setattr(ca, "run", lambda *a, **k: {"swapped": False})
    assert ca.main([]) == 1
    monkeypatch.setattr(ca, "run", lambda *a, **k: {"swapped": True})
    assert ca.main([]) == 0
    monkeypatch.setattr(ca, "run", lambda *a, **k: None)                # apply refused: tables absent
    assert ca.main([]) == 0


def test_screen_prints_progress_so_run_all_can_check_its_timeout(monkeypatch, capsys):
    monkeypatch.setattr(ca, "SCREEN_PROGRESS", 2)
    eng = FakeEngine(stored_aff=[(str(i), 1, "A B", "Weill Cornell Medicine") for i in range(5)])
    assert ca.screen_stored(eng, text, lambda aid: set(), ca.Tally(), False) is True
    out = capsys.readouterr().out
    assert "screening affiliations: 2 articles" in out and "screening affiliations: 4 articles" in out
    assert "screened affiliations: 5 articles" in out and "screened funders: 0 articles" in out


def test_engine_has_read_and_write_timeouts(monkeypatch):
    import sqlalchemy
    seen = {}
    monkeypatch.setattr(sqlalchemy, "create_engine", lambda url, **kw: seen.update(kw, url=url) or "engine")
    for k in ("DB_USERNAME", "DB_PASSWORD", "DB_HOST", "DB_NAME"):
        monkeypatch.setenv(k, "p@ss/word" if k == "DB_PASSWORD" else "x")
    assert ca.make_engine() == "engine"
    args = seen["connect_args"]
    assert args["connect_timeout"] == 15 and 0 < args["read_timeout"] <= 1800 and 0 < args["write_timeout"] <= 1800
    assert seen["url"].password == "p@ss/word"


def test_a_miss_is_tried_again_after_the_window_and_only_a_few_times():
    now = datetime(2026, 11, 16)
    old, fresh = now - timedelta(days=ca.NONE_RETRY_DAYS + 1), now - timedelta(days=2)
    done, retry = ca.collected([
        {"article_id": "1", "source": "none", "attempts": 1, "fetched_at": old},
        {"article_id": "2", "source": "none", "attempts": 1, "fetched_at": fresh},
        {"article_id": "3", "source": "none", "attempts": ca.NONE_MAX_ATTEMPTS, "fetched_at": old},
        {"article_id": "4", "source": "efetch", "attempts": 1, "fetched_at": old},
        {"article_id": "SCOPUS:9", "source": "none", "attempts": 2, "fetched_at": old}], now)
    assert retry == {"1", "scopus:9"} and done == {"2", "3", "4"}

    stale = {"article_id": "333", "source": "none", "attempts": 1, "fetched_at": old}
    recent = {"article_id": "444", "source": "none", "attempts": 1, "fetched_at": fresh}
    eng = FakeEngine(done=["111", "222", "SCOPUS:85100000001", "SCOPUS:85100000003"], state=[stale, recent])
    asked = []
    stats = ca.run("apply", eng, text, now=now, ddb_fetch=lambda pmids: ([], set()),
                   efetch=lambda pmids: asked.extend(pmids) or ca.parse_efetch(EFETCH_XML), openalex=no_call)
    assert asked == [333] and stats["retried_misses"] == 1 and stats["by_source"] == {"efetch": 1}
    # the state row is overwritten in place and its attempt counted
    assert "ON DUPLICATE KEY UPDATE" in ca.INS_STATE and "attempts = attempts + 1" in ca.INS_STATE


def test_flag_article_one_row_per_program():
    rows = ca.flag_article("9", [("NIH HHS", "R01"), ("China Scholarship Council", "A"),
                                 ("CSC, China", "B"), ("Shanghai Pujiang Program", None)])
    assert [(r["program"], r["grant_id"]) for r in rows] == [
        ("China Scholarship Council (CSC)", "A"), ("Shanghai Magnolia / Pujiang (Category D)", None)]


# ---- run_all.py hook ------------------------------------------------------------------------------
def test_run_all_gate():
    cmd = run_all.research_security_cmd
    assert cmd(None) is None and cmd("") is None and cmd("off") is None
    assert cmd("aply") is None and cmd("on") is None and cmd("true") is None
    assert cmd("apply") == cmd(" Dry-Run ") == "python3 collectAffiliations.py"
    assert cmd("apply", 3000) == "python3 collectAffiliations.py --max-seconds 3000"
    assert cmd("off", 3000) is None


def test_run_all_hook_is_off_by_default_and_never_raises(monkeypatch):
    ran = []
    monkeypatch.setattr(run_all, "run_script",
                        lambda name, cmd, timeout_seconds=None: ran.append((name, cmd, timeout_seconds)) or True)
    monkeypatch.delenv("RESEARCH_SECURITY_MODE", raising=False)
    monkeypatch.delenv("RESEARCH_SECURITY_TIMEOUT_SECONDS", raising=False)
    run_all.run_research_security()
    assert ran == []
    monkeypatch.setenv("RESEARCH_SECURITY_MODE", "dry-run")
    run_all.run_research_security()
    assert ran == [("researchSecurity", "python3 collectAffiliations.py --max-seconds 3000", 3600)]

    def boom(*a, **k):
        raise RuntimeError("simulated")
    monkeypatch.setattr(run_all, "run_script", boom)
    run_all.run_research_security()                    # must swallow


def test_run_all_hook_runs_after_the_fatal_scripts_and_is_not_one_of_them():
    src = open(os.path.join(HERE, "run_all.py")).read()
    main_src = src[src.index("def main():"):]
    assert main_src.index("for name, cmd in scripts:") < main_src.index("run_research_security()")
    assert main_src.index("run_aar_close_attributed()") < main_src.index("run_research_security()")
    scripts_list = main_src[main_src.index("scripts = ["):main_src.index("]", main_src.index("scripts = ["))]
    assert "collectAffiliations" not in scripts_list
    guard = main_src[main_src.index("run_research_security()") - 400:main_src.index("run_research_security()")]
    assert "if overall_success:" in guard


# ---- image and migration -----------------------------------------------------------------------------
def test_dockerfile_copies_the_script_and_its_imports():
    dockerfile = open(os.path.join(ROOT, "Dockerfile")).read()
    for f in ("collectAffiliations.py", "watchlist_patterns.py", "enrichArticles.py", "aar_universe.py", "aar_db.py"):
        assert re.search(rf"^COPY update/{re.escape(f)}\s", dockerfile, re.M), f


def test_migration_is_idempotent_mirrored_and_out_of_the_swap_lists():
    mig = open(os.path.join(ROOT, "setup", "table_research_security_v3.13.sql")).read()
    creates = re.findall(r"^CREATE TABLE IF NOT EXISTS `(\w+)` \(.*?;\n", mig, re.S | re.M)
    assert sorted(creates) == sorted(ca.TABLES)
    body = re.sub(r"--.*", "", mig)
    assert not re.search(r"\b(DROP|TRUNCATE|ALTER|DELETE)\b", body, re.I)
    assert body.count("utf8mb4") == 2 * len(ca.TABLES)
    assert "Merging the PR does NOT run DDL" in mig
    baseline = open(os.path.join(ROOT, "setup", "createDatabaseTableReciterDb.sql")).read()
    stmts = re.findall(r"^CREATE TABLE IF NOT EXISTS `.*?COLLATE=utf8mb4_unicode_ci;\n", mig, re.S | re.M)
    assert len(stmts) == len(ca.TABLES)
    for stmt in stmts:
        assert stmt in baseline
    loader = open(os.path.join(HERE, "updateReciterDB.py")).read()
    for t in ca.TABLES:
        assert t not in loader
    # every column the script writes exists in the DDL
    for sql in (ca.INS_AFF, ca.INS_FUNDER, ca.INS_STATE, ca.INS_MATCH, ca.INS_FLAG):
        table = re.search(r"INTO (\w+?)(?:_new)? \(", sql).group(1)
        ddl = re.search(rf"CREATE TABLE IF NOT EXISTS `{table}` \((.*?)\) ENGINE", mig, re.S).group(1)
        for col in re.search(r"\((.*?)\) VALUES", sql).group(1).replace("`", "").split(", "):
            assert f"`{col}`" in ddl, (table, col)


def _table_columns(path, table):
    ddl = open(os.path.join(ROOT, "setup", path)).read()
    body = re.search(rf"CREATE TABLE (?:IF NOT EXISTS )?`{table}` \((.*?)\n\) ENGINE", ddl, re.S).group(1)
    return set(re.findall(r"^\s*`(\w+)`", body, re.M))


def test_baseline_block_is_loadable_by_setupReciterDB():
    """setupReciterDB.py splits the baseline on ';' and executes each piece. Between the
    statements of the mirrored block there may be only comments, and no comment may hold a
    ';' (that would cut a statement in two)."""
    baseline = open(os.path.join(ROOT, "setup", "createDatabaseTableReciterDb.sql")).read()
    start = baseline.index("-- Research-security collection and watch-list matches (v3.13)")
    last = baseline.index("CREATE TABLE IF NOT EXISTS `watchlist_funding_flag`")
    block = baseline[start:baseline.index(";\n", last) + 1]
    pieces = block.rstrip(";").split(";")
    assert len(pieces) == len(ca.TABLES)
    for piece in pieces:
        lines = [ln for ln in piece.split("\n") if ln.strip()]
        code = [ln for ln in lines if not ln.lstrip().startswith("--")]
        assert code[0].startswith("CREATE TABLE IF NOT EXISTS `"), code[0]
        assert code[-1].startswith(") ENGINE=InnoDB"), code[-1]
        # every line before the CREATE is a comment
        assert all(ln.lstrip().startswith("--") for ln in lines[:lines.index(code[0])])
    assert "CREATE TABLE IF NOT EXISTS only" not in baseline       # the migration header stays in the migration


def test_scope_sql_columns_exist_in_the_ddl():
    base = "createDatabaseTableReciterDb.sql"
    tables = {"analysis_summary_author": base, "analysis_summary_article": base, "person_article": base,
              "person": base, "authorship_review": "table_authorship_review.sql", "identity": "table_identity.sql"}
    cols = {t: _table_columns(f, t) for t, f in tables.items()}
    words = {"select", "as", "from", "join", "on", "where", "and", "or", "not", "regexp", "coalesce", "in",
             "is", "null", "status", "pending", "open", "ext", "cwid", "a", "r"}
    for sql in (ca.SQL_ACCEPTED, ca.SQL_PENDING, ca.SQL_REVIEW, ca.SQL_PERSON, ca.SQL_IDENTITY):
        used = re.findall(r"(?:FROM|JOIN)\s+(\w+)(?:\s+(\w+))?", sql)
        alias = {(a if a and a.lower() not in words - {"a", "r"} else t): t for t, a in used}
        assert all(t in cols for t in alias.values()), used
        for qual, name in re.findall(r"(?:\b(\w+)\.)?\b([A-Za-z_]\w*)\b", re.sub(r"'[^']*'|:\w+", "", sql)):
            if qual:
                assert name in cols[alias[qual]], (name, alias[qual])
            elif name.lower() not in words and name not in alias and name not in alias.values():
                assert any(name in cols[t] for t in alias.values()), (name, sql)
    # the filter column the word list hides, and the review key the accept-time id is built from
    assert "status" in cols["authorship_review"] and {"source", "external_id", "top_cwid"} <= cols["authorship_review"]
    assert "userAssertion" in cols["person_article"] and "article_id" in cols["analysis_summary_article"]


def test_no_personal_email_address_in_the_new_files():
    """Only says the new files hardcode no address. efetch goes out through
    aar_universe._req, which sends NCBI_EMAIL or, unset, that module's own default."""
    for f in ("collectAffiliations.py", "watchlist_patterns.py"):
        assert not re.search(r"[\w.]+@[\w.]+\.\w+", open(os.path.join(HERE, f)).read()), f
    cron = open(os.path.join(ROOT, "kubernetes", "k8-cronjob.yaml")).read()
    assert re.search(r"^\s+- name: NCBI_EMAIL\n\s+valueFrom:\n\s+configMapKeyRef:", cron, re.M)
