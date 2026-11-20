"""Self-check for webPageCitations pure functions: python3 test_web_page_citations.py (or pytest).
All names are fictional (public repo)."""
import json

import webPageCitations as c

PAGE = """<html><nav>Home 2026 About us and more links here today</nav><ul>
<li>Zarnowski Q, Okafor B (2021). Nectar chemistry shapes moth visitation. <i>Ecology Letters</i> 24:100-110.
 doi:10.1111/ele.99999</li>
<li>Okafor B, Zarnowski Q. 2019. Scent plumes and foraging in hawkmoths. Journal of Chemical Ecology 45: 1-12.</li>
<li>Invited talk, Annual Meeting of the Society, Ithaca NY, 2022.</li>
<li>Short</li></ul><script>var y = 2020; var a = 'one two three four five six seven';</script></html>"""


def test_blocks_split_strip_and_filter():
    b = c.citation_blocks(PAGE)
    assert len(b) == 3  # nav, script and the short item are gone
    assert b[0].startswith("Zarnowski Q, Okafor B (2021). Nectar chemistry") and "<i>" not in b[0]
    assert c.citation_blocks(PAGE + PAGE) == b  # duplicates collapse


def test_block_hash_ignores_case_space_punctuation():
    assert c.block_hash("A  Study, (2020).") == c.block_hash("a study 2020")
    assert c.block_hash("A study 2020") != c.block_hash("A study 2021")


def test_inline_and_verbatim_doi():
    blk = c.citation_blocks(PAGE)[0]
    assert c.inline_doi(blk) == "10.1111/ele.99999"
    assert c.verbatim_doi("https://doi.org/10.1111/ELE.99999", blk) == "10.1111/ele.99999"
    assert c.verbatim_doi("10.1000/invented", blk) is None  # a model DOI not in the text is discarded


def test_parse_answer_validates():
    ans = "here you go ```" + json.dumps({"results": [
        {"i": 0, "is_publication": True, "title": "Nectar chemistry shapes moth visitation", "authors": ["Zarnowski Q"],
         "year": 2021, "type": "article", "doi": None},
        {"i": 0, "is_publication": False, "title": "dup index ignored"},
        {"i": 1, "is_publication": True, "title": "short", "year": "2019"},       # title too short -> not a pub
        {"i": 7, "is_publication": True, "title": "out of range index"},
        {"i": True, "is_publication": True, "title": "bool index rejected"},
        {"i": 2, "is_publication": True, "title": "A third publication title", "year": 3000, "type": "weird"}]}) + "```"
    p = c.parse_answer(ans, 3)
    assert set(p) == {0, 1, 2}
    assert p[0]["is_publication"] and p[0]["year"] == 2021 and p[0]["title"].startswith("Nectar")
    assert p[1]["is_publication"] is False and p[1]["year"] is None
    assert p[2]["year"] is None and p[2]["type"] == "other"
    assert c.parse_answer("not json", 3) == {}


def test_title_match_and_year_guard():
    items = [{"DOI": "10.1/a", "title": ["Nectar chemistry shapes moth visitation."], "issued": {"date-parts": [[2021]]}},
             {"DOI": "10.1/b", "title": ["Nectar chemistry shapes moth visitation"], "issued": {"date-parts": [[2015]]}},
             {"DOI": "10.1/c", "title": ["Something else entirely about bees"], "issued": {"date-parts": [[2021]]}}]
    best, sim = c.pick_crossref(items, "Nectar Chemistry Shapes Moth Visitation", 2021)
    assert best["DOI"] == "10.1/a" and sim >= c.TITLE_MATCH
    assert c.pick_crossref(items[1:], "Nectar chemistry shapes moth visitation", 2021)[0] is None  # year off by 6
    assert c.pick_crossref(items[2:], "Nectar chemistry shapes moth visitation", None)[0] is None


def test_name_on_authors():
    authors = [{"given": "J. Quentin", "family": "Zarnowski"}, {"given": "Ilse Maren", "family": "Brandt Okafor"}]
    assert c.name_on_authors("Quentin B.", "Zarnowski", authors)
    assert c.name_on_authors("Ilse", "Brandt Okafor", [{"given": "Ilse", "family": "Brandt"}])  # compound surname
    assert not c.name_on_authors("Odette", "Zarnowski", authors)  # same surname, no shared initial
    assert not c.name_on_authors("Quentin", "Vellacott", authors)
    assert c.name_on_authors("Quentin", "Zarnowski", [{"name": "Quentin Zarnowski"}])  # name-only author


def test_seed_rows_need_name_and_doi():
    names = {"qz1": ("Quentin", "Zarnowski"), "ov2": ("Odette", "Vellacott")}
    resolved = {"h1": ("10.1/a", "crossref", [{"given": "Quentin", "family": "Zarnowski"}]),
                "h2": ("10.1/b", "inline", [{"given": "Ann", "family": "Student"}]),   # lab page: not the PI's paper
                "h3": (None, "no_match", [])}
    pc = [("qz1", "u1", "h1"), ("qz1", "u2", "h1"), ("qz1", "u1", "h2"), ("qz1", "u1", "h3"), ("ov2", "u3", "h1")]
    s = c.seed_rows(pc, resolved, names)
    assert set(s) == {"qz1"} and set(s["qz1"]) == {"10.1/a"}
    urls, hashes, method = s["qz1"]["10.1/a"]
    assert urls == {"u1", "u2"} and hashes == {"h1"} and method == "crossref"


class StubBedrock:
    """Answers every item except the 'lost' indices on the first full-size call."""
    def __init__(self, lose_first_batch):
        self.calls, self.lose = 0, lose_first_batch

    def converse(self, **kw):
        self.calls += 1
        items = json.loads(kw["messages"][0]["content"][0]["text"])
        if self.lose and self.calls == 1:
            return {"output": {"message": {"content": [{"text": "garbled"}]}}}
        res = [{"i": it["i"], "is_publication": True, "title": it["text"]} for it in items]
        return {"output": {"message": {"content": [{"text": json.dumps({"results": res})}]}}}


def test_parse_blocks_retries_a_lost_batch():
    blocks = [f"block number {k} 2020 with enough words here" for k in range(30)]
    stub = StubBedrock(lose_first_batch=True)
    got = c.parse_blocks(stub, blocks, "m")
    assert set(got) == set(range(30))          # the lost 25 came back on the half-size retries
    assert stub.calls == 1 + 3 + 1             # batch 1 (lost), 25 retried as 12+12+1, batch 2
    stub = StubBedrock(lose_first_batch=False)
    assert len(c.parse_blocks(stub, blocks, "m")) == 30 and stub.calls == 2


def test_clean_inline_dois():
    assert c.inline_doi("x (doi:10.1038/nature12345). y") == "10.1038/nature12345"
    assert c.inline_doi("see https://doi.org/10.1000/abc), 2020") == "10.1000/abc"
    assert c.inline_doi("doi:10.1016/j.cell.2020.01.001.PMID: 3333") == "10.1016/j.cell.2020.01.001"
    assert c.inline_doi("doi: 10.1002/ab(cd)ef. Next") == "10.1002/ab(cd)ef"  # balanced parens kept


def test_surname_particles_do_not_match():
    assert not c.name_on_authors("Maria", "de la Cruz", [{"family": "de Souza", "given": "Miguel"}])
    assert not c.name_on_authors("Maria", "Van Dyke", [{"family": "van der Berg", "given": "M."}])
    assert c.name_on_authors("Maria", "de la Cruz", [{"family": "De La Cruz", "given": "M."}])


def test_shifted_model_answer_rejected():
    blocks = ["Zarnowski Q (2021). Nectar chemistry shapes moth visitation. Ecol Lett 24:1-9.",
              "Okafor B (2019). Scent plumes and foraging in hawkmoths. J Chem Ecol 45:1-12."]

    class Shifted:
        calls = 0

        def converse(self, **kw):
            Shifted.calls += 1
            res = [{"i": 1, "is_publication": True, "title": "Nectar chemistry shapes moth visitation"},
                   {"i": 0, "is_publication": True, "title": "Scent plumes and foraging in hawkmoths"}]
            return {"output": {"message": {"content": [{"text": json.dumps({"results": res})}]}}}
    assert c.parse_blocks(Shifted(), blocks, "m") == {}  # titles belong to the other block: retried, never cached


def test_odd_crossref_dates_and_transient_fallthrough():
    assert c.pick_crossref([{"DOI": "10.1/x", "title": ["Nectar chemistry shapes moth visitation"],
                             "issued": {"date-parts": [[]]}}], "Nectar chemistry shapes moth visitation", 2021)[0]

    class CR:
        def work(self, doi):
            return None  # Crossref has never heard of the (mangled) inline DOI

        def search(self, title, first, year):
            return [{"DOI": "10.1/REAL", "title": [title], "issued": {"date-parts": [[2021]]}, "author": []}]

    parsed = {"is_publication": True, "title": "Nectar chemistry shapes moth visitation", "authors": [], "year": 2021}
    doi, method, _, _ = c.resolve(CR(), "Nectar chemistry shapes moth visitation 2021 doi:10.9999/nope", parsed)
    assert (doi, method) == ("10.1/real", "crossref")  # falls through to title search

    class Down:
        def work(self, doi):
            raise c.Transient(doi)
    try:
        c.resolve(Down(), "text doi:10.1000/abc 2021", parsed)
        raise AssertionError("Transient must propagate (never cached as an answer)")
    except c.Transient:
        pass


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
