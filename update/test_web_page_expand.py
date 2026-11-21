"""Self-check for webPageExpand pure functions: python3 test_web_page_expand.py (or pytest)."""
import webPageExpand as e

ME = "0000-0002-1825-0097"
FIELD_BIO, FIELD_ENG = "https://openalex.org/fields/11", "https://openalex.org/fields/22"
INST = "https://openalex.org/I1"


def au(aid, raw, orcid=None, insts=()):
    return {"author": {"id": aid, "orcid": orcid and "https://orcid.org/" + orcid, "display_name": raw},
            "raw_author_name": raw, "institutions": [{"id": i} for i in insts]}


def work(wid, doi, typ, authors, field=FIELD_BIO):
    return {"id": "https://openalex.org/" + wid, "doi": doi and "https://doi.org/" + doi, "type": typ,
            "authorships": authors, "primary_topic": {"field": {"id": field}}}


SEED = work("W1", "10.1/seed", "article", [au("A1", "Odette M. Vellacott", ME, [INST]), au("C1", "Quentin Zarnowski")])


def test_norm_doi():
    assert e.norm_doi("https://doi.org/10.1/AB") == "10.1/ab" and e.norm_doi(None) is None


def test_name_and_middle_initial():
    assert e.name_match("Vellacott, O.", "Odette", "Vellacott") and not e.name_match("P. Vellacott", "Odette", "Vellacott")
    assert e.middle_conflict("Odette T. Vellacott", [["o", "m"]], "Vellacott")
    assert not e.middle_conflict("Odette M. Vellacott Jr.", [["o", "m"]], "Vellacott")


def test_r6_coauthor_or_seed_institution_and_field():
    co = work("W2", "10.1/co", "article", [au("A1", "O. Vellacott", ME), au("C1", "Q. Zarnowski")])
    inst = work("W3", "10.1/inst", "review", [au("A1", "Odette Vellacott", ME, [INST])])
    inst_other_field = work("W4", "10.1/x", "article", [au("A1", "Odette Vellacott", ME, [INST])], FIELD_ENG)
    namesake = work("W5", "10.1/ns", "article", [au("A9", "Odette T. Vellacott", ME), au("C1", "Q. Zarnowski")])
    dataset = work("W6", "10.1/ds", "dataset", [au("A1", "O. Vellacott", ME), au("C1", "Q. Zarnowski")])
    letter = work("W7", "10.1/lt", "letter", [au("A1", "O. Vellacott", ME), au("C1", "Q. Zarnowski")])
    out, st = e.plan_person([SEED, co, inst, inst_other_field, namesake, dataset, letter], {"10.1/seed"},
                            ME, "Odette", "Vellacott")
    got = {w["id"].rsplit("/", 1)[-1]: v for w, v, _ in out}
    assert got == {"W2": "accept", "W3": "accept", "W4": "pending", "W5": "pending", "W7": "pending"}
    assert st["seed"] == 1 and st["type_dropped"] == 1 and not st["gated"]


def test_profile_gate_holds_everything():
    junk = [work(f"W{i}", f"10.1/j{i}", "article", [au("A9", "O. Vellacott", ME)], FIELD_ENG) for i in range(10, 20)]
    good = work("W2", "10.1/co", "article", [au("A1", "O. Vellacott", ME), au("C1", "Q. Zarnowski")])
    out, st = e.plan_person([SEED, good, *junk], {"10.1/seed"}, ME, "Odette", "Vellacott")
    assert st["gated"] == 1 and {v for _, v, _ in out} == {"pending"}


def test_seed_missing_from_orcid_pull_still_counts():
    co = work("W2", "10.1/co", "article", [au("A1", "O. Vellacott", ME), au("C1", "Q. Zarnowski")])
    out, _ = e.plan_person([co], {"10.1/seed"}, ME, "Odette", "Vellacott", seed_extra=[SEED])
    assert [v for _, v, _ in out] == ["accept"]


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
