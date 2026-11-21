"""Self-check for webPageCornellPub pure functions: python3 test_web_page_cornell_pub.py (or pytest)."""
import webPageCornellPub as cp


def test_plan_links_inserts_missing_deletes_gone_skips_unknown():
    seeds = {"ov22": {"10.1/a", "10.1/b", "10.1/nopub"}, "qz11": {"10.1/a"}, "new99": {"10.1/a"}}
    researcher = {"ov22": 1, "qz11": 2}
    pubs = {"10.1/a": 10, "10.1/b": 11}
    existing = {(1, 10): 500, (1, 12): 501}  # (1, 12): a seed that is gone
    ins, dele, skipped = cp.plan_links(seeds, researcher, pubs, existing)
    assert ins == [(1, 11), (2, 10)] and dele == [501] and skipped == 1


def test_pub_row_openalex_then_parse():
    w = {"id": "https://openalex.org/W1", "title": "T", "publication_year": 2020, "type": "article",
         "ids": {"pmid": "https://pubmed.ncbi.nlm.nih.gov/123"}, "cited_by_count": 4,
         "primary_location": {"source": {"display_name": "J", "issn_l": "1234-5678", "issn": ["1234-5678", "8765-4321"]}}}
    r = cp.pub_row("10.1/a", w, None)
    assert (r["openalex_id"], r["pmid"], r["journal"], r["issn"], r["eissn"], r["year"]) == \
        ("W1", "123", "J", "1234-5678", "8765-4321", 2020)
    r = cp.pub_row("10.1/b", None, {"title": "P", "year": 2007, "venue": "V", "type": "chapter"})
    assert (r["title"], r["year"], r["journal"], r["openalex_id"]) == ("P", 2007, "V", None)
    assert cp.pub_row("10.1/c", None, {"year": "2007?"})["year"] is None


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
