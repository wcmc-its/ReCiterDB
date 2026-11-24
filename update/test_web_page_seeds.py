"""Self-check for webPageSeeds pure functions: python3 test_web_page_seeds.py (or pytest)."""
import datetime
import time

import webPageSeeds as w

ROSTER_NAMES = [("qbz11", "Quentin B.", "Zarnowski"), ("ov22", "Odette", "Vellacott"),
                ("imb33", "Ilse Maren", "Brandt Okafor")]
ROSTER_EMAILS = [("xy4", "hollis@cornell.edu", "xy4@cornell.edu", "hollis@cornell.edu"),
                 ("pq55", "pq55@cornell.edu", "pq55@cornell.edu", None),
                 ("ov22", "odettev@math.cornell.edu", "ov22@cornell.edu", None),
                 ("zz66", "hollis@cornell.edu", "zz66@cornell.edu", None)]  # alias collision: first wins
NOW = datetime.datetime(2026, 11, 20)


def test_emails_spamspan_subdomain_obfuscated():
    html = ('<p><span class="spamspan"><span class="u">pq55</span> [at] <span class="d">cornell.edu</span></span></p>'
            '<a href="mailto:Hollis@cornell.edu">x</a> also foo (at) cornell (dot) edu, odettev@math.cornell.edu')
    assert w.page_signals(html)["emails"] == sorted(["odettev@math.cornell.edu", "hollis@cornell.edu",
                                                     "foo@cornell.edu", "pq55@cornell.edu"])


def test_email_regex_is_linear():
    t = time.monotonic()
    w.page_signals("<p>" + "a1" * 100_000 + "</p>")  # 200k-char run; quadratic version took minutes
    assert time.monotonic() - t < 2


def test_hash_ignores_scripts_and_whitespace():
    a = w.page_signals("<p>Hello   world</p><script>var t=1</script>")["hash"]
    b = w.page_signals("<p>Hello world</p>\n<script>var t=2</script>")["hash"]
    c = w.page_signals("<p>Hello there</p>")["hash"]
    assert a == b != c


def test_pub_signals():
    s = w.page_signals("<li>A (2020). doi:10.1000/abc</li><li>B (2021) https://doi.org/10.1000/xyz</li>")
    assert s["dois"] == 2 and s["years"] == 2


def test_person_urls():
    idx = w.surname_index(ROSTER_NAMES)
    yes = ["https://nbb.cornell.edu/quentin-zarnowski", "https://math.cornell.edu/odette-vellacott",
           "https://cals.cornell.edu/people/anyone", "https://x.cornell.edu/ilse-brandt"]
    no = ["https://history.cornell.edu/news-events", "https://cals.cornell.edu/about/visit",
          "https://math.cornell.edu/news/quentin-zarnowski-wins-award",  # news slug with a roster name
          "https://x.cornell.edu/zarnowski"]  # surname alone, no given-initial partner
    assert all(w.looks_like_person_url(u, idx) for u in yes)
    assert not any(w.looks_like_person_url(u, idx) for u in no)


def test_alias_linking():
    m = w.alias_map(ROSTER_EMAILS)
    assert m["hollis@cornell.edu"] == "xy4" and m["zz66@cornell.edu"] == "zz66"
    assert w.link_people(["hollis@cornell.edu"], m) == [("xy4", "roster_alias")]
    assert w.link_people(["odettev@math.cornell.edu"], m) == [("ov22", "roster_alias")]
    assert w.link_people(["pq55@cornell.edu", "unknown@cornell.edu"], m) == [("pq55", "email")]
    assert w.link_people(["pq55@cornell.edu", "xy4@cornell.edu"], m) == [("pq55", "email"), ("xy4", "email")]


def test_other_cornell_namespaces_do_not_link():
    m = w.alias_map(ROSTER_EMAILS)
    assert w.link_people(["pq55@med.cornell.edu", "pq55@weill.cornell.edu"], m) == []  # WCM CWID, not the NetID
    assert w.link_people(["odettev@cornell.edu"], m) == []  # local part of a dept mailbox is a different address


def test_needs_fetch():
    fresh = {"last_fetched": NOW - datetime.timedelta(days=3), "gone_at": None, "http_status": 200}
    assert w.needs_fetch(None, None, NOW)
    assert not w.needs_fetch(fresh, None, NOW)
    assert not w.needs_fetch({**fresh, "http_status": 304}, None, NOW)
    assert not w.needs_fetch(fresh, NOW - datetime.timedelta(days=10), NOW)
    assert w.needs_fetch(fresh, NOW - datetime.timedelta(days=1), NOW)  # sitemap says changed
    assert w.needs_fetch({**fresh, "last_fetched": NOW - datetime.timedelta(days=w.REFETCH_DAYS)}, None, NOW)
    assert w.needs_fetch({**fresh, "gone_at": NOW}, None, NOW)
    assert w.needs_fetch({**fresh, "http_status": 503}, None, NOW)  # transient error retried next run


def test_count_missing_once_per_week():
    r = {"missing_runs": 0, "last_missing_at": None}
    assert w.count_missing(r, NOW) == (1, False)
    r = {"missing_runs": 1, "last_missing_at": NOW - datetime.timedelta(hours=2)}  # crash + restart, same week
    assert w.count_missing(r, NOW) is None
    r = {"missing_runs": 1, "last_missing_at": NOW - datetime.timedelta(days=7)}
    assert w.count_missing(r, NOW) == (2, True)


def test_own_host_urls_and_relative_paths():
    entries = [("/staff-directory/", None), ("https://www.example.cornell.edu/people/a", None),
               ("https://example.cornell.edu/people/b", None), ("people/c", None)]
    got = [u for u, _ in w.own_host_urls(entries, "example.cornell.edu", "https://example.cornell.edu/")]
    assert got == ["https://example.cornell.edu/staff-directory/", "https://example.cornell.edu/people/b",
                   "https://example.cornell.edu/people/c"]  # the www. sibling belongs to its own host


def test_directory_aliases_link_only_unambiguous_roster_people():
    m = w.alias_map(ROSTER_EMAILS)
    found = w.match_directory_entries({"vandal@cornell.edu", "shared@cornell.edu"},
                                      [("pq55", {"vandal@cornell.edu", "pq55@cornell.edu"}),
                                       ("aa1", {"shared@cornell.edu"}), ("bb2", {"shared@cornell.edu"})])
    assert found == {"vandal@cornell.edu": "pq55"}  # an address on two entries proves nothing
    assert w.link_people(["vandal@cornell.edu"], m, found) == [("pq55", "ldap_alias")]
    assert w.link_people(["vandal@cornell.edu"], m, {}) == []
    # a roster address wins over a directory answer for the same address
    assert w.link_people(["hollis@cornell.edu"], m, {"hollis@cornell.edu": "zz66"}) == [("xy4", "roster_alias")]


def test_ldap_filter_escapes():
    f = w.ldap_filter(["a*b@cornell.edu"])
    assert "a\\2ab@cornell.edu" in f and f.startswith("(|") and f.count("(") == 4


def test_throttle_backoff_and_stop():
    h = object.__new__(w.Host)
    h.name, h.gap, h.throttled_in_a_row, h.throttle_events = "x.cornell.edu", 1.0, 0, 0
    h._throttle_check(429, {"Retry-After": "7"})
    assert h.gap == 7.0
    h._throttle_check(200, {})
    assert h.throttled_in_a_row == 0 and h.gap == 7.0  # stays slow for the rest of the run
    h._throttle_check(429, {"Retry-After": "soon"})
    assert h.gap == 14.0
    try:
        for _ in range(w.THROTTLE_STOP):
            h._throttle_check(405, {})
        raise AssertionError("expected HostThrottled")
    except w.HostThrottled:
        pass
    assert h.gap == w.MAX_GAP


def test_host_gap_override():
    orig = w.Host.get
    w.Host.get = lambda self, url, *a, **k: (404, None, None, "")  # no robots.txt, no network
    try:
        assert w.Host("business.cornell.edu", time.monotonic() + 60).gap == w.HOST_GAP["business.cornell.edu"]
        assert w.Host("x.cornell.edu", time.monotonic() + 60).gap == w.REQUEST_GAP
    finally:
        w.Host.get = orig


def test_dockerfile_copies_every_web_page_module():
    """The image copies update/ files one by one: a pass module missing from the Dockerfile
    crashes the CronJob on import (run 6, 2026-11-22)."""
    import os
    import re
    here = os.path.dirname(os.path.abspath(__file__))
    copied = set(re.findall(r"COPY update/(\S+?)\.py", open(os.path.join(here, "..", "Dockerfile")).read()))
    local = {m for m in re.findall(r"^import (webPage\w+)", open(os.path.join(here, "webPageSeeds.py")).read(), re.M)}
    for m in sorted(local):
        local |= set(re.findall(r"^import (webPage\w+)", open(os.path.join(here, m + ".py")).read(), re.M))
    assert local <= copied, f"not in Dockerfile: {sorted(local - copied)}"


def test_parse_lastmod():
    assert w.parse_lastmod("2026-11-01T10:00:00-05:00") == datetime.datetime(2026, 11, 1, 15, 0)
    assert w.parse_lastmod("2026-11-01") == datetime.datetime(2026, 11, 1)
    assert w.parse_lastmod("garbage") is None and w.parse_lastmod(None) is None


def test_hop_kind():
    assert w.hop_kind("https://zarnowskilab.org/", "qbz11", "Zarnowski") == "lab"
    assert w.hop_kind("https://quentinzarnowski.com", "qbz11", "Zarnowski") == "personal"
    assert w.hop_kind("https://www.cs.cornell.edu/~qbz11/", "qbz11", "Zarnowski") == "personal"
    assert w.hop_kind("https://blogs.cornell.edu/vellacottgroup/", "ov22", "Vellacott") == "lab"
    assert w.hop_kind("https://x.cornell.edu/files/cv.pdf", "ov22", "Vellacott") == "cv"
    assert w.hop_kind("https://x.cornell.edu/files/Vellacott-2024.pdf", "ov22", "Vellacott") == "cv"
    assert w.hop_kind("https://x.cornell.edu/files/syllabus.pdf", "ov22", "Vellacott") is None
    assert w.hop_kind("https://press.example.org/books/zarnowski-reader/", "qbz11", "Zarnowski") is None
    assert w.hop_kind("https://paper.example.com/2024/05/zarnowski-wins", "qbz11", "Zarnowski") is None
    assert w.hop_kind("https://example.com/zarnowski.jpg", "qbz11", "Zarnowski") is None
    assert w.hop_kind("https://www.journal.example.org/", "qbz11", "Zarnowski") is None
    assert w.hop_kind("https://www.lee.com/", "kl9", "Lee") is None  # short surnames never match alone


def test_hop_candidates():
    parent = "https://bio.cornell.edu/people/quentin-zarnowski"
    links = ["https://bio.cornell.edu/zarnowski-lab",            # same host: dept chrome
             "https://zarnowskilab.org/#team", "https://zarnowskilab.org/",
             "https://scholar.google.com/citations?user=zarnowski",
             "https://chemistry.cornell.eduzarnowski.jpg",          # broken href
             "https://emergency.example.edu/zarnowski",              # site chrome (on many profiles)
             "https://chem.cornell.edu/people/zarnowski",            # already a crawled profile
             "https://bio.cornell.edu/files/zarnowski-cv.pdf",       # same-host CV is fine
             "mailto:qbz11@cornell.edu"]
    chrome = {"https://emergency.example.edu/zarnowski": 50}
    got = w.hop_candidates(parent, links, "qbz11", "Zarnowski", chrome, {"https://chem.cornell.edu/people/zarnowski"})
    assert got == [("https://zarnowskilab.org", "lab"), ("https://bio.cornell.edu/files/zarnowski-cv.pdf", "cv")], got
    many = [f"https://zarnowski{i}.org/" for i in range(9)]
    assert len(w.hop_candidates(parent, many, "qbz11", "Zarnowski", {}, set())) == w.MAX_HOPS_PER_PERSON


def test_pubs_link():
    html = ('<a href="/team">People</a><a href="https://other.org/publications">Publications</a>'
            '<a href="/pubs/#top"><span>Selected Publications</span></a><a href="/publications">Publications</a>')
    assert w.pubs_link(html, "https://zarnowskilab.org/") == "https://zarnowskilab.org/pubs/"
    assert w.pubs_link('<a href="/cv.pdf">Papers</a>', "https://zarnowskilab.org/") is None
    assert w.pubs_link('<a href="/">Publications</a>', "https://zarnowskilab.org/") is None  # itself


def test_pdf_html_one_block_per_entry():
    text = ("PUBLICATIONS\n"
            "1. Zarnowski Q, Vellacott O. A study of fictional\n"
            "kinases in imaginary cells. J Made Up. 2021;4:1-9.\n"
            "2. Vellacott O, Zarnowski Q. Another invented result\n"
            "about nothing at all. Fake Rev. 2019.\n")
    import webPageCitations
    blocks = webPageCitations.citation_blocks(w.pdf_html(text))
    assert len(blocks) == 2, blocks
    assert blocks[0].startswith("1. Zarnowski Q") and blocks[0].endswith("2021;4:1-9.")
    assert w.pdf_html("a < b and c > d") == "<p>a &lt; b and c &gt; d"


def _run_hop_host(pages, html, hops, parent_netid):
    """hop_host with the network and DB stubbed. Returns (stats, retired urls, fetched urls)."""
    import collections
    import threading
    from unittest import mock
    retired, fetched, stats = [], [], collections.Counter()
    host = mock.Mock(robots_ok=True)
    host.robots.can_fetch.return_value = True

    def hop_page(h, write, url, kind, parent, netid, row, now, s):
        w.needs_fetch(row, None, now)  # reads the row as the real one does
        fetched.append(url)
        if html.get(url) is None:
            return None, None
        return 200, (None if kind == "cv" else html[url])  # a CV has no html, as in the real one
    with mock.patch.multiple(w, Host=lambda *a: host, db_conn=mock.Mock(), hop_page=hop_page, expired=lambda d: False,
                             writer=lambda conn: (lambda fn: fn(mock.Mock())),
                             retire_pages=lambda c, urls, now: retired.extend(urls)):
        w.hop_host("zlab.org", pages, hops, set(), parent_netid, None, stats, threading.Lock())
    return stats, retired, fetched


PUBS = "https://zlab.org/pubs"
LINK = f'<a href="{PUBS}">Publications</a>'


def _pubs_row(parent):
    return {"url": PUBS, "kind": "pubs", "host": "zlab.org", "parent_url": parent, "gone_at": None, "http_status": 200,
            "last_fetched": NOW, "content_hash": "x", "etag": None, "last_modified": None}


def test_hop_host_two_pages_lead_to_one_pubs_page():
    pages = [("https://zlab.org/a", "lab", "P1", "qbz11"), ("https://zlab.org/b", "lab", "P1", "qbz11")]
    s, retired, fetched = _run_hop_host(pages, {u: LINK for u, *_ in pages}, {}, {"P1": "qbz11"})
    assert s["page_errors"] == 0 and retired == [] and fetched.count(PUBS) == 1


def test_hop_host_sibling_without_link_does_not_retire():
    pages = [("https://zlab.org/a", "lab", "P1", "qbz11"), ("https://zlab.org/b", "cv", "P1", "qbz11")]
    html = {"https://zlab.org/a": LINK, "https://zlab.org/b": "<p>hi</p>"}
    for order in (pages, pages[::-1]):
        for hops in ({}, {PUBS: _pubs_row("P1")}):
            s, retired, _ = _run_hop_host(order, html, hops, {"P1": "qbz11"})
            assert retired == [] and s["page_errors"] == 0
    # one sibling unread (skipped / failed): it may be the one that links the pubs page
    assert _run_hop_host(pages, {"https://zlab.org/b": "<p>hi</p>"}, {PUBS: _pubs_row("P1")}, {"P1": "qbz11"})[1] == []
    # every page read and none links it: retired
    s, retired, _ = _run_hop_host(pages, {u: "<p>hi</p>" for u, *_ in pages}, {PUBS: _pubs_row("P1")}, {"P1": "qbz11"})
    assert retired == [PUBS] and s["pubs_retired"] == 1


def test_hop_host_shared_means_two_people():
    pages = [("https://zlab.org/a", "lab", "P1", "qbz11"), ("https://zlab.org/b", "lab", "P2", "qbz11")]
    html = {u: LINK for u, *_ in pages}
    s, retired, _ = _run_hop_host(pages, html, {}, {"P1": "qbz11", "P2": "qbz11"})
    assert s["pubs_shared"] == 0 and retired == []
    pages[1] = ("https://zlab.org/b", "lab", "P2", "ov22")
    s, retired, _ = _run_hop_host(pages, html, {}, {"P1": "qbz11", "P2": "ov22"})
    assert s["pubs_shared"] == 1 and retired == [PUBS]


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
