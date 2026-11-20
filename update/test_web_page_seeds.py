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


def test_parse_lastmod():
    assert w.parse_lastmod("2026-11-01T10:00:00-05:00") == datetime.datetime(2026, 11, 1, 15, 0)
    assert w.parse_lastmod("2026-11-01") == datetime.datetime(2026, 11, 1)
    assert w.parse_lastmod("garbage") is None and w.parse_lastmod(None) is None


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
