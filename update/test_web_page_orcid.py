"""Self-check for webPageOrcid pure functions: python3 test_web_page_orcid.py (or pytest)."""
import webPageOrcid as o

A = "0000-0002-1825-0097"
B = "0000-0001-5109-3700"


def cr(given, family, orcid=None):
    d = {"given": given, "family": family}
    if orcid:
        d["ORCID"] = "https://orcid.org/" + orcid
    return d


def oa(name, orcid=None):
    return {"author": {"display_name": name, "orcid": orcid and "https://orcid.org/" + orcid}}


def test_bare_orcid():
    assert o.bare_orcid("http://orcid.org/" + A) == A and o.bare_orcid(A) == A
    assert o.bare_orcid("0000-0002-1825-009") is None and o.bare_orcid(None) is None


def test_crossref_backed_inference():
    seeds = {"10.1/a": {"crossref": [cr("Odette", "Vellacott", A)], "openalex": []},
             "10.1/b": {"crossref": [], "openalex": o.openalex_authors([oa("O. Vellacott", A)])}}
    orcid, votes, runner, crv, by = o.infer("Odette", "Vellacott", seeds)
    assert (orcid, votes, runner, crv) == (A, 2, 0, 1)
    assert set(by[A]) == {"10.1/a", "10.1/b"}


def test_openalex_only_is_held():
    seeds = {d: {"crossref": [cr("Odette", "Vellacott")], "openalex": o.openalex_authors([oa("Odette Vellacott", A)])}
             for d in ("10.1/a", "10.1/b", "10.1/c")}
    assert o.infer("Odette", "Vellacott", seeds)[:4] == (None, 3, 0, 0)


def test_one_doi_votes_once_and_needs_two():
    seeds = {"10.1/a": {"crossref": [cr("Odette", "Vellacott", A)],
                        "openalex": o.openalex_authors([oa("Odette Vellacott", A)])}}
    assert o.infer("Odette", "Vellacott", seeds)[:2] == (None, 1)


def test_coauthor_orcid_ignored_and_tie_infers_nothing():
    seeds = {"10.1/a": {"crossref": [cr("Odette", "Vellacott", A), cr("Quentin", "Zarnowski", B)], "openalex": []},
             "10.1/b": {"crossref": [cr("Odette", "Vellacott", A)], "openalex": []},
             "10.1/c": {"crossref": [cr("O.", "Vellacott", B)], "openalex": []},
             "10.1/d": {"crossref": [cr("Odette", "Vellacott", B)], "openalex": []}}
    orcid, votes, runner, _, by = o.infer("Odette", "Vellacott", seeds)
    assert orcid is None and votes == runner == 2  # Zarnowski's ORCID on 10.1/a never counted for B
    assert set(by[B]) == {"10.1/c", "10.1/d"}


def test_am_status():
    assert o.am_status(A, set()) == "none" and o.am_status(A, {A}) == "agrees" and o.am_status(A, {B}) == "conflicts"


def _http_429(remaining, reset):
    import urllib.error
    return urllib.error.HTTPError("u", 429, "Too Many Requests", {"x-ratelimit-remaining": remaining, "x-ratelimit-reset": reset}, None)


def test_openalex_budget_spent_stops_at_once():
    from unittest import mock
    import webPageExpand
    with mock.patch.object(o, "_budget_blocked_until", 0.0), mock.patch("time.sleep"), \
            mock.patch("urllib.request.urlopen", side_effect=_http_429("0", "3600")) as u:
        assert o.fetch_openalex([f"10.1/{i}" for i in range(120)]) == ({}, set())
        assert u.call_count == 1
        try:
            webPageExpand.works_by_orcid("0000-0000-0000-0000")
            assert False, "must raise so the person is skipped, not written as having no works"
        except o.OpenAlexBudgetSpent:
            pass
        assert u.call_count == 1
    with mock.patch.object(o, "_budget_blocked_until", 0.0), mock.patch("time.sleep"), \
            mock.patch("urllib.request.urlopen", side_effect=_http_429("900", "3600")) as u:
        o.fetch_openalex(["10.1/a"])
        assert u.call_count == 4  # a burst 429 is still retried


def test_openalex_key_goes_in_a_header_only():
    import io
    import os
    from unittest import mock
    for env, want in (({"OPENALEX_API_KEY": "fake-key"}, "Bearer fake-key"), ({"OPENALEX_API_KEY": ""}, None)):
        with mock.patch.dict(os.environ, env), mock.patch.object(o, "_budget_blocked_until", 0.0), \
                mock.patch("urllib.request.urlopen", return_value=io.BytesIO(b'{"results": []}')) as u:
            o.openalex_get("filter=x")
            req = u.call_args[0][0]
            assert req.get_header("Authorization") == want and "fake-key" not in req.full_url


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
