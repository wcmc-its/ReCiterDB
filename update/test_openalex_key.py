"""The nightly's OpenAlex calls send OPENALEX_API_KEY in a header, never in the URL, and name
the HTTP status when they give up.

A URL can end up in a log (requests puts it in its errors), so a key there would leak. Keyless
calls share one daily budget per egress IP; the 11-24 nightly lost three steps to what was
logged only as "failed after retries".

Run: python3 update/test_openalex_key.py
"""
import os
import sys
from unittest import mock

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import aar_universe_openalex   # noqa: E402
import authorIdSuggestions     # noqa: E402
import enrichArticles          # noqa: E402
import externalAbstractImport  # noqa: E402

CALLS = {
    "aar_universe_openalex": lambda: aar_universe_openalex._openalex_get({"filter": "x"}),
    "externalAbstractImport": lambda: externalAbstractImport._get({"filter": "x"}),
    "enrichArticles": lambda: enrichArticles.openalex_get({"filter": "x"}, enrichArticles.Stats()),
    "authorIdSuggestions": lambda: authorIdSuggestions.openalex_works_by_pmid([1]),
    "authorIdSuggestions (author)": lambda: authorIdSuggestions.openalex_author_by_orcid("0000-0002-1825-0097"),
}


def reply(status):
    r = requests.Response()
    r.status_code = status
    r._content = b'{"results": []}'
    r.url = "https://api.openalex.org/works?filter=x"
    return r


def run(name, status, key):
    with mock.patch.dict(os.environ, {"OPENALEX_API_KEY": key}), mock.patch("time.sleep"), \
            mock.patch("requests.get", return_value=reply(status)) as get:
        try:
            CALLS[name]()
            error = ""
        except Exception as e:  # noqa: BLE001
            error = str(e)
    return get.call_args, error


for name in CALLS:
    call, error = run(name, 200, "fake-key")
    assert not error, (name, error)
    assert call.kwargs["headers"].get("Authorization") == "Bearer fake-key", name
    assert "fake-key" not in repr(call.args) + repr(call.kwargs["params"]), name
    for blank in ("", "  "):
        call, _ = run(name, 200, blank)
        assert "Authorization" not in call.kwargs["headers"], name
    _, error = run(name, 429, "fake-key")
    assert "HTTP 429" in error and "fake-key" not in error, (name, error)
    _, error = run(name, 403, "fake-key")
    assert "403" in error and "fake-key" not in error, (name, error)
    print("ok", name)
