"""collectArticleOrcids: its selftest, and the run_all ARTICLE_ORCID gate.

Run: python3 update/test_collect_article_orcids.py
"""
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
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

os.environ.setdefault("LOG_FILE", "/tmp/test_collect_article_orcids.log")
os.environ.setdefault("S3_BUCKET", "unused-in-this-test")
os.environ.setdefault("S3_KEY_PREFIX", "unused-in-this-test")

import collectArticleOrcids  # noqa: E402
import run_all               # noqa: E402

assert collectArticleOrcids._selftest(), "collectArticleOrcids selftest failed"

cmd = run_all.article_orcid_cmd
for off in (None, "", "off", "aply", "on", "true", "selftest"):
    assert cmd(off) is None, off
assert cmd(" Apply ") == "python3 collectArticleOrcids.py --apply"
assert cmd("dry-run", 60) == "python3 collectArticleOrcids.py --dry-run --max-seconds 60"

_ran = []
run_all.run_script = lambda name, cmd, timeout_seconds=None: _ran.append((cmd, timeout_seconds)) or True
os.environ.pop("ARTICLE_ORCID", None)
os.environ.pop("ARTICLE_ORCID_TIMEOUT_SECONDS", None)
run_all.run_article_orcids()
assert _ran == [], _ran
os.environ["ARTICLE_ORCID"] = "apply"
run_all.run_article_orcids()
assert _ran == [("python3 collectArticleOrcids.py --apply --max-seconds 1740", 1800)], _ran


def _boom(*a, **k):
    raise RuntimeError("simulated")


run_all.run_script = _boom
run_all.run_article_orcids()   # must swallow

src = open(os.path.join(HERE, "run_all.py")).read()
main_src = src[src.index("def main():"):]
assert main_src.index("run_article_orcids()") < main_src.index("run_orcid_suggestions()")
print("ALL PASS")
