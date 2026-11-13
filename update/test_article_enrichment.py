"""enrichArticles.py: the offline selftest, and the run_all.py env gate.

The gate decides whether the nightly calls OpenAlex / Scopus / NCBI and writes
article_enrichment at all, and it is driven by a CronJob env patch rather than by code
review, so the case worth pinning is the typo: an unrecognised ARTICLE_ENRICHMENT value
must mean off, never apply. Also pins the hook's place: before nightlyIndexing, so STEP 6b
joins the same night's enrichment.

Run: python3 update/test_article_enrichment.py
"""
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

# run_all imports boto3/psutil at module scope for the S3 upload and memory logging; stubbed
# so this runs on a bare checkout (same approach as test_author_id_suggestions.py).
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

os.environ.setdefault("LOG_FILE", "/tmp/test_article_enrichment.log")
os.environ.setdefault("S3_BUCKET", "unused-in-this-test")
os.environ.setdefault("S3_KEY_PREFIX", "unused-in-this-test")

import enrichArticles  # noqa: E402
import run_all         # noqa: E402

assert enrichArticles._selftest(), "enrichArticles selftest failed"
print("  PASS selftest")

cmd = run_all.article_enrichment_cmd
assert cmd(None) is None and cmd("") is None and cmd("off") is None
assert cmd("aply") is None and cmd("on") is None and cmd("true") is None
assert cmd("apply") == "python3 enrichArticles.py --apply"
assert cmd(" Apply ") == "python3 enrichArticles.py --apply"
assert cmd("dry-run") == "python3 enrichArticles.py --dry-run"
assert cmd("apply", 1140) == "python3 enrichArticles.py --apply --max-seconds 1140"
assert cmd("off", 1140) is None
print("  PASS ARTICLE_ENRICHMENT gate: unset/typo -> off, apply/dry-run recognised")

_ran = []
run_all.run_script = lambda name, cmd, timeout_seconds=None: _ran.append((cmd, timeout_seconds)) or True
os.environ.pop("ARTICLE_ENRICHMENT", None)
run_all.run_article_enrichment()
assert _ran == [], _ran
os.environ["ARTICLE_ENRICHMENT"] = "apply"
os.environ.pop("ENRICH_TIMEOUT_SECONDS", None)
run_all.run_article_enrichment()
assert _ran == [("python3 enrichArticles.py --apply --max-seconds 1140", 1200)], _ran
print("  PASS hook: off by default; apply runs with the 1200s timeout and a 1140s script deadline")


def _boom(*a, **k):
    raise RuntimeError("simulated")


run_all.run_script = _boom
run_all.run_article_enrichment()   # must swallow
print("  PASS hook: a failure is logged, never raised")

src = open(os.path.join(HERE, "run_all.py")).read()
main_src = src[src.index("def main():"):]
assert main_src.index("run_article_enrichment()") < main_src.index("for name, cmd in scripts:")
print("  PASS hook runs before the scripts loop (nightlyIndexing)")
print("ALL PASS")
