"""lane_due() — the cadence gate shared by the AAR Scopus and PubMed lanes.

The gate is three lines, but it decides how much Elsevier quota the Scopus lane spends, and
it is driven by a CronJob env patch rather than by code review. The case worth pinning is the
typo: an unrecognised value must fall back to the CONSERVATIVE schedule, never to daily.

Run: python3 update/test_aar_lane_cadence.py
"""
import os
import sys

# run_all imports boto3/psutil at module scope for the S3 upload and memory logging, neither
# of which lane_due touches. Stubbed so this test runs on a bare checkout — this repo has no
# CI, and a test that needs the full container to run is a test nobody runs.
import types

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

os.environ.setdefault("LOG_FILE", "/tmp/test_aar_lane_cadence.log")
os.environ.setdefault("S3_BUCKET", "unused-in-this-test")
os.environ.setdefault("S3_KEY_PREFIX", "unused-in-this-test")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from run_all import lane_due   # noqa: E402

MON, SUN = 0, 6
n = 0


def check(label, actual, expected):
    global n
    n += 1
    assert actual == expected, f"FAIL {label}: got {actual!r}, wanted {expected!r}"
    print(f"  PASS {label}")


# --- Scopus: default weekly, so the knob is inert until someone sets it -------------------
check("scopus default (unset) is Sundays-only", lane_due(None, MON, "weekly"), False)
check("scopus default (unset) runs on Sunday", lane_due(None, SUN, "weekly"), True)
check("scopus daily runs on a Monday", lane_due("daily", MON, "weekly"), True)
check("scopus explicit weekly skips a Monday", lane_due("weekly", MON, "weekly"), False)

# --- PubMed: default daily, and #195/#198 must stay daily -------------------------------
check("pubmed default (unset) runs on a Monday", lane_due(None, MON, "daily"), True)
check("pubmed weekly rolls back to Sundays-only", lane_due("weekly", MON, "daily"), False)
check("pubmed weekly still runs on Sunday", lane_due("weekly", SUN, "daily"), True)

# --- the failure mode that actually costs money -----------------------------------------
# A typo must not spend 7x the Elsevier quota. Anything not exactly "daily" is Sundays-only.
check("a typo'd cadence falls back to Sundays-only", lane_due("dialy", MON, "weekly"), False)
check("case and whitespace are tolerated", lane_due("  DAILY  ", MON, "weekly"), True)
check("empty string falls back to the default", lane_due("", MON, "daily"), True)
check("...and an empty string cannot force a weekly lane daily", lane_due("", MON, "weekly"), False)

# --- OpenAlex: unset means OFF, never weekly ------------------------------------------
# The lane fills a curator queue whose accepts reach SPS profiles, so it must not run until
# someone sets AAR_OPENALEX_LANE_CADENCE on purpose.
import run_all   # noqa: E402

_ran = []
run_all.run_script = lambda name, cmd, timeout_seconds=None: _ran.append(cmd) or True
for _val in (None, "", "off", "weekyl"):
    if _val is None:
        os.environ.pop("AAR_OPENALEX_LANE_CADENCE", None)
    else:
        os.environ["AAR_OPENALEX_LANE_CADENCE"] = _val
    run_all.run_openalex_lane_if_due()
check("openalex lane never runs unset, empty, off or typo'd", _ran, [])
os.environ["AAR_OPENALEX_LANE_CADENCE"] = "daily"
_saved_key = os.environ.pop("PUBMED_API_KEY", None)
run_all.run_openalex_lane_if_due()
check("openalex lane skips when daily but PUBMED_API_KEY is unset", _ran, [])
os.environ["PUBMED_API_KEY"] = "test-key-not-real"
run_all.run_openalex_lane_if_due()
check("openalex lane runs when set to daily",
      _ran, ["python3 aar_universe_openalex.py --mode rolling --apply"])
os.environ.pop("AAR_OPENALEX_LANE_CADENCE", None)
if _saved_key is None:
    os.environ.pop("PUBMED_API_KEY", None)
else:
    os.environ["PUBMED_API_KEY"] = _saved_key

print(f"\n{n}/{n} passed\n")
