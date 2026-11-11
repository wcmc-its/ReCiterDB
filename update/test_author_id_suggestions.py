"""authorIdSuggestions.py: the voting/planning selftest, and the run_all.py env gate.

The gate decides whether the nightly writes person_author_id at all, and it is driven by a
CronJob env patch rather than by code review, so the case worth pinning is the typo: an
unrecognised AUTHOR_ID_SUGGESTIONS value must mean off, never apply.

Run: python3 update/test_author_id_suggestions.py
"""
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

# run_all imports boto3/psutil at module scope for the S3 upload and memory logging; stubbed
# so this runs on a bare checkout (same approach as test_aar_lane_cadence.py).
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

os.environ.setdefault("LOG_FILE", "/tmp/test_author_id_suggestions.log")
os.environ.setdefault("S3_BUCKET", "unused-in-this-test")
os.environ.setdefault("S3_KEY_PREFIX", "unused-in-this-test")

import authorIdSuggestions                    # noqa: E402
from run_all import author_id_suggestions_cmd  # noqa: E402

assert authorIdSuggestions.selftest() == 0
print("  PASS selftest")

cases = [
    (None, None), ("", None), ("off", None), ("aply", None), ("yes", None), ("true", None),
    ("dry-run", "python3 authorIdSuggestions.py --dry-run"),
    (" Dry-Run ", "python3 authorIdSuggestions.py --dry-run"),
    ("apply", "python3 authorIdSuggestions.py --apply"),
    ("APPLY", "python3 authorIdSuggestions.py --apply"),
]
for value, want in cases:
    got = author_id_suggestions_cmd(value)
    assert got == want, f"FAIL AUTHOR_ID_SUGGESTIONS={value!r}: got {got!r}, wanted {want!r}"
    print(f"  PASS AUTHOR_ID_SUGGESTIONS={value!r} -> {got!r}")

# the hook must sit inside the guarded post-reporting block, after ORCID suggestions
src = open(os.path.join(HERE, "run_all.py")).read()
block = src[src.index("if overall_success and aar_lanes_safe():"):src.index("upload_log_to_s3()\n\n    if not overall_success")]
assert block.index("run_orcid_suggestions()") < block.index("run_author_id_suggestions()")
print("  PASS hook placed after run_orcid_suggestions() inside the aar_lanes_safe() block")
print("ok")
