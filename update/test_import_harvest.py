"""importHarvest.py: the fixture selftest, the --apply refusal, the run_all.py env gate and hook
placement, and (when a Publication Manager checkout is available) the scope/hidden type sets
against PM's src/lib/pubTypes.ts.

The gate is driven by a CronJob env patch, so the cases worth pinning are the typo (an
unrecognised IMPORT_HARVEST must mean off) and apply (must reach the script's refusal, never
a write path).

Run: python3 update/test_import_harvest.py
     PM_PUBTYPES=<PM>/src/lib/pubTypes.ts python3 update/test_import_harvest.py   (adds the drift check)
"""
import os
import re
import subprocess
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

# run_all imports boto3/psutil at module scope; stubbed so this runs on a bare checkout
# (same approach as test_author_id_suggestions.py).
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

os.environ.setdefault("LOG_FILE", "/tmp/test_import_harvest.log")
os.environ.setdefault("S3_BUCKET", "unused-in-this-test")
os.environ.setdefault("S3_KEY_PREFIX", "unused-in-this-test")

import importHarvest                     # noqa: E402
from run_all import import_harvest_cmd   # noqa: E402

assert importHarvest.selftest() == 0
print("  PASS selftest (fixtures + shared title_year_key vectors)")

# --apply refuses before it touches the DB: no DB_* env here, so reaching run() would crash
env = {k: v for k, v in os.environ.items() if not k.startswith("DB_")}
p = subprocess.run([sys.executable, os.path.join(HERE, "importHarvest.py"), "--apply"],
                   capture_output=True, text=True, env=env, timeout=60)
assert p.returncode == 2 and "REFUSING --apply" in p.stdout, (p.returncode, p.stdout, p.stderr)
print("  PASS --apply refused (exit 2) without opening a DB connection")

cases = [
    (None, None), ("", None), ("off", None), ("aply", None), ("yes", None), ("true", None),
    ("dry-run", "python3 importHarvest.py --dry-run"),
    (" Dry-Run ", "python3 importHarvest.py --dry-run"),
    ("apply", "python3 importHarvest.py --apply"),
]
for value, want in cases:
    got = import_harvest_cmd(value)
    assert got == want, f"FAIL IMPORT_HARVEST={value!r}: got {got!r}, wanted {want!r}"
    print(f"  PASS IMPORT_HARVEST={value!r} -> {got!r}")
assert import_harvest_cmd("dry-run", 3480) == "python3 importHarvest.py --dry-run --max-seconds 3480"
print("  PASS --max-seconds passed through")

src = open(os.path.join(HERE, "run_all.py")).read()
block = src[src.index("if overall_success and aar_lanes_safe():"):src.index("upload_log_to_s3()\n\n    if not overall_success")]
assert block.index("run_author_id_suggestions()") < block.index("run_import_harvest()")
print("  PASS hook placed after run_author_id_suggestions() inside the aar_lanes_safe() block")

pm = os.environ.get("PM_PUBTYPES") or os.path.join(HERE, "..", "..", "ReCiter-Publication-Manager", "src", "lib", "pubTypes.ts")
if os.path.exists(pm):
    ts = open(pm).read()

    def keys(name, const):
        block = re.search(rf"const {name}: Record<string, SourceTypeMapping> = \{{(.*?)\n\}};", ts, re.S).group(1)
        return {(m.group(1) or m.group(2)) for m in
                re.finditer(rf'^\s*(?:"([^"]+)"|([A-Za-z0-9_-]+))\s*:\s*{const},', block, re.M)}

    for src_name in ("ORCID", "OPENALEX", "CROSSREF", "SCOPUS"):
        want_out = keys(src_name, "OUT_OF_SCOPE")
        got_out = importHarvest.OUT_OF_SCOPE[src_name] - ({"paratext", "supplementary-materials"} if src_name == "OPENALEX" else set())
        assert got_out == want_out, (src_name, got_out, want_out)
        want_hidden = keys(src_name, "PRESENTATION") | keys(src_name, "ERRATUM") | keys(src_name, "RETRACTION")
        assert importHarvest.HIDDEN[src_name] == want_hidden, (src_name, importHarvest.HIDDEN[src_name], want_hidden)
    print(f"  PASS scope/hidden sets match {os.path.relpath(pm)}")
else:
    print("  SKIP pubTypes.ts drift check (no PM checkout; set PM_PUBTYPES)")
print("ok")
