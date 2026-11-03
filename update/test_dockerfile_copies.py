"""Every script run_all.py launches must be COPY'd into the image.

The Dockerfile copies update/ scripts one by one, so a new script wired into run_all.py
but not added there ships as "can't open file" — and because run_all isolates most
steps, the nightly still succeeds and the failure is easy to miss (externalAbstractImport,
2026-11-03).

Run: python3 update/test_dockerfile_copies.py
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
run_all = open(os.path.join(ROOT, "update", "run_all.py")).read()
dockerfile = open(os.path.join(ROOT, "Dockerfile")).read()

launched = sorted(set(re.findall(r"(?:python3|bash) ([A-Za-z0-9_]+\.(?:py|sh))", run_all)))
missing = [f for f in launched if not re.search(rf"^COPY update/{re.escape(f)}\s", dockerfile, re.M)]

print(f"{len(launched)} scripts launched by run_all.py")
for f in missing:
    print(f"FAIL not COPY'd in Dockerfile: {f}")
sys.exit(1 if missing or not launched else 0)
