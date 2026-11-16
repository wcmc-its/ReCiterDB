"""retrieveNIH.py: has_percentile / has_rcr, and a non-zero exit on a failed load (PM #1105 R1).

Pins three things:
  1. The flags: 1 when iCite returned a value (0.0 included), 0 when it returned null.
  2. The first 18 CSV fields are exactly what the loader wrote before, so nih_percentile and
     relative_citation_ratio still load as 0 for a missing value and v1 readers see no change.
  3. main() returns 0 only when the new data was swapped in; a validation failure, a failed
     swap or an empty load returns 1. run_all.py logs that step as failed but keeps going,
     so a stale iCite load never stops the reporting rebuild.

Runs with no database and no network (everything external is faked).
Run: python3 update/test_retrieve_nih.py
"""
import contextlib
import csv
import io
import os
import sys
import tempfile
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

# retrieveNIH opens retrieveNIH.log in the cwd at import time; keep it out of the checkout.
_tmp = tempfile.mkdtemp(prefix="test_retrieve_nih_")
_cwd = os.getcwd()
os.chdir(_tmp)
import retrieveNIH as rn  # noqa: E402
import logging  # noqa: E402

logging.disable(logging.CRITICAL)  # the faked runs below log failures on purpose

n = 0


def check(cond, msg):
    global n
    assert cond, msg
    n += 1


BASE_COLUMNS = ['pmid', 'year', 'is_research_article', 'is_clinical', 'relative_citation_ratio',
                'nih_percentile', 'citation_count', 'citations_per_year',
                'expected_citations_per_year', 'field_citation_rate', 'provisional', 'doi',
                'human', 'animal', 'molecular_cellular', 'apt', 'x_coord', 'y_coord']


def record(pmid, pct, rcr, provisional=False):
    return {"pmid": pmid, "year": 2024, "is_research_article": True, "is_clinical": False,
            "relative_citation_ratio": rcr, "nih_percentile": pct, "citation_count": 3,
            "citations_per_year": 1.5, "expected_citations_per_year": 1.0,
            "field_citation_rate": 2.0, "provisional": provisional, "doi": "10.1/x",
            "human": 1.0, "animal": 0.0, "molecular_cellular": 0.0, "apt": 0.05,
            "x_coord": 0.0, "y_coord": 1.0, "cited_by": [7], "references": [8, 9],
            "cited_by_clin": [7]}


# ---- 1. flags ------------------------------------------------------------------------------
check(rn.nih_flag_values(record(1, 89.8, 3.9)) == [1, 1], "values present -> 1, 1")
check(rn.nih_flag_values(record(2, None, None)) == [0, 0], "nulls -> 0, 0")
check(rn.nih_flag_values(record(3, 0.0, 0.0)) == [1, 1], "iCite 0.0 is a value, not missing")
check(rn.nih_flag_values(record(4, None, 1.2)) == [0, 1], "flags are independent")
check(rn.nih_flag_values({"pmid": 5}) == [0, 0], "absent keys -> 0, 0")

# ---- 2. CSV rows: 18 old fields unchanged, flags appended ----------------------------------
def write_rows(records):
    bufs = [io.StringIO() for _ in range(3)]
    writers = [csv.writer(b, delimiter='\t', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
               for b in bufs]
    rn.write_records_to_csv(records, writers)
    return [list(csv.reader(io.StringIO(b.getvalue()), delimiter='\t')) for b in bufs]


recs = [record(24586151, 89.8, 3.9), record(40643003, 0.0, 0.0, provisional=True),
        record(99, None, None)]
nih_rows, cites_rows, _ = write_rows(recs)
for rec, row in zip(recs, nih_rows):
    old = ['' if rec.get(c) is None else str(rec.get(c)) for c in BASE_COLUMNS]
    check(row[:18] == old, f"first 18 fields unchanged for {rec['pmid']}: {row[:18]}")
    check(len(row) == 20, f"20 fields for {rec['pmid']}")
check(nih_rows[0][18:] == ['1', '1'], "present -> 1 1")
check(nih_rows[1][18:] == ['1', '1'], "provisional 0.0 -> 1 1 (rated rule excludes provisional)")
check(nih_rows[2][18:] == ['0', '0'], "missing -> 0 0")
check(nih_rows[2][5] == '' and nih_rows[2][4] == '', "missing pct/rcr still written as '' (-> 0)")
check(len(cites_rows) == 9, "cites rows unchanged (3 per record)")
check(len(write_rows(recs)[2]) == 3, "cites_clin rows unchanged (1 per record)")

# ---- load column list ------------------------------------------------------------------------
check(rn.nih_load_columns(BASE_COLUMNS, True) == BASE_COLUMNS + ['has_percentile', 'has_rcr'],
      "with the columns: load them")
check(rn.nih_load_columns(BASE_COLUMNS, False) == BASE_COLUMNS + ['@has_percentile', '@has_rcr'],
      "without the columns: read into user variables and drop")


class FakeCursor:
    def __init__(self, flags_present=True):
        self.sql = []
        self.flags_present = flags_present

    def execute(self, sql, args=None):
        self.sql.append(sql)
        self._last = sql

    def fetchone(self):
        if "information_schema.columns" in self._last:
            return {"cnt": 2 if self.flags_present else 0}
        return {"cnt": 0, "COUNT(*)": 0}

    def fetchall(self):
        return []

    def close(self):
        pass


cur = FakeCursor()
rn.load_data_into_db(types.SimpleNamespace(commit=lambda: None), cur, "analysis_nih_new", "x.csv",
                     rn.nih_load_columns(BASE_COLUMNS, False))
load_sql = [s for s in cur.sql if "LOAD DATA" in s][0]
check("@has_percentile, @has_rcr)" in load_sql and "`@" not in load_sql,
      "user variables are not backquoted in LOAD DATA")
check(rn.staging_has_flag_columns(FakeCursor(True)) is True, "columns present")
check(rn.staging_has_flag_columns(FakeCursor(False)) is False, "columns absent")


# ---- 3. main() exit codes ---------------------------------------------------------------------
def run_main(*, records=True, valid=True, swap=True, flags_present=True):
    """Run main() with the DB and API faked. Returns (exit code, calls)."""
    calls = {"swap": 0, "restore": 0, "cleanup": 0, "load_columns": None}
    cur = FakeCursor(flags_present)
    db = types.SimpleNamespace(commit=lambda: None, close=lambda: None,
                               cursor=lambda *_: cur)

    def load(_db, _cur, table, path, columns):
        if table == "analysis_nih_new":
            calls["load_columns"] = list(columns)

    def do_swap(*_):
        calls["swap"] += 1
        return swap

    def do_restore(*_):
        calls["restore"] += 1
        return True

    def do_cleanup(*_):
        calls["cleanup"] += 1

    fakes = {
        "connect_mysql_server": lambda *a, **k: db,
        "create_staging_tables": lambda *a: None,
        "get_person_article_pmid": lambda *_: ["1", "2"],
        "get_nih_records": lambda _url: [record(1, 50.0, 1.0), record(2, None, None)] if records else [],
        "load_data_into_db": load,
        "validate_data": lambda *a, **k: valid,
        "atomic_table_swap": do_swap,
        "restore_from_backup": do_restore,
        "cleanup_staging_tables": do_cleanup,
    }
    saved = {k: getattr(rn, k) for k in fakes}
    saved_sleep = rn.time.sleep
    env = {"DB_USERNAME": "u", "DB_PASSWORD": "p", "DB_HOST": "h", "DB_NAME": "d"}
    saved_env = {k: os.environ.get(k) for k in env}
    try:
        for k, v in fakes.items():
            setattr(rn, k, v)
        rn.time.sleep = lambda *_: None
        os.environ.update(env)
        with contextlib.redirect_stdout(io.StringIO()):
            code = rn.main()
    finally:
        for k, v in saved.items():
            setattr(rn, k, v)
        rn.time.sleep = saved_sleep
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    return code, calls


code, calls = run_main()
check(code == 0 and calls["swap"] == 1, f"success -> 0 (got {code})")
check(calls["load_columns"][-2:] == ['has_percentile', 'has_rcr'], "flags loaded when present")

code, calls = run_main(flags_present=False)
check(code == 0 and calls["load_columns"][-2:] == ['@has_percentile', '@has_rcr'],
      "pre-DDL: still loads and swaps, flags dropped")

code, calls = run_main(valid=False)
check(code == 1 and calls["swap"] == 0 and calls["cleanup"] == 1,
      f"validation failure -> 1, no swap, staging cleaned (got {code}, {calls})")

code, calls = run_main(swap=False)
check(code == 1 and calls["restore"] == 1, f"swap failure -> 1 after restore (got {code})")

code, calls = run_main(records=False)
check(code == 1 and calls["swap"] == 0, f"no iCite data -> 1, no swap (got {code})")

# ---- run_all: retrieveNIH failure is logged but does not stop the pipeline ---------------------
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
os.environ.setdefault("LOG_FILE", os.path.join(_tmp, "run_all.log"))
os.environ.setdefault("S3_BUCKET", "unused-in-this-test")
os.environ.setdefault("S3_KEY_PREFIX", "unused-in-this-test")
import run_all  # noqa: E402

check(run_all.NON_BLOCKING_SCRIPTS == {"retrieveNIH"}, "only retrieveNIH is non-blocking")


def run_all_main(failing):
    ran = []

    def fake_run_script(name, cmd, timeout_seconds=None):
        ran.append(name)
        return name not in failing

    names = ["run_script", "run_external_article_etl", "run_external_abstract_import",
             "aar_lanes_safe", "upload_log_to_s3"]
    saved = {k: getattr(run_all, k) for k in names}
    try:
        run_all.run_script = fake_run_script
        run_all.run_external_article_etl = lambda: None
        run_all.run_external_abstract_import = lambda: None
        run_all.aar_lanes_safe = lambda: False   # skip the lanes; not under test
        run_all.upload_log_to_s3 = lambda: None
        with contextlib.redirect_stdout(io.StringIO()):
            try:
                run_all.main()
                code = None
            except SystemExit as e:
                code = e.code
    finally:
        for k, v in saved.items():
            setattr(run_all, k, v)
    return code, ran


for _h in list(run_all.logger.handlers):
    if isinstance(_h, run_all.logging.StreamHandler) and not isinstance(_h, run_all.logging.FileHandler):
        run_all.logger.removeHandler(_h)

class _ListHandler(run_all.logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


logging.disable(logging.NOTSET)  # capture the closing lines; the console handler is gone
run_all.logger.propagate = False  # keep them off retrieveNIH's root console handler
_lh = _ListHandler()
run_all.logger.addHandler(_lh)
code, ran = run_all_main({"retrieveNIH"})
run_all.logger.removeHandler(_lh)
check("nightlyIndexing" in ran and ran[-1] == "conflictsImport",
      f"retrieveNIH failure: the rest of the pipeline still runs ({ran})")
check(code == 0, f"retrieveNIH failure does not fail the job (no restart of the nightly) ({code})")
check(not any("All scripts completed successfully" in m for m in _lh.messages)
      and any("non-blocking step(s) failed: retrieveNIH" in m for m in _lh.messages),
      "a run with a failed retrieveNIH is not logged as a full success")

_lh = _ListHandler()
run_all.logger.addHandler(_lh)
code, ran = run_all_main(set())
run_all.logger.removeHandler(_lh)
check(code == 0 and any("All scripts completed successfully" in m for m in _lh.messages),
      f"a clean run still logs full success ({code})")
run_all.logger.propagate = True
logging.disable(logging.CRITICAL)

code, ran = run_all_main({"retrieveArticles"})
check("retrieveNIH" not in ran and code == 1, f"other failures still stop the pipeline ({ran}, {code})")

os.chdir(_cwd)
print(f"test_retrieve_nih: {n} checks passed")
