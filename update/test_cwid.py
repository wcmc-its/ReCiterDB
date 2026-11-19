"""CWID canonicalization (cwid.py) and the places that use it.

Run: python3 update/test_cwid.py
"""
import logging
import os
import re
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

from cwid import (EXTERNAL_COHORT_BARE_SQL, EXTERNAL_COHORT_PREFIX, EXTERNAL_COHORT_PREFIX_SQL,
                  canonical_uid, is_bare_external_prefix, is_external_cohort,
                  person_guard_queries, warn_person_identifiers)

# --- canonical_uid -------------------------------------------------------------------
# Converted on the way in: trimmed and lowercased (the bad forms found 11-18).
for raw, want in [
    ("abl9002", "abl9002"), ("Abl9002", "abl9002"), ("DLAMB", "dlamb"), ("NA", "na"),
    ("ggi9001 ", "ggi9001"), (" Isk9008", "isk9008"), ("\txoprotop \n", "xoprotop"),
    ("Minkowitz", "minkowitz"), ("jason.carmel", "jason.carmel"), ("str2025-mi", "str2025-mi"),
    ("_lsw8099", "_lsw8099"),          # leading-underscore family: undecided, kept as is
    ("ab12", "ab12"),                   # Cornell netid shape: no WCM-shape check
]:
    assert canonical_uid(raw) == (want, None), (raw, canonical_uid(raw))

# External cohorts keep their own key, case and all; only trimmed.
for raw, want in [
    ("fredhutch_Rachel.Rau", "fredhutch_Rachel.Rau"), ("fredhutch_Erin.crotty ", "fredhutch_Erin.crotty"),
    ("fh_davidg99", "fh_davidg99"), ("usc_26495", "usc_26495"), ("ucsf_63571", "ucsf_63571"),
    ("ucdavis_29922", "ucdavis_29922"), ("ucla_74837", "ucla_74837"), ("uci_226", "uci_226"),
    ("ucsd_2307", "ucsd_2307"),
]:
    assert canonical_uid(raw) == (want, None), (raw, canonical_uid(raw))
assert is_external_cohort("fredhutch_X") and not is_external_cohort("Ucb2001")
assert canonical_uid("Ucb2001") == ("ucb2001", None), "a miscased CWID is not a cohort id"
assert canonical_uid("FREDHUTCH_X") == ("fredhutch_x", None), "prefix match is case-sensitive"

# Rejected: still invalid after canonicalizing.
for raw, reason in [
    (None, "empty"), ("", "empty"), ("   ", "empty"),
    ("Hello, world!", "contains whitespace"), ("rodman john", "contains whitespace"),
    ("ldn9002@nyp.org", "contains '@'"), ("kims7@mskcc.org", "contains '@'"),
    ("abc!1", "invalid characters"), ("abc,1", "invalid characters"), ("abc/1", "invalid characters"),
    ("abc&x=1", "invalid characters"), ("abé1", "invalid characters"),
    ("fredhutch_A B", "contains whitespace"), ("fredhutch_A!", "invalid characters"),
    # A bare external prefix names nobody: rejected, also after trimming or lowercasing.
    ("usc_", "bare external prefix"), ("fh_", "bare external prefix"),
    ("fredhutch_", "bare external prefix"), ("ucsf_", "bare external prefix"),
    ("ucdavis_", "bare external prefix"), (" uci_\n", "bare external prefix"),
    ("FH_", "bare external prefix"), ("UCSF_", "bare external prefix"),
]:
    assert canonical_uid(raw) == (None, reason), (raw, canonical_uid(raw))

# The prefix is exactly ^(?:usc_|fh_|fredhutch_|uc[a-z]+_): `uc` needs one letter more.
assert EXTERNAL_COHORT_PREFIX.pattern == r"^(?:usc_|fh_|fredhutch_|uc[a-z]+_)"
assert not is_external_cohort("uc_X1") and canonical_uid("uc_X1") == ("uc_x1", None)
assert canonical_uid("uc_") == ("uc_", None), "uc_ is not a cohort prefix, so not a bare one"
assert is_external_cohort("ucb_X") and canonical_uid("ucb_X") == ("ucb_X", None)
assert not is_external_cohort("usc") and canonical_uid("USC") == ("usc", None)
assert not is_external_cohort("uscx_A") and canonical_uid("uscx_A") == ("uscx_a", None)
assert is_bare_external_prefix("ucsf_") and not is_bare_external_prefix("ucsf_1")
assert not is_bare_external_prefix("") and not is_bare_external_prefix(None)
# Underscore ids stay valid and separate from their plain form.
assert canonical_uid("_pab4001") == ("_pab4001", None) and canonical_uid("pab4001") == ("pab4001", None)

# --- guard queries -------------------------------------------------------------------
q = dict(person_guard_queries("person"))
assert set(q) == {"duplicated", "non-canonical", "invalid"}
assert "NOT REGEXP '^[A-Za-z0-9._-]+$'" in q["invalid"]
assert f"OR BINARY LOWER(TRIM(personIdentifier)) REGEXP '{EXTERNAL_COHORT_BARE_SQL}'" in q["invalid"]
assert EXTERNAL_COHORT_BARE_SQL == "^(usc_|fh_|fredhutch_|uc[a-z]+_)$"
assert "GROUP BY personIdentifier HAVING COUNT(*) > 1" in q["duplicated"]
assert "FROM `person`" in q["duplicated"] and "FROM `person`" in q["non-canonical"]
nc = q["non-canonical"]
assert "CAST(personIdentifier AS BINARY) <> CAST(TRIM(personIdentifier) AS BINARY)" in nc
assert "CAST(personIdentifier AS BINARY) <> CAST(LOWER(TRIM(personIdentifier)) AS BINARY)" in nc
assert "BINARY TRIM(personIdentifier) NOT REGEXP '^(usc_|fh_|fredhutch_|uc[a-z]+_)'" in nc
for sql in q.values():
    assert re.match(r"^SELECT ", sql) and not re.search(r"\b(INSERT|UPDATE|DELETE|ALTER|DROP)\b", sql)

# The SQL prefix and the Python prefix agree. The pattern text is the same, and every
# SQL use must apply it to a BINARY value: under the column's case-insensitive
# collation a bare REGEXP would treat `FH_Abc` as a cohort id while Python lowercases it.
assert EXTERNAL_COHORT_PREFIX_SQL == EXTERNAL_COHORT_PREFIX.pattern.replace("(?:", "(")
for raw in ["FH_Abc", "UCSF_X1", "Fredhutch_A"]:
    assert not is_external_cohort(raw) and canonical_uid(raw) == (raw.lower(), None), raw
for _f in ["setup/createEventsProceduresReciterDb.sql", "setup/proc_updateCurateSelfRole_v3.17.sql"]:
    _sql = open(os.path.join(ROOT, _f)).read().lower()
    _uses = [m.start() for m in re.finditer(re.escape("regexp '" + EXTERNAL_COHORT_PREFIX_SQL.lower()), _sql)]
    assert _uses, _f
    _bare = "regexp '" + EXTERNAL_COHORT_BARE_SQL.lower() + "'"
    for _i in _uses:
        if _sql.startswith(_bare, _i):
            # the bare-prefix refusal runs on the canonical value, still as BINARY
            assert _sql[:_i].rstrip().endswith("binary c.canon not"), (_f, _sql[_i - 60:_i])
            continue
        assert _sql[:_i].rstrip().endswith("binary trim(p.personidentifier)"), (_f, _sql[_i - 60:_i])
    assert _sql.count(_bare) == 1, _f
_g = [sql for _, sql in person_guard_queries()]
assert all(sql.count("REGEXP '" + EXTERNAL_COHORT_PREFIX_SQL) == sql.count("BINARY TRIM(personIdentifier) NOT REGEXP '" + EXTERNAL_COHORT_PREFIX_SQL)
           + sql.count("BINARY LOWER(TRIM(personIdentifier)) REGEXP '" + EXTERNAL_COHORT_BARE_SQL) for sql in _g)

# The bare-prefix SQL agrees with the Python check on every case Python decides.
_bare_re = re.compile(EXTERNAL_COHORT_BARE_SQL)
for raw in ["usc_", "fh_", "fredhutch_", "ucsf_", "FH_", "uc_", "ucsf_1", "usc", "_pab4001"]:
    _c = raw.strip().lower()
    assert bool(_bare_re.search(_c)) == is_bare_external_prefix(_c), raw


class FakeCursor:
    def __init__(self, results, fail=False):
        self.results, self.fail, self.sql = results, fail, []

    def execute(self, sql):
        if self.fail:
            raise RuntimeError("db down")
        self.sql.append(sql)

    def fetchall(self):
        return self.results.pop(0)


class Log:
    def __init__(self):
        self.warnings, self.infos = [], []

    def warning(self, m):
        self.warnings.append(m)

    def info(self, m):
        self.infos.append(m)


log = Log()
n = warn_person_identifiers(
    FakeCursor([[{"personIdentifier": "abl9002", "n": 2}], [{"personIdentifier": "Abl9002"}, ("ggi9001 ",)], []]),
    log)
assert n == 3, n
assert any("1 duplicated" in w and "'abl9002'" in w for w in log.warnings), log.warnings
assert any("2 non-canonical" in w and "'Abl9002'" in w and "'ggi9001 '" in w for w in log.warnings), log.warnings
log = Log()
assert warn_person_identifiers(FakeCursor([[], [], []]), log) == 0 and not log.warnings and len(log.infos) == 3
log = Log()
assert warn_person_identifiers(FakeCursor([], fail=True), log) == 0, "a failed check never raises"
assert len(log.warnings) == 3 and all("could not check" in w for w in log.warnings)
log = Log()
warn_person_identifiers(FakeCursor([[{"personIdentifier": f"x{i}"} for i in range(60)], [], []]), log, limit=50)
assert "(+10 more)" in log.warnings[0]

# --- executeFeatureGenerator ---------------------------------------------------------
# It reads its env at import time and imports psutil/boto3/pymysql at the top; stub what
# is missing so the pure helpers can be tested anywhere.
for k in ("DB_USERNAME", "DB_PASSWORD", "DB_HOST", "DB_NAME", "URL", "API_KEY"):
    os.environ.setdefault(k, "x")
for mod in ("psutil", "boto3", "pymysql"):
    try:
        __import__(mod)
    except ImportError:
        sys.modules[mod] = types.ModuleType(mod)
logging.disable(logging.CRITICAL)
import executeFeatureGenerator as efg  # noqa: E402

assert efg.canonical_person_identifiers(
    ["abl9002", "Abl9002", "xoprotop ", "cci4002 ", "kims7@mskcc.org", "", None, "Hello, world!",
     "fredhutch_Rachel.Rau"]) == ["abl9002", "xoprotop", "cci4002", "fredhutch_Rachel.Rau"]
url = efg.feature_generator_url("https://h/reciter/feature-generator/by/uid", "fredhutch_A.B", "ALL_PUBLICATIONS")
assert url.startswith("https://h/reciter/feature-generator/by/uid?uid=fredhutch_A.B&useGoldStandard=AS_EVIDENCE&"), url
assert url.endswith("&analysisRefreshFlag=true&retrievalRefreshFlag=ALL_PUBLICATIONS"), url
assert "uid=a%26b%3D1%20c&" in efg.feature_generator_url("u", "a&b=1 c", "X"), "uid is URL-encoded"

# --- Dockerfile ships the module both scripts import ---------------------------------
dockerfile = open(os.path.join(ROOT, "Dockerfile")).read()
assert re.search(r"^COPY update/cwid\.py \./", dockerfile, re.M), "cwid.py not COPY'd into the image"

# --- updateCurateSelfRole: reload file is the same routine as the master file --------
def routine(path):
    s = open(os.path.join(ROOT, path)).read()
    i = s.index("CREATE DEFINER=`admin`@`%` PROCEDURE `updateCurateSelfRole`()")
    return s[i:s.index("END;\n//", i)]


body = routine("setup/createEventsProceduresReciterDb.sql")
assert body == routine("setup/proc_updateCurateSelfRole_v3.17.sql"), "reload file drifted from master file"
flat = " ".join(body.split())
assert "else lower(trim(p.personIdentifier)) end as canon" in flat
assert "select case when binary trim(p.personIdentifier) regexp '^(usc_|fh_|fredhutch_|uc[a-z]+_)'" in flat
assert "where c.canon regexp '^[A-Za-z0-9._-]+$' and binary c.canon not regexp '^(usc_|fh_|fredhutch_|uc[a-z]+_)$'" in flat
assert ("and c.canon not in (select trim(u.personIdentifier) from admin_users u "
        "where u.personIdentifier is not null) group by c.canon;") in flat
for role in ("4", "3"):
    assert f"select distinct a.userID, {role} from person p" in flat
    assert (f"not exists (select 1 from admin_users_roles r where r.userID = a.userID "
            f"and r.roleID = {role})") in flat
assert "a.userID not in" not in flat and "left join (select userID" not in flat

# --- v3.17 DDL ------------------------------------------------------------------------
ddl = open(os.path.join(ROOT, "setup/alter_admin_users_unique_keys_v3.17.sql")).read()
assert "APPLY ONLY AFTER THE REMEDIATION SQL HAS REMOVED THE DUPLICATES" in ddl
assert "ADD UNIQUE KEY `uq_admin_users_personIdentifier` (`personIdentifier`)" in ddl
assert "ADD UNIQUE KEY `uq_admin_users_roles_user_role` (`userID`, `roleID`)" in ddl
assert "SET SESSION sql_mode = @old_sql_mode;" in ddl, "zero-date sql_mode restored"
assert ddl.count("WHEN @dups > 0") == 2 and ddl.count("WHEN @has > 0") == 2, "guarded + idempotent"
create = open(os.path.join(ROOT, "setup/createDatabaseTableReciterDb.sql")).read()
assert "UNIQUE KEY `uq_admin_users_personIdentifier`" in create
assert "UNIQUE KEY `uq_admin_users_roles_user_role`" in create

print("ok")
