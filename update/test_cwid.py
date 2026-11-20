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
                  canonical_sql, canonical_uid, is_bare_external_prefix, is_external_cohort,
                  java_strip, person_guard_queries, warn_person_identifiers)

# --- parity with ReCiter's reciter.utils.UidCanonicalizer (Java, authoritative) -------
# Every case from UidCanonicalizerTest. Where Java returns a value, so must Python;
# where Java throws InvalidUidException, Python returns (None, reason).
JAVA_VALID = [
    ("abl9002", "abl9002"), ("Abl9002", "abl9002"), ("DLAMB", "dlamb"), ("HAF9018", "haf9018"),
    ("Hlloyd", "hlloyd"), ("ggi9001 ", "ggi9001"), (" Isk9008", "isk9008"),
    ("\tJih9026\n", "jih9026"), (" Abl9002\u3000", "abl9002"), ("ab123", "ab123"),
    ("Jason.Carmel", "jason.carmel"), ("str2025-mi", "str2025-mi"), ("_lsw8099", "_lsw8099"),
    ("UC_Abc", "uc_abc"), ("fredhutch_Rachel.Rau", "fredhutch_Rachel.Rau"),
    (" fredhutch_Rachel.Rau ", "fredhutch_Rachel.Rau"), ("fh_catherine.albert", "fh_catherine.albert"),
    ("usc_123", "usc_123"), ("ucsf_AbC", "ucsf_AbC"), ("ucla_Xy-1", "ucla_Xy-1"),
    ("UCSF_AbC", "ucsf_AbC"), ("FREDHUTCH_A.B", "fredhutch_A.B"), ("Usc_123", "usc_123"),
]
JAVA_INVALID = [
    "", "   ", "kims7@mskcc.org", "ldn9002@nyp.org", "Mahender Jangari", "Hello, world!",
    "abc 123", "fredhutch_Rachel Rau", "usc_a@b", "abc/123", "abc\u00e9", "usc_", "fh_",
    "fredhutch_", "ucsf_", " UCLA_ ", "abc\r\n[ERROR] x", "abc\x00x",
]


def test_java_valid_cases():
    for raw, want in JAVA_VALID:
        assert canonical_uid(raw) == (want, None), (raw, canonical_uid(raw))


def test_java_invalid_cases():
    for raw in JAVA_INVALID:
        got, reason = canonical_uid(raw)
        assert got is None and reason, (raw, got)


def test_java_strip_edges():
    # Java's Character.isWhitespace excludes the no-break spaces and U+0085, which
    # Python's str.strip() removes; an id padded with them is invalid in Java.
    for ws in ("\u00a0", "\u2007", "\u202f", "\x85"):
        assert java_strip(ws + "abc" + ws) == ws + "abc" + ws, repr(ws)
        assert canonical_uid(ws + "abc")[0] is None, repr(ws)
    # ...and includes the information separators U+001C..U+001F and U+2028/U+2029.
    for ws in ("\x1c", "\x1f", "\x0b", "\x0c", "\u2028", "\u2029", "\u2003"):
        assert canonical_uid(ws + "Abc" + ws) == ("abc", None), repr(ws)


def test_ascii_only_prefix_fold():
    # Java's CASE_INSENSITIVE without UNICODE_CASE folds ASCII only: the Kelvin sign is
    # not `k`, so `uc\u212a_Ab` is not a cohort id there; both sides lowercase the whole
    # id, and `\u212a`.lower() is `k` in both Java and Python.
    assert not is_external_cohort("uc\u212a_Ab")
    assert canonical_uid("UC\u212a_Ab") == ("uck_ab", None)
    # Long s (U+017F) is not `s` either.
    assert not is_external_cohort("u\u017fc_1") and canonical_uid("u\u017fc_1")[0] is None


test_java_valid_cases()
test_java_invalid_cases()
test_java_strip_edges()
test_ascii_only_prefix_fold()

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
assert canonical_uid("Ucb2001") == ("ucb2001", None), "no underscore: a miscased CWID, not a cohort id"
assert canonical_uid("FREDHUTCH_X") == ("fredhutch_X", None), "prefix matched in any case, only it is lowercased"

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

# The prefix is exactly Java's (?:usc|fh|fredhutch|uc[a-z]+)_, ASCII case-insensitive,
# anchored by .match (Java lookingAt): `uc` needs one letter more.
assert EXTERNAL_COHORT_PREFIX.pattern == r"(?:usc|fh|fredhutch|uc[a-z]+)_"
assert EXTERNAL_COHORT_PREFIX.flags & re.IGNORECASE and EXTERNAL_COHORT_PREFIX.flags & re.ASCII
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
assert f"OR BINARY TRIM(personIdentifier) REGEXP '{EXTERNAL_COHORT_BARE_SQL}'" in q["invalid"]
assert EXTERNAL_COHORT_BARE_SQL == EXTERNAL_COHORT_PREFIX_SQL + "$"
assert "GROUP BY personIdentifier HAVING COUNT(*) > 1" in q["duplicated"]
assert "FROM `person`" in q["duplicated"] and "FROM `person`" in q["non-canonical"]
nc = q["non-canonical"]
assert f"CAST(personIdentifier AS BINARY) <> CAST({canonical_sql('personIdentifier')} AS BINARY)" in nc
for sql in q.values():
    assert re.match(r"^SELECT ", sql) and not re.search(r"\b(INSERT|UPDATE|DELETE|ALTER|DROP)\b", sql)

# The SQL prefix and the Python prefix agree. SQL spells the ASCII case-insensitivity out
# with character classes and applies it to BINARY values, so the collation plays no part.
# Simulate the SQL pattern and canonical_sql() (TRIM, then SUBSTRING_INDEX/LOCATE on the
# first '_') in Python and compare with canonical_uid on space-padded inputs.
_sql_prefix = re.compile(EXTERNAL_COHORT_PREFIX_SQL)  # case-sensitive, like BINARY
_sql_bare = re.compile(EXTERNAL_COHORT_BARE_SQL)
_sql_valid = re.compile(r"^[A-Za-z0-9._-]+$")


def _sql_canon(v):
    t = v.strip(" ")
    if _sql_prefix.search(t):
        i = t.index("_")
        return t[:i].lower() + t[i:]
    return t.lower()


def test_sql_rule_matches_python():
    samples = [r for r, _ in JAVA_VALID if "\t" not in r and "\u3000" not in r]
    samples += [r for r in JAVA_INVALID if "\r" not in r and "\x00" not in r]
    samples += ["FH_Abc", "Fh_Abc", "UCSF_X1", "ucB_x", "uC_x", "Ucb2001", "uscx_A", "USC", "uc_", "_pab4001"]
    for raw in samples:
        want, _ = canonical_uid(raw)
        c = _sql_canon(raw)
        sql_ok = bool(_sql_valid.search(c)) and not _sql_bare.search(c.strip(" "))
        if want is None:
            assert not sql_ok, (raw, c)
        else:
            assert sql_ok and c == want, (raw, c, want)
        assert bool(_sql_prefix.search(raw.strip(" "))) == is_external_cohort(raw.strip(" ")), raw


test_sql_rule_matches_python()

for _f in ["setup/createEventsProceduresReciterDb.sql", "setup/proc_updateCurateSelfRole_v3.17.sql",
           "scripts/cwid_remediation_2026_11/mysql_remediation.sql"]:
    _sql = open(os.path.join(ROOT, _f)).read()
    assert "uc[a-z]+_)" not in _sql, (_f, "old case-sensitive cohort prefix")
    _uses = [m.start() for m in re.finditer(re.escape("regexp '" + EXTERNAL_COHORT_PREFIX_SQL), _sql, re.I)]
    assert _uses, _f
    for _i in _uses:
        assert _sql[:_i].rstrip().lower().endswith(("binary trim(p.personidentifier)", "binary c.canon not",
                                                     "binary trim(u.personidentifier)",
                                                     "binary trim(personidentifier)")), (_f, _sql[_i - 60:_i])
_g = [sql for _, sql in person_guard_queries()]
assert all(sql.count("REGEXP '" + EXTERNAL_COHORT_PREFIX_SQL)
           == sql.count("BINARY TRIM(personIdentifier) REGEXP '" + EXTERNAL_COHORT_PREFIX_SQL) for sql in _g)

# The bare-prefix SQL agrees with the Python check on every case Python decides.
for raw in ["usc_", "fh_", "fredhutch_", "ucsf_", "FH_", "uc_", "ucsf_1", "usc", "_pab4001", "UCLA_"]:
    assert bool(_sql_bare.search(raw)) == is_bare_external_prefix(raw), raw


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
assert " ".join(canonical_sql("p.personIdentifier").lower().split()) + " as canon" in " ".join(flat.lower().split()).replace("( ", "(").replace(" )", ")")
assert f"where c.canon regexp '^[A-Za-z0-9._-]+$' and binary c.canon not regexp '{EXTERNAL_COHORT_BARE_SQL}'" in flat
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
