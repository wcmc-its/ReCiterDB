"""One rule for the person identifier (uid / cwid / personIdentifier) at every write.

DynamoDB keys are exact strings and MySQL compares case-insensitively, so `Abl9002`
and `abl9002` are two people in ReCiter and one duplicated person here. The rule
(approved 2026-11-18, Roadmap/cwid-normalization-plan.md) is ReCiter's
`reciter.utils.UidCanonicalizer.canonicalize` (ReCiter #787/#789), and this port must
give byte-identical output, or the two systems disagree on a person's key:

  1. Strip surrounding whitespace as Java String.strip() does: Character.isWhitespace,
     which excludes the no-break spaces U+00A0, U+2007, U+202F and the next-line
     U+0085 (Python's str.strip() removes all four). Empty -> invalid. (The SQL in
     updateCurateSelfRole uses TRIM(), which strips spaces only: an id wrapped in a
     tab or newline is refused there, not converted, and the nightly guard lists it
     as invalid.)
  2. If the id starts with an external-cohort prefix, matched ASCII-case-insensitively
     (`usc_`, `fh_`, `fredhutch_`, `uc<letters>_`): lowercase only the prefix and keep
     the partner's own key as it is (`UCSF_AbC` -> `ucsf_AbC`, `fredhutch_Rachel.Rau`
     unchanged). A bare prefix with nothing after it (`usc_`) is invalid. Otherwise
     lowercase the whole id.
  3. Valid only if the result matches ^[a-z0-9._-]+$ (^[A-Za-z0-9._-]+$ for a cohort id).

`uc_` is not a cohort prefix (uc[a-z]+ needs at least one letter after `uc`).

There is no WCM CWID shape check on purpose: Cornell netids and the external cohorts
(usc_123, fh_x, fredhutch_A.B, ucsf_..., ucdavis_..., ucla_..., ucsd_..., uci_...)
have to keep working.
"""
import re
import unicodedata

# Java: Pattern.compile("^(?:usc|fh|fredhutch|uc[a-z]+)_", CASE_INSENSITIVE) + lookingAt().
# Without UNICODE_CASE, Java folds ASCII only; re.ASCII does the same here (otherwise
# Python would let the Kelvin sign U+212A match `k`).
EXTERNAL_COHORT_PREFIX = re.compile(r"(?:usc|fh|fredhutch|uc[a-z]+)_", re.IGNORECASE | re.ASCII)
# The same prefix as a SQL pattern. ASCII case-insensitivity is spelled out with
# character classes, and every SQL use applies it to a BINARY value, so the match does
# not depend on the column collation or on the server's REGEXP case handling.
EXTERNAL_COHORT_PREFIX_SQL = ("^([Uu][Ss][Cc]|[Ff][Hh]|[Ff][Rr][Ee][Dd][Hh][Uu][Tt][Cc][Hh]"
                              "|[Uu][Cc][A-Za-z]+)_")
# A bare prefix with nothing after it (`usc_`, `UCSF_`): rejected, in SQL and Python.
EXTERNAL_COHORT_BARE_SQL = EXTERNAL_COHORT_PREFIX_SQL + "$"

_VALID = re.compile(r"[a-z0-9._-]+")
_VALID_EXTERNAL = re.compile(r"[A-Za-z0-9._-]+")

# Java Character.isWhitespace: SPACE/LINE/PARAGRAPH_SEPARATOR except the no-break
# spaces, plus these controls.
_JAVA_WS_CONTROLS = frozenset("\t\n\x0b\x0c\r\x1c\x1d\x1e\x1f")
_JAVA_NOT_WS = frozenset("\u00a0\u2007\u202f")


def _java_is_whitespace(ch):
    return ch in _JAVA_WS_CONTROLS or (
        ch not in _JAVA_NOT_WS and unicodedata.category(ch) in ("Zs", "Zl", "Zp"))


def java_strip(s):
    """Java String.strip(): trim Character.isWhitespace code points from both ends."""
    i, j = 0, len(s)
    while i < j and _java_is_whitespace(s[i]):
        i += 1
    while j > i and _java_is_whitespace(s[j - 1]):
        j -= 1
    return s[i:j]


def is_external_cohort(uid):
    """True when the (already stripped) uid starts with a cohort prefix, any ASCII case."""
    return bool(EXTERNAL_COHORT_PREFIX.match(uid or ""))


def canonical_uid(raw):
    """Return (canonical, None) or (None, reason). Never raises on bad input.

    Same output as ReCiter's UidCanonicalizer.canonicalize for every input; where Java
    throws InvalidUidException this returns (None, reason)."""
    if raw is None:
        return None, "empty"
    uid = java_strip(str(raw))
    if not uid:
        return None, "empty"
    m = EXTERNAL_COHORT_PREFIX.match(uid)
    if m and m.end() == len(uid):
        return None, "bare external prefix"
    # str.lower() and Java toLowerCase(Locale.ROOT) differ only on characters that the
    # validity check below refuses either way (the Kelvin sign lowercases to `k` in both).
    uid = (m.group().lower() + uid[m.end():]) if m else uid.lower()
    if (_VALID_EXTERNAL if m else _VALID).fullmatch(uid):
        return uid, None
    if re.search(r"\s", uid):
        return None, "contains whitespace"
    if "@" in uid:
        return None, "contains '@'"
    return None, "invalid characters"


def is_bare_external_prefix(uid):
    """True for an external-cohort prefix with nothing after it: `usc_`, `UCSF_`."""
    m = EXTERNAL_COHORT_PREFIX.match(uid or "")
    return bool(m) and m.end() == len(uid)


def canonical_sql(col):
    """SQL expression for the canonical form of `col`, as far as SQL can compute it:
    TRIM() (spaces only), then lowercase the cohort prefix only, or the whole id. The
    prefix is letters followed by its first `_`, so SUBSTRING_INDEX/LOCATE split it."""
    t = f"TRIM({col})"
    return (f"CASE WHEN BINARY {t} REGEXP '{EXTERNAL_COHORT_PREFIX_SQL}' "
            f"THEN CONCAT(LOWER(SUBSTRING_INDEX({t}, '_', 1)), SUBSTRING({t}, LOCATE('_', {t}))) "
            f"ELSE LOWER({t}) END")


def person_guard_queries(table="person"):
    """Three read-only queries for the nightly guard, as (label, sql) pairs.

    duplicated:    personIdentifier values held by more than one row. The table's
                   collation is case-insensitive, so `Abl9002` and `abl9002` group
                   together here, which is exactly the collision PM sees.
    non-canonical: rows whose stored value differs (byte-wise) from its canonical
                   form (canonical_sql): stray spaces, upper case outside a cohort id,
                   or an upper-case cohort prefix (`UCSF_AbC`, canonical `ucsf_AbC`).
    invalid:       rows no canonicalizing can save (an email, inner whitespace, '!',
                   or a bare external prefix such as `usc_` or `FH_`).
    """
    duplicated = (
        f"SELECT personIdentifier, COUNT(*) AS n FROM `{table}` "
        "GROUP BY personIdentifier HAVING COUNT(*) > 1 "
        "ORDER BY personIdentifier"
    )
    non_canonical = (
        f"SELECT personIdentifier FROM `{table}` "
        f"WHERE CAST(personIdentifier AS BINARY) <> CAST({canonical_sql('personIdentifier')} AS BINARY) "
        "ORDER BY personIdentifier"
    )
    invalid = (
        f"SELECT personIdentifier FROM `{table}` "
        "WHERE personIdentifier IS NULL OR TRIM(personIdentifier) NOT REGEXP '^[A-Za-z0-9._-]+$' "
        f"OR BINARY TRIM(personIdentifier) REGEXP '{EXTERNAL_COHORT_BARE_SQL}' "
        "ORDER BY personIdentifier"
    )
    return [("duplicated", duplicated), ("non-canonical", non_canonical), ("invalid", invalid)]


def warn_person_identifiers(cursor, log, table="person", limit=50):
    """Run the guard queries and log a WARN naming what they find. Never raises:
    a failed check is itself logged as a WARN, and the nightly carries on."""
    found = 0
    for label, sql in person_guard_queries(table):
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
        except Exception as e:  # noqa: BLE001 - this guard must never fail the job
            log.warning(f"CWID GUARD: could not check {label} personIdentifier in {table}: {e}")
            continue
        ids = [r["personIdentifier"] if isinstance(r, dict) else r[0] for r in rows]
        found += len(ids)
        if ids:
            shown = ", ".join(repr(i) for i in ids[:limit])
            more = f" (+{len(ids) - limit} more)" if len(ids) > limit else ""
            log.warning(f"CWID GUARD: {len(ids)} {label} personIdentifier in {table}: {shown}{more}")
        else:
            log.info(f"CWID GUARD: 0 {label} personIdentifier in {table}")
    return found
