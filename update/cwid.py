"""One rule for the person identifier (uid / cwid / personIdentifier) at every write.

DynamoDB keys are exact strings and MySQL compares case-insensitively, so `Abl9002`
and `abl9002` are two people in ReCiter and one duplicated person here. The rule
(approved 2026-11-18, Roadmap/cwid-normalization-plan.md):

  1. Trim surrounding whitespace. (The SQL in updateCurateSelfRole uses TRIM(), which
     strips spaces only: an id wrapped in a tab or newline is refused there, not
     converted, and the nightly guard lists it as invalid.)
  2. Lowercase, unless the id is an external-cohort id. Those carry the partner's
     own key (fredhutch_Rachel.Rau) and must stay as they are.
  3. Reject only what is still invalid: empty, any inner whitespace, '@', or a
     character outside [a-z0-9._-] ([A-Za-z0-9._-] for an external-cohort id).

There is no WCM CWID shape check on purpose: Cornell netids and the external cohorts
(usc_123, fh_x, fredhutch_A.B, ucsf_..., ucdavis_..., ucla_..., ucsd_..., uci_...)
have to keep working.
"""
import re

# Case-sensitive on purpose: `Ucb2001` is a miscased CWID, not a cohort id.
EXTERNAL_COHORT_PREFIX = re.compile(r"^(?:usc|fh|fredhutch|uc[a-z]*)_")
# The same prefixes as a SQL pattern. Every SQL use applies it to a BINARY value
# (`BINARY TRIM(x) REGEXP ...`) so it is case-sensitive like the Python one; under
# the column's case-insensitive collation `FH_x` would otherwise count as a cohort id.
# Prod is MariaDB, where REGEXP on BINARY is supported (and used elsewhere here).
EXTERNAL_COHORT_PREFIX_SQL = "^(usc|fh|fredhutch|uc[a-z]*)_"

_VALID = re.compile(r"^[a-z0-9._-]+$")
_VALID_EXTERNAL = re.compile(r"^[A-Za-z0-9._-]+$")


def is_external_cohort(uid):
    return bool(EXTERNAL_COHORT_PREFIX.match(uid or ""))


def canonical_uid(raw):
    """Return (canonical, None) or (None, reason). Never raises on bad input."""
    if raw is None:
        return None, "empty"
    uid = str(raw).strip()
    if not uid:
        return None, "empty"
    external = is_external_cohort(uid)
    if not external:
        uid = uid.lower()
    if re.search(r"\s", uid):
        return None, "contains whitespace"
    if "@" in uid:
        return None, "contains '@'"
    if not (_VALID_EXTERNAL if external else _VALID).match(uid):
        return None, "invalid characters"
    return uid, None


def person_guard_queries(table="person"):
    """Three read-only queries for the nightly guard, as (label, sql) pairs.

    duplicated:    personIdentifier values held by more than one row. The table's
                   collation is case-insensitive, so `Abl9002` and `abl9002` group
                   together here, which is exactly the collision PM sees.
    non-canonical: rows whose stored value differs (byte-wise) from its canonical
                   form. External-cohort ids are checked for whitespace only.
    invalid:       rows no canonicalizing can save (an email, inner whitespace, '!').
    """
    duplicated = (
        f"SELECT personIdentifier, COUNT(*) AS n FROM `{table}` "
        "GROUP BY personIdentifier HAVING COUNT(*) > 1 "
        "ORDER BY personIdentifier"
    )
    non_canonical = (
        f"SELECT personIdentifier FROM `{table}` "
        "WHERE CAST(personIdentifier AS BINARY) <> CAST(TRIM(personIdentifier) AS BINARY) "
        f"OR (BINARY TRIM(personIdentifier) NOT REGEXP '{EXTERNAL_COHORT_PREFIX_SQL}' "
        "AND CAST(personIdentifier AS BINARY) <> CAST(LOWER(TRIM(personIdentifier)) AS BINARY)) "
        "ORDER BY personIdentifier"
    )
    invalid = (
        f"SELECT personIdentifier FROM `{table}` "
        "WHERE personIdentifier IS NULL OR TRIM(personIdentifier) NOT REGEXP '^[A-Za-z0-9._-]+$' "
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
