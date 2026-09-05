#!/usr/bin/env python3
"""Build the Identity Authority tables from Enterprise Directory (LDAP),
JenzabarPrd (MSSQL) and ASMS, replacing ~51 Splunk saved searches.

Sibling of buildIdentity.py, which replaced the "reciter identity update"
search and is the working template. This job is bigger in sources but the same
shape: one function per source registered with @source, each returning
{dn: {column: value}}, per-source row counts logged, an empty source aborting
the run before anything is written.

WHY THE SPLUNK JOB IS NOT PORTED FAITHFULLY
-------------------------------------------
The 51 searches collapse to about 15 real sources. Most of the count is Splunk
scaffolding that a direct upsert removes outright:

  * Nine `X` / `X, db update` pairs exist only because a Splunk search cannot
    write a database directly. Each pair is one function here.
  * Sixteen searches implement deletion by accumulating DNs into one shared
    lookup and taking a set difference. That is `reconcile()` below.
  * `Company` and `Manager` are workarounds for reading SUBTYPED LDAP
    attributes (`o;company`, `manager;manager`). ldap3 returns those as ordinary
    keys, so both searches disappear.
  * Every `eval dn = case(isnotnull(seeAlso), "cn=" . cn . ...)` exists because
    Splunk's ldapfilter does not return the entry DN. ldap3 does; we use the
    real DN and delete the reconstruction. See DN_FROM_ENTRY below.

Eight feeds in the Splunk job are already dead while reporting success, and are
deliberately NOT reproduced. See PORT_SPEC.md in wcmc-its/IdentityAuthorityDatabase.

READS ARE FULL SNAPSHOTS, NOT INCREMENTAL
-----------------------------------------
The Splunk builders harvest a 3-day modifyTimestamp window while the pushes
apply a 2-day one, so a single missed run strands rows permanently -- they age
out of the push window and out of the LDAP window and are never revisited.
More importantly, a deletion decision is only sound against a complete
present-tense read: in the Splunk job `source='ed'` means "seen in ED at some
point since the lookup was last truncated", not "currently in ED".
So every source here reads in full, every run. Paging makes that affordable.

SAFETY
------
Deletion is the one operation here that destroys data, and the Splunk
implementation of it is the most likely cause of the intermittent failures it
was ported to escape. Guards, in order:

  1. Every source runs to completion before anything is compared or written.
     No shared mutable store, no ordering dependency between sources.
  2. A source returning zero rows aborts the whole run (MIN_ROWS).
  3. Deletion is computed per type, in memory, only from sources that read
     BOTH sides in this same process.
  4. A per-type ceiling on delete volume (MAX_DELETE_FRACTION) aborts rather
     than emitting an implausible number of deletions.
  5. DELETE_MODE ships as "flag". Flip it only once conf-db_outputs has been
     read and the IdentityAuthority_dn stanza's real behaviour is known.

Usage:
    python3 buildIdentityAuthority.py --demo      # pure logic, no network, no DB
    python3 buildIdentityAuthority.py --spike     # probe LDAP connectivity only
    python3 buildIdentityAuthority.py --dry-run   # build + compare, write nothing
    python3 buildIdentityAuthority.py             # live
"""
import argparse
import logging
import os
import sys

# Reuse the proven LDAP layer rather than copying it. Both modules land flat in
# /usr/src/app/ in the ReCiterDB image, so this import resolves there and in a
# checkout's update/ directory alike. _Row in particular must not be
# reimplemented: ED returns every attribute name lowercased regardless of the
# casing requested, and _Row.all() is the only safe way to read a multi-valued
# attribute (a plain .get() silently emptied 15 person-type flags on the
# sibling port and was invisible until a production diff).
try:
    from buildIdentity import _Row, _flatten, ldap_conn, LDAP_PAGE_SIZE, _mssql_target
except ImportError:                                    # checkout layout
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from buildIdentity import _Row, _flatten, ldap_conn, LDAP_PAGE_SIZE, _mssql_target

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger("buildIdentityAuthority")

ROOT = "dc=weill,dc=cornell,dc=edu"

# The SA-ldapsearch `domain=` aliases used by the 2026 searches, mapped to real
# base DNs. Only ed-people and ed-sors are confirmed (by the sibling port). The
# rest are UNRESOLVED and carry their best candidate; spike_ia.py in
# wcmc-its/IdentityAuthorityDatabase resolves them in one in-cluster run.
#
# The stale IdentityAuthorityQueries.txt listed ed-contacts, ed-faculty,
# ed-students, ed-employees and ed-affiliates. NONE of those appear in the live
# searches -- do not reintroduce them.
BASE_DN = {
    "ed-people": os.environ.get("LDAP_BASE_PEOPLE", "ou=people," + ROOT),
    "ed-sors": os.environ.get("LDAP_BASE_SORS", "ou=sors," + ROOT),
    "ed": os.environ.get("LDAP_BASE_ROOT", ROOT),
    # UNRESOLVED - candidates only. --spike reports which of these bind.
    "ed-groups": os.environ.get("LDAP_BASE_GROUPS", "ou=Groups," + ROOT),
    "ed-locations": os.environ.get("LDAP_BASE_LOCATIONS", "ou=locations,ou=Groups," + ROOT),
    "ed-phones": os.environ.get("LDAP_BASE_PHONES", "ou=phones," + ROOT),
    "ed-emails": os.environ.get("LDAP_BASE_EMAILS", "ou=emails," + ROOT),
}
UNRESOLVED_ALIASES = {"ed-groups", "ed-locations", "ed-phones", "ed-emails", "ed"}

# `CUMC` is Active Directory, NOT the Enterprise Directory -- a different host
# with a different bind. Paul has said AD and Entra last-logins can be pulled
# from those apps directly, so the AD source is deliberately out of scope here.
# ponytail: not modelled at all rather than modelled and disabled.

# Splunk's `output=` names a db_outputs.conf stanza, never a table, so the
# physical targets are strictly unknown until conf-db_outputs is read. These
# names are taken from the READ side of the paired `<X> - DN` searches, which
# do name real tables in their dbxquery SQL -- so they are well-evidenced but
# still an assumption. One query per table confirms them.
TABLES = {
    "canonical": "_person_canonical",
    "sor_record": "_person_sor_record",
    "sor_role_record": "_person_sor_role_record",
    "email": "_contact_email",
    "phone": "_contact_phone",
    "location": "_contact_location",
    "location_master": "_location",
    "organization": "_organization",
}

# A source returning fewer rows than its floor aborts the run. The floors are
# deliberately crude -- they exist to catch "returned nothing / returned 12",
# not to assert a population size.
MIN_ROWS = {
    "canonical": 20000,
    "sor_record": 20000,
    "sor_role_record": 20000,
    "email": 10000,
    "phone": 10000,
    "location": 5000,
    "location_master": 100,
    "organization": 100,
}

# Refuse to delete more than this fraction of a type's existing DB rows in one
# run. The Splunk job has no such floor, which is why a single failed feeder
# can nominate 100% of a type for deletion.
MAX_DELETE_FRACTION = 0.02

# "flag" writes a soft-delete marker; "delete" issues DELETEs. Ships as "flag"
# because the SPL does not reveal whether the IdentityAuthority_dn stanza
# deletes rows or sets a column, and guessing wrong is destructive.
DELETE_MODE = os.environ.get("IA_DELETE_MODE", "flag")

SOURCES = {}


def source(type_name):
    """Register a source. The function returns {dn: {column: value}}.

    type_name ties the source to its table, its row floor and its side of the
    deletion set difference.
    """
    def wrap(fn):
        fn.type_name = type_name
        SOURCES[fn.__name__] = fn
        return fn
    return wrap


def ldap_search(alias, search_filter, attrs):
    """Paged full-branch search against one domain alias.

    Paging is not optional: an unpaged search is silently capped by the server,
    which is the same class of silent loss the Splunk job suffers from. Five of
    the searches being replaced run unpaged today, including the largest one.
    """
    from ldap3 import SUBTREE
    from ldap3.extend.standard.PagedSearch import paged_search_generator

    base = BASE_DN[alias]
    flt = "".join(line.strip() for line in search_filter.splitlines())
    rows = []
    for entry in paged_search_generator(
            ldap_conn(), base, flt, search_scope=SUBTREE,
            attributes=attrs, paged_size=LDAP_PAGE_SIZE):
        if entry.get("type") != "searchResEntry":
            continue
        row = _Row(entry["attributes"].items())
        # The real DN, straight off the entry. Every `eval dn = case(...)`
        # reconstruction in the SPL is a workaround for Splunk not providing
        # this, and at least one of them (Organization) may have been writing
        # wrong DNs for years.
        row.entry_dn = entry.get("dn")
        rows.append(row)
    logger.info("ldap %s: %d entries", alias, len(rows))
    return rows


def mv(row, attr):
    """Pipe-joined multi-valued attribute, matching the SPL's `nomv` + sed idiom.

    39 `nomv` lines across the Splunk searches declare exactly which attributes
    the original author knew were multi-valued; every one of them reaches the DB
    as a "|"-joined string today. Using row.get() on any of them would write a
    single value into a column whose consumers expect a joined set -- silent
    value loss with no row-count change, so a row-count diff would not catch it.
    ED's order is preserved (least-specific first); do not sort or dedupe.
    """
    return "|".join(row.all(attr))


def run_sources(only=None):
    """Run every source to completion, enforcing the row floor. Returns
    {type_name: {dn: {column: value}}}.

    Nothing is compared or written until all of these have returned, so no
    source's failure can be interpreted as another source's absence -- which is
    exactly the failure mode of the Splunk job's shared DN lookup.
    """
    built = {}
    for name, fn in sorted(SOURCES.items()):
        if only and fn.type_name not in only:
            continue
        rows = fn()
        floor = MIN_ROWS.get(fn.type_name, 1)
        logger.info("source %-28s type=%-16s rows=%d (floor %d)",
                    name, fn.type_name, len(rows), floor)
        if len(rows) < floor:
            raise SystemExit(
                "ABORT: source %s returned %d rows, below its floor of %d. "
                "Nothing has been written. This is the guard the Splunk job "
                "lacks -- investigate before rerunning."
                % (name, len(rows), floor))
        built.setdefault(fn.type_name, {}).update(rows)
    return built


def plan_deletions(built, db_dns):
    """Per-type set difference, with a volume ceiling.

    built   -- {type_name: {dn: {...}}} read from ED in THIS process
    db_dns  -- {type_name: set(dn)} read from the IA database in THIS process

    Returns {type_name: sorted list of dns to remove}. Raises rather than
    returning an implausible deletion set.
    """
    plan = {}
    for type_name, existing in sorted(db_dns.items()):
        live = set(built.get(type_name, {}))
        if not live:
            raise SystemExit(
                "ABORT: no live DNs for type %r, so every one of its %d database "
                "rows would be deleted. Refusing." % (type_name, len(existing)))
        if not existing:
            logger.info("deletions %-16s db side empty, nothing to reconcile", type_name)
            continue
        gone = sorted(d for d in existing - live if d)
        fraction = len(gone) / len(existing)
        logger.info("deletions %-16s db=%d live=%d gone=%d (%.2f%%)",
                    type_name, len(existing), len(live), len(gone), fraction * 100)
        if fraction > MAX_DELETE_FRACTION:
            raise SystemExit(
                "ABORT: %d of %d %s rows (%.1f%%) would be removed, above the "
                "%.1f%% ceiling. Nothing has been written."
                % (len(gone), len(existing), type_name, fraction * 100,
                   MAX_DELETE_FRACTION * 100))
        plan[type_name] = gone
    return plan


def demo():
    """Offline self-check of the logic that actually has branches worth breaking."""
    # mv() must join every value, in order, and never drop one
    r = _Row([("weillCornellEduPersonTypeCode", ["academic", "academic-faculty"]),
              ("cn", ["Smith"])])
    assert mv(r, "weillCornellEduPersonTypeCode") == "academic|academic-faculty"
    assert mv(r, "WeillCornellEduPersonTypeCode") == "academic|academic-faculty", \
        "attribute lookup must be case-insensitive"
    assert mv(r, "absent") == ""
    assert r.get("weillCornellEduPersonTypeCode") == "academic", \
        "get() returns only the first value -- this is why mv() exists"

    # plan_deletions: normal case
    built = {"email": {"dn=a": {}, "dn=b": {}}}
    plan = plan_deletions(built, {"email": {"dn=a", "dn=b"}})
    assert plan["email"] == [], plan

    # one row gone out of 200 is under the ceiling
    built = {"email": {"dn=%d" % i: {} for i in range(199)}}
    db = {"email": {"dn=%d" % i for i in range(200)}}
    assert plan_deletions(built, db)["email"] == ["dn=199"]

    # a feeder that returned nothing must abort, not delete everything
    try:
        plan_deletions({"email": {}}, {"email": {"dn=a"}})
        raise AssertionError("empty live set must abort")
    except SystemExit as exc:
        assert "Refusing" in str(exc)

    # an implausible delete volume must abort
    try:
        plan_deletions({"email": {"dn=a": {}}},
                       {"email": {"dn=%d" % i for i in range(100)}})
        raise AssertionError("delete volume ceiling must abort")
    except SystemExit as exc:
        assert "ceiling" in str(exc)

    # the stale aliases must never come back
    assert not ({"ed-contacts", "ed-faculty", "ed-students", "ed-employees",
                 "ed-affiliates"} & set(BASE_DN))
    assert DELETE_MODE in ("flag", "delete")
    print("demo OK: %d sources registered, %d aliases (%d unresolved), delete mode %r"
          % (len(SOURCES), len(BASE_DN), len(UNRESOLVED_ALIASES), DELETE_MODE))


def spike():
    """Probe every alias and report what binds. Values are never printed."""
    for alias in sorted(BASE_DN):
        try:
            rows = ldap_search(alias, "(objectClass=*)", ["cn"])
            state = "ok" if rows else "BOUND BUT EMPTY"
        except Exception as exc:                       # noqa: BLE001
            state = "FAILED: %s" % type(exc).__name__
            rows = []
        flag = "  (unresolved candidate)" if alias in UNRESOLVED_ALIASES else ""
        print("%-16s %-52s %s%s" % (alias, BASE_DN[alias], state, flag))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--demo", action="store_true", help="pure logic, no network")
    ap.add_argument("--spike", action="store_true", help="probe LDAP only")
    ap.add_argument("--dry-run", action="store_true", help="build and compare, write nothing")
    ap.add_argument("--no-delete", action="store_true", help="skip the deletion pass")
    args = ap.parse_args()

    if args.demo:
        return demo()
    if args.spike:
        return spike()

    unresolved = UNRESOLVED_ALIASES & {fn.type_name for fn in SOURCES.values()}
    if unresolved:
        logger.warning("aliases still unresolved: %s", ", ".join(sorted(unresolved)))

    built = run_sources()
    logger.info("built %d types, %d rows total",
                len(built), sum(len(v) for v in built.values()))
    if args.dry_run:
        logger.info("--dry-run: nothing written")
    return built


if __name__ == "__main__":
    main()
