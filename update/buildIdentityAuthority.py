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
    from buildIdentity import _Row, _flatten, ldap_conn, LDAP_PAGE_SIZE
except ImportError:                                    # checkout layout
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from buildIdentity import _Row, _flatten, ldap_conn, LDAP_PAGE_SIZE

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
# MEASURED AGAINST LIVE ED 2026-09-06 by spike_ia.py. Every base below either
# resolved or is recorded as nonexistent; none is a guess any more.
BASE_DN = {
    "ed-people": os.environ.get("LDAP_BASE_PEOPLE", "ou=people," + ROOT),
    "ed-sors": os.environ.get("LDAP_BASE_SORS", "ou=sors," + ROOT),
    "ed-groups": os.environ.get("LDAP_BASE_GROUPS", "ou=departments,ou=Groups," + ROOT),
    # The Location Master feed's bare `ed` alias. Confirmed: 200+ entries.
    "ed": os.environ.get("LDAP_BASE_ROOT", "ou=locations,ou=Groups," + ROOT),
    # The authoritative org unit hierarchy. Confirmed: 2,457 entries.
    "ed-orgunits": os.environ.get("LDAP_BASE_ORGUNITS", "ou=orgunits,ou=Groups," + ROOT),
}

# ou=contacts DOES NOT EXIST IN ED. Measured 2026-09-06 with a BASE-scope search:
# ou=contacts, ou=emails/ou=contacts, ou=telephoneNumbers/ou=contacts and
# ou=locations/ou=contacts all return success with NO entry, and the directory
# root has exactly two children -- ou=Groups and ou=People. (A subtree search
# against a missing base returns success-and-empty here rather than
# noSuchObject, which is why an earlier probe misreported it as "bound but
# empty"; only BASE scope settles it.)
#
# So every dn the SPL writes for those three feeds --
# "uid=" . uid . ",ou=emails,ou=contacts,..." and its siblings -- names a branch
# that does not exist. Those DNs are string concatenations that were never
# validated, so _contact_email / _contact_phone / _contact_location are keyed on
# FICTIONAL DNs. assert_dn_overlap() will read ~0% for those three tables, and
# it will be right to abort.
#
# What this does NOT establish: where those searches actually read from. The
# `ed-emails` / `ed-phones` / `ed-locations` aliases are SA-ldapsearch domain
# configs living in Splunk, which we have never seen -- the dn reconstruction is
# just a string the author wrote and is not evidence of the search base. Person
# entries under ou=people DO carry mail and telephoneNumber directly (confirmed),
# so ou=people is the likely real source, but that must be read out of Splunk's
# SA-ldapsearch config before these three sources are written.
CONTACTS_BRANCH_ABSENT = ("ed-emails", "ed-phones", "ed-locations")

# Empty: every alias in BASE_DN resolved against live ED on 2026-09-06.
UNRESOLVED_ALIASES = set()

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
    # New feed, no Splunk ancestor and no existing table -- see the org unit
    # notes below. Name is provisional until the DDL is agreed.
    "org_unit": "_org_unit",
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
    # Measured 2026-09-06: 2,457 entries under ou=orgunits,ou=Groups.
    "org_unit": 2000,
}

# Refuse to delete more than this fraction of a type's existing DB rows in one
# run. The Splunk job has no such floor, which is why a single failed feeder
# can nominate 100% of a type for deletion.
MAX_DELETE_FRACTION = 0.02

# DO NOT USE source_organization, AND DO NOT USE ZOUKEY.
# Paul, 2026-09-06: source_organization is a one-time artifact several years old
# and is not to be trusted. The 2022 Duplicate CWID Detector walks it with 36
# hand-unrolled self-joins across DEPTH 4..10 to flatten the hierarchy into
# deptCSIDLevel1/Level2 on analysis_person; that whole approach is dead, and any
# recommendation built on ZOUKEY/ZOUKEYP (including one I made earlier today) is
# withdrawn.
#
# The authoritative org unit hierarchy lives in ED at
#   ou=orgUnits,ou=Groups,dc=weill,dc=cornell,dc=edu
# and should become part of this feed. That is a NEW source: the Splunk export
# contains no reference to it (its Organization search reads the older
# ou=departments,ou=Groups branch instead), which is consistent with ED's
# department -> orgUnit migration and makes this driver #2 of the port.
#
# Open, and what --spike must answer before this source is written: how ED
# expresses parenthood in that branch (a parent DN, seeAlso, or the entry DN's
# own position), what the CSID attribute is called, and how deep it goes. CSIDs
# have N levels -- do not model them as Level1/Level2 columns, which is exactly
# the mistake the flattened analysis_person columns encode.

# THE IDENTITY AUTHORITY DATABASE IS MARIADB, NOT SQL SERVER.
# Established 2026-09-06 from a live error ("check the manual that corresponds to
# your MariaDB server version", 1064). Both draft writers were written against
# SQL Server and are non-functional here: MariaDB has no MERGE statement at all,
# no NVARCHAR(MAX), no WITH (HOLDLOCK), no sys.indexes and no @@SERVERNAME, and
# pymssql is the wrong driver.
#
# The correct pattern already exists and is in production next door:
# buildIdentity.py writes reciterdb with pymysql and
#   INSERT ... ON DUPLICATE KEY UPDATE `t`.`c` = COALESCE(VALUES(`c`), `t`.`c`)
# which is exactly what this job needs -- COALESCE so a NULL from one run never
# erases history, and backtick quoting rather than brackets. Reuse it; do not
# write a second upsert.
#
# Two consequences for the review findings already recorded:
#   * "do not CAST the key, it costs the index seek" was SQL Server advice and is
#     moot here.
#   * the Python-vs-SQL string-equality mismatch STILL applies: MariaDB's usual
#     utf8mb4_general_ci is case-insensitive while Python set difference is not,
#     so the DN comparison must still be normalised on one canonical key.
# ponytail: the fix is to delete a writer, not to write one.

# "flag" writes a soft-delete marker; "delete" issues DELETEs. Ships as "flag"
# because the SPL does not reveal whether the IdentityAuthority_dn stanza
# deletes rows or sets a column, and guessing wrong is destructive.
DELETE_MODE = os.environ.get("IA_DELETE_MODE", "flag")

SOURCES = {}


def source(type_name, alias=None):
    """Register a source. The function returns {dn: {column: value}}.

    type_name ties the source to its table, its row floor and its side of the
    deletion set difference. alias is the LDAP domain alias it reads, and exists
    so the unresolved-base-DN guard can actually fire -- an earlier version
    intersected UNRESOLVED_ALIASES (alias names) with type names, which are
    disjoint vocabularies, so the warning was dead code.
    """
    def wrap(fn):
        fn.type_name = type_name
        fn.alias = alias
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


def assert_dn_overlap(built, db_dns, min_overlap=0.9):
    """Abort if the DNs we just built do not look like the DNs already stored.

    This job keys on the REAL entry DN from ldap3, while every row already in
    the database was keyed by a Splunk string reconstruction
    (`eval dn = "uid=" . uid . ",ou=emails,ou=contacts,..."`). Where the two
    agree, an upsert updates the existing row. Where they disagree, the upsert
    silently INSERTS a duplicate and the set difference reports the original as
    deleted -- so a format mismatch is simultaneously a data-duplication bug and
    a mass-deletion trigger.

    The contact feeds should agree, because the SPL reconstructed exactly the
    branch the entries live in. The Organization and SOR-affiliate paths are the
    risk: both have a `case()` branch that builds the DN from `seeAlso` instead,
    and if seeAlso is not the parent container DN those rows have been wrong for
    years. That is an open question in PORT_SPEC.md, so it is checked at runtime
    rather than assumed either way.

    Runs before any write, and independently of whether deletion is enabled.
    """
    for type_name, existing in sorted(db_dns.items()):
        if not existing:
            continue
        live = set(built.get(type_name, {}))
        overlap = len(live & existing) / len(existing)
        logger.info("dn overlap %-16s db=%d live=%d matched=%d (%.1f%%)",
                    type_name, len(existing), len(live),
                    len(live & existing), overlap * 100)
        if overlap < min_overlap:
            sample = sorted(existing - live)[:1]
            raise SystemExit(
                "ABORT: only %.1f%% of stored %s DNs match the DNs built from "
                "ED, below the %.0f%% floor. The stored DNs are Splunk string "
                "reconstructions and this job uses the real entry DN; if the "
                "two formats disagree the upsert would insert duplicates and "
                "the reconcile would report every stored row as deleted. "
                "Nothing has been written. Example stored DN with no live "
                "match: %s"
                % (overlap * 100, type_name, min_overlap * 100,
                   sample[0] if sample else "(none)"))


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

    # assert_dn_overlap must catch a re-key rather than let it reach the upsert.
    # A total format mismatch is the realistic shape: every stored DN is a Splunk
    # reconstruction, so if the real entry DN differs it differs for all of them.
    try:
        assert_dn_overlap({"email": {"uid=a,ou=emails,ou=contacts,x": {}}},
                          {"email": {"uid=a,ou=emails,x"}})
        raise AssertionError("a re-keyed table must abort")
    except SystemExit as exc:
        assert "insert duplicates" in str(exc), exc
    # matching DNs must pass
    assert_dn_overlap({"email": {"dn=%d" % i: {} for i in range(100)}},
                      {"email": {"dn=%d" % i for i in range(100)}})

    # the unresolved-base guard must be able to fire: alias names and type names
    # are disjoint vocabularies, and an earlier version intersected the wrong one
    # The guard reads alias names, not type names. Stated as a subset rather than
    # a non-empty intersection so it stays true once every alias is resolved --
    # which is now the case, and an assertion that only held while work was
    # outstanding would have to be deleted exactly when it started mattering.
    assert UNRESOLVED_ALIASES <= set(BASE_DN), \
        "UNRESOLVED_ALIASES must contain alias names, not %s" % (
            UNRESOLVED_ALIASES - set(BASE_DN))
    assert not (UNRESOLVED_ALIASES & set(TABLES)), \
        "alias names must never be type names, or the guard is dead again"
    # The contacts branch does not exist in ED, so no source may claim to read it.
    assert not (set(CONTACTS_BRANCH_ABSENT) & set(BASE_DN)), \
        "a base DN was added for a branch measured absent from ED"

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

    unresolved = UNRESOLVED_ALIASES & {fn.alias for fn in SOURCES.values() if fn.alias}
    if unresolved:
        logger.warning(
            "base DN not yet proved against live ED for: %s -- run --spike",
            ", ".join(sorted(unresolved)))

    built = run_sources()
    logger.info("built %d types, %d rows total",
                len(built), sum(len(v) for v in built.values()))

    # The re-key check must run before any write, and independently of
    # --no-delete: a DN format mismatch is a duplicate-INSERT bug on the upsert
    # path just as much as it is a mass-delete trigger on the reconcile path.
    # It needs the DB side, so it lives here rather than inside run_sources().
    #
    # Until db_dns() lands this is a no-op against an empty mapping, and it says
    # so out loud -- an earlier version of this module defined the function,
    # exercised it only in demo(), and let source docstrings claim it was
    # protecting them. It was not.
    existing = db_dns(TABLES) if "db_dns" in globals() else {}
    if not existing:
        logger.warning("dn overlap NOT checked: no database read available yet, "
                       "so nothing has verified that the DNs built from ED match "
                       "the DNs already stored")
    else:
        assert_dn_overlap(built, existing)

    if args.dry_run:
        logger.info("--dry-run: nothing written")
    return built


if __name__ == "__main__":
    main()
