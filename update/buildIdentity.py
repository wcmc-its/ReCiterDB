#!/usr/bin/env python3
"""
Build reciterdb.identity from Enterprise Directory (LDAP) and ASMS (MSSQL).

Replaces the Splunk saved search "reciter identity update", which assembled the
same 37 columns from 1 dbxquery + 16 ldapsearch subsearches stitched together
with `append` and a terminal `stats ... by weillCornellEduCWID`, wrote them to a
CSV lookup (`reciterIdentity`), and upserted that lookup into reciterdb.identity
via DB Connect (`| inputlookup reciterIdentity | dbxoutput output=ReCiter-Identity`).

Why it was replaced: Splunk `append` subsearches are silently truncated at
maxresultrows/maxtime, and `list()` silently caps at 100 values per group. Both
drop rows with no error, which is what made the job unreliable. Here every source
logs its own row count and an empty source aborts the run before any write.

Semantics deliberately preserved from the SPL -- do not "fix" these without a
diff run to back it up:

  * The table is CUMULATIVE. Rows are upserted on cwid and NEVER deleted, so
    department/division survive after someone drops out of ED's ou=canonical.
    That is a business requirement. Do NOT convert this to the shadow-build /
    atomic-swap pattern used by the person_* tables (setup/person_table_swap.sql)
    -- that pattern would delete every person who falls out of the population.

  * `notes` and `alumniResidentNYP` are written by something outside this job
    (1,979 and 778 rows respectively as of 2026-09-05). They are absent from
    UPSERT_COLUMNS so the upsert can never clobber them.

  * surname / givenName use max() across sources -- an arbitrary lexicographic
    tie-break, not a rule. Kept verbatim so the first diff against Splunk is
    empty. Fix it in a follow-up once the diff is clean.

  * The 43 excluded cwids are inlined below exactly as the SPL had them. They
    belong in a table; moving them is a follow-up, for the same reason.

Where the SPL was genuinely ambiguous -- Splunk's multivalue-to-string coercion
in `list()` followed by `replace(x," ","")` -- this reads the intent as "yes if
any source said yes, else empty". That is what the final `where` clause needs.
The diff harness is what proves it; see docs/IDENTITY_PORT.md.
"""

import collections
import datetime
import logging
import os
import re
import sys

from ldap3 import ALL, Connection, Server, SUBTREE
from ldap3.extend.standard.PagedSearch import paged_search_generator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
#                                  CONFIG
# ---------------------------------------------------------------------------

# ldap.hostname / ldap.port from the institutional client's application.properties.
LDAP_URL = os.environ.get("LDAP_URL", "ldaps://ed.weill.cornell.edu:636")
LDAP_BIND_DN = os.environ.get(
    "LDAP_BIND_DN", "cn=reciter,ou=binds,dc=weill,dc=cornell,dc=edu")
LDAP_PAGE_SIZE = 500

# The five SA-ldapsearch `domain=` aliases used by the SPL, mapped to real base
# DNs. Four are confirmed against the institutional client, which reads the same
# directory (application.properties ldap.base.dn, and the ldapSources block in
# its k8-scheduling-default.yaml, itself a verbatim move from
# LdapIdentityDaoImpl.getActivePeopleFromED):
#
#   ed-people    ou=people,dc=weill,dc=cornell,dc=edu        (ldap.base.dn)
#   ed-faculty   ou=faculty,ou=sors,...                      (SOURCE_INACTIVE_ACADEMIC)
#   ed-students  ou=students,ou=sors,...                     (SOURCE_STUDENT_MD_OR_PHD)
#   ed-sors      ou=sors,...                                 (parent of the three
#                above; the SPL filters on (ou=faculty)/(ou=students)/
#                (ou=nyp affiliates), which only resolves from the parent)
#
# ed-organizations is NOT confirmed -- inferred from the sibling taxonomy branch
# ou=locations,ou=Groups,... documented in the Everbridge location analysis. It
# drives primaryOrg only. `--spike` proves or disproves it.
# ponytail: env-overridable dict, not a config class. Five constants.
BASE_DN = {
    "ed-people": os.environ.get(
        "LDAP_BASE_PEOPLE", "ou=people,dc=weill,dc=cornell,dc=edu"),
    "ed-organizations": os.environ.get(
        "LDAP_BASE_ORGS", "ou=organizations,ou=Groups,dc=weill,dc=cornell,dc=edu"),
    "ed-faculty": os.environ.get(
        "LDAP_BASE_FACULTY", "ou=faculty,ou=sors,dc=weill,dc=cornell,dc=edu"),
    "ed-sors": os.environ.get(
        "LDAP_BASE_SORS", "ou=sors,dc=weill,dc=cornell,dc=edu"),
    "ed-students": os.environ.get(
        "LDAP_BASE_STUDENTS", "ou=students,ou=sors,dc=weill,dc=cornell,dc=edu"),
}

# 43 cwids excluded by the SPL's `where cwid != "..."` chain, verbatim.
EXCLUDED_CWIDS = {
    "act4001", "adz4001", "alm4016", "alp4016", "anp3012", "ant4017", "ars4011",
    "brw4008", "chi4001", "daa4014", "das9200", "dds4001", "doz4001", "eem4001",
    "ehj4001", "fke4001", "hae4001", "hbs4001", "hrf4001", "hrw4001", "jes4023",
    "jfb4001", "kdf4001", "kiw4002", "kjz4001", "kms4002", "kvc4002", "luz4002",
    "mal4027", "mmc4001", "mrd4002", "mur4003", "nmk4001", "oaf4001", "olt4001",
    "pan4004", "pas4007", "rgs4002", "ses4004", "shd4007", "smg4003", "ssh4002",
    "vsa4001",
}

# The 37 columns this job owns. `notes` and `alumniResidentNYP` are excluded on
# purpose -- another writer owns them (see module docstring).
UPSERT_COLUMNS = [
    "cwid", "surname", "middleName", "givenName", "primaryTitle",
    "primaryAcademicDepartment", "primaryAcademicDivision", "primaryProgram",
    "fullTimeFaculty", "studentMDNYC", "studentMDQatar", "studentMDPhD",
    "studentPhDTriI", "studentPhDWeill", "partTimeFaculty", "voluntaryFaculty",
    "emeritusFaculty", "adjunctFaculty", "fellow", "postdoc", "faculty",
    "nonFaculty", "residentNYP", "inactiveFaculty", "alumniMD", "alumniMDPHD",
    "alumniPHD", "startDateWCMFaculty", "endDateWCMFaculty",
    "startDateWCMStudent", "endDateWCMStudent", "popsProfile",
    "directoryProfile", "vivoProfile", "facultyRank", "primaryOrg",
    "inactiveNonAlumniStudent",
]

# Columns the SPL emits as "yes"/"" flags. Merge rule: yes if ANY source says so.
FLAG_COLUMNS = {
    "fullTimeFaculty", "studentMDNYC", "studentMDQatar", "studentMDPhD",
    "studentPhDTriI", "studentPhDWeill", "partTimeFaculty", "voluntaryFaculty",
    "emeritusFaculty", "adjunctFaculty", "fellow", "postdoc", "faculty",
    "nonFaculty", "residentNYP", "inactiveFaculty", "alumniMD", "alumniMDPHD",
    "alumniPHD", "inactiveNonAlumniStudent",
}

# Columns whose SPL aggregation was max(), not list().
MAX_COLUMNS = {"surname", "givenName", "endDateWCMFaculty"}

# `varchar(128)` in the live DDL but genuinely longer in ED. DB Connect wrote in
# non-strict mode and truncated silently; we truncate explicitly so the behaviour
# is visible and countable rather than a surprise on a strict connection.
# ponytail: widen the columns and delete this once the diff is clean.
TRUNCATE_AT_128 = {
    "primaryTitle", "popsProfile", "directoryProfile", "vivoProfile",
    "primaryAcademicDepartment", "primaryAcademicDivision", "primaryProgram",
    "surname", "middleName", "givenName", "facultyRank", "primaryOrg",
}

YEAR_COLUMNS = {
    "startDateWCMFaculty", "endDateWCMFaculty",
    "startDateWCMStudent", "endDateWCMStudent",
}

# Refuse to write if a build comes back more than 5% smaller than recent ones.
# The baseline is PRIOR BUILDS, not the row count of `identity`: that table is
# cumulative and its residue only grows, so any fixed fraction of it drifts out
# of reach and would eventually refuse every run. Measured 2026-09-05, a healthy
# build stages 33,388 rows against 35,448 live -- 93%, already under a naive
# floor. The first run has no history and is allowed through with a warning.
MIN_ROWS_FLOOR = 0.95
BUILD_LOG_WINDOW_DAYS = 30

SOURCES = {}


def source(fn):
    """Register a source function. Each returns {cwid: {column: value}}."""
    SOURCES[fn.__name__] = fn
    return fn


# ---------------------------------------------------------------------------
#                                  LDAP
# ---------------------------------------------------------------------------

_conn = None


def ldap_conn():
    global _conn
    if _conn is None:
        _conn = Connection(
            Server(LDAP_URL, get_info=ALL),
            user=LDAP_BIND_DN,
            password=os.environ["LDAP_BIND_PASSWORD"],
            auto_bind=True,
            raise_exceptions=True,
        )
        logger.info("LDAP bound to %s", LDAP_URL)
    return _conn


def ldap_search(domain, search_filter, attrs, limit=None):
    """Paged search against one SA-ldapsearch domain alias.

    Paging is not optional: ED holds 30k+ people and the server-side size limit
    silently caps an unpaged search. That is the same class of bug as Splunk's
    subsearch truncation, so it is handled here rather than trusted.
    """
    base = BASE_DN[domain]
    # SPL search filters are written across several lines for readability;
    # LDAP does not allow whitespace between filter components.
    flt = "".join(line.strip() for line in search_filter.splitlines())
    rows = []
    for entry in paged_search_generator(
        ldap_conn(), base, flt,
        search_scope=SUBTREE, attributes=attrs, paged_size=LDAP_PAGE_SIZE,
    ):
        if entry.get("type") != "searchResEntry":
            continue
        # Raw values are kept: _Row.get() flattens on access and _Row.all()
        # exposes every value of a multi-valued attribute.
        rows.append(_Row(entry["attributes"].items()))
        if limit and len(rows) >= limit:
            break          # probe path only; a real source never passes limit
    logger.info("ldap %s: %d entries", domain, len(rows))
    return rows


class _Row(dict):
    """LDAP attribute descriptions are case-insensitive (RFC 4512), options
    included, and ED does not return them in the casing the SPL wrote. The live
    directory returns `labeledURI;onlinedirectory`; the SPL asked for
    `labeledURI;onlineDirectory`. A plain dict lookup misses on that and silently
    nulls the column -- exactly the failure mode this port exists to remove. Keys
    are stored lowercased and looked up lowercased, so call sites keep the
    readable spelling and casing can never drop a value.
    """

    def __init__(self, items):
        super().__init__((k.lower(), v) for k, v in items)

    def get(self, key, default=""):
        return _flatten(super().get(key.lower(), "")) or default

    def all(self, key):
        """Every value of a multi-valued attribute.

        weillCornellEduPersonTypeCode carries up to 17 values per person --
        ['academic', 'academic-faculty', 'academic-faculty-assistant',
         'academic-faculty-voluntary', 'affiliate', 'affiliate-nyp', ...].
        Splunk's `field = "x"` matches if ANY value equals x, while get()
        returns only the first, which is always the least specific one
        ('academic'). Reading person types through get() silently emptied every
        flag, facultyRank, and the NYP org default.
        """
        value = super().get(key.lower(), "")
        if isinstance(value, list):
            return [_flatten(v) for v in value if v not in (None, "")]
        return [_flatten(value)] if value not in (None, "") else []

    def __getitem__(self, key):
        return self.get(key)


def _flatten(value):
    """ldap3 returns lists for every attribute. Take the first non-empty.

    Values are normalised to str here. ldap3 parses GeneralizedTime attributes
    into datetime objects when they are populated, while an absent one comes
    back as "" -- sorting or comparing that mix raises
    `TypeError: '<' not supported between instances of 'str' and
    'datetime.datetime'`. Normalising at the boundary means no comparison
    downstream can hit it. ISO format also slices correctly for the [:4] year
    extractions.
    """
    if isinstance(value, list):
        value = next((v for v in value if v not in (None, "")), "")
    if value is None or value == "":
        return ""
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    return value if isinstance(value, str) else str(value)


def _cwid(row):
    return (row.get("weillCornellEduCWID") or "").strip()


def _by_cwid(rows, mapper):
    """Collapse LDAP rows to {cwid: {col: val}}, first entry per cwid wins."""
    out = {}
    for row in rows:
        cwid = _cwid(row)
        if not cwid or cwid in out:
            continue
        vals = mapper(row)
        if vals:
            out[cwid] = vals
    return out


# ---------------------------------------------------------------------------
#                              SOURCE: ASMS (MSSQL)
# ---------------------------------------------------------------------------

# Verbatim from the SPL's `dbxquery connection=ASMS`. The SELECT keeps
# appt_end_date and done_date only to drive the ORDER BY -- the SPL dropped both
# before output, and so do we. The ORDER BY + dedup is load-bearing: it picks
# the division from the most recent primary appointment.
ASMS_QUERY = """
select distinct cwid,
       d2.title as primaryAcademicDivision,
       CONVERT(VARCHAR(10), a.appt_end_date, 120) AS weillCornellEduEndDate
from wcmc_person p
  join fc_appointment a on a.person_id = p.id
  join wcmc_department d ON d.id = a.department_id
  join fc_appointment_priority r on r.id = a.priority_id
  join fc_appointment_action c on c.id = a.action_id
  JOIN wcmc_division d2 ON d2.id = a.division_id
  join wcmc_institution i on i.id = a.institution_id
where d2.title not in ('NO DIVISION','EDUCATION','Administration','Other',
                       'General','Research','Chairman')
  and (i.id = 3592711 or i.id = 10000141)
  and r.title = 'Primary'
  and cwid is not null
  and cwid != ''
  and a.id NOT IN (
      SELECT a.id FROM wcmc_person
        join fc_appointment a ON p.id = a.person_id
        join fc_appointment_action c ON c.id = a.action_id
      WHERE a.done_date is null and c.title <> 'Reappt')
order by weillCornellEduEndDate desc
"""


def _mssql_target(url):
    """MSSQL_DB_URL is shared with the institutional client, which is Java and
    puts a JDBC URL there (jdbc:sqlserver://host:1433;databaseName=...). pymssql
    wants host and port separately, so accept either form and split them out
    rather than relying on pymssql to parse a colon.
    """
    host = url.strip()
    if "://" in host:
        host = host.split("://", 1)[1]
    host = host.split(";", 1)[0].split("/", 1)[0]
    if ":" in host:
        host, _, port = host.rpartition(":")
        return host, int(port)
    return host, 1433


@source
def asms_division():
    import pymssql  # lazy: --spike and --demo must run without the MSSQL driver

    host, port = _mssql_target(os.environ["MSSQL_DB_URL"])
    conn = pymssql.connect(
        server=host, port=port,
        user=os.environ["MSSQL_DB_USERNAME"],
        password=os.environ["MSSQL_DB_PASSWORD"],
        database=os.environ.get("MSSQL_DB_NAME", "ASMS"),
    )
    try:
        cur = conn.cursor(as_dict=True)
        cur.execute(ASMS_QUERY)
        rows = cur.fetchall()
    finally:
        conn.close()

    out = {}
    for row in rows:  # already ordered by end date desc; first wins (SPL dedup)
        cwid = (row.get("cwid") or "").strip()
        if cwid and cwid not in out:
            out[cwid] = {"primaryAcademicDivision": row["primaryAcademicDivision"]}
    return out


# ---------------------------------------------------------------------------
#                            SOURCES: ED (LDAP)
# ---------------------------------------------------------------------------

PERSON_TYPE_FLAGS = {
    "academic-faculty-weillfulltime": "fullTimeFaculty",
    "student-md-new-york": "studentMDNYC",
    "student-md-qatar": "studentMDQatar",
    "student-md-phd-tri-i": "studentMDPhD",
    "student-phd-tri-i": "studentPhDTriI",
    "student-phd-weill": "studentPhDWeill",
    "academic-faculty-weillparttime": "partTimeFaculty",
    "academic-faculty-voluntary": "voluntaryFaculty",
    "academic-faculty-emeritus": "emeritusFaculty",
    "academic-faculty-adjunct": "adjunctFaculty",
    "academic-nonfaculty-postdoc-fellow": "fellow",
    "academic-nonfaculty-postdoc": "postdoc",
    "academic-faculty": "faculty",
    "academic-nonfaculty": "nonFaculty",
    "affiliate-nyp-resident": "residentNYP",
}

FACULTY_RANK = {
    "academic-faculty-fullprofessor": "Full Professor",
    "academic-faculty-associate": "Associate Professor",
    "academic-faculty-assistant": "Assistant Professor",
    "academic-faculty-instructor": "Instructor or Lecturer",
    "academic-faculty-lecturer": "Instructor or Lecturer",
}


@source
def ed_people_main():
    """The main population: titles, profile URLs, person-type flags, org."""
    orgs = _org_lookup()
    rows = ldap_search(
        "ed-people",
        """(&(objectClass=weillCornellEduPerson)
            (|(weillCornellEduPersonTypeCode=affiliate-nyp-resident)
              (weillCornellEduPersonTypeCode=academic)
              (weillCornellEduPersonTypeCode=student-md*)
              (weillCornellEduPersonTypeCode=student-phd-*)))""",
        # The SPL used attrs="*". Naming them cuts the payload enormously and
        # is the single biggest speed win in this port.
        ["weillCornellEduCWID", "weillCornellEduPersonTypeCode",
         "weillCornellEduPrimaryTitle", "labeledURI;onlineDirectory",
         "labeledURI;pops", "labeledURI;vivo",
         "weillCornellEduPrimaryOrganization;faculty",
         "weillCornellEduPrimaryOrganization;student"],
    )

    def mapper(row):
        # EVERY person type, not just the first -- see _Row.all().
        ptypes = set(row.all("weillCornellEduPersonTypeCode"))
        org = (row.get("weillCornellEduPrimaryOrganization;faculty")
               or row.get("weillCornellEduPrimaryOrganization;student")
               or ("NYP" if "affiliate-nyp-resident" in ptypes else ""))
        vals = {
            "primaryTitle": row.get("weillCornellEduPrimaryTitle", ""),
            "popsProfile": row.get("labeledURI;pops", ""),
            "directoryProfile": row.get("labeledURI;onlineDirectory", ""),
            "vivoProfile": row.get("labeledURI;vivo", ""),
            # FACULTY_RANK is ordered most senior first, matching the SPL's
            # case(), which returns its first matching branch.
            "facultyRank": next(
                (v for k, v in FACULTY_RANK.items() if k in ptypes), ""),
            "primaryOrg": orgs.get(org, ""),
        }
        for ptype in ptypes:
            flag = PERSON_TYPE_FLAGS.get(ptype)
            if flag:
                vals[flag] = "yes"
        return vals

    return _by_cwid(rows, mapper)


def _org_lookup():
    """ed-organizations `o` -> `cn`, the SPL's `join o type=left`."""
    rows = ldap_search("ed-organizations", "(o=*)", ["o", "cn"])
    return {r.get("o"): r.get("cn") for r in rows if r.get("o")}


@source
def ed_faculty_expired():
    rows = ldap_search(
        "ed-faculty",
        "(&(objectClass=weillCornellEduSORRecord)(weillCornellEduStatus=faculty:expired))",
        ["weillCornellEduCWID"])
    return _by_cwid(rows, lambda r: {"inactiveFaculty": "yes"})


@source
def ed_sors_names():
    rows = ldap_search(
        "ed-sors",
        "(&(objectClass=weillCornellEduSORRecord)(|(ou=faculty)(ou=students)))",
        ["weillCornellEduCWID", "sn", "givenName", "weillCornellEduMiddleName"])
    return _by_cwid(rows, _name_mapper)


@source
def ed_sors_nyp_names():
    rows = ldap_search(
        "ed-sors",
        """(&(objectClass=weillCornellEduSORRecord)(ou=nyp affiliates)
            (weillCornellEduPersonTypeCode=affiliate-nyp-resident))""",
        ["weillCornellEduCWID", "sn", "givenName", "weillCornellEduMiddleName"])
    return _by_cwid(rows, _name_mapper)


def _name_mapper(row):
    return {
        "surname": row.get("sn", ""),
        "givenName": row.get("givenName", ""),
        "middleName": row.get("weillCornellEduMiddleName", ""),
    }


@source
def ed_faculty_inactive_department():
    """Latest expired-faculty department -- and, under PREFER_ORGUNIT, division.

    Feeds the primaryAcademicDepartment fallback chain. The SPL sorted by end
    date desc after dedup; so does this, so the most recent role record wins.

    The two orgUnit levels are SPLIT here rather than flattened into the deepest
    value, unlike the primary-department path. Role records carry the hierarchy
    that SOR records mostly lack -- L2 on ~37% of expired role records against
    3.4% of faculty SOR records -- so the levels mean something here:

        L1 -> inactiveDepartment   "Medicine"
        L2 -> primaryAcademicDivision  "Infectious Diseases"

    which is the same shape an active colleague gets from ASMS. Flattening to
    the deepest value would put a division in the department column and leave
    these people the only cohort whose department means something different.

    The division half is a genuine backfill: ASMS can never supply one for
    expired faculty because its query requires a live appointment, which is why
    72% of the table has no division. ASMS still wins where it has a value --
    it is registered first in SOURCES and merge() keeps the first non-empty.
    """
    rows = ldap_search(
        "ed-faculty",
        "(&(objectClass=weillCornellEduSORRoleRecord)(weillCornellEduStatus=faculty:expired))",
        ["weillCornellEduCWID", "weillCornellEduDepartment", "weillCornellEduEndDate",
         "weillCornellEduOrgUnit;level1", "weillCornellEduOrgUnit;level2"])
    rows.sort(key=lambda r: r.get("weillCornellEduEndDate", ""), reverse=True)

    def mapper(r):
        if not PREFER_ORGUNIT:
            return {"inactiveDepartment": r.get("weillCornellEduDepartment")}
        vals = {"inactiveDepartment": (r.get("weillCornellEduOrgUnit;level1")
                                       or r.get("weillCornellEduDepartment"))}
        division = r.get("weillCornellEduOrgUnit;level2")
        if division:
            vals["primaryAcademicDivision"] = division
        return vals

    return _by_cwid(rows, mapper)


@source
def ed_students_alumni_md():
    rows = ldap_search(
        "ed-students",
        """(&(objectClass=weillCornellEduSORRoleRecord)(weillCornellEduDegreeDate=*)
            (weillCornellEduDegreeCode=MD))""",
        ["weillCornellEduCWID", "sn", "givenName"])
    return _by_cwid(rows, lambda r: {
        "alumniMD": "yes", "surname": r.get("sn", ""),
        "givenName": r.get("givenName", "")})


@source
def ed_students_alumni_phd():
    rows = ldap_search(
        "ed-students",
        """(&(objectClass=weillCornellEduSORRoleRecord)(weillCornellEduDegreeDate=*)
            (weillCornellEduDegreeCode=PHD))""",
        ["weillCornellEduCWID", "sn", "givenName"])
    return _by_cwid(rows, lambda r: {
        "alumniPHD": "yes", "surname": r.get("sn", ""),
        "givenName": r.get("givenName", "")})


@source
def ed_students_alumni_mdphd():
    """MD-PhD alumni: the SPL required a name, an MD degree AND a PhD degree
    code for the same cwid (three appends, then `where` all three non-null)."""
    named = ldap_search(
        "ed-students", "(&(objectClass=weillCornellEduSORRecord))",
        ["weillCornellEduCWID", "sn", "givenName"])
    md = ldap_search(
        "ed-students",
        """(&(objectClass=weillCornellEduSORRoleRecord)
            (weillCornellEduExitReason=Graduated)(weillCornellEduDegreeCode=MD))""",
        ["weillCornellEduCWID", "weillCornellEduDegree"])
    phd = ldap_search(
        "ed-students",
        """(&(objectClass=weillCornellEduSORRoleRecord)
            (weillCornellEduExitReason=Graduated)(weillCornellEduDegreeCode=PHD))""",
        ["weillCornellEduCWID", "weillCornellEduDegreeCode"])

    names = _by_cwid(named, _name_mapper)
    md_cwids = {_cwid(r) for r in md if r.get("weillCornellEduDegree")}
    phd_cwids = {_cwid(r) for r in phd if r.get("weillCornellEduDegreeCode")}

    both = md_cwids & phd_cwids & set(names)
    return {c: dict(names[c], alumniMDPHD="yes") for c in both if c}


@source
def ed_sors_student_end_date():
    """max of degree date / end date / expected grad year, truncated to a year."""
    rows = ldap_search(
        "ed-sors",
        """(&(ou=students)(objectClass=weillCornellEduSORRoleRecord)
            (|(weillCornellEduDegreeCode=MD)(weillCornellEduDegreeCode=MDPHD)
              (weillCornellEduDegreeCode=PHD)))""",
        ["weillCornellEduCWID", "weillCornellEduDegreeDate",
         "weillCornellEduEndDate", "weillCornellEduExpectedGradYear"])
    best = {}
    for row in rows:
        cwid = _cwid(row)
        if not cwid:
            continue
        candidate = max(
            str(row.get("weillCornellEduDegreeDate", "")),
            str(row.get("weillCornellEduEndDate", "")),
            str(row.get("weillCornellEduExpectedGradYear", "")),
        )
        if candidate > best.get(cwid, ""):
            best[cwid] = candidate
    return {c: {"endDateWCMStudent": v[:4]} for c, v in best.items() if v}


# ED is migrating department/departmentCode to orgUnit/orgUnitCode (tagged
# ;level1, ;level2, ...). Measured 2026-09-05 the old attributes are still
# strictly more complete -- PrimaryDepartment 100% vs PrimaryOrgUnit;level1 93%,
# and zero records carry a new attribute without the old one -- so this job still
# reads the old model. The two are NOT equivalent: level1 is an org-chart
# reporting line, and 9% of values differ substantively (Orthopaedic Surgery ->
# Hospital for Special Surgery, Library -> Information Technologies and
# Services). Switching is a business decision about which hierarchy the
# reporting table should express, not a mechanical rename.
#
# This counter is the early warning: when ED starts retiring the old attributes,
# old coverage falls and new coverage rises, and --dry-run will say so before
# anything breaks. Division stays on ASMS -- orgUnit;level2 covers only ~7%.
# Prefer ED's new orgUnit model over the old department attributes, falling back
# to the old value when ED has no orgUnit for that person -- coalesce, never a
# hard switch, so nobody is blanked. Measured 2026-09-05 over all 8,765 faculty
# SOR records, flipping this to True changes exactly 241 rows across four
# mappings and blanks nobody:
#
#     125  Otolaryngology - Head and Neck Surgery -> Otolaryngology Head and Neck Surgery
#     113  Brain and Mind Research Institute      -> Brain and Mind Research
#       2  Orthopaedic Surgery                    -> Hospital for Special Surgery
#       1  Administration                         -> Administration & Finance
#
# 391 people have a department and no orgUnit; the fallback keeps their value,
# which matters because identity_index feeds this column into affiliation
# matching (affil_dept_match) -- a blank dept silently removes that signal.
#
# Ships False so the first diff against Splunk is empty. Flip to True once that
# diff is clean: the delta is fully predicted, so it stays verifiable.
# ponytail: a boolean, not a strategy class. Delete it once the old attributes go.
PREFER_ORGUNIT = False

ORGUNIT_MIGRATION_WATCH = [
    ("weillCornellEduPrimaryDepartment", "weillCornellEduPrimaryOrgUnit;level1"),
    ("weillCornellEduDepartment", "weillCornellEduOrgUnit;level1"),
]

_migration_counts = collections.Counter()


def _dept_value(row, primary=True):
    """Department for one record, honouring PREFER_ORGUNIT.

    `primary` picks which attribute pair applies: the person's primary
    department (ed-sors faculty SOR records) or their role department
    (ed-faculty role records, and the NYP fallback).
    """
    pre = "weillCornellEduPrimary" if primary else "weillCornellEdu"
    old = pre + "Department"
    if PREFER_ORGUNIT:
        # DEEPEST level wins. L1 is the parent org, L2 is the actual unit:
        # Library sits at L2 under an L1 of "Information Technologies and
        # Services", and taking L1 would file every librarian under ITS. Where
        # L2 exists the old department matched L1 in 0 of 299 records.
        return (row.get(f"{pre}OrgUnit;level2")
                or row.get(f"{pre}OrgUnit;level1")
                or row.get(old))
    return row.get(old)


def _program_value(row, primary=False):
    """Program for one record, honouring PREFER_ORGUNIT, normalised through
    PROGRAM_OVERRIDE.

    orgUnit;level2 carries the program name directly (1,938/1,998 doctoral role
    records; 1,309/1,309 active-student SOR records), which is the way off
    weillCornellEduProgramCode. The override table is reused rather than
    replaced -- it already collapses the MD-PhD variants, and ED's L2 spellings
    were added to it rather than a second mechanism being introduced.
    """
    pre = "weillCornellEduPrimary" if primary else "weillCornellEdu"
    # weillCornellEduProgram is MULTI-VALUED on ~335 records --
    # ['MD-PhD Program', 'Biochemistry & Structural Biology'] -- and the generic
    # entry sorts first. Splunk's case() evaluated every value and its
    # sort-by-priority picked the specific one; taking row.get() here handed the
    # generic label to 72 students. weillCornellEduPrimaryProgram is
    # single-valued, but reading both the same way costs nothing.
    olds = row.all(pre + "Program")
    news = row.all(f"{pre}OrgUnit;level2") if PREFER_ORGUNIT else []
    raws = news or olds
    if not raws:
        return ""
    raw = raws[0]
    # `program` was ALWAYS normalised -- the SPL's case() ran unconditionally,
    # and PROGRAM_PRIORITY is keyed on the normalised names, so skipping it also
    # breaks the ranking ("MD-PhD WGS Neuroscience" scores 999 instead of 6 and
    # loses to "MD-PhD Program"). Only primaryProgram was left raw by the SPL,
    # and only until PREFER_ORGUNIT makes the two columns consistent.
    if primary and not PREFER_ORGUNIT:
        return raw
    # Best-ranked of the candidates, matching the SPL's `sort 0 cwid priority`
    # followed by dedup. ed_students_program applies the same rule ACROSS
    # records; this one applies it WITHIN a record.
    return min((PROGRAM_OVERRIDE.get(r, r) for r in raws),
               key=lambda p: PROGRAM_PRIORITY.get(p, 999))


def _watch_orgunit_migration(rows):
    for old, new in ORGUNIT_MIGRATION_WATCH:
        for r in rows:
            if r.get(old):
                _migration_counts[old] += 1
            if r.get(new):
                _migration_counts[new] += 1


@source
def ed_sors_primary_department():
    """Faculty primary department, else the cleaned NYP department."""
    faculty = ldap_search(
        "ed-sors",
        """(&(objectClass=weillCornellEduSORRecord)(ou=faculty)
            (weillCornellEduPersonTypeCode=academic))""",
        ["weillCornellEduCWID", "weillCornellEduPrimaryDepartment",
         # requested only to measure the migration; not read into any column
         "weillCornellEduPrimaryOrgUnit;level1", "weillCornellEduOrgUnit;level1",
         "weillCornellEduPrimaryOrgUnit;level2", "weillCornellEduOrgUnit;level2",
         "weillCornellEduDepartment"])
    _watch_orgunit_migration(faculty)
    nyp = ldap_search(
        "ed-sors",
        """(&(objectClass=weillCornellEduSORRecord)(ou=nyp affiliates)
            (weillCornellEduPersonTypeCode=affiliate-nyp-resident))""",
        ["weillCornellEduCWID", "weillCornellEduDepartment",
         "weillCornellEduPrimaryDepartment",
         "weillCornellEduOrgUnit;level1", "weillCornellEduPrimaryOrgUnit;level1",
         "weillCornellEduOrgUnit;level2", "weillCornellEduPrimaryOrgUnit;level2"])

    out = {}
    for row in faculty:
        cwid, dept = _cwid(row), _dept_value(row)
        if cwid and dept and cwid not in out:
            out[cwid] = {"primaryAcademicDepartment": dept}
    for row in nyp:
        cwid = _cwid(row)
        if not cwid or cwid in out:
            continue
        dept = _dept_value(row) or _dept_value(row, primary=False) or ""
        for old, new in (("&&Weill Cornell GME", ""), ("Blank_dept", ""), (".GME", "")):
            dept = dept.replace(old, new)
        if dept:
            out[cwid] = {"primaryAcademicDepartment": dept}
    return out


@source
def ed_students_primary_program():
    rows = ldap_search(
        "ed-students",
        """(&(objectClass=weillCornellEduSORRecord)
            (|(weillCornellEduPersonTypeCode=student-md-*)
              (weillCornellEduPersonTypeCode=student-md-phd-tri-i)
              (weillCornellEduPersonTypeCode=student-phd-weill)
              (weillCornellEduPersonTypeCode=student-phd-tri-i))
            (weillCornellEduStatus=student:active))""",
        ["weillCornellEduCWID", "weillCornellEduPrimaryProgram",
         "weillCornellEduPrimaryOrgUnit;level2"])
    return _by_cwid(rows, lambda r: {"primaryProgram": _program_value(r, primary=True)})


@source
def ed_faculty_dates():
    """min start / max end across faculty role records.

    The SPL ran this search twice -- once for weillCornellEduStartDate and once
    for weillCornellEduEndDate. One search, two aggregations.
    """
    rows = ldap_search(
        "ed-faculty",
        "(&(ou=faculty)(objectClass=weillCornellEduSORRoleRecord))",
        ["weillCornellEduCWID", "weillCornellEduStartDate", "weillCornellEduEndDate"])
    out = {}
    for row in rows:
        cwid = _cwid(row)
        if not cwid:
            continue
        cur = out.setdefault(cwid, {})
        start = str(row.get("weillCornellEduStartDate", ""))
        end = str(row.get("weillCornellEduEndDate", ""))
        if start and (not cur.get("_start") or start < cur["_start"]):
            cur["_start"] = start
        if end and end > cur.get("_end", ""):
            cur["_end"] = end
    return {
        c: {k: v for k, v in (
            ("startDateWCMFaculty", d.get("_start", "")[:4]),
            ("endDateWCMFaculty", d.get("_end", "")[:4])) if v}
        for c, d in out.items()
    }


@source
def ed_sors_student_start_date():
    rows = ldap_search(
        "ed-sors",
        "(&(ou=students)(objectClass=weillCornellEduSORRoleRecord))",
        ["weillCornellEduCWID", "weillCornellEduStartDate"])
    best = {}
    for row in rows:
        cwid, start = _cwid(row), str(row.get("weillCornellEduStartDate", ""))
        if cwid and start and (cwid not in best or start < best[cwid]):
            best[cwid] = start
    return {c: {"startDateWCMStudent": v[:4]} for c, v in best.items()}


@source
def ed_people_inactive_student():
    rows = ldap_search(
        "ed-people",
        """(&(objectClass=weillCornellEduPerson)
            (!(weillCornellEduPersonTypeCode=affiliate-alumni))
            (weillCornellEduStatus=student:expired))""",
        ["weillCornellEduCWID"])
    return _by_cwid(rows, lambda r: {"inactiveNonAlumniStudent": "yes"})


# Program name normalisation, verbatim from the SPL's program_override case().
PROGRAM_OVERRIDE = {
    "MD-PhD WGS Biochemistry & Structural Biology": "Biochemistry & Structural Biology",
    "MD-PhD WGS Cell & Developmental Biology": "Cell & Developmental Biology",
    "MD-PhD WGS Immunology & Microbial Pathogenesis": "Immunology & Microbial Pathogenesis",
    "MD-PhD WGS Neuroscience": "Neuroscience",
    "MD-PhD WGS Pharmacology": "Pharmacology",
    "MD-PhD WGS Physiology, Biophysics & System Biology": "Physiology, Biophysics & Systems Biology",
    "Tri-I Program in Computational Biology & Medicine": "Computational Biology & Medicine",
    "Tri-I Program in Chemical Biology": "Chemical Biology",
    # ED's orgUnit;level2 spells these "MD-PhD <X>" where the old program
    # attribute said "MD-PhD WGS <X>". Same programs, so they collapse to the
    # same names -- Paul's call 2026-09-05: "Neuroscience" over "MD-PhD
    # Neuroscience". Note ED's own quirks: "TriI" without the hyphen, and
    # "System Biology" without the plural. Taken from the live L2 vocabulary,
    # not guessed.
    "MD-PhD Neuroscience": "Neuroscience",
    "MD-PhD Immunology & Microbial Pathogenesis": "Immunology & Microbial Pathogenesis",
    "MD-PhD Cell & Developmental Biology": "Cell & Developmental Biology",
    "MD-PhD Biochemistry & Structural Biology": "Biochemistry & Structural Biology",
    "MD-PhD Pharmacology": "Pharmacology",
    "MD-PhD Molecular Biology": "Molecular Biology",
    "MD-PhD Physiology, Biophysics & System Biology": "Physiology, Biophysics & Systems Biology",
    "MD-PhD TriI Computational Biology & Medicine": "Computational Biology & Medicine",

    "MD-PhD Rockefeller University Major": "MD-PhD Program",
    "MD-PhD Gerstner Sloan-Kettering": "MD-PhD Program",
}

# Lower number wins when one cwid has several programs. 999 for anything unlisted.
PROGRAM_PRIORITY = {
    "Biochemistry & Structural Biology": 1,
    "Biochemistry, Cell & Molecular Biology": 2,
    "Cell & Developmental Biology": 3,
    "Immunology & Microbial Pathogenesis": 4,
    "Molecular Biology": 5,
    "Neuroscience": 6,
    "Pharmacology": 7,
    "Physiology, Biophysics & Systems Biology": 8,
    "Computational Biology & Medicine": 15,
    "Chemical Biology": 16,
    "Population Health Sciences": 17,
    "Qatar Doctor of Medicine": 18,
    "Doctor of Medicine": 19,
    "MD-PhD Program": 20,
}


@source
def ed_students_program():
    rows = ldap_search(
        "ed-students",
        """(&(objectClass=weillCornellEduSORRoleRecord)
            (|(weillCornellEduDegreeCode=PHD)(weillCornellEduDegreeCode=MDPHD)
              (weillCornellEduDegreeCode=MD)))""",
        ["weillCornellEduCWID", "weillCornellEduProgram",
         "weillCornellEduOrgUnit;level2"])
    best = {}
    for row in rows:
        cwid = _cwid(row)
        if not cwid:
            continue
        program = _program_value(row)
        priority = PROGRAM_PRIORITY.get(program, 999)
        if cwid not in best or priority < best[cwid][0]:
            best[cwid] = (priority, program)
    return {c: {"program": p} for c, (_, p) in best.items() if p}


# ---------------------------------------------------------------------------
#                 SOURCE: CORNELL ITHACA  (gated, ships OFF)
# ---------------------------------------------------------------------------
#
# Cornell Ithaca people have no route into this table at all. `identity` is WCM
# Enterprise Directory + ASMS, and IdentityIndex.load() drives off `identity`, so
# bulk-loading them into DynamoDB `Identity` puts them in front of the AAR
# matcher exactly nowhere. This source is that route, and it ships OFF:
# registration is conditional on IDENTITY_CORNELL_SOURCE, so with the variable
# unset the function is not in SOURCES, build() never calls it, and the staged
# WCM rows are byte-for-byte what they were.
#
# It is NOT an LDAP enumeration like the sources above. Cornell's directory caps
# EVERY search at 200 entries -- it is a lookup interface, not a bulk one -- so
# the population cannot be read out of it. The roster comes from
# reciterdb.identity_cornell (33,872 rows, loaded once from Cornell's
# IthacaResearchFaculty.xlsx; there is no live feed), and the directory is asked
# only "is this netid still there?", 100 netids per OR filter. Ported from
# scripts/sync_cornell_ithaca_identities.py in the ReCiter Research repo, which
# proved the shape at this scale on 2026-09-03: 150 batches, 14,222 resolved,
# 15 seconds.


def _cornell_enabled(value):
    """The env gate. Unset, empty and anything unrecognised all read as off.

    Tests the value rather than presence because os.environ.get returns "" for a
    variable set to empty -- the same trap k8-cronjob-identity.yaml calls out
    when it refuses to re-declare LDAP_URL and the five base DNs.
    """
    return (value or "").strip().lower() in ("on", "true", "1")


# Research-relevant = holds, or has ever held, a role that publishes. Verbatim
# from the sync script. The `_all` flags are Cornell's "ever was" variants, which
# is what keeps former grad students and former project researchers in scope: an
# emeritus professor still needs an identity so a 2021 paper can be attributed to
# them. The set only grows, which is what a cumulative table wants.
CORNELL_POPULATION = """(is_faculty OR is_rte_faculty OR is_researcher OR is_postdoc
    OR is_student_grad_all OR is_rsrch_teach_title OR is_proj_researcher
    OR is_incubator_assoc OR is_academic
    OR affiliations_all REGEXP 'emeritus|retired faculty|former postdoc')"""

# Only the seven columns that reach a real `identity` column. The sync selects
# fourteen more -- role flags, three email columns, ORCID, affiliations_all --
# because it builds a DynamoDB payload; every one of them would be discarded here
# by _coerce, which iterates UPSERT_COLUMNS and drops unknown keys without a
# warning. The non-empty name guard is the one IdentityIndex.load() applies on
# the other side (`WHERE i.surname IS NOT NULL AND i.surname <> ''`): a row
# without both names is un-indexable, so it is not worth writing.
CORNELL_SELECT_SQL = """
SELECT netid, name, name_first, name_last,
       primary_department, primary_job_title, primary_business_title
FROM identity_cornell
WHERE {population}
  AND name_first IS NOT NULL AND name_first <> ''
  AND name_last  IS NOT NULL AND name_last  <> ''
ORDER BY netid
"""

# == identity_index.CAMPUS_ITHACA, pinned by an assertion in demo(). The two must
# agree or the campus scope silently reads every Cornell person as WCM.
CORNELL_CAMPUS_TYPE = "cornell-ithaca"

# Written on every Cornell record so finalize() can skip the WCM population
# filters. Not an UPSERT_COLUMN, so _coerce drops it before anything is written.
CORNELL_MARK = "cornellIthaca"

# Cornell's live directory publishes `cornellEduCWID` for people who ALSO hold a
# WCM identity. That is the authoritative dedup key: minting under the netid
# creates a SECOND person for the same human and splits their publications across
# two uids. Already happening for Martin Wells -- mtw1 (24 pubs) and maw2065 (65
# pubs) are one person. Measured 2026-09-03: 273 of the 15,030 carry a CWID.
#
# ponytail: a dated CSV carried in the image, not a second LDAP attribute. The
# directory search this source already runs would return cornellEduCWID for one
# more entry in `attributes`, so regenerating the file is a one-attribute change
# when the roster next moves. Nothing regenerates it today.
CORNELL_BRIDGE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "netid_cwid_bridge_2026-09-03.csv")


def load_cwid_bridge(path=CORNELL_BRIDGE):
    """netid -> (WCM cwid, already a ReCiter identity), for every bridged netid."""
    import csv  # lazy: nothing else in this module reads a CSV

    if not os.path.exists(path):
        raise SystemExit(
            f"cornell: the CWID bridge is missing ({path}). It is gate 1 -- "
            f"without it a dual-appointment person is minted under their netid "
            f"and split across two uids. Check the Dockerfile COPY and "
            f"CORNELL_BRIDGE agree on the filename.")
    with open(path, newline="") as fh:
        return {r["netid"]: (r["cornellEduCWID"], r.get("cwid_in_reciter") == "yes")
                for r in csv.DictReader(fh)}


def _cornell_bridge_skips(bridge, live_cwids):
    """Bridged netids to drop, on either of two verdicts.

    The CSV's `cwid_in_reciter` was computed against DynamoDB `Identity` -- 57 of
    the 273. This is a different table: measured 2026-09-07, only 37 of those 57
    CWIDs have a row in `identity`, and one netid the CSV marks `no` (jje1 ->
    joe2011, John Eckenrode) does. Either verdict alone splits somebody, so both
    are honoured -- DynamoDB's because the WCM sources above may add that cwid
    here tomorrow, this table's because it is the one the matcher reads today.
    """
    return {n for n, (cwid, in_reciter) in bridge.items()
            if in_reciter or (cwid or "").strip().lower() in live_cwids}


CORNELL_DIRECTORY_HOST = "query.directory.cornell.edu"
CORNELL_DIRECTORY_PORT = 636
CORNELL_DIRECTORY_BASE = "ou=People,o=Cornell University,c=us"

# The directory enforces a hard 200-entry cap per search. 100 netids per OR
# filter keeps clear of it, and the headroom is for DETECTABILITY rather than
# politeness: at a batch of 200 a fully-resolving batch returns exactly 200
# entries and is indistinguishable from a truncated one, so the cap check below
# could never fire usefully. One netid can also return more than one entry.
CORNELL_DIRECTORY_BATCH = 100
CORNELL_DIRECTORY_MAX_ENTRIES = 200


def _cornell_batches(seq, size=CORNELL_DIRECTORY_BATCH):
    seq = list(seq)
    return [seq[i:i + size] for i in range(0, len(seq), size)]


def _cornell_filter(netids):
    """An OR filter over a batch of netids, each escaped.

    Not routed through ldap_search()'s multi-line flattening -- that helper is
    for hand-written literal filters; this one is generated and every value has
    to be escaped, because an unescaped `*` in a netid would silently widen the
    search.
    """
    from ldap3.utils.conv import escape_filter_chars

    return "(|" + "".join(f"(uid={escape_filter_chars(n)})" for n in netids) + ")"


def cornell_directory_lookup(netids, batch_size=CORNELL_DIRECTORY_BATCH):
    """The netids Cornell's live directory still returns, lowercased.

    Its own bind and its own Connection, deliberately: ldap_conn() above is a
    module-level singleton bound to ED with LDAP_BIND_PASSWORD, and ldap_search()
    is hard-wired to BASE_DN and to paged_search_generator. Paging does not
    defeat a server-side 200-entry cap -- batching the FILTER is the mechanism --
    and binding the wrong directory would return nobody without erroring.

    get_info=None rather than ALL skips the schema fetch, which is why 150
    batches take 15 seconds. Errors are read off conn.result rather than raised,
    so raise_exceptions is left off too.

    Absence is expressed by omission. Any search that errors, or comes back at
    the entry cap, aborts: a partial answer here reads downstream as "these
    people no longer exist", which would silently shrink the roster.
    """
    dn = os.environ.get("CORNELL_ITHACA_ED_DN")
    pw = os.environ.get("CORNELL_ITHACA_ED_PASS")
    if not (dn and pw):
        raise SystemExit(
            "IDENTITY_CORNELL_SOURCE is on but CORNELL_ITHACA_ED_DN / "
            "CORNELL_ITHACA_ED_PASS are unset, so no netid can be checked against "
            "the live directory. The extract behind identity_cornell is dated "
            "2025-10-16 and has no live feed; admitting people from it unchecked "
            "mints identities for people who have left.")

    server = Server(CORNELL_DIRECTORY_HOST, port=CORNELL_DIRECTORY_PORT,
                    use_ssl=True, get_info=None)
    try:
        conn = Connection(server, user=dn, password=pw, auto_bind=True)
    except Exception as exc:            # noqa: BLE001 - the detail echoes the DN
        raise SystemExit(f"Cornell directory bind failed ({type(exc).__name__}).")

    found = set()
    batches = _cornell_batches(netids, batch_size)
    try:
        for n, batch in enumerate(batches, 1):
            conn.search(CORNELL_DIRECTORY_BASE, _cornell_filter(batch),
                        search_scope=SUBTREE, attributes=["uid"], time_limit=120)
            desc = (conn.result or {}).get("description")
            if desc != "success":
                raise SystemExit(
                    f"Cornell directory search failed on batch {n}/{len(batches)}: "
                    f"{desc}. Refusing to treat an incomplete answer as absence.")
            if len(conn.entries) >= CORNELL_DIRECTORY_MAX_ENTRIES:
                raise SystemExit(
                    f"Cornell directory returned {len(conn.entries)} entries for "
                    f"batch {n}/{len(batches)} -- at or over the "
                    f"{CORNELL_DIRECTORY_MAX_ENTRIES} cap, so the answer is "
                    f"truncated. Lower CORNELL_DIRECTORY_BATCH.")
            for entry in conn.entries:
                uid = (entry.entry_attributes_as_dict.get("uid") or [""])[0]
                if uid:
                    found.add(uid.strip().lower())
            if n % 25 == 0 or n == len(batches):
                logger.info("cornell directory: %d/%d batches, %d resolved",
                            n, len(batches), len(found))
    finally:
        conn.unbind()
    return found


# Cornell IT assigns netids and WCM IT assigns cwids with no coordination, so the
# two sequences will eventually issue the same string to two different people.
# That is worse here than in DynamoDB, not better: a PUT replaces the item and
# the wrong record is at least visibly wrong, while this upsert MERGES --
# COALESCE(VALUES(c), c) overwrites the WCM person's name, title and department
# and leaves their fullTimeFaculty, WCM dates and profile URLs standing,
# producing a chimera that looks like a valid WCM record and that no existing
# check would spot. identity.cwid is utf8mb4_unicode_ci, so `abc1234` and
# `ABC1234` are the same row -- the comparison below is case-folded to match.
#
# mtw1 and id93 are the two adjudicated cases, each the same human in both
# systems. Anything else aborts the build. Measured 2026-09-07: 0 of the 33,872
# netids exist as an identity.cwid, and that proves nothing about tomorrow, which
# is why this runs on every build rather than once.
CORNELL_KNOWN_COLLISIONS = {"mtw1", "id93"}


def _cornell_collisions(netids, live_cwids, ours, allowed=CORNELL_KNOWN_COLLISIONS):
    """Rostered netids that already name somebody else in `identity`.

    `ours` is the set already carrying the campus marker in person_person_type.
    `identity` has no institution column, so a re-run has no other way to
    recognise the rows it wrote last night and would otherwise flag all 14k of
    them as collisions.
    """
    return sorted(n for n in netids
                  if n.lower() in live_cwids
                  and n.lower() not in allowed
                  and n.lower() not in ours)


def _cornell_middle_name(full, first, last):
    """Cornell ships NAME as one string; NAME_FIRST/NAME_LAST never carry the
    middle. Empty unless the full name brackets exactly, so the compound-surname
    case ("Iwijn De Vlaminck") yields "" rather than a middle name of "De" --
    that family of empty-middleName/compound-surname rows has bitten ReCiter
    before.
    """
    full, first, last = (full or "").strip(), (first or "").strip(), (last or "").strip()
    if not (full and first and last):
        return ""
    # Whole names on both sides, not a bare prefix/suffix test. "Jones" suffixes
    # "Mary Jo Smith-Jones" and without this that yields a middleName of
    # "Jo Smith-"; "Jo" prefixes "Joanna". The surname boundary is whitespace and
    # NOT a hyphen, because a hyphen is inside a compound surname rather than
    # before it -- "De Vlaminck" still brackets, "Smith-Jones" still does not.
    if not (re.match(rf"{re.escape(first)}\b", full, re.I)
            and re.search(rf"(?:^|\s){re.escape(last)}$", full, re.I)):
        return ""
    return full[len(first):len(full) - len(last)].strip()


def _cornell_record(row):
    """One identity_cornell row -> the `identity` columns a Cornell person has.

    Five columns and the marker, and deliberately no more. Every FLAG_COLUMN is
    an assertion about a Weill Cornell appointment -- fullTimeFaculty, the four
    WCM date columns, facultyRank, the alumni flags -- and an Ithaca netid has
    none of them; IdentityIndex._record never reads them for a Cornell record
    anyway, because it takes the campus branch and labels from person_person_type.
    primaryTitle and primaryAcademicDepartment are varchar(200) upstream and
    varchar(128) here, so _coerce truncates the long ones and says so.
    """
    first = (row["name_first"] or "").strip()
    last = (row["name_last"] or "").strip()
    return {
        "surname": last,
        "givenName": first,
        "middleName": _cornell_middle_name(row["name"], first, last),
        "primaryTitle": row["primary_business_title"] or row["primary_job_title"] or "",
        "primaryAcademicDepartment": row["primary_department"] or "",
        CORNELL_MARK: "yes",
    }


def _cornell_marked(roster, ours):
    """Roster rows that person_person_type already agrees are Cornell.

    Dropped, not warned about. An unmarked netid is not merely unlabelled: it
    joins the WCM candidate pool (R1), `identity` is cumulative so the row cannot
    be taken back out, and the damage lands in a rarity term computed before any
    confidence exists. Admitting one is unrecoverable; skipping one costs a day,
    because the next run picks it up as soon as its marker lands.
    """
    marked = [r for r in roster if r["netid"].lower() in ours]
    if not marked:
        raise SystemExit(
            f"cornell: none of the {len(roster)} admitted netids has a "
            f"`{CORNELL_CAMPUS_TYPE}` row in person_person_type, so every one of "
            f"them would be indexed as WCM (R1). Run "
            f"scripts/sync_cornell_ithaca_identities.py, let one nightly cycle "
            f"land the person types, then enable IDENTITY_CORNELL_SOURCE.")
    if len(marked) != len(roster):
        logger.warning(
            "cornell: skipping %d of %d admitted netids with no `%s` row in "
            "person_person_type - they would be indexed as WCM (R1). Each is "
            "admitted on the first run after its marker lands; run "
            "scripts/sync_cornell_ithaca_identities.py to put it there.",
            len(roster) - len(marked), len(roster), CORNELL_CAMPUS_TYPE)
    return marked


def cornell_ithaca():
    """The Cornell Ithaca research population, gated by three checks.

    Order is the sync script's and is load-bearing: the CWID bridge runs first so
    the directory is only asked about netids that survive it, the live directory
    second, the namespace collision scan last, against the roster that is
    actually about to be written.
    """
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(update_date) FROM identity_cornell")
            extract = cur.fetchone()[0]
            cur.execute(CORNELL_SELECT_SQL.format(population=CORNELL_POPULATION))
            names = [d[0] for d in cur.description]
            roster = [dict(zip(names, r)) for r in cur.fetchall()]
            # `identity` is small enough (35,868 rows, 2026-09-07) to read whole;
            # it answers both the bridge's live-cwid test and the collision scan.
            cur.execute("SELECT LOWER(cwid) FROM identity")
            live_cwids = {c for (c,) in cur.fetchall()}
            cur.execute("SELECT LOWER(personIdentifier) FROM person_person_type "
                        "WHERE personType = %s", (CORNELL_CAMPUS_TYPE,))
            ours = {c for (c,) in cur.fetchall()}
    finally:
        conn.close()

    # There is no live feed behind identity_cornell, so the extract goes stale
    # silently -- 327 days old on 2026-09-07. That is reported, not enforced: the
    # per-netid directory check below is what turns "the extract is N days old"
    # from a blanket refusal into a per-person answer, and a hard age limit in an
    # unattended cron would simply mean this source never runs again.
    if extract:
        logger.info("cornell roster: %d rows, extract dated %s (%d days old)",
                    len(roster), extract, (datetime.date.today() - extract).days)

    rostered = {r["netid"] for r in roster}
    bridged = _cornell_bridge_skips(load_cwid_bridge(), live_cwids) & rostered
    roster = [r for r in roster if r["netid"] not in bridged]

    present = cornell_directory_lookup([r["netid"] for r in roster])
    absent = [r for r in roster if r["netid"].strip().lower() not in present]
    roster = [r for r in roster if r["netid"].strip().lower() in present]
    logger.info("cornell: %d rostered, %d skipped by the CWID bridge, %d absent "
                "from the live directory, %d admitted",
                len(rostered), len(bridged), len(absent), len(roster))

    collisions = _cornell_collisions([r["netid"] for r in roster], live_cwids, ours)
    if collisions:
        raise SystemExit(
            f"cornell: {len(collisions)} netid(s) already name somebody else in "
            f"identity and this upsert would merge onto their row: "
            f"{', '.join(collisions)}. Adjudicate each one, then add it to "
            f"CORNELL_KNOWN_COLLISIONS or drop it from the roster.")

    # ponytail: this source writes `identity` and NOTHING ELSE, and `identity` has
    # no campus column to write. IdentityIndex reads campus from
    # person_person_type (_campus_person_types), whose only writer is
    # retrieveArticles.py's unfiltered DynamoDB `Identity` scan via
    # dataTransformer.process_person_person_type -- and that table is dropped and
    # RENAME-swapped whole every night by setup/person_table_swap.sql, so a second
    # writer bolted in here would survive less than 24 hours. So the marker has to
    # arrive the way it already can: scripts/sync_cornell_ithaca_identities.py
    # PUTs these same netids to /reciter/save/identities/ with person_types()
    # emitting `cornell-ithaca` first and the rest of CORNELL_PERSON_TYPES after
    # it, and the nightly reciterdb job lands one row per type. RUN THAT FIRST. A
    # netid here with no marker is not merely unlabelled: IdentityIndex._record
    # stamps it CAMPUS_WCM and it joins the WCM candidate pool, which is plan risk
    # R1 -- 5,834 WCM surname cohorts grow and 1,169 go from cohort 1 to >1. Hence
    # a loud count rather than silence.
    # Dropped, not warned about. An unmarked netid is not merely unlabelled: it
    # joins the WCM candidate pool (R1), `identity` is cumulative so the row cannot
    # be taken back out, and the damage is in a rarity term computed before any
    # confidence exists. Admitting one is unrecoverable; skipping one costs a day,
    # because the next run picks it up as soon as its marker lands. So the roster
    # this source returns is exactly the people person_person_type already agrees
    # are Cornell.
    marked = _cornell_marked(roster, ours)

    return {r["netid"]: _cornell_record(r) for r in marked}


# Registered ONLY when the gate is on, and registered LAST so merge()'s setdefault
# leaves every WCM value in place on a shared cwid -- for the setdefault columns.
# ponytail: MAX_COLUMNS (surname, givenName) fold by MAX rather than first-wins, so
# on a shared cwid a Cornell name can still beat the WCM one. _cornell_collisions
# is what stops that, and it skips anyone already in `ours`, so it catches the
# collisions present on the first gate-on run but not a WCM cwid issued later that
# equals an admitted netid. Closing that needs a WCM-side signal on the identity
# row (any FLAG_COLUMN set) rather than the person_person_type marker; worth doing
# if WCM and Cornell ever share an issuing authority, not before. Deliberately not
# the other shape -- "always registered, returns {} when off" -- because build()
# raises SystemExit on a source that returns 0 rows, which is the guard that stops
# a silently missing source publishing a partial build, so an always-registered
# off source would kill the nightly WCM build every night.
#
# Turning this ON is still a one-way door for the ROWS -- `identity` is cumulative
# and the upsert is COALESCE-guarded, so switching the variable back off does not
# remove the ~14k Cornell rows. What it no longer does is take the WCM build down
# with it: identity_build_log carries a `lane`, so the MIN_ROWS_FLOOR baseline for
# a WCM-only run is other WCM-only runs (see write()). The variable is a real kill
# switch for future builds; it is not an undo for past ones.
if _cornell_enabled(os.getenv("IDENTITY_CORNELL_SOURCE")):
    source(cornell_ithaca)


def build_lane():
    """Which population this run stages, for the MIN_ROWS_FLOOR baseline.

    Derived from SOURCES rather than the env var so it can never disagree with
    what actually ran. Two lanes today; a third source would add a third name and
    start its own baseline, which is the correct behaviour -- a floor is only
    meaningful against runs that staged the same population.
    """
    return "wcm+cornell" if cornell_ithaca.__name__ in SOURCES else "wcm"


# ---------------------------------------------------------------------------
#                                  MERGE
# ---------------------------------------------------------------------------

def merge(collected):
    """Fold per-source dicts into one record per cwid.

    Replaces the SPL's `append` + terminal `stats ... by weillCornellEduCWID`.
    Flags are OR-ed, MAX_COLUMNS take the lexicographic max (verbatim SPL
    behaviour), everything else takes the first non-empty value in source order.
    """
    merged = {}
    for name in SOURCES:
        for cwid, vals in collected[name].items():
            record = merged.setdefault(cwid, {})
            for col, val in vals.items():
                if val in (None, ""):
                    continue
                if col in FLAG_COLUMNS:
                    record[col] = "yes"
                elif col in MAX_COLUMNS:
                    record[col] = max(record.get(col, ""), str(val))
                else:
                    record.setdefault(col, val)
    return merged


def finalize(merged):
    """Apply the SPL's post-stats evals and its three `where` filters."""
    out = []
    for cwid, r in merged.items():
        # MD-PhD suppresses the standalone MD and PhD alumni flags.
        if r.get("alumniMDPHD") == "yes":
            r["alumniMD"] = ""
            r["alumniPHD"] = ""

        # primaryAcademicDepartment falls back to program, then inactive dept.
        r["primaryAcademicDepartment"] = (
            r.get("primaryAcademicDepartment")
            or r.get("program")
            or r.get("inactiveDepartment")
            or "")

        if cwid in EXCLUDED_CWIDS:
            continue

        # Cornell Ithaca rows skip the two WCM population filters below. Those
        # filters are the SPL's and they answer "does this WCM person belong on
        # the roster?" out of WCM appointment flags -- which a Cornell person
        # holds none of, so every Cornell row would be dropped here silently,
        # leaving a source that logs 14k cwids and stages nothing. Their
        # membership was already decided upstream, by the research population
        # predicate and the live-directory gate in cornell_ithaca(). The marker
        # is not an UPSERT_COLUMN, so _coerce drops it and nothing new reaches
        # the table; with the source unregistered no record carries it and this
        # branch never fires.
        if r.get(CORNELL_MARK):
            out.append(_coerce(dict(r, cwid=cwid)))
            continue

        # `where isnotnull(...)` -- any evidence this person belongs at all.
        if not any(r.get(c) for c in (
                "primaryAcademicDepartment", "primaryProgram", "nonFaculty",
                "faculty", "inactiveFaculty", "alumniMDPHD", "alumniMD",
                "alumniPHD", "residentNYP")):
            continue

        # The final population filter: a real role, or a student whose start and
        # end years differ (i.e. an actual enrolment span, not a stub).
        has_role = any(r.get(c) == "yes" for c in (
            "fullTimeFaculty", "postdoc", "partTimeFaculty", "voluntaryFaculty",
            "emeritusFaculty", "adjunctFaculty", "residentNYP", "fellow",
            "faculty", "nonFaculty", "inactiveFaculty", "alumniMD",
            "alumniMDPHD", "alumniPHD"))
        start, end = r.get("startDateWCMStudent"), r.get("endDateWCMStudent")
        if not (has_role or (start and start != end)):
            continue

        out.append(_coerce(dict(r, cwid=cwid)))
    return out


def _coerce(r):
    """Type/width handling the non-strict DB Connect write did implicitly."""
    row = {}
    for col in UPSERT_COLUMNS:
        val = r.get(col, "")
        if col in YEAR_COLUMNS:
            # Empty years become NULL, not 0. Non-strict MySQL wrote 0, which
            # reads back as a valid year and is worse than a null.
            row[col] = int(val) if str(val).strip().isdigit() else None
        elif col in TRUNCATE_AT_128 and isinstance(val, str) and len(val) > 128:
            logger.warning("truncating %s for %s (%d chars)", col, r["cwid"], len(val))
            row[col] = val[:128]
        else:
            # The live table stores NULL for an absent value, not "". Writing ""
            # made every column look changed in the diff and is a different
            # value to any consumer testing IS NULL.
            row[col] = val if val != "" else None
    return row


# ---------------------------------------------------------------------------
#                                  WRITE
# ---------------------------------------------------------------------------

def _upsert_clauses():
    """Column list, placeholders and the ON DUPLICATE KEY UPDATE clause.

    Every column updates as COALESCE(VALUES(col), col): a new value overwrites,
    but NULL leaves whatever is already there. Without this the port erases
    history. It reaches 8,916 people Splunk's truncated output never did, and for
    those people the columns they do not qualify for come back empty -- measured
    2026-09-05, a plain VALUES() upsert would have wiped 237 primaryProgram and
    261 primaryOrg values that had been correct for years.

    The trade-off is that a value can never be cleared once set, only replaced.
    That is the deliberate reading of a cumulative table whose whole purpose is
    retaining department and division after someone leaves ED's ou=canonical.
    Clearing a stale value is a separate, explicit operation.
    """
    cols = ", ".join(f"`{c}`" for c in UPSERT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(UPSERT_COLUMNS))
    # The target column MUST be table-qualified. This runs as
    # INSERT INTO identity ... SELECT ... FROM identity_staging, so both tables
    # are in scope and a bare column name in the UPDATE clause is rejected:
    #   (1052, "Column 'surname' in UPDATE is ambiguous")
    # No dry run reaches this -- stage_only returns before the upsert, so this
    # statement first executed on the go-live attempt.
    updates = ", ".join(
        f"`identity`.`{c}`=COALESCE(VALUES(`{c}`), `identity`.`{c}`)"
        for c in UPSERT_COLUMNS if c != "cwid")
    return cols, placeholders, updates


def db_conn():
    import pymysql  # lazy: see asms_division

    return pymysql.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USERNAME"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ.get("DB_NAME", "reciterdb"),
        charset="utf8mb4",
        connect_timeout=10,
        read_timeout=500,
        write_timeout=500,
    )


def write(rows, stage_only=False):
    """Stage, gate on row count, then upsert in one transaction.

    Staging is not a shadow table for a swap -- identity is cumulative and rows
    are never deleted (see module docstring). It exists so the floor gate has
    something complete to measure before anything touches the live table.
    """
    cols, placeholders, updates = _upsert_clauses()

    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS identity_staging")
            cur.execute("CREATE TABLE identity_staging LIKE identity")
            cur.executemany(
                f"INSERT INTO identity_staging ({cols}) VALUES ({placeholders})",
                [[r[c] for c in UPSERT_COLUMNS] for r in rows])

            # pymysql opens an implicit transaction and does not autocommit.
            # Commit the staging load here: without it, a stage-only run returns
            # before any commit and conn.close() silently rolls the rows back,
            # leaving an empty table that DDL made look real.
            conn.commit()

            cur.execute("SELECT COUNT(*) FROM identity_staging")
            staged = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM identity")
            live = cur.fetchone()[0]
            logger.info("staged %d rows against %d live", staged, live)

            cur.execute("""CREATE TABLE IF NOT EXISTS identity_build_log (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    run_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    staged_rows INT NOT NULL,
                    upserted TINYINT(1) NOT NULL DEFAULT 0,
                    lane VARCHAR(16) NOT NULL DEFAULT 'wcm',
                    KEY idx_run_at (run_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
            cur.execute("ALTER TABLE identity_build_log ADD COLUMN IF NOT EXISTS "
                        "lane VARCHAR(16) NOT NULL DEFAULT 'wcm'")
            # The floor compares this run against previous runs OF THE SAME LANE.
            # Without that, one Cornell-on run (~48k staged) raises the 30-day
            # MAX for every WCM-only run after it (~33k), which is below the 95%
            # floor -- so flipping IDENTITY_CORNELL_SOURCE back off would refuse
            # to write the WCM identity build for 30 days. The env var has to be a
            # real kill switch, so the lane travels with the number it explains.
            lane = build_lane()
            cur.execute(
                "SELECT MAX(staged_rows) FROM identity_build_log "
                "WHERE upserted = 1 AND lane = %s "
                "AND run_at > NOW() - INTERVAL %s DAY",
                (lane, BUILD_LOG_WINDOW_DAYS))
            baseline = cur.fetchone()[0]
            if baseline:
                short = staged < baseline * MIN_ROWS_FLOOR
                logger.info("baseline %d rows from the last %d days",
                            baseline, BUILD_LOG_WINDOW_DAYS)
            else:
                short = False
                logger.warning("no prior %s build in the last %d days - "
                               "floor not enforced on this run",
                               lane, BUILD_LOG_WINDOW_DAYS)
            if short and not stage_only:
                raise SystemExit(
                    f"staged {staged} rows against a recent best of {baseline} "
                    f"(<{MIN_ROWS_FLOOR:.0%}) - refusing to write")
            if stage_only:
                # Staged and stopped. identity is untouched, so the diff queries
                # in docs/IDENTITY_PORT.md can compare the two side by side.
                # The floor is reported rather than enforced -- a dry run should
                # always finish and show its numbers.
                cur.execute("INSERT INTO identity_build_log (staged_rows, "
                            "upserted, lane) VALUES (%s, 0, %s)", (staged, lane))
                conn.commit()
                logger.info("--dry-run: %d rows staged, identity untouched%s",
                            staged, "  [BELOW FLOOR]" if short else "")
                return

            conn.begin()
            cur.execute(
                f"INSERT INTO identity ({cols}) "
                f"SELECT {cols} FROM identity_staging "
                f"ON DUPLICATE KEY UPDATE {updates}")
            cur.execute("INSERT INTO identity_build_log (staged_rows, upserted, "
                        "lane) VALUES (%s, 1, %s)", (staged, lane))
            conn.commit()
            logger.info("upserted %d rows into identity", staged)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
#                                   MAIN
# ---------------------------------------------------------------------------

def build():
    collected = {}
    for name, fn in SOURCES.items():
        rows = fn()
        logger.info("source %s: %d cwids", name, len(rows))
        if not rows:
            # An empty source is how the Splunk job failed silently. Never
            # publish a build with a missing source.
            raise SystemExit(f"source {name} returned 0 rows - refusing to build")
        collected[name] = rows

    merged = merge(collected)
    logger.info("merged: %d cwids", len(merged))
    rows = finalize(merged)
    logger.info("after filters: %d rows", len(rows))
    return rows


def main(dry_run=False):
    rows = build()
    if dry_run:
        for col in ("primaryAcademicDepartment", "primaryAcademicDivision",
                    "surname", "primaryOrg"):
            filled = sum(1 for r in rows if r.get(col))
            logger.info("  %s populated on %d/%d rows", col, filled, len(rows))
        for old, new in ORGUNIT_MIGRATION_WATCH:
            logger.info("  ED migration: %s=%d  %s=%d",
                        old, _migration_counts[old], new, _migration_counts[new])
    write(rows, stage_only=dry_run)


def spike():
    """Confirm the five SA-ldapsearch base DNs before trusting any query.

    The SPL addressed ED through `domain=` aliases resolved by SA-ldapsearch's
    own ldap.conf, which this job does not have. Every base DN in BASE_DN is a
    guess until this prints entries for all five.
    """
    probes = [
        ("ed-people", "(objectClass=weillCornellEduPerson)"),
        ("ed-organizations", "(o=*)"),
        ("ed-faculty", "(objectClass=weillCornellEduSORRecord)"),
        ("ed-sors", "(objectClass=weillCornellEduSORRecord)"),
        ("ed-students", "(objectClass=weillCornellEduSORRoleRecord)"),
    ]
    failed = []
    for domain, flt in probes:
        try:
            rows = ldap_search(domain, flt, ["*"], limit=3)
        except Exception as exc:                     # noqa: BLE001 - report all
            logger.error("%-18s FAIL base=%s: %s", domain, BASE_DN[domain], exc)
            failed.append(domain)
            continue
        if not rows:
            logger.error("%-18s EMPTY base=%s", domain, BASE_DN[domain])
            failed.append(domain)
            continue
        logger.info("%-18s OK base=%s", domain, BASE_DN[domain])
        logger.info("%-18s attrs=%s", "", sorted(rows[0])[:25])
    if failed:
        raise SystemExit(f"unresolved base DNs: {', '.join(failed)}")
    print("spike ok - all five base DNs resolve")


def demo():
    """Self-check for the merge/finalize logic -- the only non-obvious part."""
    collected = {
        "a": {"abc1001": {"surname": "Adams", "faculty": "yes"},
              "xyz2002": {"surname": "Young", "alumniMDPHD": "yes",
                          "alumniMD": "yes", "alumniPHD": "yes"},
              "exc0001": {"surname": "Gone", "faculty": "yes"}},
        "b": {"abc1001": {"surname": "Zeta", "primaryTitle": "Professor",
                          "primaryAcademicDepartment": "Medicine"},
              "nob0003": {"surname": "Nobody"}},
    }
    real_sources, real_excluded = dict(SOURCES), set(EXCLUDED_CWIDS)
    try:
        SOURCES.clear()
        SOURCES.update({"a": None, "b": None})
        EXCLUDED_CWIDS.clear()
        EXCLUDED_CWIDS.add("exc0001")
        rows = {r["cwid"]: r for r in finalize(merge(collected))}
    finally:
        SOURCES.clear()
        SOURCES.update(real_sources)
        EXCLUDED_CWIDS.clear()
        EXCLUDED_CWIDS.update(real_excluded)

    assert rows["abc1001"]["surname"] == "Zeta", "max() tie-break across sources"
    assert rows["abc1001"]["primaryTitle"] == "Professor", "first non-empty wins"
    # Absent values are written as NULL, not "" -- the live table uses NULL and
    # every consumer tests `= 'yes'`, so the two behave identically. Splunk wrote
    # "" for rows its main append touched and NULL for the rest; that split is a
    # Splunk artifact and is deliberately not reproduced.
    assert rows["xyz2002"]["alumniMD"] is None, "MD-PhD suppresses standalone MD"
    assert rows["xyz2002"]["alumniPHD"] is None, "MD-PhD suppresses standalone PhD"
    assert "exc0001" not in rows, "excluded cwid dropped"
    assert "nob0003" not in rows, "no role and no department - filtered out"
    assert rows["abc1001"]["startDateWCMFaculty"] is None, "empty year is NULL not 0"

    assert _mssql_target("jdbc:sqlserver://asms.db:1433;databaseName=ASMS") == ("asms.db", 1433)
    assert _mssql_target("asms.db") == ("asms.db", 1433)
    assert _mssql_target("sqlserver://asms.db:1500") == ("asms.db", 1500)

    # ED returns `labeledURI;onlinedirectory`; the SPL spelled it
    # `labeledURI;onlineDirectory`. Neither casing may miss.
    row = _Row([("labeledURI;onlinedirectory", "http://d"), ("weillCornellEduCWID", "abc1001")])
    assert row.get("labeledURI;onlineDirectory") == "http://d", "attr option casing"
    assert row.get("weillcornelleducwid") == "abc1001", "attr name casing"
    assert row.get("nosuchattr") == "", "missing attr defaults to empty string"

    # Multi-valued attributes: get() returns the first, all() returns every one.
    # Person types arrive least-specific-first, so reading them through get()
    # silently emptied every flag.
    multi = _Row([("weillCornellEduPersonTypeCode",
                   ["academic", "academic-faculty", "academic-faculty-weillfulltime"])])
    assert multi.get("weillCornellEduPersonTypeCode") == "academic", "get() takes first"
    assert "academic-faculty-weillfulltime" in multi.all("weillCornellEduPersonTypeCode")
    assert len(multi.all("weillCornellEduPersonTypeCode")) == 3
    assert _Row([("x", "solo")]).all("x") == ["solo"], "scalar wraps to a list"
    assert _Row([("x", "")]).all("x") == [], "empty yields nothing"

    # ldap3 hands back datetimes for populated GeneralizedTime attributes and ""
    # for absent ones; sorting that mix used to raise TypeError.
    assert _flatten(datetime.datetime(2019, 5, 15)).startswith("2019-05-15")
    assert _flatten([]) == "" and _flatten(None) == ""
    assert sorted([_flatten(datetime.datetime(2019, 5, 15)), _flatten("")]) == \
        ["", "2019-05-15T00:00:00"], "mixed date/empty must sort"
    assert _flatten(datetime.datetime(2019, 5, 15))[:4] == "2019", "year slice"

    # PREFER_ORGUNIT is a coalesce, never a hard switch: the old value survives
    # wherever ED has no orgUnit, so flipping the flag can blank nobody.
    # NB: mutate globals() directly. `import buildIdentity` from __main__ creates
    # a SECOND module object, so setting the flag there would not affect the one
    # _dept_value actually reads.
    has_both = _Row([("weillCornellEduPrimaryDepartment", "Brain and Mind Research Institute"),
                     ("weillCornellEduPrimaryOrgUnit;level1", "Brain and Mind Research")])
    old_only = _Row([("weillCornellEduPrimaryDepartment", "Pediatrics")])
    was = PREFER_ORGUNIT
    try:
        globals()["PREFER_ORGUNIT"] = False
        assert _dept_value(has_both) == "Brain and Mind Research Institute"
        assert _dept_value(old_only) == "Pediatrics"
        globals()["PREFER_ORGUNIT"] = True
        assert _dept_value(has_both) == "Brain and Mind Research", "prefers orgUnit"
        assert _dept_value(old_only) == "Pediatrics", "falls back, never blanks"

        # DEEPEST level, not L1 -- otherwise every librarian files under ITS.
        lib = _Row([("weillCornellEduPrimaryDepartment", "Library"),
                    ("weillCornellEduPrimaryOrgUnit;level1", "Information Technologies and Services"),
                    ("weillCornellEduPrimaryOrgUnit;level2", "Library")])
        assert _dept_value(lib) == "Library", "L2 beats L1"

        # ED's L2 spellings collapse through the same override table.
        for raw, want in [("MD-PhD Neuroscience", "Neuroscience"),
                          ("MD-PhD TriI Computational Biology & Medicine",
                           "Computational Biology & Medicine"),
                          ("MD-PhD Physiology, Biophysics & System Biology",
                           "Physiology, Biophysics & Systems Biology"),
                          ("Pharmacology", "Pharmacology"),
                          ("MD-PhD Program", "MD-PhD Program")]:
            got = _program_value(_Row([("weillCornellEduOrgUnit;level2", raw)]))
            assert got == want, f"program override {raw!r} -> {got!r}, wanted {want!r}"

        # Inactive faculty SPLIT the levels: L1 is the department, L2 the
        # division. Flattening to deepest would put a division in the
        # department column for this cohort alone.
        role = _Row([("weillCornellEduDepartment", "Medicine"),
                     ("weillCornellEduOrgUnit;level1", "Medicine"),
                     ("weillCornellEduOrgUnit;level2", "Infectious Diseases")])
        assert _dept_value(role, primary=False) == "Infectious Diseases", \
            "primary path still flattens to deepest"

        # No level2 -> falls back to the old program attribute, never blank.
        assert _program_value(_Row([("weillCornellEduProgram", "Molecular Biology")])) \
            == "Molecular Biology", "program falls back"
    finally:
        globals()["PREFER_ORGUNIT"] = was

    # Flag off: `program` is STILL normalised -- the SPL's case() ran
    # unconditionally, and PROGRAM_PRIORITY is keyed on the normalised names, so
    # skipping it also breaks the ranking. Only primaryProgram stays raw.
    assert _program_value(_Row([("weillCornellEduPrimaryProgram", "MD-PhD WGS Neuroscience")]),
                          primary=True) == "MD-PhD WGS Neuroscience", "primaryProgram raw when off"
    assert _program_value(_Row([("weillCornellEduProgram",
                                 "Tri-I Program in Chemical Biology")])) == "Chemical Biology", \
        "program normalised even when off"
    assert PROGRAM_PRIORITY[_program_value(_Row([("weillCornellEduProgram",
                                                  "MD-PhD WGS Neuroscience")]))] == 6, \
        "normalised name must score in PROGRAM_PRIORITY, not fall to 999"

    # A NULL must never overwrite an existing value: the port reaches people
    # Splunk's truncated output never did, and writing their empty columns over
    # real history is data loss, not a correction.
    _cols, _ph, _upd = _upsert_clauses()
    assert "VALUES(`surname`), `identity`.`surname`" in _upd, \
        "the fallback column must be table-qualified or MySQL 1052s"
    assert "COALESCE" in _upd
    assert "`identity`.`cwid`=" not in _upd, "the join key is never updated"
    assert _upd.count("COALESCE") == len(UPSERT_COLUMNS) - 1, "every column guarded"
    assert _ph.count("%s") == len(UPSERT_COLUMNS)

    # Multi-valued program: the generic entry sorts first, the specific one wins.
    multi_prog = _Row([("weillCornellEduProgram",
                        ["MD-PhD Program", "Biochemistry & Structural Biology"])])
    assert _program_value(multi_prog) == "Biochemistry & Structural Biology", \
        "highest-priority program wins within a record"
    assert _program_value(_Row([("weillCornellEduProgram", ["MD-PhD Program"])])) \
        == "MD-PhD Program", "single value unaffected"

    # -- Cornell Ithaca source (ships off) ---------------------------------
    # The gate. Unset, empty, "off" and any junk all read as off; anything else
    # registering the source would put ~14k Cornell people into the nightly WCM
    # build without anyone asking for them.
    assert _cornell_enabled(None) is False, "unset reads as off"
    assert _cornell_enabled("") is False, "an empty variable reads as off"
    assert _cornell_enabled("off") is False
    assert _cornell_enabled("yes") is False, "only on/true/1 enable it"
    assert _cornell_enabled(" ON ") and _cornell_enabled("True") and _cornell_enabled("1")
    # Registration follows the gate in BOTH directions. Registering an off source
    # that returns {} would hit build()'s zero-row SystemExit and kill the WCM
    # build, so the two must never come apart.
    assert ("cornell_ithaca" in SOURCES) == \
        _cornell_enabled(os.getenv("IDENTITY_CORNELL_SOURCE")), \
        "cornell_ithaca is registered if and only if IDENTITY_CORNELL_SOURCE is on"

    # The floor's baseline is per-lane, so flipping the gate back off cannot make
    # the next WCM-only build look short against a Cornell-inflated best.
    assert build_lane() == ("wcm+cornell" if "cornell_ithaca" in SOURCES else "wcm")

    # R1: an unmarked netid is dropped, never admitted as WCM. Partial -> skip the
    # unmarked ones; none marked -> refuse, because that is the precondition
    # (sync + one nightly cycle) not having been met.
    _r = [{"netid": "aa1"}, {"netid": "BB2"}, {"netid": "cc3"}]
    assert [r["netid"] for r in _cornell_marked(_r, {"aa1", "bb2"})] == ["aa1", "BB2"], \
        "marker test is case-insensitive on the netid"
    try:
        _cornell_marked(_r, set())
    except SystemExit as e:
        assert "person_person_type" in str(e)
    else:
        raise AssertionError("a wholly unmarked roster must refuse, not return []")

    # The campus marker is the whole point of the exercise; if it drifts from
    # identity_index's constant the scope reads every Cornell person as WCM.
    from identity_index import CAMPUS_ITHACA
    assert CORNELL_CAMPUS_TYPE == CAMPUS_ITHACA, "campus marker must match identity_index"

    # One roster row -> exactly the columns a Cornell person has, and no WCM flag.
    cornell_row = {"netid": "mtw1", "name": "Martin Timothy Wells",
                   "name_first": "Martin", "name_last": "Wells",
                   "primary_department": "Statistics and Data Science",
                   "primary_job_title": "Professor", "primary_business_title": None}
    rec = _cornell_record(cornell_row)
    assert rec == {"surname": "Wells", "givenName": "Martin", "middleName": "Timothy",
                   "primaryTitle": "Professor",
                   "primaryAcademicDepartment": "Statistics and Data Science",
                   CORNELL_MARK: "yes"}, rec
    assert not (set(rec) & FLAG_COLUMNS), "no WCM appointment flag on a Cornell person"
    assert set(rec) - {CORNELL_MARK} <= set(UPSERT_COLUMNS), "no invented columns"
    # Compound surname: "De" is not a middle name.
    assert _cornell_middle_name("Iwijn De Vlaminck", "Iwijn", "De Vlaminck") == ""
    assert _cornell_middle_name("Andres Arroyo", "Andres", "Arroyo") == ""
    # a surname that merely SUFFIXES the real one is not a match
    assert _cornell_middle_name("Mary Jo Smith-Jones", "Mary", "Jones") == ""
    assert _cornell_middle_name("Mary Jo Smith-Jones", "Mary", "Smith-Jones") == "Jo"
    # ... and neither is a given name that merely prefixes it
    assert _cornell_middle_name("Joanna Lee", "Jo", "Lee") == ""

    # finalize() keeps it. Without the CORNELL_MARK branch the has_role filter
    # drops it -- primaryAcademicDepartment satisfies the isnotnull filter but
    # not the role one, and the two lists are NOT the same list.
    kept = {r["cwid"]: r for r in finalize({"mtw1": dict(rec)})}
    assert kept["mtw1"]["surname"] == "Wells", "Cornell row survives finalize"
    assert kept["mtw1"]["fullTimeFaculty"] is None, "no WCM flag written"
    assert CORNELL_MARK not in kept["mtw1"], "the marker never reaches the table"
    assert not finalize({"mtw1": {k: v for k, v in rec.items() if k != CORNELL_MARK}}), \
        "unmarked, the same row is dropped by the WCM role filter"

    # CWID bridge: skip on DynamoDB's verdict OR on a live row in this table.
    # jje1 is the case the CSV alone gets wrong (marked `no`, cwid joe2011 is in
    # `identity`), and admitting it would split John Eckenrode across two rows.
    bridge = {"aa749": ("ava4001", True), "jje1": ("joe2011", False),
              "aam346": ("alm4018", False)}
    assert _cornell_bridge_skips(bridge, {"joe2011"}) == {"aa749", "jje1"}
    assert _cornell_bridge_skips(bridge, set()) == {"aa749"}, "CSV verdict alone"

    # Namespace collision: a netid already naming somebody else in `identity`
    # aborts. Case-folded, because identity.cwid is utf8mb4_unicode_ci and the
    # two would upsert onto one row.
    assert _cornell_collisions(["aa749"], {"aa749"}, ours=set()) == ["aa749"]
    assert _cornell_collisions(["AA749"], {"aa749"}, ours=set()) == ["AA749"], \
        "collision test is case-folded"
    assert _cornell_collisions(["aa749"], {"aa749"}, ours={"aa749"}) == [], \
        "our own row from last night is not a collision"
    assert _cornell_collisions(["mtw1"], {"mtw1"}, ours=set()) == [], "adjudicated"
    assert _cornell_collisions(["aa749"], set(), ours=set()) == [], "no live row"

    # Batching and filter escaping. The headroom assertion is the invariant, not
    # the constant: at a batch of 200 a healthy batch is indistinguishable from a
    # truncated one and the cap check could never fire.
    assert CORNELL_DIRECTORY_BATCH * 2 <= CORNELL_DIRECTORY_MAX_ENTRIES
    assert _cornell_batches(range(250), 100) == \
        [list(range(100)), list(range(100, 200)), list(range(200, 250))]
    assert _cornell_batches([], 100) == [], "no netids, no searches"
    assert len(_cornell_batches(range(15030))) == 151, "one batch per 100 rostered"
    assert _cornell_filter(["a1", "b2"]) == "(|(uid=a1)(uid=b2))"
    assert _cornell_filter(["a*b"]) == r"(|(uid=a\2ab))", "filter metachars escaped"

    print("demo ok")


if __name__ == "__main__":
    if "--demo" in sys.argv:        # merge logic only, no network, no DB
        demo()
    elif "--spike" in sys.argv:     # confirm the five LDAP base DNs
        spike()
    else:
        main(dry_run="--dry-run" in sys.argv)
