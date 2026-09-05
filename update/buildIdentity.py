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
                    KEY idx_run_at (run_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
            cur.execute(
                "SELECT MAX(staged_rows) FROM identity_build_log "
                "WHERE upserted = 1 AND run_at > NOW() - INTERVAL %s DAY",
                (BUILD_LOG_WINDOW_DAYS,))
            baseline = cur.fetchone()[0]
            if baseline:
                short = staged < baseline * MIN_ROWS_FLOOR
                logger.info("baseline %d rows from the last %d days",
                            baseline, BUILD_LOG_WINDOW_DAYS)
            else:
                short = False
                logger.warning("no prior build in the last %d days - "
                               "floor not enforced on this run",
                               BUILD_LOG_WINDOW_DAYS)
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
                            "upserted) VALUES (%s, 0)", (staged,))
                conn.commit()
                logger.info("--dry-run: %d rows staged, identity untouched%s",
                            staged, "  [BELOW FLOOR]" if short else "")
                return

            conn.begin()
            cur.execute(
                f"INSERT INTO identity ({cols}) "
                f"SELECT {cols} FROM identity_staging "
                f"ON DUPLICATE KEY UPDATE {updates}")
            cur.execute("INSERT INTO identity_build_log (staged_rows, upserted) "
                        "VALUES (%s, 1)", (staged,))
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
    print("demo ok")


if __name__ == "__main__":
    if "--demo" in sys.argv:        # merge logic only, no network, no DB
        demo()
    elif "--spike" in sys.argv:     # confirm the five LDAP base DNs
        spike()
    else:
        main(dry_run="--dry-run" in sys.argv)
