"""DRAFT v2 source functions -- NOT IMPORTED, NOT WIRED IN.

Round two: the 25 findings from IA_REVIEW_FINDINGS.md were handed to four fixers
and the results re-verified. All four slices still returned NEEDS_FIXES, with 28
unresolved findings and -- the important number -- 14 REGRESSIONS: things the
draft had right that the fix round broke.

The regressions share one shape: hard aborts added on unmeasured premises. The
fixers turned weak-but-harmless gaps into job-killing failures. Examples the
verifiers reproduced by running the assembled module:

  * _ts() raises SystemExit from inside one source on a single unparseable date,
    taking down all eight tables. The draft wrote a wrong-shaped string and
    finished the run.
  * ed_location_master() aborts before its LDAP read unless
    IA_LOCATION_MASTER_MIN_ROWS is exported, and nothing sets it -- so the exact
    --dry-run command the cronjob manifest ships does a full read and then dies.
  * _assert_target_schema() aborts whenever a target table holds more rows than
    the ED read returns, which is the EXPECTED state: the module's own docstring
    says the Splunk delete lane under-deletes, so the tables carry residue.
  * ed_phones_contact() aborts if either phone arm is empty, which halts the job
    if ED simply keeps every number in telephoneNumber.

Round two is kept for the SPL analysis in its comments and for the measurement
list it produced, not as code to ship. See IA_MEASUREMENTS.md: the real blocker
is 32 measurements, not another generation pass.
"""


# ==========================================================================
# SLICE: canonical | verdict NEEDS_FIXES
# ==========================================================================
# ==========================================================================
# SLICE: canonical (types: canonical, sor_record)
# ==========================================================================
#
# Reviewer's premise, accepted: `nomv` chooses the SEPARATOR, not whether an
# attribute is multi-valued. Every `nomv` in the SPL is immediately followed by
# `rex mode=sed "s/\n/|/g"` precisely because nomv had already collapsed a
# multivalue to a NEWLINE-joined string. An attribute that is never nomv'd is
# not thereby single-valued -- it simply reaches the lookup newline-joined
# instead of pipe-joined, and dbxoutput pushes that whole string. For the SOR
# search it is not even an inference: `stats list(*) as * by dn` (SPL L609)
# converts EVERY field to a multivalue immediately before `table`.
#
# So the rule below is NOT "the 39 nomv lines". It is: anything LDAP permits to
# carry more than one value goes through mv(). "|".join(["x"]) == "x", so mv()
# is byte-identical to get() on single-valued data and strictly safer on
# multi-valued data. The only reads left on a single value are the ones where a
# joined string would be meaningless -- the timestamp columns, which hold one
# instant -- and those log when they see more than one value rather than
# assuming they cannot.

# ED is migrating weillCornellEduDepartment/DepartmentCode to
# weillCornellEduOrgUnit/OrgUnitCode, and those OrgUnitCode values are the CSIDs
# Paul wants preferred over department codes (PORT_SPEC.md, "Design direction:
# prefer CSIDs over department codes"). Ships OFF so the first diff against
# Splunk is empty; flipping it makes the department columns coalesce
# new-then-old, never hard-switch. Assigned exactly once for the whole module --
# if another slice also assigns PREFER_ORGUNIT, delete the duplicate rather than
# letting concatenation order decide it.
PREFER_ORGUNIT = os.environ.get("IA_PREFER_ORGUNIT", "0") == "1"

# The SPL's canonical-vs-SOR discriminator. Ships ON, i.e. the port pushes the
# same population the Splunk job has always pushed. Set IA_UID_LEN_FILTER=0 only
# after the counts logged below have been read: the reviewer is right that the
# "shared lookup" story is false (canonical writes identity_authority_canonical
# at SPL L150, SOR writes identity_authority_sor_record at L614, and the one
# lookup they share carries no uid at all), so the filters' real purpose is
# unestablished and dropping them is an undeclared behaviour change either way.
# Both branches are counted every run, so the assumption is measured rather than
# asserted.
UID_LEN_FILTER = os.environ.get("IA_UID_LEN_FILTER", "1") == "1"

# Requested on both person searches. The ";level1"/";level2" LDAP options are
# what carry the migration: L1 is the parent org, L2 the actual unit. The BARE
# forms are requested for measurement only -- the cardinality log below reports
# whether ED ever answers a bare request with the subtyped value, which is an
# open question the SPL's separate "Company" search (a whole saved search built
# only to see o;company) is evidence about. They take no part in any value.
_PERSON_ORGUNIT_ATTRS = [
    "weillCornellEduOrgUnit",
    "weillCornellEduOrgUnit;level1", "weillCornellEduOrgUnit;level2",
    "weillCornellEduOrgUnitCode",
    "weillCornellEduOrgUnitCode;level1", "weillCornellEduOrgUnitCode;level2",
    "weillCornellEduPrimaryOrgUnit",
    "weillCornellEduPrimaryOrgUnit;level1", "weillCornellEduPrimaryOrgUnit;level2",
    "weillCornellEduPrimaryOrgUnitCode",
    "weillCornellEduPrimaryOrgUnitCode;level1",
    "weillCornellEduPrimaryOrgUnitCode;level2",
]

# The eight columns those attributes are emitted as. A ";" in a column name is a
# fight with SQL for no gain, so ";level1" becomes "Level1". None of these exist
# in _person_canonical or _person_sor_record yet: the SPL never read the
# attributes, so its `table` clauses (L144, L609) cannot name them. Logged as a
# DDL prerequisite on every run.
_PERSON_ORGUNIT_COLUMNS = [
    "weillCornellEduOrgUnitLevel1", "weillCornellEduOrgUnitLevel2",
    "weillCornellEduOrgUnitCodeLevel1", "weillCornellEduOrgUnitCodeLevel2",
    "weillCornellEduPrimaryOrgUnitLevel1", "weillCornellEduPrimaryOrgUnitLevel2",
    "weillCornellEduPrimaryOrgUnitCodeLevel1",
    "weillCornellEduPrimaryOrgUnitCodeLevel2",
]

# Row counts by type, recorded as each source finishes. Read only by the
# sor_record floor cross-check below, which tolerates the entry being absent, so
# this is not an ordering dependency between sources -- run_sources() may run
# either source alone and the cross-check simply reports itself skipped.
_PERSON_ROW_COUNTS = {}

# Attribute names whose multi-value warning has already been logged, so a
# 33,000-row read cannot emit 33,000 identical warnings.
_TS_MULTI_WARNED = set()


# ---------------------------------------------------------------------------
#            SHARED: displayName, GeneralizedTime, orgUnit, uid length
# ---------------------------------------------------------------------------

def _display_name(row):
    """The SPL's two-branch displayName, verbatim:

        case(isnotnull(weillCornellEduMiddleName),
             replace(givenName." ".weillCornellEduMiddleName." ".sn,"  "," "),
             1=1, givenName." ".sn)

    Splunk's isnotnull() is TRUE for a present-but-empty middle name, and the
    replace("  "," ") exists to clean up the double space that branch produces.
    _Row.get() returns "" for absent and empty alike, so the falsy branch here
    emits exactly the string the collapse would have. The two agree on every
    input; the collapse is kept anyway because ED also stores middle names
    padded with a trailing space.

    THE ONE PLACE row.get() IS CORRECT ON A NAME. givenName, sn and
    weillCornellEduMiddleName are all multi-valued in LDAP (RFC 4519 marks none
    of them SINGLE-VALUE) and the columns that carry them go through mv(). A
    display name is one name, though, not a set: "Jane|Janet Smith" is not a
    display name, so this composes the first value of each and the cardinality
    log reports how often there is more than one.

    weillCornellEduPreferredGivenName takes NO part. The SOR record search
    requests it, emits it as its own column, and never lets it override
    givenName -- do not "improve" that here, it would change every display name
    of everyone who has one.
    """
    given = row.get("givenName")
    middle = row.get("weillCornellEduMiddleName")
    surname = row.get("sn")
    if middle:
        return ("%s %s %s" % (given, middle, surname)).replace("  ", " ")
    return "%s %s" % (given, surname)


def _ts(row, attr):
    """GeneralizedTime -> "YYYY-MM-DD HH:MM:SS", the SPL's substr() reassembly.

    EVERY GeneralizedTime attribute goes through here, not just the three the
    SPL happened to slice. The SPL sliced createTimestamp, modifyTimestamp and
    weillCornellEduHireDate (L598-604); weillCornellEduStartDate,
    weillCornellEduEndDate and weillCornellEduDOB it passed through untouched,
    which was fine for Splunk because ldapfilter had already rendered them as
    strings. ldap3 parses a populated GeneralizedTime into a tz-aware datetime,
    and _Row.get() normalises that to ISO-8601 WITH the offset ("...+00:00").
    Writing that into a column that holds "YYYY-MM-DD HH:MM:SS" is a third
    shape: rejected or truncated if the column is DATETIME, and silently fatal
    to every downstream LEFT(col,10) or strptime if it is VARCHAR.

    Read through dict.get rather than _Row.get so the datetime survives to
    strftime. Anything ldap3 hands back as a raw string is sliced the way the
    SPL sliced it, so neither shape can silently write nonsense.

    The output shape is CHECKED, not assumed. Nobody has yet read
    information_schema for these columns (see the measurements list), so the one
    thing this can do is refuse to write a shape it did not intend. A value that
    will not render aborts the run naming the attribute and the DN -- never the
    value, because weillCornellEduDOB is on the sensitive list.

    The SPL's `eval createTimestamp = max(createTimestamp)` ahead of the slicing
    is Splunk defensiveness against a multivalue that operational attributes
    cannot have. Not reproduced as a max(), but a multivalue is no longer
    assumed away either: it is logged once per attribute.
    """
    raw = dict.get(row, attr.lower(), "")
    if isinstance(raw, list):
        values = [v for v in raw if v not in (None, "")]
    elif raw in (None, ""):
        values = []
    else:
        values = [raw]
    if not values:
        return ""
    if len(values) > 1 and attr.lower() not in _TS_MULTI_WARNED:
        _TS_MULTI_WARNED.add(attr.lower())
        # Count only. A timestamp column holds one instant, so there is nothing
        # to join; this says the assumption was violated and by how much.
        logger.warning(
            "%s returned %d values on at least one entry -- a timestamp column "
            "holds one instant, so the first is kept. Investigate before "
            "trusting that column. No value is logged.", attr, len(values))
    value = values[0]
    # Duck-typed rather than isinstance(datetime): this module does not import
    # datetime and one attribute is not enough of a reason to start.
    if hasattr(value, "strftime"):
        text = value.strftime("%Y-%m-%d %H:%M:%S")
    else:
        text = str(value)
        if len(text) >= 14 and text[:14].isdigit():    # "20190515000000Z"
            text = "%s-%s-%s %s:%s:%s" % (text[0:4], text[4:6], text[6:8],
                                          text[8:10], text[10:12], text[12:14])
        elif len(text) >= 8 and text[:8].isdigit():    # date-only "19850315"
            text = "%s-%s-%s 00:00:00" % (text[0:4], text[4:6], text[6:8])
        else:
            text = text.replace("T", " ")[:19]         # already ISO-8601
    if not (len(text) == 19
            and text[4] == "-" and text[7] == "-" and text[10] == " "
            and text[13] == ":" and text[16] == ":"
            and text[0:4].isdigit() and text[5:7].isdigit()
            and text[8:10].isdigit() and text[11:13].isdigit()
            and text[14:16].isdigit() and text[17:19].isdigit()):
        raise SystemExit(
            "ABORT: %s did not render as 'YYYY-MM-DD HH:MM:SS' on %s. The "
            "Splunk-era columns hold that shape and this job must not write a "
            "third one. Nothing has been written. The value is deliberately "
            "not logged -- read it from the directory entry."
            % (attr, getattr(row, "entry_dn", "(no dn)") or "(no dn)"))
    return text


def _active_member(row):
    """weillCornellEduActiveMember as 1/0.

    The SPL is `if(max(weillCornellEduActiveMember) > "false", 1, 0)` -- a
    LEXICOGRAPHIC, case-SENSITIVE comparison over a multivalue. RFC 4517 renders
    LDAP Booleans uppercase, and "TRUE" < "false" in ASCII (uppercase sorts
    first), so if ED returns "TRUE" this flag has been 0 for every person in the
    table for as long as it has run. That is suspected broken, not intended.

    Implemented as the intent instead: 1 if ANY value is true, case-insensitive.
    row.all() because a value-set membership test must see every value; with
    lowercase data the two agree exactly, with uppercase data the SPL is always
    0. PORT_SPEC.md, "Measurements needed before porting", carries the query
    that settles which casing ED returns
    (`SELECT weillCornellEduActiveMember, COUNT(*) FROM _person_canonical GROUP
    BY 1`); until it is run, expect this column to differ from Splunk for
    everyone and treat the difference as the fix, not a regression.
    """
    return 1 if any(v.strip().lower() == "true"
                    for v in row.all("weillCornellEduActiveMember")) else 0


def _orgunit_value(row, old_attr, new_base, counts):
    """One department/CSID value: the old attribute, coalesced with the new.

    DEEPEST LEVEL WINS. ;level1 is the parent org and ;level2 the actual unit --
    the Library sits at L2 under an L1 of "Information Technologies and
    Services", so taking L1 alone files every librarian under ITS
    (buildIdentity.py:645, where the same rule is already in production).

    NEVER A HARD SWITCH. The sibling port measured that zero records carry a new
    attribute without the old one, so preferring the new value can only ever be
    a coalesce. `counts` records, per run, how many rows carry the old value,
    how many carry a new one, and how many carry a new one WITHOUT an old one --
    that third number is the sibling port's measurement, re-taken here every
    night instead of assumed, and it is what decides whether PREFER_ORGUNIT can
    be flipped.

    With PREFER_ORGUNIT off this returns the old value alone, so the first diff
    against Splunk is empty and the new columns are pure addition.
    """
    old = mv(row, old_attr)
    # The bare form takes no part: the SPL never saw it and nobody has confirmed
    # ED populates it. It is requested and measured, not read.
    new = mv(row, new_base + ";level2") or mv(row, new_base + ";level1")
    seen = counts.setdefault(new_base, [0, 0, 0])
    if old:
        seen[0] += 1
    if new:
        seen[1] += 1
        if not old:
            seen[2] += 1
    return (new or old) if PREFER_ORGUNIT else old


def _uid_len(row):
    """(longest uid length, number of uid values) for the SPL's len(uid) tests.

    uid is multi-valued in LDAP (RFC 4519 does not mark it SINGLE-VALUE), and
    the SPL's `len()` returns null on a multivalue, so Splunk drops those rows
    outright -- in the SOR search that drop happens AFTER `stats list(*) as * by
    dn` has made every field a multivalue, which PORT_SPEC.md identifies as
    throwing away multi-company affiliates that belong in the table. There is no
    stats step here, so that pathology is gone by construction; the length test
    is applied to the longest value, which keeps those affiliates and still
    excludes a genuinely short uid.
    """
    values = row.all("uid")
    return (max((len(v) for v in values), default=0), len(values))


def _log_cardinality(rows, attrs, label):
    """Measure which requested attributes ED actually returns more than once.

    This is the measurement the reviewer asked for, taken in the same pass that
    builds the rows rather than in a separate probe: per attribute,
    max(len(row.all(attr))) across every entry. Everything below is already
    routed through mv(), so a name appearing here is not a data-loss report --
    it is the evidence that the routing was necessary, and the list to hand to
    whoever has to decide whether a "|" separator is acceptable to that column's
    consumers. Attribute NAMES only; no value is ever logged.
    """
    worst = {}
    for row in rows:
        for attr in attrs:
            count = len(row.all(attr))
            if count > worst.get(attr, 0):
                worst[attr] = count
    multi = sorted(a for a, n in worst.items() if n > 1)
    logger.info("cardinality %s: %d of %d requested attributes returned more "
                "than one value: %s", label, len(multi), len(attrs),
                ", ".join(multi) if multi else "(none)")
    empty = sorted(a for a in attrs if not worst.get(a))
    logger.info("cardinality %s: %d of %d requested attributes were empty on "
                "every entry: %s", label, len(empty), len(attrs),
                ", ".join(empty) if empty else "(none)")
    return multi


# ---------------------------------------------------------------------------
#                        SOURCE: canonical (ed-people)
# ---------------------------------------------------------------------------

@source("canonical", alias="ed-people")
def ed_people_canonical():
    """"Identity Authority - Canonical" -- one row per ED person entry.

    Divergences from the SPL, each deliberate:

      * FULL SNAPSHOT. The SPL harvests (modifyTimestamp>=now-3d) into an
        append=t lookup, and "Canonical, db update" then pushes a 2-day slice of
        that lookup. A person missed by one run is stranded: they age out of the
        push window while staying in the lookup forever, so `source="ed"` in the
        DN reconciliation means "seen at some point since the lookup was last
        truncated", not "in ED now". No deletion decision is sound against that.
      * NO DN RECONSTRUCTION. The SPL evals
        dn = "uid=" . uid . ",ou=people,dc=weill,dc=cornell,dc=edu" (L139).
        ldap3 returns the real DN; we key on row.entry_dn. The rows already in
        _person_canonical carry the reconstruction, so the two key spaces must
        be proved equal before anything is written -- assert_dn_overlap() does
        that at runtime, ahead of every write, and aborts below 90%.
      * `where ... len(uid) < 20` (SPL L180 and L189) is KEPT, not dropped, and
        counted. The draft's justification for dropping it -- a shared lookup
        the two searches separate with mirror-image length tests -- is false:
        canonical writes identity_authority_canonical (L150), SOR writes
        identity_authority_sor_record (L614), and identity_authority_dn carries
        no uid at all. With the real purpose unestablished, dropping the filter
        would insert people the Splunk job has never pushed, and being inserts
        no row-count diff would flag them. The excluded count is logged every
        run; set IA_UID_LEN_FILTER=0 once that count is known.
      * `ou` is requested by the SPL and never emitted; not requested here.
      * weillCornellEduOrgUnit / weillCornellEduOrgUnitCode ARE requested and
        emitted -- see _PERSON_ORGUNIT_ATTRS. They are the reason this port
        exists (HANDOFF, "The ask", driver 2) and the SPL reads neither.
    """
    attrs = [
        "weillCornellEduCWID", "uid", "givenName", "sn",
        "weillCornellEduMiddleName", "weillCornellEduStatus", "o",
        "weillCornellEduPrimaryTitle", "weillCornellEduPrimaryTitleCode",
        "weillCornellEduWorkingTitle", "weillCornellEduPrimaryRoleCode",
        "telephoneNumber", "postalCode", "street", "l", "st",
        "weillCornellEduPrimaryDepartment",
        "weillCornellEduPrimaryDepartmentCode", "weillCornellEduDepartment",
        "weillCornellEduDepartmentCode", "weillCornellEduPersonTypeCode",
        "mail", "weillCornellEduDepartmentCodeHierarchy",
        "weillCornellEduDepartmentHierarchy", "weillCornellEduActiveMember",
        "createTimestamp", "modifyTimestamp", "weillCornellEduCWIDRetired",
        "weillCornellEduProviderID", "eduPersonPrimaryAffiliation",
        # The subtyped descriptions are requested EXPLICITLY alongside their
        # bare types. The SPL got labeledURI;pops and weillCornellEduReleaseCode
        # ;picture back from a bare request via spath, but the sibling Company
        # search is evidence ED does not always return options for a bare type
        # -- it needed a whole separate search to see o;company. Asking for both
        # costs one list entry and cannot miss.
        "labeledURI", "labeledURI;pops", "labeledURI;vivo",
        "labeledURI;onlinedirectory",
        "weillCornellEduReleaseCode", "weillCornellEduReleaseCode;picture",
        "weillCornellEduReleaseCode;person", "weillCornellEduReleaseCode;mail",
        "weillCornellEduReleaseCode;telephonenumber",
        "weillCornellEduReleaseCode;location",
    ] + _PERSON_ORGUNIT_ATTRS

    rows = ldap_search("ed-people", "(objectClass=weillCornellEduPerson)", attrs)
    _log_cardinality(rows, attrs, "canonical")

    out = {}
    counts = {}
    no_cwid = 0
    uid_excluded = 0
    uid_multi = 0
    for row in rows:
        # mv(), not get(), even here. If weillCornellEduCWID is ever
        # multi-valued the joined "a|b" is visible in the column and in the
        # first join that fails; get() would pick one arbitrarily and nothing
        # downstream would ever know. The cardinality line above says whether it
        # happens at all.
        cwid = mv(row, "weillCornellEduCWID")
        if not cwid:
            no_cwid += 1
            continue                       # SPL: where isnotnull(...CWID)
        uid_len, uid_count = _uid_len(row)
        if uid_count > 1:
            uid_multi += 1
        if uid_len >= 20:
            uid_excluded += 1
            if UID_LEN_FILTER:
                continue                   # SPL: where len(uid) < 20
        # `dn` is the dict key, not a column -- the writer takes it from there.
        out[row.entry_dn] = {
            "weillCornellEduCWID": cwid,
            "uid": mv(row, "uid"),
            "givenName": mv(row, "givenName"),
            "sn": mv(row, "sn"),
            "weillCornellEduMiddleName": mv(row, "weillCornellEduMiddleName"),
            "displayName": _display_name(row),

            # RFC 4519 marks none of these SINGLE-VALUE, and `nomv` never said
            # they were single-valued -- it only chose the separator for the
            # ones the SPL author happened to think about. Routed through mv()
            # so a second value cannot be dropped silently.
            "weillCornellEduPrimaryTitle": mv(row, "weillCornellEduPrimaryTitle"),
            "weillCornellEduPrimaryTitleCode":
                mv(row, "weillCornellEduPrimaryTitleCode"),
            "weillCornellEduWorkingTitle": mv(row, "weillCornellEduWorkingTitle"),
            "weillCornellEduPrimaryRoleCode":
                mv(row, "weillCornellEduPrimaryRoleCode"),
            "telephoneNumber": mv(row, "telephoneNumber"),
            "postalCode": mv(row, "postalCode"),
            "street": mv(row, "street"),
            "l": mv(row, "l"),
            "st": mv(row, "st"),
            "mail": mv(row, "mail"),
            "weillCornellEduProviderID": mv(row, "weillCornellEduProviderID"),
            "eduPersonPrimaryAffiliation":
                mv(row, "eduPersonPrimaryAffiliation"),
            # labeledURI carries no SINGLE-VALUE in RFC 2079, and neither does a
            # subtype of it. The SPL renamed these only because Splunk cannot
            # table a field whose name contains a semicolon; that rename says
            # nothing about cardinality.
            "labeledURIpops": mv(row, "labeledURI;pops"),
            "labeledURIvivo": mv(row, "labeledURI;vivo"),
            "labeledURIonlinedirectory": mv(row, "labeledURI;onlinedirectory"),

            # The 13 attributes this search does nomv + `rex "s/\n/|/g"`. They
            # reach the DB pipe-joined today, so mv() is not just safe here, it
            # is the format the column already holds. (The SPL also nomv's
            # `title` and `weillCornellEduFTE`, but this search neither requests
            # nor tables either one -- dead blocks, deliberately not emitted.
            # Both are live on the SOR record side.)
            "weillCornellEduStatus": mv(row, "weillCornellEduStatus"),
            "o": mv(row, "o"),
            "weillCornellEduDepartmentHierarchy":
                mv(row, "weillCornellEduDepartmentHierarchy"),
            "weillCornellEduDepartmentCodeHierarchy":
                mv(row, "weillCornellEduDepartmentCodeHierarchy"),
            "weillCornellEduPersonTypeCode":
                mv(row, "weillCornellEduPersonTypeCode"),
            "weillCornellEduCWIDRetired": mv(row, "weillCornellEduCWIDRetired"),
            "releaseCodeMail": mv(row, "weillCornellEduReleaseCode;mail"),
            "releaseCodeTelephonenumber":
                mv(row, "weillCornellEduReleaseCode;telephonenumber"),
            "releaseCodeLocation": mv(row, "weillCornellEduReleaseCode;location"),
            "releaseCodePerson": mv(row, "weillCornellEduReleaseCode;person"),
            "releaseCodePicture": mv(row, "weillCornellEduReleaseCode;picture"),

            # Department, coalesced with orgUnit under PREFER_ORGUNIT. Off by
            # default, so these are byte-identical to the SPL today.
            "weillCornellEduDepartment": _orgunit_value(
                row, "weillCornellEduDepartment",
                "weillCornellEduOrgUnit", counts),
            "weillCornellEduDepartmentCode": _orgunit_value(
                row, "weillCornellEduDepartmentCode",
                "weillCornellEduOrgUnitCode", counts),
            "weillCornellEduPrimaryDepartment": _orgunit_value(
                row, "weillCornellEduPrimaryDepartment",
                "weillCornellEduPrimaryOrgUnit", counts),
            "weillCornellEduPrimaryDepartmentCode": _orgunit_value(
                row, "weillCornellEduPrimaryDepartmentCode",
                "weillCornellEduPrimaryOrgUnitCode", counts),

            # The new columns, carried alongside the old ones rather than
            # instead of them. The OrgUnitCode pair is the CSID Paul wants
            # preferred over departmentCode; emitting both leaves that
            # preference to PREFER_ORGUNIT instead of to a re-port.
            "weillCornellEduOrgUnitLevel1": mv(row, "weillCornellEduOrgUnit;level1"),
            "weillCornellEduOrgUnitLevel2": mv(row, "weillCornellEduOrgUnit;level2"),
            "weillCornellEduOrgUnitCodeLevel1":
                mv(row, "weillCornellEduOrgUnitCode;level1"),
            "weillCornellEduOrgUnitCodeLevel2":
                mv(row, "weillCornellEduOrgUnitCode;level2"),
            "weillCornellEduPrimaryOrgUnitLevel1":
                mv(row, "weillCornellEduPrimaryOrgUnit;level1"),
            "weillCornellEduPrimaryOrgUnitLevel2":
                mv(row, "weillCornellEduPrimaryOrgUnit;level2"),
            "weillCornellEduPrimaryOrgUnitCodeLevel1":
                mv(row, "weillCornellEduPrimaryOrgUnitCode;level1"),
            "weillCornellEduPrimaryOrgUnitCodeLevel2":
                mv(row, "weillCornellEduPrimaryOrgUnitCode;level2"),

            "weillCornellEduActiveMember": _active_member(row),
            "createTimestamp": _ts(row, "createTimestamp"),
            "modifyTimestamp": _ts(row, "modifyTimestamp"),
        }

    logger.info("canonical: %d entries, %d without a CWID, %d with uid length "
                ">= 20 (%s), %d with more than one uid",
                len(rows), no_cwid, uid_excluded,
                "excluded, SPL len(uid) < 20" if UID_LEN_FILTER
                else "ADMITTED -- IA_UID_LEN_FILTER=0", uid_multi)
    for base in sorted(counts):
        old_n, new_n, orphan = counts[base]
        logger.info("orgunit canonical %-42s old=%d new=%d new-without-old=%d "
                    "(PREFER_ORGUNIT=%s)", base, old_n, new_n, orphan,
                    PREFER_ORGUNIT)
    logger.warning("canonical emits %d columns that %s has never had and that "
                   "need DDL before the first live run: %s",
                   len(_PERSON_ORGUNIT_COLUMNS), TABLES["canonical"],
                   ", ".join(_PERSON_ORGUNIT_COLUMNS))
    _PERSON_ROW_COUNTS["canonical"] = len(out)
    return out


# ---------------------------------------------------------------------------
#                       SOURCE: sor_record (ed-sors)
# ---------------------------------------------------------------------------

@source("sor_record", alias="ed-sors")
def ed_sors_sor_record():
    """"Identity Authority - SOR record" -- one row per SOR record entry.

    Divergences from the SPL, each deliberate:

      * FULL SNAPSHOT, and NO DN RECONSTRUCTION -- same reasons as
        ed_people_canonical. The SPL builds (L605)
        dn = "uid=" . uid . ",ou=" . ou . ",ou=sors,dc=weill,dc=cornell,dc=edu",
        which interpolates the `ou` ATTRIBUTE VALUE rather than the RDN and
        additionally assumes every SOR branch is exactly one level under
        ou=sors. Both assumptions can make the stored dn differ from the real
        one; assert_dn_overlap() measures that before anything is written.
      * `where ... len(uid) > 20` (SPL L611) is KEPT, not dropped, and counted.
        See ed_people_canonical -- the shared-lookup justification for dropping
        it is false, and this one sits in the BUILDER, ahead of outputlookup, so
        a SOR record with a short uid has never reached _person_sor_record at
        all. What is NOT reproduced is the way Splunk evaluates it: the SPL runs
        this test after `stats list(*) as * by dn` has made uid a multivalue, so
        len() returns null and the whole row is dropped -- PORT_SPEC.md
        identifies those rows as multi-company affiliates that belong in the
        table. _uid_len() tests the longest value, so they are kept.
      * THE COMPANY JOIN IS GONE. The SPL left-joins identityAuthorityCompany
        .csv -- itself the output of a third search that reads o;company off
        these very entries -- on (ou, weillCornellEduCWID) with the join key
        pinned by `eval ou = "affiliates"`. So a company only ever landed on a
        record already sitting in ou=affiliates, and only if the CSV had been
        rebuilt. ldap3 returns o;company as an ordinary key, so it is read
        straight off the entry, for every record, in this one pass.
      * FOUR COLUMNS DROPPED. releaseCodePerson, releaseCodePicture, requester
        and sponsor are in the SPL's output table and are never assigned by it
        -- this search has no spath and no rename for any of them, so all four
        have always written NULL. Emitting four permanently-NULL columns would
        only preserve the illusion that something populates them.
      * `stats list(*) as * by dn` is dropped. DNs are unique per entry, so it
        collapses nothing -- and list() silently caps at 100 values per group,
        which is a Splunk hazard, not behaviour worth keeping.
      * weillCornellEduStartDate / EndDate / DOB now go through _ts() like every
        other GeneralizedTime attribute. The SPL left them as whatever
        ldapfilter rendered; ldap3 renders a datetime, and _Row.get() would put
        an offset-bearing ISO string into a column holding "YYYY-MM-DD
        HH:MM:SS".
      * weillCornellEduOrgUnit / weillCornellEduOrgUnitCode requested and
        emitted -- see _PERSON_ORGUNIT_ATTRS and ed_people_canonical.
    """
    attrs = [
        "weillCornellEduCWID", "uid", "ou", "weillCornellEduSORID",
        "givenName", "sn", "weillCornellEduMiddleName",
        "weillCornellEduStatus", "o", "o;company",
        "weillCornellEduPrimaryTitle", "weillCornellEduPrimaryTitleCode",
        "weillCornellEduPrimaryRole", "weillCornellEduWorkingTitle",
        "weillCornellEduPrimaryRoleCode", "weillCornellEduStartDate",
        "weillCornellEduEndDate", "weillCornellEduDegree", "title",
        "personalTitle", "telephoneNumber", "postalCode", "weillCornellEduDOB",
        "street", "l", "st", "weillCornellEduPrimaryDepartment",
        "weillCornellEduPrimaryDepartmentCode", "weillCornellEduDepartment",
        "weillCornellEduDepartmentCode", "weillCornellEduPersonTypeCode",
        "weillCornellEduFTE", "manager", "manager;manager", "mail",
        "weillCornellEduProgramCode", "weillCornellEduProgram",
        "createTimestamp", "modifyTimestamp", "weillCornellEduHireDate",
        "weillCornellEduPreferredGivenName", "weillCornellEduProviderID",
        "weillCornellEduCredentialedLocation",
        # weillCornellEduReleaseCode is requested by the SPL and never read --
        # see the four dropped columns above -- so it is not requested here.
    ] + _PERSON_ORGUNIT_ATTRS

    rows = ldap_search("ed-sors", "(objectClass=weillCornellEduSORRecord)", attrs)
    _log_cardinality(rows, attrs, "sor_record")

    out = {}
    counts = {}
    no_cwid = 0
    uid_excluded = 0
    uid_multi = 0
    for row in rows:
        cwid = mv(row, "weillCornellEduCWID")
        if not cwid:
            no_cwid += 1
            continue                       # SPL: where isnotnull(...CWID)
        uid_len, uid_count = _uid_len(row)
        if uid_count > 1:
            uid_multi += 1
        if uid_len <= 20:
            uid_excluded += 1
            if UID_LEN_FILTER:
                continue                   # SPL: where len(uid) > 20
        out[row.entry_dn] = {
            "weillCornellEduCWID": cwid,
            "uid": mv(row, "uid"),
            # ou is flagged multi-valued by three separate nomv lines elsewhere
            # in the SPL (L224, L298, L516) and is what makes the SPL's dn
            # reconstruction unstable. Joined, never first-value-wins.
            "ou": mv(row, "ou"),
            "weillCornellEduSORID": mv(row, "weillCornellEduSORID"),
            "givenName": mv(row, "givenName"),
            "sn": mv(row, "sn"),
            "weillCornellEduMiddleName": mv(row, "weillCornellEduMiddleName"),
            "displayName": _display_name(row),

            # Not on any nomv line in this search -- which, per the corrected
            # premise, says nothing about cardinality. `stats list(*) as * by
            # dn` at L609 makes EVERY one of these a multivalue immediately
            # before the table and outputlookup, so wherever ED returns more
            # than one value the column already holds the joined set today.
            "weillCornellEduStatus": mv(row, "weillCornellEduStatus"),
            "weillCornellEduPrimaryTitle": mv(row, "weillCornellEduPrimaryTitle"),
            "weillCornellEduPrimaryTitleCode":
                mv(row, "weillCornellEduPrimaryTitleCode"),
            "weillCornellEduWorkingTitle": mv(row, "weillCornellEduWorkingTitle"),
            "weillCornellEduPrimaryRole": mv(row, "weillCornellEduPrimaryRole"),
            "weillCornellEduPrimaryRoleCode":
                mv(row, "weillCornellEduPrimaryRoleCode"),
            "weillCornellEduDegree": mv(row, "weillCornellEduDegree"),
            "personalTitle": mv(row, "personalTitle"),
            "telephoneNumber": mv(row, "telephoneNumber"),
            "postalCode": mv(row, "postalCode"),
            "street": mv(row, "street"),
            "l": mv(row, "l"),
            "st": mv(row, "st"),
            "mail": mv(row, "mail"),
            "weillCornellEduPreferredGivenName":
                mv(row, "weillCornellEduPreferredGivenName"),
            "weillCornellEduProviderID": mv(row, "weillCornellEduProviderID"),

            # `nomv` + `rex "s/\n/|/g"` in this search -- already pipe-joined in
            # the DB today.
            "o": mv(row, "o"),
            "title": mv(row, "title"),
            "weillCornellEduFTE": mv(row, "weillCornellEduFTE"),
            "weillCornellEduPersonTypeCode":
                mv(row, "weillCornellEduPersonTypeCode"),
            "weillCornellEduCredentialedLocation":
                mv(row, "weillCornellEduCredentialedLocation"),
            # The SPL `nomv`s weillCornellEduProgram but seds
            # weillCornellEduProgramCode (L688-692) -- a copy-paste slip that
            # leaves Program NEWLINE-joined in the lookup and ProgramCode never
            # joined at all. Both are multi-valued in ED, so both go through
            # mv() here and both will differ from Splunk on the first diff.
            # That is the bug being fixed, not a regression.
            "weillCornellEduProgram": mv(row, "weillCornellEduProgram"),
            "weillCornellEduProgramCode": mv(row, "weillCornellEduProgramCode"),

            # Department, coalesced with orgUnit under PREFER_ORGUNIT.
            "weillCornellEduDepartment": _orgunit_value(
                row, "weillCornellEduDepartment",
                "weillCornellEduOrgUnit", counts),
            "weillCornellEduDepartmentCode": _orgunit_value(
                row, "weillCornellEduDepartmentCode",
                "weillCornellEduOrgUnitCode", counts),
            "weillCornellEduPrimaryDepartment": _orgunit_value(
                row, "weillCornellEduPrimaryDepartment",
                "weillCornellEduPrimaryOrgUnit", counts),
            "weillCornellEduPrimaryDepartmentCode": _orgunit_value(
                row, "weillCornellEduPrimaryDepartmentCode",
                "weillCornellEduPrimaryOrgUnitCode", counts),

            "weillCornellEduOrgUnitLevel1": mv(row, "weillCornellEduOrgUnit;level1"),
            "weillCornellEduOrgUnitLevel2": mv(row, "weillCornellEduOrgUnit;level2"),
            "weillCornellEduOrgUnitCodeLevel1":
                mv(row, "weillCornellEduOrgUnitCode;level1"),
            "weillCornellEduOrgUnitCodeLevel2":
                mv(row, "weillCornellEduOrgUnitCode;level2"),
            "weillCornellEduPrimaryOrgUnitLevel1":
                mv(row, "weillCornellEduPrimaryOrgUnit;level1"),
            "weillCornellEduPrimaryOrgUnitLevel2":
                mv(row, "weillCornellEduPrimaryOrgUnit;level2"),
            "weillCornellEduPrimaryOrgUnitCodeLevel1":
                mv(row, "weillCornellEduPrimaryOrgUnitCode;level1"),
            "weillCornellEduPrimaryOrgUnitCodeLevel2":
                mv(row, "weillCornellEduPrimaryOrgUnitCode;level2"),

            # `manager` and the subtyped `manager;manager` name the same
            # relationship. The SPL needed a whole separate search ("Identity
            # Authority - Manager", three cwid-sharded ldapsearches into a CSV)
            # purely because Splunk's spath renders the subtype as its own field;
            # ldap3 returns both as ordinary keys, so both are read here. It then
            # reduced the result to ONE value with max(manager, manager1) -- a
            # lexicographic pick, arbitrary rather than a rule, and manager1 is
            # never assigned anywhere in that search. Keep every value instead.
            # dict.fromkeys preserves ED's order and drops the duplicate a server
            # that answers the bare request with the subtyped value would give.
            "manager": "|".join(dict.fromkeys(
                row.all("manager") + row.all("manager;manager"))),

            # o;company is the subtyped sibling of o, which this search does
            # nomv. Read straight off the entry -- see the dropped join above.
            "company": mv(row, "o;company"),

            "createTimestamp": _ts(row, "createTimestamp"),
            "modifyTimestamp": _ts(row, "modifyTimestamp"),
            "weillCornellEduHireDate": _ts(row, "weillCornellEduHireDate"),
            "weillCornellEduStartDate": _ts(row, "weillCornellEduStartDate"),
            "weillCornellEduEndDate": _ts(row, "weillCornellEduEndDate"),
            # DOB is date-only in the schema and its VALUE is on the sensitive
            # list -- it must never appear in a log line, an exception message
            # or a comment. _ts() validates the full "YYYY-MM-DD HH:MM:SS" shape
            # first, so this slice is a checked date rather than a blind
            # truncation, and _ts()'s own abort names the attribute and the DN
            # only.
            "weillCornellEduDOB": _ts(row, "weillCornellEduDOB")[:10],
        }

    logger.info("sor_record: %d entries, %d without a CWID, %d with uid length "
                "<= 20 (%s), %d with more than one uid",
                len(rows), no_cwid, uid_excluded,
                "excluded, SPL len(uid) > 20" if UID_LEN_FILTER
                else "ADMITTED -- IA_UID_LEN_FILTER=0", uid_multi)
    for base in sorted(counts):
        old_n, new_n, orphan = counts[base]
        logger.info("orgunit sor_record %-42s old=%d new=%d new-without-old=%d "
                    "(PREFER_ORGUNIT=%s)", base, old_n, new_n, orphan,
                    PREFER_ORGUNIT)
    logger.warning("sor_record emits %d columns that %s has never had and that "
                   "need DDL before the first live run: %s",
                   len(_PERSON_ORGUNIT_COLUMNS), TABLES["sor_record"],
                   ", ".join(_PERSON_ORGUNIT_COLUMNS))

    # MIN_ROWS["sor_record"] is the SAME floor as canonical, and it cannot be
    # right: canonical is one row per person, sor_record is one row per SOR
    # record and every person in ou=people has at least one. A shared floor
    # therefore only catches a read that lost roughly three quarters of the
    # branch -- and on the --no-delete path the floor is the ONLY guard, since
    # MAX_DELETE_FRACTION never runs. Two floors are added here rather than
    # editing the shared constant:
    #
    #   1. The structural one. sor_record can never legitimately return fewer
    #      rows than canonical, whatever the populations turn out to be.
    #      Skipped, with a log line, when canonical did not run in this process.
    #   2. The measured one, IA_MIN_ROWS_SOR_RECORD. Nobody has run
    #      SELECT COUNT(*) on the table yet, so no number is invented here: the
    #      variable is unset until someone measures, and until then the run says
    #      so on every pass.
    people = _PERSON_ROW_COUNTS.get("canonical")
    if people is None:
        logger.info("sor_record: canonical did not run in this process, so the "
                    "sor_record >= canonical floor is not applied")
    elif len(out) < people:
        raise SystemExit(
            "ABORT: sor_record returned %d rows against canonical's %d. Every "
            "person carries at least one SOR record, so sor_record can never "
            "legitimately be the smaller of the two -- this reads as a "
            "truncated paged search, which MIN_ROWS['sor_record'] (%d, shared "
            "with canonical) is far too low to catch. Nothing has been written."
            % (len(out), people, MIN_ROWS["sor_record"]))
    measured = os.environ.get("IA_MIN_ROWS_SOR_RECORD", "")
    if measured:
        if len(out) < int(measured):
            raise SystemExit(
                "ABORT: sor_record returned %d rows, below the measured floor "
                "IA_MIN_ROWS_SOR_RECORD=%s. Nothing has been written."
                % (len(out), measured))
    else:
        logger.warning(
            "IA_MIN_ROWS_SOR_RECORD is unset, so sor_record is guarded only by "
            "MIN_ROWS['sor_record']=%d (shared with canonical, and too low for "
            "this population) and by the canonical cross-check. Run SELECT "
            "COUNT(*) FROM %s and set it to about 0.8x that number.",
            MIN_ROWS["sor_record"], TABLES["sor_record"])

    _PERSON_ROW_COUNTS["sor_record"] = len(out)
    return out

# ==========================================================================
# SLICE: roles-org | verdict NEEDS_FIXES
# ==========================================================================
# ==========================================================================
# SLICE: roles-org (types: sor_role_record, organization)
# ==========================================================================
#
# DDL REQUIRED BEFORE THE FIRST LIVE RUN
# --------------------------------------
# ed_sor_role_record() emits six weillCornellEduOrgUnit* columns that do not
# exist in the target table: the SPL's `table` clause (line 703 of the export)
# stops at createTimestamp/modifyTimestamp and names none of them. They are
# emitted anyway -- they are the measurable half of the department ->
# orgUnit migration and the whole reason this port was commissioned -- but a
# comment saying "needs DDL" is not enforcement, so _assert_target_schema()
# below reads the target table's real column list and aborts naming any
# emitted key that is missing. Apply this by hand first.
ORGUNIT_DDL = """
-- Target table is TABLES["sor_role_record"] (_person_sor_role_record today);
-- if that mapping is corrected when conf-db_outputs is read, correct this too.
--
-- The widths below are a starting point, NOT a measurement. These six columns
-- carry the pipe-joined multivalue mv() produces, exactly as
-- weillCornellEduDepartment and weillCornellEduDepartmentCode already do in
-- the same table -- so copy those two columns' declared type and length rather
-- than trusting NVARCHAR(4000). The confirming INFORMATION_SCHEMA query is in
-- this round's measurement list.
ALTER TABLE _person_sor_role_record ADD weillCornellEduOrgUnit           NVARCHAR(4000) NULL;
ALTER TABLE _person_sor_role_record ADD weillCornellEduOrgUnitLevel1     NVARCHAR(4000) NULL;
ALTER TABLE _person_sor_role_record ADD weillCornellEduOrgUnitLevel2     NVARCHAR(4000) NULL;
ALTER TABLE _person_sor_role_record ADD weillCornellEduOrgUnitCode       NVARCHAR(4000) NULL;
ALTER TABLE _person_sor_role_record ADD weillCornellEduOrgUnitCodeLevel1 NVARCHAR(4000) NULL;
ALTER TABLE _person_sor_role_record ADD weillCornellEduOrgUnitCodeLevel2 NVARCHAR(4000) NULL;
"""


def _gtime(row, attr):
    """GeneralizedTime -> "YYYY-MM-DD HH:MM:SS", the shape the SPL's six
    substr() calls produce and the shape createTimestamp already holds in both
    of this slice's tables.

    Not named _ts: the canonical slice carries a helper of that name, the four
    slices are concatenated into one module, and a same-named function defined
    twice would silently give both slices whichever copy landed last. Two short
    functions are cheaper than that class of bug.

    Why this exists at all: _Row.get() runs every value through _flatten(),
    which renders a datetime with isoformat(). ldap3 parses a populated
    GeneralizedTime into a tz-aware datetime, so a bare get() yields
    "2019-07-01T00:00:00+00:00" -- a shape no column in these tables has ever
    held, since the Splunk-era value came from ldapfilter's own rendering. The
    slice-and-swap below reproduces the SPL's output byte for byte, and the
    isdigit() branch handles the case where ldap3 hands back the raw
    "20190701000000Z" string instead (no formatter configured), which [:19]
    alone would pass through as nonsense.

    weillCornellEduStartDate / weillCornellEduEndDate go through this too. The
    SPL never sliced them -- it wrote whatever ldapfilter rendered -- so their
    stored shape is unknown and is listed as a measurement. Writing them in the
    same shape as createTimestamp in the same row is the only choice available
    that is not a third shape.

    The SPL's `eval createTimestamp = max(createTimestamp)` guards against a
    multivalue that RFC 4512 forbids on operational attributes (SINGLE-VALUE,
    NO-USER-MODIFICATION); not reproduced. get() rather than mv() here is
    deliberate and is the one place in this slice where it is: joining two
    timestamps with a pipe would produce a value no date column can store.
    """
    text = row.get(attr)
    if not text:
        return ""
    if len(text) >= 14 and text[:14].isdigit():        # raw "20190701000000Z"
        return "%s-%s-%s %s:%s:%s" % (text[0:4], text[4:6], text[6:8],
                                      text[8:10], text[10:12], text[12:14])
    return text.replace("T", " ", 1)[:19]              # ldap3 datetime -> isoformat


def _assert_target_schema(type_name, out):
    """Read the target table's column list and row count once, before any write.

    Called at the end of each source in this slice, which is inside
    run_sources() and therefore before anything is compared, planned or
    written. It needs db_conn() from the db slice; that resolves at call time
    from module globals, so concatenation order does not matter.

    Two guards, both of which the review found missing:

    1. COLUMNS. Every key a source emits must already exist in the target
       table. Six weillCornellEduOrgUnit* keys do not -- see ORGUNIT_DDL above.
       Without this check the first live run either dies inside the writer on
       an unknown column, or (if the writer intersects the row dict against the
       table's real columns, which _columns_for() effectively does) drops all
       six silently while reporting success -- and the migration they exist to
       measure produces nothing. Comparison is case-folded because SQL Server
       identifiers are case-insensitive and ED returns attribute names
       lowercased regardless of the casing requested.

    2. POPULATION. MIN_ROWS for both of this slice's types is a hand-typed
       round number with no measured provenance: 20000 for sor_role_record --
       the same floor as canonical, although PORT_SPEC.md calls role records
       "the highest-cardinality object in the pipeline ... Every person has one
       SOR record but typically several role records" -- and 100 for
       organization, which is a floor against "returned 12", not against a
       subtree that returns 150 of several thousand org units. So neither floor
       can catch a large partial read.

       No replacement is invented here. The number that would calibrate the
       floor is COUNT(*) on the target table, and this logs it every run next
       to what ED actually returned, so the provenance stops being a guess the
       moment the job runs once. It aborts only on a shortfall larger than
       MAX_DELETE_FRACTION, the module's own declared ceiling -- reused rather
       than replaced with a second hand-typed fraction.

       That abort adds no refusal the run did not already have: a shortfall
       that large makes plan_deletions() abort on the same arithmetic. What it
       closes is --no-delete, which skips plan_deletions entirely and is
       therefore the one path where a 75%-short read is upserted with no signal
       at all. A table that is genuinely empty (a fresh schema) is logged and
       allowed through; MIN_ROWS still applies to it.
    """
    if not out:
        return                       # run_sources' MIN_ROWS floor aborts next

    table = TABLES[type_name]
    # Table names come from TABLES, a module constant, but they are still
    # interpolated into SQL below, so they are validated rather than trusted.
    if not (table.isascii() and table.replace("_", "").isalnum()):
        raise SystemExit("ABORT: unsafe table name %r in TABLES[%r]"
                         % (table, type_name))

    emitted = set()
    for row in out.values():
        emitted.update(row)

    conn = db_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_NAME = %s", (table,))
        actual = {r[0].lower() for r in cur.fetchall()}
        cur.execute("SELECT COUNT(*) FROM [%s]" % table)
        stored = cur.fetchone()[0] or 0
    finally:
        conn.close()

    if not actual:
        raise SystemExit(
            "ABORT: table %s has no columns in INFORMATION_SCHEMA -- it does "
            "not exist, or IA_DB_NAME points at the wrong database. Nothing "
            "has been written. TABLES[%r] is taken from the READ side of the "
            "paired `<X> - DN` searches and is evidence, not proof; confirm it "
            "against conf-db_outputs." % (table, type_name))

    missing = sorted(k for k in emitted if k.lower() not in actual)
    if missing:
        # The six orgUnit columns are the known-missing set and ship with their
        # DDL. Anything else missing means the emitted key set and the table
        # have drifted apart for a reason nobody has written down yet, so the
        # message names the columns and stops rather than suggesting a fix.
        hint = ORGUNIT_DDL if any(
            k.lower().startswith("weillcornelleduorgunit") for k in missing) else ""
        raise SystemExit(
            "ABORT: %s emits %d column(s) that do not exist in %s: %s. Nothing "
            "has been written. Add them before the first live run.%s"
            % (type_name, len(missing), table, ", ".join(missing), hint))

    built = len(out)
    logger.info("schema %-16s %-26s columns=%d ok, ed=%d stored=%d (%.1f%% of stored)",
                type_name, table, len(emitted), built, stored,
                (built / stored * 100) if stored else 0.0)
    if not stored:
        logger.warning("%s is empty -- MIN_ROWS[%r]=%d is the only floor on "
                       "this source, and it was not calibrated from a count",
                       table, type_name, MIN_ROWS.get(type_name, 1))
        return
    shortfall = (stored - built) / stored
    if shortfall > MAX_DELETE_FRACTION:
        raise SystemExit(
            "ABORT: ED returned %d %s rows against %d stored in %s -- %.1f%% "
            "short, above the %.1f%% ceiling. MIN_ROWS[%r]=%d did not catch it "
            "because that floor is a hand-typed round number, not a measured "
            "population. Either the read was truncated, or %d is the real "
            "population and MIN_ROWS should be set from it. Nothing has been "
            "written. (plan_deletions() aborts on the same arithmetic; this "
            "check exists so --no-delete cannot skip past it.)"
            % (built, type_name, stored, table, shortfall * 100,
               MAX_DELETE_FRACTION * 100, type_name,
               MIN_ROWS.get(type_name, 1), built))


@source("sor_role_record", alias="ed-sors")
def ed_sor_role_record():
    """"Identity Authority - SOR role record" -- every role record under ou=sors.

    The largest ED read in this job, and the SPL runs it UNPAGED with attrs="*"
    and no time window. Unpaged is the dangerous half: the server caps the
    result set silently, so a truncated run and a complete run look identical
    to everything downstream. ldap_search() pages. The attribute list is also
    spelled out rather than "*", partly so a schema addition in ED cannot
    quietly widen the read, and partly because the subtyped keys below have to
    be named explicitly to come back.

    DN_FROM_ENTRY. The SPL reconstructs the DN:

        eval dn = case(ou = "affiliates", "uid=" . uid . "," . seeAlso,
                       1=1, "uid=" . uid . ",ou=" . ou . ",ou=sors,...")

    Not ported. `ou` is multi-valued on these entries, so the second branch
    concatenates a multivalue into the key -- Splunk expands that into one row
    per ou, and `_key = dn` then makes the lookup key depend on which ou came
    back first. row.entry_dn is the DN the server actually holds: correct, and
    stable across runs, which is what a deletion set difference needs.
    assert_dn_overlap() is what proves the stored keys agree with it, before
    any write and independently of --no-delete, so the re-key cannot reach the
    upsert as a silent duplicate-insert.

    ACCESSORS. Everything except the four date-typed columns and the two
    derived ones goes through mv(). That is not caution for its own sake:
    "|".join(["x"]) == "x", so mv() is byte-identical to get() on genuinely
    single-valued data and strictly safer on anything else -- and this search
    is where the SPL's own `nomv` list is least trustworthy, since it runs
    attrs="*" and nomv's only o, weillCornellEduProgram and
    weillCornellEduProgramCode while `stats`-free Splunk still carried every
    other attribute's full multivalue through to the lookup. row.get() here
    would write one value into a column whose consumers expect the joined set,
    with no row-count change to expose it -- the same shape as the bug that
    emptied 15 person-type flags on the sibling port.
    """
    rows = ldap_search(
        "ed-sors",
        "(&(objectClass=weillCornellEduSORRoleRecord))",
        ["weillCornellEduCWID", "uid", "ou", "weillCornellEduSORID",
         "weillCornellEduExitReason", "weillCornellEduStatus", "o", "title",
         "weillCornellEduTitleCode", "weillCornellEduRole",
         "weillCornellEduRoleCode", "weillCornellEduStartDate",
         "weillCornellEduEndDate", "weillCornellEduExpectedGradYear",
         "weillCornellEduDepartment", "weillCornellEduDepartmentCode",
         "weillCornellEduProgram", "weillCornellEduProgramCode",
         "weillCornellEduFTE", "weillCornellEduPrimaryEntry",
         "weillCornellEduType", "weillCornellEduDegree",
         "weillCornellEduDegreeCode", "seeAlso",
         # Subtyped keys. The SPL needs a whole extra saved search ("Company",
         # "Manager") plus inline `spath input=_raw path="o;company{}"` to get
         # at these, because Splunk's ldapsearch flattens the entry to JSON and
         # loses the options. ldap3 returns them as ordinary attribute keys, so
         # they are simply requested and read.
         "manager", "manager;manager", "manager;requester", "manager;sponsor",
         "o;company",
         # orgUnit / orgUnitCode: ED is migrating department/departmentCode to
         # these, and it tags them ;level1 (parent org) and ;level2 (the actual
         # unit). Requested and carried as their own columns -- deliberately
         # NOT coalesced into weillCornellEduDepartment yet, so the migration
         # can be measured against the old columns in the same table before
         # anything switches over. When someone does want the flattened value,
         # the rule is buildIdentity.py's _dept_value(): L2 or L1 or
         # department, never L1 alone -- Library sits at L2 under an L1 of
         # "Information Technologies and Services", so L1 alone files every
         # librarian under ITS. Role records are the right place for this:
         # they carry the hierarchy that SOR records mostly lack (L2 on ~37% of
         # expired faculty role records against 3.4% of faculty SOR records).
         # These six columns DO NOT EXIST in the target table yet; ORGUNIT_DDL
         # above adds them and _assert_target_schema() below refuses to run
         # until they are there.
         "weillCornellEduOrgUnit", "weillCornellEduOrgUnit;level1",
         "weillCornellEduOrgUnit;level2",
         "weillCornellEduOrgUnitCode", "weillCornellEduOrgUnitCode;level1",
         "weillCornellEduOrgUnitCode;level2",
         "createTimestamp", "modifyTimestamp"])

    out = {}
    for row in rows:
        # weillCornellEduPrimaryEntry: the SPL is
        #     case(weillCornellEduPrimaryEntry = true,  "true",
        #          weillCornellEduPrimaryEntry = "1",   "true",
        #          1=1,                                 "false")
        # and a bare `true` in a Splunk eval is a FIELD REFERENCE, not a
        # boolean. No field named `true` exists, so branch one can never match,
        # and ED spells the value "TRUE" rather than "1" -- which makes this
        # column the literal string "false" on every row in the table today.
        # PORT_SPEC.md carries the confirming query (it is in this round's
        # measurement list). Implemented here as the intent rather than the
        # behaviour, so expect real rows to flip to "true" on cutover: that is
        # the bug being fixed, not a regression.
        # row.all() because a membership test must see every value.
        primary = "true" if any(
            v.strip().lower() in ("true", "1")
            for v in row.all("weillCornellEduPrimaryEntry")) else "false"

        # SPL: `eval manager = max(manager, manager1)`, manager1 being the
        # spath of manager;manager{}. Splunk's max() ignores nulls, so in
        # practice this is a coalesce; where both are set it keeps the lexically
        # larger, which is not a rule anyone chose. Ported as the coalesce, and
        # joined -- both keys are multi-valued in ED and the SPL never nomv'd
        # either, so it is dropping values today.
        manager = mv(row, "manager") or mv(row, "manager;manager")

        out[row.entry_dn] = {
            "weillCornellEduCWID": mv(row, "weillCornellEduCWID"),
            # uid carries no SINGLE-VALUE in RFC 4519, and neither the SPL nor
            # ED guarantees one here.
            "uid": mv(row, "uid"),
            # ou is multi-valued here -- it is what makes the SPL's dn case()
            # unstable. Joined rather than first-value-wins.
            "ou": mv(row, "ou"),
            "weillCornellEduSORID": mv(row, "weillCornellEduSORID"),
            "weillCornellEduExitReason": mv(row, "weillCornellEduExitReason"),
            "weillCornellEduStatus": mv(row, "weillCornellEduStatus"),
            "o": mv(row, "o"),                       # SPL nomv
            # title carries no SINGLE-VALUE in RFC 4519, and the SPL nomv's it
            # in the sibling SOR record search -- i.e. its own author knew.
            "title": mv(row, "title"),
            "weillCornellEduTitleCode": mv(row, "weillCornellEduTitleCode"),
            "weillCornellEduRole": mv(row, "weillCornellEduRole"),
            "weillCornellEduRoleCode": mv(row, "weillCornellEduRoleCode"),
            # Date-typed: see _gtime. get(), not mv() -- a pipe-joined pair of
            # timestamps is not a value any date column can hold.
            "weillCornellEduStartDate": _gtime(row, "weillCornellEduStartDate"),
            "weillCornellEduEndDate": _gtime(row, "weillCornellEduEndDate"),
            # SPL: eval weillCornellEduExpectedGradYear = max(...). Genuinely
            # multi-valued on some records and the SPL takes the largest, not
            # the first, so this is max() over every value rather than mv().
            "weillCornellEduExpectedGradYear": max(
                row.all("weillCornellEduExpectedGradYear"), default=""),
            "weillCornellEduDepartment": mv(row, "weillCornellEduDepartment"),
            "weillCornellEduDepartmentCode":
                mv(row, "weillCornellEduDepartmentCode"),
            "weillCornellEduProgram": mv(row, "weillCornellEduProgram"),          # SPL nomv
            "weillCornellEduProgramCode": mv(row, "weillCornellEduProgramCode"),  # SPL nomv
            "weillCornellEduFTE": mv(row, "weillCornellEduFTE"),
            "weillCornellEduPrimaryEntry": primary,
            "weillCornellEduType": mv(row, "weillCornellEduType"),
            "weillCornellEduDegree": mv(row, "weillCornellEduDegree"),
            "weillCornellEduDegreeCode": mv(row, "weillCornellEduDegreeCode"),
            # seeAlso is multi-valued in LDAP. The SPL leaves it mv, which is
            # also why its "affiliates" dn branch could emit several DNs for one
            # entry. Joined here; the DN itself no longer depends on it.
            "seeAlso": mv(row, "seeAlso"),
            "manager": manager,
            # manager;requester and manager;sponsor are arrays in the SPL's own
            # spath (`{}`), so they are joined rather than read with get().
            "requester": mv(row, "manager;requester"),
            "sponsor": mv(row, "manager;sponsor"),
            # o;company is the subtyped sibling of o, which the SPL does nomv;
            # treated the same rather than silently keeping one value.
            "company": mv(row, "o;company"),
            # New columns, carried not coalesced. The ";level1"/";level2" LDAP
            # options are spelled Level1/Level2 in the column name because a
            # ";" in a column name is a fight with SQL for no gain. All six go
            # through mv(): nothing in ED or the SPL says a subtyped orgUnit is
            # single-valued, and mv() cannot lose a value that get() would.
            "weillCornellEduOrgUnit": mv(row, "weillCornellEduOrgUnit"),
            "weillCornellEduOrgUnitLevel1": mv(row, "weillCornellEduOrgUnit;level1"),
            "weillCornellEduOrgUnitLevel2": mv(row, "weillCornellEduOrgUnit;level2"),
            "weillCornellEduOrgUnitCode": mv(row, "weillCornellEduOrgUnitCode"),
            "weillCornellEduOrgUnitCodeLevel1":
                mv(row, "weillCornellEduOrgUnitCode;level1"),
            "weillCornellEduOrgUnitCodeLevel2":
                mv(row, "weillCornellEduOrgUnitCode;level2"),
            "createTimestamp": _gtime(row, "createTimestamp"),
            "modifyTimestamp": _gtime(row, "modifyTimestamp"),
        }
    # Dropped from the SPL, deliberately:
    #   * the paired "db update" search's 2-day modifyTimestamp window. This is
    #     a full snapshot -- see READS ARE FULL SNAPSHOTS above. Deletion cannot
    #     be decided from a window.
    #   * `where isnotnull(dn)`, which existed because the case() above could
    #     produce a null DN when uid or ou was missing. entry_dn is never null.
    _assert_target_schema("sor_role_record", out)
    return out


@source("organization", alias="ed-groups")
def ed_organization():
    """"Identity Authority - Organization" -- weillCornellEduOrgUnit group entries.

    Runs unpaged in the SPL, like the role-record search; ldap_search() pages.

    The alias is ed-groups, which is in UNRESOLVED_ALIASES: BASE_DN takes it
    from the SPL's own `,ou=departments,ou=Groups,...` reconstruction, which
    names the branch these entries live in, but nothing has proved it against
    live ED. Passing alias= to @source is what makes main()'s unresolved-base
    warning fire for this source at all -- the earlier version intersected
    UNRESOLVED_ALIASES with type names, a disjoint vocabulary, so the warning
    was unreachable. Run --spike before a live run regardless.

    DN_FROM_ENTRY, and this is the reconstruction most likely to have been
    writing wrong DNs for years:

        eval dn = case(isnotnull(seeAlso), "cn=" . cn . "," . seeAlso,
                       isnotnull(o),  "cn=" . cn . ",ou=departments,ou=Groups,...",
                       1=1,           "cn=" . cn . ",ou=departments,ou=Groups,...")

    Branches two and three are byte-identical, so the isnotnull(o) test decides
    nothing -- it is dead weight, and its presence suggests nobody has looked at
    this eval in a long time. Branch one is the live risk: it assumes seeAlso
    holds the entry's PARENT container DN. If seeAlso points anywhere else --
    a peer org, a person, a URL -- _organization holds DNs that match no entry
    in ED. All of it is deleted here in favour of row.entry_dn, and
    assert_dn_overlap() is what decides whether the stored keys agree, before
    any write: this is the type it was written for.

    The SPL's `dedup dn` is dropped with it. cn and seeAlso are both
    multi-valued, so its reconstructed key collided across siblings -- an org
    with two cn values produced two rows, and dedup kept whichever Splunk
    ordered first. Keyed on entry_dn, siblings are distinct entries and there is
    nothing to dedup.
    """
    rows = ldap_search(
        "ed-groups",
        "(&(objectClass=weillCornellEduOrgUnit))",
        ["weillCornellEduSORID", "weillCornellEduDepartment",
         "weillCornellEduDepartment;academic", "seeAlso", "uid", "objectClass",
         "cn", "memberURL", "weillCornellEduCWID", "weillCornellEduCWID;da",
         "weillCornellEduCWID;dd", "weillCornellEduCWID;iamdela",
         "weillCornellEduReleaseCode", "weillCornellEduSource",
         "weillCornellEduSubtype", "weillCornellEduType", "weillCornellEduAlias",
         "weillCornellEduStatus", "telephoneNumber", "labeledURI",
         "createTimestamp", "modifyTimestamp", "o"])

    out = {}
    for row in rows:
        out[row.entry_dn] = {
            "weillCornellEduSORID": mv(row, "weillCornellEduSORID"),
            "weillCornellEduDepartment": mv(row, "weillCornellEduDepartment"),
            # weillCornellEduDepartment;academic -- read as a subtyped key
            # instead of the SPL's `rename weillCornellEduDepartment;academic{}`.
            # The `{}` is Splunk telling us it is an array, so it is joined.
            "weillCornellEduDepartmentAcademic":
                mv(row, "weillCornellEduDepartment;academic"),
            # cn, uid, seeAlso and o are multi-valued in ED but the SPL only
            # nomv's some of its multi-values, so these reach the CSV lookup as
            # Splunk mv fields and land in the DB however its writer flattens
            # them. Joining them keeps every value, which is the point of mv().
            "seeAlso": mv(row, "seeAlso"),
            "uid": mv(row, "uid"),
            "cn": mv(row, "cn"),
            "objectClass": mv(row, "objectClass"),           # SPL nomv
            "memberURL": mv(row, "memberURL"),               # SPL nomv
            # The three weillCornellEduCWID subtypes are the org's people:
            # ;da and ;dd departmental administrators/directors, ;iamdela the
            # IAM delegates. All three are nomv'd in the SPL.
            "da": mv(row, "weillCornellEduCWID;da"),
            "dd": mv(row, "weillCornellEduCWID;dd"),
            "iamdela": mv(row, "weillCornellEduCWID;iamdela"),
            "weillCornellEduReleaseCode": mv(row, "weillCornellEduReleaseCode"),  # SPL nomv
            "weillCornellEduSource": mv(row, "weillCornellEduSource"),
            "weillCornellEduSubtype": mv(row, "weillCornellEduSubtype"),
            "weillCornellEduType": mv(row, "weillCornellEduType"),
            "weillCornellEduAlias": mv(row, "weillCornellEduAlias"),             # SPL nomv
            "weillCornellEduStatus": mv(row, "weillCornellEduStatus"),
            # telephoneNumber (RFC 4519) and labeledURI (RFC 2079) both carry
            # no SINGLE-VALUE, neither is nomv'd in this search, and Splunk
            # therefore pushes every value today. row.get() would keep only the
            # first: an org unit with a main line and a fax, or with both a
            # department site and a directory URL, would silently lose the
            # second on every run, with no row-count change to catch it.
            "telephoneNumber": mv(row, "telephoneNumber"),
            "labeledURI": mv(row, "labeledURI"),
            "o": mv(row, "o"),
            "createTimestamp": _gtime(row, "createTimestamp"),
            "modifyTimestamp": _gtime(row, "modifyTimestamp"),
        }
    # Dropped: the "Organization, db update" 2-day modifyTimestamp window and
    # its `where isnotnull(dn)`, for the same reasons as the role-record source.
    # Not ported here: "Organization roles", which reads the same entries from
    # the bare `ed` alias to build a CWID-per-org roles table. That is a second
    # table (_organization_roles) and belongs in its own source.
    #
    # Every key above is one the SPL's own `table` clause already names, so this
    # check should pass for organization on day one. It runs anyway: if it does
    # not pass, TABLES["organization"] is wrong, and that is worth knowing
    # before a MERGE rather than during one.
    _assert_target_schema("organization", out)
    return out


# ==========================================================================
# SLICE: contact | verdict NEEDS_FIXES
# ==========================================================================
# ==========================================================================
# SLICE: contact (types: email, phone, location, location_master)
# ==========================================================================
# Four full-snapshot reads: three person-contact feeds under ou=contacts, and
# the building/room master under ou=locations,ou=Groups.
#
# BASE DNs. Nothing below reconstructs or hard-codes a base; each function names
# its alias and lets BASE_DN resolve it. That matters most for the two feeds
# whose names collide. `ed-locations` is ou=locations,ou=contacts -- the
# PERSON-to-location feed, per the SPL's own reconstruction at L296. The
# location MASTER is a different branch read through the bare `ed` alias,
# ou=locations,ou=Groups, per L331. An earlier draft pointed ed-locations at the
# Groups branch, which binds and returns rows for (objectClass=*) while matching
# nothing for (weillCornellEduType=location); _contact_search() below reports
# that as a wrong base instead of letting it surface as an ED outage.
#
# KEYS. Every row is keyed on row.entry_dn, the real DN from ldap3, replacing
# four `eval dn = "uid=" . uid . ",ou=...` reconstructions (L228, L296, L331,
# L520). The stored `dn` columns in _contact_email / _contact_phone /
# _contact_location / _location are those reconstructions, so the two key spaces
# have to be proved equal before a write: assert_dn_overlap() does exactly that,
# before any write and independently of --no-delete, and aborts below 90%. It is
# not re-implemented here, and the keying is not reverted to a string rebuild --
# reverting would carry the L520 defect (one key per uid, ignoring type) forward.
#
# ACCESSORS. mv() for every directory attribute; row.get() only for
# createTimestamp/modifyTimestamp, which RFC 4512 defines as SINGLE-VALUE. Note
# what mv() does and does not change: only weillCornellEduReleaseCode and ou are
# nomv'd in these searches (L222-225, L294-299, L514-517), so only those two are
# pipe-joined in the tables today -- but `nomv` chooses the SEPARATOR, it does
# not decide whether a set reaches the column. Every other multi-valued
# attribute has been arriving newline-joined all along. So mv() is a declared
# separator change (newline -> pipe) on those columns and nothing more, while
# row.get() would have been the value change: the first value written into a
# column whose consumers hold the set, with no row-count change to expose it.
# Sizing that separator change is one of the measurements filed with this slice.
#
# WINDOW. Email, Phone and Location each carry (modifyTimestamp>=now-3d) plus
# `outputlookup append=t`, which is what makes source='ed' mean "seen at some
# point since the lookup was last truncated" rather than "currently in ED". All
# four read in full here. Dropping the window is not an optimisation; it is what
# makes the deletion set difference sound.


def _contact_ts(row, attr):
    """createTimestamp / modifyTimestamp as "YYYY-MM-DD HH:MM:SS".

    That is the whole of the SPL's substr() chain: its offsets -- (1,4), (6,2),
    (9,2), (12,2), (15,2), (18,2) -- only line up if Splunk has already been
    handed "2026-09-05T12:34:56Z", so the chain swaps the "T" for a space and
    drops the zone. ldap3 parses GeneralizedTime into a datetime and _Row.get()
    normalises it through _flatten()'s isoformat(), so [:19] cuts the offset and
    the swap is the entire port. A directory that hands back the raw
    "20260905123456Z" string instead is reassembled explicitly rather than
    sliced into a wrong-shaped string.

    Location (L300-301) and Location Master (L333-334) stop at minute precision.
    Keeping seconds for all four is safe and checked, not assumed: the only
    consumers that parse those two feeds with "%Y-%m-%d %H:%M" are L361 and
    L368, both `db update` searches this port deletes outright.

    Operational attributes are SINGLE-VALUE (RFC 4512), so get() is correct
    here -- this is the one place in the slice where it is.
    """
    value = row.get(attr)
    if not value:
        return ""
    if len(value) >= 14 and value[:14].isdigit():        # raw "20260905123456Z"
        return "%s-%s-%s %s:%s:%s" % (value[0:4], value[4:6], value[6:8],
                                      value[8:10], value[10:12], value[12:14])
    return value[:19].replace("T", " ")


def _contact_search(alias, search_filter, attrs):
    """ldap_search plus the one diagnostic these four aliases still need.

    The bases in BASE_DN are read out of the SPL's own dn reconstructions rather
    than guessed, but none of the four used here has been proved against live
    ED, so all four remain in UNRESOLVED_ALIASES. A base that binds to the wrong
    branch does not error: it returns zero entries for the source's real filter,
    run_sources() then aborts on the row floor, and the operator reads that as
    an ED outage. Name the actual suspect instead. Counts only; no attribute
    value is ever logged.
    """
    if alias in UNRESOLVED_ALIASES:
        logger.warning("alias %s reads an unproved candidate base: %s "
                       "-- confirm with --spike", alias, BASE_DN[alias])
    rows = ldap_search(alias, search_filter, attrs)
    if not rows:
        raise SystemExit(
            "ABORT: alias %s returned zero entries from %s for filter %s. A "
            "base DN that binds to the wrong branch reads exactly like this -- "
            "ou=locations,ou=contacts (person locations) and "
            "ou=locations,ou=Groups (location master) are different branches "
            "with similar names. Confirm the base with --spike before treating "
            "this as an ED outage. Nothing has been written."
            % (alias, BASE_DN[alias], " ".join(search_filter.split())))
    return rows


@source("email", alias="ed-emails")
def ed_emails_contact():
    """Contact email entries, keyed on the real entry DN.

    Dropped from the SPL: `eval dn = "uid=" . uid . ",ou=emails,ou=contacts,
    ..."` (L228) -- row.entry_dn is the real thing -- and the duplicate request
    for weillCornellEduSubtype in the attrs list, which asks for it twice.

    The emitted key set is exactly the SPL's `table` clause at L229, so no
    column is added or dropped here.

    weillCornellEduPrimaryEntry is emitted raw, as the SPL stores it.
    PORT_SPEC.md L351-354 proposes normalising it to a real boolean for
    sor_role_record, where a Splunk parse bug makes it uniformly "false", and
    closes with "Also decide whether email/phone/location should get the same
    normalisation, since they currently store the raw value" -- i.e. an open
    decision, not a settled one. Normalising three tables on the way past would
    change values no finding asked to change. Reading it through mv() is
    separate and is not optional: PORT_SPEC.md L353 notes the attribute may be
    multi-valued, and get() on a multi-valued flag is the accessor bug this
    module exists to remove.
    """
    rows = _contact_search(
        "ed-emails",
        "(objectClass=weillCornellEduContactEmail)",
        ["uid", "weillCornellEduCWID", "weillCornellEduPrimaryEntry", "mail",
         "ou", "weillCornellEduReleaseCode", "weillCornellEduSource",
         "weillCornellEduSubtype", "weillCornellEduType",
         "createTimestamp", "modifyTimestamp"])
    return {r.entry_dn: {
        "uid": mv(r, "uid"),
        "weillCornellEduCWID": mv(r, "weillCornellEduCWID"),
        "weillCornellEduPrimaryEntry": mv(r, "weillCornellEduPrimaryEntry"),
        # RFC 4524 mail carries no SINGLE-VALUE. A second address is exactly the
        # kind of value that reaches the column today (newline-joined) and would
        # vanish under get().
        "mail": mv(r, "mail"),
        "ou": mv(r, "ou"),                          # nomv'd in the SPL (L224)
        # nomv'd in the SPL (L222). OPEN QUESTION, unchanged from the draft: ED
        # stores release codes subtyped (weillCornellEduReleaseCode;person,
        # ;picture) and an unqualified request returns the subtyped descriptions
        # under their own ldap3 keys, so this plain read may be empty here and
        # may equally have been empty in Splunk. No contact search runs `spath`,
        # so the export cannot answer it. The measurement is filed with this
        # slice; do not "fix" it by guessing subtype names.
        "weillCornellEduReleaseCode": mv(r, "weillCornellEduReleaseCode"),
        "weillCornellEduSource": mv(r, "weillCornellEduSource"),
        "weillCornellEduSubtype": mv(r, "weillCornellEduSubtype"),
        # Proven multi-valued by the phone value-set trap (PORT_SPEC.md L343).
        "weillCornellEduType": mv(r, "weillCornellEduType"),
        "createTimestamp": _contact_ts(r, "createTimestamp"),
        "modifyTimestamp": _contact_ts(r, "modifyTimestamp"),
    } for r in rows if r.entry_dn}


@source("phone", alias="ed-phones")
def ed_phones_contact():
    """Desk and mobile contact entries in ONE paged search, in TWO columns.

    THE MERGE IS THE BUG, AND IT IS NOT REPRODUCED. The SPL runs two branches
    (L512, L513), the second renaming `mobile` to `telephoneNumber` so both feed
    one column, and then keys every row on `uid` alone (L520), ignoring type.
    PORT_SPEC.md L343 establishes what that costs: weillCornellEduType is
    multi-valued and is compared against a value set, so an entry carrying both
    types is returned by BOTH arms, the two arms produce two rows with the same
    _key, and `outputlookup append=t` keeps whichever wrote last -- the desk
    number and the mobile number of the same person overwrite each other,
    non-deterministically, on every run.

    So the two attributes are emitted as their own columns and never coalesced.
    ONE premise, stated once: it does not matter whether ED puts the desk number
    and the mobile on one entry or on two, because the port is lossless under
    both shapes. One entry -> one DN, one row, both columns populated. Two
    entries -> two DNs, two rows, one column each. `r.get("telephoneNumber") or
    r.get("mobile")` was lossless under neither: it writes the desk number and
    discards the mobile with no row-count change, so the extra rows this feed is
    expected to produce could not have revealed it. (An earlier divergence note
    claimed entry_dn by itself splits the two; it does not, under the one-entry
    shape, and that claim is withdrawn.)

    DDL PREREQUISITE, NOT OPTIONAL. `mobile` is a new column: the SPL's table
    clause (L521) names telephoneNumber only, because the rename collapsed them.
    `ALTER TABLE _contact_phone ADD mobile NVARCHAR(256) NULL;` (width to match
    telephoneNumber) must land before the first live write, or the writer either
    fails on an unknown column or drops it silently. The column list query is
    filed with this slice's measurements; enforcing it belongs in the writer,
    which is not this slice.

    Both numbers also go through mv(): RFC 4519 gives neither telephoneNumber
    nor mobile SINGLE-VALUE, so a desk line plus a fax is one entry with two
    values, and get() would take the first.

    The SPL's mobile branch is `append [ | ldapfilter ... ]` -- a subsearch whose
    first command is ldapfilter, a STREAMING command given no input events -- so
    it may always have returned nothing, and is capped by the subsearch row/time
    limit even when it does not (PORT_SPEC.md L246-249). One paged search over
    the branch has no append, no subsearch and no cap. MORE ROWS HERE ARE
    EXPECTED AND CORRECT; do not tune it back.
    """
    rows = _contact_search(
        "ed-phones",
        """(|(weillCornellEduType=telephoneNumber)
             (weillCornellEduType=mobile))""",
        ["uid", "telephoneNumber", "mobile", "weillCornellEduCWID",
         "weillCornellEduPrimaryEntry", "weillCornellEduReleaseCode",
         "weillCornellEduSource", "weillCornellEduSubtype",
         "weillCornellEduType", "createTimestamp", "modifyTimestamp", "ou"])

    # PORT_SPEC.md L249 requires the two populations be counted separately and
    # either being zero to abort -- an OR'd filter would otherwise hide a dead
    # half behind a healthy total. `both` is the measurement the review asked
    # for (does one entry carry both types?), taken from this same read instead
    # of guessed: it is the count that says whether the SPL has been losing a
    # number per person, and it is the reason the two columns exist. Counts
    # only -- no number, and no attribute value of any kind, is ever logged.
    desk = sum(1 for r in rows if r.all("telephoneNumber"))
    cell = sum(1 for r in rows if r.all("mobile"))
    both = sum(1 for r in rows if r.all("telephoneNumber") and r.all("mobile"))
    logger.info("ldap ed-phones entries=%d with-telephoneNumber=%d "
                "with-mobile=%d carrying-both=%d", len(rows), desk, cell, both)
    if not desk or not cell:
        raise SystemExit(
            "ABORT: ed-phones returned %d entries but %d carry a "
            "telephoneNumber and %d carry a mobile. One half of this feed is "
            "empty, which is what a changed weillCornellEduType value, a wrong "
            "base DN or a half-populated replica looks like. PORT_SPEC.md L249 "
            "requires both halves to be non-zero before this source is "
            "trusted. Nothing has been written."
            % (len(rows), desk, cell))

    return {r.entry_dn: {
        "uid": mv(r, "uid"),
        # Two attributes, two columns, never coalesced. See the docstring.
        "telephoneNumber": mv(r, "telephoneNumber"),
        "mobile": mv(r, "mobile"),
        "weillCornellEduCWID": mv(r, "weillCornellEduCWID"),
        "weillCornellEduPrimaryEntry": mv(r, "weillCornellEduPrimaryEntry"),
        "weillCornellEduReleaseCode": mv(r, "weillCornellEduReleaseCode"),
        "weillCornellEduSource": mv(r, "weillCornellEduSource"),
        "weillCornellEduSubtype": mv(r, "weillCornellEduSubtype"),
        # Kept, and kept pipe-joined: with both numbers now in their own columns
        # this is what still distinguishes a desk-only entry from a mobile-only
        # one from an entry that is both. get() here would report every dual
        # entry as whichever type ED happens to return first.
        "weillCornellEduType": mv(r, "weillCornellEduType"),
        "createTimestamp": _contact_ts(r, "createTimestamp"),
        "modifyTimestamp": _contact_ts(r, "modifyTimestamp"),
        "ou": mv(r, "ou"),                          # nomv'd in the SPL (L516)
    } for r in rows if r.entry_dn}


@source("location", alias="ed-locations")
def ed_locations_contact():
    """Person-to-location contact entries, keyed on the real entry DN.

    Alias ed-locations resolves to ou=locations,ou=contacts (SPL L296). This is
    NOT the location master -- that is ou=locations,ou=Groups, read below
    through the bare `ed` alias. Conflating the two is the failure this slice's
    base DNs were corrected for; neither branch is named in this function.

    Two SPL lines dropped, both lossy:
      * `eval dn = "uid=" . uid . ",ou=locations,ou=contacts,..."` (L296) --
        row.entry_dn is the real DN.
      * `dedup uid` (L297) -- free-looking against a uid-derived dn, but a
        person with two location entries has two real DNs and dedup threw the
        second away. Keying on entry_dn keeps both, so this feed should return
        MORE rows than Splunk. That is correct; do not add a dedup back.

    Correction against the task brief, confirmed in PORT_SPEC.md L433: this
    search carries no postalCode/street/l/st. Those are Location Master's.

    The emitted key set is exactly the SPL's `table` clause at L304.
    """
    rows = _contact_search(
        "ed-locations",
        "(weillCornellEduType=location)",
        ["ou", "uid", "roomNumber", "weillCornellEduLocationCode",
         "weillCornellEduCWID", "weillCornellEduPrimaryEntry",
         "weillCornellEduReleaseCode", "weillCornellEduSource",
         "weillCornellEduSubtype", "weillCornellEduType",
         "createTimestamp", "modifyTimestamp"])
    return {r.entry_dn: {
        "ou": mv(r, "ou"),                          # nomv'd in the SPL (L298)
        "uid": mv(r, "uid"),
        # RFC 4524 roomNumber carries no SINGLE-VALUE.
        "roomNumber": mv(r, "roomNumber"),
        "weillCornellEduLocationCode": mv(r, "weillCornellEduLocationCode"),
        "weillCornellEduCWID": mv(r, "weillCornellEduCWID"),
        "weillCornellEduPrimaryEntry": mv(r, "weillCornellEduPrimaryEntry"),
        # Same open question as email -- see ed_emails_contact().
        "weillCornellEduReleaseCode": mv(r, "weillCornellEduReleaseCode"),
        "weillCornellEduSource": mv(r, "weillCornellEduSource"),
        "weillCornellEduSubtype": mv(r, "weillCornellEduSubtype"),
        "weillCornellEduType": mv(r, "weillCornellEduType"),
        # The SPL wrapped both timestamps in max() (L302-303), a no-op over a
        # single value that existed only because its substr() chain could fan
        # out over a multivalue. Not ported.
        "createTimestamp": _contact_ts(r, "createTimestamp"),
        "modifyTimestamp": _contact_ts(r, "modifyTimestamp"),
    } for r in rows if r.entry_dn}


@source("location_master", alias="ed")
def ed_location_master():
    """The building/room master list, from the bare `ed` alias at
    ou=locations,ou=Groups (SPL L331).

    THE MOST DANGEROUS FEED IN THE JOB. `outputlookup
    identity_authority_location_master` at L337 has no append=t and
    override_if_empty defaults to true, so an ldapsearch returning zero rows
    REPLACES the master list with nothing; `Location Master - DN from ED` (L349)
    then contributes no DNs, every row of _location reads as 'db only', and the
    whole table is nominated for deletion from one bad LDAP call, with nothing
    in Splunk capping the volume (PORT_SPEC.md L220, L462). It also runs unpaged
    today, so the directory silently caps it and a capped run replaces the
    master list with the short one. ldap_search() pages.

    WHY THIS SOURCE REQUIRES A MEASURED FLOOR AND NOTHING ELSE DOES.
    MIN_ROWS["location_master"] is 100 -- fifty times below the next floor in
    the dict and, unlike every other floor there, not derived from a population.
    It catches a zero-row read and nothing more, and this is precisely the feed
    where a short-but-non-zero read is destructive rather than merely stale (the
    other three write append=t, so a short read strands rows instead of
    replacing them). No number is invented here to replace it: the floor comes
    from IA_LOCATION_MASTER_MIN_ROWS, which the operator sets from a measured
    COUNT(*), and this source refuses to read until they have. Checked before
    the search so an unmeasured run costs nothing.
    """
    floor = os.environ.get("IA_LOCATION_MASTER_MIN_ROWS", "").strip()
    if not floor.isdigit() or int(floor) < 1:
        raise SystemExit(
            "ABORT: IA_LOCATION_MASTER_MIN_ROWS is unset or not a positive "
            "integer, so this source has no floor derived from its own "
            "population. MIN_ROWS['location_master'] = %d is a placeholder that "
            "only catches a zero-row read, and this is the one feed whose SPL "
            "write (L337, no append=t, override_if_empty defaulting to true) "
            "replaces the master list wholesale -- a read returning 150 of N "
            "entries clears the floor and underwrites %s. Measure it: "
            "SELECT COUNT(*) FROM %s; then export "
            "IA_LOCATION_MASTER_MIN_ROWS=<~80%% of that count>. Setting it to 1 "
            "is the deliberate way to take a first exploratory read; that "
            "choice is logged rather than hidden. Nothing has been written."
            % (MIN_ROWS["location_master"], TABLES["location_master"],
               TABLES["location_master"]))
    floor = int(floor)

    rows = _contact_search(
        "ed",
        "(&(objectClass=groupOfURLs)(ou=locations))",
        ["weillCornellEduLocationCode", "physicalDeliveryOfficeName",
         "postalAddress", "l", "st", "postalCode", "street",
         "createTimestamp", "modifyTimestamp"])

    # Enforced here rather than left to run_sources(), which reads MIN_ROWS
    # after this function returns: the guard on the one destructive feed should
    # not depend on the placeholder floor or on evaluation order.
    logger.info("location master: %d entries, measured floor %d "
                "(MIN_ROWS placeholder %d is not the guard for this source)",
                len(rows), floor, MIN_ROWS["location_master"])
    if floor <= MIN_ROWS["location_master"]:
        logger.warning(
            "IA_LOCATION_MASTER_MIN_ROWS=%d is at or below the un-derived "
            "placeholder of %d -- this run is NOT protected against a short "
            "read of %s. Set it from SELECT COUNT(*) FROM %s.",
            floor, MIN_ROWS["location_master"], TABLES["location_master"],
            TABLES["location_master"])
    if len(rows) < floor:
        raise SystemExit(
            "ABORT: location master returned %d entries, below the measured "
            "floor of %d. This feed is a full replace, so a short read is the "
            "one that underwrites %s wholesale -- treat it as a paging fault, "
            "a changed filter or a partially populated replica, not as a floor "
            "to lower. Nothing has been written."
            % (len(rows), floor, TABLES["location_master"]))

    # Dropped: `eval dn = "cn=" . cn . ",ou=locations,ou=Groups,..."` (L331) and
    # the `dedup dn` after it (L332). cn is multi-valued in LDAP, so that
    # reconstruction can yield a multi-valued dn -- an unstable _key, with dedup
    # then discarding siblings on it (PORT_SPEC.md L368). row.entry_dn is single
    # and real, which is why cn is not even requested here; the SPL wanted it
    # only to build the string. The emitted key set is otherwise exactly the
    # SPL's `table` clause at L336.
    return {r.entry_dn: {
        "weillCornellEduLocationCode": mv(r, "weillCornellEduLocationCode"),
        "physicalDeliveryOfficeName": mv(r, "physicalDeliveryOfficeName"),
        # postalAddress, l, st, postalCode and street are all multi-valued in
        # RFC 4519 -- a two-line street address is one attribute with two
        # values, and get() would keep the first line only.
        "postalAddress": mv(r, "postalAddress"),
        "l": mv(r, "l"),
        "st": mv(r, "st"),
        "postalCode": mv(r, "postalCode"),
        "street": mv(r, "street"),
        "createTimestamp": _contact_ts(r, "createTimestamp"),
        "modifyTimestamp": _contact_ts(r, "modifyTimestamp"),
    } for r in rows if r.entry_dn}

# ==========================================================================
# SLICE: dbside | verdict NEEDS_FIXES
# ==========================================================================
# ==========================================================================
# SLICE: dbside (types: db reads, the writer, main)
# ==========================================================================
# ---------------------------------------------------------------------------
#                                  DATABASE
# ---------------------------------------------------------------------------

# The Identity Authority database is MSSQL: Splunk reached it through the
# `Identity_Authority` DB Connect connection and every `<X> - DN` search is
# plain T-SQL against it. So this side is pymssql, not the pymysql
# buildIdentity.py uses -- the two jobs write different servers, and the upsert
# form differs with them (MERGE here, ON DUPLICATE KEY UPDATE there).
#
# NONE OF THESE FOUR SECRETS EXIST TODAY. reciter-inst-secrets carries
# LDAP_BIND_PASSWORD and the ASMS trio (MSSQL_DB_URL / MSSQL_DB_USERNAME /
# MSSQL_DB_PASSWORD) and nothing else; the Identity Authority credentials live
# only inside Splunk's DB Connect connection object, which this job cannot
# read.
#
# IA_DB_NAME is REQUIRED, not defaulted. The abort message tells the operator
# to build IA_DB_URL from Splunk's Identity_Authority DB Connect connection,
# i.e. to paste a JDBC URL -- and _mssql_target() (buildIdentity.py:324) keeps
# only what precedes the first ';', which is exactly where a JDBC URL puts
# databaseName=. A defaulted database name plus a discarded one is how a full
# snapshot lands in a restore or a dev copy that happens to answer to the
# default, with every log line reporting success.
#
# Deliberately NOT defaulted to the MSSQL_DB_* variables. ASMS is a different
# server; silently writing Identity Authority rows into it would be far worse
# than refusing to start.
IA_DB_ENV = ("IA_DB_URL", "IA_DB_USERNAME", "IA_DB_PASSWORD", "IA_DB_NAME")

# Every table is keyed on the entry DN -- that is what the `<X> - DN` searches
# select and what plan_deletions reconciles on. MERGE requires it to be unique;
# db_dns() warns if the table says otherwise.
KEY_COLUMN = "dn"

# T-SQL caps a VALUES row constructor at 1000 rows and TDS caps a statement at
# ~2100 parameters, so _batch_size() honours both. The parameter cap binds for
# every real type here; MERGE_MAX_ROWS only binds for a type with a single
# value column. Both are kept because both are real server limits.
MERGE_MAX_ROWS = 1000
DELETE_CHUNK = 500

# The soft-delete marker column. NOT VERIFIED against the real schema: the
# `<X> - DN` searches only ever SELECT dn, and the write side goes through the
# opaque IdentityAuthority_dn stanza, so no query in the SPL names this column.
# db_dns() resolves it from INFORMATION_SCHEMA on EVERY run, dry or live, and
# every use of it below is gated on DELETE_MODE == "flag" -- if the name is
# wrong, this job must not read or clear a column it does not own.
DELETE_FLAG_COLUMN = os.environ.get("IA_DELETE_FLAG_COLUMN", "deleted")

# Types whose IA table is allowed to be empty or below its floor, comma
# separated. The opt-in exists for a genuinely new table on its first run;
# without it an empty read is an abort, because "the read failed" and "the
# table is new" are otherwise the same observation.
IA_ALLOW_EMPTY_TABLE = {t.strip() for t in
                        os.environ.get("IA_ALLOW_EMPTY_TABLE", "").split(",")
                        if t.strip()}

# Floor for the DB side of the reconcile, mirroring MIN_ROWS on the ED side.
# NOT MEASURED: no COUNT(*) against the IA tables has been run, so these reuse
# the source floors rather than inventing new numbers. That is sound as a floor
# because every one of these tables is written from the source of the same name
# on every run and deletion is capped at MAX_DELETE_FRACTION, so a populated
# table cannot legitimately sit far below the floor its own source must clear.
# Calibrate from one COUNT(*) per table and replace this line.
DB_MIN_ROWS = dict(MIN_ROWS)

# Types a NOT NULL delete-flag column may be cleared to 0 in. Anything else
# NOT NULL has no known "not deleted" value and aborts rather than guessing.
NUMERIC_FLAG_TYPES = frozenset((
    "bit", "tinyint", "smallint", "int", "bigint",
    "decimal", "numeric", "float", "real", "money", "smallmoney"))


def _dn_key(dn):
    """Canonical comparison key for a DN.

    plan_deletions() and assert_dn_overlap() are Python set operations, which
    are case- and whitespace-sensitive. SQL Server's default collation
    (SQL_Latin1_General_CP1_CI_AS) is neither, and it is what the MERGE's
    `ON tgt.[dn] = src.[dn]` runs under. Without a shared canonical key a DN
    stored as "UID=7,ou=people,..." is UPDATEd by the MERGE (matched
    case-insensitively) and, in the same run, reported gone by the set
    difference and soft-deleted -- one run both refreshing and deleting the
    same live person.

    casefold() folds harder than the collation and strip() removes leading
    space the collation would not ignore, so this errs toward calling two DNs
    equal. On the deletion side that is the safe direction: fewer rows are
    nominated, never more. The original stored string is carried alongside the
    key (db_dns' `originals`) because the DELETE/UPDATE must name the row the
    way the table holds it.
    """
    return (dn or "").strip().casefold()


def _database_name(url):
    """databaseName= / database= out of a JDBC-style IA_DB_URL, or "".

    Verified shape: _mssql_target('jdbc:sqlserver://host:1433;databaseName=X')
    returns ('host', 1433) and throws X away. Parsing it here is what makes the
    disagreement check in db_conn() possible.
    """
    for part in url.split(";")[1:]:
        key, _, value = part.partition("=")
        if key.strip().lower() in ("databasename", "database"):
            return value.strip()
    return ""


def db_conn():
    """Connection to the Identity Authority MSSQL database, target verified.

    autocommit=False is explicit, not decorative: upsert() and apply_deletions()
    both depend on statements landing in a transaction a single failure rolls
    back.

    The target is asserted twice -- IA_DB_URL's own databaseName= against
    IA_DB_NAME before connecting, and DB_NAME() against IA_DB_NAME after -- and
    logged once per connection. Nothing else in this job can tell an operator
    which database a "committed 33,000 rows" line landed in.
    """
    import pymssql  # lazy: --demo and --spike must run with no driver installed

    missing = [k for k in IA_DB_ENV if not os.environ.get(k)]
    if missing:
        raise SystemExit(
            "ABORT: %s not set. These are NEW secrets -- reciter-inst-secrets "
            "has only LDAP_BIND_PASSWORD and the ASMS MSSQL credentials today. "
            "Create IA_DB_URL, IA_DB_USERNAME, IA_DB_PASSWORD and IA_DB_NAME "
            "from Splunk's Identity_Authority DB Connect connection before "
            "running live. IA_DB_NAME is required and has no default: the URL "
            "form the connection object uses carries the database name after a "
            "';', and that part is discarded when the host is parsed. Do NOT "
            "point these at MSSQL_DB_* -- that is ASMS."
            % ", ".join(missing))

    url = os.environ["IA_DB_URL"]
    wanted = os.environ["IA_DB_NAME"].strip()
    in_url = _database_name(url)
    if in_url and in_url.lower() != wanted.lower():
        raise SystemExit(
            "ABORT: IA_DB_URL names database %r but IA_DB_NAME is %r. The URL's "
            "database name is discarded when the host is parsed, so continuing "
            "would connect to %r while the operator believes they configured "
            "%r. Fix one of the two. Nothing has been written."
            % (in_url, wanted, wanted, in_url))

    host, port = _mssql_target(url)
    conn = pymssql.connect(
        server=host, port=port,
        user=os.environ["IA_DB_USERNAME"],
        password=os.environ["IA_DB_PASSWORD"],
        database=wanted,
        autocommit=False,
        login_timeout=30,
        timeout=600,
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT DB_NAME(), @@SERVERNAME, SUSER_SNAME()")
        actual_db, server_name, login = cur.fetchone()
        logger.info("ia db target: database=%s server=%s login=%s (%s:%d)",
                    actual_db, server_name, login, host, port)
        if (actual_db or "").strip().lower() != wanted.lower():
            raise SystemExit(
                "ABORT: connected to database %r but IA_DB_NAME is %r. Nothing "
                "has been written." % (actual_db, wanted))
    except BaseException:
        conn.close()
        raise
    return conn


def _ident(name):
    """Bracket-quote a T-SQL identifier, refusing anything that is not one.

    Table names come from TABLES and column names from the source dicts, so a
    typo reaches the server as SQL unless something stops it here.
    """
    ok = (name and name.isascii()
          and (name[0].isalpha() or name[0] == "_")
          and all(c.isalnum() or c == "_" for c in name))
    if not ok:
        raise ValueError("unsafe SQL identifier: %r" % (name,))
    return "[%s]" % name


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _flag_column(cur, table):
    """(data_type, nullable) for DELETE_FLAG_COLUMN in `table`, or None.

    Called from db_dns(), which runs in BOTH --dry-run and live mode. In the
    draft this check lived inside apply_deletions() below the dry-run branch,
    so a green dry run in flag mode proved nothing about the column whose name
    the code itself calls a guess, and the live run discovered it was wrong
    only after upsert() had already committed. That is hazard #6 from the
    identity port (three green dry runs, then a failure on the first real
    write) reproduced in the delete path.
    """
    cur.execute("SELECT DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_NAME = %s AND COLUMN_NAME = %s",
                (table, DELETE_FLAG_COLUMN))
    found = {((r[0] or "").lower(), (r[1] or "").upper() == "YES")
             for r in cur.fetchall()}
    if not found:
        return None
    if len(found) > 1:
        raise SystemExit(
            "ABORT: %r resolves to more than one column named %r across schemas, "
            "with disagreeing types. Qualify the table or set "
            "IA_DELETE_FLAG_COLUMN. Nothing has been written."
            % (table, DELETE_FLAG_COLUMN))
    return found.pop()


def _delete_marker(dtype):
    """Value written into DELETE_FLAG_COLUMN, chosen from its declared type."""
    import datetime

    if "date" in dtype or "time" in dtype:
        # Naive UTC: what a datetime/datetime2 column holds, and utcnow() is
        # deprecated from 3.12.
        return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    if "char" in dtype or "text" in dtype:
        return "deleted"
    return 1


def db_dns(types):
    """Read the IA tables. Returns (existing, originals, flags).

    existing  -- {type_name: set(_dn_key(dn))}, the left operand of the set
                 difference and of assert_dn_overlap().
    originals -- {type_name: {_dn_key(dn): [stored dn, ...]}}, so the delete or
                 flag statement names the row the way the table holds it. A key
                 maps to a LIST because two stored DNs can differ only in case;
                 both are then flagged, which cannot lose a deletion.
    flags     -- {type_name: (data_type, nullable) or None} for
                 DELETE_FLAG_COLUMN, resolved in dry-run and live alike.

    Replaces the eight `<X> - DN` dbxquery searches. Theirs run with
    maxrows=5000000, a silent cap nobody measured, and none of them excludes an
    already soft-deleted row -- so Splunk's `existing - live` is cumulative and
    a per-run delete ceiling degrades into a lifetime budget. Simulated on a
    30,000-row _contact_email with 0.4% ED churn per run, that reaches the 2%
    ceiling on run 6 and SystemExits on every run after it, taking the upsert
    down with it because plan_deletions() runs first. Excluding flagged rows
    here is what makes MAX_DELETE_FRACTION measure one run's churn again.

    The exclusion and the flag column are both gated on DELETE_MODE == "flag":
    under "delete" there are no soft-flagged rows, and if the guessed column
    name is wrong this job must not filter on a column it does not own.
    """
    conn = db_conn()
    existing, originals, flags = {}, {}, {}
    try:
        cur = conn.cursor()
        for type_name in sorted(set(types)):
            if type_name not in TABLES:
                raise SystemExit("ABORT: no table mapped for type %r" % type_name)
            table = TABLES[type_name]

            info = _flag_column(cur, table)
            flags[type_name] = info
            where, dtype = "", None
            if info is not None and DELETE_MODE == "flag":
                dtype = info[0]
                col = _ident(DELETE_FLAG_COLUMN)
                if "date" in dtype or "time" in dtype:
                    where = " AND %s IS NULL" % col
                elif "char" in dtype or "text" in dtype:
                    where = " AND (%s IS NULL OR %s = '')" % (col, col)
                else:
                    where = " AND (%s IS NULL OR %s = 0)" % (col, col)

            key = _ident(KEY_COLUMN)
            cur.execute("SELECT %s FROM %s WHERE %s IS NOT NULL%s"
                        % (key, _ident(table), key, where))
            fetched = [r[0] for r in cur.fetchall() if r[0]]

            excluded = 0
            if where:
                cur.execute("SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL"
                            % (_ident(table), key))
                excluded = cur.fetchone()[0] - len(fetched)

            keyed = {}
            for dn in fetched:
                keyed.setdefault(_dn_key(dn), []).append(dn)
            keyed.pop("", None)
            existing[type_name] = set(keyed)
            originals[type_name] = keyed

            logger.info("db %-16s %-24s rows=%d keys=%d flag_column=%s "
                        "flagged_excluded=%d",
                        type_name, table, len(fetched), len(keyed),
                        "absent" if info is None else dtype or info[0],
                        excluded)
            if len(fetched) != len(keyed):
                # MERGE raises "attempted to UPDATE or DELETE the same row more
                # than once" against a non-unique key, so this is a real defect
                # in the table, not a curiosity. It is counted after
                # normalisation, so DNs differing only in case are included --
                # SQL Server's collation considers those the same row.
                logger.warning("%s has %d %s values that collide under the "
                               "database's case-insensitive collation -- the "
                               "upsert key is not unique there",
                               table, len(fetched) - len(keyed), KEY_COLUMN)

            floor = DB_MIN_ROWS.get(type_name, 1)
            if type_name in IA_ALLOW_EMPTY_TABLE:
                logger.warning("db %s: floor of %d waived by IA_ALLOW_EMPTY_TABLE",
                               type_name, floor)
            elif not keyed:
                raise SystemExit(
                    "ABORT: %s (%s) returned no usable %s values. An empty read "
                    "and a genuinely new table are the same observation from "
                    "here, and an empty DB side silently suppresses every real "
                    "deletion, so this is an abort. If the table really is new, "
                    "set IA_ALLOW_EMPTY_TABLE=%s. Nothing has been written."
                    % (table, type_name, KEY_COLUMN, type_name))
            elif len(keyed) < floor:
                raise SystemExit(
                    "ABORT: %s (%s) returned %d %s values, below the DB floor of "
                    "%d. A truncated DB read suppresses real deletions and hides "
                    "that it was truncated. Nothing has been written."
                    % (table, type_name, len(keyed), KEY_COLUMN, floor))
    finally:
        conn.close()
    return existing, originals, flags


def _null_if_blank(value):
    """An empty string becomes NULL.

    mv() and row.get() both return "" for an attribute ED did not supply, and
    "" is not NULL, so COALESCE would not fire on it: an absent attribute would
    overwrite a populated column with an empty string and the COALESCE guard
    below would be decorative. The live tables use NULL for absent, matching
    buildIdentity.py's _coerce().
    """
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return value


def _columns_for(rows):
    """Union of the columns present across a type's rows, sorted.

    Sorted because a stable statement is diffable in a log and is what demo()
    asserts against. KEY_COLUMN is excluded here and re-added first by
    _merge_sql: the DN comes from the dict key (row.entry_dn, the real DN), and
    a stray "dn" inside a row dict must never win over it.
    """
    cols = set()
    for row in rows.values():
        cols.update(row)
    cols.discard(KEY_COLUMN)
    return sorted(cols)


def _batch_size(columns):
    return max(1, min(MERGE_MAX_ROWS, 2000 // (len(columns) + 1)))


def _merge_sql(table, columns, n_rows, clear_flag=None):
    """One MERGE covering n_rows, parameterised, nothing interpolated but names.

    Four things here are load-bearing, each because the SPL or the sibling port
    got it wrong:

      * It is an upsert, never delete-then-insert. The SPL's dbxoutput stanzas
        are opaque, and a replace-shaped one is how a partial ED read becomes a
        row that vanishes and comes back.
      * COALESCE(src.c, tgt.c): a NULL from this run leaves whatever is already
        there. Without it the sibling port would have wiped 237 primaryProgram
        and 261 primaryOrg values that had been correct for years (measured
        2026-09-05). The trade-off is that a value can be replaced but never
        cleared; clearing is a separate, explicit operation.
      * clear_flag is that explicit exception, and the ONLY column not
        COALESCE'd. The delete flag was previously written by the flag path and
        never cleared by anything, because _columns_for() only ever sees
        ED-sourced column names -- so a row dropped by one bad ED read stayed
        flagged after it came back, forever, with no row-count change on either
        side to expose it. Presence in ED this run is exactly the evidence the
        row is not deleted, so the clear is unconditional for matched rows.
        A literal, never a parameter, and only ever "NULL" or "0".
      * The UPDATE targets are table-qualified. The sibling port shipped
        unqualified ones and got (1052, "Column 'x' in UPDATE is ambiguous") on
        the go-live attempt, having passed three dry runs -- because dry-run
        returned before the write. demo() asserts on this string for that
        reason.

    WITH (HOLDLOCK) closes MERGE's insert/update race under concurrency. The
    key is never in the SET clause: it is the join condition.

    Every VALUE placeholder is CAST to NVARCHAR. pymssql interpolates
    parameters client-side, so a None arrives as a bare NULL literal, and SQL
    Server types a bare NULL in a VALUES constructor as int. A batch in which
    one column is NULL on every row would then COALESCE an int against an
    nvarchar column, and type precedence makes SQL Server convert the EXISTING
    value to int -- "Conversion failed when converting the nvarchar value ...
    to data type int" on rows that were perfectly fine.

    The KEY placeholder is deliberately NOT cast. It can never be NULL -- it is
    the dict key from `built` -- so the rationale does not apply, and casting it
    made the join compare the stored column against an NVARCHAR(MAX) expression:
    if `dn` is varchar, nvarchar's higher precedence forces CONVERT_IMPLICIT on
    the column and the index seek is lost. With ~40k rows at 43 rows per batch
    that is ~900 potentially scanning MERGEs per large table.
    """
    if clear_flag not in (None, "NULL", "0"):
        raise ValueError("clear_flag must be None, 'NULL' or '0': %r" % (clear_flag,))
    cols = [KEY_COLUMN] + [c for c in columns if c != KEY_COLUMN]
    q = [_ident(c) for c in cols]
    if len(cols) < 2:
        raise ValueError("nothing to update for %s: no columns beyond the key" % table)
    row_ph = "(" + ", ".join(
        ["%s"] + ["CAST(%s AS NVARCHAR(MAX))"] * (len(cols) - 1)) + ")"
    values = ", ".join([row_ph] * n_rows)
    sets = ["    tgt.{c} = COALESCE(src.{c}, tgt.{c})".format(c=name)
            for name in q[1:]]
    if clear_flag is not None:
        sets.append("    tgt.{c} = {v}".format(
            c=_ident(DELETE_FLAG_COLUMN), v=clear_flag))
    return (
        "MERGE INTO {table} WITH (HOLDLOCK) AS tgt\n"
        "USING (VALUES {values}) AS src ({cols})\n"
        "    ON tgt.{key} = src.{key}\n"
        "WHEN MATCHED THEN UPDATE SET\n"
        "{updates}\n"
        "WHEN NOT MATCHED THEN INSERT ({cols})\n"
        "    VALUES ({srccols});".format(
            table=_ident(table), values=values, cols=", ".join(q),
            key=q[0], updates=",\n".join(sets),
            srccols=", ".join("src." + c for c in q)))


def _merge_params(dn, row, columns):
    """Bind one row. `row` is a plain dict -- upsert() refuses anything else.

    That guard is the point: _Row is a dict subclass whose .get() flattens a
    multivalue to its FIRST value, which is the bug that emptied 15 person-type
    flags on the sibling port. A source that returned _Row objects instead of
    plain dicts would bind 'academic' where the column holds
    'academic|academic-faculty|affiliate', with no row-count change to expose it.
    """
    return [dn] + [_null_if_blank(row.get(c)) for c in columns]


def upsert(built, flags, dry_run=False):
    """Write every type to its table. Returns {type_name: rows written}.

    One connection, one transaction PER TABLE. The draft held one transaction
    over all eight tables, which means MERGE's HOLDLOCK serializable range locks
    are held across the whole IA database for the duration of the run while
    ~2,300 batched MERGEs execute -- long enough to hit db_conn()'s 600s
    statement timeout and to block every reader in between. Per-table is the
    honest trade: a failure on table eight leaves tables one to seven refreshed,
    which is recoverable by rerunning an idempotent upsert, and the log names
    exactly which types committed.
    """
    # Refuse ambiguous row objects before anything opens a connection, so
    # --dry-run catches them too.
    for type_name, rows in sorted(built.items()):
        bad = sum(1 for r in rows.values() if type(r) is not dict)
        if bad:
            raise SystemExit(
                "ABORT: %d of %d rows for type %r are not plain dicts. _Row is a "
                "dict subclass whose .get() returns only the FIRST value of a "
                "multivalue, so binding one here would write a single value into "
                "a column whose consumers expect the pipe-joined set -- with no "
                "row-count change to expose it. Have source(s) %s build plain "
                "dicts. Nothing has been written."
                % (bad, len(rows), type_name,
                   ", ".join(sorted(n for n, fn in SOURCES.items()
                                    if fn.type_name == type_name)) or "(unknown)"))

    conn = None if dry_run else db_conn()
    written, committed = {}, []
    try:
        cur = None if conn is None else conn.cursor()
        for type_name, rows in sorted(built.items()):
            if type_name not in TABLES:
                raise SystemExit("ABORT: no table mapped for type %r" % type_name)
            table = TABLES[type_name]
            if not rows:
                logger.warning("upsert %-16s no rows, skipped", type_name)
                continue

            clear = None
            info = flags.get(type_name)
            if info is not None and DELETE_MODE == "flag":
                dtype, nullable = info
                if nullable:
                    clear = "NULL"
                elif dtype in NUMERIC_FLAG_TYPES:
                    clear = "0"
                else:
                    raise SystemExit(
                        "ABORT: %s.%s is NOT NULL and typed %r, so there is no "
                        "known 'not deleted' value to write back and a row that "
                        "returned to ED could never be un-flagged. Make the "
                        "column nullable, or set IA_DELETE_FLAG_COLUMN to the "
                        "real one. Nothing has been written."
                        % (table, DELETE_FLAG_COLUMN, dtype))

            columns = _columns_for(rows)
            size = _batch_size(columns)
            items = sorted(rows.items())
            batches = -(-len(items) // size)

            if dry_run:
                # SQL text only, and shown one row wide: a real batch just
                # repeats the row constructor, and 1000 of them buries the log.
                # Every value is a placeholder either way, so no mail / mobile /
                # postalCode VALUE can reach a log from here.
                logger.info("--dry-run %s: %d rows in %d batch(es) of up to %d, "
                            "delete-flag clear %s. Statement (one row shown):\n%s",
                            table, len(items), batches, size,
                            "disabled" if clear is None else "tgt.%s = %s" % (
                                _ident(DELETE_FLAG_COLUMN), clear),
                            _merge_sql(table, columns, 1, clear_flag=clear))
                written[type_name] = len(items)
                logger.info("upsert %-16s %-24s rows=%d cols=%d  "
                            "(dry run, nothing executed)",
                            type_name, table, len(items), len(columns))
                continue

            done = 0
            for chunk in _chunks(items, size):
                params = []
                for dn, row in chunk:
                    params.extend(_merge_params(dn, row, columns))
                cur.execute(_merge_sql(table, columns, len(chunk), clear_flag=clear),
                            tuple(params))
                done += len(chunk)
            conn.commit()
            committed.append(type_name)
            written[type_name] = done
            logger.info("upsert %-16s %-24s rows=%d cols=%d committed",
                        type_name, table, done, len(columns))
        if conn is not None:
            logger.info("upsert complete: %d rows across %d tables (%s)",
                        sum(written.values()), len(committed),
                        ", ".join(committed) or "none")
    except BaseException:
        # BaseException, not Exception: every abort in this function raises
        # SystemExit, which does NOT derive from Exception, so an `except
        # Exception` arm skipped the rollback and -- more to the point -- never
        # logged the line an operator reads to decide whether anything landed.
        if conn is not None:
            conn.rollback()
            logger.error("upsert rolled back the in-flight table; committed "
                         "before the failure: %s", ", ".join(committed) or "none")
        raise
    finally:
        if conn is not None:
            conn.close()
    return written


def _flag_sql(table, n_dns):
    """Soft-delete UPDATE. Target qualified for the same reason _merge_sql is."""
    return ("UPDATE tgt\n"
            "   SET tgt.{flag} = %s\n"
            "  FROM {table} AS tgt\n"
            " WHERE tgt.{key} IN ({ph})".format(
                flag=_ident(DELETE_FLAG_COLUMN), table=_ident(table),
                key=_ident(KEY_COLUMN), ph=", ".join(["%s"] * n_dns)))


def _delete_sql(table, n_dns):
    return ("DELETE tgt\n"
            "  FROM {table} AS tgt\n"
            " WHERE tgt.{key} IN ({ph})".format(
                table=_ident(table), key=_ident(KEY_COLUMN),
                ph=", ".join(["%s"] * n_dns)))


def apply_deletions(plan, flags, dry_run=False):
    """Consume plan_deletions()' output under DELETE_MODE. Returns per-type counts.

    `plan` holds ORIGINAL stored DN strings (main() maps the normalised keys
    plan_deletions returns back through db_dns' `originals`), because the WHERE
    clause has to name the row the way the table holds it.

    `flags` comes from db_dns(), which resolved DELETE_FLAG_COLUMN from
    INFORMATION_SCHEMA in this run -- dry or live. Nothing here rediscovers it,
    so the dry run exercises exactly the check the live run depends on.
    """
    if DELETE_MODE not in ("flag", "delete"):
        raise SystemExit("ABORT: DELETE_MODE=%r is neither 'flag' nor 'delete'"
                         % DELETE_MODE)

    if DELETE_MODE == "delete" and os.environ.get("IA_DELETE_CONFIRMED") != "yes":
        # Refused in dry-run too, deliberately. A dry run that "passes" here is
        # exactly what would give someone the confidence to flip to live, and
        # that is hazard #6 from the identity port: three green dry runs, then a
        # failure on the first real write.
        raise SystemExit(
            "ABORT: DELETE_MODE='delete' issues real DELETEs, and nobody has "
            "read Splunk's conf-db_outputs yet -- so what the IdentityAuthority_dn "
            "stanza actually does with a deleted DN (delete the row? insert into "
            "a tombstone table? set a column?) is unknown. If the SPL only ever "
            "flagged, this mode destroys history the Splunk job kept. Read the "
            "stanza first. To proceed anyway, set IA_DELETE_CONFIRMED=yes. "
            "Nothing has been written.")

    total = sum(len(v) for v in plan.values())
    if not total:
        logger.info("deletions: nothing to apply")
        return {}

    conn = None if dry_run else db_conn()
    applied = {}
    try:
        cur = None if conn is None else conn.cursor()
        for type_name, dns in sorted(plan.items()):
            if not dns:
                continue
            if type_name not in TABLES:
                raise SystemExit("ABORT: no table mapped for type %r" % type_name)
            table = TABLES[type_name]

            marker = None
            if DELETE_MODE == "flag":
                info = flags.get(type_name)
                if info is None:
                    raise SystemExit(
                        "ABORT: %s has no column %r, so DELETE_MODE='flag' has "
                        "nowhere to write. That column name is a guess -- no "
                        "query in the SPL names it, because the write goes "
                        "through the opaque IdentityAuthority_dn stanza. Read "
                        "conf-db_outputs, then set IA_DELETE_FLAG_COLUMN. "
                        "Nothing has been written." % (table, DELETE_FLAG_COLUMN))
                marker = _delete_marker(info[0])

            build = _flag_sql if DELETE_MODE == "flag" else _delete_sql
            if dry_run:
                # DNs stay parameters, never inlined: a location or email DN
                # carries identifying values. One placeholder shown, as above.
                logger.info("--dry-run %s: %s %d dn(s); flag column %s typed %s, "
                            "marker would be %r. Statement (one dn shown):\n%s",
                            table, DELETE_MODE, len(dns),
                            DELETE_FLAG_COLUMN if marker is not None else "(unused)",
                            flags.get(type_name)[0] if marker is not None else "-",
                            marker, build(table, 1))
                applied[type_name] = len(dns)
                logger.info("deletions %-16s %-24s mode=%s count=%d  "
                            "(dry run, nothing executed)",
                            type_name, table, DELETE_MODE, len(dns))
                continue

            for chunk in _chunks(list(dns), DELETE_CHUNK):
                params = list(chunk) if marker is None else [marker] + list(chunk)
                cur.execute(build(table, len(chunk)), tuple(params))
            applied[type_name] = len(dns)
            logger.info("deletions %-16s %-24s mode=%s count=%d",
                        type_name, table, DELETE_MODE, len(dns))
        if conn is not None:
            # Deletions stay all-or-nothing in one transaction: the volume is
            # capped at MAX_DELETE_FRACTION, so the lock window is short, and a
            # half-applied deletion set is the one outcome with no clean rerun.
            conn.commit()
            logger.info("deletions committed: %d rows across %d tables (mode=%s)",
                        sum(applied.values()), len(applied), DELETE_MODE)
    except BaseException:
        if conn is not None:
            conn.rollback()
            logger.error("deletions rolled back -- no table was modified")
        raise
    finally:
        if conn is not None:
            conn.close()
    return applied


# ---------------------------------------------------------------------------
#                                   MAIN
# ---------------------------------------------------------------------------

_demo_before_dbside = demo   # extended below, not replaced


def demo():
    """Everything above, then the SQL builders. No network, no DB, no driver."""
    _demo_before_dbside()

    # Hazard #6 from the identity port: --dry-run returned before the upsert
    # there, so three green dry runs shipped SQL that MySQL rejected on the
    # first real write. Everything below runs the actual builder offline.
    # Column NAMES only -- never a value of mail / mobile / postalCode.
    sql = _merge_sql("_contact_email", ["mail", "ou", "weillCornellEduCWID"], 2)

    assert sql.startswith("MERGE INTO [_contact_email]"), sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql, sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql, sql
    # It must be an upsert, not a delete-then-insert.
    assert "DELETE" not in sql.upper() and "TRUNCATE" not in sql.upper(), sql
    assert "WHEN NOT MATCHED BY SOURCE" not in sql.upper(), \
        "a BY SOURCE clause would delete rows this run did not see"

    set_clause = sql.split("WHEN MATCHED THEN UPDATE SET\n", 1)[1] \
                    .split("\nWHEN NOT MATCHED", 1)[0]
    assert set_clause.strip(), "empty SET clause"
    for line in set_clause.splitlines():
        # Unqualified targets are what produced 1052 in production.
        assert line.strip().startswith("tgt.["), \
            "UPDATE target must be table-qualified: %r" % line
        assert "COALESCE(src." in line, \
            "every ED column must COALESCE or a NULL run erases the DB: %r" % line
    assert "tgt.[dn] = COALESCE" not in sql, "the merge key must not be updated"
    assert sql.count("%s") == 2 * 4, "one placeholder per column per row"
    # An all-NULL VALUE column in a batch must not be typed int by the server;
    # the KEY must NOT be cast, or the ON clause stops being SARGable.
    assert sql.count("CAST(%s AS NVARCHAR(MAX))") == 2 * 3, sql
    assert "USING (VALUES (%s, CAST(%s AS NVARCHAR(MAX))" in sql, \
        "the key placeholder must be bare: casting it costs the index seek"
    assert "ON tgt.[dn] = src.[dn]" in sql, sql
    assert "WITH (HOLDLOCK)" in sql, "MERGE races on concurrent insert without it"

    # The delete flag must be cleared for every matched row, and must be the one
    # column that is NOT COALESCE'd -- a row present in ED this run is not
    # deleted, whatever the flag currently says.
    cleared = _merge_sql("_contact_email", ["mail"], 1, clear_flag="NULL")
    assert "tgt.%s = NULL" % _ident(DELETE_FLAG_COLUMN) in cleared, cleared
    assert "COALESCE(src.%s" % _ident(DELETE_FLAG_COLUMN) not in cleared, \
        "the flag clear must not be COALESCE'd or it can never un-flag"
    assert cleared.count("%s") == 2, "the clear is a literal, not a parameter"
    assert "tgt.%s = 0" % _ident(DELETE_FLAG_COLUMN) in \
        _merge_sql("_contact_email", ["mail"], 1, clear_flag="0")
    assert "tgt.%s" % _ident(DELETE_FLAG_COLUMN) not in \
        _merge_sql("_contact_email", ["mail"], 1), "clear is opt-in"
    try:
        _merge_sql("_contact_email", ["mail"], 1, clear_flag="1 -- x")
        raise AssertionError("clear_flag must be a whitelisted literal")
    except ValueError:
        pass

    # A blank from ED must arrive as NULL, or COALESCE never fires and the
    # guard above is decorative.
    assert _null_if_blank("") is None and _null_if_blank("  ") is None
    assert _null_if_blank("x") == "x" and _null_if_blank(0) == 0
    assert _merge_params("cn=a,ou=x", {"mail": ""}, ["mail", "ou"]) == \
        ["cn=a,ou=x", None, None], "absent and blank columns both bind NULL"
    # The DN comes from the dict key, never from a "dn" column inside the row.
    assert _columns_for({"cn=a": {"dn": "WRONG", "ou": "x"}}) == ["ou"]

    assert _batch_size(["a"]) == MERGE_MAX_ROWS, "the 1000-row VALUES cap binds"
    assert _batch_size(["c%d" % i for i in range(44)]) == 44, "the ~2100 param cap"
    assert _batch_size(["c%d" % i for i in range(4000)]) >= 1, "wide types still batch"
    try:
        _ident("dn; drop table x --")
        raise AssertionError("_ident must reject a non-identifier")
    except ValueError:
        pass

    # Case- and whitespace-insensitive comparison, matching the collation the
    # MERGE's ON clause runs under. Without this the same run refreshes a row
    # and soft-deletes it.
    assert _dn_key(" UID=7,OU=People,DC=weill ") == "uid=7,ou=people,dc=weill"
    live = {_dn_key("uid=7,ou=people,dc=weill"): {}}
    stored = {_dn_key("UID=7,ou=People,dc=weill")}
    assert plan_deletions({"canonical": live}, {"canonical": stored})["canonical"] == [], \
        "a case-only difference must not be a deletion"
    assert_dn_overlap({"canonical": live}, {"canonical": stored})

    # _Row leaking into a built dict binds the FIRST value of a multivalue. It
    # must abort before anything opens a connection.
    try:
        upsert({"email": {"cn=a": _Row([("ou", ["x", "y"])])}}, {}, dry_run=True)
        raise AssertionError("a _Row-valued built dict must abort")
    except SystemExit as exc:
        assert "not plain dicts" in str(exc), exc

    flag = _flag_sql("_contact_email", 3)
    assert flag.startswith("UPDATE tgt"), flag
    assert "tgt.%s = %%s" % _ident(DELETE_FLAG_COLUMN) in flag, flag
    assert flag.count("%s") == 4, "marker plus one placeholder per dn"
    assert "DELETE" not in flag.split("SET", 1)[0].upper(), "flag mode never deletes"
    assert _delete_sql("_contact_email", 2).startswith("DELETE tgt"), "delete mode does"
    assert _delete_marker("bit") == 1 and _delete_marker("nvarchar") == "deleted"
    assert hasattr(_delete_marker("datetime2"), "strftime")

    # The delete branch must refuse without the explicit confirmation, and must
    # refuse before it opens a connection -- so this runs offline.
    was_mode, was_env = DELETE_MODE, os.environ.pop("IA_DELETE_CONFIRMED", None)
    try:
        globals()["DELETE_MODE"] = "delete"
        try:
            apply_deletions({"email": ["cn=a"]}, {}, dry_run=True)
            raise AssertionError("unconfirmed delete mode must abort")
        except SystemExit as exc:
            assert "IA_DELETE_CONFIRMED" in str(exc) and "conf-db_outputs" in str(exc)
    finally:
        globals()["DELETE_MODE"] = was_mode
        if was_env is not None:
            os.environ["IA_DELETE_CONFIRMED"] = was_env

    # The database name in a JDBC URL is discarded by _mssql_target, so it has
    # to be recovered separately or the run lands wherever the default points.
    assert _mssql_target("jdbc:sqlserver://ia.db:1433;databaseName=IA") == ("ia.db", 1433)
    assert _database_name("jdbc:sqlserver://ia.db:1433;databaseName=IA") == "IA"
    assert _database_name("ia.db") == ""
    assert "IA_DB_NAME" in IA_DB_ENV, "the target database must be a required secret"

    assert set(TABLES) >= set(MIN_ROWS), "every table needs an ED row floor"
    assert set(TABLES) >= set(DB_MIN_ROWS), "every table needs a DB row floor"
    print("demo OK: SQL builders exercised offline for %d tables" % len(TABLES))


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

    # Intersect on the ALIAS the source actually reads. An earlier version
    # intersected type names, which are a disjoint vocabulary, so the warning
    # was unreachable and the run proceeded against five unproved base DNs in
    # silence. A base DN that binds to the wrong subtree produces wrong DNs,
    # which is the input to the deletion set difference -- so on a live run this
    # is an abort, not a warning.
    unresolved = UNRESOLVED_ALIASES & {fn.alias for fn in SOURCES.values() if fn.alias}
    if unresolved:
        if args.dry_run or os.environ.get("IA_ALLOW_UNRESOLVED_BASE") == "yes":
            logger.warning(
                "base DN not yet proved against live ED for: %s -- run --spike",
                ", ".join(sorted(unresolved)))
        else:
            raise SystemExit(
                "ABORT: the base DN for %s has never been proved against live "
                "ED. A base that binds to the wrong subtree returns real rows "
                "with the wrong DNs, and DNs are what the deletion set "
                "difference compares. Run --spike, or --dry-run, or set "
                "IA_ALLOW_UNRESOLVED_BASE=yes. Nothing has been written."
                % ", ".join(sorted(unresolved)))

    built = run_sources()
    logger.info("built %d types, %d rows total",
                len(built), sum(len(v) for v in built.values()))

    # One canonical key space for every comparison. built keeps its real entry
    # DNs (they are what the upsert writes); this is the view the set
    # difference and the overlap check run on.
    norm = {}
    for type_name, rows in built.items():
        norm_rows = {}
        for dn, row in rows.items():
            key = _dn_key(dn)
            if not key:
                raise SystemExit(
                    "ABORT: a source for type %r produced a blank DN. Nothing "
                    "has been written." % type_name)
            if key in norm_rows:
                raise SystemExit(
                    "ABORT: two live DNs for type %r differ only in case or "
                    "whitespace, so the database's collation cannot tell them "
                    "apart and the MERGE would update the same row twice. "
                    "Nothing has been written." % type_name)
            norm_rows[key] = row
        norm[type_name] = norm_rows

    existing, originals, flags = db_dns(sorted(built))

    # Runs on EVERY path, --no-delete included: --no-delete removes the
    # deletion guard but not the duplication hazard, and a DN-format mismatch
    # makes the upsert INSERT a second copy of every row.
    assert_dn_overlap(norm, existing)

    if DELETE_MODE == "flag" and not args.no_delete:
        absent = sorted(t for t in built if flags.get(t) is None)
        if absent:
            raise SystemExit(
                "ABORT: DELETE_MODE='flag' but column %r does not exist on the "
                "table(s) for: %s. Checked before the upsert deliberately -- the "
                "draft discovered this after the write had already committed. "
                "Read conf-db_outputs and set IA_DELETE_FLAG_COLUMN, or run "
                "--no-delete. Nothing has been written."
                % (DELETE_FLAG_COLUMN, ", ".join(absent)))

    if args.no_delete:
        # plan_deletions aborts the process on its ceiling. Those aborts exist
        # to stop a deletion, so on an explicitly delete-free run they must not
        # be what stops the upsert.
        plan = {}
        logger.info("--no-delete: deletion pass skipped entirely")
    else:
        plan = {}
        for type_name, keys in plan_deletions(norm, existing).items():
            # Back to the strings the table actually holds. One key can map to
            # several stored DNs that differ only in case; all of them are gone
            # from ED, so all of them are named.
            plan[type_name] = [dn for key in keys
                               for dn in originals[type_name].get(key, [])]

    upsert(built, flags, dry_run=args.dry_run)
    apply_deletions(plan, flags, dry_run=args.dry_run)
    if args.dry_run:
        logger.info("--dry-run complete: SQL logged, nothing written")
    return built
