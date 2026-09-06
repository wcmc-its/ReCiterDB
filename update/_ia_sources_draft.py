"""DRAFT source functions for buildIdentityAuthority.py -- NOT IMPORTED, NOT WIRED IN.

Written 2026-09-05 by a four-way fan-out over the authoritative Splunk export,
then rejected by four adversarial reviewers: 8 data-loss and 17 correctness
findings, all four slices NEEDS_FIXES. See IA_REVIEW_FINDINGS.md.

Kept here rather than deleted because the SPL analysis embedded in the comments
is the expensive part and is mostly sound -- the defects are concentrated in
which accessor is used (row.get vs mv) and in the DN keying, not in the reading
of the SPL. Do not import this file until the findings are closed.
"""


# ==========================================================================
# SLICE: canonical (types: canonical, sor_record)
# ==========================================================================


# ---------------------------------------------------------------------------
#                  SHARED: displayName, GeneralizedTime
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

    The SPL slices positions 1-4, 6-7, 9-10, 12-13, 15-16, 18-19, which is an
    ISO-8601 string with the "T" swapped for a space -- Splunk's ldapfilter had
    already parsed the directory's GeneralizedTime for it.

    Read through dict.get rather than _Row.get: ldap3 parses a populated
    GeneralizedTime into a tz-aware datetime and _Row.get() normalises that to
    ISO-8601 WITH the offset ("...+00:00"), which is not the shape the column
    holds. strftime on the datetime reproduces the SPL's output exactly.
    Anything ldap3 hands back as a raw string is sliced the way the SPL sliced
    it, so neither shape can silently write nonsense.

    The SPL's `eval createTimestamp = max(createTimestamp)` ahead of the slicing
    is Splunk defensiveness against a multivalue that operational attributes
    cannot have; not reproduced.
    """
    value = dict.get(row, attr.lower(), "")
    if isinstance(value, list):
        value = next((v for v in value if v not in (None, "")), "")
    # Duck-typed rather than isinstance(datetime): this module does not import
    # datetime and one attribute is enough of a reason not to start.
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    text = str(value or "")
    if not text:
        return ""
    if len(text) >= 14 and text[:14].isdigit():        # raw "20190515000000Z"
        return "%s-%s-%s %s:%s:%s" % (text[0:4], text[4:6], text[6:8],
                                      text[8:10], text[10:12], text[12:14])
    return text.replace("T", " ")[:19]                 # already ISO-8601


# ---------------------------------------------------------------------------
#                        SOURCE: canonical (ed-people)
# ---------------------------------------------------------------------------

@source("canonical")
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
        dn = "uid=" . uid . ",ou=people,dc=weill,dc=cornell,dc=edu".
        ldap3 returns the real DN; we key on row.entry_dn.
      * `where ... len(uid) < 20` (on the db-update half) is dropped. It exists
        only to keep ou=sors entries out of a lookup shared with the SOR record
        search, which used the mirror-image `len(uid) > 20`. Reading ou=people
        from its own base DN separates those populations structurally.
      * `ou` is requested by the SPL and never emitted; not requested here.
    """
    rows = ldap_search(
        "ed-people",
        "(objectClass=weillCornellEduPerson)",
        # The subtyped descriptions are requested EXPLICITLY alongside their
        # bare types. The SPL got labeledURI;pops and weillCornellEduReleaseCode
        # ;picture back from a bare request via spath, but the sibling Company
        # search is evidence ED does not always return options for a bare type
        # -- it needed a whole separate search to see o;company. Asking for both
        # costs one list entry and cannot miss.
        ["weillCornellEduCWID", "uid", "givenName", "sn",
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
         "labeledURI", "labeledURI;pops", "labeledURI;vivo",
         "labeledURI;onlinedirectory",
         "weillCornellEduReleaseCode", "weillCornellEduReleaseCode;picture",
         "weillCornellEduReleaseCode;person", "weillCornellEduReleaseCode;mail",
         "weillCornellEduReleaseCode;telephonenumber",
         "weillCornellEduReleaseCode;location"])

    out = {}
    for row in rows:
        cwid = row.get("weillCornellEduCWID")
        if not cwid:
            continue                       # SPL: where isnotnull(...CWID)
        # `dn` is the dict key, not a column -- the writer takes it from there.
        out[row.entry_dn] = {
            "weillCornellEduCWID": cwid,
            "uid": row.get("uid"),
            "givenName": row.get("givenName"),
            "sn": row.get("sn"),
            "weillCornellEduMiddleName": row.get("weillCornellEduMiddleName"),
            "displayName": _display_name(row),
            "weillCornellEduPrimaryDepartment":
                row.get("weillCornellEduPrimaryDepartment"),
            "weillCornellEduPrimaryDepartmentCode":
                row.get("weillCornellEduPrimaryDepartmentCode"),
            "weillCornellEduPrimaryTitle": row.get("weillCornellEduPrimaryTitle"),
            "weillCornellEduPrimaryTitleCode":
                row.get("weillCornellEduPrimaryTitleCode"),
            "weillCornellEduWorkingTitle": row.get("weillCornellEduWorkingTitle"),
            "weillCornellEduPrimaryRoleCode":
                row.get("weillCornellEduPrimaryRoleCode"),
            "telephoneNumber": row.get("telephoneNumber"),
            "postalCode": row.get("postalCode"),
            "street": row.get("street"),
            "l": row.get("l"),
            "st": row.get("st"),
            "mail": row.get("mail"),
            "weillCornellEduProviderID": row.get("weillCornellEduProviderID"),
            "eduPersonPrimaryAffiliation": row.get("eduPersonPrimaryAffiliation"),

            # The 13 `nomv` + `rex "s/\n/|/g"` attributes this search emits.
            # Anything on this list reaches the DB pipe-joined today and MUST
            # go through mv(); row.get() would write one value into a column
            # whose consumers expect the set, with no row-count change to show
            # for it. The SPL also nomv's `title` and `weillCornellEduFTE`, but
            # this search neither requests nor tables either one -- dead blocks,
            # deliberately not emitted. (Both are live on the SOR record side.)
            "weillCornellEduStatus": mv(row, "weillCornellEduStatus"),
            "o": mv(row, "o"),
            "weillCornellEduDepartment": mv(row, "weillCornellEduDepartment"),
            "weillCornellEduDepartmentCode":
                mv(row, "weillCornellEduDepartmentCode"),
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

            # Subtyped, single-valued, and renamed by the SPL only because
            # Splunk cannot table a field whose name contains a semicolon.
            "labeledURIpops": row.get("labeledURI;pops"),
            "labeledURIvivo": row.get("labeledURI;vivo"),
            "labeledURIonlinedirectory": row.get("labeledURI;onlinedirectory"),

            "weillCornellEduActiveMember": _active_member(row),
            "createTimestamp": _ts(row, "createTimestamp"),
            "modifyTimestamp": _ts(row, "modifyTimestamp"),
        }
    return out


def _active_member(row):
    """weillCornellEduActiveMember as 1/0.

    The SPL is `if(max(weillCornellEduActiveMember) > "false", 1, 0)` -- a
    LEXICOGRAPHIC, case-SENSITIVE comparison over a multivalue. RFC 4517 renders
    LDAP Booleans uppercase, and "TRUE" < "false" in ASCII (uppercase sorts
    first), so if ED returns "TRUE" this flag has been 0 for every person in the
    table for as long as it has run. That is suspected broken, not intended.

    Implemented as the intent instead: 1 if ANY value is true, case-insensitive.
    PORT_SPEC.md carries the one-line query that settles which casing ED
    actually returns; until it is run, expect this column to differ from Splunk
    for everyone and treat the difference as the fix.
    """
    return 1 if any(v.strip().lower() == "true"
                    for v in row.all("weillCornellEduActiveMember")) else 0


# ---------------------------------------------------------------------------
#                       SOURCE: sor_record (ed-sors)
# ---------------------------------------------------------------------------

@source("sor_record")
def ed_sors_sor_record():
    """"Identity Authority - SOR record" -- one row per SOR record entry.

    Divergences from the SPL, each deliberate:

      * FULL SNAPSHOT, and NO DN RECONSTRUCTION -- same reasons as
        ed_people_canonical. The SPL builds
        dn = "uid=" . uid . ",ou=" . ou . ",ou=sors,dc=weill,dc=cornell,dc=edu",
        which additionally assumes every SOR branch is exactly one level deep.
      * `where ... len(uid) > 20` is dropped: the mirror image of canonical's
        `len(uid) < 20`, and only there to separate ou=sors from ou=people
        inside a shared lookup. Separate base DNs do that here.
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
    """
    rows = ldap_search(
        "ed-sors",
        "(objectClass=weillCornellEduSORRecord)",
        # weillCornellEduReleaseCode is requested by the SPL and never read --
        # see the dropped columns above -- so it is not requested here.
        ["weillCornellEduCWID", "uid", "ou", "weillCornellEduSORID",
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
         "weillCornellEduCredentialedLocation"])

    out = {}
    for row in rows:
        cwid = row.get("weillCornellEduCWID")
        if not cwid:
            continue                       # SPL: where isnotnull(...CWID)
        out[row.entry_dn] = {
            "weillCornellEduCWID": cwid,
            "uid": row.get("uid"),
            "ou": row.get("ou"),
            "weillCornellEduSORID": row.get("weillCornellEduSORID"),
            "givenName": row.get("givenName"),
            "sn": row.get("sn"),
            "weillCornellEduMiddleName": row.get("weillCornellEduMiddleName"),
            "displayName": _display_name(row),
            "weillCornellEduPrimaryDepartment":
                row.get("weillCornellEduPrimaryDepartment"),
            "weillCornellEduPrimaryDepartmentCode":
                row.get("weillCornellEduPrimaryDepartmentCode"),
            # Not on any nomv line in this search: the status of ONE SOR record,
            # unlike the canonical entry's status, which aggregates every SOR a
            # person has and is nomv'd there for exactly that reason.
            "weillCornellEduStatus": row.get("weillCornellEduStatus"),
            "weillCornellEduPrimaryTitle": row.get("weillCornellEduPrimaryTitle"),
            "weillCornellEduPrimaryTitleCode":
                row.get("weillCornellEduPrimaryTitleCode"),
            "weillCornellEduWorkingTitle": row.get("weillCornellEduWorkingTitle"),
            "weillCornellEduPrimaryRole": row.get("weillCornellEduPrimaryRole"),
            "weillCornellEduPrimaryRoleCode":
                row.get("weillCornellEduPrimaryRoleCode"),
            "weillCornellEduStartDate": row.get("weillCornellEduStartDate"),
            "weillCornellEduEndDate": row.get("weillCornellEduEndDate"),
            "weillCornellEduDegree": row.get("weillCornellEduDegree"),
            "personalTitle": row.get("personalTitle"),
            "telephoneNumber": row.get("telephoneNumber"),
            "postalCode": row.get("postalCode"),
            "weillCornellEduDOB": row.get("weillCornellEduDOB"),
            "street": row.get("street"),
            "l": row.get("l"),
            "st": row.get("st"),
            "mail": row.get("mail"),
            "weillCornellEduPreferredGivenName":
                row.get("weillCornellEduPreferredGivenName"),
            "weillCornellEduProviderID": row.get("weillCornellEduProviderID"),

            # `nomv` + `rex "s/\n/|/g"` in this search -- pipe-joined in the DB
            # today, so mv(), never row.get().
            "o": mv(row, "o"),
            "title": mv(row, "title"),
            "weillCornellEduFTE": mv(row, "weillCornellEduFTE"),
            "weillCornellEduDepartment": mv(row, "weillCornellEduDepartment"),
            "weillCornellEduDepartmentCode":
                mv(row, "weillCornellEduDepartmentCode"),
            "weillCornellEduPersonTypeCode":
                mv(row, "weillCornellEduPersonTypeCode"),
            "weillCornellEduCredentialedLocation":
                mv(row, "weillCornellEduCredentialedLocation"),
            # The SPL `nomv`s weillCornellEduProgram but seds
            # weillCornellEduProgramCode -- a copy-paste slip that leaves Program
            # NEWLINE-joined in the lookup (nomv with no separator swap) and
            # ProgramCode never joined at all (the sed has no multivalue left to
            # act on). Both are multi-valued in ED, so both go through mv() here
            # and both will differ from Splunk on the first diff. That is the bug
            # being fixed, not a regression.
            "weillCornellEduProgram": mv(row, "weillCornellEduProgram"),
            "weillCornellEduProgramCode": mv(row, "weillCornellEduProgramCode"),

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

            # Straight off the entry. See the dropped join above.
            "company": row.get("o;company"),

            "createTimestamp": _ts(row, "createTimestamp"),
            "modifyTimestamp": _ts(row, "modifyTimestamp"),
            "weillCornellEduHireDate": _ts(row, "weillCornellEduHireDate"),
        }
    return out


# ==========================================================================
# SLICE: roles-org (types: sor_role_record, organization)
# ==========================================================================
@source("sor_role_record")
def ed_sor_role_record():
    """Identity Authority - SOR role record: every role record under ou=sors.

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
    concatenates a multi-value into the key -- Splunk expands that into one row
    per ou, and `_key = dn` then makes the lookup key depend on which ou came
    back first. row.entry_dn is the DN the server actually holds: correct, and
    stable across runs, which is what a deletion set difference needs.
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
        # PORT_SPEC.md carries the confirming query. Implemented here as the
        # intent rather than the behaviour, so expect real rows to flip to
        # "true" on cutover: that is the bug being fixed, not a regression.
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
            "weillCornellEduCWID": row.get("weillCornellEduCWID"),
            "uid": row.get("uid"),
            # ou is multi-valued here -- it is what makes the SPL's dn case()
            # unstable. Joined rather than first-value-wins.
            "ou": mv(row, "ou"),
            "weillCornellEduSORID": row.get("weillCornellEduSORID"),
            "weillCornellEduExitReason": row.get("weillCornellEduExitReason"),
            "weillCornellEduStatus": row.get("weillCornellEduStatus"),
            "o": mv(row, "o"),                       # SPL nomv
            "title": row.get("title"),
            "weillCornellEduTitleCode": row.get("weillCornellEduTitleCode"),
            "weillCornellEduRole": row.get("weillCornellEduRole"),
            "weillCornellEduRoleCode": row.get("weillCornellEduRoleCode"),
            "weillCornellEduStartDate": row.get("weillCornellEduStartDate"),
            "weillCornellEduEndDate": row.get("weillCornellEduEndDate"),
            # SPL: eval weillCornellEduExpectedGradYear = max(...). Genuinely
            # multi-valued on some records and the SPL takes the largest, not
            # the first, so this is max() over every value rather than mv().
            "weillCornellEduExpectedGradYear": max(
                row.all("weillCornellEduExpectedGradYear"), default=""),
            "weillCornellEduDepartment": row.get("weillCornellEduDepartment"),
            "weillCornellEduDepartmentCode": row.get("weillCornellEduDepartmentCode"),
            "weillCornellEduProgram": mv(row, "weillCornellEduProgram"),          # SPL nomv
            "weillCornellEduProgramCode": mv(row, "weillCornellEduProgramCode"),  # SPL nomv
            "weillCornellEduFTE": row.get("weillCornellEduFTE"),
            "weillCornellEduPrimaryEntry": primary,
            "weillCornellEduType": row.get("weillCornellEduType"),
            "weillCornellEduDegree": row.get("weillCornellEduDegree"),
            "weillCornellEduDegreeCode": row.get("weillCornellEduDegreeCode"),
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
            # New columns, carried not coalesced. These need DDL before the
            # first live run. The ";level1"/";level2" LDAP options are spelled
            # Level1/Level2 in the column name because a ";" in a column name
            # is a fight with SQL for no gain.
            "weillCornellEduOrgUnit": mv(row, "weillCornellEduOrgUnit"),
            "weillCornellEduOrgUnitLevel1": row.get("weillCornellEduOrgUnit;level1"),
            "weillCornellEduOrgUnitLevel2": row.get("weillCornellEduOrgUnit;level2"),
            "weillCornellEduOrgUnitCode": mv(row, "weillCornellEduOrgUnitCode"),
            "weillCornellEduOrgUnitCodeLevel1": row.get("weillCornellEduOrgUnitCode;level1"),
            "weillCornellEduOrgUnitCodeLevel2": row.get("weillCornellEduOrgUnitCode;level2"),
            # The SPL rebuilds these with six substr() calls off an ISO string;
            # ldap3 parses GeneralizedTime into a datetime and _flatten renders
            # it isoformat, so the same "YYYY-MM-DD HH:MM:SS" is a slice and a
            # separator swap. Its `eval createTimestamp = max(createTimestamp)`
            # is a no-op guard against a multi-valued operational attribute;
            # _flatten's first-value is the same value for a single-valued one.
            "createTimestamp": row.get("createTimestamp")[:19].replace("T", " "),
            "modifyTimestamp": row.get("modifyTimestamp")[:19].replace("T", " "),
        }
    # Dropped from the SPL, deliberately:
    #   * the paired "db update" search's 2-day modifyTimestamp window. This is
    #     a full snapshot -- see READS ARE FULL SNAPSHOTS above. Deletion cannot
    #     be decided from a window.
    #   * `where isnotnull(dn)`, which existed because the case() above could
    #     produce a null DN when uid or ou was missing. entry_dn is never null.
    return out


@source("organization")
def ed_organization():
    """Identity Authority - Organization: weillCornellEduOrgUnit group entries.

    Runs unpaged in the SPL, like the role-record search; ldap_search() pages.

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
    in ED, and the deletion set difference will nominate every one of them the
    first time it is run against real DNs. PORT_SPEC.md lists that as an open
    question; it is worth a diff of the old table against this source's keys
    before DELETE_MODE is flipped.

    All of it is deleted here in favour of row.entry_dn.

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
            "weillCornellEduSORID": row.get("weillCornellEduSORID"),
            "weillCornellEduDepartment": row.get("weillCornellEduDepartment"),
            # weillCornellEduDepartment;academic -- read as a subtyped key
            # instead of the SPL's `rename weillCornellEduDepartment;academic{}`.
            # The `{}` is Splunk telling us it is an array, so it is joined.
            "weillCornellEduDepartmentAcademic": mv(row, "weillCornellEduDepartment;academic"),
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
            "weillCornellEduSource": row.get("weillCornellEduSource"),
            "weillCornellEduSubtype": row.get("weillCornellEduSubtype"),
            "weillCornellEduType": row.get("weillCornellEduType"),
            "weillCornellEduAlias": mv(row, "weillCornellEduAlias"),             # SPL nomv
            "weillCornellEduStatus": row.get("weillCornellEduStatus"),
            "telephoneNumber": row.get("telephoneNumber"),
            "labeledURI": row.get("labeledURI"),
            "o": mv(row, "o"),
            "createTimestamp": row.get("createTimestamp")[:19].replace("T", " "),
            "modifyTimestamp": row.get("modifyTimestamp")[:19].replace("T", " "),
        }
    # Dropped: the "Organization, db update" 2-day modifyTimestamp window and
    # its `where isnotnull(dn)`, for the same reasons as the role-record source.
    # Not ported here: "Organization roles", which reads the same entries from
    # the bare `ed` alias to build a CWID-per-org roles table. That is a second
    # table (_organization_roles) and belongs in its own source.
    return out

# ==========================================================================
# SLICE: contact (types: email, phone, location, location_master)
# ==========================================================================
# ---------------------------------------------------------------------------
# Contact and location feeds
# ---------------------------------------------------------------------------
# ed-emails, ed-phones, ed-locations and the bare `ed` root are UNRESOLVED
# candidates in BASE_DN. These four functions are written against the alias
# names only; --spike is what proves the base DNs. Nothing below reconstructs
# or hard-codes one.
#
# All four read FULL SNAPSHOTS. Every SPL builder in this slice except Location
# Master carries `(modifyTimestamp>=now-3d)` and writes with `outputlookup
# append=t`, which makes `source='ed'` mean "seen at some point since the
# lookup was last truncated" rather than "currently in ED" -- the exact fact
# the deletion set difference depends on. Dropping the window is not an
# optimisation, it is what makes deletion sound.


def _contact_ts(row, attr):
    """createTimestamp / modifyTimestamp as "YYYY-MM-DD HH:MM:SS".

    This is the whole of the SPL's substr() chain. Its offsets --
    substr(x,1,4), (6,2), (9,2), (12,2), (15,2), (18,2) -- only line up if
    Splunk has already handed it "2026-09-05T12:34:56Z", so the chain does
    nothing but swap the "T" for a space and drop the zone. ldap3 parses
    generalizedTime into a datetime and _flatten() renders it ISO, so the same
    swap is the entire port of it.

    Location and Location Master stop at (15,2), i.e. minute precision. That
    truncation is not a rule, it is a shorter copy of the same chain, so this
    keeps the seconds for all four feeds. Harmless for a DATETIME column;
    confirm the column type if any consumer parses the string with "%H:%M".
    """
    value = row.get(attr)
    return value[:19].replace("T", " ") if value else ""


@source("email")
def ed_emails_contact():
    """Contact email entries, keyed on the real entry DN.

    Dropped from the SPL: `eval dn = "uid=" . uid . ",ou=emails,ou=contacts,
    ..."`. row.entry_dn is the real thing. The SPL also asked for
    weillCornellEduSubtype twice; once is enough.
    """
    rows = ldap_search(
        "ed-emails",
        "(objectClass=weillCornellEduContactEmail)",
        ["uid", "weillCornellEduCWID", "weillCornellEduPrimaryEntry", "mail",
         "ou", "weillCornellEduReleaseCode", "weillCornellEduSource",
         "weillCornellEduSubtype", "weillCornellEduType",
         "createTimestamp", "modifyTimestamp"])
    return {r.entry_dn: {
        "uid": r.get("uid"),
        "weillCornellEduCWID": r.get("weillCornellEduCWID"),
        # Raw, as the SPL stores it. The port spec's normalisation of this
        # attribute to a real boolean is proposed for sor_role_record only and
        # explicitly leaves email/phone/location as an open decision -- so this
        # keeps today's value rather than quietly changing three tables.
        "weillCornellEduPrimaryEntry": r.get("weillCornellEduPrimaryEntry"),
        "mail": r.get("mail"),
        "ou": mv(r, "ou"),                          # nomv'd in the SPL
        # nomv'd in the SPL, so pipe-joined. OPEN QUESTION: ED stores release
        # codes subtyped (weillCornellEduReleaseCode;person, ;picture) and an
        # unqualified LDAP request returns the subtyped descriptions too, which
        # ldap3 hands back under their own keys -- so this plain read may be
        # empty in the port and may also have been empty in Splunk. None of the
        # contact searches run `spath`, so the file cannot answer it. Measure
        # `SELECT COUNT(weillCornellEduReleaseCode) FROM _contact_email` before
        # deciding whether to read the subtypes explicitly.
        "weillCornellEduReleaseCode": mv(r, "weillCornellEduReleaseCode"),
        "weillCornellEduSource": r.get("weillCornellEduSource"),
        "weillCornellEduSubtype": r.get("weillCornellEduSubtype"),
        # Multi-valued in ED -- proven by the phone aliasing below. get() would
        # take the first value and hide the rest.
        "weillCornellEduType": mv(r, "weillCornellEduType"),
        "createTimestamp": _contact_ts(r, "createTimestamp"),
        "modifyTimestamp": _contact_ts(r, "modifyTimestamp"),
    } for r in rows if r.entry_dn}


@source("phone")
def ed_phones_contact():
    """Desk and mobile contact entries in ONE paged search.

    THIS FIXES A LIVE DATA LOSS. weillCornellEduType is multi-valued and is
    compared against a value set, so LDAP returns an entry from BOTH arms of
    the SPL's two-branch search when it carries a desk number and a mobile.
    The SPL then builds its key from uid alone -- `eval dn = "uid=" . uid .
    ",ou=telephoneNumbers,ou=contacts,..."` -- ignoring type, so the desk row
    and the mobile row collide on one _key in identity_authority_phone and
    whichever writes last wins. Anyone with both numbers loses one of them,
    non-deterministically, on every run. Keying on row.entry_dn fixes it
    outright: the desk entry and the mobile entry are different LDAP entries
    with different DNs, so both survive.

    The SPL's mobile branch is `append [ | ldapfilter ... ]` -- a subsearch
    whose first command is ldapfilter, a STREAMING command given no input
    events -- so it may always have returned nothing, and it is capped by the
    subsearch row/time limit even when it does not. Here both types come from
    one paged search over the branch: no append, no subsearch, no cap. Expect
    more rows than Splunk produced and do not tune it back.
    """
    rows = ldap_search(
        "ed-phones",
        """(|(weillCornellEduType=telephoneNumber)
             (weillCornellEduType=mobile))""",
        ["uid", "telephoneNumber", "mobile", "weillCornellEduCWID",
         "weillCornellEduPrimaryEntry", "weillCornellEduReleaseCode",
         "weillCornellEduSource", "weillCornellEduSubtype",
         "weillCornellEduType", "createTimestamp", "modifyTimestamp", "ou"])
    return {r.entry_dn: {
        "uid": r.get("uid"),
        # The SPL's `rename mobile as telephoneNumber` -- one column, fed by
        # whichever attribute the entry carries. weillCornellEduType below is
        # what tells the two apart, which is why it is emitted: the SPL's table
        # carries it too but its key ignores it.
        "telephoneNumber": r.get("telephoneNumber") or r.get("mobile"),
        "weillCornellEduCWID": r.get("weillCornellEduCWID"),
        "weillCornellEduPrimaryEntry": r.get("weillCornellEduPrimaryEntry"),
        "weillCornellEduReleaseCode": mv(r, "weillCornellEduReleaseCode"),
        "weillCornellEduSource": r.get("weillCornellEduSource"),
        "weillCornellEduSubtype": r.get("weillCornellEduSubtype"),
        # Pipe-joined, so an entry carrying both types is visible in the data
        # instead of silently deciding which number survives.
        "weillCornellEduType": mv(r, "weillCornellEduType"),
        "createTimestamp": _contact_ts(r, "createTimestamp"),
        "modifyTimestamp": _contact_ts(r, "modifyTimestamp"),
        "ou": mv(r, "ou"),                          # nomv'd in the SPL
    } for r in rows if r.entry_dn}


@source("location")
def ed_locations_contact():
    """Person-to-location contact entries, keyed on the real entry DN.

    Two SPL lines dropped, both lossy:
      * `eval dn = "uid=" . uid . ",ou=locations,ou=contacts,..."` -- row.entry_dn.
      * `dedup uid` -- with a uid-derived dn that dedup looks free, but a person
        with two location entries has two real DNs, and dedup threw the second
        away. Keying on entry_dn keeps both, so this feed should return more
        rows than Splunk. That is correct.

    Note against the brief: this search does NOT carry postalCode/street/l/st.
    Those are Location Master's, below.
    """
    rows = ldap_search(
        "ed-locations",
        "(weillCornellEduType=location)",
        ["ou", "uid", "roomNumber", "weillCornellEduLocationCode",
         "weillCornellEduCWID", "weillCornellEduPrimaryEntry",
         "weillCornellEduReleaseCode", "weillCornellEduSource",
         "weillCornellEduSubtype", "weillCornellEduType",
         "createTimestamp", "modifyTimestamp"])
    return {r.entry_dn: {
        "ou": mv(r, "ou"),                          # nomv'd in the SPL
        "uid": r.get("uid"),
        "roomNumber": r.get("roomNumber"),
        "weillCornellEduLocationCode": r.get("weillCornellEduLocationCode"),
        "weillCornellEduCWID": r.get("weillCornellEduCWID"),
        "weillCornellEduPrimaryEntry": r.get("weillCornellEduPrimaryEntry"),
        # Same open question as email -- see ed_emails_contact().
        "weillCornellEduReleaseCode": mv(r, "weillCornellEduReleaseCode"),
        "weillCornellEduSource": r.get("weillCornellEduSource"),
        "weillCornellEduSubtype": r.get("weillCornellEduSubtype"),
        "weillCornellEduType": mv(r, "weillCornellEduType"),
        # The SPL wrapped these in max(), which is a no-op over a single value
        # and only existed because its substr() chain could fan out over a
        # multivalue. Not ported.
        "createTimestamp": _contact_ts(r, "createTimestamp"),
        "modifyTimestamp": _contact_ts(r, "modifyTimestamp"),
    } for r in rows if r.entry_dn}


@source("location_master")
def ed_location_master():
    """The building/room master list, from the bare `ed` root alias.

    THE MOST DANGEROUS SEARCH IN THE JOB, and the reason MIN_ROWS exists.
    `outputlookup identity_authority_location_master` has NO append=t, and
    override_if_empty defaults to true, so an ldapsearch that returns zero rows
    REPLACES the master list with nothing. `Location Master - DN from ED` then
    contributes no DNs, every row of `_location` reads as 'db only', and the
    whole table is nominated for deletion -- from one bad LDAP call, with
    nothing in Splunk capping the volume. Here the floor in MIN_ROWS
    ("location_master": 100) plus run_sources()'s abort stops the run before
    anything is compared or written, and MAX_DELETE_FRACTION is the second net.

    It also runs unpaged today, so the directory silently caps it and a capped
    run replaces the master list with the short one. ldap_search() pages.

    Dropped: `eval dn = "cn=" . cn . ",ou=locations,ou=Groups,..."` and the
    `dedup dn` that follows it. cn is multi-valued in LDAP, so that
    reconstruction can produce a multi-valued dn -- an unstable _key, with
    dedup then discarding siblings on it. row.entry_dn is single and real, so
    cn is not even requested here; the SPL only wanted it to build the string.
    """
    rows = ldap_search(
        "ed",
        "(&(objectClass=groupOfURLs)(ou=locations))",
        ["weillCornellEduLocationCode", "physicalDeliveryOfficeName",
         "postalAddress", "l", "st", "postalCode", "street",
         "createTimestamp", "modifyTimestamp"])
    return {r.entry_dn: {
        "weillCornellEduLocationCode": r.get("weillCornellEduLocationCode"),
        "physicalDeliveryOfficeName": r.get("physicalDeliveryOfficeName"),
        "postalAddress": r.get("postalAddress"),
        "l": r.get("l"),
        "st": r.get("st"),
        "postalCode": r.get("postalCode"),
        "street": r.get("street"),
        "createTimestamp": _contact_ts(r, "createTimestamp"),
        "modifyTimestamp": _contact_ts(r, "modifyTimestamp"),
    } for r in rows if r.entry_dn}


# ==========================================================================
# SLICE: dbside (types: db reads)
# ==========================================================================
# ---------------------------------------------------------------------------
#                                  DATABASE
# ---------------------------------------------------------------------------

# The Identity Authority database is MSSQL: Splunk reached it through the
# `Identity_Authority` DB Connect connection and every `<X> - DN` search is
# plain T-SQL against it. So this side is pymssql, not the pymysql
# buildIdentity.py uses -- the two jobs write different servers, and the
# upsert form differs with them (MERGE here, ON DUPLICATE KEY UPDATE there).
#
# NONE OF THESE FOUR SECRETS EXIST TODAY. reciter-inst-secrets carries
# LDAP_BIND_PASSWORD and the ASMS trio (MSSQL_DB_URL / MSSQL_DB_USERNAME /
# MSSQL_DB_PASSWORD) and nothing else; the Identity Authority credentials live
# only inside Splunk's DB Connect connection object, which this job cannot
# read. Someone must create IA_DB_URL, IA_DB_USERNAME, IA_DB_PASSWORD and
# IA_DB_NAME in the cluster before a live run is possible.
#
# Deliberately NOT defaulted to the MSSQL_DB_* variables. ASMS is a different
# server; silently writing Identity Authority rows into it would be far worse
# than refusing to start.
IA_DB_ENV = ("IA_DB_URL", "IA_DB_USERNAME", "IA_DB_PASSWORD")

# Every table is keyed on the entry DN -- that is what the `<X> - DN` searches
# select and what plan_deletions reconciles on. MERGE requires it to be unique;
# db_dns() warns if the table says otherwise.
KEY_COLUMN = "dn"

# T-SQL caps a VALUES row constructor at 1000 rows, so a batch can never exceed
# it however many columns a type has.
MERGE_MAX_ROWS = 1000
DELETE_CHUNK = 500

# The soft-delete marker column. NOT VERIFIED against the real schema: the
# `<X> - DN` searches only ever SELECT dn, and the write side goes through the
# opaque IdentityAuthority_dn stanza, so no query in the SPL names this column.
# apply_deletions checks INFORMATION_SCHEMA before writing and aborts if it is
# missing, so a wrong guess fails loudly rather than half-running.
DELETE_FLAG_COLUMN = os.environ.get("IA_DELETE_FLAG_COLUMN", "deleted")


def db_conn():
    """Connection to the Identity Authority MSSQL database.

    autocommit=False is explicit, not decorative: upsert() and apply_deletions()
    both depend on every statement in the run landing in one transaction that a
    single failure rolls back whole.
    """
    import pymssql  # lazy: --demo and --spike must run with no driver installed

    missing = [k for k in IA_DB_ENV if not os.environ.get(k)]
    if missing:
        raise SystemExit(
            "ABORT: %s not set. These are NEW secrets -- reciter-inst-secrets "
            "has only LDAP_BIND_PASSWORD and the ASMS MSSQL credentials today. "
            "Create IA_DB_URL, IA_DB_USERNAME, IA_DB_PASSWORD and IA_DB_NAME "
            "from Splunk's Identity_Authority DB Connect connection before "
            "running live. Do NOT point them at MSSQL_DB_* -- that is ASMS."
            % ", ".join(missing))

    host, port = _mssql_target(os.environ["IA_DB_URL"])
    return pymssql.connect(
        server=host, port=port,
        user=os.environ["IA_DB_USERNAME"],
        password=os.environ["IA_DB_PASSWORD"],
        # Database name is a guess until conf-db_outputs or the connection
        # object is read; set IA_DB_NAME rather than editing this default.
        database=os.environ.get("IA_DB_NAME", "IdentityAuthority"),
        autocommit=False,
        login_timeout=30,
        timeout=600,
    )


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


def db_dns(types):
    """{type_name: set(dn)} straight from the IA tables.

    Replaces the eight `<X> - DN` dbxquery searches. Theirs run with
    maxrows=5000000, which is a silent cap, not a limit anyone measured -- a
    truncated read makes rows look absent from the DB, and absent-from-DB is
    the input to a set difference. Here the whole column is read and counted.

    (The parameter of plan_deletions is also called db_dns; it shadows this
    function only inside that function's body, where it is exactly this data.)
    """
    conn = db_conn()
    out = {}
    try:
        cur = conn.cursor()
        for type_name in sorted(set(types)):
            if type_name not in TABLES:
                raise SystemExit("ABORT: no table mapped for type %r" % type_name)
            table = TABLES[type_name]
            cur.execute("SELECT %s FROM %s WHERE %s IS NOT NULL"
                        % (_ident(KEY_COLUMN), _ident(table), _ident(KEY_COLUMN)))
            fetched = [r[0] for r in cur.fetchall()]
            dns = {d for d in fetched if d}
            out[type_name] = dns
            logger.info("db %-16s %-24s rows=%d distinct=%d",
                        type_name, table, len(fetched), len(dns))
            if len(fetched) != len(dns):
                # MERGE raises "attempted to UPDATE or DELETE the same row more
                # than once" against a non-unique key, so this is a real defect
                # in the table, not a curiosity.
                logger.warning("%s has %d duplicate/blank %s values -- the "
                               "upsert key is not unique there",
                               table, len(fetched) - len(dns), KEY_COLUMN)
    finally:
        conn.close()
    return out


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


def _merge_sql(table, columns, n_rows):
    """One MERGE covering n_rows, parameterised, nothing interpolated but names.

    Three things here are load-bearing, each because the SPL or the sibling
    port got it wrong:

      * It is an upsert, never delete-then-insert. The SPL's dbxoutput stanzas
        are opaque, and a replace-shaped one is how a partial ED read becomes a
        row that vanishes and comes back.
      * COALESCE(src.c, tgt.c): a NULL from this run leaves whatever is already
        there. Without it the sibling port would have wiped 237 primaryProgram
        and 261 primaryOrg values that had been correct for years (measured
        2026-09-05). The trade-off is that a value can be replaced but never
        cleared; clearing is a separate, explicit operation.
      * The UPDATE targets are table-qualified. The sibling port shipped
        unqualified ones and got (1052, "Column 'x' in UPDATE is ambiguous") on
        the go-live attempt, having passed three dry runs -- because dry-run
        returned before the write. demo() asserts on this string for that
        reason.

    WITH (HOLDLOCK) closes MERGE's insert/update race under concurrency. The
    key is never in the SET clause: it is the join condition.

    Every placeholder is CAST to NVARCHAR. pymssql interpolates parameters
    client-side, so a None arrives as a bare NULL literal, and SQL Server types
    a bare NULL in a VALUES constructor as int. A batch in which one column is
    NULL on every row would then COALESCE an int against an nvarchar column,
    and type precedence makes SQL Server convert the EXISTING value to int --
    "Conversion failed when converting the nvarchar value ... to data type int"
    on rows that were perfectly fine. The CAST pins the source side to text and
    lets the column's own type drive the conversion, which is what DB Connect's
    non-strict write did.
    """
    cols = [KEY_COLUMN] + [c for c in columns if c != KEY_COLUMN]
    q = [_ident(c) for c in cols]
    row_ph = "(" + ", ".join(["CAST(%s AS NVARCHAR(MAX))"] * len(cols)) + ")"
    values = ", ".join([row_ph] * n_rows)
    updates = ",\n".join(
        "    tgt.{c} = COALESCE(src.{c}, tgt.{c})".format(c=name)
        for name in q[1:])
    if not updates:
        raise ValueError("nothing to update for %s: no columns beyond the key" % table)
    return (
        "MERGE INTO {table} WITH (HOLDLOCK) AS tgt\n"
        "USING (VALUES {values}) AS src ({cols})\n"
        "    ON tgt.{key} = src.{key}\n"
        "WHEN MATCHED THEN UPDATE SET\n"
        "{updates}\n"
        "WHEN NOT MATCHED THEN INSERT ({cols})\n"
        "    VALUES ({srccols});".format(
            table=_ident(table), values=values, cols=", ".join(q),
            key=q[0], updates=updates,
            srccols=", ".join("src." + c for c in q)))


def _merge_params(dn, row, columns):
    return [dn] + [_null_if_blank(row.get(c)) for c in columns]


def upsert(built, dry_run=False):
    """Write every type to its table. Returns {type_name: rows written}.

    One connection, one transaction, one rollback: a failure on the eighth
    table must not leave the first seven half-applied. That is the property the
    Splunk job cannot have at all, since each `db update` search is its own
    independent write.
    """
    conn = None if dry_run else db_conn()
    written = {}
    try:
        cur = None if conn is None else conn.cursor()
        for type_name, rows in sorted(built.items()):
            if type_name not in TABLES:
                raise SystemExit("ABORT: no table mapped for type %r" % type_name)
            table = TABLES[type_name]
            if not rows:
                logger.warning("upsert %-16s no rows, skipped", type_name)
                continue
            columns = _columns_for(rows)
            size = _batch_size(columns)
            items = sorted(rows.items())
            batches = -(-len(items) // size)

            if dry_run:
                # SQL text only, and shown one row wide: a real batch just
                # repeats the row constructor, and 1000 of them buries the log.
                # Every value is a placeholder either way, so no mail / mobile /
                # postalCode VALUE can reach a log from here.
                logger.info("--dry-run %s: %d rows in %d batch(es) of up to %d. "
                            "Statement (one row shown):\n%s",
                            table, len(items), batches, size,
                            _merge_sql(table, columns, 1))
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
                cur.execute(_merge_sql(table, columns, len(chunk)), tuple(params))
                done += len(chunk)
            written[type_name] = done
            logger.info("upsert %-16s %-24s rows=%d cols=%d",
                        type_name, table, done, len(columns))
        if conn is not None:
            conn.commit()
            logger.info("upsert committed: %d rows across %d tables",
                        sum(written.values()), len(written))
    except Exception:
        if conn is not None:
            conn.rollback()
            logger.error("upsert rolled back -- no table was modified")
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


def _delete_marker(cur, table):
    """Value to write into DELETE_FLAG_COLUMN, chosen from its declared type.

    The column name is a guess (see DELETE_FLAG_COLUMN), so confirm it exists
    before writing anything: a missing column mid-transaction rolls back the
    upsert too, and the error would name the column without saying it was never
    verified.
    """
    import datetime

    cur.execute("SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_NAME = %s AND COLUMN_NAME = %s",
                (table, DELETE_FLAG_COLUMN))
    row = cur.fetchone()
    if not row:
        raise SystemExit(
            "ABORT: %s has no column %r, so DELETE_MODE='flag' has nowhere to "
            "write. That column name is a guess -- no query in the SPL names "
            "it, because the write goes through the opaque IdentityAuthority_dn "
            "stanza. Read conf-db_outputs, then set IA_DELETE_FLAG_COLUMN. "
            "Nothing has been written." % (table, DELETE_FLAG_COLUMN))
    dtype = (row[0] or "").lower()
    if "date" in dtype or "time" in dtype:
        return datetime.datetime.utcnow()
    if "char" in dtype or "text" in dtype:
        return "deleted"
    return 1


def apply_deletions(plan, dry_run=False):
    """Consume plan_deletions()' output under DELETE_MODE. Returns per-type counts."""
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
            build = _flag_sql if DELETE_MODE == "flag" else _delete_sql
            if dry_run:
                # DNs stay parameters, never inlined: a location or email DN
                # carries identifying values. One placeholder shown, as above.
                logger.info("--dry-run %s: %s %d dn(s). Statement (one dn "
                            "shown):\n%s", table, DELETE_MODE, len(dns), build(table, 1))
                applied[type_name] = len(dns)
                logger.info("deletions %-16s %-24s mode=%s count=%d  "
                            "(dry run, nothing executed)",
                            type_name, table, DELETE_MODE, len(dns))
                continue

            marker = _delete_marker(cur, table) if DELETE_MODE == "flag" else None
            for chunk in _chunks(list(dns), DELETE_CHUNK):
                params = list(chunk) if marker is None else [marker] + list(chunk)
                cur.execute(build(table, len(chunk)), tuple(params))
            applied[type_name] = len(dns)
            logger.info("deletions %-16s %-24s mode=%s count=%d",
                        type_name, table, DELETE_MODE, len(dns))
        if conn is not None:
            conn.commit()
            logger.info("deletions committed: %d rows across %d tables (mode=%s)",
                        sum(applied.values()), len(applied), DELETE_MODE)
    except Exception:
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

_demo_logic = demo   # the source-side demo above; extended below, not replaced


def demo():
    """Source-side logic, then the SQL builders. No network, no DB, no driver."""
    _demo_logic()

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
            "every column must COALESCE or a NULL run erases the DB: %r" % line
    assert "tgt.[dn] = COALESCE" not in sql, "the merge key must not be updated"
    assert sql.count("%s") == 2 * 4, "one placeholder per column per row"
    # An all-NULL column in a batch must not be typed int by the server; see
    # _merge_sql. Without the CAST this fails only on live data, never in a test.
    assert sql.count("CAST(%s AS NVARCHAR(MAX))") == 2 * 4, sql
    assert "WITH (HOLDLOCK)" in sql, "MERGE races on concurrent insert without it"

    # A blank from ED must arrive as NULL, or COALESCE never fires and the
    # guard above is decorative.
    assert _null_if_blank("") is None and _null_if_blank("  ") is None
    assert _null_if_blank("x") == "x" and _null_if_blank(0) == 0
    assert _merge_params("cn=a,ou=x", {"mail": ""}, ["mail", "ou"]) == \
        ["cn=a,ou=x", None, None], "absent and blank columns both bind NULL"
    # The DN comes from the dict key, never from a "dn" column inside the row.
    assert _columns_for({"cn=a": {"dn": "WRONG", "ou": "x"}}) == ["ou"]

    assert 1 <= _batch_size(["a"]) <= MERGE_MAX_ROWS
    assert _batch_size(["c%d" % i for i in range(400)]) >= 1, "wide types still batch"
    try:
        _ident("dn; drop table x --")
        raise AssertionError("_ident must reject a non-identifier")
    except ValueError:
        pass

    flag = _flag_sql("_contact_email", 3)
    assert flag.startswith("UPDATE tgt"), flag
    assert "tgt.%s = %%s" % _ident(DELETE_FLAG_COLUMN) in flag, flag
    assert flag.count("%s") == 4, "marker plus one placeholder per dn"
    assert "DELETE" not in flag.split("SET", 1)[0].upper(), "flag mode never deletes"
    assert _delete_sql("_contact_email", 2).startswith("DELETE tgt"), "delete mode does"

    # The delete branch must refuse without the explicit confirmation, and must
    # refuse before it opens a connection -- so this runs offline.
    was_mode, was_env = DELETE_MODE, os.environ.pop("IA_DELETE_CONFIRMED", None)
    try:
        globals()["DELETE_MODE"] = "delete"
        try:
            apply_deletions({"email": ["cn=a"]}, dry_run=True)
            raise AssertionError("unconfirmed delete mode must abort")
        except SystemExit as exc:
            assert "IA_DELETE_CONFIRMED" in str(exc) and "conf-db_outputs" in str(exc)
    finally:
        globals()["DELETE_MODE"] = was_mode
        if was_env is not None:
            os.environ["IA_DELETE_CONFIRMED"] = was_env

    assert _mssql_target("jdbc:sqlserver://ia.db:1433;databaseName=IA") == ("ia.db", 1433)
    assert set(TABLES) >= set(MIN_ROWS), "every table needs a row floor"
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

    unresolved = UNRESOLVED_ALIASES & {fn.type_name for fn in SOURCES.values()}
    if unresolved:
        logger.warning("aliases still unresolved: %s", ", ".join(sorted(unresolved)))

    built = run_sources()
    logger.info("built %d types, %d rows total",
                len(built), sum(len(v) for v in built.values()))

    existing = db_dns(sorted(built))
    if args.no_delete:
        # plan_deletions aborts the process on its ceiling. Those aborts exist
        # to stop a deletion, so on an explicitly delete-free run they must not
        # be what stops the upsert.
        plan = {}
        logger.info("--no-delete: deletion pass skipped entirely")
    else:
        plan = plan_deletions(built, existing)

    upsert(built, dry_run=args.dry_run)
    apply_deletions(plan, dry_run=args.dry_run)
    if args.dry_run:
        logger.info("--dry-run complete: SQL logged, nothing written")
    return built
