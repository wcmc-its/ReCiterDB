#!/usr/bin/env python3
"""
Identity index for the Adversarial Attribution Review lanes.

In-memory index of reciterdb `identity` keyed by normalised surname, plus the
name-normalisation helpers and the person-type precedence table that both the
PubMed matcher (`aar_matcher`) and the Scopus detector (`aar_universe_scopus`)
share. Extracted from `aar_matcher.py` so the Scopus lane can reuse the roster
WITHOUT dragging in the matcher's S3 / pinned-model dependencies — which lets the
detector run in-cluster (reciterdb CronJob) with only sqlalchemy + PyMySQL.

Also home to the TEMPORAL PLAUSIBILITY penalty (issue #159): a candidate whose WCM
appointment ended long before the paper was published is almost always a homonym, so
the gap between the two years subtracts from that candidate's `confidence`, and is
published per row as `authorship_review.top_years_after_wcm`. See `temporal_penalty`.

Env: DB_USERNAME/DB_PASSWORD/DB_HOST/DB_NAME (reciterdb, read-only).
"""
import os, re, unicodedata

from sqlalchemy import create_engine, text

# person-type flags in reporting precedence order (mirrors the pubs skill CASE,
# extended with the columns that table actually carries). label + historical flag.
PERSON_TYPES = [
    ("fullTimeFaculty", "Full-Time Faculty", False),
    ("partTimeFaculty", "Part-Time Faculty", False),
    ("voluntaryFaculty", "Voluntary Faculty", False),
    ("adjunctFaculty", "Adjunct Faculty", False),
    ("emeritusFaculty", "Emeritus Faculty", True),
    ("inactiveFaculty", "Inactive Faculty", True),
    ("faculty", "Faculty", False),
    ("postdoc", "Postdoc", False),
    ("fellow", "Fellow", False),
    ("nonFaculty", "Non-Faculty", False),
    ("residentNYP", "Resident (NYP)", False),
    ("studentMDNYC", "Student MD (NYC)", False),
    ("studentMDPhD", "Student MD-PhD", False),
    ("studentMDQatar", "Student MD (Qatar)", False),
    ("studentPhDTriI", "Student PhD (Tri-I)", False),
    ("studentPhDWeill", "Student PhD (Weill)", False),
    ("inactiveNonAlumniStudent", "Inactive Student", True),
    ("alumniMD", "Alumni MD", True),
    ("alumniMDPHD", "Alumni MD-PhD", True),
    ("alumniPHD", "Alumni PhD", True),
    ("alumniResidentNYP", "Alumni Resident (NYP)", True),
]
_PTYPE_COLS = [c for c, _, _ in PERSON_TYPES]


# ---- normalisation ---------------------------------------------------------
def _norm(s):
    """Lowercase, strip accents, keep [a-z0-9] only. 'O’Brien'->'obrien'."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return "".join(ch for ch in s.lower() if ch.isalnum())


def name_tokens(s):
    """Name -> normalised word tokens. 'Han-Jo Kim' -> ('han', 'jo', 'kim').

    `_norm` collapses a whole string ('Kristin Lees' -> 'kristinlees'), which is what a
    surname index key wants and the opposite of what comparing a byline word by word
    wants -- the byline-owner gate (issue #174) needs to line up the FIRST token and the
    TRAILING tokens separately. Hyphens split like spaces because reciterdb carries both
    spellings of the same compound name: 'Han-Jo'/'Han Jo' in `identity.givenName`,
    'Lees-Haggerty'/'Lees Haggerty' between `person.lastName` and the PubMed byline in
    `person_article`. Apostrophes deliberately do NOT split -- `_norm` already folds
    "O'Brien" and 'OBrien' onto the same token, and splitting would pull them apart again.

    Anything that isn't a string is no tokens at all, not the token 'nan': reciterdb hands
    NULL through as None, and the pandas ledger hands a missing byline through as a float
    NaN that str() would happily turn into a name."""
    if not isinstance(s, str):
        return ()
    return tuple(t for t in (_norm(w) for w in re.split(r"[\s,.\-‐-―]+", s)) if t)


def _first_initial(fore, initials):
    """Author's first-name initial from ForeName, falling back to Initials."""
    for src in (fore, initials):
        n = _norm(src)
        if n:
            return n[0]
    return ""


def _byline_first_name(fore):
    """The byline forename's FIRST name token, for the `full` given-name tier -- but
    ONLY when every trailing token is a bare initial, i.e. the "Gregory W" shape
    (issue #185). Returns "" for every other shape, which leaves ranking exactly as
    it was.

    Whitespace split ONLY, deliberately NOT `name_tokens()`: that helper also splits
    on hyphens -- right for surname variants ('Han-Jo'/'Han Jo'), wrong here, where it
    would hand compound given names a false exact first name ('Chung-Han' -> 'chung').
    And the trailing-initial requirement is what keeps multi-token given names that
    are NOT First+Initial out ('Soo Young', 'Ana Paula' -- measured live, 12 of 1,136
    single-candidate flagged rows already hold a correct full-tier pick that a bare
    first-token split would introduce a false rival for). A single-token forename
    returns "" too: `_norm` over the whole field already covers it exactly."""
    if not isinstance(fore, str):
        return ""
    toks = [t for t in (_norm(w) for w in fore.split()) if t]
    if len(toks) < 2 or len(toks[0]) < 2:
        return ""
    if all(len(t) == 1 for t in toks[1:]):
        return toks[0]
    return ""


# ---- temporal plausibility -------------------------------------------------
# A paper carrying a WCM affiliation was written by someone who WAS at WCM when it
# was published, so a candidate whose appointment ended long before the paper is
# almost always a homonym. The identity table's `endDateWCMFaculty` /
# `endDateWCMStudent` are YEAR ints (2008, 2016 — NOT dates; YEAR() on them returns
# NULL) and people who are still here carry a FUTURE end year (2027 typical, 2099
# open-ended / emeritus), so a positive gap only ever arises for someone who has
# actually left. Identities with neither year are never penalised.
#
# RANK DOWN, DO NOT SUPPRESS: this is a penalty term, never a filter. Most open review
# rows whose top candidate has a past end year are papers published BEFORE that person
# left — exactly the missing attributions this pipeline exists to find — and
# late-career / posthumous / consortium output is real. A dropped row is also an
# invisible failure; a low-ranked one stays auditable.
#
# IT REACHES RANKING ONLY THROUGH `confidence`, which keeps its existing LAST position
# in both sort keys (here and in `aar_matcher.match_authorship`). It must never lead:
# in a lexicographic key the lead term's MAGNITUDE is irrelevant, so a leading penalty
# lets the smallest non-zero gap flip the top pick away from a full given-name +
# affiliation match, and sinks the penalised candidate below every unpenalised cohort
# member — past the top_k cut, which is suppression, not ranking down. The intended
# consequence: staleness breaks ties, it cannot turn a strong match into a weak one.
# On the Scopus lane (no identity-only score) it bites as a tiebreak after name and
# affiliation; on the PubMed lane it mostly will not move `top_cwid`. That is why the
# gap is ALSO published per row as `authorship_review.top_years_after_wcm` — a queryable
# column reaches the stale rows whose cohort is one person, where re-ranking is a no-op.
#
# `given_match == "full"` now LEADS both keys (`match_authorship` was reordered to agree
# with this file's key; see its docstring). That is the same magnitude argument used the
# other way round and it is deliberate: `full` is a BOOLEAN, so there is no magnitude to
# ignore, and a byline spelling the person's name out is stronger evidence than any
# ordering aid computed downstream of it. The penalty is unaffected — it still reaches
# ranking only through `confidence`, which is still LAST in both keys.
# Rate and cap measured against the live queue on 2026-08-28 (47,673 candidate entries
# on open rows, 11,279 of them past the grace, median gap 13y, widest 70y). The cap
# exists because an uncapped 0.05/yr pinned 1,065 of 2,771 stale top candidates at
# exactly 0.000, destroying the ordering the penalty exists to express. 0.01/yr puts the
# cap at a 20-year gap — the boundary of issue #159's top band — so 76% of stale
# candidates keep a graded penalty; beyond 20 years the exact gap lives in the
# `top_years_after_wcm` column, not in confidence.
#
# The cap does NOT guarantee a non-zero confidence, and an earlier version of this
# comment wrongly claimed it did. That claim came from `candidate_cwids_json`, which
# stores only the top 5 per row; `_confidence` runs over EVERY cohort member here,
# before the top_k cut. Replaying full cohorts, the smallest pre-penalty confidence is
# 0.052, not 0.167, and 141 candidate entries still clamp to 0.000 — e.g. agk9007
# (Agnes Kim, faculty end 2013) on open rows 24869/26720, where the author string
# carries no given name so given_match is "unknown": base 0.15 + rarity 0.40/266 −
# historical 0.10 = 0.052, and an 11-year gap costs 0.06. No operational effect today
# (those 141 sit far below the top-5 cut on 2 rows), but do not raise the cap on the
# assumption that a floor protects it.
TEMPORAL_GRACE_YEARS = 5          # publication lag / late output after the last appointment year
TEMPORAL_PENALTY_PER_YEAR = 0.01  # confidence points per stale year beyond the grace
TEMPORAL_PENALTY_CAP = 0.15       # reached at a 20y gap (issue #159's top band boundary)


def temporal_penalty(years_after_wcm):
    """Graduated demotion for a candidate proposed long after they left WCM.

    `years_after_wcm` = paper year - last WCM appointment year (None when either is
    unknown, negative when the paper predates the departure). Zero through the grace
    period, then TEMPORAL_PENALTY_PER_YEAR per further year, saturating at
    TEMPORAL_PENALTY_CAP. THIS function returns the authoritative, already-capped
    value — `_confidence` subtracts it once and nothing clamps it again."""
    if not years_after_wcm or years_after_wcm <= TEMPORAL_GRACE_YEARS:
        return 0.0
    return round(min(TEMPORAL_PENALTY_PER_YEAR * (years_after_wcm - TEMPORAL_GRACE_YEARS),
                     TEMPORAL_PENALTY_CAP), 3)


# ---- identity index --------------------------------------------------------
class IdentityIndex:
    """In-memory index of reciterdb `identity`, keyed by normalised surname."""

    def __init__(self, records):
        self.by_surname = {}
        for r in records:
            self.by_surname.setdefault(r["surname_norm"], []).append(r)

    @classmethod
    def load(cls):
        eng = create_engine(
            f"mysql+pymysql://{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}"
            f"@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}",
            connect_args={"connect_timeout": 15}, pool_pre_ping=True)
        cols = ("i." + ", i.".join(
            ["cwid", "givenName", "middleName", "surname", "primaryAcademicDepartment",
             "primaryAcademicDivision", "primaryTitle", "primaryProgram",
             "endDateWCMFaculty", "endDateWCMStudent"] + _PTYPE_COLS))
        # `identity` is HR/LDAP: it carries the LEGAL name. `person` mirrors DynamoDB
        # Identity.primaryName, which is the name the person actually publishes under.
        # Both cwid columns are utf8mb4_unicode_ci, so this join needs no COLLATE (the
        # 1267 trap is on authorship_review.top_cwid, which is general_ci — not here).
        with eng.connect() as c:
            rows = c.execute(text(
                f"SELECT {cols}, p.firstName AS prefFirstName "
                "FROM identity i "
                "LEFT JOIN person p ON p.personIdentifier = i.cwid "
                "WHERE i.surname IS NOT NULL AND i.surname <> ''")).mappings().all()
        return cls([cls._record(r) for r in rows])

    @staticmethod
    def _record(r):
        """One index record. `end_year` = the LATEST of the faculty/student WCM end
        years (both YEAR ints; None when both are null). Taking the max is the
        conservative reading — it penalises least — for the people who were here
        twice (student then faculty); which of the two really means "left WCM" is
        an open question on issue #159, so a reviewer can overrule this."""
        ptype, historical = "Other / CTSC", False
        for col, label, hist in PERSON_TYPES:
            if str(r[col]).lower() == "yes":
                ptype, historical = label, hist
                break
        given = r["givenName"] or ""
        ends = [y for y in (r["endDateWCMFaculty"], r["endDateWCMStudent"]) if y]
        return {
            "cwid": r["cwid"],
            "given": given, "middle": r["middleName"] or "", "surname": r["surname"] or "",
            "given_norm": _norm(given), "surname_norm": _norm(r["surname"]),
            "pref_norm": _norm(r.get("prefFirstName")),
            "dept": r["primaryAcademicDepartment"] or "",
            "division": r["primaryAcademicDivision"] or "",
            "program": r["primaryProgram"] or "",
            "title": r["primaryTitle"] or "",
            "person_type": ptype, "historical": historical,
            "end_year": max(ends) if ends else None,
        }

    def candidates(self, last, fore=None, initials=None, affiliations=None, top_k=5,
                   pub_year=None):
        """Ranked candidate CWIDs for one authorship (no identity-only score yet).

        `pub_year` is the paper's publication year; pass it to enable the temporal
        penalty (omitted -> `years_after_wcm` is None everywhere and ranking is exactly
        as it was before issue #159). The penalty is folded into `confidence` and
        reaches the sort key nowhere else, so it can only reorder candidates the name
        and affiliation evidence leaves tied — it can never sink one below a rival the
        evidence favours, and so can never push an evidence-favoured candidate past the
        top_k cut. (In a cohort larger than top_k a stale candidate can still lose the
        last slot to an evidence-EQUAL rival, exactly as any lower-confidence candidate
        does. That is ranking; what issue #159 forbids is suppression regardless of
        evidence, which a penalty at the FRONT of the key does and this does not.)

        Returns (candidates, cohort_size). cohort_size is the full homonym count
        (surname + initial) the curator faces, even if the list is capped at top_k."""
        surname_norm = _norm(last)
        if not surname_norm:
            return [], 0
        pool = self.by_surname.get(surname_norm, [])
        if not pool:
            return [], 0
        author_init = _first_initial(fore, initials)
        author_given = _norm(fore)
        # The whole-field blob above can never carry a middle initial to the `full`
        # tier: _norm("Gregory W") is "gregoryw", which equals nobody's first name, so
        # the dominant PubMed byline shape used to stall at `initial` and tie with
        # non-matching first names (issue #185). `author_first` is the byline's own
        # first name for exactly that shape ("" otherwise -- see _byline_first_name),
        # and below it is tested against given_norm and pref_norm ONLY. NEVER against
        # middleName: admitting it through the byline side reopens #173's measured
        # 26.4%-precision pattern by another door -- replayed over the curator-resolved
        # ledger it flipped 10 curator-ACCEPTED rows to the wrong person (byline first
        # token matching a rival's middle name, then a generic dept-substring
        # affiliation key displacing the correct pick), while this restricted form
        # flipped zero in both measurement methodologies.
        author_first = _byline_first_name(fore)
        affil_blob = " ".join(_norm(a) for a in (affiliations or []))

        cohort = []
        for rec in pool:
            # An adopted/preferred first name is sometimes recorded in middleName
            # rather than givenName in the HR-sourced identity table (legal name in
            # givenName, adopted Western first name in middleName); PubMed/Scopus
            # bylines use the preferred name, so middleName is an alternate
            # given-name source -- EXACT ONLY, on the whole field (issue #173).
            #
            # It used to feed the INITIAL tier too, and that is where nearly all of its
            # output came from. Measured against curator resolutions: top picks reached
            # through a givenName initial were accepted as proposed 91.7% of the time
            # (11,360/12,388), middleName-only picks 26.4% (29/110), and all 16
            # accepted-but-overridden rows in the whole ledger were middleName-only
            # picks. Sharing an initial with someone's middle name is not evidence, it
            # is a ~1-in-20 coincidence, and the surname pool is already the homonym
            # set. This tier is what proposed Leah *T*eresa Rosen (Alumni MD, cohort of
            # one) for a byline reading "Tony Rosen" on pmid 42424133 -- a paper aer2006
            # had already held at score 100.0 ACCEPTED for 44 days when the row opened.
            #
            # The WHOLE normalised field, not its tokens. Splitting on whitespace was
            # tried and reverted: it promotes junk into the `full` tier, and `full` is the
            # LEAD term of the sort below, so a spurious full match displaces a correct
            # pick rather than merely joining it. 392 of the 910 multi-token middleNames
            # are comma-joined concatenations of separately recorded names, and 222 yield
            # a token of <=2 characters -- so "Keith Richards,Keith Richards" offers a bare
            # "keith", and a middleName of "L." offers "l", each an EXACT first-name match.
            # Measured over all 11,668 curator-resolved rows, tokenising bought zero
            # correct top picks and zero extra reachability on either lane while flipping
            # 7 curator-ACCEPTED scopus rows to the wrong person (byline "Keith Jamison",
            # resolution kwj2001, displaced by jaj7021 on exactly that concatenation).
            #
            # ponytail: whole field only. This gives up the adopted-name case where the
            # byline carries only an initial ("J Zhong" for Hua Judy Zhong) AND the case
            # where the adopted name is a non-leading token of a multi-token middleName.
            # Both want a PUBLISHING-name source rather than a looser test on the legal
            # one -- that is `person.firstName` (issue #171 / PR #172), exact-only for the
            # same reason this is.
            middle_norm = _norm(rec["middle"])
            # The person mirror's first name is the one the byline actually uses. It
            # disagrees with identity.givenName on the FIRST INITIAL for 143 cwids
            # (Anthony->Tony, Xiaoxuan->Emily, Ho-Yee->Tommy), and the initial test below
            # drops every one of them outright however obviously they wrote the paper.
            #
            # EXACT matches only, and ahead of the rest so a byline that names the person
            # outright wins. The mirror does NOT widen the initial tier. That tier is
            # givenName-only as of issue #173, which removed middleName from it after it
            # scored 26% curator precision there against givenName's 92%; adding a second
            # loose source now would rebuild exactly what that change tore out.
            # ponytail: full tier only. Widen to initials only if a precision run says so.
            #
            # A byline that prints the person's WHOLE legal name -- given plus middle,
            # the ordinary Hispanic/Filipino compound shape -- matches neither field on
            # its own: _norm("Eileen Ruth Samson") is "eileenruthsamson" against
            # givenName "eileenruth" and middleName "samson". Live anchor:
            # authorship_review 3043, pmid 39629475, first-author byline "Eileen Ruth
            # Samson Torres". est4003 stalled at `initial` and stored confidence 0.35 --
            # 0.25 initial + 0.40/2 cohort - 0.10 inactive, "Low match" in the curator UI
            # -- while an unrelated Torres held top_cwid.
            #
            # The two WHOLE normalised fields concatenated, exactly as tight as its
            # neighbours: no tokenising, for the measured reasons above. Unlike every
            # other widening tried on this tier, it CANNOT widen the pool.
            # author_given == given_norm + middle_norm implies author_given STARTS WITH
            # given_norm, so the byline's first initial always already matched and the
            # candidate was already in the cohort at `initial`. The clause can only
            # promote a tier; it can never admit somebody the `continue` below excludes.
            # Replayed over all 19,050 recoverable pubmed rows it promoted 216 candidates
            # on 216 rows, moved 7 top picks and disturbed ZERO curator-resolved rows.
            # Guarded on a non-empty middleName: with an empty one the concatenation is
            # just given_norm, the test beside it.
            if author_given and (rec.get("pref_norm") == author_given
                                 or (author_first
                                     and author_first == rec.get("pref_norm"))):
                given_match = "full"
            elif author_init and (rec["given_norm"] or middle_norm):
                if author_given and (author_given == rec["given_norm"]
                                     or author_given == middle_norm
                                     or (middle_norm
                                         and author_given
                                         == rec["given_norm"] + middle_norm)
                                     or (author_first
                                         and author_first == rec["given_norm"])):
                    given_match = "full"
                elif rec["given_norm"][:1] == author_init:
                    given_match = "initial"
                else:
                    continue                       # no exact name, no givenName initial -> not this person
            else:
                given_match = "unknown"            # no usable given on either side
            cohort.append((rec, given_match))

        cohort_size = len(cohort)
        out = []
        for rec, given_match in cohort:
            affil_match, where = self._affil_match(rec, affil_blob)
            gap = (pub_year - rec["end_year"]) if (pub_year and rec["end_year"]) else None
            penalty = temporal_penalty(gap)
            out.append({
                "cwid": rec["cwid"],
                "name": " ".join(x for x in (rec["given"], rec["middle"], rec["surname"]) if x),
                "dept": rec["dept"], "division": rec["division"],
                "person_type": rec["person_type"], "title": rec["title"],
                "given_match": given_match,
                "affil_dept_match": affil_match, "affil_match_on": where,
                "cohort_size": cohort_size,
                "years_after_wcm": gap,
                "confidence": self._confidence(given_match, cohort_size, affil_match,
                                               rec["historical"], penalty),
            })
        out.sort(key=lambda d: (d["given_match"] == "full", d["affil_dept_match"],
                                d["confidence"]), reverse=True)
        return out[:top_k], cohort_size

    @staticmethod
    def _affil_match(rec, affil_blob):
        """Does the affiliation text name the candidate's dept or division?"""
        if not affil_blob:
            return False, None
        dept_n = _norm(rec["dept"])
        if dept_n and len(dept_n) >= 4 and dept_n in affil_blob:
            return True, "dept"
        # division: any distinctive word (>=5 chars) present in the affiliation
        for word in (rec["division"] or "").replace("&", " ").replace(",", " ").split():
            w = _norm(word)
            if len(w) >= 5 and w in affil_blob:
                return True, "division"
        return False, None

    @staticmethod
    def _confidence(given_match, cohort_size, affil_match, historical, penalty=0.0):
        """Explainable ordering aid, NOT a probability. `historical` is the flat
        person-type flag (alumni/inactive/emeritus); `penalty` is the already-capped
        temporal term from `temporal_penalty`, applied exactly once and ONLY here —
        this is the single route by which staleness reaches either sort key."""
        base = 0.50 if given_match == "full" else 0.25 if given_match == "initial" else 0.15
        rarity = 0.40 / max(cohort_size, 1)
        affil = 0.25 if affil_match else 0.0
        hist = -0.10 if historical else 0.0
        return round(max(0.0, min(1.0, base + rarity + affil + hist - penalty)), 3)


# ---- offline self-test ------------------------------------------------------
def _selftest():
    """Builds an IdentityIndex from hand-built records (no DB) to prove the
    middleName-as-alternate-given-name fix (Judy/Hua Zhong, PMID 40681448), its
    narrowing to exact-match-only (issue #173), the person-mirror publishing name
    (issue #171), and the temporal-plausibility penalty (issue #159)."""
    def rec(given, middle, surname, end_year=None, cwid=None, pref_first=None):
        return {
            "cwid": cwid or f"{given or middle}_{surname}".lower(),
            "given": given, "middle": middle, "surname": surname,
            "given_norm": _norm(given), "surname_norm": _norm(surname),
            "pref_norm": _norm(pref_first),
            "dept": "", "division": "", "program": "", "title": "",
            "person_type": "Full-Time Faculty", "historical": False,
            "end_year": end_year,
        }

    records = [
        rec("Hua", "Judy", "Zhong"),   # publishing name in middleName -> should match "full"
        rec("Jian", "", "Zhong"),      # plain given-name initial match -> unchanged behaviour
        rec("Xiu", "Wei", "Zhong"),    # neither given nor middle matches initial -> excluded
    ]
    idx = IdentityIndex(records)
    out, cohort_size = idx.candidates("Zhong", "Judy", "J")
    by_cwid = {c["cwid"]: c for c in out}

    checks = []
    hua = by_cwid.get("hua_zhong")
    checks.append(("Judy/Hua Zhong present with given_match=full",
                    hua is not None and hua["given_match"] == "full"))
    jian = by_cwid.get("jian_zhong")
    checks.append(("Jian Zhong present with given_match=initial",
                    jian is not None and jian["given_match"] == "initial"))
    checks.append(("Xiu/Wei Zhong absent (no initial match on either field)",
                    "xiu_zhong" not in by_cwid))
    checks.append(("cohort_size counts only the two initial-matching records",
                    cohort_size == 2))

    # --- middleName is exact-match-only (issue #173) --------------------------
    # The anchor case. Byline "Tony Rosen" (pmid 42424133) against Leah *T*eresa
    # Rosen, a cohort of one, whose only tie to the byline is a shared middle
    # initial. The initial tier used to hand her the row at confidence 0.55 while
    # the real author already held the paper at score 100.
    rosens = IdentityIndex([rec("Leah", "Teresa", "Rosen", cwid="ltr4001")])
    tony, tony_cohort = rosens.candidates("Rosen", "Tony", "T")
    # ...and the same person under a byline that DOES carry her name, to prove the
    # exclusion is about the initial tier and not about middleName as such.
    leah, _ = rosens.candidates("Rosen", "Teresa", "T")
    checks += [
        ("a shared middleName INITIAL no longer admits a candidate ('Tony Rosen' "
         "vs Leah Teresa Rosen)", tony == [] and tony_cohort == 0),
        ("...while the exact middleName still does, at given_match=full",
         len(leah) == 1 and leah[0]["given_match"] == "full"),
    ]

    # Multi-token middleName is deliberately NOT split. _norm over the whole field
    # collapses "Wing Guinevere" to "wingguinevere", so only a byline spelling every
    # middle name out reaches her -- 910 identity rows are shaped like this and this
    # change does not help them. Splitting was implemented, measured and reverted:
    # `full` leads the sort, 392 of those 910 fields are comma-joined concatenations
    # and 222 yield a <=2-char token, so tokenising promotes junk to the top of the
    # ranking. It flipped 7 curator-ACCEPTED scopus rows to the wrong person and won
    # nothing anywhere in the ledger. gul4001's real fix is person.firstName (PR #172),
    # where the mirror already holds "Guinevere".
    lees = IdentityIndex([rec("Qi", "Wing Guinevere", "Lee", cwid="gul4001")])
    spelled, _ = lees.candidates("Lee", "Wing Guinevere", "WG")
    second_token, _ = lees.candidates("Lee", "Guinevere", "G")
    junk = IdentityIndex([rec("Jonathan", "Keith Richards,Keith Richards", "J", cwid="jaj7021")])
    checks += [
        ("a byline spelling the whole middleName out matches at full",
         len(spelled) == 1 and spelled[0]["given_match"] == "full"),
        ("a multi-token middleName is NOT split -- its second token alone does not match",
         second_token == []),
        ("...which is what keeps a comma-joined concatenation from offering a bare "
         "first name ('Keith' out of 'Keith Richards,Keith Richards')",
         junk.candidates("J", "Keith", "K")[0] == []),
    ]

    # givenName keeps BOTH tiers, untouched: nothing above may narrow it.
    kims = IdentityIndex([rec("John", "Andrew", "Kim", cwid="jak")])
    checks += [
        ("givenName full tier intact",
         kims.candidates("Kim", "John", "J")[0][0]["given_match"] == "full"),
        ("givenName initial tier intact",
         kims.candidates("Kim", "Jonathan", "J")[0][0]["given_match"] == "initial"),
        ("a givenName initial match is not disturbed by a mismatched middleName",
         kims.candidates("Kim", "Jonathan", "J")[1] == 1),
    ]

    # --- byline middle initial reaches the full tier (issue #185) -------------
    # The anchor: pmid 40935722, byline "Gregory W Fischer" against Gregory Walter
    # Fischer. _norm("Gregory W") is "gregoryw", which equals nobody's first name, so
    # the row stalled at `initial` and tied 0.70 with a non-matching first name.
    fischers = IdentityIndex([
        rec("Gregory", "Walter", "Fischer", cwid="gwf2001"),
        rec("Brett", "Gilman", "Fischer", cwid="brf9036"),
    ])
    greg, greg_cohort = fischers.candidates("Fischer", "Gregory W", "GW")
    by_fischer = {c["cwid"]: c for c in greg}
    checks += [
        ("byline 'Gregory W' reaches given_norm 'Gregory' at given_match=full (#185)",
         by_fischer.get("gwf2001", {}).get("given_match") == "full"),
        ("...and leads the ranking", greg and greg[0]["cwid"] == "gwf2001"),
        ("a non-matching first name is still excluded ('Brett' vs author initial 'g')",
         "brf9036" not in by_fischer and greg_cohort == 1),
    ]

    # --- byline spelling givenName + middleName out reaches the full tier ----
    # Live anchor: authorship_review 3043, pmid 39629475, first-author byline "Eileen
    # Ruth Samson Torres". est4003 is givenName "Eileen Ruth" + middleName "Samson", so
    # _norm of the byline forename ("eileenruthsamson") equals neither field alone and
    # the row stalled at `initial`, confidence 0.35 -- "Low match" in the curator UI --
    # while eft4002 ("Emily Fujika Torres"), who shares only the initial, held top_cwid.
    torres = IdentityIndex([
        rec("Eileen Ruth", "Samson", "Torres", cwid="est4003"),
        rec("Emily", "Fujika", "Torres", cwid="eft4002"),
    ])
    torr, torr_cohort = torres.candidates("Torres", "Eileen Ruth Samson", "ERS")
    by_torres = {c["cwid"]: c for c in torr}
    checks += [
        ("byline 'Eileen Ruth Samson' reaches givenName+middleName at given_match=full "
         "(pmid 39629475)", by_torres.get("est4003", {}).get("given_match") == "full"),
        ("...and leads the ranking over the rival Torres",
         torr and torr[0]["cwid"] == "est4003"),
        ("the rival, who shares only the byline's INITIAL, stays at `initial` -- the "
         "concatenation promotes a tier, it does not hand one out",
         by_torres.get("eft4002", {}).get("given_match") == "initial"
         and torr_cohort == 2),
    ]

    # NON-WIDENING. Matching given_norm + middle_norm means the byline forename starts
    # with given_norm, so its first initial matched too and the candidate was already in
    # the cohort at `initial`. The clause promotes; it never admits. A byline naming the
    # same two words in the OTHER order shares neither the concatenation nor the initial
    # and must still be excluded outright, exactly as before.
    ngs = IdentityIndex([rec("Bao", "Ling", "Ng", cwid="bal9001")])
    checks.append(("given+middle concatenation does not widen the pool: 'Ling Bao' vs "
                   "Bao Ling Ng is still excluded (wrong order, wrong initial)",
                   ngs.candidates("Ng", "Ling Bao", "LB") == ([], 0)))

    # An EMPTY middleName leaves the clause inert -- given_norm + "" is given_norm, the
    # test beside it -- so nothing about a middle-less identity changes. The 'John Q P' /
    # 'John Quincy' Adams pair above is the live proof and must keep passing verbatim;
    # assert the degenerate shape outright too, so a future edit cannot quietly turn the
    # concatenation into a prefix test.
    empties = IdentityIndex([rec("John", "", "Adams", cwid="jqa2")])
    checks += [
        ("empty middleName: an exact given name is still full",
         empties.candidates("Adams", "John", "J")[0][0]["given_match"] == "full"),
        ("empty middleName: a LONGER byline forename is not promoted by the "
         "concatenation ('Johnquincy' vs 'John' + '')",
         empties.candidates("Adams", "Johnquincy", "J")[0][0]["given_match"]
         == "initial"),
    ]

    # The byline's first token must NEVER be tested against middleName -- that is the
    # naive variant, measured at 10 curator-ACCEPTED regressions (byline first token
    # matching a rival's MIDDLE name, then a generic affiliation key displacing the
    # correct pick). Identity Jonathan Keith Jamison vs byline "Keith R": first token
    # 'keith' equals his middle name exactly, and he must still be excluded outright
    # (author initial 'k' matches neither 'j' nor the whole-field blob).
    jamisons = IdentityIndex([rec("Jonathan", "Keith", "Jamison", cwid="jaj7021")])
    checks.append(("byline first token matching only a MIDDLE name admits nobody "
                   "('Keith R' vs Jonathan Keith Jamison)",
                   jamisons.candidates("Jamison", "Keith R", "KR") == ([], 0)))

    # Trailing token must be a bare initial, or the byline is a compound given name
    # ('Soo Young', 'Ana Paula') and its first token is NOT the person's first name --
    # 12 measured open rows hold a correct full-tier pick this shape would rival.
    soos = IdentityIndex([rec("Soo", "", "Kim", cwid="soo1")])
    soo_cands, _ = soos.candidates("Kim", "Soo Young", "SY")
    checks.append(("compound given name 'Soo Young' does NOT full-match given name "
                   "'Soo' (trailing token is not an initial) -- stays at initial tier",
                   len(soo_cands) == 1 and soo_cands[0]["given_match"] == "initial"))

    # Hyphens do not split (whitespace only): 'Chung-Han' offers no bare 'chung'.
    chungs = IdentityIndex([rec("Chung", "", "Lee", cwid="chu1")])
    ch_cands, _ = chungs.candidates("Lee", "Chung-Han", "CH")
    checks.append(("hyphenated 'Chung-Han' does NOT full-match given name 'Chung' -- "
                   "whitespace split only",
                   len(ch_cands) == 1 and ch_cands[0]["given_match"] == "initial"))

    # First token vs the person mirror's publishing name, same restricted shape:
    # 'Tony E' names Anthony (Tony) Rosen as surely as 'Tony' does.
    tonys = IdentityIndex([rec("Anthony", "Ehren", "Rosen", cwid="aer2006",
                               pref_first="Tony")])
    tony_mi, _ = tonys.candidates("Rosen", "Tony E", "TE")
    checks.append(("byline 'Tony E' reaches the preferred name 'Tony' at full",
                   len(tony_mi) == 1 and tony_mi[0]["given_match"] == "full"))

    # Several trailing initials still qualify; a trailing full word still does not.
    quincys = IdentityIndex([rec("John", "", "Adams", cwid="jqa")])
    checks += [
        ("multiple trailing initials qualify: 'John Q P' full-matches 'John'",
         quincys.candidates("Adams", "John Q P", "JQP")[0][0]["given_match"] == "full"),
        ("'Mary Jane' shape stays at initial ('John Quincy' vs 'John')",
         quincys.candidates("Adams", "John Quincy", "JQ")[0][0]["given_match"]
         == "initial"),
    ]

    # --- preferred publishing name from the person mirror (issue #171) -------
    # The live anchor: authorship_review 70797, pmid 42424133, byline "Tony Rosen".
    # identity has aer2006 as Anthony Ehren Rosen, so the initial test drops him ('t'
    # matches neither 'a' nor 'e') and the producer proposed Leah *T*eresa Rosen
    # instead -- on an article aer2006 already holds ACCEPTED at 100.
    #
    # Since #173, ltr4001 is not a candidate for this byline either: her only tie was
    # the shared middle initial. So the two changes cover different halves of the same
    # row -- #173 stops the wrong name being offered, #171 supplies the right one --
    # and neither is sufficient alone. Ordering the mirror FIRST is what makes aer2006
    # a `full` match rather than nothing at all.
    rosens = IdentityIndex([
        rec("Anthony", "Ehren", "Rosen", cwid="aer2006", pref_first="Tony"),
        rec("Leah", "Teresa", "Rosen", cwid="ltr4001"),        # middleName-initial match only
        rec("Neal", "", "Rosen", cwid="ner2007"),              # unrelated, must stay out
    ])
    ranked, rosen_cohort = rosens.candidates("Rosen", "Tony", "T")
    by_rosen = {c["cwid"]: c for c in ranked}
    checks += [
        ("byline 'Tony Rosen' reaches aer2006 via the person mirror, as a FULL match",
         by_rosen.get("aer2006", {}).get("given_match") == "full"),
        ("aer2006 is the top pick for the byline that names him",
         [c["cwid"] for c in ranked][0] == "aer2006"),
        ("the middleName-initial match he was losing to is gone (issue #173), so this "
         "byline now yields exactly one candidate",
         "ltr4001" not in by_rosen and rosen_cohort == 1),
        ("an unrelated Rosen is still excluded", "ner2007" not in by_rosen),
    ]

    # The mirror must not widen the INITIAL tier: a preferred name that merely shares
    # the byline's first letter earns nothing, or it repeats the middleName mistake.
    tims = IdentityIndex([rec("Robert", "", "Ng", cwid="rob1", pref_first="Tim")])
    initial_only, _ = tims.candidates("Ng", "Tony", "T")
    checks.append(("a mirror name sharing only the INITIAL does not create a candidate",
                   initial_only == []))

    # The pool is still keyed on identity.surname ALONE. Also indexing under
    # person.lastName would reach 7 more open rows (zhp4001 "Pan" publishing as "Poon"),
    # but it enlarges cohorts enough to reshuffle 44 initial-tier top picks that no new
    # evidence favours -- 44 rewritten curator-facing rows to gain 7. Measured
    # 2026-08-30 by replaying all 12,171 open pubmed rows through both indexes.
    # ponytail: one surname source. Revisit if the byline-surname miss is ever the
    # dominant cause of a wrong proposal; it is not today.
    poons = IdentityIndex([rec("Zhi-Lin", "", "Pan", cwid="zhp4001", pref_first="Chi-Lam")])
    checks.append(("a mirror-only SURNAME is deliberately not indexed",
                   poons.candidates("Poon", "Chi-Lam", "C") == ([], 0)))

    # --- temporal plausibility (issue #159) ---------------------------------
    # Three Smiths who all match "J Smith" equally on name; only the WCM end year
    # differs. Paper published 2024.
    smiths = IdentityIndex([
        rec("John", "", "Smith", end_year=2027, cwid="here"),      # still here
        rec("James", "", "Smith", end_year=2021, cwid="recent"),   # left 3y ago
        rec("Jane", "", "Smith", end_year=2001, cwid="departed"),  # left 23y ago
    ])
    ranked, _ = smiths.candidates("Smith", None, "J", pub_year=2024)
    order = [c["cwid"] for c in ranked]
    by = {c["cwid"]: c for c in ranked}
    checks += [
        ("2024 paper: 23-years-departed Smith ranks last (name evidence ties, the "
         "penalty breaks it)", order[-1] == "departed"),
        ("recent leaver (3y < grace) is NOT penalised",
         by["recent"]["confidence"] == by["here"]["confidence"]),
        ("penalty is visible in the curator-facing confidence",
         by["departed"]["confidence"] < by["here"]["confidence"]),
        ("nobody is dropped: all three still returned", len(ranked) == 3),
        ("the signed gap rides along for authorship_review.top_years_after_wcm "
         "(negative = the paper predates the departure, distinct from NULL)",
         by["departed"]["years_after_wcm"] == 23 and by["here"]["years_after_wcm"] == -3),
    ]

    # REGRESSION (issue #159 rework, blocker 1 — SUPPRESSION). Cohort of 7 against
    # top_k=5, where the stale candidate is the one the evidence actually favours: full
    # given name plus an affiliation-department match, against six initial-only
    # homonyms. A penalty at the FRONT of the sort key sinks him below all six and the
    # top_k cut then deletes him from the list outright — he never reaches cwid_pool,
    # never reaches candidate_cwids_json, and the curator cannot select him. He must
    # come back FIRST.
    crowd = IdentityIndex(
        [dict(rec("Robert", "", "Weiss", end_year=2003, cwid="true_author"),
              dept="Pathology")]
        + [rec(g, "", "Weiss", end_year=2030, cwid=f"homonym{i}")
           for i, g in enumerate(("Rita", "Ramona", "Rachel", "Rebecca", "Rhonda", "Rose"))])
    crowded, crowd_size = crowd.candidates(
        "Weiss", "Robert", "R", ["Department of Pathology, Weill Cornell Medicine"],
        pub_year=2024, top_k=5)
    checks += [
        ("cohort (7) is larger than top_k (5), so the cut is real",
         crowd_size == 7 and len(crowded) == 5),
        ("21-years-stale candidate with full given name + affiliation match survives "
         "the top_k cut and still ranks first (rank down != suppress)",
         crowded[0]["cwid"] == "true_author"),
    ]

    # REGRESSION (blocker 2 — EPSILON OUTRANKS EVIDENCE). In a lexicographic key the
    # lead term's magnitude is irrelevant, so a leading penalty lets the smallest
    # non-zero gap (6 years, one past the grace) flip the top pick away from a full
    # given-name + affiliation match to an initial-only homonym.
    eps = IdentityIndex([
        dict(rec("Robert", "", "Weiss", end_year=2018, cwid="true_author"),
             dept="Pathology"),
        rec("Rita", "", "Weiss", end_year=2030, cwid="homonym"),
    ])
    eps_ranked, _ = eps.candidates(
        "Weiss", "Robert", "R", ["Department of Pathology, Weill Cornell Medicine"],
        pub_year=2024)
    checks += [
        ("6-year gap does not flip the top pick away from a full given-name + "
         "affiliation match", eps_ranked[0]["cwid"] == "true_author"),
        ("...and that epsilon penalty is genuinely non-zero", temporal_penalty(6) == 0.01),
    ]

    # A paper published BEFORE they left must not be penalised at all (the largest
    # measured band is exactly this case).
    early, _ = smiths.candidates("Smith", None, "J", pub_year=1998)
    # No pub_year supplied -> pre-#159 behaviour, bit for bit.
    noyear, _ = smiths.candidates("Smith", None, "J")
    checks += [
        ("paper predating the departure: nobody penalised, ranking and confidences "
         "identical to the no-pub_year run",
         [c["cwid"] for c in early] == [c["cwid"] for c in noyear]
         and [c["confidence"] for c in early] == [c["confidence"] for c in noyear]),
        ("no pub_year -> pre-#159 behaviour: no gap recorded anywhere",
         all(c["years_after_wcm"] is None for c in noyear)),
    ]

    # CAP. Widest gap in the live open queue is 70 years; uncapped 0.05/yr pinned 1,065
    # of 2,771 stale top candidates at confidence 0.000, which is why the cap exists.
    # The cap is NOT a guarantee of non-zero confidence — see the deep-cohort case
    # below, which is asserted rather than left to prose so nobody re-derives the
    # "there is a floor" mistake from an earlier draft of this file.
    widest, _ = IdentityIndex(
        [rec("Jane", "", "Smith", end_year=1954, cwid="ancient")]).candidates(
            "Smith", None, "J", pub_year=2024)
    checks += [
        ("penalty saturates: a 20y gap and the widest real 70y gap cost the same 0.15",
         temporal_penalty(20) == temporal_penalty(70) == TEMPORAL_PENALTY_CAP == 0.15),
        ("a deep-cohort historical candidate CAN still clamp to 0.000: the cap is not "
         "a floor (agk9007-shaped — 'Kim', cohort 266, no given name, 11y gap)",
         IdentityIndex._confidence("unknown", 266, False, True,
                                   temporal_penalty(11)) == 0.0),
        ("widest real gap (70y) does not pin confidence at 0.000",
         widest[0]["years_after_wcm"] == 70 and widest[0]["confidence"] > 0.0),
        ("temporal_penalty: 0 through the grace, then monotone up to the cap",
         [temporal_penalty(g) for g in (None, -20, 0, 5, 6, 10, 20, 70)]
         == [0.0, 0.0, 0.0, 0.0, 0.01, 0.05, 0.15, 0.15]),
    ]

    ok = True
    for desc, passed in checks:
        print(f"[OK] {desc}" if passed else f"[** FAIL] {desc}")
        ok = ok and passed
    print("SELFTEST PASS" if ok else "SELFTEST FAIL")
    return ok


if __name__ == "__main__":
    import sys
    sys.exit(0 if _selftest() else 1)
