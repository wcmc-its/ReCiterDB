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

And home to the CAMPUS SCOPE (Cornell Ithaca onboarding, risks R1/R6). One index holds
exactly one campus's roster; `candidates()` is untouched by the scoping, because the
records of every other campus are never indexed in the first place. See `CAMPUS_WCM`.

And to the VENUE TERM (v3.14): on the external lanes a candidate whose department
publishes in the venue's subfield gains a little `confidence`, and one whose department
never does loses a little. Cornell Ithaca only, off unless AAR_VENUE_FIT=on. See
`venue_fit_enabled`.

And to the BYLINE ORCID (v3.15): the external lanes store the ORCID the source attaches
to a byline author, and on a Cornell Ithaca byline (any external byline once
AAR_BYLINE_ORCID_WCM is on as well) the person that ORCID is on file for
leads the candidate list, whether or not the name reached them. Off unless
AAR_BYLINE_ORCID=on. See `byline_orcid_enabled`.

Env: DB_USERNAME/DB_PASSWORD/DB_HOST/DB_NAME (reciterdb, read-only); AAR_VENUE_FIT;
AAR_BYLINE_ORCID; AAR_BYLINE_ORCID_WCM.
"""
import difflib, logging, os, re, unicodedata
from collections import Counter

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


# ---- campus scope ----------------------------------------------------------
# ONE INDEX HOLDS ONE CAMPUS. `IdentityIndex(records, campus=...)` keeps only the
# records of that campus, so `candidates()` -- the ranking function every prior
# incident on this file was about -- is not touched by the scoping at all, and a
# cross-campus candidate is not "ranked down", it is structurally absent.
#
# WHY A HARD SCOPE AND NOT A CONFIDENCE PREFERENCE. The damage the Cornell Ithaca
# load does to WCM (plan risk R1) is measured as: 5,751 WCM people's surname+initial
# cohorts grow, and 1,161 go from cohort 1 to cohort >1. Re-measured 2026-09-07
# against the live rosters (identity 35,855; identity_cornell research population
# 15,030) it is 5,834 and 1,169 -- the plan's numbers, plus the WCM roster's own
# growth since. That loss is in the RARITY
# TERM, `0.40 / cohort_size` in `_confidence` -- a WCM person who is the unique holder
# of their surname+initial scores 0.40 there and drops to 0.20 the moment one Cornell
# homonym joins the cohort. A preference expressed as a confidence nudge cannot repair
# that, because `cohort_size` is counted BEFORE any confidence is computed (see
# `candidates`): only removing the record from the cohort restores the signal. And a
# Cornell candidate offered against a WCM authorship is never merely lower-ranked --
# it can never be correct, because Cornell people are deliberately never scored and
# hold no publications in this system, so the proposal costs a curator a decision and
# buys nothing.
#
# WHY THE SCOPE IS DECLARED BY THE CALLER, NOT INFERRED FROM AFFILIATION TEXT. An
# authorship's campus is not written on the row, but it does not need to be: each AAR
# lane sweeps ONE institution's publication universe and therefore already knows which
# roster it is matching against. The PubMed lane's universe is a `[Affiliation]` query
# over `aar_universe`'s home-institution keywords; the Scopus lane's is an AF-ID family
# read from `--afid-list`, and the Ithaca lane is a SEPARATE run over
# `scopus_afids_cornell_ithaca.csv`, whose own comment already forbids merging the two
# AF-ID sets for the same reason this forbids merging the two rosters. Campus is a
# property of the RUN. Inferring it per row from affiliation text would be strictly
# worse: the commonest WCM affiliation string is a bare "Weill Cornell Medicine, New
# York, NY", which carries no campus discriminator the sweep did not already apply.
#
# WHAT CHANGED 2026-09-27, and what did not. The curator requirement is "If WCM
# affiliation, show WCM people. If CU affiliation, show CU options. If both, show both",
# so WHICH ROSTER(S) a byline is matched against is now decided per row -- but above the
# index, by `byline_campuses` + `CampusRoster`, never inside one. The argument above
# still decides the default: a byline with no Ithaca-side marker (the bare "Weill Cornell
# Medicine" case, "Cornell University, New York", any non-Cornell affiliation) stays on
# WCM exactly as before. Only a clause naming Ithaca, Geneva/AgriTech, Cornell Tech, the
# vet college and the like reaches for the Cornell roster. And each campus is still its
# own index, so the R1 rarity term is untouched: a Cornell homonym never enters a WCM
# cohort, it only stands BESIDE it on a byline that names both campuses.
CAMPUS_WCM = "wcm"
CAMPUS_ITHACA = "cornell-ithaca"          # == sync_cornell_ithaca_identities.CAMPUS_TYPE

# Cornell's OWN person-type vocabulary, in the same "first match wins" precedence
# shape as PERSON_TYPES above. Emitted by scripts/sync_cornell_ithaca_identities.py
# into DynamoDB `Identity.personTypes`, from which ReCiterDB's nightly import lands one
# row per type in `person_person_type` (dataTransformer.process_person_person_type).
#
# Without this table every Cornell candidate falls through PERSON_TYPES -- which is a
# list of WCM/NYP boolean COLUMNS that a Cornell person matches none of -- and renders
# as "Other / CTSC" with historical=False (plan risk R6).
#
# Current roles lead, terminal states follow, so somebody who is both reads as what
# they currently are. The label is display only; `historical` is the -0.10 term in
# `_confidence` and is computed separately, below.
CORNELL_PERSON_TYPES = [
    ("cornell-faculty",                  "Cornell Faculty", False),
    ("cornell-faculty-rte",              "Cornell Faculty (Research/Teaching/Extension)", False),
    ("cornell-faculty-new",              "Cornell Faculty (New)", False),
    ("cornell-academic",                 "Cornell Academic", False),
    ("cornell-research-teaching-title",  "Cornell Research/Teaching Title", False),
    ("cornell-researcher",               "Cornell Researcher", False),
    ("cornell-researcher-project",       "Cornell Project Researcher", False),
    ("cornell-postdoc",                  "Cornell Postdoc", False),
    ("cornell-incubator-associate",      "Cornell Incubator Associate", False),
    ("cornell-research-center-employee", "Cornell Research Center Employee", False),
    ("cornell-student-grad",             "Cornell Student (Graduate)", False),
    ("cornell-student-undergrad",        "Cornell Student (Undergraduate)", False),
    ("cornell-student",                  "Cornell Student", False),
    ("cornell-staff",                    "Cornell Staff", False),
    ("cornell-affiliate",                "Cornell Affiliate", False),
    # terminal states, last: reached only when no current role above fired
    ("cornell-emeritus",                 "Cornell Emeritus", True),
    ("cornell-retired-faculty",          "Cornell Retired Faculty", True),
    ("cornell-former-postdoc",           "Cornell Former Postdoc", True),
    ("cornell-alumni",                   "Cornell Alumni", True),
    ("cornell-retiree",                  "Cornell Retiree", True),
    ("cornell-inactive",                 "Cornell Inactive", True),
]
# `historical` is ANY terminal marker, independent of which label won precedence --
# deliberately NOT coupled to the label the way the WCM branch couples them. R6's
# complaint is that "Cornell emeriti and alumni miss the -0.10 penalty their WCM
# equivalents get", and coupling would lose exactly the people it names: Cornell's
# `is_faculty` is a current-role flag that an emeritus professor commonly still
# carries, so a precedence-coupled flag would label them "Cornell Faculty" and hand
# back historical=False -- the bug R6 reports, reintroduced one level down. The WCM
# branch can couple them safely because ITS terminal states are exclusive columns
# (`emeritusFaculty` is set only when `fullTimeFaculty` is not); Cornell's are not.
CORNELL_HISTORICAL_TYPES = frozenset(t for t, _, h in CORNELL_PERSON_TYPES if h)


def cornell_person_type(person_types):
    """Cornell `personTypes` -> (label, historical), the R6 counterpart of the
    PERSON_TYPES loop in `IdentityIndex._record`.

    Falls back to a campus label rather than "Other / CTSC": every Cornell record
    carries `cornell-ithaca` by construction (it is the first entry
    sync_cornell_ithaca_identities.person_types() emits, and the sync never emits an
    empty list), so "we know nothing beyond the campus" is the honest floor here."""
    types = frozenset(person_types)
    label = "Cornell Ithaca"
    for t, lbl, _ in CORNELL_PERSON_TYPES:
        if t in types:
            label = lbl
            break
    return label, bool(types & CORNELL_HISTORICAL_TYPES)


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


# ---- affiliation text ------------------------------------------------------
# The institution's own name is NOT a department. `_norm("Weill Cornell Medicine")` is
# "weillcornellmedicine", which CONTAINS "medicine", so under the old substring test
# every WCM affiliation self-matched a department called "Medicine" -- and naming Weill
# Cornell is this queue's entry condition, so it fired almost everywhere: 4,545
# identities carry department "Medicine" and 96 carry "Weill Cornell Medicine" itself.
# Division was looser still, matching on any single >=5-char word: Emergency Medicine
# (287 people), General Internal Medicine (1,233), Geriatrics and Palliative Medicine,
# the Englander Institute for Precision Medicine, and Cornell Medical Practice (7, via
# the word "Cornell") all self-matched a BARE WCM affiliation naming no division at all.
#
# WCM is not the only institution whose own NAME contains a department word. This queue's
# entry condition is aar_universe's home-institution keyword list, which deliberately
# includes the affiliated institutions -- "memorial|sloan|kettering" and
# "hospital|special|surgery" among them -- so their names reach this test just as WCM's
# does, and self-match exactly the same way. Measured on the live open queue, 2026-09-04:
# 106 open rows carry top_affil_match=1 with a byline naming "Hospital for Special
# Surgery" against a top_dept containing "Surgery", and 8 more match "Sloan Kettering
# Cancer Center" against a Cancer/Oncology department. ("Methodist", checked the same
# way, produced 0 -- an institution with no department-word collision belongs nowhere
# near this list, since stripping it can only lose real matches.)
#
# These phrases are removed from the affiliation before any dept/division test. The
# asymmetry is the point: "Weill Cornell Medicine, New York" must stop naming the
# Department of Medicine, while "Department of Medicine, Weill Cornell Medicine" must
# still name it -- and likewise "Hospital for Special Surgery, New York" must stop
# naming a Department of Surgery while "Department of Surgery, Hospital for Special
# Surgery" still does. Edit this list freely -- it is re-sorted longest-first below, so a
# short phrase can never eat part of a longer one ("weill cornell" must not strip
# "Weill Cornell Medicine" down to a bare, self-matching "medicine"). Add an institution
# only when its own name contains a department or division word.
INSTITUTION_PHRASES = (
    "Weill Cornell Medicine",
    "Weill Cornell Medical College",
    "Weill Cornell Medical Center",
    "Weill Medical College of Cornell University",
    "Weill Cornell Graduate School of Medical Sciences",
    "NewYork-Presbyterian/Weill Cornell Medical Center",
    "Cornell University",
    "Weill Cornell",
    # Affiliated institutions naming a department in their own name. The two Sloan
    # Kettering spellings normalise to the same tokens (`_affil_words` splits on every
    # non-word character, so the hyphen and the space are the same seam); both are
    # listed because both are how the affiliation is actually written.
    "Hospital for Special Surgery",
    "Memorial Sloan Kettering Cancer Center",
    "Memorial Sloan-Kettering Cancer Center",
)


def _affil_words(s):
    """One affiliation string -> normalised word tokens.

    Splits on EVERY non-word character, deliberately NOT `name_tokens()`: that helper's
    split class is name-shaped (whitespace, comma, period, hyphen) and would fuse the
    slashes, semicolons and parentheses affiliation text is full of into single tokens
    ('Presbyterian/Weill' -> one word, which then matches no phrase and no department)."""
    return tuple(t for t in (_norm(w) for w in re.split(r"[\W_]+", str(s))) if t)


_INSTITUTION_TOKENS = sorted((_affil_words(p) for p in INSTITUTION_PHRASES),
                             key=len, reverse=True)


# Boundary token between two affiliation strings. `_norm` keeps [a-z0-9] only, so no
# department, division or institution-phrase token can ever equal it -- which is the
# whole point: an n-gram window containing it matches nothing, so no dept phrase, no
# division word and no institution phrase can span the seam between two unrelated
# affiliations. Between #203 and this, `_affil_tokens` flattened every affiliation into
# one boundary-less tuple, so a person in "Internal Medicine" matched the byline
# ["Weill Cornell Internal", "Medicine Institute of Something Else, Chicago, IL"] --
# a phantom +0.25 assembled from two different institutions. 1,948 open PubMed and 1,841
# open Scopus rows carry more than one affiliation string. Pre-#203 the strings were
# joined with a space and the test was a substring one, so this could not arise.
_AFFIL_SEP = "|"
assert not _AFFIL_SEP.isalnum(), "_AFFIL_SEP must be unreachable by _norm's [a-z0-9]"


# How the department n-gram in `_affil_match` compares tokens, on BOTH sides (the dept
# name and the affiliation), after `_affil_tokens` has stripped the institution names:
#
#   * the connector "and" is dropped. `_norm("&")` is "", so "&" never survives either
#     tokenizer, and "A & B" was already the tokens (a, b) while "A and B" was
#     (a, and, b): the same department, spelled the two ways the roster and the bylines
#     both use, never matched across. Live anchor: authorship_review 84447 / 84457 / 401,
#     byline "Department of Population Medicine and Diagnostic Sciences, College of
#     Veterinary Medicine, Cornell University" against as3527's roster department
#     "Population Medicine & Diagnostic Science". 72 roster departments carry "&", 96
#     carry "and".
#   * a trailing "s" is dropped from every token of `_PLURAL_MIN`+ characters, so
#     "Sciences" and "Science" are one token -- but ONLY when the department is two or
#     more words. A one-word department is a bare token-equality test with nothing
#     around it to keep it honest, and the plural there is the -ic/-ics pair that names
#     a DIFFERENT thing: folding "Pediatrics" onto "Pediatric" matched "Hospital for
#     Special Surgery Pediatric Rehabilitation" (26 stored candidates, measured). In a
#     multi-word department the other words must still stand adjacent and in order,
#     which is where the specificity is. The length floor keeps short words whole
#     ("news" is not "new").
#
# `_AFFIL_SEP` is one character and not "and", so it passes through untouched and the
# seam guarantee holds exactly as before. The division test does not use this: it is a
# single-word equality test and stays the one #203 shipped.
_DEPT_CONNECTORS = frozenset({"and"})
_PLURAL_MIN = 5


def _dept_key(toks, fold_plural):
    """Tokens as the department n-gram compares them (see the block above)."""
    return tuple(t[:-1] if fold_plural and len(t) >= _PLURAL_MIN and t.endswith("s") else t
                 for t in toks if t not in _DEPT_CONNECTORS)


def _affil_tokens(affiliations):
    """Affiliation strings -> one normalised token sequence, `_AFFIL_SEP` between
    strings, with the institution's own name removed. Tokens, not a concatenated blob,
    so `_affil_match` can test on word boundaries: the old blob also matched a division
    word INSIDE another word -- "Vascular" inside "cardiovascular" (live rows
    30303/78915, below)."""
    parts = [_affil_words(a) for a in (affiliations or []) if a]
    toks = ()
    for i, part in enumerate(parts):
        toks += ((_AFFIL_SEP,) if i else ()) + part
    for phrase in _INSTITUTION_TOKENS:
        n, i, out = len(phrase), 0, []
        while i < len(toks):
            if toks[i:i + n] == phrase:
                i += n
            else:
                out.append(toks[i])
                i += 1
        toks = tuple(out)
    return toks


# ---- byline campus ---------------------------------------------------------
# Which roster(s) a byline's candidates come from (see the campus-scope block at the top
# of this file). Evaluated PER CLAUSE -- each affiliation string split on ';' -- because
# PubMed merges co-authors' affiliations into one string often enough that an AND across
# the whole blob would read a Cornell co-author's "Ithaca" as this author's campus.
#
# A clause is WCM when it names the medical college or its partners. `weill` alone is
# not enough: the Weill Institute for Cell and Molecular Biology and Weill Hall are both
# in Ithaca, so "Weill Institute ..., Cornell University, Ithaca" is an Ithaca clause.
_WCM_CLAUSE = re.compile(
    r"weill(?!\s+(?:institute|hall))|presbyterian|qatar|doha|cornell (?:university )?medic"
    r"|medical college|york avenue|\b100(?:65|21)\b", re.I)
# A clause is Cornell-Ithaca only when it is NOT WCM, says `cornell`, and names an
# Ithaca-side place or unit: Ithaca (and its measured misspellings / zip codes), Geneva
# (AgriTech), Cornell Tech on Roosevelt Island, and the units that only exist upstate.
_ITHACA_CLAUSE = re.compile(
    r"ithaca|itaca|ithica|\b1485[0-3]\b"
    r"|agritech|\b14456\b|geneva,? (?:ny|new york)"
    r"|cornell tech|technion|roosevelt island|\b10044\b"
    r"|ornithology|cooperative extension|\bchess\b|chexs|veterinary|baker institute"
    r"|biological field station", re.I)
# `cornell` that is not the university: street names (Cincinnati's Cornell Road is a
# live byline shape) and Cornell College in Mount Vernon, Iowa.
_NOT_CORNELL = re.compile(
    r"cornell (?:road|rd|st|street|ave|drive|lane|blvd)\b|cornell college", re.I)
_EMAIL = re.compile(r"\S+@\S+")


def byline_campuses(affiliation_texts):
    """Affiliation strings -> the set of campuses whose roster this byline is matched
    against: {CAMPUS_WCM}, {CAMPUS_ITHACA}, or both.

    Emails are stripped first ("...@cornell.edu" is not an affiliation). Then each ';'
    clause is WCM (`_WCM_CLAUSE`), Cornell-Ithaca (`_ITHACA_CLAUSE`, only if not WCM and
    it says `cornell` outside `_NOT_CORNELL`), an AMBIGUOUS Cornell clause (says cornell,
    no marker either way -- "Cornell University, New York, NY" -- which counts as WCM,
    today's reading), or not Cornell at all, which says nothing. A byline where nothing
    spoke is WCM: every lane's universe is WCM-first, so that is exactly the pre-campus
    behaviour for every row that does not name Ithaca."""
    out = set()
    for text_ in affiliation_texts or []:
        for clause in _EMAIL.sub(" ", str(text_ or "")).split(";"):
            if _WCM_CLAUSE.search(clause):
                out.add(CAMPUS_WCM)
                continue
            if "mount vernon" in clause.lower():
                continue
            if "cornell" not in _NOT_CORNELL.sub(" ", clause).lower():
                continue
            out.add(CAMPUS_ITHACA if _ITHACA_CLAUSE.search(clause) else CAMPUS_WCM)
    return out or {CAMPUS_WCM}


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


def _display_middle(middle):
    """Collapse a comma-duplicated middleName, for DISPLAY ONLY.

    `identity.middleName` arrives from the WCM Enterprise Directory legal-name field, and
    392 of the 910 multi-token values there are comma-joined concatenations of separately
    recorded names -- "Keith Richards,Keith Richards", "Kwon,Kwon". That is upstream data,
    not something this pipeline assembles: _display_name printed the field faithfully and
    the queue rendered "Dylan Kwon,Kwon Kim" for dkk4001, whose identity is otherwise
    perfectly well formed. Nothing is wrong with the RECORD; what is wrong is showing a
    curator the same name twice while they are trying to tell two people apart.

    DISPLAY ONLY, and the distinction is load-bearing. The matching tier deliberately
    consumes the WHOLE unsplit field (`middle_norm`, see the comment in candidates()):
    splitting it there was implemented, measured and REVERTED, because a bare token like
    "keith" reaches the `full` given-name tier, `full` leads the sort, and it flipped 7
    curator-accepted rows onto the wrong person. This function must therefore never be
    used to build `middle_norm` or anything else the ranking reads -- only the label.

    Comma-separated segments, compared case/space-insensitively, first occurrence kept in
    order, rejoined with a space. A field with no comma is returned untouched, so the
    common case is byte-for-byte unchanged."""
    if not middle or "," not in middle:
        return middle
    seen, out = set(), []
    for part in (p.strip() for p in middle.split(",")):
        key = _norm(part)
        if part and key not in seen:
            seen.add(key)
            out.append(part)
    # An all-empty field ("," or ", ,") leaves nothing; return the original rather than
    # silently turning a present-but-junk value into a blank name component.
    return " ".join(out) if out else middle


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


# ---- venue fit ---------------------------------------------------------------
# The Scopus and OpenAlex lanes have no identity-only score: name and affiliation text
# are all a candidate is ranked on, and an initials-only byline leaves the name saying
# very little. What the paper was published IN is evidence the row already carries and
# the matcher never read. `venue_fit.DeptVenueFit` scores a (department, venue subfield)
# pair from the campus's own accepted authorships, and `venue_fit.venue_term` turns the
# score into a bounded term: +0.15 where the department publishes there well above the
# campus rate, -0.20 where it essentially never does, nothing in between or when the
# venue or the department is unknown.
#
# IT REACHES RANKING ONLY THROUGH `confidence`, exactly like the temporal penalty above
# and for the same reason. `_rank_key` is unchanged, so the term reorders candidates the
# given-name tier and the affiliation match leave tied and nothing else. That is the
# measured optimum and not caution: replayed over 2,772 adjudicated multi-candidate
# Cornell rows (2026-11-16) the term in this position fixed 88 top picks and broke 17,
# while letting a good fit outrank the given-name tier fixed 62 and broke 130.
#
# RANK, DO NOT DECIDE. The term never accepts or rejects a row, removes nobody from the
# cohort, and is smaller than the 0.25 a tier or an affiliation match is worth. The
# people it penalises are the interdisciplinary and the newly hired, whose papers really
# are in an unusual field for their department. In a cohort of five or fewer they stay
# on the card. In a larger one the card shows the top five, as it always has, and a
# penalised candidate can now be the one who falls to sixth: `cohort_size` still counts
# them, the stored list does not name them.
#
# THE CANDIDATE'S OWN PAPERS DO NOT VOUCH FOR THEM. The fit is asked for with the
# candidate's cwid, and the table leaves that person's own accepted rows out (see
# `venue_fit.DeptVenueFit`): the number is what the rest of the department publishes.
#
# CORNELL ITHACA ONLY, AND OFF BY DEFAULT. Every measurement behind the term is on Ithaca
# Scopus rows. WCM rows have not been replayed, and until they are a WCM index never
# holds a fit table, whatever it is handed; `load()` builds one only for the Ithaca roster
# and only when AAR_VENUE_FIT is "on".
#
# ...AND ONLY WHERE EVERY CANDIDATE IS A CORNELL ONE. A term that only one side of a
# comparison can earn is not evidence about that comparison. On a byline that offers
# both rosters, or a Cornell byline that also keeps a WCM dual appointee, the Cornell
# candidates could gain 0.15 or lose 0.20 while the WCM ones could do neither, and a WCM
# person tied with a Cornell namesake would lose the row to the venue alone. So
# `CampusRoster.candidates` passes the subfield on only when the WCM roster offers
# nobody and the answer is the Ithaca roster's by itself; a list that mixes campuses is
# ranked exactly as it was before the term existed. A row with a WCM candidate on it
# therefore cannot change when the feature is switched on.
#
# NOTHING CHANGES UNTIL BOTH HALVES ARE PRESENT. A candidate gains the `venue_fit` key,
# and its confidence the term, only when the index holds a fit table AND the caller passed
# a venue subfield AND the table can score that department. In every other case -- the
# feature off, a WCM index, a list that mixes campuses, the PubMed lane (which passes no
# subfield), a venue with no category, a department under 20 training rows -- the
# candidate dict is byte-for-byte what it was, and so is the candidate_cwids_json the
# lanes store from it.
def venue_fit_enabled():
    """AAR_VENUE_FIT=on. Anything else, a typo included, is off."""
    return (os.environ.get("AAR_VENUE_FIT") or "").strip().lower() == "on"


# ---- byline ORCID ------------------------------------------------------------
# Scopus and OpenAlex both attach an ORCID to some byline authors, and until v3.15 the
# lanes threw it away. With AAR_BYLINE_ORCID=on they store it per row as
# authorship_review.byline_orcid (and the Scopus author id as byline_scopus_authid), and
# hand it to `candidates()`. Off, the lanes make no lookup, write NULL in both columns
# and pass nothing, and `load()` reads no ORCID map.
#
# WHAT THE ORCID DOES. `cornell_pub_orcid_mapping` says which netid an ORCID is on file
# for. When the byline's ORCID is on file for somebody on the roster, that person is a
# candidate -- added to the list when the name did not reach them -- and every candidate
# on the row carries `orcid_match`, true for that person and false for the rest.
#
# IT LEADS `_rank_key`, AHEAD OF THE GIVEN-NAME TIER. This is the one signal measured
# strong enough to: on 7,428 adjudicated Cornell Scopus rows where the ORCID mapped to a
# netid and the answer was known, it named the curator's person on 7,373 (99.3%,
# 2026-11-17), and on 295 of the rows a curator had reassigned, that person was not among
# the name-matched candidates at all. A name can only rank the people it reaches. The
# temporal and venue comments above argue that a graded term must never lead the key;
# this is a boolean, like `full`, and there is no magnitude to mis-weigh.
#
# RANK, DO NOT DECIDE. An ORCID match never accepts, rejects or dismisses a row and
# removes nobody from the list. OpenAlex assigns some ORCIDs through its own author
# disambiguation, and one wrong mapping row misattributes every paper under that ORCID,
# so a curator still reads the card; the name-matched candidates stay on it, in the
# order they had.
#
# AN ORCID ON FILE FOR TWO NETIDS IS IGNORED (`orcid_map`): it cannot say which.
#
# CORNELL ITHACA ONLY, UNTIL THE SECOND SWITCH (AAR_BYLINE_ORCID_WCM, below). With only
# AAR_BYLINE_ORCID on, the match is the v3.15 one: the map is of Cornell netids, a WCM
# index never holds one, and `CampusRoster.candidates` passes the ORCID on only when the
# WCM roster offers nobody. A row with a WCM candidate on it cannot change.
#
# NOTHING CHANGES UNTIL ALL THREE ARE PRESENT: the index holds a map, the caller passed
# an ORCID, and the ORCID is on file for somebody on this roster. In every other case --
# the feature off, the PubMed lane, a byline with no ORCID, an ORCID nobody here holds,
# and (second switch off) a WCM index or a mixed list -- no candidate carries
# `orcid_match` and the list is byte-for-byte what it was.
#
# ---- byline ORCID on WCM and mixed lists (owner decisions 2026-11-18) ----------
# AAR_BYLINE_ORCID_WCM=on, which needs AAR_BYLINE_ORCID=on too, widens the match. Off, a
# typo included, everything above stands and this section is dead code: `load()` reads
# nothing more, no candidate carries `orcid_source`, and every list is the one the
# single switch produces. The second switch exists because the first is already on in
# production, so merging this could otherwise change picks the night it deploys.
#
# WHERE THE iDs COME FROM. Scholars first, the older lists as a labelled fallback
# (`orcid_sources`, a pure function, holds the rules):
#   'scholars'             scholar_active.orcid, the iD Scholars holds as CONFIRMED, for
#                          either campus (scholar_active.campus). Columns from v3.16; a
#                          table without them is simply no Scholars source.
#   'affiliation_manager'  cornell_pub_orcid_mapping, the v3.15 list. Ithaca fallback.
#   'inferred'             pubsource_orcid_person (CWID, inferred), the nightly suggestion
#                          built from accepted articles. WCM fallback, and since
#                          2026-11-23 an Ithaca one too, after Cornell's list, for the
#                          Ithaca netids that list carries (see (e)).
# (a) an ORCID Scholars confirms belongs to the Scholars person, whatever a fallback
#     says; (b) a person with a Scholars-confirmed iD takes no fallback iD at all: Scholars
#     is the authority for that person; (c) an ORCID Scholars confirms on either campus
#     leaves the other campus's fallback too, so a cross-campus person holds it on one
#     roster; (d) an ORCID on file for two people within one list is ignored, as before,
#     and one Scholars confirms for two people is ignored everywhere and logged;
# (e) an inferred iD goes to the roster its person is on (a `cornell-*` person type puts
#     them on Ithaca), where it ranks below Cornell's own list.
#
# THE CANDIDATE SAYS WHICH. The candidate with `orcid_match` true also carries
# `orcid_source`, one of the three labels. Publication Manager reads it off the first
# stored candidate to keep its bulk accept to Scholars-confirmed matches. Nobody else on
# the row carries the key, and no row without a match does.
#
# WCM AND MIXED LISTS. A WCM index holds its map, so a WCM-only byline is matched. A
# `CampusRoster` hands the ORCID to every roster the byline already consults, and the
# person it is on file for leads the merged list (`_rank_key` leads with `orcid_match`
# on both sides). It is still RANK, DO NOT DECIDE: no status moves and nobody leaves the
# list. An ORCID that finds one person on each roster cannot say which, and is ignored
# for that row.
#
# THE EXTERNAL LANES, AND THE PUBMED LANE BEHIND ITS OWN SWITCH. Scopus and OpenAlex take
# `candidates()`' first element as top_cwid with no later re-sort, so the ORCID's person
# is the pick. The PubMed lane (`aar_matcher.match_authorship`) re-sorts by the
# identity-only score; its key now leads with `orcid_match` too, so the ORCID's person is
# its pick as well. The producer captures a PubMed byline ORCID (PubMed's own
# Author/Identifier, else OpenAlex's by DOI) only with AAR_BYLINE_ORCID_PUBMED=on
# (`byline_orcid_pubmed_enabled`); off, it passes none and no PubMed candidate carries the
# key. The replay tools pass a PubMed row the byline_orcid IT STORES whatever the switch
# says, as they do for Scopus rows. Owner decision 2026-11-23: "A Singh" on two Vet-school
# papers (pmids 42716290, 42716284) named no ORCID in PubMed, while OpenAlex has the
# author's (0000-0002-8762-7097, on file for as3527).
def byline_orcid_enabled():
    """AAR_BYLINE_ORCID=on. Anything else, a typo included, is off."""
    return (os.environ.get("AAR_BYLINE_ORCID") or "").strip().lower() == "on"


_ORCID = re.compile(r"(?:HTTPS?://ORCID\.ORG/)?(\d{4}-\d{4}-\d{4}-\d{3}[\dX])")


def bare_orcid(value):
    """An ORCID as byline_orcid CHAR(19) stores it: 0000-0000-0000-000X, no
    https://orcid.org/ prefix, check letter upper-cased. None for anything else, so a
    malformed value is never stored and never matched."""
    m = _ORCID.fullmatch(str(value or "").strip().upper())
    return m.group(1) if m else None


def orcid_map(pairs):
    """(netid, orcid) rows of cornell_pub_orcid_mapping -> {bare ORCID: netid}, netids
    lower-cased. An ORCID on file for more than one netid is left out: it identifies
    nobody. One person holding two ORCIDs keeps both."""
    by_orcid = {}
    for netid, orcid in pairs:
        orcid, netid = bare_orcid(orcid), str(netid or "").strip().lower()
        if orcid and netid:
            by_orcid.setdefault(orcid, set()).add(netid)
    return {o: next(iter(n)) for o, n in by_orcid.items() if len(n) == 1}


# An iD from the INFERRED list may add a person the byline's surname does not reach only
# when one of their surname words is close to one of the byline's (owner decision
# 2026-11-23). The inferred list is learned from accepted papers and misfiles a co-author's
# iD: the PubMed reconcile dry run that day proposed Daniel Green for "Ariana I
# Matarangas" (5 rows) and Alexander Drilon for "Antoine Desilets" (their own iDs, each
# inferred onto someone else from one accepted paper), and Ronan Chaligne for "Travis J
# Hollmann" (whose byline carries Chaligne's iD). Close keeps the name changes the add
# exists for: Dorr/Doerr 0.89, Sarkar/Sarker 0.83, Ahmad/Ahmed 0.80; those three scored
# under 0.5. Scholars-confirmed and Cornell-list iDs keep their unrestricted reach (a
# married name, the Lindqvist case).
SURNAME_CLOSE = 0.75


def surnames_close(byline_last, rec):
    """Is a surname word of the byline within SURNAME_CLOSE (difflib ratio, `name_tokens`
    folding) of a surname word of roster record `rec`, its directory surname or the last
    word of its publishing name?"""
    mine = name_tokens(byline_last)
    theirs = set(name_tokens(rec.get("surname"))) | set(name_tokens(rec.get("pref_full"))[-1:])
    return any(difflib.SequenceMatcher(None, a, b).ratio() >= SURNAME_CLOSE
               for a in mine for b in theirs)


def byline_orcid_wcm_enabled():
    """AAR_BYLINE_ORCID_WCM=on AND AAR_BYLINE_ORCID=on. Anything else, a typo in either
    included, is off, and the ORCID match is the Ithaca-only one of v3.15."""
    return (byline_orcid_enabled()
            and (os.environ.get("AAR_BYLINE_ORCID_WCM") or "").strip().lower() == "on")


def byline_orcid_pubmed_enabled():
    """AAR_BYLINE_ORCID_PUBMED=on AND AAR_BYLINE_ORCID=on: the PubMed producer captures
    the byline ORCID and ranks by it. A third switch for the second's reason: the first
    two are on in production, so merging must not change PubMed picks the night it
    deploys."""
    return (byline_orcid_enabled()
            and (os.environ.get("AAR_BYLINE_ORCID_PUBMED") or "").strip().lower() == "on")


ORCID_SCHOLARS, ORCID_AFFILIATION_MANAGER, ORCID_INFERRED = (
    "scholars", "affiliation_manager", "inferred")
_SCHOLARS_CAMPUS = {"wcm": CAMPUS_WCM, "ithaca": CAMPUS_ITHACA}    # scholar_active.campus


def orcid_sources(scholars, ithaca_fallback=(), wcm_fallback=(), ithaca_people=frozenset()):
    """The labelled maps behind AAR_BYLINE_ORCID_WCM, one per campus, and the ORCIDs
    Scholars confirms for more than one person:

        ({CAMPUS_WCM: {bare ORCID: (cwid, source)}, CAMPUS_ITHACA: {...}}, [ORCID, ...])

    `scholars` is (cwid, orcid, campus) rows of scholar_active; the fallbacks are
    (person id, orcid) rows of cornell_pub_orcid_mapping and of pubsource_orcid_person
    (CWID, inferred). Ids are lower-cased (the inferred list mixes case). The rules are
    (a)-(d) of the comment block above:

      * Scholars wins. An ORCID it confirms is mapped to its person, on that person's
        campus, and is taken out of BOTH fallbacks, the other campus's included.
      * A person Scholars holds a confirmed iD for takes nothing from their campus's
        fallback, a second and different iD included.
      * Each fallback is read as `orcid_map` reads it: an ORCID on file there for two
        people is dropped before anything else is decided.
      * An ORCID Scholars confirms for two people names nobody. It is returned for the
        log, mapped on neither campus, and not handed back to a fallback: the authority
        having contradicted itself is no reason to trust a weaker list.

    A Scholars row with a campus other than 'wcm' or 'ithaca' is skipped.

    `ithaca_people` (lower-cased ids with a `cornell-*` person type, the test that puts a
    record on the Ithaca roster, `_campus_person_types`) routes the inferred list: an
    entry for one of them goes to the Ithaca map, labelled 'inferred', after Cornell's own
    list (which wins an ORCID both name) and under the same Scholars rules. Owner decision
    2026-11-23: the nightly inference writes Ithaca netids too (138 on that date, 96 not in
    Cornell's list; as3527, Ajay Singh, from 5 accepted papers), and a WCM-only map can
    never match them. The list is still read once by `orcid_map`, so an ORCID it names for
    two people is dropped whichever rosters they are on. Empty, everything stays on WCM."""
    confirmed, held = {}, set()
    for cwid, orcid, campus in scholars:
        orcid, cwid = bare_orcid(orcid), str(cwid or "").strip().lower()
        campus = _SCHOLARS_CAMPUS.get(str(campus or "").strip().lower())
        if orcid and cwid and campus:
            confirmed.setdefault(orcid, set()).add((cwid, campus))
            held.add((cwid, campus))
    out = {CAMPUS_WCM: {}, CAMPUS_ITHACA: {}}
    inferred = orcid_map(wcm_fallback)
    for campus, mapped, source in (
            (CAMPUS_ITHACA, orcid_map(ithaca_fallback), ORCID_AFFILIATION_MANAGER),
            (CAMPUS_ITHACA, {o: c for o, c in inferred.items() if c in ithaca_people},
             ORCID_INFERRED),
            (CAMPUS_WCM, {o: c for o, c in inferred.items() if c not in ithaca_people},
             ORCID_INFERRED)):
        for orcid, cwid in mapped.items():
            if orcid not in confirmed and (cwid, campus) not in held:
                out[campus].setdefault(orcid, (cwid, source))
    for orcid, people in confirmed.items():
        if len(people) == 1:
            (cwid, campus), = people
            out[campus][orcid] = (cwid, ORCID_SCHOLARS)
    return out, sorted(o for o, people in confirmed.items() if len(people) > 1)


# ---- candidate label -------------------------------------------------------
def _display_name(rec, via="directory"):
    """The label a curator reads for a candidate: ONE name, the FULL name of the source
    that matched, exactly as that source records it. `via` is the source
    `candidates()` matched through -- "primary", "directory", "alternate" or
    "accepted" -- and the same value rides on the candidate dict as `name_source`, so
    the PM card can badge it. Provenance is a badge; the label is one name.

    This is display ONLY. Nothing the matcher reads is derived from it, and it is
    computed after the cohort is fixed, so it cannot touch given_match, confidence,
    cohort_size or the sort.

    The sources, and what each prints:

      primary    `person.firstName/middleName/lastName` -- the DynamoDB primaryName
                 mirror, the name the person publishes under, and since issue #171 a
                 route to the `full` tier on its own: a byline can match pref_norm
                 ALONE, with identity.givenName agreeing on nothing, not even the
                 first initial. Live anchor: pmid 42670968, byline "Mila Sun",
                 mis4060, `full` off pref_norm -- and the queue printed "Shuo Sun",
                 the directory name, which appears on none of her publications.
                 Prints `pref_full`; falls back to the directory name when the
                 person row is missing.
      directory  reciterdb `identity` (ED ou=people + ASMS): givenName, middleName,
                 surname. Every match that went through given_norm, the whole-field
                 middleName rule, the given+middle concatenation, the initial tier or
                 "unknown" prints this, comma-duplicated middleName collapsed (see
                 _display_middle). The WHOLE name, middle included, even when the
                 byline's middle disagrees: "David K Jones" against David Randolph
                 Jones prints "David Randolph Jones" -- the mismatch is the
                 information, and a trimmed label would hide it.
      alternate  a DynamoDB Identity.alternateNames entry (issue #227): the copy's
                 own given/middle/surname, as recorded.
      accepted   a name carried by >= ACCEPTED_BYLINE_MIN of the person's ACCEPTED
                 articles (`accepted_byline_names`): likewise its own name as
                 recorded, "Erika M Hissong" if that is what PubMed's forename
                 field said.

    WHY one name and not a parenthetical. The old label was "<matched name> (HR:
    <directory name>)". "HR" was the wrong word (the directory name is ED + ASMS, not
    an HR feed); it printed the same name twice wherever the two sources agreed
    ("Sandra Huicochea Castellanos (HR: Sandra Huicochea Castellanos)" was live); and
    a parenthetical is not provenance -- a curator cannot tell an alternate name from
    a publishing name from it, and neither can the PM card. `name_source` carries
    that as data instead.

    WHY the matched source is the label and not always the directory. Labelling a
    pref_norm match with the directory name shows the curator a name the byline does
    not contain and hides the one it does; the row reads as a mismatch to reject when
    the match is correct (mis4060 above). The same holds however the first names
    differ, initial included: jog4030 is "Joseph Gardella" in the directory and
    publishes as "Jae Gardella" -- same initial, and a directory label is exactly as
    unreadable as mis4060's.

    Longest directory name on prod is well inside top_name VARCHAR(255); every write
    site truncates to 255 anyway (aar_orchestrator, aar_universe_scopus,
    aar_reconcile_open, aar_report_changed_picks)."""
    if via == "primary" and rec.get("pref_full"):
        return rec["pref_full"]
    # An alternate-name copy (issue #227) carries the alternate's own name in
    # given/middle/surname, so this join IS its recorded name.
    return " ".join(x for x in (rec.get("given"), _display_middle(rec.get("middle")),
                                rec.get("surname")) if x)


# ---- alternate names (issue #227) -------------------------------------------
def load_alternate_names():
    """cwid -> [{firstName, middleName, lastName, source: "alternate"}, ...] from
    DynamoDB `Identity.alternateNames` -- the prior / maiden / adopted names ReCiter
    itself retrieves under, which reciterdb `identity` (directory legal name) and
    `person` (publishing name) both lack. Byline "Erika Hissong" (pmid 42644724)
    proposed nobody although emh9016 (Erika Patel) lists exactly that name. `source`
    is the provenance `candidates()` publishes as `name_source`.

    Paginated scan, same boto3 shape as retrieveS3.scan_table. On ANY failure -- no
    boto3, no credentials, no table, throttling -- warn and return {}: the roster must
    never fail because of this, it just falls back to primary names only."""
    try:
        import boto3
        table = boto3.resource("dynamodb").Table("Identity")
        kw = dict(ProjectionExpression="uid, #i.alternateNames",
                  ExpressionAttributeNames={"#i": "identity"})
        out, resp = {}, table.scan(**kw)
        while True:
            for item in resp.get("Items", []):
                # `or {}` / `or []`: a DynamoDB NULL deserialises to None with the key
                # PRESENT, so a .get() default never fires (see dataTransformer).
                alts = (item.get("identity") or {}).get("alternateNames") or []
                alts = [dict(a, source="alternate") for a in alts if isinstance(a, dict)]
                if item.get("uid") and alts:
                    out[item["uid"]] = alts
            if "LastEvaluatedKey" not in resp:
                return out
            resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"], **kw)
    except Exception as e:                                       # noqa: BLE001
        logging.getLogger(__name__).warning(
            "Identity.alternateNames unavailable (%r); indexing primary names only", e)
        return {}


# A byline name becomes an alternate only when it sits on at least this many ACCEPTED
# articles of the cwid. One is not enough: the #171 sample found wrongly-accepted homonym
# articles among single hits (2 of 4), and a name minted from one of those would pull the
# homonym's other papers toward the wrong person. Two independent accepts is the cheapest
# guard that a single curator slip cannot clear.
ACCEPTED_BYLINE_MIN = 2

_ACCEPTED_BYLINES_SQL = (
    "SELECT personIdentifier AS cwid, articleAuthorNameFirstName AS fore, "
    "articleAuthorNameLastName AS last, COUNT(*) AS n FROM person_article "
    "WHERE userAssertion = 'ACCEPTED' AND articleAuthorNameLastName <> '' "
    "GROUP BY 1, 2, 3 HAVING n >= :min")


def accepted_byline_names(rows):
    """cwid -> [{firstName, middleName, lastName, source: "accepted", n}] from the
    names a person's ACCEPTED articles actually carry (rows of `_ACCEPTED_BYLINES_SQL`).
    The second alternate-name source beside DynamoDB alternateNames: a curator who
    accepted 40 Hissong papers for emh9016 has already said "Erika Hissong" is her,
    whether or not anyone typed it into the identity. PubMed's forename field is
    "Erika M" -- first token is the given name, the rest the middle -- and
    `_alt_records` drops any pair that is the legal name. `n` is the accepted-paper
    count, published per candidate as `name_n`."""
    out = {}
    for r in rows:
        fore = (r["fore"] or "").split()
        if not fore:
            continue
        out.setdefault(r["cwid"], []).append(
            {"firstName": fore[0], "middleName": " ".join(fore[1:]), "lastName": r["last"],
             "source": "accepted", "n": r["n"]})
    return out


# ---- identity index --------------------------------------------------------
_TIER = {"full": 2, "initial": 1, "unknown": 0}   # given_match strength, for the cwid dedupe


def _rank_key(d):
    """`candidates()`' ordering, shared with `CampusRoster` so a two-campus merge ranks
    both lists by exactly the key each was already ranked by.

    `orcid_match` leads (see the byline-ORCID block above). A candidate without the key
    reads as False, so on every list no ORCID touched the first element is the same for
    everybody and the order is the one the other three give, as before."""
    return (bool(d.get("orcid_match")), d["given_match"] == "full", d["affil_dept_match"],
            d["confidence"])


class IdentityIndex:
    """In-memory index of reciterdb `identity`, keyed by normalised surname, holding
    exactly ONE campus's roster (`campus`, default `CAMPUS_WCM` -- see the campus-scope
    block above for why the scope is an exclusion and why the caller declares it).

    Records of any other campus are dropped here, at construction, and never reach
    `by_surname`. That is the whole mechanism: `candidates()` needs no campus argument,
    `cohort_size` counts only in-scope homonyms, and there is no code path by which an
    out-of-scope record can be ranked, tie-broken or surfaced.

    A record with no `campus` key is WCM. Every hand-built record in this file's and
    the sibling modules' self-tests is therefore WCM, exactly as before, and so is
    every row `_record` builds today (see `_campus_person_types`)."""

    def __init__(self, records, campus=CAMPUS_WCM, alternate_names=None, dept_venue_fit=None,
                 orcid_cwid=None):
        self.campus = campus
        # The department x subfield table behind the venue term (see the venue-fit block
        # above), or None: no term, no `venue_fit` key, today's output. Kept only on an
        # Ithaca index -- the scope decision is enforced here, not left to each caller.
        self.dept_venue_fit = dept_venue_fit if campus == CAMPUS_ITHACA else None
        # bare ORCID -> the roster record of the person it is on file for, or empty: no
        # ORCID match, no `orcid_match` key, today's output. Only for people actually on
        # this roster. `orcid_cwid` comes in two shapes. {ORCID: netid} is the v3.15 map
        # (`orcid_map`): kept on an Ithaca index only, and its matches carry no source.
        # {ORCID: (cwid, source)} is a labelled map (`orcid_sources`), which only the
        # AAR_BYLINE_ORCID_WCM load builds: kept on either campus, with the label in
        # `orcid_source` for the candidate to carry. So a WCM index holds a map exactly
        # when it was handed a labelled one.
        self.orcid_rec, self.orcid_source = {}, {}
        orcids_of = {}
        for orcid, cwid in (orcid_cwid or {}).items():
            source = None
            if isinstance(cwid, tuple):
                cwid, source = cwid
            elif campus != CAMPUS_ITHACA:
                continue
            orcids_of.setdefault(str(cwid).lower(), []).append((orcid, source))
        self.by_surname = {}
        self.n_other_campus = 0          # observability: how many the scope excluded
        self.n_alt_names = 0             # ...and how many alternate-name copies were indexed
        # In-scope cwids with a Cornell appointment on record (see `_cornell_linked_cwids`):
        # what `CampusRoster` keeps from THIS index on a Cornell-only byline.
        self.cornell_linked = set()
        alternate_names = alternate_names or {}
        for r in records:
            if (r.get("campus") or CAMPUS_WCM) != campus:
                self.n_other_campus += 1
                continue
            if r.get("cornell_linked"):
                self.cornell_linked.add(r["cwid"])
            for orcid, source in orcids_of.get(str(r["cwid"]).lower(), ()):
                self.orcid_rec[orcid] = r
                if source:
                    self.orcid_source[orcid] = source
            self.by_surname.setdefault(r["surname_norm"], []).append(r)
            for alt in self._alt_records(r, alternate_names.get(r["cwid"])):
                self.n_alt_names += 1
                self.by_surname.setdefault(alt["surname_norm"], []).append(alt)

    @staticmethod
    def _alt_records(rec, alts):
        """Copies of `rec` under each DynamoDB alternateName (issue #227), one per
        distinct (first, last) that differs from the legal name's. The copy IS the
        person -- same cwid, dept, person type, end year -- indexed under the other
        surname, so a byline "Erika Hissong" reaches emh9016 (Erika Patel) through the
        ordinary surname pool and the ordinary given-name tiers, with the alternate's
        first name standing where givenName and the person-mirror name stand for a
        primary record. `candidates()` collapses a cwid that lands in one pool twice
        (primary "Ho-Yee Wong" + alternate "Tommy Wong" both key on "wong"), so no
        cohort ever counts one person as two homonyms.

        `alt_source` / `alt_n` carry the entry's provenance ("alternate" from DynamoDB,
        "accepted" plus the paper count from `accepted_byline_names`; an entry with no
        `source` key is "alternate") through to `candidates()`, which publishes them as
        `name_source` / `name_n`. First occurrence of a (given, surname) wins, so
        `load()` lists the accepted names first and a name in both sources keeps its
        evidence count."""
        seen = {(rec["given_norm"], rec["surname_norm"])}
        for a in alts or []:
            given = (a.get("firstName") or "").strip()
            surname = (a.get("lastName") or "").strip()
            key = (_norm(given), _norm(surname))
            # An initial is not a name. An accepted byline "J L Stewart" would otherwise
            # mint given "J", and byline "J L Stewart" then EXACT-matches it as `full` --
            # a single letter masquerading as a full-name match, displacing the old pick
            # (2026-09-12 dry run: S Li, Q Zhou, Y Lin, A Gupta, H Yang, S Mann...). A
            # one-letter MIDDLE is the same hazard through the whole-field middleName
            # rule ("Y Lin" vs "R Y Lin"); `candidates()` ignores it there, while the
            # copy keeps it as recorded so the label can print "Erika M Hissong".
            if len(key[0]) < 2 or not key[1] or key in seen:
                continue
            seen.add(key)
            yield dict(rec, given=given, middle=(a.get("middleName") or "").strip(),
                       surname=surname, given_norm=key[0], surname_norm=key[1],
                       pref_norm=key[0], alt_name=True,
                       alt_source=a.get("source", "alternate"), alt_n=a.get("n"))

    @staticmethod
    def _campus_person_types(conn):
        """cwid -> frozenset of its `cornell-*` person types, for Cornell people only.

        `person_person_type` is the ONLY place a non-WCM campus is recorded anywhere in
        reciterdb: `identity` is built by buildIdentity.py from WCM Enterprise Directory
        LDAP + ASMS and has no institution column at all, while this table is rebuilt
        nightly from DynamoDB `Identity.personTypes`, one row per type. Its
        `personIdentifier` and `identity.cwid` are both utf8mb4_unicode_ci, so this
        needs no COLLATE (the 1267 trap is on authorship_review.top_cwid, not here).

        ARMED BUT DORMANT, and whoever wires the Ithaca roster in needs to know why.
        `load()` drives off `identity`, LEFT JOINs `person`, and `identity`'s only
        writer is buildIdentity.py reading WCM LDAP + ASMS. So bulk-loading Cornell
        people into DynamoDB `Identity` does NOT put them on this roster: it lands
        their personTypes here in `person_person_type` and nothing else, and a right-
        side LEFT JOIN row cannot manufacture a driving row. Measured 2026-09-07:
        `personType LIKE 'cornell-%'` returns 0 rows, so today every record `_record`
        builds is WCM and this scope excludes nobody. Whatever eventually admits
        Cornell people to `identity` must land WITH that route, not after it -- the
        R1 degradation starts the morning the roster grows, and this is what stops it.

        Two deliberate narrownesses, each guarding a measured false positive:

        * The SQL prefix is `cornell-%`, not `%cornell%`. `affiliate-cornell` is a
          live WCM person type held by 483 people -- WCM staff with an Ithaca
          affiliation -- and a substring test would move every one of them off the
          WCM roster and out of their own lane's candidate pool.
        * A cwid is Cornell only if it carries the explicit `cornell-ithaca` campus
          marker, not merely some `cornell-`-prefixed type. The sync emits that marker
          first on every record it writes, so requiring it costs nothing and means a
          future `cornell-`-prefixed type landing on a WCM person cannot silently
          deport them.

        No try/except, deliberately. If this query fails the campus labels are unknown,
        and the safe reading of "unknown" is NOT "everybody is WCM" -- that is exactly
        the silent R1 regression this scope exists to prevent. Failing here aborts the
        load, and every AAR caller already wraps its lane so a failure skips the run
        loudly instead of degrading the ranking quietly."""
        by_cwid = {}
        for cwid, ptype in conn.execute(text(
                "SELECT personIdentifier, personType FROM person_person_type "
                "WHERE personType LIKE 'cornell-%'")):
            by_cwid.setdefault(cwid, set()).add(ptype)
        return {k: frozenset(v) for k, v in by_cwid.items() if CAMPUS_ITHACA in v}

    @staticmethod
    def _cornell_linked_cwids(conn):
        """WCM-roster cwids that ALSO hold a Cornell (Ithaca) appointment: any `cornell-*`
        person type without the campus marker, or the WCM type `affiliate-cornell`.

        These are the people a Cornell-only byline must still reach on the WCM roster.
        A dual appointee is deliberately NOT on the Ithaca roster: the Cornell sync's
        bridge (sync_cornell_ithaca_identities.merge_bridged) keeps their netid off it
        and unions their cornell-* types onto the WCM identity minus `cornell-ithaca`,
        so `_campus_person_types` leaves them WCM. Measured 2026-09-09: 37 such people on
        the WCM roster with 150 open authorship_review rows (Koretzky, Spector, Sabuncu).
        Their Ithaca-side papers ("... Weill Hall, Ithaca, NY") read as Cornell-only, and
        matching those against the Ithaca roster alone would drop them from their own
        card. The Cornell-campus cwids are returned too; harmless, they are never on the
        WCM roster. Same no-try/except rule as `_campus_person_types`."""
        return {cwid for (cwid,) in conn.execute(text(
            "SELECT DISTINCT personIdentifier FROM person_person_type "
            "WHERE personType LIKE 'cornell-%' OR personType = 'affiliate-cornell'"))}

    @classmethod
    def load(cls, campus=CAMPUS_WCM, alternate_names=None):
        """`alternate_names` = {cwid: [{firstName, middleName, lastName, source?, n?}]};
        None -> read DynamoDB via `load_alternate_names()` (never raises). The accepted-
        byline names (`accepted_byline_names`) are always added, AHEAD of the DynamoDB
        ones: `_alt_records` keeps the first of a duplicated (given, surname), and the
        accepted entry is the one carrying the evidence count."""
        if alternate_names is None:
            alternate_names = load_alternate_names()
        alternate_names = {k: list(v or []) for k, v in alternate_names.items()}
        eng = create_engine(
            f"mysql+pymysql://{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}"
            f"@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}",
            connect_args={"connect_timeout": 15}, pool_pre_ping=True)
        cols = ("i." + ", i.".join(
            ["cwid", "givenName", "middleName", "surname", "primaryAcademicDepartment",
             "primaryAcademicDivision", "primaryTitle", "primaryProgram",
             "endDateWCMFaculty", "endDateWCMStudent"] + _PTYPE_COLS))
        # `identity` is the directory (ED ou=people + ASMS): it carries the LEGAL name.
        # `person` mirrors DynamoDB Identity.primaryName, which is the name the person
        # actually publishes under. Both cwid columns are utf8mb4_unicode_ci, so this
        # join needs no COLLATE (the 1267 trap is on authorship_review.top_cwid, which
        # is general_ci — not here).
        with eng.connect() as c:
            rows = c.execute(text(
                f"SELECT {cols}, p.firstName AS prefFirstName, "
                "p.middleName AS prefMiddleName, p.lastName AS prefLastName "
                "FROM identity i "
                "LEFT JOIN person p ON p.personIdentifier = i.cwid "
                "WHERE i.surname IS NOT NULL AND i.surname <> ''")).mappings().all()
            campus_types = cls._campus_person_types(c)
            linked = cls._cornell_linked_cwids(c)
            for cwid, names in accepted_byline_names(c.execute(
                    text(_ACCEPTED_BYLINES_SQL), {"min": ACCEPTED_BYLINE_MIN}).mappings()).items():
                alternate_names[cwid] = names + alternate_names.get(cwid, [])
            # Last on this connection, so a failed venue query cannot disturb a read the
            # roster depends on.
            dept_venue_fit = cls._load_dept_venue_fit(c, campus)
            orcid_cwid = cls._load_orcid_map(
                c, campus, frozenset(str(k).strip().lower() for k in campus_types))
        return cls([cls._record(r, campus_types.get(r["cwid"], ()), r["cwid"] in linked)
                    for r in rows],
                   campus=campus, alternate_names=alternate_names,
                   dept_venue_fit=dept_venue_fit, orcid_cwid=orcid_cwid)

    @staticmethod
    def _load_orcid_map(conn, campus, ithaca_people=frozenset()):
        """{bare ORCID: netid} from `cornell_pub_orcid_mapping` (`orcid_map`), or {} when
        the ORCID match does not apply: AAR_BYLINE_ORCID is not "on", this is not the
        Ithaca roster, or the table is not there (the dev instance has none).

        `_load_dept_venue_fit`'s failure rule, for its reason: a roster with no map is
        the roster of the day before the feature existed, so ANY failure is logged and
        returns {}, and the lane runs without the match. The table is looked for in
        information_schema first, so its absence is not a failed query at all.

        With AAR_BYLINE_ORCID_WCM on as well, the labelled map of `_load_orcid_sources`
        instead, for either campus. Off, this reads exactly what it read in v3.15."""
        if byline_orcid_wcm_enabled():
            return IdentityIndex._load_orcid_sources(conn, campus, ithaca_people)
        if campus != CAMPUS_ITHACA or not byline_orcid_enabled():
            return {}
        try:
            if not conn.execute(text(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = "
                    "DATABASE() AND table_name = 'cornell_pub_orcid_mapping'")).scalar():
                logging.getLogger(__name__).warning(
                    "no cornell_pub_orcid_mapping table; matching without the byline ORCID")
                return {}
            mapped = orcid_map(conn.execute(text(
                "SELECT netid, orcid FROM cornell_pub_orcid_mapping")).all())
            logging.getLogger(__name__).info(
                "byline ORCID: %d ORCIDs on file for one netid each", len(mapped))
            return mapped
        except Exception as e:                                       # noqa: BLE001
            logging.getLogger(__name__).warning(
                "ORCID map unavailable (%r); matching without the byline ORCID", e)
            return {}

    @staticmethod
    def _load_orcid_sources(conn, campus, ithaca_people=frozenset()):
        """{bare ORCID: (cwid, source)} for this campus from Scholars and the two
        fallback lists (`orcid_sources`), or {}. Called only with AAR_BYLINE_ORCID_WCM on.

        All three lists are read whichever campus is asked for, because each campus's
        map depends on what Scholars confirms on the other. A list that is not there is
        an empty list and is logged: scholar_active without its v3.16 orcid and campus
        columns (or absent), cornell_pub_orcid_mapping or pubsource_orcid_person absent
        (the dev instance). A list that is there and cannot be read is a failure, and
        `_load_orcid_map`'s rule applies to the whole map: logged, {}, and the lane runs
        without the match. Half a precedence rule is not applied."""
        log = logging.getLogger(__name__)

        def has(table, columns=()):
            if columns:
                return conn.execute(text(
                    "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = "
                    f"DATABASE() AND table_name = '{table}' AND column_name IN "
                    f"({', '.join(repr(c) for c in columns)})")).scalar() == len(columns)
            return bool(conn.execute(text(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = "
                f"DATABASE() AND table_name = '{table}'")).scalar())

        def read(present, what, sql):
            if not present:
                log.warning("byline ORCID: no %s; matching without that list", what)
                return []
            return conn.execute(text(sql)).all()
        try:
            scholars = read(has("scholar_active", ("orcid", "campus")),
                            "scholar_active.orcid and .campus (v3.16)",
                            "SELECT cwid, orcid, campus FROM scholar_active "
                            "WHERE orcid IS NOT NULL")
            ithaca = read(has("cornell_pub_orcid_mapping"), "cornell_pub_orcid_mapping table",
                          "SELECT netid, orcid FROM cornell_pub_orcid_mapping")
            wcm = read(has("pubsource_orcid_person"), "pubsource_orcid_person table",
                       "SELECT personIdentifier, orcid FROM pubsource_orcid_person "
                       "WHERE personIdentifierType = 'CWID' AND confidence_type = 'inferred'")
            # `ithaca_people`: the roster's own campus test, read once by `load()` (rule
            # (e) routes the inferred list by it).
            maps, twice = orcid_sources(scholars, ithaca, wcm, ithaca_people)
            if twice:
                log.warning("byline ORCID: %d ORCIDs are confirmed in Scholars for more "
                            "than one person and are ignored: %s", len(twice),
                            ", ".join(twice[:20]) + (" ..." if len(twice) > 20 else ""))
            by_source = Counter(source for _cwid, source in maps[campus].values())
            log.info("byline ORCID (%s): %d ORCIDs on file for one person each (%s)",
                     campus, len(maps[campus]),
                     ", ".join(f"{n} {s}" for s, n in sorted(by_source.items())) or "none")
            return maps[campus]
        except Exception as e:                                       # noqa: BLE001
            log.warning("ORCID map unavailable (%r); matching without the byline ORCID", e)
            return {}

    @staticmethod
    def _load_dept_venue_fit(conn, campus):
        """The campus's `venue_fit.DeptVenueFit`, or None when the venue term does not
        apply: AAR_VENUE_FIT is not "on", or this is not the Ithaca roster.

        The opposite failure rule from `_campus_person_types`, on purpose. A roster with
        unknown campus labels is wrong, so that query aborts the load. A roster with no
        fit table is merely the roster of the day before the feature existed, so ANY
        failure here -- no venue_fit module, no table, a bad query -- is logged and
        returns None, and the lane runs without the term."""
        if campus != CAMPUS_ITHACA or not venue_fit_enabled():
            return None
        try:
            import venue_fit
            fit = venue_fit.load_dept_fit(conn, campus)
            logging.getLogger(__name__).info(
                "venue fit: %d training rows, %d departments with >= %d",
                fit.total, sum(1 for n in fit.n_d.values() if n >= venue_fit.MIN_DEPT),
                venue_fit.MIN_DEPT)
            return fit
        except Exception as e:                                       # noqa: BLE001
            logging.getLogger(__name__).warning(
                "venue fit unavailable (%r); matching without the venue term", e)
            return None

    @staticmethod
    def _record(r, campus_types=(), cornell_linked=False):
        """One index record. `end_year` = the LATEST of the faculty/student WCM end
        years (both YEAR ints; None when both are null). Taking the max is the
        conservative reading — it penalises least — for the people who were here
        twice (student then faculty); which of the two really means "left WCM" is
        an open question on issue #159, so a reviewer can overrule this.

        `campus_types` is this cwid's `cornell-*` person types from
        `_campus_person_types`, empty for everybody on the WCM roster. Empty is the
        only shape it has today (the query returns 0 rows), so the WCM branch below is
        byte-for-byte the pre-campus code and every field it produces is unchanged.

        `cornell_linked` (a WCM record with a Cornell appointment, `_cornell_linked_cwids`)
        adds one internal key, only when true; `candidates()` never publishes it."""
        if campus_types:
            campus = CAMPUS_ITHACA
            ptype, historical = cornell_person_type(campus_types)
        else:
            campus = CAMPUS_WCM
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
            # The PRIMARY name in full, for `_display_name` only: what a pref_norm
            # match prints, exactly as `person` records it. pref_norm is lossy by
            # construction (lowercased, unpunctuated), so without this the matched
            # name was unavailable to print. Empty when the person row is missing.
            # The matcher must keep reading pref_norm and only pref_norm -- nothing
            # below this line tests `pref_full`.
            "pref_full": " ".join(x for x in (
                (r.get(k) or "").strip()
                for k in ("prefFirstName", "prefMiddleName", "prefLastName")) if x),
            "dept": r["primaryAcademicDepartment"] or "",
            "division": r["primaryAcademicDivision"] or "",
            "program": r["primaryProgram"] or "",
            "title": r["primaryTitle"] or "",
            "person_type": ptype, "historical": historical,
            "end_year": max(ends) if ends else None,
            # WCM for every row this builds today; the scope in __init__ reads it.
            # Cornell records carry no WCM end year, so `temporal_penalty` is inert
            # for them -- correct, not an oversight: endDateWCMFaculty/Student are
            # WCM appointment columns and mean nothing about an Ithaca netid.
            "campus": campus,
        }

    def candidates(self, last, fore=None, initials=None, affiliations=None, top_k=5,
                   pub_year=None, venue_subfield=None, byline_orcid=None, _swapped=False):
        """Ranked candidate CWIDs for one authorship (no identity-only score yet).

        `byline_orcid` is the bare ORCID the source attaches to this byline author
        (authorship_review.byline_orcid); pass it to enable the ORCID match. It does
        anything only on an index that holds a map (`orcid_rec`) and only when the ORCID
        is on file for somebody on this roster: that person is added to the list if the
        name did not reach them, every candidate gains an `orcid_match` key (true for
        that person, false for the rest), and the key leads the sort. Omitted, or
        matching nobody, the output is exactly what it was. It never changes a status.
        On an index holding a labelled map (AAR_BYLINE_ORCID_WCM) the matched person
        also carries `orcid_source`, the list their iD came from.

        `venue_subfield` is the Science-Metrix subfield of the paper's venue
        (`venue_fit.subfield_for`); pass it to enable the venue term. It does anything
        only on an index that holds a fit table (`dept_venue_fit`): each candidate the
        table can score gains a `venue_fit` key and the term in its `confidence`.
        Omitted, or on an index with no table, the output is exactly what it was. Like
        the temporal penalty it reaches the sort through `confidence` alone.

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
        # The roster record the byline's ORCID is on file for, or None (always None on
        # an index with no map, and for a caller that passed no ORCID).
        orcid_rec = self.orcid_rec.get(byline_orcid) if byline_orcid else None
        if (orcid_rec is not None and self.orcid_source.get(byline_orcid) == ORCID_INFERRED
                and not any(r["cwid"] == orcid_rec["cwid"] for r in pool)
                and not surnames_close(last, orcid_rec) and not surnames_close(fore, orcid_rec)):
            # An inferred iD naming someone the surname does not reach and whose surname
            # is unrelated: the row is ranked as if it carried no ORCID (SURNAME_CLOSE).
            # `fore` too: on an inverted byline (see the retry below) it holds the surname.
            orcid_rec = None
        if not pool and orcid_rec is not None and self.orcid_source and fore and not _swapped:
            # AAR_BYLINE_ORCID_WCM only (a labelled map): the ORCID found somebody, and
            # that must not cost the row the people the inverted-name retry below
            # reaches -- the ORCID ranks, it removes nobody. So when the retry has
            # somebody, the answer is the retry's with the ORCID applied to it: its `full`
            # matches, plus the ORCID's person. If that person is one of them the ORCID
            # confirms a full name match; otherwise only the ORCID keeps them on the
            # list, and they are marked as added. No retry candidate -> the ORCID's
            # person alone, built below from the byline as it came. The v3.15 map keeps
            # the rule it had (the branch after this one): with the second switch off
            # this block is never entered.
            if any(c["given_match"] == "full" for c in self.candidates(
                    fore, last, None, affiliations, None, pub_year, _swapped=True)[0]):
                out, _ = self.candidates(fore, last, None, affiliations, None, pub_year,
                                         venue_subfield, byline_orcid, _swapped=True)
                out = [c for c in out if c["given_match"] == "full" or c["orcid_match"]]
                for c in out:
                    c["cohort_size"] = len(out)
                    if c["orcid_match"] and c["given_match"] != "full":
                        c["orcid_added"] = True
                return out[:top_k], len(out)
        if not pool and orcid_rec is None:
            # PubMed sometimes files a byline with LastName and ForeName inverted --
            # pmid 42523178 has LastName='Anna J' ForeName='Podolanczuk' Initials='P' on
            # authors 2-12 -- so the surname pool is empty and the roster's own faculty
            # match nobody. Retry the two fields the other way round, `full` tier only:
            # replayed over all 2,187 open no-match rows (2026-09-20) the swap reaches 58,
            # 2 at `full` (both that pmid, both correct) and 56 initial-tier coincidences
            # ("Will Moss" -> someone surnamed Will), so the initial tier stays shut.
            # ponytail: fires only on an EMPTY pool; a swapped given name that is also a
            # real surname ("Scott") never gets here. Widen if a measured case needs it.
            if fore and not _swapped:
                out, _ = self.candidates(fore, last, None, affiliations, top_k, pub_year,
                                         venue_subfield, _swapped=True)
                out = [c for c in out if c["given_match"] == "full"]
                for c in out:
                    c["cohort_size"] = len(out)
                return out, len(out)
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
        affil_toks = _affil_tokens(affiliations)

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
            # #230: on an alternate-name copy a one-letter middle is not a name. Accepted
            # bylines mint "Erika M" for nearly everyone, and the whole-field rule below
            # would let byline "M Hissong" match it as `full`. The copy keeps the middle
            # as recorded (the label prints it); only the matcher looks past it.
            if rec.get("alt_name") and len(middle_norm) < 2:
                middle_norm = ""
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
            #
            # `via` is WHICH source the match went through, for the label and the
            # `name_source` badge: "primary" only when the pref_norm clause fired, else
            # "directory"; an alternate-name copy reports its own source (below) --
            # its pref_norm IS the alternate's given name, not the person mirror's.
            via = "directory"
            if author_given and (rec.get("pref_norm") == author_given
                                 or (author_first
                                     and author_first == rec.get("pref_norm"))):
                given_match, via = "full", "primary"
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
            cohort.append((rec, given_match, rec.get("alt_source") or via))

        # ONE CWID AT MOST ONCE (issue #227). A person indexed under both a legal and an
        # alternate name lands in the same pool twice when the surnames agree ("Ho-Yee
        # Wong" + alternate "Tommy Wong"); keep the stronger tier, first occurrence on a
        # tie (the primary is appended first), BEFORE cohort_size is counted, so the
        # curator never reads "2 WCM homonyms" for one person and the rarity term is
        # not halved by a self-rival. Same person, same cwid, so nothing downstream
        # can tell the two apart except the label.
        best = {}
        for rec, given_match, via in cohort:
            # An alternate name is evidence only when the byline spells it out. Letting it
            # into the initial tier is the widening #173 measured at 26% precision: byline
            # "Todd Rosen" reached aer2006 through alternate "Tony Rosen" on T=T, and
            # "Amanda Castillo" reached Andrew (2026-09-12 dry run, 22 of 50 fills). The
            # primary record still competes at whatever tier it earns on its own.
            if rec.get("alt_name") and given_match != "full":
                continue
            held = best.get(rec["cwid"])
            if held is None or _TIER[given_match] > _TIER[held[1]]:
                best[rec["cwid"]] = (rec, given_match, via)
        cohort = list(best.values())

        # The surname+initial cohort, counted BEFORE the ORCID adds anybody: it is the
        # rarity term of everyone the name reached, and a person the ORCID brought in
        # is not a homonym of theirs. So the name-matched candidates keep the confidence
        # they had (as each campus keeps its own in a `CampusRoster` merge), and only
        # `cohort_size`, the "choose among N" number the card shows and
        # `single_candidate` is derived from, counts the extra person.
        name_cohort = len(cohort)
        if orcid_rec is not None and orcid_rec["cwid"] not in best:
            cohort.append((orcid_rec,) + self._orcid_given_match(
                orcid_rec, author_given, author_first, author_init))
        cohort_size = len(cohort)
        fit_table = self.dept_venue_fit if venue_subfield else None
        out = []
        for k, (rec, given_match, via) in enumerate(cohort):
            affil_match, where = self._affil_match(rec, affil_toks)
            gap = (pub_year - rec["end_year"]) if (pub_year and rec["end_year"]) else None
            penalty = temporal_penalty(gap)
            fit = (fit_table.fit(rec["dept"], venue_subfield, rec["cwid"])
                   if fit_table else None)
            out.append({
                "cwid": rec["cwid"],
                "name": _display_name(rec, via),
                # Provenance of `name`, for the PM card's badge: which source the
                # match went through, and for "accepted" how many accepted papers
                # carry that name (None for every other source).
                "name_source": via, "name_n": rec.get("alt_n"),
                "dept": rec["dept"], "division": rec["division"],
                "person_type": rec["person_type"], "title": rec["title"],
                "given_match": given_match,
                "affil_dept_match": affil_match, "affil_match_on": where,
                "cohort_size": cohort_size,
                "years_after_wcm": gap,
                "confidence": self._confidence(given_match,
                                               name_cohort if k < name_cohort else cohort_size,
                                               affil_match, rec["historical"], penalty,
                                               fit_table.term(fit) if fit_table else 0.0),
            })
            if self.campus != CAMPUS_WCM:
                # Only off-WCM candidates say which campus they are: a WCM entry stays
                # byte-for-byte what it was (the stored candidate_cwids_json and every
                # replay that diffs it), and "no campus key is WCM" is already this
                # file's rule for records.
                out[-1]["campus"] = self.campus
            if fit is not None:
                # Same rule, same reason: the key exists only where there is a number.
                # A candidate the table cannot score carries no key and no term.
                out[-1]["venue_fit"] = fit
            if orcid_rec is not None:
                # And again: the key exists only on a row whose ORCID found somebody.
                out[-1]["orcid_match"] = rec["cwid"] == orcid_rec["cwid"]
                if k >= name_cohort:
                    # The name did not reach this person; the ORCID alone put them here.
                    # PM keeps such rows out of its bulk-accept lane (top_orcid_match=2).
                    out[-1]["orcid_added"] = True
                if out[-1]["orcid_match"] and byline_orcid in self.orcid_source:
                    # Which list the iD came from, on the matched person only and only
                    # from a labelled map (AAR_BYLINE_ORCID_WCM): PM limits its bulk
                    # accept to 'scholars'.
                    out[-1]["orcid_source"] = self.orcid_source[byline_orcid]
        out.sort(key=_rank_key, reverse=True)
        return out[:top_k], cohort_size

    @staticmethod
    def _orcid_given_match(rec, author_given, author_first, author_init):
        """(given_match, name source) for a person the ORCID added, i.e. one the surname
        pool and the initial test did not reach: what the byline's given name honestly
        says about them, with no credit for the ORCID.

        The tiers are the loop's own, tested in the loop's order against the same
        fields -- the publishing first name, then givenName, the whole middleName and
        the two concatenated -- so a person who would have read `full` or `initial` had
        the surname agreed reads the same here (a married or hyphenated surname is the
        common case). The one difference is the last line. Where the loop drops a
        candidate whose given name shares not even an initial with the byline, this
        returns "unknown": the ORCID is the reason they are on the list, and "unknown"
        is the lowest tier there is, worth 0.15 in `_confidence`. The selftest holds
        this to the loop for people the name does reach."""
        middle = _norm(rec["middle"])
        if author_given and (rec.get("pref_norm") == author_given
                             or (author_first and author_first == rec.get("pref_norm"))):
            return "full", "primary"
        if author_given and (author_given == rec["given_norm"]
                             or (middle and author_given in (middle, rec["given_norm"] + middle))
                             or (author_first and author_first == rec["given_norm"])):
            return "full", "directory"
        if author_init and rec["given_norm"][:1] == author_init:
            return "initial", "directory"
        return "unknown", "directory"

    @staticmethod
    def _affil_match(rec, affil_toks):
        """Does the affiliation text name the candidate's dept or division? `affil_toks`
        comes from `_affil_tokens`, so the institution's own name is already gone.

        Worth +0.25 confidence -- the same size as the WHOLE gap between a `full` and an
        `initial` given-name match (0.50 vs 0.25), so one phantom affiliation match
        exactly cancels one real full-name match. That is why this tests WORD BOUNDARIES
        rather than substrings: a department must appear as consecutive affiliation
        tokens ("Internal Medicine" needs both words, adjacent, in order) and a division
        word must EQUAL a token. The old substring test matched a division word inside
        another word -- live rows 30303/78915, byline "Rayaz A Malik", affiliation
        "Weill Cornell Medicine-Qatar, Doha, Qatar; Division of Cardiovascular Sciences,
        University of Manchester, UK.", where ram2045 (dept "Medicine") scored a phantom
        off the institution name and ram9022 (division "Vascular and Endovascular
        Surgery") scored one off "Vascular" inside "cardiovascular". Both reached 0.700
        and tied; the curator resolved both rows to ram2045. Fixing only the institution
        half makes those rows WORSE -- ram2045 loses its phantom while ram9022 keeps
        one, flipping the row to the wrong Malik -- so the two halves ship together.
        Measured that way: an institution-strip-only variant scored 18 fixes / 3 breaks
        against curator resolutions, and adding word boundaries took it to 18 / 1.

        Replayed over all 30,711 authorship_review rows / 64,972 candidate entries on
        prod with the post-#201 ranking keys: affiliation matches fall 16,945 -> 11,240,
        3,970 rows see a candidate change, 203 top picks move, and of the 24 that are
        curator-resolved this is 18 fixes, 1 break, 5 neutral. Matches CREATED: 0.

        That zero is the safety argument, and the claim it supports is a scoped one:
        WITHIN a single affiliation string, the word-boundary test is a strict SUBSET of
        the old substring test, so it can only ever remove a match. It is not an
        unconditional "this cannot manufacture a false positive anywhere" -- and between
        #203 and the `_AFFIL_SEP` boundary token, the per-affiliation qualifier did not
        hold. `_affil_tokens` flattened every affiliation string's tokens into one
        boundary-less tuple, so a multi-word department (and the institution-phrase strip
        with it) could match ACROSS two unrelated affiliations: dept "Internal Medicine"
        matched the byline ["Weill Cornell Internal", "Medicine Institute of Something
        Else, Chicago, IL"], which the pre-#203 space-joined substring test never did.
        1,948 open PubMed and 1,841 open Scopus rows carry more than one affiliation
        string. The sentinel restores the qualifier -- every n-gram window that spans a
        seam contains a token no dept, division or phrase token can equal -- and the
        selftest below holds that byline at False.

        The department n-gram compares `_dept_key` tokens, not raw ones: "&" and "and"
        are the same connector, and in a department of two or more words a trailing
        plural "s" is folded (see the block above `_dept_key`). That WIDENS the test, so
        unlike #203 it does create matches, and the measured set is the safety argument.
        Over all 87,084 authorship_review rows on prod (2026-11-23), stored candidates
        against today's roster: 545 matches created (203 of 78,149 WCM entries, 342 of
        67,376 Cornell Ithaca ones), 0 removed. Every one of the 545 is the candidate's
        own department name with "&" for "and" or back, an "and" left out, or a plural:
        "Earth & Atmospheric Sciences" 292, "Immunology and Microbial Pathogenesis" 54,
        "Brain & Mind Research Institute" 51, "Population Medicine and Diagnostic
        Sciences" 30, and 20 smaller ones. Replaying both rules moves 16 top picks (8
        assigned, 7 open, 1 rejected). The 8 curator-resolved ones are all fixes (seven
        "A Singh" rows assigned to as3527, one "Wei Wang" assigned to wew4002) and none
        is a break; 502 resolved people gain the match, none loses it or drops a rank.
        The rejected row (80505) trades one wrong Zhang for another. One open row moves
        the wrong way: 5959, byline "Junsung Lee", almost certainly jl3939 "John
        (Junsung) Lee". The byline reaches him only at the initial tier, so once the
        rival's "Mechanical & Aerospace Engineering" matches as well as his "Biomedical
        Engineering" does, the tie goes to confidence, where his alumni flag costs him
        0.10.
        Every guarantee above still holds. Tokens are still whole words, so the
        "cardiovascular" case stays False; the institution names are stripped before
        either side is folded; and the seam token is neither "and" nor foldable, so no
        window can span two affiliation strings."""
        if not affil_toks:
            return False, None
        dept_n = _norm(rec["dept"])
        dept_toks = _dept_key(name_tokens(rec["dept"]), False)
        fold = len(dept_toks) >= 2
        if fold:
            dept_toks = _dept_key(dept_toks, True)
        n = len(dept_toks)
        if dept_n and len(dept_n) >= 4 and n:
            toks = _dept_key(affil_toks, fold)
            if any(toks[i:i + n] == dept_toks for i in range(len(toks) - n + 1)):
                return True, "dept"
        # division: any distinctive word (>=5 chars) standing as a whole token
        for word in (rec["division"] or "").replace("&", " ").replace(",", " ").split():
            w = _norm(word)
            if len(w) >= 5 and w in affil_toks:
                return True, "division"
        return False, None

    @staticmethod
    def _confidence(given_match, cohort_size, affil_match, historical, penalty=0.0,
                    venue=0.0):
        """Explainable ordering aid, NOT a probability. `historical` is the flat
        person-type flag (alumni/inactive/emeritus); `penalty` is the already-capped
        temporal term from `temporal_penalty`, applied exactly once and ONLY here —
        this is the single route by which staleness reaches either sort key. `venue` is
        the signed, already-bounded term from `venue_fit.venue_term` (+0.15 / -0.20 / 0),
        added before the clip and likewise the venue's only route to either key."""
        base = 0.50 if given_match == "full" else 0.25 if given_match == "initial" else 0.15
        rarity = 0.40 / max(cohort_size, 1)
        affil = 0.25 if affil_match else 0.0
        hist = -0.10 if historical else 0.0
        return round(max(0.0, min(1.0, base + rarity + affil + hist - penalty + venue)), 3)


# ---- per-byline campus routing ---------------------------------------------
class CampusRoster:
    """The WCM index plus a lazily loaded Cornell-Ithaca index, picked PER BYLINE by
    `byline_campuses` -- what every lane proposing candidates on WCM-universe rows now
    holds instead of a bare WCM `IdentityIndex`. Same `candidates()` signature and
    `(candidates, cohort_size)` return, so `aar_matcher.match_authorship` and every
    replay tool take it unchanged.

      {wcm}     -> the WCM index's candidates(), untouched: a WCM-only row is byte-for-
                   byte what it was.
      {ithaca}  -> the Ithaca index's candidates(): a Cornell byline proposes Cornell
                   people, so top_cwid (the lane's cands[0]) is a Cornell person.
      both      -> both lists merged by `_rank_key` (stable, so WCM keeps exact ties --
                   the pre-campus answer), cut at the caller's top_k. Each entry keeps
                   the confidence its OWN campus's cohort gave it (the R1 rarity term is
                   per campus), but `cohort_size` on every entry becomes the combined
                   count: it is the "choose among N" number the card shows and what
                   `single_candidate` is derived from, and a person unique on WCM is not
                   a single candidate on a byline that also offers a Cornell one -- PM
                   would render the single-candidate card and hide the Cornell option.

    The Ithaca roster loads on the FIRST byline that needs it, once per instance (one
    instance per run), never per row; a run with no Cornell byline never pays for it.
    `campuses` lets a caller that knows more than the text (the Scopus lane's afids)
    state the set outright.

    A Cornell-only byline ALSO keeps the WCM candidates who hold a Cornell appointment
    (`IdentityIndex.cornell_linked`: bridged dual appointees and `affiliate-cornell`).
    Dual appointees live ONLY on the WCM roster -- the Cornell sync keeps their netids
    off the Ithaca one -- so without this their own Ithaca-side papers ("Meinig School,
    Weill Hall, Ithaca") would lose them to a Cornell homonym or to nobody. Every other
    WCM person stays off a Cornell-only byline: that is the homonym flood the byline rule
    exists to keep out. They are ranked with the Cornell list by `_rank_key` (stable,
    Cornell first so a Cornell person keeps an exact tie on a Cornell byline), searched
    past the caller's top_k on the WCM side so a linked person is not cut by unlinked
    WCM homonyms that are then thrown away, and each keeps its WCM confidence; the
    combined count is the cohort, as for `both`. No linked match -> the Ithaca answer
    exactly.

    `byline_orcid`, with only AAR_BYLINE_ORCID on, travels with `venue_subfield` and by
    the same rule, below: it reaches the Ithaca index only when that index's answer is
    returned as it stands, and a list with a WCM candidate on it carries no match.

    With AAR_BYLINE_ORCID_WCM on as well (the rosters then hold labelled maps, see
    `orcid_sources`) the ORCID goes to every roster the byline consults: the WCM index
    on a WCM-only byline, both on a two-campus one, and on a Cornell-only one the Ithaca
    index plus the WCM index it already asks for linked dual appointees. The person the
    ORCID is on file for leads the merged list, every other candidate carries
    `orcid_match` false, and on a Cornell-only byline that person is kept even when they
    hold no Cornell appointment: the ORCID put them there, not a shared name. A WCM-only
    byline still never reaches the Ithaca roster. An ORCID that finds a person on each
    roster is ignored for the row. Nothing here changes a status or drops a candidate.
    A WCM candidate the ORCID adds is a WCM candidate merged in, so the venue term is
    withheld from that row like any other merged one.

    `venue_subfield` reaches the Ithaca index only when its answer is returned as it
    stands: the WCM roster offers nobody for the byline (no namesake on `both`, no linked
    dual appointee on a Cornell-only one). Whenever a WCM candidate is merged in, the
    subfield is withheld from both rosters and the merged list is what it was before the
    venue term existed, because only the Cornell side could earn the term (see the
    venue-fit block above).

    `campus` / `by_surname` answer as the WCM index does, for the callers that assert
    the lane's campus or log the roster size."""

    campus = CAMPUS_WCM

    def __init__(self, wcm, ithaca=None, load_ithaca=None):
        self.wcm = wcm
        self._ithaca = ithaca
        self._load_ithaca = load_ithaca
        self.n_ithaca_bylines = 0        # observability: bylines that reached Cornell

    @property
    def by_surname(self):
        return self.wcm.by_surname

    @property
    def ithaca(self):
        if self._ithaca is None:
            # No loader (a hand-built roster): an EMPTY Ithaca index, so a Cornell byline
            # proposes nobody rather than falling back onto WCM homonyms.
            self._ithaca = (self._load_ithaca() if self._load_ithaca
                            else IdentityIndex([], campus=CAMPUS_ITHACA))
            if self._ithaca.campus != CAMPUS_ITHACA:
                raise RuntimeError(f"Ithaca roster has campus {self._ithaca.campus!r}")
        return self._ithaca

    @classmethod
    def load(cls, alternate_names=None):
        """Both rosters from reciterdb, the Ithaca one deferred. Alternate names are
        read once and shared, so the lazy load does not rescan DynamoDB."""
        if alternate_names is None:
            alternate_names = load_alternate_names()
        return cls(IdentityIndex.load(alternate_names=alternate_names),
                   load_ithaca=lambda: IdentityIndex.load(campus=CAMPUS_ITHACA,
                                                          alternate_names=alternate_names))

    def candidates(self, last, fore=None, initials=None, affiliations=None, top_k=5,
                   pub_year=None, campuses=None, venue_subfield=None, byline_orcid=None):
        campuses = campuses or byline_campuses(affiliations)
        args = (last, fore, initials, affiliations, top_k, pub_year)
        if CAMPUS_ITHACA not in campuses:
            # A WCM index holds a map only under AAR_BYLINE_ORCID_WCM; without one the
            # ORCID is not passed and this is the call it always was.
            return self.wcm.candidates(*args, byline_orcid=(byline_orcid if self.wcm.orcid_rec
                                                            else None))
        self.n_ithaca_bylines += 1
        return self._with_ithaca(args, CAMPUS_WCM in campuses, venue_subfield, byline_orcid)

    def _with_ithaca(self, args, both, venue_subfield, byline_orcid):
        """`candidates()` for a byline that names Ithaca. Each roster consulted is
        handed the ORCID if it holds a map that applies to a merged list: the WCM index
        whenever it holds one, the Ithaca index when its map is a labelled one
        (`orcid_source`). The v3.15 Ithaca map is withheld from a merge, as it always
        was."""
        last, fore, initials, affiliations, top_k, pub_year = args
        w_orcid = byline_orcid if self.wcm.orcid_rec else None
        if both:
            w, w_n = self.wcm.candidates(*args, byline_orcid=w_orcid)
        else:
            linked = self.wcm.cornell_linked
            # top_k=None: the whole ranked WCM cohort (candidates() only slices by it).
            # Kept: the linked, and the person the ORCID is on file for, who is no homonym.
            w = [d for d in (self.wcm.candidates(last, fore, initials, affiliations,
                                                 None, pub_year, byline_orcid=w_orcid)[0]
                             if linked or w_orcid else [])
                 if d["cwid"] in linked or d.get("orcid_match")]
            w_n = len(w)
        if not w:
            # No WCM candidate to merge: the Ithaca answer as it stands, and the one
            # answer the venue term may reach (see the venue-fit block above).
            return self.ithaca.candidates(*args, venue_subfield, byline_orcid)
        c, c_n = self.ithaca.candidates(
            *args, byline_orcid=byline_orcid if self.ithaca.orcid_source else None)
        if any(d.get("orcid_match") for d in w) and any(d.get("orcid_match") for d in c):
            # One ORCID, a person on each roster (two fallback lists disagreeing; Scholars
            # would have settled it). It cannot say which, so this row is ranked without.
            return self._with_ithaca(args, both, venue_subfield, None)
        # both: WCM first, so an exact tie keeps the pre-campus pick. Cornell-only byline:
        # Cornell first, so its tie is Cornell's.
        head = w + c if both else c + w
        if any("orcid_match" in d for d in head):
            # The ORCID found its person on one roster; the other roster's candidates say
            # so too, as every candidate on an ORCID-matched row does.
            for d in head:
                d.setdefault("orcid_match", False)
        merged = sorted(head, key=_rank_key, reverse=True)[:top_k]
        for d in merged:
            d["cohort_size"] = w_n + c_n
        return merged, w_n + c_n


def orcid_rows_without_map(idx, stored):
    """(no map, no labelled map): how many of the `stored` candidate lists (the JSON of
    candidate_cwids_json) the ORCID ranked through a roster that holds no such map here.
    `idx` is an `IdentityIndex` or a `CampusRoster`. What the replay tools ask before
    replaying rows the ORCID ranked.

    A row is asked of the roster ITS matched candidate is on, never of the two together.
    A WCM index holding a map says nothing about the Ithaca one, which loads separately
    and can fail separately; an Ithaca row replayed on a roster whose Ithaca index holds
    no map reads as naming nobody, whatever the WCM index holds.

    A matched candidate (`orcid_match` true) that carries `orcid_source` was ranked with
    AAR_BYLINE_ORCID_WCM on: it needs a labelled map on its own roster, Ithaca when it
    carries that campus, else WCM, and is counted second. Any other list was ranked by
    the v3.15 match, which only an Ithaca index ever held: it needs a map of either kind
    on the Ithaca index and is counted first. Asking for an Ithaca row loads a
    `CampusRoster`'s Ithaca index."""
    import json
    no_map = no_labelled = 0
    for s in stored:
        if '"orcid_match"' not in (s or ""):
            continue
        try:
            hit = next((c for c in json.loads(s)
                        if isinstance(c, dict) and c.get("orcid_match")), {})
        except (ValueError, TypeError):
            hit = {}
        if hit.get("orcid_source"):
            on_wcm = hit.get("campus", CAMPUS_WCM) == CAMPUS_WCM
            no_labelled += not getattr(getattr(idx, "wcm" if on_wcm else "ithaca", idx),
                                       "orcid_source", None)
        else:
            no_map += not getattr(getattr(idx, "ithaca", idx), "orcid_rec", None)
    return no_map, no_labelled


# ---- offline self-test ------------------------------------------------------
def _selftest():
    """Builds an IdentityIndex from hand-built records (no DB) to prove the
    middleName-as-alternate-given-name fix (Judy/Hua Zhong, PMID 40681448), its
    narrowing to exact-match-only (issue #173), the person-mirror publishing name
    (issue #171), and the temporal-plausibility penalty (issue #159)."""
    def rec(given, middle, surname, end_year=None, cwid=None, pref_first=None,
            pref_full=None):
        return {
            "cwid": cwid or f"{given or middle}_{surname}".lower(),
            "given": given, "middle": middle, "surname": surname,
            "given_norm": _norm(given), "surname_norm": _norm(surname),
            "pref_norm": _norm(pref_first),
            "pref_full": pref_full or "",
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
        # The DISPLAY half of that same defect. The matching assertions directly above
        # must keep passing unchanged: the label is deduped, the ranking still sees the
        # whole raw field, and these two live side by side so a future edit cannot quietly
        # collapse them into one behaviour.
        ("a comma-duplicated middleName is collapsed in the LABEL",
         _display_middle("Keith Richards,Keith Richards") == "Keith Richards"),
        ("...the dkk4001 case, which is what was reported",
         _display_middle("Kwon,Kwon") == "Kwon"),
        ("a comma joining two DIFFERENT names keeps both, in order",
         _display_middle("Kwon,Lee") == "Kwon Lee"),
        ("duplicates differing only by case or padding still collapse",
         _display_middle("Kwon, kwon ") == "Kwon"),
        ("a middleName with no comma is returned untouched, byte for byte",
         _display_middle("Wing Guinevere") == "Wing Guinevere"),
        ("empty and None pass straight through",
         _display_middle("") == "" and _display_middle(None) is None),
        ("an all-empty comma field is NOT turned into a blank name component",
         _display_middle(",") == ","),
        ("the full label for dkk4001 reads once, not twice",
         _display_name({"given": "Dylan", "middle": "Kwon,Kwon", "surname": "Kim"})
         == "Dylan Kwon Kim"),
        # Matching must be unmoved by all of the above.
        ("the ranking still reads the RAW field, so its behaviour is unchanged",
         junk.candidates("J", "Keith Richards,Keith Richards", "K")[0] != []),
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

    # --- the candidate LABEL (`name`) ----------------------------------------
    # ONE name: the full name of the source that matched, as that source records it.
    # A pref_norm match prints the primary name (`pref_full`); every directory match
    # prints the directory name, middle included. No parenthetical, ever -- which
    # source it was is `name_source`, data for the PM card's badge.
    legal_join = (lambda r: " ".join(x for x in (r["given"], r["middle"], r["surname"])
                                     if x))
    mila = rec("Shuo", "", "Sun", cwid="mis4060", pref_first="Mila", pref_full="Mila Sun")
    tony = rec("Anthony", "Ehren", "Rosen", cwid="aer2006", pref_first="Tony",
               pref_full="Tony Rosen")
    jae = rec("Joseph", "", "Gardella", cwid="jog4030", pref_first="Jae",
              pref_full="Jae Gardella")
    agree = rec("Jane", "Q", "Doe", cwid="agree", pref_first="Jane", pref_full="Jane Q Doe")
    nopref = rec("Jane", "Q", "Doe", cwid="nopref")
    nomiddle = rec("Xiaoxuan", "", "Chen", cwid="xic4004", pref_first="Emily",
                   pref_full="Emily Chen")
    checks += [
        ("the live anchor prints the publishing name, alone (pmid 42670968, mis4060)",
         _display_name(mila, "primary") == "Mila Sun"),
        ("a primary-name match prints the primary name in full, nothing behind it",
         _display_name(tony, "primary") == "Tony Rosen"),
        ("a SAME-INITIAL disagreement prints the matched name too (jog4030 publishes "
         "as Jae, the directory says Joseph)",
         _display_name(jae, "primary") == "Jae Gardella"),
        ("no middleName, differing first name -> the primary name, no parenthetical",
         _display_name(nomiddle, "primary") == "Emily Chen"),
        ("a directory match prints the directory name, byte-identical to the old join, "
         "middle included", _display_name(mila) == "Shuo Sun"
         and _display_name(tony) == legal_join(tony) == "Anthony Ehren Rosen"),
        ("names that AGREE print once, whichever source matched",
         _display_name(agree, "primary") == _display_name(agree) == legal_join(agree)
         == "Jane Q Doe"),
        ("an ABSENT primary name falls back to the directory name even on a primary "
         "match", _display_name(nopref, "primary") == legal_join(nopref) == "Jane Q Doe"),
        ("no label carries the old parenthetical, from any source",
         all("(HR: " not in _display_name(r, v)
             for r in (mila, tony, jae, nomiddle, agree, nopref)
             for v in ("primary", "directory", "alternate", "accepted"))),
        ("an empty middleName never leaves a double space in either branch",
         "  " not in _display_name(mila) and "  " not in _display_name(nomiddle, "primary")
         and "  " not in _display_name(rec("Jane", "", "Doe", cwid="nm"))),
    ]

    # The label is downstream of the match: relabelling must not move given_match,
    # confidence, cohort_size, name_source or the ranking. Same index, scored with
    # and without the raw primary name present.
    lbl = IdentityIndex([mila, rec("Yuqing", "", "Sun", cwid="yus9035",
                                   pref_first="Erica", pref_full="Erica Sun")])
    with_pref, coh_with = lbl.candidates("Sun", "Mila", "M")
    stripped = IdentityIndex([{**mila, "pref_full": ""},
                              {**rec("Yuqing", "", "Sun", cwid="yus9035",
                                     pref_first="Erica"), "pref_full": ""}])
    no_pref, coh_no = stripped.candidates("Sun", "Mila", "M")
    keys = ("cwid", "given_match", "confidence", "cohort_size", "affil_dept_match",
            "person_type", "dept", "years_after_wcm", "name_source", "name_n")
    checks += [
        ("dropping the RAW primary name changes nothing the matcher reads -- cwid, "
         "given_match, confidence, cohort_size, name_source and order are identical",
         [tuple(c[k] for k in keys) for c in with_pref]
         == [tuple(c[k] for k in keys) for c in no_pref] and coh_with == coh_no),
        ("...and `name` is the ONLY field that moved",
         [c["name"] for c in with_pref] != [c["name"] for c in no_pref]
         and with_pref[0]["name"] == "Mila Sun" and no_pref[0]["name"] == "Shuo Sun"),
        ("the anchor still reaches the `full` tier through pref_norm, and the "
         "candidate says so: name_source primary, name_n None",
         with_pref[0]["cwid"] == "mis4060" and with_pref[0]["given_match"] == "full"
         and with_pref[0]["name_source"] == "primary" and with_pref[0]["name_n"] is None),
    ]

    # (c) the primary source through candidates(): pref_full "Tony Rosen" against the
    # directory's "Anthony Ehren Rosen" prints the primary name and badges it.
    tony_c = IdentityIndex([tony]).candidates("Rosen", "Tony", "T")[0][0]
    # (d) a directory match keeps the WHOLE directory name even where the byline's
    # middle disagrees: "David K Jones" against David Randolph Jones. The mismatch is
    # the information; trimming to "David Jones" would hide it.
    jones = IdentityIndex([rec("David", "Randolph", "Jones", cwid="dkj")]).candidates(
        "Jones", "David K", "DK")[0][0]
    # (e) the live duplicate: given Sandra, middle Huicochea, surname Castellanos
    # printed "Sandra Huicochea Castellanos (HR: Sandra Huicochea Castellanos)". Once,
    # through the given+middle concatenation -- and once through an alternate that
    # spells the same name, the shape that produced it.
    sandra = rec("Sandra", "Huicochea", "Castellanos", cwid="shc", pref_first="Sandra",
                 pref_full="Sandra Huicochea Castellanos")
    sandra_c = IdentityIndex([sandra]).candidates(
        "Castellanos", "Sandra Huicochea", "SH")[0][0]
    sandra_alt = IdentityIndex([sandra], alternate_names={"shc": [
        {"firstName": "Sandra Huicochea", "lastName": "Castellanos"}]}).candidates(
        "Castellanos", "Sandra Huicochea", "SH")[0]
    checks += [
        ("(c) a byline naming the primary name gets that name in full: 'Tony Rosen', "
         "name_source primary",
         tony_c["name"] == "Tony Rosen" and tony_c["name_source"] == "primary"
         and tony_c["name_n"] is None and tony_c["given_match"] == "full"),
        ("(d) 'David K Jones' against David Randolph Jones prints 'David Randolph "
         "Jones' -- the directory name in full, middle kept -- name_source directory",
         jones["name"] == "David Randolph Jones" and jones["name_source"] == "directory"
         and jones["given_match"] == "full"),
        ("(e) 'Sandra Huicochea Castellanos' prints exactly once, no parenthetical",
         sandra_c["name"] == "Sandra Huicochea Castellanos"
         and sandra_c["name_source"] == "directory"),
        ("(e) ...and through an alternate spelling the same name: still one "
         "candidate, still one name (the primary's, on the tie)",
         len(sandra_alt) == 1 and sandra_alt[0]["name"] == "Sandra Huicochea Castellanos"
         and sandra_alt[0]["name_source"] == "directory"),
    ]

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

    # --- affiliation match is word-boundary, institution name removed ---------
    # An affiliation match is worth +0.25, the whole gap between `full` and `initial`,
    # so a phantom one exactly cancels a real full-name match. Two false-positive
    # families, both measured live and both fixed here.
    bare_wcm = ["Weill Cornell Medicine, New York, NY, USA."]
    dept_med = IdentityIndex([dict(rec("John", "", "Doe", cwid="med1"), dept="Medicine")])
    checks += [
        ("FAMILY 1: a bare 'Weill Cornell Medicine' affiliation no longer names the "
         "Department of Medicine (4,545 identities carry that department)",
         dept_med.candidates("Doe", "John", "J", bare_wcm)[0][0]["affil_dept_match"]
         is False),
        ("...and the strip is order-safe -- 'Weill Cornell' must not shorten the same "
         "text to a bare, self-matching 'Medicine'",
         _affil_tokens(bare_wcm) == ("new", "york", "ny", "usa")),
        ("...while 'Department of Medicine, Weill Cornell Medicine' still DOES name it "
         "-- the asymmetry is the point",
         dept_med.candidates("Doe", "John", "J",
                             ["Department of Medicine, Weill Cornell Medicine, New York"]
                             )[0][0]["affil_match_on"] == "dept"),
    ]

    # The anchor pair: live rows 30303 and 78915, byline "Rayaz A Malik", both resolved
    # by the curator to ram2045. Each Malik used to collect a phantom off a different
    # family and the two tied at 0.700. Fixing family 1 ALONE makes these rows worse --
    # ram2045 loses his phantom while ram9022 keeps one, flipping the row to the wrong
    # Malik -- which is why both families ship together.
    malik_affil = ["Weill Cornell Medicine-Qatar, Doha, Qatar; Division of "
                   "Cardiovascular Sciences, University of Manchester, UK."]
    maliks = IdentityIndex([
        dict(rec("Rayaz", "Ahmed", "Malik", cwid="ram2045"), dept="Medicine"),
        dict(rec("Rajesh", "K", "Malik", cwid="ram9022"),
             division="Vascular and Endovascular Surgery"),
    ])
    mal, _ = maliks.candidates("Malik", "Rayaz A", "RA", malik_affil)
    by_malik = {c["cwid"]: c for c in mal}
    checks += [
        ("both Maliks are still candidates -- this narrows the affiliation test, it "
         "does not drop anyone from the cohort", set(by_malik) == {"ram2045", "ram9022"}),
        ("ram2045's dept 'Medicine' no longer matches off the institution name (rows "
         "30303/78915)", by_malik["ram2045"]["affil_dept_match"] is False),
        ("FAMILY 2: ram9022's division word 'Vascular' no longer matches INSIDE "
         "'cardiovascular'", by_malik["ram9022"]["affil_dept_match"] is False),
        ("...so neither phantom survives and the +0.25 separates neither of them",
         all(c["affil_dept_match"] is False for c in mal)),
    ]

    # Positive controls: the narrowing must not cost a genuine match.
    vasc = IdentityIndex([dict(rec("Ann", "", "Roe", cwid="v1"),
                               division="Vascular and Endovascular Surgery")])
    internal = IdentityIndex([dict(rec("Ann", "", "Roe", cwid="i1"),
                                   dept="Internal Medicine")])
    cmp_div = IdentityIndex([dict(rec("Ann", "", "Roe", cwid="c1"),
                                  division="Cornell Medical Practice")])
    checks += [
        ("a division word standing as a whole token still matches",
         vasc.candidates("Roe", "Ann", "A",
                         ["Division of Vascular Surgery, Weill Cornell Medicine"]
                         )[0][0]["affil_match_on"] == "division"),
        ("a multi-word department still matches when its words are consecutive and in "
         "order", internal.candidates("Roe", "Ann", "A",
                                      ["Department of Internal Medicine, Weill Cornell "
                                       "Medicine"])[0][0]["affil_match_on"] == "dept"),
        ("...but 'Internal Medicine' does NOT match an affiliation naming only "
         "'Medicine' -- consecutive means both words",
         internal.candidates("Roe", "Ann", "A",
                             ["Department of Medicine, Weill Cornell Medicine"]
                             )[0][0]["affil_dept_match"] is False),
        ("division 'Cornell Medical Practice' no longer self-matches a bare WCM "
         "affiliation through the word 'Cornell' (7 people)",
         cmp_div.candidates("Roe", "Ann", "A", bare_wcm)[0][0]["affil_dept_match"]
         is False),
    ]

    # --- the seam between two affiliation strings ----------------------------
    # Post-#203 regression, live between #203 and _AFFIL_SEP: _affil_tokens flattened
    # every affiliation's tokens into ONE boundary-less tuple, so a multi-word dept --
    # and the institution-phrase strip with it -- could match across two unrelated
    # affiliations. 1,948 open PubMed and 1,841 open Scopus rows carry more than one
    # affiliation string. The pre-#203 code joined them with a space and tested a
    # substring, so this shape could not arise there.
    seam = ["Weill Cornell Internal",
            "Medicine Institute of Something Else, Chicago, IL"]
    checks += [
        ("a multi-word department does NOT match across the seam between two "
         "affiliation strings ('...Internal' | 'Medicine...')",
         internal.candidates("Roe", "Ann", "A", seam)[0][0]["affil_dept_match"]
         is False),
        ("...because a boundary token sits between them",
         _affil_tokens(seam) == ("internal", _AFFIL_SEP, "medicine", "institute", "of",
                                 "something", "else", "chicago", "il")),
        ("...while the same department still matches WITHIN one affiliation when a "
         "second, unrelated one is present alongside it",
         internal.candidates("Roe", "Ann", "A",
                             ["Department of Internal Medicine, Weill Cornell Medicine",
                              "Institute of Something Else, Chicago, IL"]
                             )[0][0]["affil_match_on"] == "dept"),
        ("the institution-phrase strip cannot span the seam either -- 'Weill' and "
         "'Cornell' in two different strings are not the phrase 'Weill Cornell'",
         _affil_tokens(["Some Place Weill", "Cornell Medicine, New York"])
         == ("some", "place", "weill", _AFFIL_SEP, "cornell", "medicine", "new",
             "york")),
        ("nor can a division word ever BE the boundary token: _norm keeps [a-z0-9] "
         "only, so no dept/division/phrase token can equal it",
         _norm(_AFFIL_SEP) == "" and _AFFIL_SEP not in name_tokens("Vascular | Surgery")
         and all(_AFFIL_SEP not in p for p in _INSTITUTION_TOKENS)),
    ]

    # --- affiliated institutions that name a department in their OWN name ----
    # Same family 1 as WCM's, on the other institutions the home-institution keyword
    # list admits: 106 open rows scored a phantom off "Hospital for Special Surgery"
    # against a Surgery department, 8 off "Sloan Kettering Cancer Center" against a
    # Cancer/Oncology one (measured on the live open queue, 2026-09-04).
    surg = IdentityIndex([dict(rec("Ann", "", "Roe", cwid="s1"), dept="Surgery")])
    onc = IdentityIndex([dict(rec("Ann", "", "Roe", cwid="o1"),
                              division="Cancer Prevention and Control")])
    checks += [
        ("'Hospital for Special Surgery' no longer names a Department of Surgery",
         surg.candidates("Roe", "Ann", "A", ["Hospital for Special Surgery, New York, "
                                             "NY"])[0][0]["affil_dept_match"] is False),
        ("...while 'Department of Surgery, Weill Cornell Medicine' still does -- the "
         "same asymmetry as family 1",
         surg.candidates("Roe", "Ann", "A",
                         ["Department of Surgery, Weill Cornell Medicine"]
                         )[0][0]["affil_match_on"] == "dept"),
        ("...and naming the department alongside the institution still matches there "
         "too ('Department of Surgery, Hospital for Special Surgery')",
         surg.candidates("Roe", "Ann", "A",
                         ["Department of Surgery, Hospital for Special Surgery, New "
                          "York, NY"])[0][0]["affil_match_on"] == "dept"),
        ("both Sloan Kettering spellings are stripped (the hyphen is not a token "
         "boundary _affil_words treats differently from a space)",
         _affil_tokens(["Memorial Sloan Kettering Cancer Center, New York, NY"])
         == _affil_tokens(["Memorial Sloan-Kettering Cancer Center, New York, NY"])
         == ("new", "york", "ny")),
        ("...so a Cancer division no longer self-matches off that institution name "
         "(the division test is a whole-token one, and 'Cancer' is a token of it)",
         onc.candidates("Roe", "Ann", "A", ["Memorial Sloan Kettering Cancer Center, "
                                            "New York, NY"])[0][0]["affil_dept_match"]
         is False),
    ]

    # --- "&" vs "and", and plurals, in a department name (_dept_key) ----------
    # Live rows 84447 / 84457 (byline "A Singh") and 401 (byline "Ajay Singh"): the
    # roster has as3527 in "Population Medicine & Diagnostic Science", the byline says
    # "Population Medicine and Diagnostic Sciences". Every Cornell "A. Singh" ties on the
    # initial tier, so the department is the only thing that can pick Ajay, and before
    # this it said False and the row went to Aadya Singh (as3295).
    pmds_affil = ["Department of Population Medicine and Diagnostic Sciences, College of "
                  "Veterinary Medicine, Cornell University, Ithaca, NY 14853."]
    pmds = "Population Medicine & Diagnostic Science"
    singhs = IdentityIndex([dict(rec("Ajay", "", "Singh", cwid="as3527"), dept=pmds),
                            rec("Aadya", "", "Singh", cwid="as3295")])
    sing, _ = singhs.candidates("Singh", "A", "A", pmds_affil)

    def am(dept, affils):
        return IdentityIndex._affil_match(dict(rec("Ann", "", "Roe", cwid="d1"), dept=dept),
                                          _affil_tokens(affils))[0]
    checks += [
        ("rows 84447/84457: 'Medicine & Diagnostic Science' on the roster matches "
         "'Medicine and Diagnostic Sciences' in the byline, and picks as3527 over the "
         "initial-tier tie", sing[0]["cwid"] == "as3527"
         and sing[0]["affil_match_on"] == "dept"
         and sing[1]["cwid"] == "as3295" and sing[1]["affil_dept_match"] is False),
        ("...and the other way round: 'and' on the roster, '&' in the byline",
         am("Earth and Atmospheric Sciences",
            ["Department of Earth & Atmospheric Sciences, Ithaca"])),
        ("...and the same department with the 'and' left out ('Brain Mind Research "
         "Institute', 6 stored candidates)",
         am("Brain and Mind Research Institute",
            ["Feil Brain Mind Research Institute, Weill Cornell Medicine, New York, NY"])),
        ("the words must still be adjacent and in order: an extra word between them is "
         "not the department",
         not am(pmds, ["Department of Population Medicine and Clinical Diagnostic "
                       "Sciences, Ithaca, NY"])),
        ("dropping 'and' cannot reach across the seam between two affiliation strings",
         not am(pmds, ["Department of Population Medicine and",
                       "Diagnostic Sciences, Somewhere Else"])),
        ("...because _dept_key passes the boundary token through untouched",
         _dept_key(("population", "medicine", "and", _AFFIL_SEP, "sciences"), True)
         == ("population", "medicine", _AFFIL_SEP, "science")),
        ("a ONE-word department is not plural-folded: 'Pediatrics' does not match "
         "'Pediatric Rehabilitation' (26 stored candidates would have, measured)",
         not am("Pediatrics", ["Hospital for Special Surgery Pediatric Rehabilitation "
                               "& Young Athlete Center, New York, NY"])),
        ("...while a one-word department still matches itself exactly",
         am("Pediatrics", ["Department of Pediatrics, Weill Cornell Medicine"])),
        ("a short word keeps its 's' ('news' is not 'new')",
         not am("News Media", ["Department of New Media, Somewhere, NY"])
         and am("News Media", ["Department of News Media, Somewhere, NY"])),
        ("folding is per WHOLE token, so #203's 'cardiovascular' case stays out: "
         "'Vascular Sciences' does not match 'Cardiovascular Sciences'",
         not am("Vascular Sciences", ["Division of Cardiovascular Sciences, University "
                                      "of Manchester, UK."])),
        ("...and the institution strip still runs first: 'Weill Cornell Graduate School "
         "of Medical Sciences' does not name a 'Medical Science' department",
         not am("Medical Science", ["Weill Cornell Graduate School of Medical Sciences, "
                                    "New York, NY"])),
    ]

    # --- campus scope: R1, the rarity term Cornell Ithaca would take from WCM -----
    # The measured damage (onboarding plan, risk R1): loading 15,030 Cornell Ithaca
    # people grows the surname+initial cohort of 5,751 WCM people, and takes 1,161 of
    # them from cohort 1 to cohort >1 -- from the matcher's strongest signal, a rarity
    # term of 0.40/1, to 0.20, in favour of a rival who can never be the right answer.
    #
    # `cornell` records here carry campus=CAMPUS_ITHACA the way `_record` sets it from
    # the `cornell-ithaca` marker in person_person_type.
    def ithaca(given, surname, cwid, types=("cornell-ithaca", "cornell-faculty")):
        label, hist = cornell_person_type(types)
        return dict(rec(given, "", surname, cwid=cwid),
                    campus=CAMPUS_ITHACA, person_type=label, historical=hist)

    # A WCM person who is today the UNIQUE holder of their surname+initial, and an
    # Ithaca homonym who would join that cohort.
    wcm_unique = rec("Robert", "", "Vanderlan", cwid="rov2001")
    ith_rival = ithaca("Rachel", "Vanderlan", "rbv44")
    scoped = IdentityIndex([wcm_unique, ith_rival])                 # default: WCM
    scoped_cands, scoped_cohort = scoped.candidates("Vanderlan", "Robert", "R")
    # The counterfactual, and the reason this test has teeth: the SAME two people in
    # one flat pool, which is exactly what today's unscoped index becomes the morning
    # after the load.
    flat = IdentityIndex([wcm_unique, dict(ith_rival, campus=CAMPUS_WCM)])
    flat_cands, flat_cohort = flat.candidates("Vanderlan", "Robert", "R")
    checks += [
        ("R1: a WCM person unique on surname+initial STAYS unique once Ithaca "
         "homonyms exist -- cohort 1, rarity term 0.40, top pick unchanged",
         scoped_cohort == 1 and len(scoped_cands) == 1
         and scoped_cands[0]["cwid"] == "rov2001"
         and scoped_cands[0]["confidence"] == 0.90),
        ("...and the counterfactual proves the damage is real: unscoped, the same "
         "Ithaca rival doubles the cohort and halves the rarity term (0.90 -> 0.70)",
         flat_cohort == 2 and flat_cands[0]["confidence"] == 0.70),
        ("the excluded records are counted, not silently dropped",
         scoped.n_other_campus == 1 and flat.n_other_campus == 0),
    ]

    # (b) a Cornell authorship still finds its Cornell candidate -- the scope is a
    # scope, not a Cornell blocklist. Same two records, Ithaca-scoped.
    ith_idx = IdentityIndex([wcm_unique, ith_rival], campus=CAMPUS_ITHACA)
    ith_cands, ith_cohort = ith_idx.candidates("Vanderlan", "Rachel", "R")
    checks += [
        ("R1: an Ithaca-scoped index still resolves an Ithaca byline to its Ithaca "
         "person", ith_cohort == 1 and len(ith_cands) == 1
         and ith_cands[0]["cwid"] == "rbv44"),
        ("...and the WCM person is not in that pool either -- the exclusion runs "
         "BOTH ways, so neither lane contaminates the other",
         ith_idx.n_other_campus == 1
         and "rov2001" not in {c["cwid"] for c in ith_cands}),
        # A surname held ONLY on the WCM roster is invisible to the Ithaca scope --
        # the byline resolves to nobody rather than to whoever happens to be nearest.
        # (The pair above deliberately cannot show this: 'Robert' and 'Rachel' share
        # the initial 'R', so the Ithaca Vanderlan is a legitimate Ithaca-scope
        # candidate for that byline. Sharing an initial inside one campus is ordinary
        # homonym ranking; the cross-campus leak is what this scope is about.)
        ("a surname held only on the WCM roster resolves to nobody in the Ithaca "
         "scope, instead of reaching across campuses for a rival",
         IdentityIndex([rec("Stefan", "", "Worgall", cwid="stw2004"), ith_rival],
                       campus=CAMPUS_ITHACA).candidates("Worgall", "Stefan", "S")
         == ([], 0)),
    ]

    # Scoping must not disturb WCM ranking among WCM people: the same WCM cohort,
    # scored with and without Ithaca records present, is identical field for field.
    wcm_pair = [rec("John", "", "Smith", cwid="wcm1"),
                rec("James", "", "Smith", cwid="wcm2")]
    clean, clean_cohort = IdentityIndex(wcm_pair).candidates("Smith", None, "J")
    noisy, noisy_cohort = IdentityIndex(
        wcm_pair + [ithaca("Jane", "Smith", f"ith{i}") for i in range(6)]
    ).candidates("Smith", None, "J")
    checks.append(
        ("R1: adding six Ithaca Smiths changes nothing about the WCM Smiths -- same "
         "order, same cohort_size, same confidence, same everything",
         clean == noisy and clean_cohort == noisy_cohort == 2))

    # The default scope is WCM, and a record with NO campus key is WCM -- which is
    # what makes this a no-op for every existing caller and for every hand-built
    # record in this file and in aar_matcher / aar_sweep_stale's self-tests.
    # --- PubMed LastName/ForeName inverted (pmid 42523178) -----------------------
    annas = IdentityIndex([rec("Anna", "Jadwiga", "Podolanczuk", cwid="ajp9012"),
                           rec("Zed", "", "Anna", cwid="za1")])
    swapped, swapped_n = annas.candidates("Anna J", "Podolanczuk", "P")
    checks += [
        ("swapped byline LastName='Anna J' ForeName='Podolanczuk' reaches ajp9012 as full",
         [(c["cwid"], c["given_match"]) for c in swapped] == [("ajp9012", "full")]
         and swapped_n == 1),
        ("the swap never admits an initial-tier coincidence: 'Will Moss' is nobody",
         annas.candidates("Moss", "Will", "W") == ([], 0)),
        ("a non-empty pool never swaps: LastName='Anna' finds the Anna pool (Zed Anna, "
         "excluded on initial) and stops, so ajp9012 is not reached that way",
         annas.candidates("Anna", "Podolanczuk", "P") == ([], 0)),
    ]

    checks += [
        ("default scope is WCM", IdentityIndex([]).campus == CAMPUS_WCM),
        ("a record with no `campus` key is WCM, so no existing caller changes",
         "campus" not in rec("Ann", "", "Roe")
         and IdentityIndex([rec("Ann", "", "Roe")]).n_other_campus == 0),
    ]

    # --- campus marker: which cwids `load()` treats as Cornell -------------------
    # `_campus_person_types` against a stub connection, so the two narrownesses that
    # guard measured false positives are held by the test and not only by prose.
    class _StubConn:
        def __init__(self, rows): self.rows = rows
        def execute(self, *_a, **_kw): return self.rows

    marker = IdentityIndex._campus_person_types(_StubConn([
        ("ith1", "cornell-ithaca"), ("ith1", "cornell-faculty"),
        ("ith2", "cornell-ithaca"),
        ("nomarker", "cornell-faculty"),        # cornell- prefixed, no campus marker
    ]))
    checks += [
        ("a cwid carrying the `cornell-ithaca` marker is Cornell, with all of its "
         "cornell- types",
         marker.get("ith1") == frozenset({"cornell-ithaca", "cornell-faculty"})
         and marker.get("ith2") == frozenset({"cornell-ithaca"})),
        ("a cwid with a cornell- type but NO campus marker is NOT deported off the "
         "WCM roster", "nomarker" not in marker),
        ("the SQL prefix is `cornell-%`, never `%cornell%`: `affiliate-cornell` is a "
         "live WCM person type on 483 people and must not match",
         "cornell-%" in IdentityIndex._campus_person_types.__doc__
         and not "affiliate-cornell".startswith("cornell-")),
    ]

    # --- R6: Cornell person types and the historical flag ------------------------
    # Without CORNELL_PERSON_TYPES every Cornell candidate renders "Other / CTSC"
    # (the WCM fallback) and historical=False, so Cornell emeriti and alumni miss the
    # -0.10 their WCM equivalents get.
    checks += [
        ("R6: a Cornell faculty member is not 'Other / CTSC'",
         cornell_person_type(["cornell-ithaca", "cornell-faculty"])
         == ("Cornell Faculty", False)),
        ("R6: the campus marker alone still yields a campus label, not the WCM "
         "fallback", cornell_person_type(["cornell-ithaca"])
         == ("Cornell Ithaca", False)),
        ("R6: an emeritus gets the historical flag",
         cornell_person_type(["cornell-ithaca", "cornell-emeritus"])
         == ("Cornell Emeritus", True)),
        ("R6: so do alumni, retirees, retired faculty, former postdocs and the "
         "inactive -- the whole terminal set",
         all(cornell_person_type(["cornell-ithaca", t])[1] for t in
             ("cornell-alumni", "cornell-retiree", "cornell-retired-faculty",
              "cornell-former-postdoc", "cornell-inactive"))),
        ("R6: a current role does NOT clear the historical flag -- Cornell's "
         "is_faculty is a current-role flag an emeritus commonly still carries, so "
         "coupling the flag to the winning label would lose exactly the people R6 "
         "names", cornell_person_type(
             ["cornell-ithaca", "cornell-faculty", "cornell-emeritus"])
         == ("Cornell Faculty", True)),
        ("R6: a Cornell student is a student, not 'Other / CTSC'",
         cornell_person_type(["cornell-ithaca", "cornell-student-grad"])
         == ("Cornell Student (Graduate)", False)),
        ("R6: no Cornell label can ever collide with a WCM one",
         not ({lbl for _, lbl, _ in CORNELL_PERSON_TYPES} | {"Cornell Ithaca"})
         & ({lbl for _, lbl, _ in PERSON_TYPES} | {"Other / CTSC"})),
    ]

    # ...and the flag actually reaches the curator-facing confidence, which is the
    # whole point of R6's second half: -0.10, the same term a WCM alumnus gets.
    emerita = ithaca("Ruth", "Ostrom", "rjo7", ("cornell-ithaca", "cornell-emeritus"))
    active = ithaca("Rhea", "Ostrom", "rho8", ("cornell-ithaca", "cornell-faculty"))
    r6_cands, _ = IdentityIndex([emerita, active],
                                campus=CAMPUS_ITHACA).candidates("Ostrom", None, "R")
    r6 = {c["cwid"]: c for c in r6_cands}
    checks += [
        ("R6: the historical penalty reaches confidence for a Cornell emerita, "
         "exactly as it does for a WCM alumnus (-0.10)",
         r6["rjo7"]["confidence"] == round(r6["rho8"]["confidence"] - 0.10, 3)),
        ("R6: and the label the curator reads is the Cornell one",
         r6["rjo7"]["person_type"] == "Cornell Emeritus"
         and r6["rho8"]["person_type"] == "Cornell Faculty"),
    ]

    # The WCM branch of `_record` is untouched: same columns in, same person_type,
    # historical and campus out, for a row carrying no Cornell types.
    _wcm_row = dict({c: None for c in _PTYPE_COLS}, cwid="abc1234", givenName="Ann",
                    middleName="", surname="Roe", primaryAcademicDepartment=None,
                    primaryAcademicDivision=None, primaryTitle=None,
                    primaryProgram=None, endDateWCMFaculty=None,
                    endDateWCMStudent=None, prefFirstName=None, prefMiddleName=None,
                    prefLastName=None)
    plain = IdentityIndex._record(dict(_wcm_row, alumniMD="yes"))
    faculty = IdentityIndex._record(dict(_wcm_row, fullTimeFaculty="yes"))
    checks += [
        # `_record` builds pref_full from the three person columns, stripped, and
        # leaves it empty when the person row is missing (LEFT JOIN -> all None) or
        # whitespace-only (5 such firstName rows on prod, where SQL <> '' misses them).
        ("_record: pref_full is person first + middle + last, stripped",
         IdentityIndex._record(dict(_wcm_row, prefFirstName="Tony", prefMiddleName=None,
                                    prefLastName=" Rosen "))["pref_full"] == "Tony Rosen"),
        ("_record: no person row -> pref_full empty; whitespace-only -> empty",
         plain["pref_full"] == ""
         and IdentityIndex._record(dict(_wcm_row, prefFirstName="   "))["pref_full"] == ""),
        ("_record's WCM branch is unchanged: the PERSON_TYPES loop still wins and "
         "still sets historical",
         (plain["person_type"], plain["historical"]) == ("Alumni MD", True)
         and (faculty["person_type"], faculty["historical"])
         == ("Full-Time Faculty", False)),
        ("...and every row _record builds with no Cornell types is WCM, which is "
         "every row it builds today (the marker query returns 0 rows)",
         plain["campus"] == faculty["campus"] == CAMPUS_WCM),
        ("_record with Cornell types yields an Ithaca record, so `load()` needs no "
         "second code path",
         IdentityIndex._record(_wcm_row, ("cornell-ithaca", "cornell-postdoc"))
         ["campus"] == CAMPUS_ITHACA
         and IdentityIndex._record(_wcm_row,
                                   ("cornell-ithaca", "cornell-postdoc"))
         ["person_type"] == "Cornell Postdoc"),
    ]

    # --- DynamoDB Identity.alternateNames (issue #227) ---------------------------
    # The anchor: pmid 42644724, byline "Erika Hissong", affiliation Weill Cornell
    # Medicine, proposed NOBODY -- identity has emh9016 as Erika Patel, person mirrors
    # "Erika", and no index key ever said "hissong", although her Identity item lists
    # exactly that alternate name.
    alts = {"emh9016": [{"firstName": "Erika", "middleName": "", "lastName": "Hissong"},
                        {"firstName": "Erika", "middleName": "M", "lastName": "Patel"}]}
    patel = dict(rec("Erika", "", "Patel", cwid="emh9016", pref_first="Erika"),
                 dept="Pathology and Laboratory Medicine")
    erikas = IdentityIndex([patel], alternate_names=alts)
    hiss, hiss_cohort = erikas.candidates("Hissong", "Erika", "E")
    pat, pat_cohort = erikas.candidates("Patel", "Erika", "E")
    checks += [
        ("(a) byline 'Erika Hissong' reaches emh9016 through her alternate name: exactly "
         "one candidate, given_match full (pmid 42644724)",
         len(hiss) == 1 and hiss_cohort == 1 and hiss[0]["cwid"] == "emh9016"
         and hiss[0]["given_match"] == "full"),
        ("(a) ...and the label the curator reads is the alternate name, alone, badged "
         "as an alternate with no count (b: alternate-only -> 'alternate', name_n None)",
         hiss[0]["name"] == "Erika Hissong" and hiss[0]["name_source"] == "alternate"
         and hiss[0]["name_n"] is None),
        ("(a) the copy IS the person: dept and person type carry over",
         hiss[0]["dept"] == "Pathology and Laboratory Medicine"
         and hiss[0]["person_type"] == "Full-Time Faculty"),
        ("(b) byline 'Erika Patel' still yields one candidate, cohort 1 -- the "
         "'Erika M Patel' alternate is the primary name and is NOT indexed again",
         len(pat) == 1 and pat_cohort == 1 and pat[0]["cwid"] == "emh9016"
         and pat[0]["name"] == "Erika Patel" and erikas.n_alt_names == 1),
        ("an alternate with no lastName is skipped, and None/empty lists are inert",
         IdentityIndex([patel], alternate_names={"emh9016": [
             {"firstName": "E", "lastName": ""}, {"firstName": "E"}]}).n_alt_names == 0
         and IdentityIndex([patel], alternate_names={"emh9016": None}).n_alt_names == 0),
        ("no alternate_names argument -> the index is byte-identical to before (every "
         "existing caller and hand-built selftest record is unchanged)",
         IdentityIndex([patel]).by_surname == {"patel": [patel]}
         and IdentityIndex([patel]).n_alt_names == 0),
    ]

    # (c) An alternate sharing the SURNAME with a different first name: one person,
    # two pool entries under "wong". Either byline must reach him at full with a
    # cohort of ONE -- never "2 WCM homonyms" for one cwid.
    wongs = IdentityIndex([rec("Ho-Yee", "", "Wong", cwid="hyw2001")], alternate_names={
        "hyw2001": [{"firstName": "Tommy", "middleName": "", "lastName": "Wong"}]})
    tommy, tommy_cohort = wongs.candidates("Wong", "Tommy", "T")
    hoyee, hoyee_cohort = wongs.candidates("Wong", "Ho-Yee", "H")
    checks += [
        ("(c) byline 'Tommy Wong' -> one candidate, cohort 1, full, labelled with the "
         "alternate name alone",
         len(tommy) == 1 and tommy_cohort == 1 and tommy[0]["cwid"] == "hyw2001"
         and tommy[0]["given_match"] == "full" and tommy[0]["name"] == "Tommy Wong"
         and tommy[0]["name_source"] == "alternate"),
        ("(c) byline 'Ho-Yee Wong' -> one candidate, cohort 1, full, the directory name",
         len(hoyee) == 1 and hoyee_cohort == 1 and hoyee[0]["cwid"] == "hyw2001"
         and hoyee[0]["given_match"] == "full" and hoyee[0]["name"] == "Ho-Yee Wong"
         and hoyee[0]["name_source"] == "directory"),
        ("(c) the pool really does hold him twice -- the dedupe is in candidates(), "
         "not in the index", len(wongs.by_surname["wong"]) == 2),
    ]

    # The same person landing twice at the SAME tier (person mirror already says
    # "Tommy", and the alternate says it again): still one candidate, the primary
    # record wins the tie -- so the label is the PRIMARY name and the badge says
    # "primary", not "alternate" -- and the rarity term is the cohort-of-one 0.40.
    both = IdentityIndex([rec("Ho-Yee", "", "Wong", cwid="hyw2001", pref_first="Tommy",
                              pref_full="Tommy Wong")],
                         alternate_names={"hyw2001": [
                             {"firstName": "Tommy", "lastName": "Wong"}]})
    twice, twice_cohort = both.candidates("Wong", "Tommy", "T")
    checks += [
        ("a cwid in one pool twice at the same tier is one candidate with confidence "
         "0.90 (0.50 full + 0.40/1), the primary's label and source on the tie",
         len(twice) == 1 and twice_cohort == 1 and twice[0]["confidence"] == 0.90
         and twice[0]["name"] == "Tommy Wong" and twice[0]["name_source"] == "primary"),
        ("an initial-only byline does NOT reach a person through an alternate: 'T Wong' "
         "matches neither Ho-Yee (primary, H) nor alternate Tommy (full tier only)",
         wongs.candidates("Wong", None, "T") == ([], 0)),
        # A genuinely different person under the alternate surname still counts.
        ("the dedupe is per cwid, not per surname: a real second Erika Hissong still makes "
         "a cohort of 2 when the byline spells the name out",
         IdentityIndex([patel, rec("Erika", "", "Hissong", cwid="emh0001")],
                       alternate_names=alts).candidates("Hissong", "Erika", "E")[1] == 2),
        # (d) the second source: names carried by ACCEPTED articles, first token = given,
        # carrying its provenance and the accepted-paper count.
        ("accepted bylines: 'Erika M' / 'Hissong' becomes given Erika, middle M, last "
         "Hissong, source accepted, n 40",
         accepted_byline_names([{"cwid": "emh9016", "fore": "Erika M", "last": "Hissong", "n": 40}])
         == {"emh9016": [{"firstName": "Erika", "middleName": "M", "lastName": "Hissong",
                          "source": "accepted", "n": 40}]}),
        ("accepted bylines: an empty forename is skipped, others kept",
         accepted_byline_names([{"cwid": "x", "fore": "", "last": "Q", "n": 2},
                                {"cwid": "x", "fore": "A", "last": "Q", "n": 2}])
         == {"x": [{"firstName": "A", "middleName": "", "lastName": "Q",
                    "source": "accepted", "n": 2}]}),
        ("accepted bylines reach the index by the same path: byline 'Erika Hissong' -> "
         "emh9016 full, badged accepted with the count",
         (lambda cs: len(cs) == 1 and cs[0]["cwid"] == "emh9016"
          and cs[0]["given_match"] == "full" and cs[0]["name"] == "Erika Hissong"
          and cs[0]["name_source"] == "accepted" and cs[0]["name_n"] == 40)(
             IdentityIndex([patel], alternate_names=accepted_byline_names(
                 [{"cwid": "emh9016", "fore": "Erika", "last": "Hissong", "n": 40}]))
             .candidates("Hissong", "Erika", "E")[0])),
        # (a) the same name from BOTH sources: `load()` lists the accepted entry first,
        # so the (given, surname) dedupe in _alt_records keeps the one with evidence.
        ("(a) Hissong through both sources -> 'Erika Hissong', name_source accepted, "
         "name_n 40 (the accepted entry is listed first and wins the dedupe)",
         (lambda cs: len(cs) == 1 and cs[0]["name"] == "Erika Hissong"
          and cs[0]["name_source"] == "accepted" and cs[0]["name_n"] == 40)(
             IdentityIndex([patel], alternate_names={"emh9016": accepted_byline_names(
                 [{"cwid": "emh9016", "fore": "Erika", "last": "Hissong", "n": 40}])
                 ["emh9016"] + alts["emh9016"]}).candidates("Hissong", "Erika", "E")[0])),
        ("(a) ...and the ORDER is what decides it: DynamoDB first would badge it "
         "'alternate' and lose the count, which is why load() puts accepted first",
         (lambda cs: cs[0]["name_source"] == "alternate" and cs[0]["name_n"] is None)(
             IdentityIndex([patel], alternate_names={"emh9016": alts["emh9016"]
                 + accepted_byline_names([{"cwid": "emh9016", "fore": "Erika",
                                           "last": "Hissong", "n": 40}])["emh9016"]})
             .candidates("Hissong", "Erika", "E")[0])),
        # (f) the label is the name AS RECORDED: PubMed's forename "Erika M" keeps its
        # middle initial in the label even though the matcher looks past it (#230).
        ("(f) an accepted byline recorded as 'Erika M' prints 'Erika M Hissong'",
         (lambda cs: len(cs) == 1 and cs[0]["name"] == "Erika M Hissong"
          and cs[0]["name_source"] == "accepted")(
             IdentityIndex([patel], alternate_names=accepted_byline_names(
                 [{"cwid": "emh9016", "fore": "Erika M", "last": "Hissong", "n": 40}]))
             .candidates("Hissong", "Erika", "E")[0])),
        ("the accepted-byline threshold is enforced in SQL, at two",
         "HAVING n >= :min" in _ACCEPTED_BYLINES_SQL and ACCEPTED_BYLINE_MIN == 2),
        # (e) 2026-09-12 dry run: alternates count at `full` only, and initials are not names.
        ("alternate at the initial tier is NOT a candidate: byline 'Todd Rosen' does not reach "
         "Tony Rosen (directory Anthony) through alternate 'Tony'",
         IdentityIndex([rec("Anthony", "Ehren", "Rosen", cwid="aer2006", pref_first="Tony")],
                       alternate_names={"aer2006": [{"firstName": "Tony", "lastName": "Rosen"}]})
         .candidates("Rosen", "Todd", "T") == ([], 0)),
        ("...while the primary record still earns its own initial: byline 'A Rosen' -> aer2006 initial",
         (lambda cs: len(cs) == 1 and cs[0]["given_match"] == "initial")(
             IdentityIndex([rec("Anthony", "Ehren", "Rosen", cwid="aer2006", pref_first="Tony")],
                           alternate_names={"aer2006": [{"firstName": "Tony", "lastName": "Rosen"}]})
             .candidates("Rosen", "A", "A")[0])),
        ("an initials-only accepted byline mints NO alternate: 'J L' / 'Stewart' is skipped",
         IdentityIndex([rec("Jessica", "Lauren", "Stewart", cwid="jls4002")],
                       alternate_names={"jls4002": [{"firstName": "J", "middleName": "L", "lastName": "Stewart"}]})
         .n_alt_names == 0),
        ("a one-letter middle on an alternate is ignored by the matcher, so byline 'Y Lin' cannot match 'R Y Lin' as full",
         IdentityIndex([rec("Robert", "Yao-Wen", "Lin", cwid="rol3002")],
                       alternate_names={"rol3002": [{"firstName": "Rob", "middleName": "Y", "lastName": "Lin"}]})
         .candidates("Lin", "Y", "Y") == ([], 0)),
    ]

    # --- byline campus: "If WCM affiliation, show WCM people. If CU affiliation, show CU
    # options. If both, show both." (2026-09-27) ---------------------------------------
    W, C = {CAMPUS_WCM}, {CAMPUS_ITHACA}
    checks += [
        ("classifier: a WCM clause is WCM",
         byline_campuses(["Department of Medicine, Weill Cornell Medicine, New York, NY"]) == W),
        ("classifier: an Ithaca clause is Cornell",
         byline_campuses(["Department of Chemistry, Cornell University, Ithaca, NY 14853"]) == C),
        ("classifier: 'Weill Institute ... Ithaca' is Cornell (the Weill Institute is upstate)",
         byline_campuses(["Weill Institute for Cell and Molecular Biology, Cornell University, "
                          "Ithaca, NY, USA"]) == C),
        ("classifier: 'Weill Cornell Medicine, Cornell University, Ithaca' is WCM (a WCM "
         "marker wins inside one clause)",
         byline_campuses(["Weill Cornell Medicine, Cornell University, Ithaca, NY"]) == W),
        ("classifier: a merged blob is read PER CLAUSE -- a Cornell co-author's Ithaca clause "
         "beside the author's WCM clause gives both, never Ithaca alone",
         byline_campuses(["Department of Pediatrics, Weill Cornell Medicine, New York, NY; "
                          "College of Veterinary Medicine, Cornell University, Ithaca, NY"])
         == W | C),
        ("classifier: 'Cornell Road' is not Cornell University -> the WCM default",
         byline_campuses(["Children's Hospital, 3333 Cornell Road, Cincinnati, OH"]) == W),
        ("classifier: Cornell College, Mount Vernon is not Cornell University -> WCM default",
         byline_campuses(["Department of Biology, Cornell College, Mount Vernon, IA"]) == W),
        ("classifier: two affiliation strings, one per campus, give both",
         byline_campuses(["Weill Cornell Medicine, New York, NY",
                          "Cornell Tech, Roosevelt Island, New York, NY"]) == W | C),
        ("classifier: bare 'Cornell University, New York' is ambiguous -> WCM, today's reading",
         byline_campuses(["Cornell University, New York, NY, USA"]) == W),
        ("classifier: an email never makes a clause Cornell",
         byline_campuses(["Rockefeller University, New York, NY. jdoe@cornell.edu"]) == W),
        ("classifier: AgriTech/Geneva and the vet college are Cornell",
         byline_campuses(["Cornell AgriTech, Geneva, NY 14456"]) == C
         and byline_campuses(["Baker Institute for Animal Health, Cornell University"]) == C),
        ("classifier: no affiliation at all is the WCM default",
         byline_campuses([]) == W and byline_campuses(None) == W),
    ]

    # Assembly, over hand-built rosters for BOTH campuses. "Smith, J" has a WCM holder and
    # a Cornell holder; "Ostrom" exists only at Cornell.
    w_smith = rec("John", "", "Smith", cwid="jos2001")
    c_smith = ithaca("John", "Smith", "js123")
    c_ostrom = ithaca("Elinor", "Ostrom", "eo1")
    loads = []

    def _load_ith():
        loads.append(1)
        return IdentityIndex([w_smith, c_smith, c_ostrom], campus=CAMPUS_ITHACA)

    roster = CampusRoster(IdentityIndex([w_smith, c_smith, c_ostrom]), load_ithaca=_load_ith)
    wcm_aff = ["Weill Cornell Medicine, New York, NY"]
    ith_aff = ["Cornell University, Ithaca, NY"]
    plain = IdentityIndex([w_smith]).candidates("Smith", "John", "J", wcm_aff)
    w_only = roster.candidates("Smith", "John", "J", wcm_aff)
    checks += [
        ("assembly: a WCM-only byline returns exactly the WCM index's answer (byte-for-byte "
         "unchanged row) and never loads the Ithaca roster",
         w_only == plain and not loads and "campus" not in w_only[0][0]),
    ]
    # Dual appointee: on the WCM roster with the bridge's cornell-* flag, off the Ithaca one.
    spector = dict(rec("Jason", "", "Spector", cwid="jas2037"), cornell_linked=True)
    spector_idx = IdentityIndex([spector, rec("Jason", "", "Spector", cwid="jxs9")])
    spector_aff = ["Nancy E. and Peter C. Meinig School of Biomedical Engineering, Cornell "
                   "University, Weill Hall, Ithaca, NY 14853, USA"]
    spector_c, spector_n = CampusRoster(spector_idx).candidates(
        "Spector", "Jason", "J", spector_aff)
    spector_both, spector_both_n = CampusRoster(
        spector_idx, ithaca=IdentityIndex([ithaca("Jason", "Spector", "js_ith")],
                                          campus=CAMPUS_ITHACA)).candidates(
        "Spector", "Jason", "J", spector_aff)
    spector_w = CampusRoster(spector_idx).candidates("Spector", "Jason", "J", wcm_aff)
    c_only, c_n = roster.candidates("Smith", "John", "J", ith_aff)
    both, both_n = roster.candidates("Smith", "John", "J", wcm_aff + ith_aff)
    ostrom_w = roster.candidates("Ostrom", "Elinor", "E", wcm_aff)
    ostrom_c = roster.candidates("Ostrom", "Elinor", "E", ith_aff)
    checks += [
        ("assembly: a Cornell-only byline proposes the Cornell person as top, tagged with "
         "its campus",
         [c["cwid"] for c in c_only] == ["js123"] and c_only[0]["campus"] == CAMPUS_ITHACA
         and c_only[0]["person_type"] == "Cornell Faculty" and c_n == 1),
        ("assembly: both campuses -> both people, WCM first on an exact tie, cohort_size is "
         "the combined 2 on every entry (so the card is not a single-candidate card)",
         [c["cwid"] for c in both] == ["jos2001", "js123"] and both_n == 2
         and all(c["cohort_size"] == 2 for c in both)
         and "campus" not in both[0] and both[1]["campus"] == CAMPUS_ITHACA),
        ("assembly: ...while each keeps its own campus's confidence (R1: rarity is per campus)",
         both[0]["confidence"] == both[1]["confidence"] == 0.90),
        ("assembly: a Cornell-only person is unreachable from a WCM byline, reachable from a "
         "Cornell one",
         ostrom_w == ([], 0) and [c["cwid"] for c in ostrom_c[0]] == ["eo1"]),
        ("assembly: the Ithaca roster loaded ONCE for the whole run, not per row",
         len(loads) == 1 and roster.n_ithaca_bylines == 3),
        ("assembly: a merge keeps the caller's top_k",
         len(roster.candidates("Smith", "John", "J", wcm_aff + ith_aff, top_k=1)[0]) == 1),
        ("assembly: a stronger Cornell candidate outranks a weaker WCM one on a two-campus "
         "byline (the merge ranks, it does not append)",
         [c["cwid"] for c in CampusRoster(
             IdentityIndex([rec("Anne", "", "Doe", cwid="ad_w")]),
             ithaca=IdentityIndex([ithaca("Alan", "Doe", "ad_c")], campus=CAMPUS_ITHACA))
          .candidates("Doe", "Alan", "A", wcm_aff + ith_aff)[0]] == ["ad_c", "ad_w"]),
        ("assembly: no Ithaca loader -> a Cornell byline proposes nobody, never WCM homonyms",
         CampusRoster(IdentityIndex([w_smith])).candidates("Smith", "John", "J", ith_aff)
         == ([], 0)),
        ("assembly: a bridged dual appointee (WCM record, cornell-* types, NOT on the Ithaca "
         "roster) is still offered on a Cornell-only 'Weill Hall, Ithaca' byline -- only he, "
         "not his unlinked WCM homonym; an Ithaca-only person still ranks by tier",
         [c["cwid"] for c in spector_c] == ["jas2037"] and spector_n == 1
         and "campus" not in spector_c[0]
         and byline_campuses(spector_aff) == C),
        ("assembly: ...and beside a Cornell homonym both are offered, cohort is the combined "
         "count, the Cornell person keeps an exact tie on a Cornell byline",
         [c["cwid"] for c in spector_both] == ["js_ith", "jas2037"] and spector_both_n == 2
         and all(c["cohort_size"] == 2 for c in spector_both)),
        ("assembly: ...found even when unlinked WCM homonyms outrank him past top_k",
         [c["cwid"] for c in CampusRoster(
             IdentityIndex([rec("Jason", "", "Spector", cwid=f"u{i}") for i in range(6)]
                           + [dict(rec("Jason", "", "Spector", cwid="jas2037"),
                                   cornell_linked=True)]))
          .candidates("Spector", "Jason", "J", spector_aff, top_k=1)[0]] == ["jas2037"]),
        ("assembly: a WCM-only byline is unchanged by the linked flag (no key published)",
         spector_w == IdentityIndex([rec("Jason", "", "Spector", cwid="jas2037"),
                                     rec("Jason", "", "Spector", cwid="jxs9")]).candidates(
             "Spector", "Jason", "J", wcm_aff)),
        ("assembly: an explicit `campuses` overrides the text",
         [c["cwid"] for c in roster.candidates("Smith", "John", "J", wcm_aff,
                                                campuses={CAMPUS_ITHACA})[0]] == ["js123"]),
    ]

    # --- venue fit (v3.14) -----------------------------------------------------------
    # Physics has 20 accepted rows, all in Optics; the Law School 100, all in Law. So
    # (venue_fit's own selftest works these by hand):
    #   fit(Physics, Optics)    =  1.586  -> +0.15     fit(Physics, Law)       = -1.609 -> -0.20
    #   fit(Law School, Law)    =  0.189  ->  0        fit(Law School, Optics) = -3.045 -> -0.20
    from venue_fit import DeptVenueFit
    table = DeptVenueFit([("Physics", "Optics", 20), ("Law School", "Law", 100)])
    phys = dict(ithaca("Robert", "Weiss", "rw_c"), dept="Physics")
    law = dict(ithaca("Rhea", "Weiss", "rh_c", ("cornell-ithaca", "cornell-emeritus")),
               dept="Law School")
    hotel = dict(ithaca("Rex", "Weiss", "rx_c"), dept="Hotel Administration")
    phys_aff = ["Department of Physics, Cornell University, Ithaca, NY"]
    no_table = IdentityIndex([phys, law], campus=CAMPUS_ITHACA)
    with_table = IdentityIndex([phys, law], campus=CAMPUS_ITHACA, dept_venue_fit=table)
    call = ("Weiss", "Robert", "R", phys_aff, 5, 2024)
    before = no_table.candidates(*call)
    # What origin/master returned for this call before the venue term existed, captured
    # by running that code. The other checks compare call against call; this one pins
    # the bytes, because candidate_cwids_json is stored and diffed by the replay tools.
    golden = (
        '[[{"cwid": "rw_c", "name": "Robert Weiss", "name_source": "directory", '
        '"name_n": null, "dept": "Physics", "division": "", "person_type": "Cornell Faculty", '
        '"title": "", "given_match": "full", "affil_dept_match": true, "affil_match_on": '
        '"dept", "cohort_size": 2, "years_after_wcm": null, "confidence": 0.95, "campus": '
        '"cornell-ithaca"}, {"cwid": "rh_c", "name": "Rhea Weiss", "name_source": '
        '"directory", "name_n": null, "dept": "Law School", "division": "", "person_type": '
        '"Cornell Emeritus", "title": "", "given_match": "initial", "affil_dept_match": '
        'false, "affil_match_on": null, "cohort_size": 2, "years_after_wcm": null, '
        '"confidence": 0.35, "campus": "cornell-ithaca"}], 2]')
    import json as _json
    checks += [
        # (1) the feature off, in each of the ways it can be off, is today's output.
        ("venue (1): no table and no subfield is byte-for-byte the pre-change output",
         _json.dumps(before) == golden),
        ("venue (1): ...and so is no table WITH a subfield, and a table with NO subfield "
         "(None or empty)",
         _json.dumps(no_table.candidates(*call, "Optics")) == golden
         and _json.dumps(with_table.candidates(*call)) == golden
         and _json.dumps(with_table.candidates(*call, venue_subfield=None)) == golden
         and _json.dumps(with_table.candidates(*call, venue_subfield="")) == golden),
        ("venue (1): the existing fixtures are unmoved by a subfield nobody can score -- "
         "the temporal Smiths, the 7-person Weiss cohort, the Maliks",
         smiths.candidates("Smith", None, "J", pub_year=2024, venue_subfield="Optics")
         == smiths.candidates("Smith", None, "J", pub_year=2024)
         and crowd.candidates("Weiss", "Robert", "R",
                              ["Department of Pathology, Weill Cornell Medicine"],
                              pub_year=2024, top_k=5, venue_subfield="Optics")
         == (crowded, crowd_size)
         and maliks.candidates("Malik", "Rayaz A", "RA", malik_affil,
                               venue_subfield="Optics") == (mal, 2)),
        ("venue (1): ...nor is a two-campus CampusRoster merge with no table on either side",
         roster.candidates("Smith", "John", "J", wcm_aff + ith_aff, venue_subfield="Optics")
         == (both, both_n)),
        ("venue (1): no candidate carries a `venue_fit` key in any of those",
         not any("venue_fit" in c for c in before[0] + with_table.candidates(*call)[0]
                 + no_table.candidates(*call, "Optics")[0])),
    ]

    # (2) the amounts. No affiliation, so nothing is near the clip: Robert is `full` in a
    # cohort of 2 (0.70), Rhea `initial` and emerita (0.25 + 0.20 - 0.10 = 0.35).
    plain = {c["cwid"]: c for c in with_table.candidates("Weiss", "Robert", "R")[0]}
    optics = {c["cwid"]: c for c in with_table.candidates(
        "Weiss", "Robert", "R", venue_subfield="Optics")[0]}
    lawj = {c["cwid"]: c for c in with_table.candidates(
        "Weiss", "Robert", "R", venue_subfield="Law")[0]}
    unscored = IdentityIndex([hotel], campus=CAMPUS_ITHACA, dept_venue_fit=table)
    checks += [
        ("venue (2): a fitting venue raises confidence by 0.15 and records the fit "
         "(Physics in an Optics journal: 0.70 -> 0.85, venue_fit 1.586)",
         plain["rw_c"]["confidence"] == 0.70 and optics["rw_c"]["confidence"] == 0.85
         and optics["rw_c"]["venue_fit"] == 1.586),
        ("venue (2): a non-fitting venue lowers it by 0.20 (Physics in a Law journal: "
         "0.70 -> 0.50, venue_fit -1.609; the Law School in Optics: 0.35 -> 0.15)",
         lawj["rw_c"]["confidence"] == 0.50 and lawj["rw_c"]["venue_fit"] == -1.609
         and optics["rh_c"]["confidence"] == 0.15 and optics["rh_c"]["venue_fit"] == -3.045),
        ("venue (2): a fit between the thresholds is recorded and moves nothing (the Law "
         "School in a Law journal, 0.189)",
         lawj["rh_c"]["venue_fit"] == 0.189
         and lawj["rh_c"]["confidence"] == plain["rh_c"]["confidence"] == 0.35),
        ("venue (2): a department the table cannot score (under 20 rows, or unknown) "
         "gets no key and no term -- the candidate is exactly what it was",
         unscored.candidates("Weiss", "Rex", "R", venue_subfield="Optics")
         == unscored.candidates("Weiss", "Rex", "R")
         and "venue_fit" not in unscored.candidates("Weiss", "Rex", "R",
                                                    venue_subfield="Optics")[0][0]),
        ("venue (2): the term is added BEFORE the clip: 0.95 + 0.15 is 1.0, not 1.10, and "
         "a floor-bound candidate stays at 0.0",
         with_table.candidates(*call, "Optics")[0][0]["confidence"] == 1.0
         and IdentityIndex._confidence("full", 1, True, False, 0.0, -0.20) == 0.95
         and IdentityIndex._confidence("unknown", 266, False, True, 0.0, -0.20) == 0.0),
        ("venue (2): the default term is zero, so every existing _confidence call is unchanged",
         IdentityIndex._confidence("initial", 2, False, False)
         == IdentityIndex._confidence("initial", 2, False, False, 0.0, 0.0) == 0.45),
        ("venue (2): the term changes nothing but confidence and the new key",
         [{k: v for k, v in c.items() if k not in ("confidence", "venue_fit")}
          for c in optics.values()] == list(
              {k: v for k, v in c.items() if k != "confidence"} for c in plain.values())),
    ]

    # (3) ranking. A second table where the Law School's own venue is a strong fit:
    # fit(Law School, Law) = 1.586 -> +0.15, fit(Physics, Law) = ln(5/105) = -3.045 -> -0.20.
    table2 = DeptVenueFit([("Physics", "Optics", 100), ("Law School", "Law", 20)])
    law2 = dict(ithaca("Rhea", "Weiss", "rh_c"), dept="Law School")
    ranked_idx = IdentityIndex([phys, law2], campus=CAMPUS_ITHACA, dept_venue_fit=table2)
    # given-name tier: the byline spells out Robert; the venue says Rhea.
    tier = ranked_idx.candidates("Weiss", "Robert", "R", venue_subfield="Law")[0]
    # affiliation tier: initials only, the affiliation names Physics; the venue says Law.
    aff_tier = ranked_idx.candidates("Weiss", None, "R", phys_aff, venue_subfield="Law")[0]
    # one tier: initials only, no affiliation. Here, and only here, the venue decides.
    tied = ranked_idx.candidates("Weiss", None, "R")[0]
    tied_law = ranked_idx.candidates("Weiss", None, "R", venue_subfield="Law")[0]
    tied_optics = ranked_idx.candidates("Weiss", None, "R", venue_subfield="Optics")[0]
    checks += [
        ("venue (3): a full given-name match keeps the top pick against a rival the "
         "venue favours, even with the LOWER confidence (0.50 vs 0.60)",
         [(c["cwid"], c["given_match"], c["confidence"]) for c in tier]
         == [("rw_c", "full", 0.50), ("rh_c", "initial", 0.60)]),
        ("venue (3): an affiliation match keeps the top pick the same way (0.50 vs 0.60)",
         [(c["cwid"], c["affil_dept_match"], c["confidence"]) for c in aff_tier]
         == [("rw_c", True, 0.50), ("rh_c", False, 0.60)]),
        ("venue (3): within one tier the venue DOES decide: tied at 0.45, the Law journal "
         "puts the Law School first",
         [(c["cwid"], c["confidence"]) for c in tied] == [("rw_c", 0.45), ("rh_c", 0.45)]
         and [(c["cwid"], c["confidence"]) for c in tied_law]
         == [("rh_c", 0.60), ("rw_c", 0.25)]),
        ("venue (3): ...and an Optics journal keeps Physics first by penalising the rival",
         [(c["cwid"], c["confidence"]) for c in tied_optics]
         == [("rw_c", 0.45), ("rh_c", 0.25)]),
        ("venue (3): nobody is ever dropped, and _rank_key does not read the fit: it is "
         "the three fields it always read, behind the ORCID match (False here)",
         all(len(x) == 2 for x in (tier, aff_tier, tied_law, tied_optics))
         and _rank_key(dict(tier[0], venue_fit=99.0)) == _rank_key(tier[0])
         == (False, True, False, 0.50)),
    ]

    # (4) WCM. The scope decision is structural: a WCM index holds no table whatever it
    # is handed, so a subfield passed to it does nothing.
    w_phys = dict(rec("Robert", "", "Weiss", cwid="rw_w"), dept="Physics")
    wcm_handed_table = IdentityIndex([w_phys], dept_venue_fit=table)
    venue_roster = CampusRoster(IdentityIndex([w_phys], dept_venue_fit=table),
                                ithaca=with_table)
    two = venue_roster.candidates("Weiss", "Robert", "R", wcm_aff + ith_aff,
                                  venue_subfield="Optics")[0]
    two_plain = venue_roster.candidates("Weiss", "Robert", "R", wcm_aff + ith_aff)[0]
    linked_roster = CampusRoster(IdentityIndex([dict(w_phys, cornell_linked=True)]),
                                 ithaca=with_table)
    linked_c = linked_roster.candidates("Weiss", "Robert", "R", ith_aff,
                                        venue_subfield="Optics")[0]
    linked_plain = linked_roster.candidates("Weiss", "Robert", "R", ith_aff)[0]
    no_wcm_roster = CampusRoster(IdentityIndex([rec("Ann", "", "Other")]), ithaca=with_table)
    # One WCM and one Cornell Robert Weiss, alone in their cohorts, on a byline naming
    # both campuses: 0.50 + 0.40 each, an exact tie.
    tie_roster = CampusRoster(IdentityIndex([w_phys]),
                              ithaca=IdentityIndex([phys], campus=CAMPUS_ITHACA,
                                                   dept_venue_fit=table))
    tied_two = tie_roster.candidates("Weiss", "Robert", "R", wcm_aff + ith_aff,
                                     venue_subfield="Optics")[0]
    tied_two_plain = tie_roster.candidates("Weiss", "Robert", "R", wcm_aff + ith_aff)[0]
    # Physics has 20 Optics rows and 15 of them are rw_c's own: without them the
    # department is under 20 rows, so rw_c gets no fit; Rex, in the same department with
    # no rows of his own, gets the department's 1.586.
    own_table = DeptVenueFit([("Physics", "Optics", 15, "rw_c"), ("Physics", "Optics", 5, "zz"),
                              ("Law School", "Law", 100)])
    own_idx = IdentityIndex([phys, dict(ithaca("Rex", "Weiss", "rx_c"), dept="Physics")],
                            campus=CAMPUS_ITHACA, dept_venue_fit=own_table)
    checks += [
        ("venue (4): a WCM index never holds a fit table, even when handed one",
         wcm_handed_table.dept_venue_fit is None and IdentityIndex([]).dept_venue_fit is None
         and with_table.dept_venue_fit is table),
        ("venue (4): ...so it ignores venue_subfield entirely",
         wcm_handed_table.candidates("Weiss", "Robert", "R", venue_subfield="Optics")
         == wcm_handed_table.candidates("Weiss", "Robert", "R")
         == IdentityIndex([w_phys]).candidates("Weiss", "Robert", "R")),
        ("venue (4): a WCM-only byline through a CampusRoster is the bare WCM answer, "
         "subfield or not",
         venue_roster.candidates("Weiss", "Robert", "R", wcm_aff, venue_subfield="Optics")
         == IdentityIndex([w_phys]).candidates("Weiss", "Robert", "R", wcm_aff)),
        ("venue (4): a Cornell-only byline through a CampusRoster is the Ithaca index's "
         "own answer, term included -- the one place a roster passes the subfield on",
         venue_roster.candidates("Weiss", "Robert", "R", ith_aff, venue_subfield="Optics")
         == with_table.candidates("Weiss", "Robert", "R", ith_aff, venue_subfield="Optics")
         and [c.get("venue_fit") for c in venue_roster.candidates(
             "Weiss", "Robert", "R", ith_aff, venue_subfield="Optics")[0]] == [1.586, -3.045]),
        ("venue (4): a two-campus byline is ranked exactly as before the term existed: "
         "only the Cornell side could earn it, so nobody does",
         two == two_plain and not any("venue_fit" in c for c in two)
         and {c["cwid"] for c in two} == {"rw_w", "rw_c", "rh_c"}),
        ("venue (4): ...so a WCM person tied with a Cornell namesake keeps the row in a "
         "venue that fits the Cornell department (WCM first on an exact tie, as before)",
         [(c["cwid"], c["confidence"]) for c in tied_two] == [("rw_w", 0.9), ("rw_c", 0.9)]
         and tied_two == tied_two_plain),
        ("venue (4): a Cornell byline that keeps a linked WCM dual appointee is a mixed "
         "list too, and carries no term",
         linked_c == linked_plain and not any("venue_fit" in c for c in linked_c)
         and {c["cwid"] for c in linked_c} == {"rw_c", "rh_c", "rw_w"}),
        ("venue (4): a two-campus byline on which the WCM roster has no namesake is the "
         "Ithaca answer, cohort size included, and so it is scored",
         no_wcm_roster.candidates("Weiss", "Rhea", "R", wcm_aff + ith_aff,
                                  venue_subfield="Law")
         == with_table.candidates("Weiss", "Rhea", "R", wcm_aff + ith_aff,
                                  venue_subfield="Law")
         and no_wcm_roster.candidates("Weiss", "Rhea", "R", wcm_aff + ith_aff)
         == with_table.candidates("Weiss", "Rhea", "R", wcm_aff + ith_aff)
         and [c.get("venue_fit") for c in no_wcm_roster.candidates(
             "Weiss", "Rhea", "R", wcm_aff + ith_aff, venue_subfield="Law")[0]]
         == [0.189, -1.609]),
        ("venue: the fit is asked for with the candidate's cwid, and a person whose own "
         "accepted papers are what the department's count rests on gets no term from them",
         [(c["cwid"], c.get("venue_fit"), c["confidence"]) for c in own_idx.candidates(
             "Weiss", None, "R", venue_subfield="Optics")[0]]
         == [("rx_c", 1.586, 0.6), ("rw_c", None, 0.45)]),
        ("venue: the swapped-name retry carries the subfield (LastName='Robert', "
         "ForeName='Weiss')",
         [(c["cwid"], c.get("venue_fit")) for c in with_table.candidates(
             "Robert", "Weiss", None, None, 5, None, "Optics")[0]] == [("rw_c", 1.586)]),
    ]

    # load(): the table is built only for the Ithaca roster, only when AAR_VENUE_FIT is
    # "on", and a failure to build it is a roster without the term, never a failed load.
    import venue_fit as _vf
    _real_load, _env = _vf.load_dept_fit, os.environ.get("AAR_VENUE_FIT")
    seen = []

    def _boom(conn, campus):
        raise RuntimeError("Table 'venue_subfield_inferred' doesn't exist")
    try:
        _vf.load_dept_fit = lambda conn, campus: seen.append(campus) or table
        gated = []
        for val in (None, "", "off", "true", "ON "):
            os.environ.pop("AAR_VENUE_FIT", None)
            if val is not None:
                os.environ["AAR_VENUE_FIT"] = val
            gated.append((IdentityIndex._load_dept_venue_fit(None, CAMPUS_ITHACA),
                          IdentityIndex._load_dept_venue_fit(None, CAMPUS_WCM)))
        _vf.load_dept_fit = _boom
        logging.disable(logging.CRITICAL)
        failed = IdentityIndex._load_dept_venue_fit(None, CAMPUS_ITHACA)
    finally:
        logging.disable(logging.NOTSET)
        _vf.load_dept_fit = _real_load
        os.environ.pop("AAR_VENUE_FIT", None)
        if _env is not None:
            os.environ["AAR_VENUE_FIT"] = _env
    checks += [
        ("venue load: unset, empty, off and a typo build nothing and query nothing",
         gated[:4] == [(None, None)] * 4),
        ("venue load: 'on' builds the Ithaca table and still none for WCM",
         gated[4] == (table, None) and seen == [CAMPUS_ITHACA]),
        ("venue load: a failure is a roster without the term, not a failed load",
         failed is None),
    ]

    # ---- byline ORCID (v3.15) ---------------------------------------------------
    # Three Cornell Weisses. The byline spells out "Rhea Weiss", so by name Rhea leads at
    # `full` and the two R-initial rivals follow. Lindqvist is somebody the surname pool
    # cannot reach at all: the Rhea who publishes under a married name.
    import json as _json
    O_ROBERT, O_LIND, O_TWO, O_NOBODY, O_WCM = (f"0000-0001-0000-000{k}" for k in range(1, 6))
    orcid_people = [ithaca("Rhea", "Weiss", "rh_c"), ithaca("Robert", "Weiss", "rw_c"),
                    ithaca("Rex", "Weiss", "rx_c"), ithaca("Rhea", "Lindqvist", "rl_c"),
                    ithaca("Olga", "Petrova", "op_c")]
    pairs = [("RW_C", "https://orcid.org/" + O_ROBERT), ("rl_c", O_LIND),
             ("rh_c", O_TWO), ("rx_c", O_TWO),              # one ORCID, two netids
             ("gone1", O_NOBODY),                           # on file for nobody on the roster
             ("op_c", "not-an-orcid")]
    omap = orcid_map(pairs)
    o_idx = IdentityIndex(orcid_people, campus=CAMPUS_ITHACA, orcid_cwid=omap)
    plain_idx = IdentityIndex(orcid_people, campus=CAMPUS_ITHACA)
    byline = ("Weiss", "Rhea", "R", ["Cornell University, Ithaca, NY"])
    today = plain_idx.candidates(*byline)
    promoted, promoted_n = o_idx.candidates(*byline, byline_orcid=O_ROBERT)
    added, added_n = o_idx.candidates(*byline, byline_orcid=O_LIND)
    by_today = {c["cwid"]: c for c in today[0]}
    stranger, stranger_n = o_idx.candidates("Okafor", "Chidi", "C", byline_orcid=O_LIND)
    nameless, _ = o_idx.candidates("Weiss", None, None, byline_orcid=O_LIND)
    crowd = IdentityIndex([ithaca(f"R{k}x", "Weiss", f"r{k}_c") for k in range(7)]
                          + [ithaca("Rhea", "Lindqvist", "rl_c")],
                          campus=CAMPUS_ITHACA, orcid_cwid=omap)
    crowd_out, crowd_n = crowd.candidates("Weiss", None, "R", byline_orcid=O_LIND)

    def _strip(cands):
        return [{k: v for k, v in c.items() if k != "orcid_match"} for c in cands]

    checks += [
        ("orcid: the map is bare ORCID -> lower-cased netid, URL form accepted, a malformed "
         "value dropped, and an ORCID on file for two netids left out",
         omap == {O_ROBERT: "rw_c", O_LIND: "rl_c", O_NOBODY: "gone1"}),
        ("orcid: the index keeps only ORCIDs held by somebody on its roster",
         set(o_idx.orcid_rec) == {O_ROBERT, O_LIND}
         and o_idx.orcid_rec[O_LIND]["cwid"] == "rl_c"),
        ("orcid (1): no ORCID passed is today's output byte for byte, on an index that "
         "holds a map and on one that does not",
         _json.dumps(o_idx.candidates(*byline)) == _json.dumps(today)
         and _json.dumps(o_idx.candidates(*byline, byline_orcid=None)) == _json.dumps(today)
         and [c["cwid"] for c in today[0]] == ["rh_c", "rw_c", "rx_c"]
         and not any("orcid_match" in c for c in today[0])),
        ("orcid (1): ...and so is an ORCID that is on file for nobody on the roster, or "
         "passed to an index with no map",
         _json.dumps(o_idx.candidates(*byline, byline_orcid=O_NOBODY)) == _json.dumps(today)
         and _json.dumps(o_idx.candidates(*byline, byline_orcid="0000-0009-0000-0009"))
         == _json.dumps(today)
         and _json.dumps(plain_idx.candidates(*byline, byline_orcid=O_ROBERT))
         == _json.dumps(today)),
        ("orcid (2): an ORCID on file for a lower-ranked candidate promotes them over a "
         "full given-name match: orcid_match leads _rank_key",
         [(c["cwid"], c["orcid_match"], c["given_match"]) for c in promoted]
         == [("rw_c", True, "initial"), ("rh_c", False, "full"), ("rx_c", False, "initial")]
         and _rank_key(promoted[0])[0] is True and _rank_key(promoted[1])[0] is False),
        ("orcid (2): ...and it is ranking evidence only: same three people, same cohort, "
         "every confidence and every other field as it was",
         promoted_n == today[1] == 3
         and sorted(_strip(promoted), key=lambda c: c["cwid"])
         == sorted(today[0], key=lambda c: c["cwid"])
         and not any(k in c for c in promoted for k in ("status", "classification"))),
        ("orcid (3): an ORCID on file for somebody the name did not reach adds them to the "
         "list and ranks them first",
         [(c["cwid"], c["orcid_match"]) for c in added]
         == [("rl_c", True), ("rh_c", False), ("rw_c", False), ("rx_c", False)]
         and added[0]["name"] == "Rhea Lindqvist" and added[0]["campus"] == CAMPUS_ITHACA),
        ("orcid (3): ...built as a name-matched candidate is, with the given name judged "
         "honestly: 'Rhea' is a full given-name match whatever the surname",
         set(added[0]) - {"orcid_added"} == set(added[1]) and added[0]["orcid_added"] is True
         and "orcid_added" not in promoted[0] and added[0]["given_match"] == "full"
         and added[0]["name_source"] == "directory" and added[0]["affil_dept_match"] is False
         and added[0]["confidence"] == IdentityIndex._confidence("full", 4, False, False)
         == 0.6),
        ("orcid (3): ...the cohort counts them (4, so the card is not a single-candidate "
         "one), and the name-matched candidates keep the confidence their own cohort of "
         "three gave them",
         added_n == 4 and all(c["cohort_size"] == 4 for c in added)
         and all(c["confidence"] == by_today[c["cwid"]]["confidence"] for c in added[1:])
         and [dict(c, cohort_size=3) for c in _strip(added[1:])] == today[0]),
        ("orcid (3): a byline whose surname nobody holds still reaches the ORCID's person, "
         "alone, and a given name that shares nothing with theirs reads 'unknown', the "
         "lowest tier: no credit for the ORCID",
         [(c["cwid"], c["orcid_match"], c["given_match"], c["cohort_size"], c["confidence"])
          for c in stranger] == [("rl_c", True, "unknown", 1, 0.55)] and stranger_n == 1
         and nameless[0]["cwid"] == "rl_c" and nameless[0]["given_match"] == "unknown"),
        ("orcid (3): the ORCID's person survives the top-5 cut of a cohort of eight",
         crowd_n == 8 and len(crowd_out) == 5 and crowd_out[0]["cwid"] == "rl_c"
         and [c["orcid_match"] for c in crowd_out] == [True] + [False] * 4),
        ("orcid (3): the added person's tier is the loop's own for anyone the name does "
         "reach (full, initial, publishing name, given+middle)",
         all(IdentityIndex._orcid_given_match(r, _norm(fore), _byline_first_name(fore),
                                              _first_initial(fore, None))[0]
             == IdentityIndex([r]).candidates(r["surname"], fore)[0][0]["given_match"]
             for r, fore in ((rec("Rhea", "", "Weiss"), "Rhea"),
                             (rec("Rhea", "", "Weiss"), "Rhea M"),
                             (rec("Rhea", "", "Weiss"), "R"),
                             (rec("Rhea", "", "Weiss"), "Ruth"),
                             (rec("Hua", "Judy", "Zhong"), "Judy"),
                             (rec("Eileen Ruth", "Samson", "Torres"), "Eileen Ruth Samson"),
                             (rec("Shuo", "", "Sun", pref_first="Mila"), "Mila")))),
        ("orcid (4): an ORCID on file for two netids is ignored: the row is today's",
         O_TWO not in o_idx.orcid_rec
         and _json.dumps(o_idx.candidates(*byline, byline_orcid=O_TWO)) == _json.dumps(today)),
    ]

    # (5) WCM. A WCM index never holds a map, and a roster passes the ORCID to the Ithaca
    # index only when the WCM roster offers nobody.
    wcm_weiss = rec("Rhea", "", "Weiss", cwid="rh_w")
    wcm_map_idx = IdentityIndex([wcm_weiss], orcid_cwid={O_WCM: "rh_w"})
    mixed_roster = CampusRoster(IdentityIndex([wcm_weiss]), ithaca=o_idx)
    lone_roster = CampusRoster(IdentityIndex([rec("Bea", "", "Other", cwid="bo_w")]),
                               ithaca=o_idx)
    two_campus = ["Weill Cornell Medicine, New York, NY", "Cornell University, Ithaca, NY"]
    linked_wcm = IdentityIndex([dict(rec("Rhea", "", "Weiss", cwid="rh_w"), cornell_linked=True)])
    linked_roster = CampusRoster(linked_wcm, ithaca=o_idx)
    checks += [
        ("orcid (5): a WCM index never holds a map, even when handed one, and ignores the "
         "ORCID entirely",
         wcm_map_idx.orcid_rec == {}
         and wcm_map_idx.candidates("Weiss", "Rhea", "R", byline_orcid=O_WCM)
         == wcm_map_idx.candidates("Weiss", "Rhea", "R")),
        ("orcid (5): a WCM candidate on the list -> no effect: the two-campus row is "
         "ranked exactly as with no ORCID, and nobody carries orcid_match",
         _json.dumps(mixed_roster.candidates("Weiss", "Rhea", "R", two_campus,
                                             byline_orcid=O_ROBERT))
         == _json.dumps(mixed_roster.candidates("Weiss", "Rhea", "R", two_campus))
         and mixed_roster.candidates("Weiss", "Rhea", "R", two_campus,
                                     byline_orcid=O_LIND)[0][0]["cwid"] == "rh_w"
         and "rl_c" not in {c["cwid"] for c in mixed_roster.candidates(
             "Weiss", "Rhea", "R", two_campus, byline_orcid=O_LIND)[0]}),
        ("orcid (5): ...and so is a Cornell byline that keeps a linked WCM dual appointee",
         _json.dumps(linked_roster.candidates("Weiss", "Rhea", "R", byline[3],
                                              byline_orcid=O_ROBERT))
         == _json.dumps(linked_roster.candidates("Weiss", "Rhea", "R", byline[3]))),
        ("orcid (5): a WCM-only byline never reaches the Ithaca roster, ORCID or not",
         lone_roster.candidates("Other", "Bea", "B", ["Weill Cornell Medicine, New York"],
                                byline_orcid=O_LIND)
         == lone_roster.candidates("Other", "Bea", "B", ["Weill Cornell Medicine, New York"])),
        ("orcid: a Cornell byline the WCM roster offers nobody for is the Ithaca index's "
         "own answer, ORCID included, through a CampusRoster and with a venue subfield",
         lone_roster.candidates(*byline, byline_orcid=O_LIND) == (added, added_n)
         and lone_roster.candidates("Weiss", "Rhea", "R", two_campus,
                                    venue_subfield="Optics", byline_orcid=O_ROBERT)
         == (promoted, promoted_n)),
    ]

    # load(): the map is read only for the Ithaca roster, only when AAR_BYLINE_ORCID is
    # "on", and a missing table or a failed read is a roster without the match.
    class _MapConn:
        def __init__(self, has_table=True, fail=False):
            self.has_table, self.fail, self.sql = has_table, fail, []

        def execute(self, stmt):
            self.sql.append(str(stmt))
            if self.fail:
                raise RuntimeError("connection lost")
            return self

        def scalar(self):
            return int(self.has_table)

        def all(self):
            return pairs
    _env = os.environ.get("AAR_BYLINE_ORCID")
    _env_wcm = os.environ.pop("AAR_BYLINE_ORCID_WCM", None)   # second switch off: v3.15
    try:
        o_gated = []
        for val in (None, "", "off", "true", "ON "):
            os.environ.pop("AAR_BYLINE_ORCID", None)
            if val is not None:
                os.environ["AAR_BYLINE_ORCID"] = val
            conn = _MapConn()
            o_gated.append((byline_orcid_enabled(),
                            IdentityIndex._load_orcid_map(conn, CAMPUS_ITHACA),
                            IdentityIndex._load_orcid_map(conn, CAMPUS_WCM), len(conn.sql)))
        logging.disable(logging.CRITICAL)
        no_table = _MapConn(has_table=False)
        o_missing = IdentityIndex._load_orcid_map(no_table, CAMPUS_ITHACA)
        o_failed = IdentityIndex._load_orcid_map(_MapConn(fail=True), CAMPUS_ITHACA)
    finally:
        logging.disable(logging.NOTSET)
        os.environ.pop("AAR_BYLINE_ORCID", None)
        if _env is not None:
            os.environ["AAR_BYLINE_ORCID"] = _env
        if _env_wcm is not None:
            os.environ["AAR_BYLINE_ORCID_WCM"] = _env_wcm
    checks += [
        ("orcid load: unset, empty, off and a typo read nothing and query nothing",
         o_gated[:4] == [(False, {}, {}, 0)] * 4),
        ("orcid load: 'on' reads the Ithaca map and still none for WCM",
         o_gated[4] == (True, omap, {}, 2)),
        ("orcid load: no cornell_pub_orcid_mapping table (the dev instance) loads nothing, "
         "and the table is never queried",
         o_missing == {} and len(no_table.sql) == 1 and "information_schema" in no_table.sql[0]),
        ("orcid load: a failed read is a roster without the match, not a failed load",
         o_failed == {}),
    ]

    # ---- byline ORCID on WCM and mixed lists (AAR_BYLINE_ORCID_WCM) ---------------
    # The precedence rules, on the pure function. S_* are Scholars-confirmed.
    S_W, S_I, S_X, S_TWICE, F_I, F_W, F_HELD, F_DUP = (
        f"0000-0003-0000-000{k}" for k in range(1, 9))
    src, twice = orcid_sources(
        [("RH_W", "https://orcid.org/" + S_W, "wcm"), ("rw_c", S_I, "ithaca"),
         ("xc_w", S_X, "wcm"), ("aa_w", S_TWICE, "wcm"), ("bb_c", S_TWICE, "ithaca"),
         ("zz_w", "not-an-orcid", "wcm"), ("qq_w", S_W[:-1] + "9", "qatar")],
        ithaca_fallback=[("rl_c", F_I), ("other_c", S_W),      # (a)+(c): Scholars has S_W
                         ("xc_c", S_X),                        # (c): the cross-campus person
                         ("rw_c", F_HELD),                     # (b): rw_c is in Scholars
                         ("aa_c", S_TWICE),                    # (d): Scholars said two people
                         ("d1_c", F_DUP), ("d2_c", F_DUP)],    # (d): two in one list
        wcm_fallback=[("BO_W", F_W), ("other_w", S_I),         # (a)+(c), the other way
                      ("rh_w", F_HELD[:-1] + "9"),             # (b): rh_w is in Scholars
                      ("rw_c", F_HELD[:-1] + "7")])            # an id is a person per campus
    checks += [
        ("orcid sources (a): an ORCID Scholars confirms maps to the Scholars person on "
         "their campus, labelled 'scholars', whatever a fallback list says",
         src[CAMPUS_WCM][S_W] == ("rh_w", ORCID_SCHOLARS)
         and src[CAMPUS_ITHACA][S_I] == ("rw_c", ORCID_SCHOLARS)
         and src[CAMPUS_WCM][S_X] == ("xc_w", ORCID_SCHOLARS)),
        ("orcid sources (c): ...and is removed from the OTHER campus's fallback, so a "
         "cross-campus person holds the iD on one roster",
         S_W not in src[CAMPUS_ITHACA] and S_I not in src[CAMPUS_WCM]
         and S_X not in src[CAMPUS_ITHACA]),
        ("orcid sources (b): a person with a Scholars-confirmed iD takes no fallback iD",
         F_HELD not in src[CAMPUS_ITHACA] and F_HELD[:-1] + "9" not in src[CAMPUS_WCM]),
        ("orcid sources: fallbacks are labelled by list and ids are lower-cased; the same "
         "id on the other campus is another person",
         src[CAMPUS_ITHACA][F_I] == ("rl_c", ORCID_AFFILIATION_MANAGER)
         and src[CAMPUS_WCM][F_W] == ("bo_w", ORCID_INFERRED)
         and src[CAMPUS_WCM][F_HELD[:-1] + "7"] == ("rw_c", ORCID_INFERRED)),
        ("orcid sources (d): an ORCID on file for two people in one fallback is ignored; "
         "one Scholars confirms for two people is ignored on both campuses, kept out of "
         "the fallbacks, and returned for the log",
         F_DUP not in src[CAMPUS_ITHACA] and twice == [S_TWICE]
         and S_TWICE not in src[CAMPUS_WCM] and S_TWICE not in src[CAMPUS_ITHACA]),
        ("orcid sources: a malformed iD and an unknown campus are skipped, and nothing "
         "else is in either map",
         set(src[CAMPUS_WCM]) == {S_W, S_X, F_W, F_HELD[:-1] + "7"}
         and set(src[CAMPUS_ITHACA]) == {S_I, F_I}),
        ("orcid sources (e): with no Ithaca people named, the inferred list stays on WCM "
         "exactly as before",
         orcid_sources([], [("rl_c", F_I)], [("bo_w", F_W), ("as_c", F_DUP)])
         == orcid_sources([], [("rl_c", F_I)], [("bo_w", F_W), ("as_c", F_DUP)], frozenset())
         and orcid_sources([], [], [("as_c", F_DUP)])[0][CAMPUS_WCM]
         == {F_DUP: ("as_c", ORCID_INFERRED)}),
        ("orcid sources (e): an inferred iD for an Ithaca person goes to the Ithaca map, "
         "labelled 'inferred', and leaves the WCM one",
         (lambda m: m[CAMPUS_ITHACA] == {F_I: ("rl_c", ORCID_AFFILIATION_MANAGER),
                                         F_DUP: ("as_c", ORCID_INFERRED)}
          and m[CAMPUS_WCM] == {F_W: ("bo_w", ORCID_INFERRED)})(
             orcid_sources([], [("rl_c", F_I)], [("bo_w", F_W), ("AS_C", F_DUP)],
                           frozenset({"as_c", "rl_c"}))[0])),
        ("orcid sources (e): Cornell's list wins an ORCID both name; Scholars still wins "
         "over both and a Scholars-held Ithaca person takes no inferred iD; an inferred "
         "ORCID for two people is dropped though they are on different rosters",
         orcid_sources([], [("rl_c", F_I)], [("zz_c", F_I)], frozenset({"rl_c", "zz_c"}))[0]
         [CAMPUS_ITHACA] == {F_I: ("rl_c", ORCID_AFFILIATION_MANAGER)}
         and orcid_sources([("rw_c", S_I, "ithaca")], [], [("zz_c", S_I), ("rw_c", F_HELD)],
                           frozenset({"rw_c", "zz_c"}))[0][CAMPUS_ITHACA]
         == {S_I: ("rw_c", ORCID_SCHOLARS)}
         and orcid_sources([], [], [("bo_w", F_DUP), ("as_c", F_DUP)],
                           frozenset({"as_c"}))[0] == {CAMPUS_WCM: {}, CAMPUS_ITHACA: {}}),
        ("orcid sources: no Scholars rows (v3.16 not applied) is the fallbacks, labelled",
         orcid_sources([], [("rl_c", F_I)], [("bo_w", F_W)])
         == ({CAMPUS_WCM: {F_W: ("bo_w", ORCID_INFERRED)},
              CAMPUS_ITHACA: {F_I: ("rl_c", ORCID_AFFILIATION_MANAGER)}}, [])),
    ]

    # The rosters the second switch builds: labelled maps on both. WCM has Rhea Weiss
    # (Scholars), Ruth Weiss, and Bea Other, whose inferred iD a "Weiss" byline carries.
    wcm_people = [rec("Rhea", "", "Weiss", cwid="rh_w"), rec("Ruth", "", "Weiss", cwid="ru_w"),
                  rec("Bea", "", "Other", cwid="bo_w")]
    lw_idx = IdentityIndex(wcm_people, orcid_cwid=src[CAMPUS_WCM])
    li_idx = IdentityIndex(orcid_people, campus=CAMPUS_ITHACA, orcid_cwid=src[CAMPUS_ITHACA])
    on_roster = CampusRoster(lw_idx, ithaca=li_idx)
    wcm_affil = ["Weill Cornell Medicine, New York, NY"]
    w_today = IdentityIndex(wcm_people).candidates("Weiss", "Ruth", "R", wcm_affil)
    w_promoted, _ = on_roster.candidates("Weiss", "Ruth", "R", wcm_affil, byline_orcid=S_W)
    w_added, w_added_n = on_roster.candidates("Weiss", "Ruth", "R", wcm_affil, byline_orcid=F_W)
    near_maps, _ = orcid_sources([], [], [("bz_w", F_W), ("ru_w", F_HELD)])
    near_idx = IdentityIndex([rec("Ruth", "", "Weiss", cwid="ru_w"),
                              rec("Bea", "", "Weisz", cwid="bz_w")],
                             orcid_cwid=near_maps[CAMPUS_WCM])
    near_added, _ = near_idx.candidates("Weiss", "Ruth", "R", wcm_affil, byline_orcid=F_W)
    near_confirm, _ = near_idx.candidates("Weiss", "R", "R", wcm_affil, byline_orcid=F_HELD)
    m_today = on_roster.candidates("Weiss", "Rhea", "R", two_campus)
    m_ith, m_ith_n = on_roster.candidates("Weiss", "Rhea", "R", two_campus, byline_orcid=F_I)
    m_wcm, _ = on_roster.candidates("Weiss", "Robert", "R", two_campus, byline_orcid=S_W)
    c_only, _ = on_roster.candidates("Weiss", "Robert", "R", byline[3], byline_orcid=S_W)

    def _plain(cands):
        return [{k: v for k, v in c.items()
                 if k not in ("orcid_match", "orcid_added", "orcid_source")} for c in cands]
    clash = CampusRoster(IdentityIndex(wcm_people, orcid_cwid={F_I: ("rh_w", ORCID_INFERRED)}),
                         ithaca=li_idx)
    # Stored candidate lists, as the replay guards read them: a v3.15 Ithaca row, and an
    # Ithaca and a WCM row ranked with the second switch on.
    _G_V315 = _json.dumps([{"cwid": "rl_c", "campus": CAMPUS_ITHACA, "orcid_match": True},
                           {"cwid": "rh_c", "campus": CAMPUS_ITHACA, "orcid_match": False}])
    _G_ITH = _json.dumps([{"cwid": "rl_c", "campus": CAMPUS_ITHACA, "orcid_match": True,
                           "orcid_source": ORCID_AFFILIATION_MANAGER}])
    _G_WCM = _json.dumps([{"cwid": "rh_w", "orcid_match": True, "orcid_source": ORCID_SCHOLARS},
                          {"cwid": "rl_c", "campus": CAMPUS_ITHACA, "orcid_match": False}])
    # An INVERTED byline (LastName 'Ruth J', ForeName 'Weiss'): no surname pool, so the
    # name reaches Ruth Weiss only through the swapped retry.
    inv = ("Ruth J", "Weiss", "W", wcm_affil)
    inv_today = IdentityIndex(wcm_people).candidates(*inv)
    inv_other, inv_other_n = lw_idx.candidates(*inv, byline_orcid=S_W)
    inv_homonym, _ = lw_idx.candidates(*inv, byline_orcid=S_W)
    inv_self, inv_self_n = IdentityIndex(
        wcm_people, orcid_cwid={S_W: ("ru_w", ORCID_SCHOLARS)}).candidates(*inv, byline_orcid=S_W)
    checks += [
        ("orcid wcm: a WCM index holds a labelled map, for the people on its roster",
         set(lw_idx.orcid_rec) == {S_W, F_W} and lw_idx.orcid_source
         == {S_W: ORCID_SCHOLARS, F_W: ORCID_INFERRED}),
        ("orcid wcm: on a WCM-only byline the Scholars person leads a full given-name "
         "match and carries orcid_source 'scholars'; nobody else carries the key",
         [(c["cwid"], c["orcid_match"], c.get("orcid_source"), c["given_match"])
          for c in w_promoted]
         == [("rh_w", True, "scholars", "initial"), ("ru_w", False, None, "full")]
         and "orcid_source" not in w_promoted[1] and "campus" not in w_promoted[0]),
        ("orcid wcm: ...ranking evidence only: the same people, confidences and fields",
         sorted(_plain(w_promoted), key=lambda c: c["cwid"])
         == sorted(w_today[0], key=lambda c: c["cwid"])),
        ("orcid wcm (guard): an inferred iD naming a person the surname does not reach and "
         "whose surname is unrelated (Other, on a Weiss byline) is ignored: the row is the "
         "WCM list as it was, with no orcid key",
         _json.dumps([w_added, w_added_n]) == _json.dumps(w_today)
         and not any(k.startswith("orcid") for c in w_added for k in c)),
        ("orcid wcm (guard): ...an inferred iD still adds a person whose surname is close "
         "(Weisz, on a Weiss byline), labelled 'inferred' and marked orcid_added "
         "(top_orcid_match 2), and still confirms a person the name reaches",
         [(c["cwid"], c["orcid_match"], c.get("orcid_added")) for c in near_added]
         == [("bz_w", True, True), ("ru_w", False, None)]
         and near_added[0]["orcid_source"] == "inferred"
         and [(c["cwid"], c["orcid_match"]) for c in near_confirm][0] == ("ru_w", True)),
        ("orcid (guard): surnames_close keeps spelling variants and compound surnames, and "
         "rejects the unrelated surnames the 2026-11-23 dry run proposed",
         all(surnames_close(a, {"surname": b}) for a, b in (
             ("Dörr", "Doerr"), ("Sarkar", "Sarker"), ("Ahmad", "Ahmed"),
             ("Jackvony Infurna", "Jackvony"), ("Arevalo-Perez", "Arevalo Perez")))
         and surnames_close("Landau", {"surname": "Smith", "pref_full": "Heather Landau"})
         and not any(surnames_close(a, {"surname": b}) for a, b in (
             ("Matarangas", "Green"), ("Desilets", "Drilon"), ("Hollmann", "Chaligne"),
             ("Kalra", "Ngo")))),
        ("orcid (guard): Scholars-confirmed and Cornell-list iDs keep their reach: the "
         "Lindqvist add on a Weiss byline is unchanged",
         [(c["cwid"], c["orcid_match"]) for c in m_ith][:1] == [("rl_c", True)]),
        ("orcid wcm: no ORCID, or one on file for nobody, is the WCM list as it was, byte "
         "for byte, labelled map or not",
         _json.dumps(on_roster.candidates("Weiss", "Ruth", "R", wcm_affil))
         == _json.dumps(on_roster.candidates("Weiss", "Ruth", "R", wcm_affil,
                                             byline_orcid=O_NOBODY)) == _json.dumps(w_today)),
        ("orcid wcm: a WCM-only byline still never reaches the Ithaca roster",
         _json.dumps(on_roster.candidates("Weiss", "Ruth", "R", wcm_affil, byline_orcid=F_I))
         == _json.dumps(w_today)),
        ("orcid mixed: on a two-campus list the Ithaca person the ORCID is on file for "
         "leads the WCM full-name match, and every candidate carries orcid_match",
         [(c["cwid"], c["orcid_match"]) for c in m_ith][:2] == [("rl_c", True), ("rh_w", False)]
         and all("orcid_match" in c for c in m_ith)
         and m_ith[0]["orcid_source"] == "affiliation_manager"
         and sum(c["orcid_match"] for c in m_ith) == 1),
        ("orcid mixed: ...it removes nobody: the list without the ORCID is still there, in "
         "its order, behind the added person, and the cohort counts one more",
         [c["cwid"] for c in m_ith[1:]] == [c["cwid"] for c in m_today[0]][:len(m_ith) - 1]
         and m_ith_n == m_today[1] + 1 and all(c["cohort_size"] == m_ith_n for c in m_ith)),
        ("orcid mixed: a Scholars-confirmed WCM person leads a two-campus list over a "
         "Cornell full-name match",
         (m_wcm[0]["cwid"], m_wcm[0]["orcid_match"], m_wcm[0]["orcid_source"])
         == ("rh_w", True, "scholars")
         and "rw_c" in {c["cwid"] for c in m_wcm} and not any(c["orcid_match"] for c in m_wcm[1:])),
        ("orcid mixed: a Cornell-only byline keeps the WCM person its ORCID is on file "
         "for though they hold no Cornell appointment, and no other unlinked WCM homonym",
         c_only[0]["cwid"] == "rh_w" and c_only[0]["orcid_match"] is True
         and "ru_w" not in {c["cwid"] for c in c_only}
         and {c["cwid"] for c in c_only[1:]} == {"rh_c", "rw_c", "rx_c"}),
        ("orcid mixed: an ORCID that finds a person on each roster is ignored for the row",
         _json.dumps(clash.candidates("Weiss", "Rhea", "R", two_campus, byline_orcid=F_I))
         == _json.dumps(clash.candidates("Weiss", "Rhea", "R", two_campus))),
        ("orcid: orcid_rows_without_map asks the roster the row's matched candidate is on: "
         "no map, the v3.15 map and a labelled one, for a bare index and for a roster",
         [orcid_rows_without_map(r, [_G_V315, _G_ITH, _G_WCM, "[]", None])
          for r in (plain_idx, o_idx, li_idx, lw_idx, on_roster,
                    CampusRoster(IdentityIndex(wcm_people), ithaca=o_idx),
                    CampusRoster(IdentityIndex(wcm_people)))]
         == [(1, 2), (0, 2), (0, 0), (0, 0), (0, 0), (0, 2), (1, 2)]),
        ("orcid: ...a WCM index that holds a map does not answer for an Ithaca index that "
         "holds none (its lists failed to load): Ithaca rows are counted, WCM ones not",
         orcid_rows_without_map(CampusRoster(lw_idx, ithaca=plain_idx),
                                [_G_V315, _G_ITH, _G_WCM]) == (1, 1)
         and orcid_rows_without_map(CampusRoster(IdentityIndex(wcm_people), ithaca=li_idx),
                                    [_G_V315, _G_ITH, _G_WCM]) == (0, 1)
         and orcid_rows_without_map(CampusRoster(lw_idx, ithaca=plain_idx),
                                    ['[{"orcid_match": false}]', '[{"orcid_match"']) == (2, 0)),
        ("orcid: _rank_key is unchanged by the source: orcid_source is a label, not a term",
         _rank_key(w_promoted[0]) == _rank_key(dict(w_promoted[0], orcid_source="inferred"))),
        ("orcid wcm, inverted byline: an ORCID on file for somebody else does not cost the "
         "row the person the swapped retry reaches: she stays, second, as she was but for "
         "the cohort, which counts both",
         [c["cwid"] for c in inv_today[0]] == ["ru_w"] and inv_today[1] == 1
         and [(c["cwid"], c["orcid_match"], c.get("orcid_added"), c.get("orcid_source"))
              for c in inv_other] == [("rh_w", True, True, "scholars"), ("ru_w", False, None, None)]
         and inv_other_n == 2 and all(c["cohort_size"] == 2 for c in inv_other)
         and _plain(inv_other[1:]) == [dict(inv_today[0][0], cohort_size=2)]),
        ("orcid wcm, inverted byline (guard): an inferred iD for an unrelated surname is "
         "ignored there too: the retry's answer, byte for byte",
         _json.dumps(lw_idx.candidates(*inv, byline_orcid=F_W)) == _json.dumps(inv_today)),
        ("orcid wcm, inverted byline: an ORCID on file for the person the retry reaches "
         "confirms her full name match (not orcid_added, so top_orcid_match 1), at the "
         "confidence she had",
         [(c["cwid"], c["orcid_match"], c.get("orcid_source"), c["given_match"],
           c["confidence"]) for c in inv_self]
         == [("ru_w", True, "scholars", "full", inv_today[0][0]["confidence"])]
         and "orcid_added" not in inv_self[0] and inv_self_n == 1),
        ("orcid wcm, inverted byline: a namesake the retry drops (initial tier) stays on "
         "the list only through her ORCID, and is marked as added by it",
         [(c["cwid"], c["orcid_match"], c.get("orcid_added"), c["given_match"])
          for c in inv_homonym] == [("rh_w", True, True, "initial"), ("ru_w", False, None, "full")]),
        ("orcid wcm, inverted byline: no ORCID is the retry's answer byte for byte, and "
         "with nobody for the retry to reach the ORCID's person stands alone, as before",
         _json.dumps(lw_idx.candidates(*inv)) == _json.dumps(inv_today)
         and [(c["cwid"], c["cohort_size"]) for c in lw_idx.candidates(
             "Zed Q", "Nomatch", "N", wcm_affil, byline_orcid=S_W)[0]] == [("rh_w", 1)]),
        ("orcid, inverted byline on the v3.15 map (second switch off): unchanged, the "
         "ORCID's person alone",
         [c["cwid"] for c in o_idx.candidates("Rhea J", "Weiss", "W", byline[3],
                                              byline_orcid=O_LIND)[0]] == ["rl_c"]
         and "orcid_source" not in o_idx.candidates("Rhea J", "Weiss", "W", byline[3],
                                                    byline_orcid=O_LIND)[0][0]),
    ]

    # OFF-STATE EQUIVALENCE. The rosters `load()` builds with AAR_BYLINE_ORCID_WCM off are
    # the v3.15 ones: a WCM index handed nothing, an Ithaca index with the unlabelled
    # map. Three representative bylines, each against the answer v3.15 gave.
    off_wcm = IdentityIndex(wcm_people, orcid_cwid={})
    off_roster = CampusRoster(off_wcm, ithaca=o_idx)
    off_lone = CampusRoster(IdentityIndex([rec("Bea", "", "Other", cwid="bo_w")]), ithaca=o_idx)
    checks += [
        ("orcid off-state, WCM-only byline: the ORCID is not looked at; the list is the "
         "WCM index's own, byte for byte, with no orcid key of any kind",
         _json.dumps(off_roster.candidates("Weiss", "Ruth", "R", wcm_affil, byline_orcid=S_W))
         == _json.dumps(w_today) and off_wcm.orcid_rec == {} == off_wcm.orcid_source
         and not any(k.startswith("orcid") for c in w_today[0] for k in c)),
        ("orcid off-state, Ithaca-only byline: the v3.15 answer, orcid_match and all, and "
         "no orcid_source",
         _json.dumps(off_lone.candidates(*byline, byline_orcid=O_LIND))
         == _json.dumps([added, added_n])
         and _json.dumps(off_lone.candidates(*byline, byline_orcid=O_ROBERT))
         == _json.dumps([promoted, promoted_n])
         and not any("orcid_source" in c for c in added + promoted)
         and o_idx.orcid_source == {}),
        ("orcid off-state, mixed byline: the ORCID is withheld from both rosters and the "
         "merged list is the one with no ORCID, byte for byte",
         _json.dumps(off_roster.candidates("Weiss", "Rhea", "R", two_campus, byline_orcid=O_LIND))
         == _json.dumps(off_roster.candidates("Weiss", "Rhea", "R", two_campus))
         and _json.dumps(off_roster.candidates("Weiss", "Rhea", "R", two_campus,
                                               byline_orcid=O_ROBERT))
         == _json.dumps(off_roster.candidates("Weiss", "Rhea", "R", two_campus))
         and not any("orcid_match" in c for c in off_roster.candidates(
             "Weiss", "Rhea", "R", two_campus, byline_orcid=O_ROBERT)[0])),
        ("orcid: the PubMed lane's call (no ORCID) is the same list in both states, on a "
         "WCM-only and on a mixed byline",
         _json.dumps(on_roster.candidates("Weiss", "Ruth", "R", wcm_affil))
         == _json.dumps(off_roster.candidates("Weiss", "Ruth", "R", wcm_affil))
         and _json.dumps(on_roster.candidates("Weiss", "Rhea", "R", two_campus))
         == _json.dumps(off_roster.candidates("Weiss", "Rhea", "R", two_campus))),
    ]

    # load(): the second switch needs the first, and off it reads exactly what v3.15 read.
    class _SrcConn:
        """Answers information_schema by table name and each list by its FROM."""
        ROWS = {"scholar_active": [("rh_w", S_W, "wcm"), ("rw_c", S_I, "ithaca")],
                "cornell_pub_orcid_mapping": [("rl_c", F_I), ("other_c", S_W)],
                "pubsource_orcid_person": [("BO_W", F_W)]}

        def __init__(self, absent=(), fail=None):
            self.absent, self.fail, self.sql = absent, fail, []

        def execute(self, stmt):
            sql = str(stmt)
            self.sql.append(sql)
            table = next(t for t in self.ROWS if t in sql)
            if "information_schema" in sql:
                self._answer = 0 if table in self.absent else 2 if "columns" in sql else 1
            elif table == self.fail:
                raise RuntimeError("connection lost")
            else:
                self._answer = self.ROWS[table]
            return self

        def scalar(self):
            return self._answer

        def all(self):
            return self._answer
    _keep = {k: os.environ.pop(k, None) for k in ("AAR_BYLINE_ORCID", "AAR_BYLINE_ORCID_WCM")}
    try:
        logging.disable(logging.CRITICAL)
        s_gated = []
        for first, second in ((None, "on"), ("off", "on"), ("on", None), ("on", "true"),
                              ("on", "off")):
            for k, v in (("AAR_BYLINE_ORCID", first), ("AAR_BYLINE_ORCID_WCM", second)):
                os.environ.pop(k, None)
                if v is not None:
                    os.environ[k] = v
            conn = _SrcConn()
            s_gated.append((byline_orcid_wcm_enabled(),
                            IdentityIndex._load_orcid_map(conn, CAMPUS_WCM),
                            IdentityIndex._load_orcid_map(conn, CAMPUS_ITHACA),
                            any("scholar_active" in q or "pubsource" in q for q in conn.sql)))
        os.environ["AAR_BYLINE_ORCID"], os.environ["AAR_BYLINE_ORCID_WCM"] = "on", " ON"
        s_conn = _SrcConn()
        s_on = (byline_orcid_wcm_enabled(), IdentityIndex._load_orcid_map(s_conn, CAMPUS_WCM),
                IdentityIndex._load_orcid_map(s_conn, CAMPUS_ITHACA))
        s_routed = (IdentityIndex._load_orcid_map(_SrcConn(), CAMPUS_WCM, frozenset({"bo_w"})),
                    IdentityIndex._load_orcid_map(_SrcConn(), CAMPUS_ITHACA, frozenset({"bo_w"})))
        s_bare = _SrcConn(absent=("scholar_active", "pubsource_orcid_person"))
        s_no_scholars = (IdentityIndex._load_orcid_map(s_bare, CAMPUS_WCM),
                         IdentityIndex._load_orcid_map(s_bare, CAMPUS_ITHACA))
        s_failed = [IdentityIndex._load_orcid_map(_SrcConn(fail=t), c)
                    for t in _SrcConn.ROWS for c in (CAMPUS_WCM, CAMPUS_ITHACA)]
    finally:
        logging.disable(logging.NOTSET)
        for k, v in list(_keep.items()) + [("AAR_BYLINE_ORCID_WCM", _env_wcm)]:
            os.environ.pop(k, None)
            if v is not None:
                os.environ[k] = v
    v315 = orcid_map(_SrcConn.ROWS["cornell_pub_orcid_mapping"])
    checks += [
        ("orcid wcm load: the second switch alone, or with the first off, is off: nothing "
         "is read for either campus",
         s_gated[:2] == [(False, {}, {}, False)] * 2),
        ("orcid wcm load: first switch on, second unset, a typo or off: no WCM map, the "
         "unlabelled v3.15 Ithaca map, and scholar_active and the inferred list never read",
         s_gated[2:] == [(False, {}, v315, False)] * 3
         and all(isinstance(v, str) for v in v315.values())),
        ("orcid wcm load: both on reads the labelled map of each campus, Scholars first",
         s_on == (True, {S_W: ("rh_w", "scholars"), F_W: ("bo_w", "inferred")},
                  {S_I: ("rw_c", "scholars"), F_I: ("rl_c", "affiliation_manager")})),
        ("orcid wcm load (e): the roster's Ithaca ids route an inferred iD to the Ithaca "
         "map, after Cornell's list; the WCM map loses it",
         s_routed == ({S_W: ("rh_w", "scholars")},
                      {S_I: ("rw_c", "scholars"), F_I: ("rl_c", "affiliation_manager"),
                       F_W: ("bo_w", "inferred")})),
        ("orcid wcm load: scholar_active without its v3.16 columns and no inferred table "
         "is no error: no WCM map, the Ithaca fallback labelled, the absent lists not queried",
         s_no_scholars == ({}, {F_I: ("rl_c", "affiliation_manager"),
                                S_W: ("other_c", "affiliation_manager")})
         and not any("FROM scholar_active" in q or "FROM pubsource" in q for q in s_bare.sql)),
        ("orcid wcm load: any list failing to read is a roster without the match on both "
         "campuses, not a failed load and not half the precedence rule",
         s_failed == [{}] * 6),
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
