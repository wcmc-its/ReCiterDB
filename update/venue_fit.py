#!/usr/bin/env python3
"""
Venue fit for the external AAR lanes: does this venue fit the candidate's department?

The Scopus and OpenAlex lanes have no identity-only score, so a candidate is ranked on
the byline name and the affiliation text alone. A physicist and a hotel-school lecturer
who share a surname and an initial look the same to that evidence; the journal usually
tells them apart. This module supplies the two things the matcher needs to say so:

  1. a venue -> Science-Metrix subfield lookup (`load_venue_lookup`, `subfield_for`), from
     `journal_science_metrix` by ISSN or eISSN and, for containers with no ISSN
     (proceedings, books), from `venue_subfield_inferred` by normalised title; and
  2. a department x subfield table learned from accepted authorships (`DeptVenueFit`,
     `load_dept_fit`), which scores how much more a department publishes in a subfield
     than its campus does.

`venue_term` turns that score into the bounded confidence term `identity_index` adds.
The term is ranking evidence and a curator-visible flag. It never accepts, rejects or
removes a candidate, and it reaches the sort only through `confidence`, which stays last
in `identity_index._rank_key`: on the replay behind this design, letting a good fit
outrank the given-name tier broke 130 correct picks to fix 62.

Measured before any of this was written (Cornell Scopus rows a curator had adjudicated,
2026-11-16): AUC 0.870 on single-candidate initials-only bylines, 0.891 where the byline
spells out a different given name, 0.914 on multi-candidate initial-tier rows; a
time-split replay held at 0.879 / 0.865 / 0.907. Re-ranking stored candidates with
+0.15 / -0.20 fixed 88 top picks and broke 17; larger weights changed nothing further.

Imports only sqlalchemy and cornell_hr_feed (stdlib only, for `venue_dept`), so the
lanes that load it stay free of the PubMed-lane modules.

Usage:
  python3 venue_fit.py --selftest      # offline checks, no DB
"""
import math, re, sys, unicodedata
from collections import Counter

from sqlalchemy import text

from cornell_hr_feed import venue_dept

ALPHA = 5.0          # pseudo-count pulling a department's share toward the campus share
MIN_DEPT = 20        # a department with fewer training rows returns no fit
FIT_BONUS_AT = 1.0   # fit >= this earns the bonus
FIT_PENALTY_BELOW = -1.0   # fit < this earns the penalty
VENUE_BONUS = 0.15
VENUE_PENALTY = 0.20


# ---- venue key ----------------------------------------------------------------
# A container with no ISSN is identified by its title, and the same series arrives under
# a new title every year: "CLEO 2023", "CLEO 2024", "39th Aerospace Sciences Meeting and
# Exhibit", "40th Aerospace Sciences Meeting and Exhibit". Classifying each one separately
# costs a model call per edition and leaves this year's proceedings unclassified until
# the job has seen it. The key folds the editions of one series together by removing the
# two parts of a title that change from edition to edition: a 4-digit year (1900-2099)
# and an edition ordinal ("13th", "8th", "1st").
#
# This is a heuristic and it has a ceiling. It does not fold:
#   * editions numbered any other way ("Communication Yearbook 8", "Volume 12",
#     "Part II", "Second Edition", a two-digit year such as "CHI '23");
#   * a year glued to a word ("WOOT2016");
#   * a series that was renamed, or whose acronym moved within the title.
# Those stay separate keys and each is classified on its own, which costs a model call
# and nothing else. The opposite error, two unrelated venues sharing a key, needs two
# titles that differ only by a year or an ordinal. Folding the 3,373 title venues
# classified on 2026-11-16 gives 3,123 keys; one of them had been given two different
# subfields, and that was a single conference series labelled twice.
#
# Accents are folded as well as case because the table's collation
# (utf8mb4_unicode_ci) already treats them as equal: a key Python thought was new would
# otherwise collide with a stored one on insert, and be sent to the model again every
# night.
#
# A key has to be a NAME. Two kinds of title leave something behind that is not one, and
# both would make unrelated venues share a row:
#   * a title that is only a year and punctuation. "(2023)" folds to "( )", "2019/2020"
#     to "/", "2023-2024" to "-", and the first one classified would then answer for
#     every other title of that shape. So a key with no letter in it is the empty key.
#   * a placeholder a source writes where it has no title (_NO_TITLE). The research
#     prototype sent the literal "undefined" to the model and got back "General Science &
#     Technology", the subfield that holds Nature and Science.
# The empty key is never looked up, never classified and never stored.
_YEAR = re.compile(r"\b(?:19|20)\d{2}\b")
_ORDINAL = re.compile(r"\b\d+(?:st|nd|rd|th)\b")
_NO_TITLE = frozenset({"undefined", "null", "none", "nan", "n/a", "unknown"})
VENUE_KEY_MAX = 512  # venue_subfield_inferred.venue_key VARCHAR(512)


def venue_key(title):
    """Container title -> the key `venue_subfield_inferred` stores it under: lowercased,
    accents folded, 4-digit years and edition ordinals removed, whitespace collapsed.
    '' for anything that is not a string, has no letter left, or is a placeholder for a
    missing title."""
    if not isinstance(title, str):
        return ""
    s = unicodedata.normalize("NFKD", title)
    s = "".join(ch for ch in s if not unicodedata.combining(ch)).lower()
    s = " ".join(_ORDINAL.sub(" ", _YEAR.sub(" ", s)).split())[:VENUE_KEY_MAX]
    if s in _NO_TITLE or not any(ch.isalpha() for ch in s):
        return ""
    return s


def issn_key(issn):
    """ISSN -> 8 characters, no hyphen, uppercase; None when it is not an ISSN.
    authorship_review.issn is hyphenated, journal_science_metrix carries both forms."""
    if not isinstance(issn, str):
        return None
    s = "".join(ch for ch in issn.upper() if ch.isalnum())
    return s if len(s) == 8 else None


# ---- venue -> subfield lookup -------------------------------------------------
def build_venue_lookup(journal_rows, inferred_rows):
    """One dict, keys 'issn:<8 chars>' and 'title:<venue_key>', value the subfield.

    `journal_rows` are (smsid, issn, eissn, subfield) from journal_science_metrix;
    `inferred_rows` are (venue_key, subfield) from venue_subfield_inferred.

    Where two journal rows claim the same ISSN the LOWEST smsid wins. Science-Metrix's own
    rows sit below 800001 and inferred journals at or above it, so a category the
    publisher list assigns always beats one a model guessed from the title.

    Subfields are stripped (`subfield_name`): the source list spells one of them with a
    trailing space, and the department table must count it as one subfield, not two."""
    lookup = {}
    for _smsid, issn, eissn, subfield in sorted(journal_rows, key=lambda r: r[0]):
        subfield = subfield_name(subfield)
        if not subfield:
            continue
        for raw in (issn, eissn):
            k = issn_key(raw)
            if k:
                lookup.setdefault("issn:" + k, subfield)
    for key, subfield in inferred_rows:
        subfield = subfield_name(subfield)
        if key and subfield:
            lookup.setdefault("title:" + key, subfield)
    return lookup


def subfield_name(subfield):
    """A subfield as this module and infer_venue_subfield.py compare it: stripped, ''
    when there is none. ReCiter's ScienceMetrix.json spells 'Econometrics ' with a
    trailing space on all 22 of its rows; MariaDB's padded comparison hides that, Python's
    does not."""
    return subfield.strip() if isinstance(subfield, str) else ""


def load_venue_lookup(conn):
    """`build_venue_lookup` over reciterdb. Before migration v3.14 the inferred table
    does not exist; the lookup is then the journals alone, which is what it was worth
    before any container was classified."""
    journals = conn.execute(text(
        "SELECT smsid, issn, eissn, subfield FROM journal_science_metrix "
        "WHERE subfield IS NOT NULL AND subfield <> ''")).all()
    inferred = []
    if inferred_table_ready(conn):
        inferred = conn.execute(text(
            "SELECT venue_key, subfield FROM venue_subfield_inferred")).all()
    return build_venue_lookup(journals, inferred)


def inferred_table_ready(conn):
    """Does venue_subfield_inferred exist (migration v3.14 applied)?"""
    return bool(conn.execute(text(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() "
        "AND table_name = 'venue_subfield_inferred'")).scalar())


def subfield_for(lookup, issn, title):
    """The venue's subfield, or None when it has no category.

    A venue with an ISSN is looked up by ISSN and nothing else, as in the replay the
    weights were measured on. Its title is not a fallback: an unmapped ISSN is classified
    under that ISSN by infer_venue_subfield.py, and until then the venue is unknown and
    contributes no term."""
    if not lookup:
        return None
    k = issn_key(issn)
    if k:
        return lookup.get("issn:" + k)
    key = venue_key(title)
    return lookup.get("title:" + key) if key else None


# ---- department x subfield ----------------------------------------------------
class DeptVenueFit:
    """How much more a department publishes in a subfield than its campus does.

        fit(d, s) = ln( (n_ds + ALPHA * p_s) / (n_d + ALPHA) / p_s )

    n_ds = the department's training rows in the subfield, n_d = all of its training
    rows, p_s = the campus-wide share of the subfield. ALPHA pseudo-rows at the campus
    share keep a small department from reading as certain. p_s is add-one smoothed,
    (n_s + 1) / (N + S + 1) with S the number of subfields seen, because open rows land in
    subfields no accepted row has reached yet and an unsmoothed share of zero has no
    logarithm.

    Zero means the department publishes there at the campus rate. A department with
    MIN_DEPT rows and none in a subfield scores ln(5/25) = -1.61 there whatever the
    subfield, so "never published in this field" is always past the penalty threshold.

    Built once per campus: WCM and Ithaca each compare against their own background, and
    a physics department's fit for an optics journal means something different on a
    medical campus.

    THE SCORED PERSON'S OWN ROWS ARE LEFT OUT. `fit(dept, subfield, cwid)` subtracts the
    training rows that are that person's from n_ds and n_d before anything else, the
    MIN_DEPT floor included, so the answer is what the REST of the department publishes.
    Every figure the term was sized on was measured this way (venue_fit_go_nogo.py:
    "the scored person's own training rows are subtracted, so the fit is the
    department's, not theirs"), and it is what stops a person's accepted papers from
    vouching for their own next one: a department whose 20 rows are all one author's
    gives that author no fit at all. p_s is the campus share and is not adjusted, as in
    that script. With no cwid nothing is subtracted.

    The rows are (dept, subfield, n) or (dept, subfield, n, cwid). Both sides go through
    `cornell_hr_feed.venue_dept`: the `top_dept` the producer stored and the `dept` the
    roster carries today are folded to one name per department, so rows stored under any
    of a department's spellings pool, and a section too small to score borrows its parent
    unit's rows. A renamed department that the table there does not know starts again
    from no rows. A person's own rows are found by cwid, case-folded, under the folded
    department they were accepted in, so they still come off."""

    def __init__(self, rows):
        self.n_ds, self.n_d, self.n_s = Counter(), Counter(), Counter()
        self.own_ds, self.own_d = Counter(), Counter()
        for dept, subfield, n, *who in rows:
            if not dept or not subfield or not n:
                continue
            dept = venue_dept(dept)
            self.n_ds[(dept, subfield)] += n
            self.n_d[dept] += n
            self.n_s[subfield] += n
            cwid = _cwid_key(who[0]) if who else ""
            if cwid:
                self.own_ds[(cwid, dept, subfield)] += n
                self.own_d[(cwid, dept)] += n
        self.total = sum(self.n_s.values())

    def fit(self, dept, subfield, cwid=None):
        """The fit, rounded to 3 places, or None when it cannot be stated: no
        department, no subfield, or a department with fewer than MIN_DEPT training rows
        (156 of 255 Ithaca departments on 2026-11-16) once `cwid`'s own are left out."""
        if not dept or not subfield:
            return None
        dept = venue_dept(dept)
        who = _cwid_key(cwid)
        n_d = self.n_d.get(dept, 0) - self.own_d.get((who, dept), 0)
        if n_d < MIN_DEPT:
            return None
        n_ds = self.n_ds.get((dept, subfield), 0) - self.own_ds.get((who, dept, subfield), 0)
        p = (self.n_s.get(subfield, 0) + 1) / (self.total + len(self.n_s) + 1)
        return round(math.log((n_ds + ALPHA * p) / (n_d + ALPHA) / p), 3)

    @staticmethod
    def term(fit):
        """`venue_term`, reachable from the table itself so that identity_index, which
        holds a table or None, needs no import of this module to score a candidate."""
        return venue_term(fit)


def _cwid_key(cwid):
    """cwid as `own_*` is keyed: top_cwid's collation is case-insensitive."""
    return str(cwid).strip().lower() if cwid else ""


# The training set: authorships a curator accepted as proposed where the byline spelled
# the person's given name out and nobody else on the campus shared it. Those are the rows
# whose attribution owes nothing to the venue, which is what keeps the table from simply
# learning its own output back. It does not make them error-free: most were bulk-accepted
# and their error rate has not been sampled. top_cwid is the accepted person (an accept
# takes the proposal as it stands), kept so `fit` can leave a person's own rows out.
_TRAINING_SQL = (
    "SELECT top_dept, top_cwid, issn, journal, COUNT(*) AS n FROM authorship_review "
    "WHERE status = 'accepted' AND single_candidate = 1 AND top_given_match = 'full' "
    "AND source IN ('scopus', 'openalex') AND top_dept IS NOT NULL AND top_dept <> '' "
    "AND {campus} GROUP BY top_dept, top_cwid, issn, journal")
# authorship_review has no campus column. Every Cornell person-type label starts with
# "Cornell" and no WCM label does (identity_index asserts the two sets are disjoint).
_ITHACA_ROWS = "top_person_type LIKE 'Cornell%'"
_WCM_ROWS = "(top_person_type IS NULL OR top_person_type NOT LIKE 'Cornell%')"


def load_dept_fit(conn, campus, lookup=None):
    """The campus's `DeptVenueFit`, from its accepted single-candidate full-given-name
    Scopus and OpenAlex rows. Each row's venue is resolved through `lookup` (loaded here
    when not passed); a row whose venue has no category teaches nothing and is left out."""
    from identity_index import CAMPUS_ITHACA   # lazy: identity_index imports this module
    if lookup is None:
        lookup = load_venue_lookup(conn)
    where = _ITHACA_ROWS if campus == CAMPUS_ITHACA else _WCM_ROWS
    rows = []
    for dept, cwid, issn, journal, n in conn.execute(
            text(_TRAINING_SQL.format(campus=where))):
        subfield = subfield_for(lookup, issn, journal)
        if subfield:
            rows.append((dept, subfield, n, cwid))
    return DeptVenueFit(rows)


# ---- confidence term ------------------------------------------------------------
def venue_term(fit):
    """The bounded term `identity_index._confidence` adds: +VENUE_BONUS when the venue
    fits the department (fit >= 1), -VENUE_PENALTY when it clearly does not (fit < -1),
    nothing in between or when the fit is unknown.

    Measured on the spelled-out-mismatch class: precision 89.3% at fit >= 1 (n=1,018)
    and 16.5% below -1 (n=649). The two weights are the ones the re-rank replay was run
    with (88 top picks fixed, 17 broken); +0.10 / -0.10 fixed 67 and broke 17, and larger
    weights changed nothing further."""
    if fit is None:
        return 0.0
    if fit >= FIT_BONUS_AT:
        return VENUE_BONUS
    if fit < FIT_PENALTY_BELOW:
        return -VENUE_PENALTY
    return 0.0


# ---- offline self-test ----------------------------------------------------------
def _selftest():
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  [{'OK' if cond else '** FAIL'}] {label}")

    # --- key normalisation
    check("two editions of one conference share a key ('CLEO 2023' / 'CLEO 2024')",
          venue_key("CLEO 2023") == venue_key("CLEO 2024") == "cleo")
    check("an edition ordinal is removed ('39th' / '40th Aerospace Sciences Meeting')",
          venue_key("39th Aerospace Sciences Meeting and Exhibit")
          == venue_key("40th Aerospace Sciences Meeting and Exhibit")
          == "aerospace sciences meeting and exhibit")
    check("year and ordinal together, the year in two places",
          venue_key("2023 Conference on Lasers and Electro Optics Europe CLEO Europe Eqec 2023")
          == "conference on lasers and electro optics europe cleo europe eqec"
          and venue_key("Websci 2020 Proceedings of the 12th ACM Conference on Web Science")
          == "websci proceedings of the acm conference on web science")
    check("1st / 2nd / 3rd are ordinals too",
          venue_key("Methods for General and Molecular Microbiology 3rd Edition")
          == "methods for general and molecular microbiology edition")
    check("case, accents and runs of whitespace fold",
          venue_key("  Revue  d'Études\tÉconomiques ") == "revue d'etudes economiques")
    check("a word that only looks like an ordinal or a year is kept",
          venue_key("Hagia Sophia in the Long Nineteenth Century")
          == "hagia sophia in the long nineteenth century"
          and venue_key("Communication Yearbook 8") == "communication yearbook 8"
          and venue_key("Route 1066 and the 30000 Islands") == "route 1066 and the 30000 islands")
    check("the documented ceiling: a year glued to a word is not removed",
          venue_key("WOOT2016") == "woot2016")
    check("nothing left, or not a string, is the empty key",
          venue_key("2024") == "" and venue_key(None) == "" and venue_key(float("nan")) == "")
    check("a title that is only years and punctuation has no key: unrelated venues "
          "must not meet under '( )', '/' or '-'",
          all(venue_key(t) == "" for t in ("(2023)", "2019/2020", "2023-2024", "[2022]",
                                           "2023.", "13th", "- / -")))
    check("...a title needs one letter, in any script, and digits beside it are kept",
          venue_key("4OR") == "4or" and venue_key("2D Materials") == "2d materials"
          and venue_key("物理学报 2024") == "物理学报")
    check("a placeholder for a missing title is not a venue, with or without a year",
          all(venue_key(t) == "" for t in ("undefined", "Undefined 2024", " NULL ", "None",
                                           "N/A", "unknown", "NaN"))
          and venue_key("Undefined Behavior Workshop") == "undefined behavior workshop")
    check("the key fits the column", len(venue_key("x" * 600)) == VENUE_KEY_MAX)
    check("ISSN key: hyphenated and bare forms agree, anything else is None",
          issn_key("0732-183X") == issn_key("0732183x") == "0732183X"
          and issn_key("bad") is None and issn_key(None) is None and issn_key("") is None)

    # --- lookup precedence
    lookup = build_venue_lookup(
        [(800001, "1111-1111", None, "Inferred Subfield"),      # listed first, higher smsid
         (100001, "1111-1111", "2222-2222", "Optics"),
         (100002, "3333-3333", None, None),                      # no subfield: not a mapping
         (800002, "4444-4444", None, "Law")],
        [("cleo", "Optoelectronics & Photonics"), ("", "Law"), ("some book", None)])
    check("journal row with the lowest smsid wins an ISSN (Science-Metrix over inferred)",
          lookup["issn:11111111"] == "Optics")
    check("eISSN resolves as well as ISSN, hyphenated or not",
          subfield_for(lookup, "2222-2222", None) == "Optics"
          and subfield_for(lookup, "22222222", "ignored") == "Optics")
    check("an inferred journal (smsid >= 800001) resolves when nothing else claims the ISSN",
          subfield_for(lookup, "4444-4444", None) == "Law")
    check("a journal row with no subfield, and an empty key or subfield, map nothing",
          "issn:33333333" not in lookup and "title:" not in lookup
          and "title:some book" not in lookup)
    check("no ISSN -> the normalised title",
          subfield_for(lookup, None, "CLEO 2025") == "Optoelectronics & Photonics"
          and subfield_for(lookup, "", "CLEO 2025") == "Optoelectronics & Photonics")
    check("an ISSN takes precedence over the title: an unmapped ISSN is unknown even when "
          "the title is a known container",
          subfield_for(lookup, "9999-9999", "CLEO 2025") is None)
    junk = build_venue_lookup([(100001, "1111-1111", None, "Econometrics ")],
                              [("undefined", "General Science & Technology"), ("( )", "Law"),
                               ("cleo", " Optics ")])
    check("a stored row under a placeholder or punctuation-only key is never reached",
          subfield_for(junk, None, "undefined") is None
          and subfield_for(junk, "", "Undefined 2024") is None
          and subfield_for(junk, None, "(1999)") is None)
    check("a subfield is read stripped, from either table ('Econometrics ' is one "
          "subfield, not two)",
          subfield_for(junk, "1111-1111", None) == "Econometrics"
          and subfield_for(junk, None, "CLEO 2025") == "Optics"
          and subfield_name(None) == "" and subfield_name("  ") == "")
    check("unknown venue, empty title, empty lookup -> None",
          subfield_for(lookup, None, "Never Seen") is None
          and subfield_for(lookup, None, "2024") is None
          and subfield_for({}, "1111-1111", "CLEO") is None
          and subfield_for(None, "1111-1111", "CLEO") is None)

    # --- load_* against a stub connection (no DB)
    class _Result(list):
        def all(self):
            return list(self)

        def scalar(self):
            return self[0][0] if self else None

    class _Conn:
        def __init__(self, table_ready=True):
            self.table_ready, self.sql = table_ready, []

        def execute(self, stmt, *_a, **_kw):
            sql = str(stmt)
            self.sql.append(sql)
            if "information_schema" in sql:
                return _Result([(1 if self.table_ready else 0,)])
            if "FROM journal_science_metrix" in sql:
                return _Result([(100001, "1111-1111", None, "Optics"),
                                (100002, "5555-5555", None, "Law")])
            if "FROM venue_subfield_inferred" in sql:
                return _Result([("cleo", "Optics")])
            if "FROM authorship_review" in sql:
                return _Result([("Physics", "abc1", "1111-1111", "J Opt", 12),
                                ("Physics", "ABC1", None, "CLEO 2022", 3),
                                ("Physics", "xyz2", None, "CLEO 2022", 5),
                                ("Physics", "abc1", "9999-9999", "Unmapped", 50),
                                ("Law School", None, "5555-5555", "L Rev", 100)])
            raise AssertionError(f"unexpected SQL: {sql}")

    conn = _Conn()
    loaded = load_venue_lookup(conn)
    check("load_venue_lookup reads both tables",
          loaded == {"issn:11111111": "Optics", "issn:55555555": "Law", "title:cleo": "Optics"})
    before = _Conn(table_ready=False)
    check("...and before v3.14 reads the journals alone, without touching the absent table",
          load_venue_lookup(before) == {"issn:11111111": "Optics", "issn:55555555": "Law"}
          and not any("FROM venue_subfield_inferred" in s for s in before.sql))

    # --- the fit formula, by hand
    # Physics: 20 rows, all Optics (12 by ISSN + 8 by title; the unmapped 50 teach
    # nothing). Law School: 100 rows, all Law. N = 120, 2 subfields seen.
    #   p_optics = (20 + 1) / (120 + 2 + 1) = 21/123
    #   fit(Physics, Optics) = ln((20 + 5*21/123) / 25 / (21/123)) = ln(4.8857...) = 1.586
    #   fit(Physics, Law)    = ln((0 + 5*p) / 25 / p) = ln(0.2) = -1.609
    #   fit(Law School, Law) = ln((100 + 5*101/123) / 105 / (101/123)) = ln(1.2074...) = 0.189
    #   an unseen subfield:  p = 1/123, fit(Physics, .) = ln(0.2) again
    conn = _Conn()
    fit = load_dept_fit(conn, "cornell-ithaca")
    check("training rows whose venue has no category are left out",
          fit.n_d == {"Physics": 20, "Law School": 100} and fit.total == 120)
    check("fit(Physics, Optics) = 1.586 (hand-computed)",
          fit.fit("Physics", "Optics") == 1.586
          and fit.fit("Physics", "Optics")
          == round(math.log((20 + 5 * 21 / 123) / 25 / (21 / 123)), 3))
    check("fit(Physics, Law) = ln(0.2) = -1.609: a department with no rows in a subfield",
          fit.fit("Physics", "Law") == -1.609)
    check("fit(Law School, Law) = 0.189: a department that IS most of the campus share",
          fit.fit("Law School", "Law") == 0.189)
    check("a subfield no training row has reached is smoothed, not an error",
          fit.fit("Physics", "Mycology & Parasitology") == -1.609)
    ithaca_sql = conn.sql[-1]
    load_dept_fit(conn, "wcm", loaded)
    check("the Ithaca query keeps Cornell person types, the WCM query excludes them",
          "top_person_type LIKE 'Cornell%'" in ithaca_sql and "NOT LIKE" not in ithaca_sql
          and "NOT LIKE 'Cornell%'" in conn.sql[-1])
    check("a lookup passed in is reused, not loaded again",
          not any("FROM journal_science_metrix" in s for s in conn.sql[conn.sql.index(ithaca_sql) + 1:]))
    check("the training set is accepted, single-candidate, full given name, external only",
          all(c in _TRAINING_SQL for c in (
              "status = 'accepted'", "single_candidate = 1", "top_given_match = 'full'",
              "source IN ('scopus', 'openalex')")))

    # --- the scored person's own rows are left out
    # Physics is abc1's 15 rows (12 + 3, two spellings of the cwid) and xyz2's 5.
    #   for abc1: n_d = 20 - 15 = 5  -> under MIN_DEPT, no fit
    #   for xyz2: n_d = 20 - 5 = 15  -> under MIN_DEPT, no fit
    #   for anyone else in Physics, and with no cwid: the whole department, 1.586
    check("the training query keeps the accepted person, and counts are kept per person",
          "top_cwid" in _TRAINING_SQL.split("FROM")[0]
          and "GROUP BY top_dept, top_cwid, issn, journal" in _TRAINING_SQL
          and fit.own_d == {("abc1", "Physics"): 15, ("xyz2", "Physics"): 5})
    check("a person whose own rows are what lifts the department over 20 gets no fit; "
          "a colleague with none, or a call with no cwid, gets the department's",
          fit.fit("Physics", "Optics", "abc1") is None
          and fit.fit("Physics", "Optics", "ABC1 ") is None
          and fit.fit("Physics", "Optics", "xyz2") is None
          and fit.fit("Physics", "Optics", "new9") == fit.fit("Physics", "Optics") == 1.586
          and fit.fit("Physics", "Optics", None) == 1.586)
    # Chemistry: 60 rows, 40 Optics (30 of them pat7's) and 20 Law. Campus: 180 rows,
    # Optics 60, Law 120, 2 subfields -> p_optics = 61/183.
    #   whole department: ln((40 + 5p) / 65 / p)            = ln(1.9231) =  0.654
    #   without pat7:     ln((10 + 5p) / 35 / p)            = ln(1.0000) =  0.000
    #   pat7 in Law:      ln((20 + 5q) / 35 / q), q=121/183 = ln(1.0071) =  0.007
    #   pat7's rows under another department do not count against Physics
    big = DeptVenueFit([("Chemistry", "Optics", 30, "pat7"), ("Chemistry", "Optics", 10, "x"),
                        ("Chemistry", "Law", 20, "y"), ("Physics", "Optics", 20, "z"),
                        ("Law School", "Law", 100)])
    check("own rows come off both the subfield count and the department count, by hand: "
          "0.654 for the department, 0.0 without the person's 30 Optics rows",
          big.fit("Chemistry", "Optics") == 0.654 and big.fit("Chemistry", "Optics", "pat7") == 0.0
          and big.fit("Chemistry", "Optics", "pat7")
          == round(math.log((10 + 5 * 61 / 183) / 35 / (61 / 183)), 3))
    check("...in a subfield the person never published in only the department count "
          "moves, and their rows elsewhere touch no other department",
          big.fit("Chemistry", "Law", "pat7") == 0.007
          and big.fit("Chemistry", "Law", "pat7")
          == round(math.log((20 + 5 * 121 / 183) / 35 / (121 / 183)), 3)
          and big.fit("Physics", "Optics", "pat7") == big.fit("Physics", "Optics"))
    solo = DeptVenueFit([("Optics Lab", "Optics", 20, "one1"), ("Law School", "Law", 100, "l")])
    check("a department whose 20 rows are all one author's gives that author nothing, "
          "and the bonus to a namesake beside them",
          solo.fit("Optics Lab", "Optics", "one1") is None
          and venue_term(solo.fit("Optics Lab", "Optics", "two2")) == 0.15)

    # --- Cornell Tech's generic HR units pool under "Cornell Tech"
    # 12 + 6 + 4 rows under three stored names: no name reaches MIN_DEPT alone, the
    # pooled department (22) does, and it is the same table whichever name is asked.
    #   N = 122, 2 subfields -> p_optics = 23/125
    tech = DeptVenueFit([("Faculty and Research", "Optics", 12, "ra1"),
                         ("Tech Instruction", "Optics", 6, "ti2"),
                         ("Cornell Tech", "Optics", 4, "ct3"),
                         ("Law School", "Law", 100)])
    check("the three Cornell Tech unit names and 'Cornell Tech' are one department",
          tech.n_d == {"Cornell Tech": 22, "Law School": 100}
          and tech.n_ds[("Cornell Tech", "Optics")] == 22)
    check("...looked up under any of its names, with the same answer (by hand)",
          tech.fit("Cornell Tech", "Optics") is not None
          and tech.fit("Cornell Tech", "Optics") == tech.fit("Faculty and Research", "Optics")
          == tech.fit("JTC Instruction", "Optics")
          == round(math.log((22 + 5 * 23 / 125) / 27 / (23 / 125)), 3))
    check("...and a person's own rows stored under an old name still come off "
          "(ra1's 12 leave 10 < 20: no fit; ct3's 4 leave 18: no fit)",
          tech.fit("Cornell Tech", "Optics", "ra1") is None
          and tech.fit("Faculty and Research", "Optics", "ra1") is None
          and tech.fit("Cornell Tech", "Optics", "ct3") is None
          and tech.own_d == {("ra1", "Cornell Tech"): 12, ("ti2", "Cornell Tech"): 6,
                             ("ct3", "Cornell Tech"): 4})
    check("...and no other department is folded",
          DeptVenueFit([("Research", "Optics", 30)]).n_d == {"Research": 30})

    # --- the under-20 rule and missing inputs
    small = DeptVenueFit([("Tiny", "Optics", 19), ("Big", "Optics", 20), ("Big", "Law", 1)])
    check("a department with 19 training rows returns no fit; 20 does",
          small.fit("Tiny", "Optics") is None and small.fit("Big", "Optics") is not None)
    check("an unknown department, a missing department or a missing subfield -> None",
          small.fit("Nobody", "Optics") is None and small.fit("", "Optics") is None
          and small.fit(None, "Optics") is None and small.fit("Big", None) is None
          and small.fit("Big", "") is None)
    check("rows with no department, subfield or count are ignored; an empty table fits nothing",
          DeptVenueFit([("", "Optics", 5), ("Big", None, 5), ("Big", "Optics", 0)]).total == 0
          and DeptVenueFit([]).fit("Big", "Optics") is None)

    # --- venue_term thresholds
    check("venue_term: +0.15 at fit >= 1, inclusive",
          venue_term(1.0) == 0.15 and venue_term(1.586) == 0.15)
    check("venue_term: -0.20 at fit < -1, exclusive",
          venue_term(-1.001) == -0.20 and venue_term(-1.609) == -0.20
          and venue_term(-1.0) == 0.0)
    check("venue_term: nothing in between, nothing when unknown",
          venue_term(0.999) == 0.0 and venue_term(0.0) == 0.0 and venue_term(-0.5) == 0.0
          and venue_term(None) == 0.0)
    check("the fits above land where the term expects them",
          venue_term(fit.fit("Physics", "Optics")) == 0.15
          and venue_term(fit.fit("Physics", "Law")) == -0.20
          and venue_term(fit.fit("Law School", "Law")) == 0.0
          and venue_term(small.fit("Tiny", "Optics")) == 0.0)

    print("\nSELFTEST", "PASS" if ok else "FAIL")
    return ok


if __name__ == "__main__":
    if "--selftest" in sys.argv[1:]:
        sys.exit(0 if _selftest() else 1)
    sys.exit("usage: python3 venue_fit.py --selftest")
