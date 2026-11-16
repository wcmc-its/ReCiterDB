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

Imports only sqlalchemy, so the lanes that load it stay free of the PubMed-lane modules.

Usage:
  python3 venue_fit.py --selftest      # offline checks, no DB
"""
import math, re, sys, unicodedata
from collections import Counter

from sqlalchemy import text

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
_YEAR = re.compile(r"\b(?:19|20)\d{2}\b")
_ORDINAL = re.compile(r"\b\d+(?:st|nd|rd|th)\b")
VENUE_KEY_MAX = 512  # venue_subfield_inferred.venue_key VARCHAR(512)


def venue_key(title):
    """Container title -> the key `venue_subfield_inferred` stores it under: lowercased,
    accents folded, 4-digit years and edition ordinals removed, whitespace collapsed.
    '' for anything that is not a string or has nothing left."""
    if not isinstance(title, str):
        return ""
    s = unicodedata.normalize("NFKD", title)
    s = "".join(ch for ch in s if not unicodedata.combining(ch)).lower()
    s = _ORDINAL.sub(" ", _YEAR.sub(" ", s))
    return " ".join(s.split())[:VENUE_KEY_MAX]


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
    publisher list assigns always beats one a model guessed from the title."""
    lookup = {}
    for _smsid, issn, eissn, subfield in sorted(journal_rows, key=lambda r: r[0]):
        if not subfield:
            continue
        for raw in (issn, eissn):
            k = issn_key(raw)
            if k:
                lookup.setdefault("issn:" + k, subfield)
    for key, subfield in inferred_rows:
        if key and subfield:
            lookup.setdefault("title:" + key, subfield)
    return lookup


def load_venue_lookup(conn):
    """`build_venue_lookup` over reciterdb. Before migration v3.13 the inferred table
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
    """Does venue_subfield_inferred exist (migration v3.13 applied)?"""
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

    The rows are (dept, subfield, n). Department is matched as an exact string, the
    `top_dept` the producer stored against the `dept` the roster carries today, so a
    renamed department starts again from no rows."""

    def __init__(self, rows):
        self.n_ds, self.n_d, self.n_s = Counter(), Counter(), Counter()
        for dept, subfield, n in rows:
            if not dept or not subfield or not n:
                continue
            self.n_ds[(dept, subfield)] += n
            self.n_d[dept] += n
            self.n_s[subfield] += n
        self.total = sum(self.n_s.values())

    def fit(self, dept, subfield):
        """The fit, rounded to 3 places, or None when it cannot be stated: no
        department, no subfield, or a department with fewer than MIN_DEPT training rows
        (156 of 255 Ithaca departments on 2026-11-16)."""
        if not dept or not subfield:
            return None
        n_d = self.n_d.get(dept, 0)
        if n_d < MIN_DEPT:
            return None
        p = (self.n_s.get(subfield, 0) + 1) / (self.total + len(self.n_s) + 1)
        return round(math.log((self.n_ds.get((dept, subfield), 0) + ALPHA * p)
                              / (n_d + ALPHA) / p), 3)

    @staticmethod
    def term(fit):
        """`venue_term`, reachable from the table itself so that identity_index, which
        holds a table or None, needs no import of this module to score a candidate."""
        return venue_term(fit)


# The training set: authorships a curator accepted as proposed where the byline spelled
# the person's given name out and nobody else on the campus shared it. Those are the rows
# whose attribution owes nothing to the venue, which is what keeps the table from simply
# learning its own output back. It does not make them error-free: most were bulk-accepted
# and their error rate has not been sampled.
_TRAINING_SQL = (
    "SELECT top_dept, issn, journal, COUNT(*) AS n FROM authorship_review "
    "WHERE status = 'accepted' AND single_candidate = 1 AND top_given_match = 'full' "
    "AND source IN ('scopus', 'openalex') AND top_dept IS NOT NULL AND top_dept <> '' "
    "AND {campus} GROUP BY top_dept, issn, journal")
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
    for dept, issn, journal, n in conn.execute(text(_TRAINING_SQL.format(campus=where))):
        subfield = subfield_for(lookup, issn, journal)
        if subfield:
            rows.append((dept, subfield, n))
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
                return _Result([("Physics", "1111-1111", "J Opt", 12),
                                ("Physics", None, "CLEO 2022", 8),
                                ("Physics", "9999-9999", "Unmapped", 50),
                                ("Law School", "5555-5555", "L Rev", 100)])
            raise AssertionError(f"unexpected SQL: {sql}")

    conn = _Conn()
    loaded = load_venue_lookup(conn)
    check("load_venue_lookup reads both tables",
          loaded == {"issn:11111111": "Optics", "issn:55555555": "Law", "title:cleo": "Optics"})
    before = _Conn(table_ready=False)
    check("...and before v3.13 reads the journals alone, without touching the absent table",
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
