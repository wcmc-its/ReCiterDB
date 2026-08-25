#!/usr/bin/env python3
"""
Identity index for the Adversarial Attribution Review lanes.

In-memory index of reciterdb `identity` keyed by normalised surname, plus the
name-normalisation helpers and the person-type precedence table that both the
PubMed matcher (`aar_matcher`) and the Scopus detector (`aar_universe_scopus`)
share. Extracted from `aar_matcher.py` so the Scopus lane can reuse the roster
WITHOUT dragging in the matcher's S3 / pinned-model dependencies — which lets the
detector run in-cluster (reciterdb CronJob) with only sqlalchemy + PyMySQL.

Behaviour-preserving: the class, helpers, and person-type table are verbatim the
ones `aar_matcher` used before the extraction.

Env: DB_USERNAME/DB_PASSWORD/DB_HOST/DB_NAME (reciterdb, read-only).
"""
import os, unicodedata

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


def _first_initial(fore, initials):
    """Author's first-name initial from ForeName, falling back to Initials."""
    for src in (fore, initials):
        n = _norm(src)
        if n:
            return n[0]
    return ""


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
        cols = ("cwid, givenName, middleName, surname, primaryAcademicDepartment, "
                "primaryAcademicDivision, primaryTitle, primaryProgram, "
                + ", ".join(_PTYPE_COLS))
        with eng.connect() as c:
            rows = c.execute(text(
                f"SELECT {cols} FROM identity "
                "WHERE surname IS NOT NULL AND surname <> ''")).mappings().all()
        return cls([cls._record(r) for r in rows])

    @staticmethod
    def _record(r):
        ptype, historical = "Other / CTSC", False
        for col, label, hist in PERSON_TYPES:
            if str(r[col]).lower() == "yes":
                ptype, historical = label, hist
                break
        given = r["givenName"] or ""
        return {
            "cwid": r["cwid"],
            "given": given, "middle": r["middleName"] or "", "surname": r["surname"] or "",
            "given_norm": _norm(given), "surname_norm": _norm(r["surname"]),
            "dept": r["primaryAcademicDepartment"] or "",
            "division": r["primaryAcademicDivision"] or "",
            "program": r["primaryProgram"] or "",
            "title": r["primaryTitle"] or "",
            "person_type": ptype, "historical": historical,
        }

    def candidates(self, last, fore=None, initials=None, affiliations=None, top_k=5):
        """Ranked candidate CWIDs for one authorship (no identity-only score yet).

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
        affil_blob = " ".join(_norm(a) for a in (affiliations or []))

        cohort = []
        for rec in pool:
            # An adopted/preferred first name is sometimes recorded in middleName
            # rather than givenName in the HR-sourced identity table (legal name in
            # givenName, adopted Western first name in middleName); PubMed/Scopus
            # bylines use the preferred name, so try middleName as an alternate
            # given-name source whenever givenName alone doesn't match.
            middle_norm = _norm(rec["middle"])
            name_sources = [n for n in (rec["given_norm"], middle_norm) if n]
            if author_init and name_sources:
                if author_given and any(n == author_given for n in name_sources):
                    given_match = "full"
                elif any(n[0] == author_init for n in name_sources):
                    given_match = "initial"
                else:
                    continue                       # initial mismatch on both fields -> not this person
            else:
                given_match = "unknown"            # no usable given on either side
            cohort.append((rec, given_match))

        cohort_size = len(cohort)
        out = []
        for rec, given_match in cohort:
            affil_match, where = self._affil_match(rec, affil_blob)
            out.append({
                "cwid": rec["cwid"],
                "name": " ".join(x for x in (rec["given"], rec["middle"], rec["surname"]) if x),
                "dept": rec["dept"], "division": rec["division"],
                "person_type": rec["person_type"], "title": rec["title"],
                "given_match": given_match,
                "affil_dept_match": affil_match, "affil_match_on": where,
                "cohort_size": cohort_size,
                "confidence": self._confidence(given_match, cohort_size, affil_match,
                                               rec["historical"]),
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
    def _confidence(given_match, cohort_size, affil_match, historical):
        base = 0.50 if given_match == "full" else 0.25 if given_match == "initial" else 0.15
        rarity = 0.40 / max(cohort_size, 1)
        affil = 0.25 if affil_match else 0.0
        hist = -0.10 if historical else 0.0
        return round(max(0.0, min(1.0, base + rarity + affil + hist)), 3)


# ---- offline self-test ------------------------------------------------------
def _selftest():
    """Builds an IdentityIndex from hand-built records (no DB) to prove the
    middleName-as-alternate-given-name fix (Judy/Hua Zhong, PMID 40681448)."""
    def rec(given, middle, surname):
        return {
            "cwid": f"{given or middle}_{surname}".lower(),
            "given": given, "middle": middle, "surname": surname,
            "given_norm": _norm(given), "surname_norm": _norm(surname),
            "dept": "", "division": "", "program": "", "title": "",
            "person_type": "Full-Time Faculty", "historical": False,
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

    ok = True
    for desc, passed in checks:
        print(f"[OK] {desc}" if passed else f"[** FAIL] {desc}")
        ok = ok and passed
    print("SELFTEST PASS" if ok else "SELFTEST FAIL")
    return ok


if __name__ == "__main__":
    import sys
    sys.exit(0 if _selftest() else 1)
