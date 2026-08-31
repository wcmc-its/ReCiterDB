#!/usr/bin/env python3
"""
Adversarial Attribution Review — Step 2: attribution gate.

PRIMARY GATE (article-first, orphan detection) — reciterdb:
  An article is ATTRIBUTED iff its pmid appears in `analysis_summary_author`
  (built from the Analysis, which holds only >=30 / accepted articles). So
  `orphans(universe_pmids)` = pmids NOT in that table = no WCM person at >=30.
  One indexed SQL query, evaluated globally across all WCM people. This is the
  gate the orphan ledger uses.

SECONDARY (per-uid scoring) — kept for RANKING orphan candidates, not gating:
  AttributionResolver recomputes the production final per (uid, pmid) from the
  S3 scoring inputs with the pinned models, used to rank how likely an orphan
  belongs to a candidate (identity-only score). reciterdb can't do this because
  orphans aren't in it.

Per-(uid,pmid) status (secondary path):

Per-pair status (authoritative, cheap — no 25GB Analysis scan):
  accepted        pmid in the uid's GoldStandard.knownpmids
  rejected        pmid in the uid's GoldStandard.rejectedpmids
  suggested_ge30  production final score >= 30 (already surfaced in PM's pending queue)
  buried          retrieved & scored but final < 30  (the Worgall case)
  absent          never retrieved for this uid
  input_unavailable   scoring input missing/cold-storage — can't confirm

"final" is recomputed from the per-user scoring input with the pinned local models
(reusing the detector): feedback users -> min(fb, io*33)*100; feedback-less users ->
identity-only*100 (their production pipeline). This matches what the storage filter
persisted, so "final >= 30" is equivalent to "present in the Analysis".

A PMID is ATTRIBUTED (gate drops it) iff some candidate is accepted or suggested_ge30.
rejected / buried / absent are NOT attribution (a reject by A doesn't make it B's;
buried/absent are exactly what we want to surface).

Usage (self-test):
  python aar_gate.py --selftest
  python aar_gate.py --pmid 42220538 --uids stw2006
"""
import argparse, json, os, sys

import boto3
import pandas as pd
from sqlalchemy import create_engine, text, bindparam

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import adversarial_attribution_review as det  # model scoring engine (step 0)
from identity_index import name_tokens        # shared name normalisation

_dyn = boto3.client("dynamodb", region_name="us-east-1")
IDENTITY_ONLY_SUFFIX = "-identityOnlyScoringInput.json"
STORAGE_THRESHOLD = det.STORAGE_THRESHOLD  # 30

ATTRIBUTED = {"accepted", "suggested_ge30"}


# ===========================================================================
# PRIMARY GATE (article-first, reciterdb) — orphan-article detection
# ===========================================================================
_ENGINE = None


def _reciterdb():
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(
            f"mysql+pymysql://{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}"
            f"@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}",
            connect_args={"connect_timeout": 15}, pool_pre_ping=True)
    return _ENGINE


def attributed_pmids(pmids):
    """Subset of pmids already attributed to some WCM person.

    Presence in analysis_summary_author == in some Analysis at >=30 / accepted.
    This is the gate: anything NOT returned is an orphan article."""
    pmids = sorted({int(p) for p in pmids})
    if not pmids:
        return set()
    stmt = text("SELECT DISTINCT pmid FROM analysis_summary_author WHERE pmid IN :ps") \
        .bindparams(bindparam("ps", expanding=True))
    found = set()
    with _reciterdb().connect() as c:
        for i in range(0, len(pmids), 1000):
            chunk = pmids[i:i + 1000]
            found.update(int(r[0]) for r in c.execute(stmt, {"ps": chunk}))
    return found


def _identifying_name(first, last):
    """(given token, surname tokens) for a name spelling, or None when the spelling
    identifies nobody: no surname, no given name, or a given name that is a bare initial
    ('J Kim' names half the Kims at WCM). This is the granularity a byline can be pinned
    to, so it is also the key the homonym census counts on."""
    f, l = name_tokens(first), name_tokens(last)
    if not f or not l or len(f[0]) < 2:
        return None
    return (f[0], l)


_AMBIGUOUS = None


def _ambiguous_names():
    """Name spellings more than one WCM person answers to, so a byline carrying one of
    them cannot be pinned to a single person. Measured live 2026-08-30: 68,539 roster rows
    yield 43,506 identifying spellings, 1,222 of them shared, and those 1,222 cover 90 of
    the 479 authorships the byline signal would otherwise suppress. 'Andrew Lee' is 8 WCM
    people, 'Arnab Ghosh' 4, 'David Chung' 3.

    Both rosters in ONE census, since `attributions` returns names from both: `person`
    (33k DynamoDB publishing names) and `identity` (35k HR legal names). Combined, not
    per-table then unioned -- a spelling that is unique inside each table but names two
    different people across them is still ambiguous, and that is 121 of the 1,222.
    0.75s, once per process; the roster is a nightly snapshot and a run is minutes.

    `person_article` is deliberately NOT censused, though it is the third name source. It
    is ReCiter's own matching OUTPUT, not a roster: a byline it once matched to two
    different people becomes an "ambiguity" no human shares ('Aaron Gupta' -> aag2010 +
    ajg9004; 'Aastha Bansal' -> fh_abansal + fredhutch_abansal, the same person under an
    external-validation cohort id). Measured, it adds 1,066 further keys -- nearly
    doubling the census off the very evidence this gate exists to audit -- to spare 32
    more rows. The roster answers who exists, which is the question being asked here."""
    global _AMBIGUOUS
    if _AMBIGUOUS is None:
        held = {}
        with _reciterdb().connect() as c:
            for cwid, first, last in c.execute(text(
                    "SELECT personIdentifier, firstName, lastName FROM person "
                    "UNION ALL "
                    "SELECT cwid, givenName, surname FROM identity")):
                key = _identifying_name(first, last)
                if key:
                    held.setdefault(key, set()).add(cwid)
        _AMBIGUOUS = frozenset(k for k, cwids in held.items() if len(cwids) > 1)
    return _AMBIGUOUS


def attributions(pmids):
    """pmid -> [(cwid, authorPosition, names), ...] for the article-level matcher (who's
    already assigned, so the ledger can mark which WCM authorships are still open).

    `names` is the ((first, last), ...) spellings that person is known by, which is what
    lets the orchestrator ask the question the cwid alone cannot -- "is this BYLINE
    already attributed, to ANYONE?" (issue #174). Three sources, all of them, because they
    disagree and the anchor case is exactly where: aer2006 publishes as 'Tony Rosen' but
    is 'Anthony Rosen' in HR, so an identity-only comparison misses the byline this was
    written for.

      person_article.articleAuthorName{First,Last}Name   the exact PubMed byline ReCiter
          matched on this very pmid. Preferred over normalising a roster name and hoping
          it lands: it reproduces 472 of the 473 known leak bylines character-for-
          character. Measured alone it catches 478 of the 479, so it carries the signal
          and the two rosters are the belt-and-braces.
      person.firstName / lastName        the DynamoDB Identity publishing name (461)
      identity.givenName / surname       the HR/LDAP LEGAL name (320 -- on its own it
          misses a third of the set, which is what 'Anthony' vs 'Tony' costs at scale)

    A spelling shared by two or more WCM people is dropped rather than returned (see
    `_ambiguous_names`): it identifies nobody, and suppressing an authorship on it would
    hide a genuine unattributed one behind a homonym.

    Aggregated per (pmid, cwid) in Python rather than left to the joins, which are not
    one-to-one -- 23 (personIdentifier, pmid) pairs are duplicated in `person_article`
    and 14 personIdentifiers in `person`, and a caller reading `attr_hits[0][0]` must not
    see the same person twice."""
    pmids = sorted({int(p) for p in pmids})
    if not pmids:
        return {}
    stmt = text("SELECT a.pmid, a.personIdentifier, a.authorPosition, "
                "       pa.articleAuthorNameFirstName, pa.articleAuthorNameLastName, "
                "       p.firstName, p.lastName, i.givenName, i.surname "
                "FROM analysis_summary_author a "
                "LEFT JOIN person_article pa ON pa.pmid = a.pmid "
                "                           AND pa.personIdentifier = a.personIdentifier "
                "LEFT JOIN person p ON p.personIdentifier = a.personIdentifier "
                "LEFT JOIN identity i ON i.cwid = a.personIdentifier "
                "WHERE a.pmid IN :ps") \
        .bindparams(bindparam("ps", expanding=True))
    # no COLLATE: every join key here is utf8mb4_unicode_ci. (authorship_review's
    # top_cwid/resolution_cwid are utf8mb4_general_ci and would need one -- this query
    # deliberately never reaches that table.)
    ambiguous = _ambiguous_names()
    per_person = {}                                    # (pmid, cwid) -> [position, names]
    with _reciterdb().connect() as c:
        for i in range(0, len(pmids), 1000):
            for pmid, cwid, pos, a_first, a_last, p_first, p_last, i_given, i_sur in \
                    c.execute(stmt, {"ps": pmids[i:i + 1000]}):
                rec = per_person.setdefault((int(pmid), cwid), [pos, []])
                for spelling in ((a_first, a_last), (p_first, p_last), (i_given, i_sur)):
                    key = _identifying_name(*spelling)
                    if key and key not in ambiguous and spelling not in rec[1]:
                        rec[1].append(spelling)
    out = {}
    for (pmid, cwid), (pos, names) in per_person.items():
        out.setdefault(pmid, []).append((cwid, pos, tuple(names)))
    return out


def orphan_pmids(pmids):
    """Orphan articles = pmids with no WCM attribution at all."""
    attr = attributed_pmids(pmids)
    return [int(p) for p in dict.fromkeys(int(x) for x in pmids) if int(p) not in attr]


def _final_scores_for_uid(uid):
    """Return (source, {pmid: final_score}). Tries feedback input, then identity-only."""
    status, _, rows = det.score_user(uid)          # feedback-identity input path
    if status == "ok":
        return "feedback", {r["pmid"]: r["final_score"] for r in rows}
    if status == "error":
        return "input_unavailable", {}             # e.g. cold-storage InvalidObjectState
    if status == "missing":
        try:
            obj = det._s3.get_object(Bucket=det.BUCKET, Key=uid + IDENTITY_ONLY_SUFFIX)
            arts = json.loads(obj["Body"].read())
        except det._s3.exceptions.NoSuchKey:
            return "none", {}
        except Exception:                          # noqa: BLE001
            return "input_unavailable", {}
        if not isinstance(arts, list) or not arts:
            return "none", {}
        df = pd.DataFrame(arts)
        pmid_key = next((k for k in det.PMID_KEYS if k in df.columns), None)
        if pmid_key is None:
            return "none", {}
        try:
            io_cal = det._score(df, det.IO_MODEL, det.IO_SCALER, det.IO_CALIB,
                                det.IDENTITY_ONLY_BASE_FEATURES,
                                det.compute_derived_features_identity_only,
                                det.IDENTITY_ONLY_FEATURES)
        except Exception:                          # noqa: BLE001
            return "input_unavailable", {}
        pmids = pd.to_numeric(df[pmid_key], errors="coerce")
        scores = (io_cal * 100.0)
        return "identity_only", {int(p): float(s) for p, s in zip(pmids, scores)
                                 if pd.notna(p)}
    return "none", {}                              # empty


class AttributionResolver:
    """Caches per-uid gold standard + recomputed final scores across many lookups."""

    def __init__(self):
        self._gs = {}
        self._scores = {}

    def _gold(self, uid):
        if uid not in self._gs:
            item = _dyn.get_item(
                TableName="GoldStandard", Key={"uid": {"S": uid}},
                ProjectionExpression="knownpmids, rejectedpmids").get("Item", {})

            def to_set(field):
                return {int(x["N"]) for x in item.get(field, {}).get("L", []) if "N" in x}

            self._gs[uid] = (to_set("knownpmids"), to_set("rejectedpmids"))
        return self._gs[uid]

    def _final(self, uid):
        if uid not in self._scores:
            self._scores[uid] = _final_scores_for_uid(uid)
        return self._scores[uid]

    def status(self, uid, pmid):
        pmid = int(pmid)
        known, rejected = self._gold(uid)
        if pmid in known:
            return ("accepted", None)
        if pmid in rejected:
            return ("rejected", None)
        source, scores = self._final(uid)
        if source == "input_unavailable":
            return ("input_unavailable", None)
        if pmid in scores:
            f = scores[pmid]
            return ("suggested_ge30" if f >= STORAGE_THRESHOLD else "buried", round(f, 2))
        return ("absent", None)

    def gate(self, pmid, candidate_uids):
        per = {u: self.status(u, pmid) for u in candidate_uids}
        attributed = any(s[0] in ATTRIBUTED for s in per.values())
        return {"pmid": int(pmid), "attributed": attributed, "per_candidate": per}


def _selftest():
    r = AttributionResolver()
    # a currently-buried pmid: scored sub-threshold, in neither gold list. Derived,
    # not named -- naming one is exactly what rotted this selftest (issue #178): a
    # curator accepting or rejecting the named pmid flips its status out from under
    # the fixture. Tried over a short list of uids, in order, because the uid that
    # supplied this case originally (stw2006, the Worgall case) now has zero -- every
    # one of his scored pmids is accepted -- and a permanently-skipped case carries
    # no more signal than the permanently-red one #178 replaced.
    buried_uid = buried_pmid = None
    for candidate in ("stw2006", "meb7002", "ltr4001"):
        known_c, rejected_c = r._gold(candidate)
        _, scores_c = r._final(candidate)
        pmid = next((p for p, s in scores_c.items()
                     if s < STORAGE_THRESHOLD and p not in known_c and p not in rejected_c),
                    None)
        if pmid is not None:
            buried_uid, buried_pmid = candidate, pmid
            break

    # an accepted pmid for stw2006 (first in his knownpmids)
    known, _ = r._gold("stw2006")
    accepted_pmid = next(iter(known))

    cases = [
        ("stw2006", accepted_pmid, "accepted"),
        ("stw2006", 99999999, "absent"),
    ]
    if buried_pmid is not None:
        cases.insert(0, (buried_uid, buried_pmid, "buried"))
        print(f"buried case supplied by {buried_uid}")
    else:
        print("SKIP: no currently-buried pmid for stw2006, meb7002 or ltr4001 "
              "(every scored pmid is now accepted/rejected/>=30)")
    print(f"{'uid':9} {'pmid':10} {'expect':16} {'got':16} score")
    ok = True
    for uid, pmid, exp in cases:
        st, sc = r.status(uid, pmid)
        flag = "OK" if st == exp else "** MISMATCH"
        ok &= st == exp
        print(f"{uid:9} {pmid:<10} {exp:16} {st:16} {sc}  {flag}")
    if buried_pmid is not None:
        print("\ngate(%d,[%s]) ->" % (buried_pmid, buried_uid),
              r.gate(buried_pmid, [buried_uid]))
    print("gate(%d,[stw2006]) ->" % accepted_pmid, r.gate(accepted_pmid, ["stw2006"]))
    print("\nSELFTEST", "PASS" if ok else "FAIL")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--pmid", type=int)
    ap.add_argument("--uids", nargs="*", default=[])
    args = ap.parse_args()
    if args.selftest:
        sys.exit(0 if _selftest() else 1)
    elif args.pmid and args.uids:
        print(json.dumps(AttributionResolver().gate(args.pmid, args.uids), indent=2))
    else:
        ap.error("use --selftest or --pmid with --uids")


if __name__ == "__main__":
    main()
