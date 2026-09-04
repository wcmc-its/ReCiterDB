#!/usr/bin/env python3
"""
One-off reconciliation pass over the open AAR queue for issues #181 and #182 --
the two staleness classes PR #180 (issue #177, NO_MATCH) does not cover.

  CLASS A (#181) -- the row's STORED top_cwid already holds the pmid, per reciterdb's
    own source of truth. The authorship is already correctly attributed; the row
    should leave the queue entirely.  Action: status='dismissed' + a distinct,
    greppable 'auto:' reason appended to note (see REASON COLUMN below) + resolved_at.
    Never touches resolution_cwid / reviewer / snooze_until. Since #186, --class-a-only
    runs CLASS A alone, unattended, from run_all.py's nightly aarCloseAttributed step
    (see T2's design note near _load_class_b_modules() below); CLASS B remains a
    manual, one-off pass only.

  CLASS B (#182, #203) -- a REPLAY of the authorship through the CURRENT matcher would
    write something DIFFERENT from what is stored. Action: refresh the same 9 producer-
    owned columns aar_sweep_stale.py (#180) nulls: top_cwid, top_name, top_person_type,
    top_dept, top_given_match, top_affil_match, top_confidence, candidate_cwids_json,
    n_candidates. Two sub-classes, by what moved:

      PICK_CHANGE  the replay proposes a different top_cwid. A curator is looking at the
        wrong NAME. Gated by given_match tier move -- stronger applied, sideways behind
        --include-sideways, weaker never.
      DRIFT_ONLY   the replay keeps the same top_cwid but writes different EVIDENCE
        about that person. Reported every run, written behind --include-drift (or
        --drift-only) -- see PRODUCER-COLUMN DRIFT below for why it is not on by default.

    Only STRONGER tier-moves (initial -> full) are applied by default. SIDEWAYS moves
    (same tier, most of the CHANGED population -- homonym-cohort reshuffles from #175
    removing a tie-break fallback) are real but low-value and high-blast-radius; they
    sit behind an explicit --include-sideways opt-in, never on by default. WEAKER moves
    (all pubmed) are NEVER applied, under any flag -- see RANKING DECISION below for why
    they exist and why writing one would be a regression, not a reconciliation.

    Full dry run, 2026-08-31, live reciterdb: CLASS A 195 (193 person_article-ACCEPTED +
    2 GoldStandard-only, see DETERMINING ATTRIBUTION); CLASS B considered=17,228
    (12,292 pubmed + 4,936 scopus; 5 rows unresolvable and untouched), UNCHANGED=14,135,
    CHANGED=2,619 (stronger=291, sideways=2,262, weaker=66), NO_MATCH=474 -- matching
    issue #182's own 474 "cross-validated byte-for-byte against #180" figure exactly,
    and its ~290/~2,251/~67 tier-move split closely (the small deltas are the same io-
    rescore run-to-run wobble #182 documents, not a discrepancy in the tool). See the
    module's own dry-run output for the current, authoritative numbers -- these are a
    point-in-time record, not a promise the next run reproduces them exactly.

PRODUCER-COLUMN DRIFT / the DRIFT_ONLY class (#186, option 2)
  #186's invariant: "an open row's proposal always reflects the current matcher, so a
  curator never acts on a suggestion the producer already knows is wrong." The producer
  writes a proposal once and can never revise it (aar_orchestrator._db_rows skips
  authorships with no candidate; aar_db.upsert only touches rows it is handed), so every
  matcher tightening strands its predecessors' output and has needed its own bespoke
  one-shot sweep: #177 (~464 rows matching nobody), #181 (195 already attributed), #182
  (2,608 showing a stale pick). This is option 2 of the two the ticket offers -- one
  periodic reconciliation pass owns all the classes -- because option 1 (have the nightly
  producer reprocess every open row) is the same pass by another name and far dearer.

  #203 is the fourth instance and the one that showed the taxonomy itself was too narrow.
  It narrows `identity_index._affil_match` to word boundaries; its own replay over all
  30,711 rows measured 3,970 rows seeing a candidate change but only 203 top picks
  moving. Both pre-#186 triggers are blind to the other ~3,767:
    - CHANGED keys solely on top_cwid inequality, and top_cwid does not move; and
    - tier_move reads given_match, which #203 never touches, so all 203 picks that DO
      move are `sideways` -- reachable only by --include-sideways, which would drag in
      ~1,584 unrelated homonym reshuffles to get at them.
  Those rows keep an inflated top_confidence (a phantom affil match is worth +0.25, the
  entire gap between a `full` and an `initial` given-name match) and render a "Dept
  match" chip the matcher knows is wrong, permanently.

  So DRIFT_ONLY = same top_cwid, but at least one producer-owned column differs from
  what the matcher would write today. Detection is aar_report_changed_picks._drift_cols
  (see the long block comment there for the exact comparison); this module consumes its
  verdict and owns the write.

  WHY IT IS OFF BY DEFAULT, despite being the lowest-risk class here (the proposed
  PERSON never changes -- only the evidence displayed about them). Not risk: VOLUME.
  #203 is the fourth matcher change to strand rows but it is not the only one that ever
  did, and nothing has ever reconciled the queue against any of them. Full pubmed-lane
  dry run, 2026-09-04, live reciterdb: considered=11,624 (1 unresolvable), UNCHANGED=
  9,594, CHANGED=1,608 (stronger=0, sideways=1,608, weaker=0 -- #201's reordered key
  emptying the weaker bucket exactly as RANKING DECISION predicts), NO_MATCH=422, and of
  the UNCHANGED, DRIFT_ONLY=5,308. Columns that moved, most to least: candidate_cwids_
  json 5,308, top_confidence 4,103, top_given_match 1,478, n_candidates 962,
  top_affil_match 254, top_person_type 129, top_dept 38, top_name 26.

  Every one of the 237 DRIFT_ONLY rows in a 398-row sample of that population traced to
  a shipped matcher change, with none left unexplained:
      125  given_match initial -> full     -- #185 (a byline middle initial, "Andrew S
                                              Lee", reaching the full tier) and #201
      190  stored candidate JSON predates #159's temporal penalty (no years_after_wcm)
       28  affil_dept_match True -> False  -- #203
       10  a roster field (dept/person_type/name) moved -- ordinary HR churn
       18  evidence moved only on a NON-top candidate
  (the buckets overlap; a row can carry several.) So the population is real, not a
  comparison bug -- but it is five generations of debt, and applying it is a one-off
  catch-up of thousands of rows, not a trickle. Hence: every dry run REPORTS the class
  and its column breakdown, and writing it takes an explicit --include-drift (or
  --drift-only), exactly like --include-sideways. run_all.py's aarReconcileDrift step
  passes --drift-only but ships with AAR_DRIFT_CADENCE off, so a human runs and reads
  the catch-up before a cron owns the steady state. The pick-changing classes stay
  manual and keep their exact previous gating.

  The scopus lane is the same story at a very different price: its reconstruction
  pre-filter flagged 3,591 of 4,926 open rows for a live Scopus GET, ~6s each, about six
  hours for the lane, against ~15 minutes for the whole pubmed lane. On a 200-row sample
  135 of 144 flagged rows were confirmed drifted or CHANGED once verified, so those are
  real rows and not pre-filter noise -- they are just expensive to confirm, which is why
  --lane exists and why the wired step runs `--lane pubmed`.

  THE TRAP, and the first thing to check if this ever starts rewriting the whole queue:
  io_score/final_score/io_source are re-read from live S3 scoring inputs the nightly
  inst-client keeps refreshing -- the RANKING DECISION section below records #182
  measuring CHANGED at 2,394 vs 2,398 fifty minutes apart on identical code. Those three
  fields are stored inside candidate_cwids_json on the pubmed lane and are deliberately
  NOT part of the drift trigger; if they were, DRIFT_ONLY would match nearly every row on
  every run. They ARE still written by _write_payload whenever a refresh fires for some
  other reason -- trigger and payload are different sets, on purpose. The stability test
  for that is two back-to-back dry runs: a materially moving DRIFT_ONLY count means an
  io-derived field has leaked into the trigger.

  Measured, 2026-09-04, two consecutive full pubmed-lane dry runs: DRIFT_ONLY 5,304 then
  5,286. Run B's set is a strict SUBSET of run A's -- 0 rows entered it -- and all 5,286
  rows in both carried a BYTE-IDENTICAL drift column list. The 18 that left were all
  resolved by a curator between the runs (checked in the DB: 0 of the 18 were still
  status='open', all rejected/assigned at 19:15-19:17Z), which is the same guarantee
  _apply_class_b's status='open' re-check enforces, showing up in the measurement. So
  the trigger is deterministic against a queue whose io scores are moving underneath it,
  which is exactly what excluding io_score/final_score/io_source is supposed to buy.

RANKING DECISION -- io-rescored (default) vs --no-io-rescore
  Issue #182 flags that the io-rescored CHANGED count wobbles run-to-run (2,394 vs
  2,398, 50 minutes apart) because `aar_matcher.IdentityOnlyScorer` reads S3 inputs the
  nightly inst-client keeps refreshing, while --no-io-rescore is exactly reproducible.
  This tool uses the io-rescored ranking (aar_report_changed_picks.py's default,
  io_rescore=True) for the APPLY path, not --no-io-rescore. Reasoning:

  aar_matcher.match_authorship() -- io-rescore included -- is not an alternative view of
  the producer's behaviour, it IS the producer's behaviour: it's the exact function
  aar_orchestrator._db_rows calls to build a candidate's `top_cwid`/`top_confidence`/etc
  at row-creation time. A "refresh the pick" tool exists to make an OPEN row's stored
  columns match what the producer would write for that authorship TODAY. Writing the
  --no-io-rescore ranking instead would write a value the producer's own upsert path
  will never itself produce -- a manufactured state with no real producer run behind
  it, which is a worse failure mode than picking-the-wrong-N-rows-on-a-given-day. Run-
  to-run variance in WHICH open rows cross the CHANGED threshold this week is an
  accepted, ordinary cost of any refresh keyed off live, moving upstream data (same as
  aar_sweep_stale.py's own re-fetch-from-PubMed step, or the producer's nightly run
  itself) -- it is not evidence the write is wrong.

  This also fully explains, and contains, the "weaker" bucket: match_authorship USED TO
  lead its sort key with the identity-only score, so "any io_score beats no score"
  however small (unrelated to #171/#173/#174) -- real production ranking behaviour, not
  a bug this tool should paper over by picking the other ranking, but not evidence the
  identity fixes did anything wrong either, and applying a weaker move would demote a
  good pick on that artifact. So: use the real ranking (io-rescored), but hard-exclude
  "weaker" from the write set regardless of flags, and show the excluded count so it's
  never silently dropped. That key now leads with the given-name tier, so the io layer
  can no longer manufacture a weaker move at all and the bucket should come back empty;
  the hard-exclusion stays as the guard it always was, and a weaker move that DOES turn
  up now means the identity data itself moved, which is worth reading before applying. --no-io-rescore stays available as a read-only diagnostic on the sibling
  aar_report_changed_picks.py (its `--no-io-rescore --check <ids>`) -- this file does
  not expose that flag itself, and never wires it to --apply.

REASON COLUMN for CLASS A's dismissal (updated for #186 -- see ticket T3)
  aar_db.py's DDL/docstring names status/resolution_cwid/reviewer/note/snooze_until as
  curator-owned, but there IS a live precedent for an AUTOMATED status='dismissed'
  write with a distinguishable reason on this exact table: aar_universe_scopus.
  recheck_open_scopus() sets status='dismissed', resolved_at=:ts, and
  note=CONCAT('auto: now in PubMed (...)') when a scopus row is auto-resolved out --
  i.e. the established convention for "system, not curator, dismissed this, and here's
  why" IS `note`, prefixed 'auto:'. An earlier revision of this tool wrote `dup_reason`
  instead, as a workaround for a since-lifted note-never-touch instruction; the PM
  Sequelize model doesn't select dup_flag/dup_reason at all, so that write was
  invisible to curators. #186 lifts the restriction: CLASS A now writes the SAME
  convention recheck_open_scopus already uses, via `note=CONCAT_WS(' | ',
  NULLIF(note, ''), :reason)` (_CLASS_A_UPDATE_SQL) so any existing curator text in
  `note` is preserved, not overwritten. dup_reason is no longer written by this path.

  Format: "auto: already attributed (#181): {cwid} already holds pmid {pmid} via
  {signal}" (_class_a_reason) where {signal} is "person_article ACCEPTED",
  "GoldStandard knownpmids", or both.

DETERMINING ATTRIBUTION (CLASS A) -- reuse, not a new check
  Two signals, both already how aar_gate.py determines attribution elsewhere in this
  pipeline, OR'd together (issue #181: "person_article ACCEPTED and/or GoldStandard
  knownpmids"):
    1. gate.attributions(pmids) -- the SAME function aar_orchestrator._already_curated
       calls as its own authoritative signal 1. Backed by analysis_summary_author,
       which reciterdb's own nightly SQL (setup/populateAnalysisSummaryTables_v2.sql)
       builds from person_article.userAssertion='ACCEPTED' (plus score>=30). This is
       the "person_article ACCEPTED" half, reused wholesale, not re-derived.
    2. AttributionResolver._gold(cwid) -- GoldStandard.knownpmids per cwid, batched via
       aar_orchestrator._batch_gold_standard (BatchGetItem, 100/call) rather than one
       GetItem per row. Catches an accept that hasn't yet propagated into
       analysis_summary_author (nightly ETL lag). Measured 2026-08-31: adds exactly 2
       rows over signal 1's 193 on the live queue (195 total) -- both real lag, not
       false positives: id 39381 (est4003/pmid 39607927) has a person_article row for
       that exact pair whose userAssertion is '' rather than 'ACCEPTED' yet; id 73290
       (slb9028/pmid 39562276) has no person_article row for that pair at all. Both
       cwids' GoldStandard.knownpmids already contain the pmid either way. This is the
       "and/or" doing real work, not a defensive no-op.
  Deliberately NOT used: AttributionResolver.status()'s score-rescore fallback
  (suggested_ge30/buried/absent). That path re-scores from S3 per uid and answers "is
  this a good pick", a different, heavier question than issue #181's "does this person
  ALREADY hold it per recorded ground truth" -- pulling it in would also require an S3
  fetch per one of ~5,132 distinct top_cwids for no evidentiary gain here.

  Only pubmed-source rows can ever match: person_article and GoldStandard.knownpmids
  are keyed on pmid, and open scopus-source rows in this table carry pmid=NULL (a
  scopus doc that resolves to a PubMed pmid is dropped from the scopus lane, not kept
  with one -- see aar_universe_scopus.run() step 2). Not hard-coded as a source='pubmed'
  filter, though -- the query is pmid IS NOT NULL, source-agnostic, and the source
  breakdown of what actually matched is printed every run rather than assumed.

CLASS A vs CLASS B COLLISION -- CLASS A wins, and no row is ever double-handled
  A row's stored top_cwid can simultaneously (a) already hold the pmid (CLASS A) and
  (b) have a replay that prefers a different top_cwid, tier-stronger (CLASS B) --
  e.g. a byline whose true author already holds the article at 100/ACCEPTED, while a
  homonym-cohort replay of the SAME byline text turns up a plausible-looking rival.
  CLASS A wins: reciterdb's own ground truth that the article is ALREADY correctly
  attributed makes "refresh the suggestion" moot -- aar_orchestrator._already_curated
  encodes exactly this priority (attribution truth is signal 1, "authoritative...full
  stop"; a matcher's ranked guess is never allowed to override it). Mechanically:
  CLASS A is computed FIRST, over the full open queue; CLASS B's write-candidate set is
  then filtered to exclude every CLASS-A id BEFORE anything is queued to write. The
  dry-run output below reports the raw overlap (rows that qualify for both, pre-
  filter) and proves the final CLASS-B write set's intersection with CLASS-A ids is
  exactly {} -- not merely asserted, counted.

CROSS-CHECK vs PR #180's NO_MATCH population
  CLASS B is built from the SAME io-rescored replay as aar_report_changed_picks.py
  (imported, not re-implemented -- see REUSE below), filtered to classification ==
  "CHANGED" (PICK_CHANGE) or to UNCHANGED-with-drift (DRIFT_ONLY). NO_MATCH rows from
  that same replay are a disjoint classification bucket by construction (UNCHANGED /
  CHANGED / NO_MATCH still partition every considered row exactly once -- #186 added a
  sub-class of UNCHANGED, not a fourth classification value) and are never part of any
  CLASS-B write candidate. The two sub-classes cannot overlap either, for the same
  reason, and the write set is asserted duplicate-free every run. The dry run below
  prints this run's own NO_MATCH and DRIFT_ONLY counts alongside the CHANGED count so
  the partition is visible, not merely claimed.

REUSE, NOT A FOURTH MATCHING PATH
  Imports aar_report_changed_picks.py's `_open_rows`, `_replay_pubmed`, `_replay_scopus`,
  `_row_result`, `_cwid_eq`, `TIER_RANK` directly -- the SAME verified replay #182's own
  measurement used, unmodified on disk. That replay's own output shape (`_row_result`)
  keeps only the trimmed old/new cwid+name+given_match+confidence fields it needs for
  its CSV -- not the full candidate dict (person_type, dept, affil_dept_match,
  candidate list) this tool needs to actually WRITE the 9 columns. Rather than adding a
  second, parallel re-implementation of the fetch+match loop (a real "fourth path",
  and a real risk of quietly drifting from what #182 measured), this module installs a
  transparent, in-process capture around aar_report_changed_picks._row_result: the
  wrapper calls straight through to the original and returns its result completely
  unchanged; it only additionally stashes the full candidate list the row's
  classification was computed from, keyed by row id. Every write this tool ever proposes
  is asserted, per row, to have `cands[0]["cwid"] == <that row's own classified
  new_cwid>` before being trusted -- a mismatch (e.g. from a future refactor of the
  sibling module's internal call shape) raises loudly instead of writing a quietly-wrong
  value.

  #186 took the upgrade path an earlier revision of this note reserved. `_row_result`
  now accepts the candidate list as its own 4th argument (it needs it anyway, to compute
  `drift`), so the capture is one passthrough on one function instead of three wrappers
  around `matcher.match_authorship`, the shared `IdentityIndex` instance's `.candidates`
  and `_row_result` -- and the ceiling that came with them (those calls firing exactly
  once per row, immediately before `_row_result`, single-threaded) is gone with them.
  The per-row assertion above stays as the tripwire it always was.

SYS.PATH FORK TRAP / COLLATION TRAP: same guards as aar_sweep_stale.py and
  aar_report_changed_picks.py -- this file's own directory must resolve last (asserted
  below, paths printed), and every cwid comparison is done in Python via the imported
  `_cwid_eq` (case-folded; authorship_review.top_cwid is utf8mb4_general_ci, identity/
  person/GoldStandard keys are utf8mb4_unicode_ci or plain Python strings) -- no SQL
  join ever crosses that collation boundary, and gate.attributions()'s own module
  docstring makes the same guarantee for its half of the CLASS A query.

Usage:
  python aar_reconcile_open.py                       # dry run: report + JSONL ledger
  python aar_reconcile_open.py --apply                # perform the writes for real
  python aar_reconcile_open.py --class-a-only --apply  # CLASS A only, no CLASS B
                                                        # modules imported (nightly, #186)
  python aar_reconcile_open.py --drift-only --apply     # CLASS-B DRIFT_ONLY rows only --
                                                        # no row's proposed PERSON changes
                                                        # (aarReconcileDrift's own cmd, #186)
  python aar_reconcile_open.py --include-drift          # (dry run) queue DRIFT_ONLY
                                                        # alongside the stronger picks
  python aar_reconcile_open.py --include-sideways      # (dry run) also queue sideways
                                                        # CLASS-B moves; still needs --apply
                                                        # to write anything
  python aar_reconcile_open.py --limit 25              # cap rows per lane (fast sanity)
  python aar_reconcile_open.py --check 70797 74858     # named-row verdicts, no full sweep
  python aar_reconcile_open.py --selftest              # offline checks, no DB/network
"""
import argparse, json, os, sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)  # must be the LAST insert(0, ...) before these imports -- see
                          # the SYS.PATH FORK TRAP note above.
import aar_db
import aar_gate as gate
import aar_orchestrator as orch          # read-only reuse of _batch_gold_standard/_compact/
                                          # _trunc -- never modified, never runs orch.main()

# Every module CLASS A trusts (the only classes --class-a-only needs) must resolve to
# THIS directory, not the stale ~/Dropbox/Projects/ReCiter Research/scripts/ copy (sys.
# path fork trap). CLASS B's own modules are trap-checked in _load_class_b_modules()
# below, at the point they are actually imported.
for _mod in (aar_db, gate, orch):
    assert os.path.dirname(os.path.abspath(_mod.__file__)) == HERE, (
        f"{_mod.__name__} resolved to {_mod.__file__}, not {HERE} -- "
        "sys.path fork trap: a stale producer copy is winning. Refusing to trust any "
        "replay result computed against it.")

# CLASS B's extra dependencies (aar_universe_scopus/aar_sweep_stale/
# aar_report_changed_picks) are NOT imported here -- see _load_class_b_modules() below
# (T2 / #186). --class-a-only never triggers that loader, so it never imports those
# three, never constructs identity_index.IdentityIndex.load()'s DB roster query or
# aar_matcher.IdentityOnlyScorer(), and never runs CLASS B's io-rescore replay (its own
# S3 reads). These module-level names stay None until then.
#
# What --class-a-only does NOT avoid: the required aar_gate/aar_orchestrator imports
# above already pull in aar_matcher, aar_universe, identity_index and
# adversarial_attribution_review, and the latter builds a boto3 S3 client and
# joblib.loads all six models at import time. So the nightly closer still pays the
# xgboost/model-load cost -- unavoidable without editing those modules. Do not describe
# this path as "no xgboost/S3 machinery"; it only skips CLASS B's *additional* work.
idxmod = uni = scop = matcher = sweep = rcp = IdentityIndex = None
REFRESH_COLS = None


def _cwid_eq(a, b):
    """Case-folded cwid comparison -- authorship_review.top_cwid is utf8mb4_general_ci,
    identity/person/GoldStandard keys are utf8mb4_unicode_ci, both case-insensitive, so a
    naive Python `==` on differently-cased-but-equal cwids would misreport a match as a
    miss. This is a deliberate, exact duplicate of aar_report_changed_picks._cwid_eq
    (checked for drift in _selftest below) -- CLASS A needs this collation-safe compare
    but must not import rcp's own module chain (aar_matcher -> adversarial_attribution_
    review) just to get it; see T2 / #186."""
    if a is None or b is None:
        return a is b
    return str(a).strip().lower() == str(b).strip().lower()


def _load_class_b_modules():
    """Lazily import CLASS B's own dependencies, on first use. Called from the default
    (both-class) run, --check, and _selftest -- but NEVER from the --class-a-only path
    (see T2 / #186's design note: CLASS A needs only the DB engine, gate.attributions,
    and _batch_gold_standard). Idempotent; sets the module globals declared above."""
    global idxmod, uni, scop, matcher, sweep, rcp, IdentityIndex, REFRESH_COLS
    if rcp is not None:
        return
    import identity_index as _idxmod
    import aar_universe as _uni
    import aar_universe_scopus as _scop
    import aar_matcher as _matcher
    import aar_sweep_stale as _sweep          # reuse NULL_COLUMNS / _SELECT_COLS / _snapshot
    import aar_report_changed_picks as _rcp   # reuse the verified replay -- see REUSE above
    from identity_index import IdentityIndex as _IdentityIndex

    for _mod in (_idxmod, _uni, _scop, _matcher, _sweep, _rcp):
        assert os.path.dirname(os.path.abspath(_mod.__file__)) == HERE, (
            f"{_mod.__name__} resolved to {_mod.__file__}, not {HERE} -- "
            "sys.path fork trap: a stale producer copy is winning. Refusing to trust "
            "any replay result computed against it.")
    # rcp's own _cwid_eq must agree with the local duplicate above -- a drift here
    # would mean the two silently disagree on the same collation-safety guarantee.
    assert _rcp._cwid_eq("ABC", "abc") == _cwid_eq("ABC", "abc") and \
           _rcp._cwid_eq(None, "x") == _cwid_eq(None, "x"), (
        "aar_report_changed_picks._cwid_eq disagrees with this module's own "
        "_cwid_eq duplicate -- one of them changed; refusing to trust CLASS B.")

    idxmod, uni, scop, matcher, sweep, rcp, IdentityIndex = (
        _idxmod, _uni, _scop, _matcher, _sweep, _rcp, _IdentityIndex)
    # The exact 9 columns #180 nulls and #182 says need refreshing -- imported, not
    # redefined, so the two tools can never silently diverge on which columns are in play.
    REFRESH_COLS = sweep.NULL_COLUMNS


# ============================================================================
# CLASS A -- already-attributed dismissal
# ============================================================================
def _class_a_candidates(engine):
    """id -> {row, via_attr, via_gold, reason} for every OPEN row whose stored top_cwid
    already holds its pmid, per gate.attributions() (person_article ACCEPTED, via
    analysis_summary_author) and/or GoldStandard.knownpmids (batched). See the module
    docstring's DETERMINING ATTRIBUTION section for why these two and not the score-
    rescore path."""
    from sqlalchemy import text
    with engine.connect() as c:
        rows = c.execute(text(
            "SELECT id, source, pmid, top_cwid, note, resolved_at "
            "FROM authorship_review "
            "WHERE status='open' AND pmid IS NOT NULL AND top_cwid IS NOT NULL"
        )).mappings().all()
    rows = [dict(r) for r in rows]
    print(f"      open rows with a pmid and a proposal (CLASS A candidate pool): "
          f"{len(rows)}", flush=True)

    pmids = sorted({r["pmid"] for r in rows})
    attr = gate.attributions(pmids)  # pmid -> [(cwid, pos, names), ...]
    print(f"      gate.attributions() resolved {len(attr)} of {len(pmids)} distinct "
          f"pmids to at least one attributed cwid", flush=True)

    cwids = sorted({r["top_cwid"] for r in rows})
    gold = orch._batch_gold_standard(cwids)
    print(f"      GoldStandard.knownpmids fetched for {len(gold)} distinct top_cwids "
          f"(batched)", flush=True)

    hits = {}
    for r in rows:
        via_attr = any(_cwid_eq(r["top_cwid"], c) for c, _pos, _names in attr.get(r["pmid"], ()))
        known, _rej = gold.get(r["top_cwid"], (set(), set()))
        via_gold = r["pmid"] in known
        if not (via_attr or via_gold):
            continue
        signal = ("person_article ACCEPTED + GoldStandard knownpmids" if via_attr and via_gold
                  else "person_article ACCEPTED" if via_attr else "GoldStandard knownpmids")
        reason = _class_a_reason(r["top_cwid"], r["pmid"], signal)
        hits[r["id"]] = {"row": r, "via_attr": via_attr, "via_gold": via_gold,
                         "reason": reason}
    return hits


def _class_a_reason(cwid, pmid, signal):
    """The 'auto:' note text for a CLASS-A dismissal -- the established convention
    (aar_universe_scopus.recheck_open_scopus precedent) PM already renders. Factored
    out so _selftest can assert the exact format without touching the DB."""
    return f"auto: already attributed (#181): {cwid} already holds pmid {pmid} via {signal}"


def _class_a_ledger_entry(hit, run_ts, applied):
    r = hit["row"]
    return {
        "id": r["id"], "class": "A", "rule": "already_attributed",
        "source": r["source"], "pmid": r["pmid"], "top_cwid": r["top_cwid"],
        "via_attr": hit["via_attr"], "via_gold": hit["via_gold"],
        "before": {"status": "open", "note": r["note"], "resolved_at": r["resolved_at"]},
        "after": {"status": "dismissed", "note_appended": hit["reason"], "resolved_at": run_ts},
        "swept_at": run_ts, "applied": applied,
    }


# The ONLY write CLASS A performs. note=CONCAT_WS(...) preserves any curator text
# already in note (the established 'auto:' convention -- aar_universe_scopus.
# recheck_open_scopus precedent, see the module docstring's REASON COLUMN section);
# the status='open' re-check makes a concurrent curator win every race. Module-level
# so _selftest can assert its shape without touching the DB.
_CLASS_A_UPDATE_SQL = ("UPDATE authorship_review SET status='dismissed', resolved_at=:ts, "
                       "note=CONCAT_WS(' | ', NULLIF(note, ''), :reason) "
                       "WHERE id=:id AND status='open'")


def _apply_class_a(engine, hits, run_ts):
    from sqlalchemy import text
    if not hits:
        return 0
    stmt = text(_CLASS_A_UPDATE_SQL)
    ids = list(hits.items())
    n = 0
    with engine.begin() as c:
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            params = [{"id": rid, "ts": run_ts, "reason": h["reason"]} for rid, h in chunk]
            # rowcount, not len(chunk): a row a curator resolved between the SELECT and
            # here is correctly skipped by the status='open' guard, and must not be counted.
            n += c.execute(stmt, params).rowcount
    return n


# ============================================================================
# CLASS B -- stale-pick refresh
# ============================================================================
# Transparent capture: stash the FULL candidate list each replayed row's classification
# was computed from, keyed by row id, without altering anything either wrapped call
# returns. See the module docstring's REUSE section.
FULL_CANDS_BY_ID = {}


def _install_capture():
    """Stash the full ranked candidate list each replayed row was classified from,
    keyed by row id, without altering what the wrapped call returns.

    Since #186 rcp._row_result takes that list as its own 4th argument, so this is a
    single pure passthrough on one function -- the upgrade the REUSE note above named
    as the way out of the old three-wrapper monkeypatch (which also wrapped
    matcher.match_authorship and the idx instance's .candidates, and depended on those
    firing exactly once per row immediately before _row_result). The per-row assertion
    in _write_payload stays as the tripwire it always was."""
    orig_row_result = rcp._row_result

    def _row_result_capture(r, new_top, source, cands=None):
        rec = orig_row_result(r, new_top, source, cands)
        FULL_CANDS_BY_ID[r["id"]] = list(cands or [])
        return rec
    rcp._row_result = _row_result_capture


def _write_payload(rec):
    """The 9-column refresh payload for one CLASS-B CHANGED record, built from the
    captured full candidate list -- asserted against what the replay itself classified
    before being trusted (see REUSE)."""
    cands = FULL_CANDS_BY_ID.get(rec["id"])
    if not cands:
        raise AssertionError(f"row {rec['id']}: no captured candidate list -- capture "
                             "wiring broke (see ponytail note in the module docstring)")
    top = cands[0]
    assert _cwid_eq(top["cwid"], rec["new_cwid"]), (
        f"row {rec['id']}: captured top candidate {top['cwid']} != replay's own "
        f"new_cwid {rec['new_cwid']} -- refusing to write a value the replay didn't "
        "actually classify. See the capture ponytail note.")
    compact = orch._compact if rec["source"] == "pubmed" else scop._compact
    trunc = orch._trunc
    return {
        "top_cwid": top["cwid"],
        "top_name": trunc(top["name"], 255),
        "top_person_type": trunc(top["person_type"], 64),
        "top_dept": trunc(top["dept"], 255),
        "top_given_match": top["given_match"],
        "top_affil_match": int(bool(top["affil_dept_match"])),
        "top_confidence": top["confidence"],
        "candidate_cwids_json": json.dumps(compact(cands)),
        "n_candidates": len(cands),
    }


def _class_b_all_changed(engine, idx, io_scorer, limit=None, lane="both"):
    """Replay every open row (both lanes, unfiltered by CLASS A) through the CURRENT
    matcher, io-rescore on (see RANKING DECISION), and return the full CHANGED list
    plus per-lane NO_MATCH/considered totals -- unfiltered, so the caller can measure
    the true CLASS A/B overlap rather than have it hidden by an upfront exclude."""
    pm_results = pm_unresolvable = sc_results = sc_unresolvable = ()
    if lane in ("both", "pubmed"):
        print("[pubmed] loading open rows ...", flush=True)
        pm_rows = rcp._open_rows(engine, "pubmed")
        print(f"      {len(pm_rows)} open pubmed rows", flush=True)
        pm_results, pm_unresolvable = rcp._replay_pubmed(pm_rows, idx, io_scorer, limit,
                                                          io_rescore=True)
    else:
        print("[pubmed] skipped (--lane scopus)", flush=True)

    if lane in ("both", "scopus"):
        print("\n[scopus] loading open rows ...", flush=True)
        sc_rows = rcp._open_rows(engine, "scopus")
        print(f"      {len(sc_rows)} open scopus rows", flush=True)
        sc_results, sc_unresolvable = rcp._replay_scopus(sc_rows, idx, limit)
    else:
        print("\n[scopus] skipped (--lane pubmed)", flush=True)
    pm_results, sc_results = list(pm_results), list(sc_results)
    pm_unresolvable, sc_unresolvable = list(pm_unresolvable), list(sc_unresolvable)

    all_results = pm_results + sc_results
    by_cls = {}
    for x in all_results:
        by_cls.setdefault(x["classification"], []).append(x)
    return {
        "pm_results": pm_results, "sc_results": sc_results,
        "pm_unresolvable": pm_unresolvable, "sc_unresolvable": sc_unresolvable,
        "all_results": all_results, "by_cls": by_cls,
    }


def _class_b_write_set(changed, class_a_ids, include_sideways):
    """-> (write_candidates, weaker_excluded, sideways_excluded, overlap_with_class_a).
    weaker is excluded unconditionally; sideways only ships when include_sideways is
    set; anything whose id is in class_a_ids is excluded regardless (CLASS A wins --
    see the module docstring)."""
    overlap = [r for r in changed if r["id"] in class_a_ids]
    pool = [r for r in changed if r["id"] not in class_a_ids]
    weaker = [r for r in pool if r.get("tier_move") == "weaker"]
    sideways = [r for r in pool if r.get("tier_move") == "sideways"]
    stronger = [r for r in pool if r.get("tier_move") == "stronger"]
    write = list(stronger) + (list(sideways) if include_sideways else [])
    return write, weaker, sideways, overlap


def _drift_rows(results):
    """The DRIFT_ONLY population: replayed rows whose top PICK is unchanged but whose
    stored producer columns are not what the current matcher would write. Deliberately
    a filter over the SAME replay records the pick-change classes come from -- one
    replay, one classification pass, no second matching path."""
    return [r for r in results
            if r["classification"] == "UNCHANGED" and r.get("drift")]


def _drift_write_set(drift, class_a_ids, include_drift):
    """-> (write_candidates, overlap_with_class_a). Same collision rule as
    _class_b_write_set: CLASS A wins, so an already-attributed row is dismissed rather
    than repicked, whatever its columns say. Separate from _class_b_write_set because
    DRIFT_ONLY has no tier_move to gate on -- the top pick is the same person; only the
    evidence displayed about them is stale."""
    overlap = [r for r in drift if r["id"] in class_a_ids]
    pool = [r for r in drift if r["id"] not in class_a_ids]
    return (list(pool) if include_drift else []), overlap


def _drift_col_counts(drift):
    counts = {}
    for r in drift:
        for c in r.get("drift") or ():
            counts[c] = counts.get(c, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: -kv[1]))


def _full_before_snapshots(engine, ids):
    """Real pre-write values for all 9 refresh columns, for the given row ids -- not
    just the 4 fields aar_report_changed_picks._row_result happens to carry (old_cwid/
    old_name/old_given_match/old_confidence). Reuses aar_sweep_stale's own column list
    and snapshot helper so the ledger's 'before' is a genuine, reversible record.

    Chunked since #186: DRIFT_ONLY is a thousands-of-rows class, not the low hundreds
    the tier-move classes are, and a single expanding IN list that wide is a needlessly
    large statement."""
    from sqlalchemy import text, bindparam
    ids = list(ids)
    if not ids:
        return {}
    cols = ", ".join(["id"] + sweep.NULL_COLUMNS)
    stmt = text(f"SELECT {cols} FROM authorship_review WHERE id IN :ids") \
        .bindparams(bindparam("ids", expanding=True))
    out = {}
    with engine.connect() as c:
        for i in range(0, len(ids), 1000):
            for r in c.execute(stmt, {"ids": ids[i:i + 1000]}).mappings().all():
                out[r["id"]] = sweep._snapshot(r)
    return out


def _class_b_ledger_entry(rec, run_ts, applied, before):
    payload = _write_payload(rec)
    return {
        # `rule` is the greppable class: a tier_move for a pick change, "drift" for a
        # DRIFT_ONLY refresh (same person, stale evidence), with the columns that
        # actually moved recorded alongside so the ledger says WHY, not just what.
        "id": rec["id"], "class": "B", "rule": rec.get("tier_move") or "drift",
        "drift_cols": rec.get("drift") or None,
        "source": rec["source"], "pmid": rec["pmid"], "external_id": rec["external_id"],
        "wcm_author": rec["wcm_author"],
        "before": before, "after": payload,
        "swept_at": run_ts, "applied": applied,
    }


def _class_b_update_sql():
    """CLASS B's only write. Like _CLASS_A_UPDATE_SQL it re-checks status='open', so a
    row a curator touched between the SELECT and here is skipped, not overwritten --
    the guarantee that matters most now that DRIFT_ONLY makes this a thousands-of-rows
    nightly pass rather than a hand-reviewed one-off. Factored out of _apply_class_b so
    _selftest can assert that shape without a DB."""
    sets = ", ".join(f"{c}=:{c}" for c in REFRESH_COLS)
    return f"UPDATE authorship_review SET {sets} WHERE id=:id AND status='open'"


def _apply_class_b(engine, entries, run_ts):
    from sqlalchemy import text
    if not entries:
        return 0
    stmt = text(_class_b_update_sql())
    n = 0
    with engine.begin() as c:
        for i in range(0, len(entries), 500):
            chunk = entries[i:i + 500]
            params = []
            for e in chunk:
                p = dict(e["after"])
                p["id"] = e["id"]
                params.append(p)
            # rowcount, not len(chunk): a row a curator resolved between the SELECT and
            # here is correctly skipped by the status='open' guard, and must not be counted.
            n += c.execute(stmt, params).rowcount
    return n


def _write_ledger(path, entries):
    with open(path, "w") as fh:
        for e in entries:
            fh.write(json.dumps(e, default=str) + "\n")


# ============================================================================
# --check <ids>
# ============================================================================
def _check_ids(engine, idx, io_scorer, ids):
    _load_class_b_modules()   # --check always reports both CLASS A and CLASS B verdicts
    from sqlalchemy import text
    rows = [dict(r) for r in engine.connect().execute(text(
        "SELECT id, source, pmid, top_cwid, top_name, top_given_match, top_confidence, "
        "status FROM authorship_review WHERE id IN :ids"),
        {"ids": tuple(ids)}).mappings()]
    by_id = {r["id"]: r for r in rows}

    pmids = sorted({r["pmid"] for r in rows if r["pmid"]})
    attr = gate.attributions(pmids) if pmids else {}
    cwids = sorted({r["top_cwid"] for r in rows if r["top_cwid"]})
    gold = orch._batch_gold_standard(cwids) if cwids else {}

    # rcp._open_rows only returns status='open' rows; a --check id that is no longer
    # open (already resolved elsewhere) simply won't appear here, and is reported as
    # such -- "status != open" -- by the per-id loop below, with no replay attempted.
    pm_rows = [r for r in rcp._open_rows(engine, "pubmed") if r["id"] in by_id]
    sc_rows = [r for r in rcp._open_rows(engine, "scopus") if r["id"] in by_id]

    pm_res, pm_unresolv = rcp._replay_pubmed(pm_rows, idx, io_scorer) if pm_rows else ([], [])
    sc_res, sc_unresolv = rcp._replay_scopus(sc_rows, idx) if sc_rows else ([], [])
    b_by_id = {r["id"]: r for r in pm_res + sc_res}

    print(f"\n==== --check verdicts for {ids} ====\n")
    for rid in ids:
        row = by_id.get(rid)
        if row is None:
            print(f"id={rid}: NOT FOUND in authorship_review")
            continue
        if row["status"] != "open":
            print(f"id={rid}: status={row['status']} (not 'open' -- out of scope for "
                  "this tool, shown for context only)")
            continue
        via_attr = any(_cwid_eq(row["top_cwid"], c)
                       for c, _p, _n in attr.get(row["pmid"], ())) if row["pmid"] else False
        known, _r = gold.get(row["top_cwid"], (set(), set()))
        via_gold = bool(row["pmid"]) and row["pmid"] in known
        class_a = via_attr or via_gold
        b_rec = b_by_id.get(rid)
        print(f"id={rid}  source={row['source']}  pmid={row['pmid']}")
        print(f"  stored : {row['top_cwid']} / {row['top_name']} / "
              f"{row['top_given_match']} / {row['top_confidence']}")
        if b_rec:
            print(f"  replay : {b_rec.get('new_cwid')} / {b_rec.get('new_name')} / "
                  f"{b_rec.get('new_given_match')} / {b_rec.get('new_confidence')}"
                  f"  [{b_rec['classification']}"
                  f"{', ' + b_rec['tier_move'] if b_rec.get('tier_move') else ''}]")
        print(f"  CLASS A (already-attributed): {class_a}"
              f"{' via_attr' if via_attr else ''}{' via_gold' if via_gold else ''}")
        if class_a:
            verdict = "DISMISS (class A wins)"
        elif b_rec and b_rec["classification"] == "CHANGED" and b_rec.get("tier_move") == "stronger":
            verdict = "REFRESH -- CLASS B stronger (applied by default)"
        elif b_rec and b_rec["classification"] == "CHANGED" and b_rec.get("tier_move") == "sideways":
            verdict = "REFRESH -- CLASS B sideways (needs --include-sideways)"
        elif b_rec and b_rec["classification"] == "CHANGED" and b_rec.get("tier_move") == "weaker":
            verdict = "NO ACTION -- CLASS B weaker (never applied, io-rescore tie-break artifact)"
        elif b_rec and b_rec["classification"] == "NO_MATCH":
            verdict = "NO ACTION -- NO_MATCH (belongs to PR #180 / aar_sweep_stale.py)"
        elif b_rec and b_rec.get("drift"):
            verdict = ("REFRESH -- DRIFT_ONLY, same top_cwid, stale columns "
                       f"{b_rec['drift']} (needs --include-drift or --drift-only)")
        else:
            verdict = "NO ACTION -- UNCHANGED"
        print(f"  VERDICT: {verdict}\n")


# ============================================================================
# driver
# ============================================================================
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="perform the writes for real; default is a read-only dry run")
    ap.add_argument("--class-a-only", action="store_true",
                    help="run CLASS A only and skip CLASS B entirely -- no identity "
                         "roster load, no matcher.IdentityOnlyScorer, no io-rescore "
                         "replay, and none of CLASS B's own module imports (T2 / #186). "
                         "This is what run_all.py's nightly aarCloseAttributed step runs.")
    ap.add_argument("--include-sideways", action="store_true",
                    help="also queue CLASS-B SIDEWAYS tier moves (same-tier homonym "
                         "reshuffles, ~2,251 of the ~2,608 CHANGED population). Off by "
                         "default -- see the module docstring. STRONGER moves are "
                         "always queued; WEAKER moves are never queued under any flag.")
    ap.add_argument("--include-drift", action="store_true",
                    help="also queue CLASS-B DRIFT_ONLY rows: same top_cwid, but at "
                         "least one stored producer column is no longer what the "
                         "matcher would write. Off by default for the same reason "
                         "--include-sideways is -- not risk (the proposed PERSON never "
                         "changes) but VOLUME: measured 2026-09-04 it is roughly three "
                         "fifths of the open queue, five matcher generations of "
                         "never-reconciled backlog. Every dry run REPORTS the class and "
                         "its column breakdown whether or not this flag is set.")
    ap.add_argument("--drift-only", action="store_true",
                    help="CLASS B queues DRIFT_ONLY rows and nothing else -- no "
                         "stronger, no sideways, no weaker, i.e. no row's proposed "
                         "PERSON is ever changed. Implies --include-drift. This is what "
                         "run_all.py's aarReconcileDrift step runs; the pick-changing "
                         "classes stay manual.")
    ap.add_argument("--ledger", default=os.path.join(HERE, "aar_reconcile_open_ledger.jsonl"),
                    help="JSONL before/after output path (written on every run, "
                         "dry-run included)")
    ap.add_argument("--lane", choices=("both", "pubmed", "scopus"), default="both",
                    help="restrict the CLASS-B replay to one lane. The two are very "
                         "differently priced: the pubmed lane is a batched efetch plus "
                         "a threaded identity-only warm-up (~15 min for the whole open "
                         "queue), while the scopus lane needs one live Scopus GET per "
                         "flagged row -- 3,591 of 4,926 open rows on the 2026-09-04 run, "
                         "measured at ~6s each, so about six hours. CLASS A is pure "
                         "SQL/DynamoDB and always runs in full, whatever this is set to.")
    ap.add_argument("--limit", type=int, default=None,
                    help="cap rows considered per lane in the CLASS-B replay, before "
                         "any network call (sanity-check runs; does not limit CLASS A, "
                         "which is pure SQL/DynamoDB and cheap at full scale)")
    ap.add_argument("--check", type=int, nargs="*", default=None,
                    help="only report verdicts for these specific authorship_review "
                         "ids and exit -- no full sweep, no ledger write")
    ap.add_argument("--selftest", action="store_true", help="offline checks, no DB/network")
    args = ap.parse_args()

    if args.selftest:
        sys.exit(0 if _selftest() else 1)

    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    # --check always reports both classes; --class-a-only skips CLASS B's own modules
    # entirely (T2 / #186) -- see _load_class_b_modules()'s docstring.
    need_class_b = bool(args.check) or not args.class_a_only
    if need_class_b:
        _load_class_b_modules()

    print("Resolved producer modules (must all live under this file's directory):")
    resolved_mods = [aar_db, gate, orch] + (
        [idxmod, uni, scop, matcher, sweep, rcp] if need_class_b else [])
    for _mod in resolved_mods:
        print(f"  {_mod.__name__:20} {_mod.__file__}")
    print()

    engine = aar_db.engine()
    idx = io_scorer = None
    if need_class_b:
        idx = IdentityIndex.load()
        n_roster = sum(len(v) for v in idx.by_surname.values())
        print(f"Identity roster: {n_roster} people\n")
        io_scorer = matcher.IdentityOnlyScorer()
        _install_capture()

    if args.check:
        _check_ids(engine, idx, io_scorer, args.check)
        return

    print("==== CLASS A: already-attributed ====")
    class_a = _class_a_candidates(engine)
    src_breakdown = {}
    for h in class_a.values():
        src_breakdown[h["row"]["source"]] = src_breakdown.get(h["row"]["source"], 0) + 1
    via_attr_n = sum(1 for h in class_a.values() if h["via_attr"])
    via_gold_only_n = sum(1 for h in class_a.values() if h["via_gold"] and not h["via_attr"])
    print(f"  CLASS A total: {len(class_a)}  (source breakdown: {src_breakdown})")
    print(f"    via person_article ACCEPTED (gate.attributions): {via_attr_n}")
    print(f"    via GoldStandard knownpmids ONLY (attr signal missed it): {via_gold_only_n}")

    if args.class_a_only:
        # CLASS A summary line printed above lands in the nightly log either way (T2).
        ledger_entries = [_class_a_ledger_entry(h, run_ts, args.apply) for h in class_a.values()]
        _write_ledger(args.ledger, ledger_entries)
        print(f"\n  ledger -> {args.ledger} ({len(ledger_entries)} rows: "
              f"{len(class_a)} class A)")
        if args.apply:
            na = _apply_class_a(engine, class_a, run_ts)
            print(f"\n  APPLIED: {na} rows dismissed (class A)")
        else:
            print(f"\n  DRY RUN -- 0 rows written. Re-run with --apply to write these "
                  f"{len(class_a)} dismissals for real.")
        return

    include_drift = args.include_drift or args.drift_only
    include_sideways = args.include_sideways and not args.drift_only

    print("\n==== CLASS B: stale-pick refresh (io-rescored, unfiltered by CLASS A) ====")
    cb = _class_b_all_changed(engine, idx, io_scorer, args.limit, args.lane)
    for label, results in (("pubmed", cb["pm_results"]), ("scopus", cb["sc_results"])):
        by_cls = {}
        for x in results:
            by_cls.setdefault(x["classification"], []).append(x)
        print(f"  {label}: considered={len(results)}"
              + "".join(f"  {k}={len(v)}" for k, v in sorted(by_cls.items()))
              + f"  DRIFT_ONLY={len(_drift_rows(results))}")
    changed = cb["by_cls"].get("CHANGED", [])
    no_match = cb["by_cls"].get("NO_MATCH", [])
    unchanged = cb["by_cls"].get("UNCHANGED", [])
    drift = _drift_rows(cb["all_results"])
    print(f"  TOTAL: considered={len(cb['all_results'])}  UNCHANGED={len(unchanged)}"
          f"  CHANGED={len(changed)}  NO_MATCH={len(no_match)}"
          f"  (of the UNCHANGED, DRIFT_ONLY={len(drift)})")
    print(f"  DRIFT_ONLY columns that moved: {_drift_col_counts(drift)}")
    for x in drift[:5]:
        print(f"    e.g. id={x['id']} ({x['source']}) '{x['wcm_author']}': "
              f"{x['old_cwid']} stays, stale {x['drift']}")

    write_set, weaker, sideways, overlap = _class_b_write_set(
        changed, set(class_a.keys()), include_sideways)
    if args.drift_only:
        # No row's proposed PERSON is changed on this path -- see --drift-only's help.
        write_set = []
    drift_write, drift_overlap = _drift_write_set(drift, set(class_a.keys()), include_drift)
    write_set = list(write_set) + list(drift_write)
    stronger_all = [r for r in changed if r["id"] not in class_a
                    and r.get("tier_move") == "stronger"]
    print(f"\n  tier moves within CHANGED (excluding CLASS-A overlap):"
          f"  stronger={len(stronger_all)}  sideways={len(sideways)}  weaker={len(weaker)}")
    print(f"  CLASS-A / CLASS-B raw overlap (rows that qualify for both before "
          f"filtering): {len(overlap)}")
    if overlap:
        print(f"    ids: {[r['id'] for r in overlap][:20]}"
              f"{' ...' if len(overlap) > 20 else ''}")
    print(f"  -> CLASS A wins on every one of those {len(overlap) + len(drift_overlap)} "
          f"rows ({len(overlap)} pick-change + {len(drift_overlap)} drift): they are "
          f"dismissed (CLASS A), not repicked (CLASS B).")
    write_ids = {r["id"] for r in write_set}
    assert len(write_ids) == len(write_set), (
        "CLASS-B write set contains a duplicate id -- a row queued as both a pick "
        "change and a drift refresh; the two classes must partition CHANGED/UNCHANGED")
    assert write_ids.isdisjoint(class_a.keys()), (
        "CLASS-B write set overlaps CLASS-A ids -- collision handling broken")
    assert write_ids.isdisjoint({r["id"] for r in no_match}), (
        "CLASS-B write set overlaps this run's own NO_MATCH bucket -- classification "
        "partition broken"
    )
    queued = (["stronger"] if not args.drift_only else []) \
        + (["sideways"] if include_sideways else []) \
        + (["drift"] if include_drift else [])
    print(f"  CLASS-B write set ({' + '.join(queued) or 'nothing'}): "
          f"{len(write_set)} rows ({len(write_set) - len(drift_write)} pick-change + "
          f"{len(drift_write)} DRIFT_ONLY) -- disjoint from CLASS A ({len(class_a)}) and "
          f"from NO_MATCH ({len(no_match)}), asserted above.")
    if not include_sideways and sideways:
        print(f"  ({len(sideways)} sideways rows NOT queued -- rerun with "
              f"--include-sideways to include them)")
    if not include_drift and drift:
        print(f"  ({len(drift)} DRIFT_ONLY rows NOT queued -- rerun with "
              f"--include-drift, or --drift-only for that class alone)")
    print(f"  {len(weaker)} weaker rows excluded, always (never applied under any flag).")
    if weaker:
        print("    examples:")
        for x in weaker[:5]:
            print(f"      id={x['id']} ({x['source']}) '{x['wcm_author']}': "
                  f"{x['old_cwid']}/{x['old_given_match']} -> "
                  f"{x['new_cwid']}/{x['new_given_match']}")

    if cb["pm_unresolvable"]:
        print(f"\n  pubmed unresolvable ids (untouched): "
              f"{[r['id'] for r in cb['pm_unresolvable']][:20]}"
              f"{' ...' if len(cb['pm_unresolvable']) > 20 else ''} "
              f"({len(cb['pm_unresolvable'])} total)")
    if cb["sc_unresolvable"]:
        print(f"  scopus unresolvable ids (untouched): "
              f"{[r['id'] for r in cb['sc_unresolvable']][:20]}"
              f"{' ...' if len(cb['sc_unresolvable']) > 20 else ''} "
              f"({len(cb['sc_unresolvable'])} total)")

    before_snapshots = _full_before_snapshots(engine, [r["id"] for r in write_set])
    ledger_entries = ([_class_a_ledger_entry(h, run_ts, args.apply) for h in class_a.values()]
                      + [_class_b_ledger_entry(r, run_ts, args.apply, before_snapshots[r["id"]])
                         for r in write_set])
    _write_ledger(args.ledger, ledger_entries)
    print(f"\n  ledger -> {args.ledger} ({len(ledger_entries)} rows: "
          f"{len(class_a)} class A + {len(write_set)} class B)")

    if args.apply:
        na = _apply_class_a(engine, class_a, run_ts)
        # _apply_class_b consumes ledger-shaped entries (e["after"] is the exact
        # 9-column payload the ledger promises), NOT raw write_set records (#189).
        nb = _apply_class_b(engine, [e for e in ledger_entries if e["class"] == "B"],
                            run_ts)
        print(f"\n  APPLIED: {na} rows dismissed (class A), {nb} rows refreshed (class B)")
    else:
        print(f"\n  DRY RUN -- 0 rows written. Re-run with --apply to write these "
              f"{len(class_a)} dismissals + {len(write_set)} refreshes for real.")


# ============================================================================
# self-test
# ============================================================================
def _selftest():
    """Offline: no DB, no network. Proves the pieces that don't need live data:
    column-list identity with #180, the collision-priority filter, the weaker-exclusion
    rule, the write-payload assertion tripwire, --class-a-only's parsing and its
    note/CONCAT_WS write shape, and the CLASS-A reason string format (T2/T3 -- #186)."""
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  [{'OK' if cond else '** FAIL'}] {label}")

    # --class-a-only parses, defaults to off, and is independent of --apply.
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--class-a-only", action="store_true")
    a1 = ap.parse_args([])
    a2 = ap.parse_args(["--apply", "--class-a-only"])
    check("--class-a-only parses and defaults to False", a1.class_a_only is False)
    check("--apply --class-a-only both parse True together",
          a2.apply is True and a2.class_a_only is True)

    # CLASS A's reason string format (T3): "auto: ..." into note, not dup_reason.
    r = _class_a_reason("abc123", 42424133, "person_article ACCEPTED")
    check("CLASS A reason uses the 'auto:' note convention (#181)",
          r == "auto: already attributed (#181): abc123 already holds pmid "
               "42424133 via person_article ACCEPTED")

    # CLASS A's write statement targets note via CONCAT_WS, keeps the status='open'
    # race guard, and no longer writes dup_reason (T3).
    check("CLASS A UPDATE re-checks status='open'",
          "status='open'" in _CLASS_A_UPDATE_SQL)
    check("CLASS A UPDATE writes note via CONCAT_WS(..., NULLIF(note, ''), :reason), "
          "preserving curator text", "note=CONCAT_WS(' | ', NULLIF(note, ''), :reason)"
          in _CLASS_A_UPDATE_SQL)
    check("CLASS A UPDATE no longer writes dup_reason",
          "dup_reason" not in _CLASS_A_UPDATE_SQL)
    set_clause_a = _CLASS_A_UPDATE_SQL.split("WHERE")[0]
    check("CLASS A UPDATE never touches other curator columns",
          all(col not in set_clause_a for col in
              ("resolution_cwid", "reviewer", "snooze_until")))

    # --class-a-only must never import CLASS B's own dependencies (T2): before this
    # point in a real --class-a-only run, _load_class_b_modules() is never called, so
    # these stay None. (This selftest itself calls it below to test CLASS B's pieces
    # too -- that's fine, --selftest is offline and exercises both classes.)
    check("CLASS B modules are lazy globals, unset until _load_class_b_modules() runs",
          "idxmod" in globals() and "matcher" in globals())

    _load_class_b_modules()
    check("_load_class_b_modules's own _cwid_eq duplicate matches "
          "aar_report_changed_picks._cwid_eq (drift tripwire)",
          _cwid_eq("ABC123", "abc123") is True and _cwid_eq(None, "x") is False)

    check("REFRESH_COLS is exactly aar_sweep_stale.NULL_COLUMNS (imported, not redefined)",
          REFRESH_COLS is sweep.NULL_COLUMNS)
    check("REFRESH_COLS is the issue's 9 columns",
          REFRESH_COLS == ["top_cwid", "top_name", "top_person_type", "top_dept",
                           "top_given_match", "top_affil_match", "top_confidence",
                           "candidate_cwids_json", "n_candidates"])

    changed = [
        {"id": 1, "classification": "CHANGED", "tier_move": "stronger"},
        {"id": 2, "classification": "CHANGED", "tier_move": "sideways"},
        {"id": 3, "classification": "CHANGED", "tier_move": "weaker"},
        {"id": 4, "classification": "CHANGED", "tier_move": "stronger"},  # also class A
    ]
    write, weaker, sideways, overlap = _class_b_write_set(changed, {4}, include_sideways=False)
    check("default run: only the non-overlapping stronger row is queued",
          [r["id"] for r in write] == [1])
    check("weaker is excluded even though sideways/stronger are eligible",
          [r["id"] for r in weaker] == [3])
    check("the class-A-overlapping stronger row (id 4) is excluded from the write set",
          4 not in {r["id"] for r in write})
    check("overlap is reported (id 4)", [r["id"] for r in overlap] == [4])

    write2, weaker2, sideways2, overlap2 = _class_b_write_set(changed, {4}, include_sideways=True)
    check("--include-sideways: sideways row (id 2) joins the write set",
          sorted(r["id"] for r in write2) == [1, 2])
    check("--include-sideways still never includes weaker",
          3 not in {r["id"] for r in write2})

    # write-payload assertion tripwire: a captured top whose cwid disagrees with the
    # replay's own classification must refuse to write, not write the wrong value.
    FULL_CANDS_BY_ID[999] = [{"cwid": "WRONG", "name": "x", "person_type": "y", "dept": "z",
                             "given_match": "full", "affil_dept_match": False,
                             "confidence": 0.9}]
    bad_rec = {"id": 999, "source": "pubmed", "new_cwid": "expected_cwid"}
    raised = False
    try:
        _write_payload(bad_rec)
    except AssertionError:
        raised = True
    check("write_payload refuses a captured/classified cwid mismatch instead of "
          "silently writing it", raised)

    FULL_CANDS_BY_ID[1000] = [{"cwid": "abc123", "name": "Jane Doe", "person_type": "Faculty",
                              "dept": "Medicine", "given_match": "full",
                              "affil_dept_match": True, "confidence": 0.87}]
    good_rec = {"id": 1000, "source": "pubmed", "new_cwid": "abc123"}
    payload = _write_payload(good_rec)
    check("write_payload builds all 9 columns for a matching capture",
          set(payload.keys()) == set(REFRESH_COLS))
    check("write_payload's top_affil_match is coerced to 0/1",
          payload["top_affil_match"] == 1)

    # apply-wiring contract (#189): main() feeds _apply_class_b the class-B slice of
    # ledger_entries, and _apply_class_b consumes exactly e["id"] + e["after"][col]
    # for every REFRESH_COL. The first live --apply crashed (KeyError: 'after')
    # because it was handed raw write_set records instead.
    full_rec = dict(good_rec, tier_move="stronger", pmid=1, external_id=None,
                    wcm_author="Jane Doe")
    entry = _class_b_ledger_entry(full_rec, "2026-01-01 00:00:00", applied=False,
                                  before={c: None for c in REFRESH_COLS})
    b_slice = [e for e in [entry] if e["class"] == "B"]
    check("class-B ledger entry survives main()'s apply-slice filter", len(b_slice) == 1)
    check("ledger entry carries id + a complete 9-column 'after' payload "
          "(what _apply_class_b consumes)",
          "id" in entry and set(REFRESH_COLS) <= set(entry["after"].keys()))

    # ---- DRIFT_ONLY (#186) ---------------------------------------------------
    # A curator-touched row is never written: CLASS B's UPDATE carries the same
    # status='open' re-check CLASS A's does, and _open_rows only ever hands it open
    # rows in the first place. Worth asserting now that the drift class turns this from
    # a hand-reviewed one-off into an unattended nightly pass over thousands of rows.
    sql_b = _class_b_update_sql()
    check("CLASS B UPDATE re-checks status='open' (curator-touched rows never written)",
          sql_b.endswith("WHERE id=:id AND status='open'"))
    set_clause_b = sql_b.split("WHERE")[0]
    check("CLASS B UPDATE never touches curator columns",
          all(col not in set_clause_b for col in
              ("status", "resolution_cwid", "reviewer", "note", "snooze_until")))
    check("CLASS B UPDATE writes exactly the 9 producer-owned columns",
          all(f"{c}=:{c}" in set_clause_b for c in REFRESH_COLS))

    def _cand(cwid, **kw):
        c = {"cwid": cwid, "name": "Jane Q Doe", "person_type": "Full-Time Faculty",
             "dept": "Medicine", "division": "", "title": "", "given_match": "full",
             "affil_dept_match": True, "affil_match_on": "dept", "cohort_size": 3,
             "years_after_wcm": None, "confidence": 0.883}
        c.update(kw)
        return c

    c1 = _cand("abc123")
    c2 = _cand("def456", name="John Doe", dept="Surgery", given_match="initial",
               affil_dept_match=True, affil_match_on="dept", confidence=0.633)
    c3 = _cand("ghi789", name="Jo Doe", dept="Pediatrics", given_match="initial",
               affil_dept_match=False, affil_match_on=None, confidence=0.383)
    stored_cands = [c1, c2, c3]
    FULL_CANDS_BY_ID[2001] = stored_cands
    stored_payload = _write_payload({"id": 2001, "source": "pubmed", "new_cwid": "abc123"})
    # The row exactly as the producer wrote it -- built FROM _write_payload, so this
    # doubles as the cross-module tripwire that rcp._drift_cols and _write_payload agree
    # on truncation, 0/1 coercion and JSON shape. If they ever diverge, every row in the
    # queue reads as drifted and this check fails first.
    row = dict(stored_payload, id=2001, source="pubmed", pmid=1, external_id=None,
               wcm_author="Jane Doe")

    rec = rcp._row_result(row, stored_cands[0], "pubmed", stored_cands)
    check("a row the producer would write identically today is UNCHANGED with no drift "
          "(_drift_cols agrees with _write_payload)",
          rec["classification"] == "UNCHANGED" and rec["drift"] == [])

    # THE wobble guard. io_score/final_score/io_source are re-read from live S3 inputs
    # the nightly inst-client keeps refreshing -- issue #182 measured CHANGED at 2,394
    # vs 2,398 on two runs 50 minutes apart. If they triggered drift, DRIFT_ONLY would
    # match nearly every pubmed row on every run and the pass would rewrite the whole
    # open queue nightly.
    io_cands = [dict(c) for c in stored_cands]
    for c, io in zip(io_cands, (91.4, 3.1, None)):
        c["io_score"] = io
        c["final_score"] = io
        c["io_source"] = "retrieved" if io is not None else "not_retrieved"
    rec_io = rcp._row_result(row, io_cands[0], "pubmed", io_cands)
    check("a row where ONLY io_score/final_score/io_source differ is UNCHANGED with no "
          "drift (the #182 run-to-run wobble can never trigger a refresh)",
          rec_io["classification"] == "UNCHANGED" and rec_io["drift"] == [])

    # io is aar_matcher.match_authorship's SECOND sort term, so it reorders the stored
    # candidate list run to run without changing which 5 people are in it (the top_k cut
    # happens in identity_index.candidates(), before io is attached). The comparison is
    # therefore an unordered map keyed by cwid, never positional.
    rec_reorder = rcp._row_result(row, io_cands[0], "pubmed",
                                  [io_cands[0], io_cands[2], io_cands[1]])
    check("io-driven reordering of the candidate list below the top pick is not drift",
          rec_reorder["drift"] == [])

    # The #203 shape: same top_cwid, but the top candidate loses a phantom affiliation
    # match (worth +0.25 confidence). CHANGED cannot see it (top_cwid is equal) and
    # tier_move cannot see it (given_match is untouched, so the move would be sideways).
    post203 = [dict(c1, affil_dept_match=False, affil_match_on=None, confidence=0.633),
               c2, c3]
    rec203 = rcp._row_result(row, post203[0], "pubmed", post203)
    check("#203 shape: top_cwid unchanged but top_affil_match differs -> DRIFT_ONLY",
          rec203["classification"] == "UNCHANGED"
          and rec203["drift"] == ["candidate_cwids_json", "top_affil_match",
                                  "top_confidence"])
    check("the #203 shape is invisible to the pre-#186 triggers (still UNCHANGED, and "
          "no tier_move, since given_match never moves)",
          rec203["classification"] == "UNCHANGED" and "tier_move" not in rec203)

    # Drift below the top pick still counts: candidate_cwids_json is a producer-owned
    # column and the curator UI renders its chips.
    below = [c1, dict(c2, affil_dept_match=False, affil_match_on=None, confidence=0.383), c3]
    rec_below = rcp._row_result(row, below[0], "pubmed", below)
    check("evidence drift on a NON-top candidate drifts candidate_cwids_json alone",
          rec_below["drift"] == ["candidate_cwids_json"])

    rec_fewer = rcp._row_result(row, c1, "pubmed", [c1, c2])
    check("a shorter candidate list drifts n_candidates and candidate_cwids_json",
          rec_fewer["drift"] == ["candidate_cwids_json", "n_candidates"])

    # top_confidence is a 4-byte FLOAT column: a stored 0.883 reads back as
    # 0.8830000162124634. Comparing it with == would mark every row on the queue as
    # drifted on the very first run.
    import struct
    f32 = struct.unpack("f", struct.pack("f", 0.883))[0]
    check("the FLOAT column really does perturb the value (else this test proves "
          "nothing)", f32 != 0.883)
    rec_f32 = rcp._row_result(dict(row, top_confidence=f32), c1, "pubmed", stored_cands)
    check("float32 round-trip of top_confidence does not manufacture drift",
          rec_f32["drift"] == [])
    rec_real = rcp._row_result(dict(row, top_confidence=0.882), c1, "pubmed", stored_cands)
    check("the tolerance is still tighter than one 0.001 confidence quantum",
          rec_real["drift"] == ["top_confidence"])

    # CHANGED rows never carry drift -- the two classes partition the replay, so a row
    # can never be queued twice.
    rec_changed = rcp._row_result(dict(row, top_cwid="zzz9999"), c1, "pubmed", stored_cands)
    check("a CHANGED row carries an empty drift list (classes never overlap)",
          rec_changed["classification"] == "CHANGED" and rec_changed["drift"] == [])
    rec_nomatch = rcp._row_result(row, None, "pubmed", [])
    check("a NO_MATCH row carries an empty drift list",
          rec_nomatch["classification"] == "NO_MATCH" and rec_nomatch["drift"] == [])

    # write-set gating: drift needs an explicit opt-in, CLASS A still wins over it, and
    # neither drift flag disturbs the tier-move classes.
    drift_pool = [{"id": 11, "classification": "UNCHANGED", "drift": ["top_affil_match"]},
                  {"id": 12, "classification": "UNCHANGED", "drift": ["top_confidence"]}]
    dw, dov = _drift_write_set(drift_pool, {12}, include_drift=True)
    check("DRIFT_ONLY write set excludes the CLASS-A-overlapping row",
          [r["id"] for r in dw] == [11] and [r["id"] for r in dov] == [12])
    dw_off, _ = _drift_write_set(drift_pool, set(), include_drift=False)
    check("without --include-drift no drift row is queued at all", dw_off == [])
    check("_drift_rows selects UNCHANGED-with-drift only",
          [r["id"] for r in _drift_rows(
              drift_pool + [{"id": 13, "classification": "UNCHANGED", "drift": []},
                            {"id": 14, "classification": "CHANGED", "drift": []}])]
          == [11, 12])
    check("_drift_col_counts reports the per-column breakdown",
          _drift_col_counts(drift_pool) == {"top_affil_match": 1, "top_confidence": 1})

    # The tier-move classes must be untouched by all of the above: same gating, same
    # weaker hard-exclusion, whatever the drift flags say.
    write3, weaker3, sideways3, overlap3 = _class_b_write_set(changed, {4},
                                                              include_sideways=False)
    check("tier-move gating is unchanged by the drift class (stronger only, weaker "
          "excluded, class-A overlap removed)",
          [r["id"] for r in write3] == [1] and [r["id"] for r in weaker3] == [3]
          and [r["id"] for r in sideways3] == [2] and [r["id"] for r in overlap3] == [4])

    print("\nSELFTEST", "PASS" if ok else "FAIL")
    return ok


if __name__ == "__main__":
    main()
