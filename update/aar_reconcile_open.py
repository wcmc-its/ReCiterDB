#!/usr/bin/env python3
"""
One-off reconciliation pass over the open AAR queue for issues #181 and #182 --
the two staleness classes PR #180 (issue #177, NO_MATCH) does not cover.

  CLASS A (#181) -- the row's STORED top_cwid already holds the pmid, per reciterdb's
    own source of truth. The authorship is already correctly attributed; the row
    should leave the queue entirely.  Action: status='dismissed' + a distinct,
    greppable reason recorded (see REASON COLUMN below) + resolved_at. Never touches
    resolution_cwid / reviewer / note / snooze_until.

  CLASS B (#182) -- a REPLAY of the authorship through the CURRENT matcher proposes a
    DIFFERENT, and given_match-TIER-STRONGER, top_cwid than what is stored. The pick is
    stale; a curator is looking at a wrong name. Action: refresh the same 9 producer-
    owned columns aar_sweep_stale.py (#180) nulls: top_cwid, top_name, top_person_type,
    top_dept, top_given_match, top_affil_match, top_confidence, candidate_cwids_json,
    n_candidates.

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

  This also fully explains, and contains, the "weaker" bucket: match_authorship's
  pre-existing "any io_score beats no score" tie-break (aar_matcher.py:158-160, unrelated
  to #171/#173/#174) is real production ranking behaviour, not a bug this tool should
  paper over by picking the other ranking -- but it is also not evidence the identity
  fixes did anything wrong, and applying a weaker move would demote a good pick on that
  artifact. So: use the real ranking (io-rescored), but hard-exclude "weaker" from the
  write set regardless of flags, and show the excluded count so it's never silently
  dropped. --no-io-rescore stays available as a read-only diagnostic on the sibling
  aar_report_changed_picks.py (its `--no-io-rescore --check <ids>`) -- this file does
  not expose that flag itself, and never wires it to --apply.

REASON COLUMN for CLASS A's dismissal (read the DDL + the PM reader first, as asked)
  aar_db.py's DDL/docstring names status/resolution_cwid/reviewer/note/snooze_until as
  curator-owned. There IS a live precedent for an AUTOMATED status='dismissed' write
  with a distinguishable reason on this exact table: aar_universe_scopus.
  recheck_open_scopus() sets status='dismissed', resolved_at=:ts, and
  note=CONCAT('auto: now in PubMed (...)') when a scopus row is auto-resolved out --
  i.e. the established convention for "system, not curator, dismissed this, and here's
  why" IS `note`, prefixed 'auto:'. This task's own instructions, however, name `note`
  as never-touch here. Given that explicit instruction, and since dup_reason is the
  ONLY other free-text, non-curator column on the table, CLASS A instead writes a
  distinct, greppable reason into `dup_reason` (format below), NOT into `note`.

  This is a real compromise, not a clean answer, and it has a concrete downside: the
  Publication Manager Sequelize model (ReCiter-Publication-Manager/src/db/models/
  AuthorshipReview.ts, checked 2026-08-31) does not declare dup_flag/dup_reason at all
  -- those columns predate that model file and the PM UI cannot render them today. So a
  curator looking at a CLASS-A-dismissed row in PM sees status=dismissed with no reason
  text, even though one exists in the database. `note` is both the actual convention
  AND the only field PM already renders; `dup_reason` is the only field this tool is
  allowed to touch. Flagged in `concerns` as a real product gap, not swept under the
  rug: either lift the note restriction for this one write, or extend the PM model to
  select dup_flag/dup_reason, or add a dedicated `dismiss_reason` column later.

  Format: "already-attributed (#181): {cwid} already holds pmid {pmid} via {signal}"
  where {signal} is "person_article ACCEPTED", "GoldStandard knownpmids", or both.

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
  "CHANGED". NO_MATCH rows from that same replay are a disjoint classification bucket
  by construction (UNCHANGED / CHANGED / NO_MATCH partition every considered row
  exactly once, per that module's own docstring) and are never part of any CLASS-B
  write candidate. The dry run below prints this run's own NO_MATCH count alongside
  the CHANGED count so the partition is visible, not merely claimed.

REUSE, NOT A FOURTH MATCHING PATH
  Imports aar_report_changed_picks.py's `_open_rows`, `_replay_pubmed`, `_replay_scopus`,
  `_row_result`, `_cwid_eq`, `TIER_RANK` directly -- the SAME verified replay #182's own
  measurement used, unmodified on disk. That replay's own output shape (`_row_result`)
  keeps only the trimmed old/new cwid+name+given_match+confidence fields it needs for
  its CSV -- not the full candidate dict (person_type, dept, affil_dept_match,
  candidate list) this tool needs to actually WRITE the 9 columns. Rather than adding a
  second, parallel re-implementation of the fetch+match loop (a real "fourth path",
  and a real risk of quietly drifting from what #182 measured), this module installs a
  transparent, in-process capture around three call sites -- `aar_matcher.
  match_authorship`, the shared `IdentityIndex` instance's `.candidates`, and
  aar_report_changed_picks._row_result -- each wrapper calls straight through to the
  original and returns its result completely unchanged; it only additionally stashes
  the full candidate list the row's classification was computed from, keyed by row id.
  Nothing in aar_report_changed_picks.py is edited on disk, and every write this tool
  ever proposes is asserted, per row, to have `cands[0]["cwid"] == <that row's own
  classified new_cwid>` before being trusted -- a mismatch (e.g. from a future refactor
  of the sibling module's internal call shape) raises loudly instead of writing a
  quietly-wrong value.

  ponytail: the capture is a runtime monkeypatch, not a supported extension point on
  aar_report_changed_picks.py. Ceiling: it depends on that module continuing to call
  `matcher.match_authorship` (pubmed) / `idx.candidates` (scopus) exactly once per row,
  immediately before `_row_result`, single-threaded. The per-row assertion above is the
  tripwire if that ever stops being true. Upgrade path if it ever fires: have
  aar_report_changed_picks._row_result optionally accept/stash the full candidate list
  itself (a small, additive change to the reviewed tool, worth doing once rather than
  defended against here forever).

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
import identity_index as idxmod
import aar_universe as uni
import aar_universe_scopus as scop
import aar_matcher as matcher
import aar_gate as gate
import aar_orchestrator as orch          # read-only reuse of _batch_gold_standard/_compact/
                                          # _trunc -- never modified, never runs orch.main()
import aar_sweep_stale as sweep          # reuse NULL_COLUMNS / _SELECT_COLS / _snapshot
import aar_report_changed_picks as rcp   # reuse the verified replay -- see REUSE above
from identity_index import IdentityIndex

# Every module this script trusts must resolve to THIS directory, not the stale
# ~/Dropbox/Projects/ReCiter Research/scripts/ copy (sys.path fork trap).
for _mod in (aar_db, idxmod, uni, scop, matcher, gate, orch, sweep, rcp):
    assert os.path.dirname(os.path.abspath(_mod.__file__)) == HERE, (
        f"{_mod.__name__} resolved to {_mod.__file__}, not {HERE} -- "
        "sys.path fork trap: a stale producer copy is winning. Refusing to trust any "
        "replay result computed against it.")

# The exact 9 columns #180 nulls and #182 says need refreshing -- imported, not
# redefined, so the two tools can never silently diverge on which columns are in play.
REFRESH_COLS = sweep.NULL_COLUMNS
_cwid_eq = rcp._cwid_eq


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
            "SELECT id, source, pmid, top_cwid, dup_reason, resolved_at "
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
        reason = (f"already-attributed (#181): {r['top_cwid']} already holds pmid "
                  f"{r['pmid']} via {signal}")[:255]
        hits[r["id"]] = {"row": r, "via_attr": via_attr, "via_gold": via_gold,
                         "reason": reason}
    return hits


def _class_a_ledger_entry(hit, run_ts, applied):
    r = hit["row"]
    return {
        "id": r["id"], "class": "A", "rule": "already_attributed",
        "source": r["source"], "pmid": r["pmid"], "top_cwid": r["top_cwid"],
        "via_attr": hit["via_attr"], "via_gold": hit["via_gold"],
        "before": {"status": "open", "dup_reason": r["dup_reason"],
                  "resolved_at": r["resolved_at"]},
        "after": {"status": "dismissed", "dup_reason": hit["reason"], "resolved_at": run_ts},
        "swept_at": run_ts, "applied": applied,
    }


def _apply_class_a(engine, hits, run_ts):
    from sqlalchemy import text
    if not hits:
        return 0
    stmt = text("UPDATE authorship_review SET status='dismissed', resolved_at=:ts, "
                "dup_reason=:reason WHERE id=:id AND status='open'")
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
_LAST_CANDS = {"pubmed": None, "scopus": None}
FULL_CANDS_BY_ID = {}


def _install_capture(idx):
    """Wrap matcher.match_authorship (pubmed lane) and this specific idx instance's
    .candidates (scopus lane) so their full return value is observable, then wrap
    rcp._row_result so the row id currently being classified is known at the moment
    each capture needs to be filed. All three wrappers are pure passthroughs."""
    orig_match = matcher.match_authorship

    def _match_capture(*a, **kw):
        result = orig_match(*a, **kw)
        _LAST_CANDS["pubmed"] = result
        return result
    matcher.match_authorship = _match_capture

    orig_candidates = idx.candidates

    def _candidates_capture(*a, **kw):
        result = orig_candidates(*a, **kw)
        _LAST_CANDS["scopus"] = result[0]
        return result
    idx.candidates = _candidates_capture

    orig_row_result = rcp._row_result

    def _row_result_capture(r, new_top, source):
        rec = orig_row_result(r, new_top, source)
        _LAST_CANDS[source] = _LAST_CANDS.get(source) or []
        FULL_CANDS_BY_ID[r["id"]] = list(_LAST_CANDS[source])
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


def _class_b_all_changed(engine, idx, io_scorer, limit=None):
    """Replay every open row (both lanes, unfiltered by CLASS A) through the CURRENT
    matcher, io-rescore on (see RANKING DECISION), and return the full CHANGED list
    plus per-lane NO_MATCH/considered totals -- unfiltered, so the caller can measure
    the true CLASS A/B overlap rather than have it hidden by an upfront exclude."""
    print("[pubmed] loading open rows ...", flush=True)
    pm_rows = rcp._open_rows(engine, "pubmed")
    print(f"      {len(pm_rows)} open pubmed rows", flush=True)
    pm_results, pm_unresolvable = rcp._replay_pubmed(pm_rows, idx, io_scorer, limit,
                                                      io_rescore=True)

    print("\n[scopus] loading open rows ...", flush=True)
    sc_rows = rcp._open_rows(engine, "scopus")
    print(f"      {len(sc_rows)} open scopus rows", flush=True)
    sc_results, sc_unresolvable = rcp._replay_scopus(sc_rows, idx, limit)

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


def _full_before_snapshots(engine, ids):
    """Real pre-write values for all 9 refresh columns, for the given row ids -- not
    just the 4 fields aar_report_changed_picks._row_result happens to carry (old_cwid/
    old_name/old_given_match/old_confidence). Reuses aar_sweep_stale's own column list
    and snapshot helper so the ledger's 'before' is a genuine, reversible record."""
    from sqlalchemy import text, bindparam
    if not ids:
        return {}
    cols = ", ".join(["id"] + sweep.NULL_COLUMNS)
    stmt = text(f"SELECT {cols} FROM authorship_review WHERE id IN :ids") \
        .bindparams(bindparam("ids", expanding=True))
    with engine.connect() as c:
        rows = c.execute(stmt, {"ids": list(ids)}).mappings().all()
    return {r["id"]: sweep._snapshot(r) for r in rows}


def _class_b_ledger_entry(rec, run_ts, applied, before):
    payload = _write_payload(rec)
    return {
        "id": rec["id"], "class": "B", "rule": rec.get("tier_move"),
        "source": rec["source"], "pmid": rec["pmid"], "external_id": rec["external_id"],
        "wcm_author": rec["wcm_author"],
        "before": before, "after": payload,
        "swept_at": run_ts, "applied": applied,
    }


def _apply_class_b(engine, entries, run_ts):
    from sqlalchemy import text
    if not entries:
        return 0
    sets = ", ".join(f"{c}=:{c}" for c in REFRESH_COLS)
    stmt = text(f"UPDATE authorship_review SET {sets} WHERE id=:id AND status='open'")
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
    ap.add_argument("--include-sideways", action="store_true",
                    help="also queue CLASS-B SIDEWAYS tier moves (same-tier homonym "
                         "reshuffles, ~2,251 of the ~2,608 CHANGED population). Off by "
                         "default -- see the module docstring. STRONGER moves are "
                         "always queued; WEAKER moves are never queued under any flag.")
    ap.add_argument("--ledger", default=os.path.join(HERE, "aar_reconcile_open_ledger.jsonl"),
                    help="JSONL before/after output path (written on every run, "
                         "dry-run included)")
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
    print("Resolved producer modules (must all live under this file's directory):")
    for _mod in (aar_db, idxmod, uni, scop, matcher, gate, orch, sweep, rcp):
        print(f"  {_mod.__name__:20} {_mod.__file__}")
    print()

    engine = aar_db.engine()
    idx = IdentityIndex.load()
    n_roster = sum(len(v) for v in idx.by_surname.values())
    print(f"Identity roster: {n_roster} people\n")
    io_scorer = matcher.IdentityOnlyScorer()
    _install_capture(idx)

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

    print("\n==== CLASS B: stale-pick refresh (io-rescored, unfiltered by CLASS A) ====")
    cb = _class_b_all_changed(engine, idx, io_scorer, args.limit)
    for label, results in (("pubmed", cb["pm_results"]), ("scopus", cb["sc_results"])):
        by_cls = {}
        for x in results:
            by_cls.setdefault(x["classification"], []).append(x)
        print(f"  {label}: considered={len(results)}"
              + "".join(f"  {k}={len(v)}" for k, v in sorted(by_cls.items())))
    changed = cb["by_cls"].get("CHANGED", [])
    no_match = cb["by_cls"].get("NO_MATCH", [])
    unchanged = cb["by_cls"].get("UNCHANGED", [])
    print(f"  TOTAL: considered={len(cb['all_results'])}  UNCHANGED={len(unchanged)}"
          f"  CHANGED={len(changed)}  NO_MATCH={len(no_match)}")

    write_set, weaker, sideways, overlap = _class_b_write_set(
        changed, set(class_a.keys()), args.include_sideways)
    stronger_all = [r for r in changed if r["id"] not in class_a
                    and r.get("tier_move") == "stronger"]
    print(f"\n  tier moves within CHANGED (excluding CLASS-A overlap):"
          f"  stronger={len(stronger_all)}  sideways={len(sideways)}  weaker={len(weaker)}")
    print(f"  CLASS-A / CLASS-B raw overlap (rows that qualify for both before "
          f"filtering): {len(overlap)}")
    if overlap:
        print(f"    ids: {[r['id'] for r in overlap][:20]}"
              f"{' ...' if len(overlap) > 20 else ''}")
    print(f"  -> CLASS A wins on every one of those {len(overlap)} rows: they are "
          f"dismissed (CLASS A), not repicked (CLASS B).")
    write_ids = {r["id"] for r in write_set}
    assert write_ids.isdisjoint(class_a.keys()), (
        "CLASS-B write set overlaps CLASS-A ids -- collision handling broken")
    assert write_ids.isdisjoint({r["id"] for r in no_match}), (
        "CLASS-B write set overlaps this run's own NO_MATCH bucket -- classification "
        "partition broken"
    )
    print(f"  CLASS-B write set ({'stronger + sideways' if args.include_sideways else 'stronger only'}): "
          f"{len(write_set)} rows -- disjoint from CLASS A ({len(class_a)}) and from "
          f"NO_MATCH ({len(no_match)}), asserted above.")
    if not args.include_sideways and sideways:
        print(f"  ({len(sideways)} sideways rows NOT queued -- rerun with "
              f"--include-sideways to include them)")
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
    rule, and the write-payload assertion tripwire."""
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  [{'OK' if cond else '** FAIL'}] {label}")

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

    print("\nSELFTEST", "PASS" if ok else "FAIL")
    return ok


if __name__ == "__main__":
    main()
