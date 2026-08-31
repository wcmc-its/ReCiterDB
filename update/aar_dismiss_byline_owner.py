#!/usr/bin/env python3
"""
One-off sweep for issue #181, population (b) -- the half aar_reconcile_open.py
(PR #184) explicitly did not implement: OPEN authorship_review rows whose BYLINE'S
REAL OWNER already holds the pmid as ACCEPTED, regardless of which candidate the
row proposes. PM#937 established this population (454 open rows, measured
2026-08-30, anchor: row 70797, byline 'Tony Rosen', proposed off a middle-initial
fallback while aer2006 -- the person that byline actually names -- had held pmid
42424133 at 100/ACCEPTED since 2026-07-15).

  Action per eligible row: status='dismissed' + a distinct, greppable reason in
  dup_reason + resolved_at. Never touches resolution_cwid / reviewer / note /
  snooze_until, and re-checks status='open' at write time, so a row a curator
  resolves between the SELECT and the UPDATE is skipped, not clobbered.

METHODOLOGY -- PM#937's, reproduced from the gate, not reinvented
  PM#937's population is defined by the signal issue #174 / PR #176 added to the
  producer's gate: "is this BYLINE already attributed, to ANYONE?" -- the exact
  check aar_orchestrator._already_curated runs (its signal 2) to stop NEW rows of
  this class being created. This tool applies the SAME machinery to the rows that
  already exist, imported and called directly, never re-implemented:

    1. gate.attributions(pmids)   pmid -> [(cwid, authorPosition, names)] over
       analysis_summary_author, with each attributed person's name spellings from
       all three sources (the person_article byline ReCiter matched on that very
       pmid, the `person` publishing name, the `identity` legal name), homonym
       spellings already censored out (a spelling two WCM people answer to
       identifies nobody -- see aar_gate._ambiguous_names).
    2. orch._byline_owner(wcm_author, entry)   the strict byline-to-person match
       (surname is the byline's tail AND first tokens equal, no initials) that
       resolves the row's stored byline to the one attributed person it names, or
       None. This is the resolver; what the row PROPOSES (top_cwid) plays no part.
    3. ACCEPTED confirmation. attributions() membership means the (owner, pmid)
       pair is in analysis_summary_author, which the nightly SQL builds from
       person_article rows that are userAssertion='ACCEPTED' OR score>=30. PM#937
       says "holds the pmid as ACCEPTED", so a byline-owner hit is only ELIGIBLE
       for dismissal when the assertion itself is confirmed, by either of the same
       two signals aar_reconcile_open.py's CLASS A uses:
         - a person_article row for (owner, pmid) with userAssertion='ACCEPTED', or
         - pmid in the owner's GoldStandard.knownpmids (BatchGetItem, batched --
           catches an accept the nightly ETL hasn't mirrored yet).
       A hit confirmed by NEITHER (an owner attributed at score>=30 without an
       accept) is reported and left open -- it is a real residual population, not
       an error, but it is not what PM#937 counted and it is not dismissed.

WHY THIS IS NOT aar_reconcile_open.py's CLASS A, AND WHY IT IS MORE DANGEROUS
  CLASS A dismisses a row because its OWN stored proposal already holds the
  article -- defensible from the row itself. This tool dismisses a row based on a
  person the row may never name anywhere. That is why (#181's comment, option 2)
  it is a separate tool behind its own --apply, and why every ledger entry must be
  independently auditable: the row's full identity/state before, the resolved
  owner, WHICH name spelling matched the byline, and WHICH holding record
  establishes ownership (the person_article assertion row and/or the GoldStandard
  membership) -- enough to re-derive the verdict from the entry alone.

  Rows whose owner happens to EQUAL the stored top_cwid also qualify here (the
  populations overlap; #181 asks for the union) -- they are counted and labelled
  `proposed_is_owner` in both the report and the ledger, and double-handling with
  a concurrent aar_reconcile_open.py run is impossible: whichever tool dismisses
  first wins, the other's status='open' write guard skips the row.

REASON COLUMN -- same compromise as aar_reconcile_open.py, distinct prefix
  aar_reconcile_open.py's REASON COLUMN section documents why the reason lands in
  `dup_reason` (this task's never-touch list includes `note`, the actual 'auto:'
  convention; dup_reason is the only other free-text producer column) and what the
  downside is (the PM Sequelize model doesn't select it). Same call here, one
  prefix apart so the two sweeps stay greppable from each other:
    "already-attributed-owner (#181): byline '{byline}' resolves to {owner} who
     already holds pmid {pmid} via {signal}"

COLLATION TRAP -- no SQL join ever crosses it
  authorship_review.top_cwid is utf8mb4_general_ci; person_article.personIdentifier
  / identity.cwid / person.personIdentifier are utf8mb4_unicode_ci. A naive join
  throws MySQL 1267; a filtered variant silently returns zero rows and makes the
  sweep look done. So: authorship_review is queried alone; person_article is
  fetched by pmid (BIGINT, no collation) and every cwid comparison happens in
  Python via rcp._cwid_eq (case-folded, both collations are case-insensitive);
  gate.attributions()'s own docstring makes the same no-cross-join guarantee for
  its half. And every place a silent zero could hide is a LOUD, non-zero exit:
    - an empty open pool (a live queue of ~17k rows cannot legitimately be empty)
    - attributions() resolving zero of the pool's pmids
    - byline-owner hits whose person_article confirmation fetch returns zero rows
    - hits > 0 with zero ACCEPTED-confirmed (the exact shape the silent-zero
      collation bug would produce)
    - a byline-owner match this tool cannot reproduce spelling-by-spelling
      (internal tripwire -- would mean orch._byline_owner's semantics changed
      under us, refuse to write anything)

INERT BY DESIGN
  Dry-run by default; --apply is the only write path. NOT wired into run_all.py
  and must not be: whether these rows leave the queue at all is the open decision
  on #181, and this tool exists so that decision can be taken on measured, row-
  level evidence rather than an aggregate.

Usage:
  python aar_dismiss_byline_owner.py                  # dry run: report + JSONL ledger
  python aar_dismiss_byline_owner.py --apply          # perform the dismissals for real
  python aar_dismiss_byline_owner.py --limit 200      # cap the pool (fast sanity; NOT
                                                      #   the population count)
  python aar_dismiss_byline_owner.py --check 70797    # named-row verdicts, no ledger
  python aar_dismiss_byline_owner.py --selftest       # offline checks, no DB/network
"""
import argparse, json, os, sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)  # must be the LAST insert(0, ...) before these imports --
                          # sys.path fork trap, same guard as aar_reconcile_open.py.
import aar_db
import aar_gate as gate
import aar_matcher as matcher
import aar_orchestrator as orch          # read-only reuse of _byline_owner /
                                          # _batch_gold_standard -- never runs orch.main()
import aar_report_changed_picks as rcp   # reuse _cwid_eq -- the collation-safe compare
import identity_index as idxmod

# Every module this script trusts must resolve to THIS directory, not the stale
# ~/Dropbox/Projects/ReCiter Research/scripts/ copy (sys.path fork trap).
for _mod in (aar_db, gate, matcher, orch, rcp, idxmod):
    assert os.path.dirname(os.path.abspath(_mod.__file__)) == HERE, (
        f"{_mod.__name__} resolved to {_mod.__file__}, not {HERE} -- "
        "sys.path fork trap: a stale producer copy is winning. Refusing to trust any "
        "result computed against it.")

_cwid_eq = rcp._cwid_eq

REASON_PREFIX = "already-attributed-owner (#181)"

# The ONLY write this tool ever performs. SET touches exactly status / resolved_at /
# dup_reason; the status='open' re-check makes a concurrent curator (or concurrent
# aar_reconcile_open.py --apply) win every race.
_UPDATE_SQL = ("UPDATE authorship_review SET status='dismissed', resolved_at=:ts, "
               "dup_reason=:reason WHERE id=:id AND status='open'")


def _die(msg):
    print(f"\n** INCONSISTENCY: {msg}", flush=True)
    print("** Refusing to continue -- a silent zero here is exactly the failure mode "
          "this tool guards against.", flush=True)
    sys.exit(2)


# ============================================================================
# candidate pool + owner resolution
# ============================================================================
def _open_pool(engine):
    """Every OPEN row with a pmid and a byline. top_cwid may be NULL (e.g. rows PR #180
    nulled) -- the byline, not the proposal, is what this sweep resolves, so such rows
    stay in the pool. Open scopus rows carry pmid=NULL by construction (see
    aar_universe_scopus.run step 2), so this is in practice the pubmed lane; the query
    is source-agnostic on purpose and the source split is printed, not assumed."""
    from sqlalchemy import text
    with engine.connect() as c:
        rows = c.execute(text(
            "SELECT id, source, pmid, author_key, wcm_author, top_cwid, top_name, "
            "dup_reason, resolved_at "
            "FROM authorship_review "
            "WHERE status='open' AND pmid IS NOT NULL AND wcm_author IS NOT NULL "
            "ORDER BY id"
        )).mappings().all()
    return [dict(r) for r in rows]


def _matched_spelling(byline, cwid, pos, names):
    """WHICH of the owner's roster spellings the byline matched -- reproduced by feeding
    orch._byline_owner one spelling at a time, so the audit trail records the real
    predicate's verdict, never a re-implementation that could drift from it."""
    for spelling in names:
        if orch._byline_owner(byline, ((cwid, pos, (spelling,)),)):
            return spelling
    return None


def _resolve_owners(rows, attr):
    """[{row, owner, author_position, matched_spelling, n_spellings}] for every pool row
    whose byline resolves to an already-attributed person for that pmid. Tripwire: an
    owner whose match cannot be reproduced spelling-by-spelling means
    orch._byline_owner's semantics changed under us -- raise, never write."""
    hits = []
    for r in rows:
        entry = attr.get(r["pmid"], ())
        owner = orch._byline_owner(r["wcm_author"], entry)
        if not owner:
            continue
        pos, names = next((p, n) for c, p, n in entry if _cwid_eq(c, owner))
        spelling = _matched_spelling(r["wcm_author"], owner, pos, names)
        if spelling is None:
            raise AssertionError(
                f"row {r['id']}: _byline_owner resolved byline '{r['wcm_author']}' to "
                f"{owner} but no single spelling of {names} reproduces the match -- "
                "the resolver's semantics changed; refusing to trust any verdict.")
        hits.append({"row": r, "owner": owner, "author_position": pos,
                     "matched_spelling": spelling, "n_spellings": len(names)})
    return hits


# ============================================================================
# ACCEPTED confirmation -- which holding record establishes ownership
# ============================================================================
def _fetch_person_article(engine, pmids):
    """pmid -> [person_article rows]. Fetched by pmid alone (BIGINT -- no collation in
    play); the owner-cwid comparison happens in Python via _cwid_eq."""
    from sqlalchemy import text, bindparam
    pmids = sorted({int(p) for p in pmids})
    out = {}
    if not pmids:
        return out
    stmt = text("SELECT pmid, personIdentifier, userAssertion, "
                "articleAuthorNameFirstName, articleAuthorNameLastName "
                "FROM person_article WHERE pmid IN :ps") \
        .bindparams(bindparam("ps", expanding=True))
    with engine.connect() as c:
        for i in range(0, len(pmids), 1000):
            for row in c.execute(stmt, {"ps": pmids[i:i + 1000]}).mappings():
                out.setdefault(int(row["pmid"]), []).append(dict(row))
    return out


def _confirm(hits, pa_by_pmid, gold):
    """Annotate each hit in place with the holding evidence:
      pa_row    the owner's ACCEPTED person_article record for this pmid, or None
      via_pa    bool -- that record exists
      via_gold  bool -- pmid in the owner's GoldStandard.knownpmids"""
    for h in hits:
        pmid, owner = h["row"]["pmid"], h["owner"]
        pa_row = next(
            (p for p in pa_by_pmid.get(int(pmid), ())
             if _cwid_eq(p["personIdentifier"], owner)
             and (p["userAssertion"] or "").strip().upper() == "ACCEPTED"),
            None)
        known, _rej = gold.get(owner, (set(), set()))
        h["pa_row"] = pa_row
        h["via_pa"] = pa_row is not None
        h["via_gold"] = int(pmid) in known
    return hits


def _partition(hits):
    """(eligible, unconfirmed): a hit is eligible for dismissal only when the ACCEPT
    itself is confirmed by person_article and/or GoldStandard -- see METHODOLOGY."""
    eligible = [h for h in hits if h["via_pa"] or h["via_gold"]]
    unconfirmed = [h for h in hits if not (h["via_pa"] or h["via_gold"])]
    return eligible, unconfirmed


def _signal(via_pa, via_gold):
    if via_pa and via_gold:
        return "person_article ACCEPTED + GoldStandard knownpmids"
    return "person_article ACCEPTED" if via_pa else "GoldStandard knownpmids"


def _reason(byline, owner, pmid, signal):
    return (f"{REASON_PREFIX}: byline '{byline}' resolves to {owner} who already "
            f"holds pmid {pmid} via {signal}")[:255]


# ============================================================================
# ledger + write
# ============================================================================
def _ledger_entry(h, run_ts, applied):
    """One self-contained audit record: enough to re-derive the verdict without the
    database -- the row's pre-write identity/state, the resolved owner, WHICH byline
    spelling matched, and WHICH holding record(s) establish ownership."""
    r = h["row"]
    sp = h["matched_spelling"]
    signal = _signal(h["via_pa"], h["via_gold"])
    pa = h["pa_row"]
    return {
        "id": r["id"], "rule": "byline_owner_already_attributed",
        "issue": "#181 population (b) / PM#937",
        "source": r["source"], "pmid": r["pmid"], "author_key": r["author_key"],
        "wcm_author": r["wcm_author"],
        "before": {"status": "open", "top_cwid": r["top_cwid"], "top_name": r["top_name"],
                   "dup_reason": r["dup_reason"], "resolved_at": r["resolved_at"]},
        "owner": {
            "cwid": h["owner"],
            "proposed_is_owner": bool(r["top_cwid"]) and _cwid_eq(r["top_cwid"], h["owner"]),
            "byline_match": {
                "byline": r["wcm_author"],
                "matched_spelling": {"first": sp[0], "last": sp[1]},
                "spellings_on_record": h["n_spellings"],
                "analysis_summary_author_position": h["author_position"],
            },
            "holding": {
                "person_article": ({"userAssertion": pa["userAssertion"],
                                    "articleAuthorNameFirstName": pa["articleAuthorNameFirstName"],
                                    "articleAuthorNameLastName": pa["articleAuthorNameLastName"]}
                                   if pa else None),
                "goldstandard_knownpmids": h["via_gold"],
                "signal": signal,
            },
        },
        "after": {"status": "dismissed",
                  "dup_reason": _reason(r["wcm_author"], h["owner"], r["pmid"], signal),
                  "resolved_at": run_ts},
        "swept_at": run_ts, "applied": applied,
    }


def _write_ledger(path, entries):
    with open(path, "w") as fh:
        for e in entries:
            fh.write(json.dumps(e, default=str) + "\n")


def _apply(engine, eligible, run_ts):
    from sqlalchemy import text
    if not eligible:
        return 0
    stmt = text(_UPDATE_SQL)
    n = 0
    with engine.begin() as c:
        for i in range(0, len(eligible), 500):
            chunk = eligible[i:i + 500]
            params = [{"id": h["row"]["id"], "ts": run_ts,
                       "reason": _reason(h["row"]["wcm_author"], h["owner"],
                                         h["row"]["pmid"],
                                         _signal(h["via_pa"], h["via_gold"]))}
                      for h in chunk]
            # rowcount, not len(chunk): a row resolved concurrently (curator, or an
            # aar_reconcile_open.py --apply) is correctly skipped by the status='open'
            # guard, and must not be counted as written.
            n += c.execute(stmt, params).rowcount
    return n


# ============================================================================
# --check <ids>
# ============================================================================
def _check_ids(engine, ids):
    from sqlalchemy import text, bindparam
    stmt = text("SELECT id, source, pmid, author_key, wcm_author, top_cwid, top_name, "
                "dup_reason, resolved_at, status FROM authorship_review WHERE id IN :ids") \
        .bindparams(bindparam("ids", expanding=True))
    with engine.connect() as c:
        rows = [dict(r) for r in c.execute(stmt, {"ids": list(ids)}).mappings()]
    by_id = {r["id"]: r for r in rows}
    pmids = sorted({r["pmid"] for r in rows if r["pmid"]})
    attr = gate.attributions(pmids) if pmids else {}
    pool = [r for r in rows if r["status"] == "open" and r["pmid"] and r["wcm_author"]]
    hits = _resolve_owners(pool, attr)
    pa = _fetch_person_article(engine, [h["row"]["pmid"] for h in hits])
    gold = orch._batch_gold_standard([h["owner"] for h in hits]) if hits else {}
    _confirm(hits, pa, gold)
    h_by_id = {h["row"]["id"]: h for h in hits}

    print(f"\n==== --check verdicts for {ids} ====\n")
    for rid in ids:
        r = by_id.get(rid)
        if r is None:
            print(f"id={rid}: NOT FOUND in authorship_review")
            continue
        if r["status"] != "open":
            print(f"id={rid}: status={r['status']} (not 'open' -- out of scope, shown "
                  "for context only)")
            continue
        h = h_by_id.get(rid)
        print(f"id={rid}  source={r['source']}  pmid={r['pmid']}  "
              f"byline='{r['wcm_author']}'  proposed={r['top_cwid']}")
        if h is None:
            print("  VERDICT: NO ACTION -- byline resolves to no attributed owner\n")
            continue
        sp = h["matched_spelling"]
        print(f"  owner  : {h['owner']} (matched spelling '{sp[0]} {sp[1]}', "
              f"{h['n_spellings']} on record)")
        print(f"  holding: via_pa={h['via_pa']} via_gold={h['via_gold']}")
        if h["via_pa"] or h["via_gold"]:
            print(f"  VERDICT: DISMISS -- {_reason(r['wcm_author'], h['owner'], r['pmid'], _signal(h['via_pa'], h['via_gold']))}\n")
        else:
            print("  VERDICT: NO ACTION -- owner attributed but ACCEPT unconfirmed "
                  "(score>=30 residual; left open)\n")


# ============================================================================
# driver
# ============================================================================
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="perform the dismissals for real; default is a read-only dry run")
    ap.add_argument("--ledger",
                    default=os.path.join(HERE, "aar_dismiss_byline_owner_ledger.jsonl"),
                    help="JSONL audit-ledger path (written on every run, dry-run included)")
    ap.add_argument("--limit", type=int, default=None,
                    help="cap the candidate pool (fast sanity runs; the resulting counts "
                         "are NOT the population)")
    ap.add_argument("--check", type=int, nargs="*", default=None,
                    help="only report verdicts for these authorship_review ids and exit "
                         "-- no ledger write")
    ap.add_argument("--selftest", action="store_true", help="offline checks, no DB/network")
    args = ap.parse_args()

    if args.selftest:
        sys.exit(0 if _selftest() else 1)

    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print("Resolved producer modules (must all live under this file's directory):")
    for _mod in (aar_db, gate, matcher, orch, rcp, idxmod):
        print(f"  {_mod.__name__:26} {_mod.__file__}")
    print()

    engine = aar_db.engine()

    if args.check:
        _check_ids(engine, args.check)
        return

    print("==== byline-owner dismissal sweep (#181 population (b) / PM#937) ====")
    pool = _open_pool(engine)
    src = {}
    for r in pool:
        src[r["source"]] = src.get(r["source"], 0) + 1
    print(f"  open rows with a pmid and a byline (candidate pool): {len(pool)}  "
          f"(source breakdown: {src})", flush=True)
    if not pool:
        _die("candidate pool is EMPTY. A live queue of ~17k open rows cannot "
             "legitimately produce zero candidates -- check DB_* env / connectivity "
             "before believing any zero.")
    if args.limit:
        pool = pool[:args.limit]
        print(f"  --limit {args.limit}: pool capped to {len(pool)} rows -- counts below "
              f"are a sanity sample, NOT the population", flush=True)

    pmids = sorted({r["pmid"] for r in pool})
    attr = gate.attributions(pmids)
    print(f"  gate.attributions() resolved {len(attr)} of {len(pmids)} distinct pmids "
          f"to at least one attributed person", flush=True)
    if not attr:
        _die(f"attributions() resolved 0 of {len(pmids)} pmids. On this queue most "
             "pmids have SOME attributed co-author; zero means the analysis_summary_"
             "author join silently broke, not that nothing is attributed.")

    hits = _resolve_owners(pool, attr)
    print(f"  byline resolves to an already-attributed owner: {len(hits)} rows", flush=True)

    pa_by_pmid = _fetch_person_article(engine, [h["row"]["pmid"] for h in hits])
    n_pa = sum(len(v) for v in pa_by_pmid.values())
    print(f"  person_article fetched for confirmation: {n_pa} rows across "
          f"{len(pa_by_pmid)} pmids", flush=True)
    if hits and n_pa == 0:
        _die("byline-owner hits exist but the person_article confirmation fetch "
             "returned ZERO rows -- every hit pmid is in analysis_summary_author, "
             "which is BUILT from person_article, so zero here is a broken query, "
             "not an empty result.")

    gold = orch._batch_gold_standard(sorted({h["owner"] for h in hits})) if hits else {}
    print(f"  GoldStandard.knownpmids fetched for {len(gold)} distinct owners (batched)",
          flush=True)

    _confirm(hits, pa_by_pmid, gold)
    eligible, unconfirmed = _partition(hits)
    if hits and not eligible:
        _die(f"{len(hits)} byline-owner hits but ZERO confirmed ACCEPTED. PR #176 "
             "measured every hit of this class as ACCEPTED; an all-zero confirmation "
             "is the silent collation/comparison failure shape, not a real result.")

    via_pa_n = sum(1 for h in eligible if h["via_pa"])
    via_gold_only_n = sum(1 for h in eligible if h["via_gold"] and not h["via_pa"])
    overlap_n = sum(1 for h in eligible
                    if h["row"]["top_cwid"] and _cwid_eq(h["row"]["top_cwid"], h["owner"]))
    null_prop_n = sum(1 for h in eligible if not h["row"]["top_cwid"])
    print(f"\n  ELIGIBLE for dismissal (owner's ACCEPT confirmed): {len(eligible)}")
    print(f"    via person_article ACCEPTED: {via_pa_n}")
    print(f"    via GoldStandard knownpmids ONLY (ETL lag): {via_gold_only_n}")
    print(f"    owner == stored proposal (aar_reconcile_open CLASS-A overlap): {overlap_n}")
    print(f"    owner != stored proposal (the population only this tool reaches): "
          f"{len(eligible) - overlap_n - null_prop_n}")
    print(f"    stored proposal is NULL (rows PR #180 nulled): {null_prop_n}")
    print(f"  owner attributed but ACCEPT unconfirmed (score>=30 residual, LEFT OPEN): "
          f"{len(unconfirmed)}")
    if unconfirmed:
        for h in unconfirmed[:5]:
            print(f"    e.g. id={h['row']['id']} byline='{h['row']['wcm_author']}' "
                  f"owner={h['owner']}")

    entries = [_ledger_entry(h, run_ts, args.apply) for h in eligible]
    assert len(entries) == len(eligible), "ledger/eligible count mismatch"
    _write_ledger(args.ledger, entries)
    print(f"\n  ledger -> {args.ledger} ({len(entries)} rows)")

    if args.apply:
        n = _apply(engine, eligible, run_ts)
        print(f"\n  APPLIED: {n} rows dismissed "
              f"({len(eligible) - n} skipped -- resolved concurrently)")
    else:
        print(f"\n  DRY RUN -- 0 rows written. Re-run with --apply to dismiss these "
              f"{len(eligible)} rows for real.")


# ============================================================================
# self-test
# ============================================================================
def _selftest():
    """Offline: no DB, no network. Proves the pieces that don't need live data: the
    resolver reuse (not a re-implementation), the matched-spelling reproduction and its
    tripwire, the ACCEPTED-confirmation partition, the reason format, the write
    statement's guards, and the ledger entry's audit completeness."""
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  [{'OK' if cond else '** FAIL'}] {label}")

    check("_cwid_eq is rcp._cwid_eq (imported, not redefined)", _cwid_eq is rcp._cwid_eq)

    # matched-spelling reproduction runs the REAL orch._byline_owner per spelling
    entry = ("aer2006", "last", (("Anthony", "Rosen"), ("Tony", "Rosen")))
    sp = _matched_spelling("Tony Rosen", *entry)
    check("matched spelling for byline 'Tony Rosen' is ('Tony','Rosen'), not the "
          "legal name", sp == ("Tony", "Rosen"))
    sp2 = _matched_spelling("Anthony E Rosen", *entry)
    check("matched spelling for byline 'Anthony E Rosen' is ('Anthony','Rosen')",
          sp2 == ("Anthony", "Rosen"))
    check("a bare-initial byline matches no spelling (the resolver's own rule)",
          _matched_spelling("T Rosen", *entry) is None)

    # _resolve_owners end-to-end on fabricated rows, real resolver
    rows = [
        {"id": 1, "pmid": 100, "wcm_author": "Tony Rosen"},
        {"id": 2, "pmid": 100, "wcm_author": "Leah Teresa Rosen"},
        {"id": 3, "pmid": 200, "wcm_author": "Tony Rosen"},   # pmid with no attributions
    ]
    attr = {100: [("aer2006", "last", (("Anthony", "Rosen"), ("Tony", "Rosen")))]}
    hits = _resolve_owners(rows, attr)
    check("row 1's byline resolves to aer2006; rows 2 and 3 resolve to nobody",
          [h["row"]["id"] for h in hits] == [1]
          and hits[0]["owner"] == "aer2006"
          and hits[0]["matched_spelling"] == ("Tony", "Rosen"))

    # tripwire: an owner whose match cannot be reproduced spelling-by-spelling raises
    orig = orch._byline_owner
    try:
        orch._byline_owner = (lambda byline, attributed:
                              "aer2006" if len(attributed[0][2]) > 1 else None)
        raised = False
        try:
            _resolve_owners([rows[0]], attr)
        except AssertionError:
            raised = True
        check("irreproducible byline match raises instead of writing", raised)
    finally:
        orch._byline_owner = orig

    # ACCEPTED-confirmation partition
    def hit(rid, via_pa, via_gold):
        pa_rows = ([{"personIdentifier": "aer2006", "userAssertion": "ACCEPTED",
                     "articleAuthorNameFirstName": "Tony",
                     "articleAuthorNameLastName": "Rosen"}] if via_pa else [])
        h = {"row": {"id": rid, "pmid": 100, "wcm_author": "Tony Rosen",
                     "author_key": "100:5", "source": "pubmed", "top_cwid": "ltr4001",
                     "top_name": "Leah Teresa Rosen", "dup_reason": None,
                     "resolved_at": None},
             "owner": "aer2006", "author_position": "last",
             "matched_spelling": ("Tony", "Rosen"), "n_spellings": 2}
        _confirm([h], {100: pa_rows}, {"aer2006": ({100} if via_gold else set(), set())})
        return h

    eligible, unconfirmed = _partition([hit(1, True, False), hit(2, False, True),
                                        hit(3, False, False), hit(4, True, True)])
    check("person_article-only and gold-only hits are both eligible; neither-signal "
          "is not", [h["row"]["id"] for h in eligible] == [1, 2, 4]
          and [h["row"]["id"] for h in unconfirmed] == [3])
    check("_confirm matches the owner cwid case-insensitively",
          _confirm([{"row": {"pmid": 100}, "owner": "AER2006"}],
                   {100: [{"personIdentifier": "aer2006", "userAssertion": "ACCEPTED",
                           "articleAuthorNameFirstName": "T",
                           "articleAuthorNameLastName": "R"}]},
                   {})[0]["via_pa"])
    check("_confirm does NOT accept a non-ACCEPTED assertion",
          not _confirm([{"row": {"pmid": 100}, "owner": "aer2006"}],
                       {100: [{"personIdentifier": "aer2006", "userAssertion": "",
                               "articleAuthorNameFirstName": "T",
                               "articleAuthorNameLastName": "R"}]},
                       {})[0]["via_pa"])

    check("signal strings distinguish the three confirmations",
          _signal(True, True) == "person_article ACCEPTED + GoldStandard knownpmids"
          and _signal(True, False) == "person_article ACCEPTED"
          and _signal(False, True) == "GoldStandard knownpmids")

    r = _reason("X" * 300, "aer2006", 42424133, _signal(True, False))
    check("reason fits dup_reason VARCHAR(255) and keeps its greppable prefix",
          len(r) == 255 and r.startswith(REASON_PREFIX + ":"))

    check("the write statement re-checks status='open' at write time",
          "status='open'" in _UPDATE_SQL)
    set_clause = _UPDATE_SQL.split("WHERE")[0]
    check("the write statement touches ONLY status/resolved_at/dup_reason -- never "
          "curator columns",
          all(col not in set_clause for col in
              ("note", "resolution_cwid", "reviewer", "snooze_until"))
          and all(col in set_clause for col in ("status", "resolved_at", "dup_reason")))

    e = _ledger_entry(hit(1, True, True), "2026-08-31 00:00:00", applied=False)
    check("ledger entry carries the full audit trail: before-state, owner, byline "
          "match, holding record",
          e["before"]["top_cwid"] == "ltr4001"
          and e["owner"]["cwid"] == "aer2006"
          and e["owner"]["byline_match"]["matched_spelling"] == {"first": "Tony",
                                                                "last": "Rosen"}
          and e["owner"]["holding"]["person_article"]["userAssertion"] == "ACCEPTED"
          and e["owner"]["holding"]["goldstandard_knownpmids"] is True
          and e["after"]["status"] == "dismissed"
          and e["applied"] is False)
    check("ledger entry records that the owner is NOT the stored proposal",
          e["owner"]["proposed_is_owner"] is False)

    print("\nSELFTEST", "PASS" if ok else "FAIL")
    return ok


if __name__ == "__main__":
    main()
