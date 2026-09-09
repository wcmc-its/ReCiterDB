#!/usr/bin/env python3
"""
One-shot backfill for the comma-duplicated middleName artifact in AAR display names.

WHAT IT FIXES
  The homonym queue rendered a candidate as "Dylan Kwon,Kwon Kim" (dkk4001). The
  identity is fine; `identity.middleName` arrives from the WCM Enterprise Directory
  legal-name field already holding "Kwon,Kwon", and `identity_index._display_name`
  printed it faithfully. `_display_middle()` now collapses that for DISPLAY ONLY.

  But `authorship_review.top_name` and `.candidate_cwids_json` are WRITTEN ONCE PER
  REFRESH AND PERSISTED -- the producer's columns never self-heal. Rows already carrying
  the doubled string keep it until their next refresh, which for a resolved or snoozed
  row may be never. This rewrites those two columns in place.

  candidates()' own comment measured the population: 392 of the 910 multi-token
  middleNames are comma-joined concatenations of separately recorded names.

WHAT IT DOES NOT DO
  - Does not touch the identity record. The comma is upstream ED data and is left alone;
    only the rendered label changes. Nothing here writes to `identity`.
  - Does not touch curator columns (status, resolution_cwid, reviewer, note,
    snooze_until) or any scoring/producer column other than the two NAME columns.
  - Does not re-run the matcher, re-fetch anything, or change WHICH candidates a row
    carries. A candidate's cwid, scores and flags are preserved byte for byte; only its
    `name` string is rewritten.
  - Does not "fix" a name that differs from the expected old rendering for any other
    reason -- see SAFETY.

SAFETY: every write is a targeted, verified swap
  For each affected cwid this computes BOTH renderings from the same identity record:
  the OLD one (what `_display_name` produced before, i.e. with the raw middleName) and
  the NEW one (with `_display_middle` applied). A row's `top_name` is only rewritten
  when it EQUALS the old rendering exactly. Anything else -- a hand-edited label, a name
  written by an older matcher generation, an unrelated string -- is left untouched and
  counted as `skipped_mismatch`. The same rule applies per candidate inside
  candidate_cwids_json.

  That makes the pass idempotent: a second run finds the new string, which never equals
  the old one, and writes nothing.

  Rows are updated by primary key. The JSON is rewritten by parsing, replacing only the
  `name` field of matching entries, and re-serialising -- key order and every other
  field are preserved via json.loads/json.dumps on the same dict.

USAGE
  python backfill_display_name_dedupe.py --selftest   # offline, no DB
  python backfill_display_name_dedupe.py              # DRY RUN, writes nothing
  python backfill_display_name_dedupe.py --apply      # perform the writes
  python backfill_display_name_dedupe.py --limit 50   # cap rows scanned (sanity check)

  A ledger of every intended change is written regardless of --apply, so a dry run
  produces the full reviewable diff.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)  # must be the LAST insert(0, ...) before these imports
import aar_db
import identity_index as idxmod
from identity_index import _display_middle, _display_name

# The sys.path fork trap: a stale copy of the producer lives in
# ~/Dropbox/Projects/ReCiter Research/scripts/. Computing the "old" rendering against the
# wrong module would produce a string that never matches, silently writing nothing -- or
# worse, one that matches rows it should not.
for _mod in (aar_db, idxmod):
    assert os.path.dirname(os.path.abspath(_mod.__file__)) == HERE, (
        f"{_mod.__name__} resolved to {_mod.__file__}, not {HERE} -- "
        "sys.path fork trap: a stale producer copy is winning.")

NAME_COLUMNS = ("top_name", "candidate_cwids_json")

# Both name columns must be ones the producer itself refreshes; if either is renamed,
# fail at import rather than no-op silently. (Same guard aar_sweep_stale uses.)
_refresh = getattr(aar_db, "_REFRESH_COLS", None)
if _refresh:
    for _c in NAME_COLUMNS:
        assert _c in _refresh, (
            f"{_c} is not in aar_db._REFRESH_COLS -- column renamed? Refusing to run "
            "a backfill that may be writing a column the producer no longer owns.")


def _legacy_display_name(rec):
    """What identity_index._display_name emitted BEFORE the dedupe landed.

    Deliberately a plain restatement of the old function rather than a trick to make the
    current one behave like the old one: this is the string the backfill matches stored
    rows against, so it must be obvious and checkable. It mirrors _display_name exactly
    except that the middle component is the RAW field. The selftest pins the two together
    on a record with no comma, where they must agree byte for byte -- if _display_name's
    structure ever changes, that check fails and this stops being trusted."""
    legal = " ".join(x for x in (rec.get("given"), rec.get("middle"),
                                 rec.get("surname")) if x)
    pref = (rec.get("pref") or "").strip()
    if not pref or idxmod._norm(pref) == idxmod._norm(rec.get("given")):
        return legal
    publishing = " ".join(x for x in (pref, rec.get("surname")) if x)
    if idxmod._norm(publishing) == idxmod._norm(legal):
        return legal
    return f"{publishing} (HR: {legal})"


def old_and_new_display(rec):
    """(old_label, new_label) for one identity record, or (None, None) when unaffected."""
    raw = rec.get("middle") or ""
    if _display_middle(raw) == raw:
        return None, None
    old, new = _legacy_display_name(rec), _display_name(rec)
    return (old, new) if old != new else (None, None)


def affected_identities(engine):
    """Every cwid whose identity.middleName carries a comma. Small (910 multi-token rows
    total on prod, 392 of them comma-joined), so this is a single scan, not a join."""
    from sqlalchemy import text
    # `pref` is person.firstName (the DynamoDB primaryName mirror) and is load-bearing:
    # _display_name renders "Publishing Name (HR: Legal Name)" whenever it disagrees with
    # identity.givenName. Omitting it would reconstruct the wrong OLD label for exactly
    # those people, and every one of their rows would be skipped as a mismatch -- a silent
    # under-fix rather than a visible error. LEFT join: a cwid with no person row keeps
    # pref empty, which is the same branch as "agrees".
    with engine.connect() as c:
        rows = c.execute(text(
            "SELECT i.personIdentifier AS cwid, i.givenName, i.middleName, i.surname, "
            "       p.firstName AS pref "
            "FROM identity i "
            "LEFT JOIN person p ON p.personIdentifier = i.personIdentifier "
            "WHERE i.middleName LIKE '%,%'")).mappings().all()
    out = {}
    for r in rows:
        rec = {"given": r["givenName"] or "", "middle": r["middleName"] or "",
               "surname": r["surname"] or "", "pref": r["pref"] or ""}
        old, new = old_and_new_display(rec)
        if old and new and old != new:
            out[r["cwid"]] = (old, new)
    return out


def rows_to_fix(engine, affected, limit=None):
    """Rows whose top_name or candidate_cwids_json mentions an affected cwid.

    Selected by cwid, then verified by exact string match in plan_row -- the LIKE is only
    a cheap prefilter, never the thing that authorises a write."""
    from sqlalchemy import bindparam, text
    if not affected:
        return []
    cwids = list(affected)
    # top_cwid is an exact IN; the JSON column gets one LIKE per cwid, which is only a
    # cheap prefilter -- plan_row re-checks every value by exact string match before
    # anything is written, so a LIKE that over-matches costs a skipped row, never a bad
    # write. Quoted as `"cwid"` so a short cwid cannot match inside another field's value.
    likes = " OR ".join(f"candidate_cwids_json LIKE :c{i}" for i in range(len(cwids)))
    stmt = text(
        "SELECT id, top_cwid, top_name, candidate_cwids_json FROM authorship_review "
        f"WHERE top_cwid IN :cwids OR ({likes})"
    ).bindparams(bindparam("cwids", expanding=True))
    params = {"cwids": cwids}
    params.update({f"c{i}": f'%"{cw}"%' for i, cw in enumerate(cwids)})
    with engine.connect() as c:
        rows = [dict(r) for r in c.execute(stmt, params).mappings().all()]
    return rows[:limit] if limit else rows


def plan_row(row, affected):
    """What this row should become. Returns (updates, skipped) where `updates` is a dict
    of column -> new value (empty when nothing to do) and `skipped` counts values that
    named an affected cwid but did NOT match the expected old rendering."""
    updates, skipped = {}, []

    cwid = row.get("top_cwid")
    if cwid in affected:
        old, new = affected[cwid]
        if row.get("top_name") == old:
            updates["top_name"] = new
        elif row.get("top_name") not in (None, "", new):
            skipped.append(("top_name", cwid, row.get("top_name"), old))

    raw = row.get("candidate_cwids_json")
    if raw:
        try:
            cands = json.loads(raw)
        except (ValueError, TypeError):
            cands = None
        if isinstance(cands, list):
            changed = False
            for cand in cands:
                if not isinstance(cand, dict):
                    continue
                cw = cand.get("cwid")
                if cw not in affected:
                    continue
                old, new = affected[cw]
                if cand.get("name") == old:
                    cand["name"] = new
                    changed = True
                elif cand.get("name") not in (None, "", new):
                    skipped.append(("candidate", cw, cand.get("name"), old))
            if changed:
                updates["candidate_cwids_json"] = json.dumps(cands, ensure_ascii=False)
    return updates, skipped


def apply_updates(engine, planned):
    """One UPDATE per row, by primary key, touching only the two name columns."""
    from sqlalchemy import text
    n = 0
    with engine.begin() as c:
        for row_id, updates in planned:
            sets = ", ".join(f"{col}=:{col}" for col in updates)
            params = dict(updates)
            params["id"] = row_id
            c.execute(text(f"UPDATE authorship_review SET {sets} WHERE id=:id"), params)
            n += 1
    return n


def _selftest():
    checks = []
    rec = {"given": "Dylan", "middle": "Kwon,Kwon", "surname": "Kim", "pref": ""}
    old, new = old_and_new_display(rec)
    checks += [
        ("the OLD rendering is reconstructed exactly as the queue showed it",
         old == "Dylan Kwon,Kwon Kim"),
        ("the NEW rendering reads the name once", new == "Dylan Kwon Kim"),
    ]

    clean = {"given": "John", "middle": "Andrew", "surname": "Kim", "pref": ""}
    checks.append(("a record with no comma is not affected at all",
                   old_and_new_display(clean) == (None, None)))
    # Pins the local restatement to the real function. If _display_name's structure ever
    # changes, this fails and the backfill stops being trusted rather than quietly
    # matching nothing.
    for probe in (clean,
                  {"given": "Qi", "middle": "Wing", "surname": "Lee", "pref": "Guinevere"},
                  {"given": "Mila", "middle": "", "surname": "Sun", "pref": "Mila"},
                  {"given": "Shuo", "middle": "", "surname": "Sun", "pref": "Mila"}):
        checks.append((
            f"legacy renderer agrees with _display_name on an unaffected record "
            f"({probe['given']}/{probe['pref']})",
            _legacy_display_name(probe) == _display_name(probe)))
    # ...and the publishing-name branch still reaches the affected population.
    pref_rec = {"given": "Shuo", "middle": "Kwon,Kwon", "surname": "Sun", "pref": "Mila"}
    old_p, new_p = old_and_new_display(pref_rec)
    checks += [
        ("a person whose publishing name differs still gets both renderings",
         old_p == "Mila Sun (HR: Shuo Kwon,Kwon Sun)"),
        ("...with the duplication collapsed only in the new one",
         new_p == "Mila Sun (HR: Shuo Kwon Sun)"),
    ]

    affected = {"dkk4001": ("Dylan Kwon,Kwon Kim", "Dylan Kwon Kim")}

    row = {"id": 1, "top_cwid": "dkk4001", "top_name": "Dylan Kwon,Kwon Kim",
           "candidate_cwids_json": json.dumps([
               {"cwid": "dkk4001", "name": "Dylan Kwon,Kwon Kim", "io_score": 1.5},
               {"cwid": "other1", "name": "Someone Else", "io_score": 0.2}])}
    upd, skipped = plan_row(row, affected)
    cands = json.loads(upd["candidate_cwids_json"])
    checks += [
        ("top_name is rewritten", upd["top_name"] == "Dylan Kwon Kim"),
        ("the candidate's name is rewritten", cands[0]["name"] == "Dylan Kwon Kim"),
        ("...and every other field on that candidate survives untouched",
         cands[0]["io_score"] == 1.5 and cands[0]["cwid"] == "dkk4001"),
        ("an unaffected candidate is left exactly as it was",
         cands[1] == {"cwid": "other1", "name": "Someone Else", "io_score": 0.2}),
        ("nothing is skipped when everything matched", skipped == []),
    ]

    # Idempotence: the same row, already fixed, must produce no writes.
    fixed = {"id": 1, "top_cwid": "dkk4001", "top_name": "Dylan Kwon Kim",
             "candidate_cwids_json": json.dumps([
                 {"cwid": "dkk4001", "name": "Dylan Kwon Kim"}])}
    upd2, skipped2 = plan_row(fixed, affected)
    checks += [
        ("a second run writes nothing", upd2 == {}),
        ("...and does not report it as a mismatch either", skipped2 == []),
    ]

    # A label that is neither the old nor the new string is NOT touched.
    odd = {"id": 2, "top_cwid": "dkk4001", "top_name": "Hand Edited Name",
           "candidate_cwids_json": None}
    upd3, skipped3 = plan_row(odd, affected)
    checks += [
        ("an unexpected label is left alone rather than overwritten", upd3 == {}),
        ("...and is reported so it can be eyeballed", len(skipped3) == 1),
    ]

    # Malformed JSON must not throw or clobber.
    bad = {"id": 3, "top_cwid": None, "top_name": None,
           "candidate_cwids_json": "{not json"}
    checks.append(("malformed candidate JSON is skipped, not crashed on",
                   plan_row(bad, affected) == ({}, [])))

    failed = [label for label, ok in checks if not ok]
    for label, ok in checks:
        print(f"  [{'OK' if ok else 'FAIL'}] {label}")
    if failed:
        print(f"\nSELFTEST FAIL ({len(failed)})")
        return 1
    print(f"\nSELFTEST PASS ({len(checks)} checks)")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="perform the writes; without it this is a dry run")
    ap.add_argument("--limit", type=int, default=None,
                    help="cap the number of rows considered")
    ap.add_argument("--ledger", default=os.path.join(HERE, "backfill_display_name_dedupe_ledger.jsonl"),
                    help="where to write the per-row record of intended changes")
    ap.add_argument("--selftest", action="store_true",
                    help="offline checks only, no DB")
    args = ap.parse_args()

    if args.selftest:
        return _selftest()

    engine = aar_db.engine()
    affected = affected_identities(engine)
    print(f"identities with a comma-duplicated middleName: {len(affected)}")
    if not affected:
        print("nothing to do")
        return 0

    rows = rows_to_fix(engine, affected, args.limit)
    print(f"authorship_review rows naming one of them: {len(rows)}")

    planned, all_skipped = [], []
    for row in rows:
        updates, skipped = plan_row(row, affected)
        all_skipped += [(row["id"],) + s for s in skipped]
        if updates:
            planned.append((row["id"], updates))

    stamp = datetime.now(timezone.utc).isoformat()
    with open(args.ledger, "a") as fh:
        for row_id, updates in planned:
            fh.write(json.dumps({"ts": stamp, "id": row_id, "applied": bool(args.apply),
                                 "updates": updates}) + "\n")

    print(f"rows to rewrite: {len(planned)}")
    print(f"values naming an affected cwid but NOT matching the expected old label "
          f"(left untouched): {len(all_skipped)}")
    for entry in all_skipped[:10]:
        print(f"    row {entry[0]} {entry[1]} {entry[2]}: stored={entry[3]!r} "
              f"expected_old={entry[4]!r}")
    if len(all_skipped) > 10:
        print(f"    ... and {len(all_skipped) - 10} more")

    if not args.apply:
        print(f"\n  DRY RUN -- 0 rows written. Ledger: {args.ledger}")
        print("  Re-run with --apply to perform these rewrites.")
        return 0

    n = apply_updates(engine, planned)
    print(f"\n  rewrote {n} rows. Ledger: {args.ledger}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
