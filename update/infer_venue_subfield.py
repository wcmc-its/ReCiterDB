#!/usr/bin/env python3
"""
Give every venue on the external AAR lanes a Science-Metrix subfield.

`venue_fit.py` scores how well a venue fits a candidate's department, and can only do
that for a venue that has a category. `journal_science_metrix` covers the journals
Science-Metrix lists; Scopus and OpenAlex rows also arrive from journals it does not
list, and from conference proceedings and books that have no ISSN at all. On the Cornell
Scopus rows of 2026-11-16 that was 13,465 of 40,851 rows with no category. This script
classifies those venues by title, which took coverage to 40,846 of 40,851.

What it does, in its default mode:
  1. Reads the distinct venues on authorship_review (source scopus/openalex) and keeps
     the ones with no category: an ISSN that is in journal_science_metrix neither as issn
     nor as eissn, or no ISSN and a title whose `venue_fit.venue_key` is not in
     venue_subfield_inferred. Most-used venues first, --limit of them per run.
  2. Sends them to Claude Haiku on Amazon Bedrock in batches of 40, with the taxonomy
     (every distinct subfield in journal_science_metrix) in the system prompt.
  3. Keeps an answer only when its subfield is one of those, copied verbatim. Anything
     else is dropped and the venue stays unclassified, to be tried again next run.
     A title that names nothing ("undefined", "(2023)") is never sent: see
     venue_fit.venue_key.
  4. Writes ISSN venues into journal_science_metrix with smsid >= 800001, domain, field
     and subfield_id copied from the lowest-smsid row of that subfield, exactly as the
     earlier person_article augmentation did; and no-ISSN venues into
     venue_subfield_inferred under their venue key.

Bedrock rather than a hosted API key: boto3 is already in the image and the CronJob's
IRSA role signs the call, so there is no secret to store or rotate. The role needs
bedrock:InvokeModel on the Haiku inference profile and its foundation model.

FAIL-NEUTRAL. A venue with no category contributes no venue term, which is exactly
today's behaviour, so nothing downstream depends on this step succeeding. Any Bedrock
failure (no permission, throttling, an unreadable answer) is logged, the answers already
in hand are still written, and the script exits 0.

One-off loads, both honouring dry-run/--apply:
  --import-science-metrix <ScienceMetrix.json>   ReCiter's copy of the journal list holds
        47,281 rows against 19,866 here. Inserts the journals whose ISSN is not in the
        table yet, under their own smsid.
  --import-json <file>   venues already classified by the research prototype (keys
        "issn:XXXX-XXXX" / "title:<exact title>", values {title, subfield, field,
        domain}), so they need no model call.

Env: DB_USERNAME/DB_PASSWORD/DB_HOST/DB_NAME (reciterdb); VENUE_INFER_MODEL (Bedrock
model or inference-profile id, default us.anthropic.claude-haiku-5-5);
AWS_REGION / AWS_DEFAULT_REGION (default us-east-1).

Usage:
  python3 infer_venue_subfield.py --selftest               # offline, Bedrock stubbed
  python3 infer_venue_subfield.py                          # dry run: what it would classify
  python3 infer_venue_subfield.py --apply --limit 500      # classify and write (nightly)
  python3 infer_venue_subfield.py --import-json f.json [--apply]
  python3 infer_venue_subfield.py --import-science-metrix ScienceMetrix.json [--apply]
"""
import argparse, json, os, sys, time
from collections import Counter
from datetime import datetime

from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import aar_db                    # noqa: E402  (engine only)
import venue_fit                 # noqa: E402

BATCH = 40
DEFAULT_LIMIT = 500
DEFAULT_MODEL = "us.anthropic.claude-haiku-5-5"
INFERRED_SMSID_FLOOR = 800001    # Science-Metrix's own rows sit below this
MAX_TOKENS = 8000                # 40 short answers, plus the model's own reasoning
# Seconds after which no further batch is started. run_all.py kills the step at 1800 and
# a killed run writes nothing, so a slow or throttled Bedrock would otherwise pay for the
# same venues every night and never store one. One batch can still take about nine
# minutes at worst (three attempts at the 180 s read timeout), which fits in the gap.
TIME_BUDGET = 1200


# ---- taxonomy -------------------------------------------------------------------
def taxonomy_from_rows(rows):
    """(subfield, domain, field, subfield_id) rows in smsid order -> {subfield: (domain,
    field, subfield_id, stored spelling)}, first row wins. In smsid order the first row
    of a subfield is one of Science-Metrix's own, which matters because the inferred rows
    above 800001 disagree with each other about domain and field (128 of 176 subfields
    carry more than one pairing in ReCiter's ScienceMetrix.json).

    The key is the subfield stripped (venue_fit.subfield_name), which is what the model
    is shown and what its answer is matched on. The source list spells 'Econometrics '
    with a trailing space; matched as stored, a model answering "Econometrics" would be
    rejected every night and no venue could ever be classified there. The stored
    spelling is kept beside it and is what gets written, so an inferred row carries the
    same bytes as the Science-Metrix rows of its subfield and joins to them under any
    collation."""
    out = {}
    for subfield, domain, field, subfield_id in rows:
        name = venue_fit.subfield_name(subfield)
        if name:
            out.setdefault(name, (domain, field, subfield_id, subfield))
    return out


def load_taxonomy(conn):
    return taxonomy_from_rows(conn.execute(text(
        "SELECT subfield, domain, field, subfield_id FROM journal_science_metrix "
        "WHERE subfield IS NOT NULL AND subfield <> '' ORDER BY smsid")))


def system_prompt(taxonomy):
    lines = "\n".join(f"{d} > {f} > {s}" for s, (d, f, _id, _stored) in sorted(
        taxonomy.items(), key=lambda kv: (kv[1][0] or "", kv[1][1] or "", kv[0])))
    return ("You classify publication venues (journals, conference proceedings, books) into "
            "the Science-Metrix taxonomy. For each venue choose exactly one subfield from "
            "this list, copied verbatim (format is Domain > Field > Subfield; return only "
            "the Subfield part):\n" + lines +
            '\nReturn JSON: {"results": [{"i": <index>, "subfield": "<subfield>"}]} with one '
            "entry per venue. Return only the JSON object, with no other text.")


# ---- which venues need a category -----------------------------------------------
_VENUES_SQL = ("SELECT issn, journal, pub_type, COUNT(*) AS n FROM authorship_review "
               "WHERE source IN ('scopus', 'openalex') AND journal IS NOT NULL "
               "AND journal <> '' GROUP BY issn, journal, pub_type")


def unclassified(rows, lookup):
    """(issn, title, pub_type, n) rows -> {lookup key: venue}, most rows first, for the
    venues `lookup` has no category for. A venue is keyed exactly as
    `venue_fit.subfield_for` will look it up: by ISSN when it has one, else by its
    normalised title. Its display title and type are those of its most-used spelling.

    A spelling with no venue key (empty, "undefined", "(2023)") is passed over even when
    the row has an ISSN: the title is all the model is shown, and one that names nothing
    can only be guessed at. An ISSN venue is then classified from another spelling, or
    waits for one."""
    out = {}
    for issn, title, ptype, n in sorted(rows, key=lambda r: -(r[3] or 0)):
        title = (title or "").strip()
        named = venue_fit.venue_key(title)
        k = venue_fit.issn_key(issn)
        key = "issn:" + k if k else "title:" + named
        if not named or key in lookup:
            continue
        v = out.setdefault(key, {"issn": f"{k[:4]}-{k[4:]}" if k else None,
                                 "title": title, "type": ptype, "n": 0})
        v["n"] += n or 0
    return dict(sorted(out.items(), key=lambda kv: (-kv[1]["n"], kv[0])))


# ---- Bedrock --------------------------------------------------------------------
def bedrock_client():
    import boto3
    from botocore.config import Config
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
    return boto3.client("bedrock-runtime", region_name=region, config=Config(
        connect_timeout=10, read_timeout=180, retries={"max_attempts": 3, "mode": "standard"}))


def batch_items(venues):
    """The user message for one batch: the model sees an index, the title and the
    publication type, never our keys."""
    return [{"i": i, "title": v["title"], "type": v["type"]} for i, v in enumerate(venues)]


def parse_answer(answer, n, taxonomy):
    """Model text -> {index: subfield}, keeping only what can be trusted: an integer
    index inside the batch and a subfield that is in the taxonomy, verbatim apart from
    surrounding whitespace. A near miss ("Optics & Photonics" for "Optoelectronics &
    Photonics") is rejected, not repaired: the venue stays unclassified and is asked
    again next run. The first answer for an index wins. Text around the JSON object (a
    code fence, a sentence) is ignored; anything that does not parse is no answers at
    all."""
    start, end = (answer or "").find("{"), (answer or "").rfind("}")
    if start < 0 or end <= start:
        return {}
    try:
        results = json.loads(answer[start:end + 1]).get("results", [])
    except (ValueError, AttributeError):
        return {}
    out = {}
    for o in results if isinstance(results, list) else []:
        if not isinstance(o, dict):
            continue
        i, subfield = o.get("i"), venue_fit.subfield_name(o.get("subfield"))
        if (isinstance(i, int) and not isinstance(i, bool) and 0 <= i < n
                and subfield in taxonomy):
            out.setdefault(i, subfield)
    return out


def classify_batch(client, model, system, venues, taxonomy):
    """One converse() call. No temperature: the default model rejects any sampling
    parameter, and the answer is validated against the taxonomy either way. The reply
    can carry reasoning blocks ahead of the text; only text blocks are read."""
    resp = client.converse(
        modelId=model,
        system=[{"text": system}],
        messages=[{"role": "user", "content": [
            {"text": json.dumps(batch_items(venues), ensure_ascii=False)}]}],
        inferenceConfig={"maxTokens": MAX_TOKENS})
    blocks = ((resp.get("output") or {}).get("message") or {}).get("content") or []
    answer = "".join(b.get("text", "") for b in blocks if isinstance(b, dict))
    return parse_answer(answer, len(venues), taxonomy)


def classify(todo, taxonomy, client, model, budget=TIME_BUDGET, clock=time.monotonic):
    """{key: venue} -> ({key: subfield}, error or None). Stops at the first Bedrock
    failure: boto has already retried it, and a missing permission fails every batch the
    same way. Stops as well once `budget` seconds have gone (see TIME_BUDGET). Either
    way what was answered before the stop is kept, and the caller writes it."""
    system, keys, answers, start = system_prompt(taxonomy), list(todo), {}, clock()
    for b in range(0, len(keys), BATCH):
        if clock() - start > budget:
            return answers, f"time budget of {budget}s reached"
        batch = keys[b:b + BATCH]
        try:
            got = classify_batch(client, model, system, [todo[k] for k in batch], taxonomy)
        except Exception as e:                                   # noqa: BLE001
            return answers, f"{type(e).__name__}: {e}"
        for i, subfield in got.items():
            answers[batch[i]] = subfield
        print(f"  batch {b // BATCH + 1}: {len(got)} of {len(batch)} venues classified",
              flush=True)
    return answers, None


# ---- imports --------------------------------------------------------------------
def import_json_venues(data, lookup, taxonomy):
    """The research prototype's output -> ({key: venue}, {key: subfield}, stats), in the
    same shape `classify` produces, so both go through `plan_writes`.

    "issn:XXXX-XXXX" keys map one to one. "title:<exact title>" keys are re-keyed with
    `venue_fit.venue_key`, which folds editions of one series together, so several
    entries can land on one key; the subfield most of them carry wins, the first seen on
    a tie. Entries already in `lookup`, and entries whose subfield is not in the
    taxonomy, are skipped and counted. So is an entry whose title has no venue key: the
    file classifies the literal title "undefined", and that answer is about no venue."""
    venues, votes, stats = {}, {}, Counter()
    for raw, v in data.items():
        kind, _, rest = raw.partition(":")
        subfield = venue_fit.subfield_name((v or {}).get("subfield"))
        title = ((v or {}).get("title") or (rest if kind == "title" else "")).strip()
        if kind == "issn":
            k = venue_fit.issn_key(rest)
            key = "issn:" + k if k else None
        elif kind == "title":
            vk = venue_fit.venue_key(rest)
            key = "title:" + vk if vk else None
        else:
            key = None
        if not key or not venue_fit.venue_key(title):
            stats["skipped: unusable key or title"] += 1
        elif subfield not in taxonomy:
            stats["rejected: subfield not in the taxonomy"] += 1
        elif key in lookup:
            stats["skipped: already has a category"] += 1
        else:
            if key in venues:
                stats["folded into another title's key"] += 1
            venues.setdefault(key, {"issn": f"{key[5:9]}-{key[9:]}" if kind == "issn" else None,
                                    "title": title, "type": None, "n": 0})
            votes.setdefault(key, Counter())[subfield] += 1
    answers = {key: c.most_common(1)[0][0] for key, c in votes.items()}
    stats["keys with conflicting subfields (majority kept)"] = sum(
        1 for c in votes.values() if len(c) > 1)
    return venues, answers, +stats


def science_metrix_rows(data, existing_issn_keys, existing_smsids):
    """ReCiter's ScienceMetrix.json -> (journal rows to insert, stats).

    A journal is missing when NEITHER its issn nor its eissn is in the table, as issn or
    as eissn, on any row. Missing is decided by ISSN and not by smsid because ISSN is
    how the table is read: a second row for an ISSN that is already there would change
    nothing in the lookup and would double every join on it. Rows are taken in smsid
    order, so where the file itself lists an ISSN twice (173 do) Science-Metrix's own
    row is the one kept.

    Rows with no ISSN at all (5,459 in the file) are left out: nothing can look them up
    by ISSN, and the file spells their missing issn as '', which would join to every
    article that has no ISSN either.

    A missing journal keeps its own smsid. Should that id already be taken by a
    different journal here, it gets the next free id above every id in the table and the
    file, and the collision is counted."""
    seen, taken, rows, stats = set(existing_issn_keys), set(existing_smsids), [], Counter()
    next_free = max([INFERRED_SMSID_FLOOR - 1] + list(taken)
                    + [int(j["smsid"]) for j in data if j.get("smsid") is not None]) + 1
    for j in sorted(data, key=lambda j: int(j.get("smsid") or 0)):
        keys = {k for k in (venue_fit.issn_key(j.get("issn")),
                            venue_fit.issn_key(j.get("eissn"))) if k}
        subfield = j.get("scienceMetrixSubfield")
        if not keys:
            stats["skipped: no ISSN"] += 1
            continue
        if not subfield:
            stats["skipped: no subfield"] += 1
            continue
        if keys & seen:
            stats["skipped: ISSN already in the table"] += 1
            continue
        smsid = int(j["smsid"])
        if smsid in taken:
            stats["smsid taken by another journal (new id assigned)"] += 1
            smsid, next_free = next_free, next_free + 1
        seen |= keys
        taken.add(smsid)

        def hyphenated(raw):
            k = venue_fit.issn_key(raw)
            return f"{k[:4]}-{k[4:]}" if k else None
        sid = j.get("scienceMatrixSubfieldId")
        rows.append({"smsid": smsid, "publication_name": (j.get("publicationName") or "")[:500],
                     "issn": hyphenated(j.get("issn")), "eissn": hyphenated(j.get("eissn")),
                     "domain": j.get("scienceMetrixDomain"), "field": j.get("scienceMetrixField"),
                     "subfield": subfield,
                     "subfield_id": int(sid) if str(sid or "").strip().isdigit() else None})
    stats["to insert"] = len(rows)
    return rows, +stats


# ---- writes ---------------------------------------------------------------------
def plan_writes(venues, answers, taxonomy, model, now):
    """Classified venues -> (journal rows still needing an smsid, title rows). The
    subfield written is the table's own spelling of it (see `taxonomy_from_rows`)."""
    journals, titles = [], []
    for key, subfield in answers.items():
        v = venues[key]
        domain, field, subfield_id, stored = taxonomy[subfield]
        if key.startswith("issn:"):
            journals.append({"publication_name": v["title"][:500], "issn": v["issn"],
                             "eissn": None, "domain": domain, "field": field,
                             "subfield": stored, "subfield_id": subfield_id})
        else:
            titles.append({"venue_key": key[len("title:"):], "title": v["title"][:512],
                           "subfield": stored, "model": model[:64], "inferred_at": now})
    return journals, titles


_INSERT_JOURNAL = text(
    "INSERT INTO journal_science_metrix "
    "(smsid, publication_name, issn, eissn, domain, field, subfield, subfield_id) VALUES "
    "(:smsid, :publication_name, :issn, :eissn, :domain, :field, :subfield, :subfield_id)")
# An existing key keeps its row. The collation folds a few spellings Python's key does
# not, and a venue that already has a category is never re-classified by a later answer.
_INSERT_TITLE = text(
    "INSERT INTO venue_subfield_inferred (venue_key, title, subfield, model, inferred_at) "
    "VALUES (:venue_key, :title, :subfield, :model, :inferred_at) "
    "ON DUPLICATE KEY UPDATE venue_key = venue_key")


def next_smsid(conn):
    """The next free inferred id: one past the table's highest, never below 800001
    (the earlier augmentation script's rule)."""
    top = conn.execute(text("SELECT MAX(smsid) FROM journal_science_metrix")).scalar()
    return max(INFERRED_SMSID_FLOOR, (top or 0) + 1)


def write(conn, journals, titles):
    """Insert both sets on one connection (the caller owns the transaction). Journal
    rows that carry no smsid are numbered here, after any that do."""
    numbered = [j for j in journals if j.get("smsid") is not None]
    unnumbered = [j for j in journals if j.get("smsid") is None]
    for chunk in (numbered[i:i + 500] for i in range(0, len(numbered), 500)):
        conn.execute(_INSERT_JOURNAL, chunk)
    smsid = next_smsid(conn) if unnumbered else None
    for j in unnumbered:
        j["smsid"], smsid = smsid, smsid + 1
    for chunk in (unnumbered[i:i + 500] for i in range(0, len(unnumbered), 500)):
        conn.execute(_INSERT_JOURNAL, chunk)
    for chunk in (titles[i:i + 500] for i in range(0, len(titles), 500)):
        conn.execute(_INSERT_TITLE, chunk)
    return len(journals), len(titles)


def _show(label, rows, fmt, n=15):
    print(f"{label}: {len(rows)}")
    for r in rows[:n]:
        print("    " + fmt(r))
    if len(rows) > n:
        print(f"    ... and {len(rows) - n} more")


def _report_plan(journals, titles, applied):
    verb = "inserted" if applied else "would insert"
    _show(f"journal_science_metrix rows {verb} (smsid >= {INFERRED_SMSID_FLOOR} unless the "
          f"row carries its own)", journals,
          lambda j: f"{j['issn'] or j['eissn']}  {j['subfield']}  <- {j['publication_name'][:70]}")
    _show(f"venue_subfield_inferred rows {verb}", titles,
          lambda t: f"{t['subfield']}  <- {t['title'][:70]}  [{t['venue_key'][:50]}]")


# ---- driver ---------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write (default: dry run)")
    ap.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                    help=f"venues to classify per run (default {DEFAULT_LIMIT})")
    ap.add_argument("--import-json", metavar="PATH",
                    help="load venues the research prototype already classified; no model call")
    ap.add_argument("--import-science-metrix", metavar="PATH",
                    help="insert journals from ReCiter's ScienceMetrix.json that are not in "
                         "journal_science_metrix yet")
    ap.add_argument("--selftest", action="store_true", help="offline checks, Bedrock stubbed")
    args = ap.parse_args()
    if args.selftest:
        sys.exit(0 if _selftest() else 1)
    if args.import_json and args.import_science_metrix:
        ap.error("run --import-science-metrix and --import-json one at a time (journals first)")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    mode = "APPLY" if args.apply else "DRY-RUN"
    eng = aar_db.engine()
    with eng.connect() as c:
        table_ready = venue_fit.inferred_table_ready(c)
        lookup = venue_fit.load_venue_lookup(c)
        taxonomy = load_taxonomy(c)
        print(f"[{mode}] {len(taxonomy)} subfields in journal_science_metrix; "
              f"{len(lookup)} venues already have a category", flush=True)

        if args.import_science_metrix:
            existing = c.execute(text("SELECT smsid, issn, eissn FROM journal_science_metrix")).all()
            with open(args.import_science_metrix) as fh:
                data = json.load(fh)
            journals, stats = science_metrix_rows(
                data, {k for _s, i, e in existing for k in (venue_fit.issn_key(i),
                                                            venue_fit.issn_key(e)) if k},
                {s for s, _i, _e in existing})
            titles = []
            print(f"{len(data)} rows in {args.import_science_metrix}; {len(existing)} in the table")
        elif args.import_json:
            with open(args.import_json) as fh:
                data = json.load(fh)
            venues, answers, stats = import_json_venues(data, lookup, taxonomy)
            journals, titles = plan_writes(
                venues, answers, taxonomy, f"import:{os.path.basename(args.import_json)}", now)
            print(f"{len(data)} entries in {args.import_json}")
        else:
            stats = None
            todo = unclassified(c.execute(text(_VENUES_SQL)).all(), lookup)
            total = len(todo)
            todo = dict(list(todo.items())[:max(args.limit, 0)])
            print(f"{total} venues on authorship_review have no category; "
                  f"{len(todo)} in this run (--limit {args.limit})", flush=True)

    if not taxonomy and not args.import_science_metrix:
        print("journal_science_metrix holds no subfields, so there is nothing to classify "
              "against. Load it first (--import-science-metrix). Nothing written.")
        return
    # Only the journal import can run before v3.13: every other mode writes the new table.
    if args.apply and not args.import_science_metrix and not table_ready:
        print("venue_subfield_inferred does not exist: apply setup/alter_venue_fit_v3.13.sql "
              "first. Nothing written.")
        sys.exit(2)

    if stats is not None:
        for label, n in stats.items():
            print(f"  {label}: {n}")
    elif not args.apply:
        _show("venues this run would send to the model", list(todo.items()),
              lambda kv: f"{kv[1]['n']:5d} rows  {kv[0][:60]}  <- {kv[1]['title'][:60]}", n=25)
        print("DRY-RUN: no model call, nothing written (pass --apply)")
        return
    elif not todo:
        print("nothing to classify")
        return
    else:
        model = os.environ.get("VENUE_INFER_MODEL") or DEFAULT_MODEL
        print(f"classifying {len(todo)} venues with {model} ...", flush=True)
        try:
            answers, error = classify(todo, taxonomy, bedrock_client(), model)
        except Exception as e:                                   # noqa: BLE001
            answers, error = {}, f"{type(e).__name__}: {e}"
        if error:
            print(f"WARNING: stopped early ({error}); the remaining venues stay "
                  f"unclassified and are tried again next run", flush=True)
        print(f"{len(answers)} of {len(todo)} venues classified; "
              f"{len(todo) - len(answers)} unanswered or rejected")
        journals, titles = plan_writes(todo, answers, taxonomy, model, now)

    if args.apply and (journals or titles):
        with eng.begin() as c:
            write(c, journals, titles)
    _report_plan(journals, titles, args.apply)
    if not args.apply:
        print("DRY-RUN: nothing written (pass --apply)")


# ---- offline self-test ----------------------------------------------------------
def _selftest():
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  [{'OK' if cond else '** FAIL'}] {label}")

    taxonomy = taxonomy_from_rows([
        ("Optics", "Natural Sciences", "Physics & Astronomy", 120),
        ("Law", "Economic & Social Sciences", "Social Sciences", 77),
        ("Optics", "Applied Sciences", "Engineering", 999),        # an inferred row's pairing
        ("Econometrics ", "Economic & Social Sciences", "Economics & Business", 30),
        (None, "x", "y", 1), ("", "x", "y", 2), ("  ", "x", "y", 3)])
    check("taxonomy: one entry per subfield, the first (lowest smsid) row's domain/field/id, "
          "keyed stripped with the table's own spelling kept beside it",
          taxonomy == {"Optics": ("Natural Sciences", "Physics & Astronomy", 120, "Optics"),
                       "Law": ("Economic & Social Sciences", "Social Sciences", 77, "Law"),
                       "Econometrics": ("Economic & Social Sciences", "Economics & Business",
                                        30, "Econometrics ")})

    # --- prompt building
    system = system_prompt(taxonomy)
    check("system prompt lists every subfield as Domain > Field > Subfield, sorted, with "
          "no trailing space for the model to copy or drop",
          "Economic & Social Sciences > Economics & Business > Econometrics\n"
          "Economic & Social Sciences > Social Sciences > Law\n"
          "Natural Sciences > Physics & Astronomy > Optics" in system
          and "Econometrics \n" not in system)
    check("...asks for the verbatim subfield and a JSON object only",
          "copied verbatim" in system and '{"results": [{"i": <index>, "subfield"' in system
          and "only the JSON object" in system)
    lookup = {"issn:11111111": "Optics", "title:cleo": "Optics"}
    todo = unclassified([
        ("1111-1111", "Mapped Journal", "Article", 50),             # ISSN has a category
        (None, "CLEO 2024", "Conference Paper", 40),                # title key has one
        ("2222-2222", "Cornell Law Review", "Article", 7),
        ("2222-2222", "Cornell L Rev", "Review", 2),                # same ISSN, rarer spelling
        (None, "SIGGRAPH 2023 Conference Papers", "Conference Paper", 3),
        (None, "SIGGRAPH 2024 Conference Papers", "Conference Paper", 9),
        ("", "Handbook of Optics", "Book Chapter", 1),
        (None, "undefined", "Article", 900), (None, "(2023)", "Book", 800),
        ("3333-3333", "undefined", "Article", 700),                # ISSN, but nothing to show
        (None, "2024", "Book", 1), (None, "  ", "Book", 1), (None, None, "Book", 1)], lookup)
    check("venues with a category are left out; the rest are keyed as subfield_for reads them",
          list(todo) == ["title:siggraph conference papers", "issn:22222222",
                         "title:handbook of optics"])
    check("a title that names nothing is never sent to the model, however many rows carry "
          "it and whether or not it has an ISSN ('undefined', '(2023)')",
          not any(v["title"] in ("undefined", "(2023)") for v in todo.values())
          and "issn:33333333" not in todo)
    check("...but an ISSN venue with one real spelling is classified from that one",
          unclassified([("3333-3333", "undefined", "Article", 700),
                        ("3333-3333", "Journal of Things", "Article", 2)], lookup)
          == {"issn:33333333": {"issn": "3333-3333", "title": "Journal of Things",
                                "type": "Article", "n": 2}})
    check("two editions are one venue: rows summed, the most-used spelling shown",
          todo["title:siggraph conference papers"]
          == {"issn": None, "title": "SIGGRAPH 2024 Conference Papers",
              "type": "Conference Paper", "n": 12})
    check("an ISSN venue carries its hyphenated ISSN and most-used title",
          todo["issn:22222222"] == {"issn": "2222-2222", "title": "Cornell Law Review",
                                    "type": "Article", "n": 9})
    items = batch_items(list(todo.values()))
    check("the model sees index, title and type only",
          items[0] == {"i": 0, "title": "SIGGRAPH 2024 Conference Papers",
                       "type": "Conference Paper"} and [it["i"] for it in items] == [0, 1, 2])

    # --- answer validation
    good = '{"results": [{"i": 0, "subfield": "Optics"}, {"i": 1, "subfield": "Law"}]}'
    check("a clean answer parses", parse_answer(good, 3, taxonomy) == {0: "Optics", 1: "Law"})
    check("a code fence and prose around the object are ignored",
          parse_answer("Here you go:\n```json\n" + good + "\n```", 3, taxonomy)
          == {0: "Optics", 1: "Law"})
    check("an answer is matched stripped, so the subfield the table spells with a "
          "trailing space can be chosen",
          parse_answer('{"results": [{"i": 0, "subfield": "Econometrics"}, '
                       '{"i": 1, "subfield": "Econometrics "}, {"i": 2, "subfield": " Law"}]}',
                       3, taxonomy) == {0: "Econometrics", 1: "Econometrics", 2: "Law"})
    check("a subfield not in the taxonomy is rejected, not repaired (case, near miss, "
          "the Domain > Field > Subfield line)",
          parse_answer('{"results": [{"i": 0, "subfield": "optics"}, '
                       '{"i": 1, "subfield": "Optics & Photonics"}, '
                       '{"i": 2, "subfield": "Natural Sciences > Physics & Astronomy > Optics"}]}',
                       3, taxonomy) == {})
    check("an index outside the batch, a non-integer index and a non-string subfield are rejected",
          parse_answer('{"results": [{"i": 3, "subfield": "Law"}, {"i": -1, "subfield": "Law"}, '
                       '{"i": "0", "subfield": "Law"}, {"i": true, "subfield": "Law"}, '
                       '{"i": 1, "subfield": ["Law"]}, "junk", {"subfield": "Law"}]}',
                       3, taxonomy) == {})
    check("the first answer for an index wins",
          parse_answer('{"results": [{"i": 0, "subfield": "Law"}, {"i": 0, "subfield": "Optics"}]}',
                       3, taxonomy) == {0: "Law"})
    check("no JSON, broken JSON, a bare list and an empty answer are no answers",
          parse_answer("I cannot classify these.", 3, taxonomy) == {}
          and parse_answer('{"results": [{"i": 0, "subf', 3, taxonomy) == {}
          and parse_answer('[{"i": 0, "subfield": "Law"}]', 3, taxonomy) == {}
          and parse_answer('{"results": "Law"}', 3, taxonomy) == {}
          and parse_answer("", 3, taxonomy) == {} and parse_answer(None, 3, taxonomy) == {})

    # --- the Bedrock call, stubbed
    class _Bedrock:
        def __init__(self, fail_on=None):
            self.calls, self.fail_on = [], fail_on

        def converse(self, **kw):
            self.calls.append(kw)
            if self.fail_on == len(self.calls):
                raise RuntimeError("AccessDeniedException: not authorized to InvokeModel")
            sent = json.loads(kw["messages"][0]["content"][0]["text"])
            results = [{"i": it["i"], "subfield": "Law" if "Law" in it["title"] else
                        "Optics" if "Optics" in it["title"] else "Astrology"} for it in sent]
            return {"stopReason": "end_turn", "output": {"message": {"content": [
                {"reasoningContent": {"reasoningText": {"text": '{"results": []}'}}},
                {"text": json.dumps({"results": results})}]}}}

    client = _Bedrock()
    answers, error = classify(todo, taxonomy, client, "model-x")
    call = client.calls[0]
    check("one converse() call for a batch under 40, with the model id, the system prompt "
          "and the batch as JSON",
          len(client.calls) == 1 and call["modelId"] == "model-x"
          and call["system"] == [{"text": system}]
          and json.loads(call["messages"][0]["content"][0]["text"]) == items
          and call["messages"][0]["role"] == "user")
    check("no temperature or other sampling parameter is sent",
          set(call) == {"modelId", "system", "messages", "inferenceConfig"}
          and call["inferenceConfig"] == {"maxTokens": MAX_TOKENS})
    check("valid answers are kept by venue key, the out-of-taxonomy one is dropped, and a "
          "reasoning block is not read as the answer",
          answers == {"issn:22222222": "Law", "title:handbook of optics": "Optics"}
          and error is None)
    many = {f"title:venue {i}": {"issn": None, "title": f"Law venue {i}", "type": "Book", "n": 1}
            for i in range(BATCH * 2 + 5)}
    client = _Bedrock()
    answers, error = classify(many, taxonomy, client, "m")
    check("85 venues go out in batches of 40, 40 and 5, and indexes map back per batch",
          [len(json.loads(c["messages"][0]["content"][0]["text"])) for c in client.calls]
          == [40, 40, 5] and len(answers) == 85 and error is None)
    client = _Bedrock(fail_on=2)
    answers, error = classify(many, taxonomy, client, "m")
    check("a Bedrock failure stops the run, is reported, and keeps the batch already answered",
          len(client.calls) == 2 and len(answers) == 40 and "AccessDenied" in error)
    ticks = iter([0, 10, TIME_BUDGET + 1, TIME_BUDGET + 2])
    client = _Bedrock()
    answers, error = classify(many, taxonomy, client, "m", clock=lambda: next(ticks))
    check("past the time budget no further batch is started, and the batch already "
          "answered is kept for writing (a step killed at its timeout writes nothing)",
          len(client.calls) == 1 and len(answers) == 40 and "time budget" in error
          and TIME_BUDGET < 1800)
    check("the default model is the Haiku inference profile, overridable by env",
          DEFAULT_MODEL == "us.anthropic.claude-haiku-5-5")

    # --- planned writes
    journals, titles = plan_writes(todo, {"issn:22222222": "Law",
                                          "title:handbook of optics": "Optics"},
                                   taxonomy, "model-x", "2026-11-16 00:00:00")
    check("an ISSN venue becomes a journal_science_metrix row with the subfield's own "
          "domain, field and id, and no smsid yet",
          journals == [{"publication_name": "Cornell Law Review", "issn": "2222-2222",
                        "eissn": None, "domain": "Economic & Social Sciences",
                        "field": "Social Sciences", "subfield": "Law", "subfield_id": 77}])
    check("a no-ISSN venue becomes a venue_subfield_inferred row under its venue key",
          titles == [{"venue_key": "handbook of optics", "title": "Handbook of Optics",
                      "subfield": "Optics", "model": "model-x",
                      "inferred_at": "2026-11-16 00:00:00"}])
    econ_j, econ_t = plan_writes(todo, {"issn:22222222": "Econometrics",
                                        "title:handbook of optics": "Econometrics"},
                                 taxonomy, "model-x", "t")
    check("the subfield written is the table's own spelling, trailing space included, so "
          "an inferred row joins to the Science-Metrix rows of its subfield",
          econ_j[0]["subfield"] == econ_t[0]["subfield"] == "Econometrics "
          and econ_j[0]["subfield_id"] == 30)

    class _Conn:
        def __init__(self, top):
            self.top, self.executed = top, []

        def execute(self, stmt, params=None):
            self.executed.append((str(stmt), params))
            return self

        def scalar(self):
            return self.top

    conn = _Conn(832274)
    write(conn, [dict(journals[0]), dict(journals[0], issn="3333-3333")], titles)
    inserted = [p for s, p in conn.executed if "INSERT INTO journal_science_metrix" in s][0]
    check("inferred journals are numbered from one past the table's highest smsid",
          [j["smsid"] for j in inserted] == [832275, 832276])
    check("...and never below 800001 on a table that holds only Science-Metrix's own rows",
          next_smsid(_Conn(115008)) == 800001 and next_smsid(_Conn(None)) == 800001)
    check("a title insert leaves an existing key's row alone",
          "ON DUPLICATE KEY UPDATE venue_key = venue_key" in str(_INSERT_TITLE))

    # --- --import-json mapping
    data = {
        "issn:0002-1466": {"title": "Agricultural Finance Review", "subfield": "Law",
                           "field": "Social Sciences", "domain": "Economic & Social Sciences"},
        "issn:1111-1111": {"title": "Mapped Journal", "subfield": "Law"},        # already mapped
        "title:CLEO 2023": {"title": "CLEO 2023", "subfield": "Optics"},         # key already mapped
        "title:Websci 2019 Proceedings": {"title": "Websci 2019 Proceedings", "subfield": "Law"},
        "title:Websci 2020 Proceedings": {"title": "Websci 2020 Proceedings", "subfield": "Optics"},
        "title:Websci 2021 Proceedings": {"title": "Websci 2021 Proceedings", "subfield": "Optics"},
        "title:Tie 2020": {"title": "Tie 2020", "subfield": "Law"},
        "title:Tie 2021": {"title": "Tie 2021", "subfield": "Optics"},
        "title:Tasting Qualities": {"title": "Tasting Qualities", "subfield": "Astrology"},
        "title:2024": {"title": "2024", "subfield": "Law"},
        "title:undefined": {"title": "undefined", "subfield": "Law"},
        "title:(2023)": {"title": "(2023)", "subfield": "Law"},
        "issn:7777-7777": {"title": "undefined", "subfield": "Law"},
        "issn:8888-8888": {"title": "Econometric Notes", "subfield": "Econometrics"},
        "doi:10.1/x": {"title": "Not a venue key", "subfield": "Law"},
    }
    venues, answers, stats = import_json_venues(data, lookup, taxonomy)
    check("import: an ISSN entry maps one to one, keeping its hyphenated ISSN",
          answers["issn:00021466"] == "Law" and venues["issn:00021466"]["issn"] == "0002-1466"
          and venues["issn:00021466"]["title"] == "Agricultural Finance Review")
    check("import: editions fold onto one venue key and the majority subfield wins",
          answers["title:websci proceedings"] == "Optics"
          and venues["title:websci proceedings"]["title"] == "Websci 2019 Proceedings")
    check("import: a tie keeps the first subfield seen", answers["title:tie"] == "Law")
    check("import: entries that already have a category, a subfield outside the taxonomy, "
          "an empty key, a title that names nothing ('undefined', '(2023)', under an "
          "ISSN too) and an unknown key kind are skipped and counted",
          set(answers) == {"issn:00021466", "issn:88888888", "title:websci proceedings",
                           "title:tie"}
          and stats == {"skipped: already has a category": 2,
                        "rejected: subfield not in the taxonomy": 1,
                        "skipped: unusable key or title": 5,
                        "folded into another title's key": 3,
                        "keys with conflicting subfields (majority kept)": 2})
    journals, titles = plan_writes(venues, answers, taxonomy, "import:f.json", "t")
    check("import: a subfield the file spells without the table's trailing space is "
          "accepted and written as the table spells it",
          answers["issn:88888888"] == "Econometrics"
          and [j["subfield"] for j in journals if j["issn"] == "8888-8888"] == ["Econometrics "])
    journals = [j for j in journals if j["issn"] != "8888-8888"]
    check("import: goes through the same planned writes as a model answer",
          [j["issn"] for j in journals] == ["0002-1466"]
          and sorted(t["venue_key"] for t in titles) == ["tie", "websci proceedings"]
          and {t["model"] for t in titles} == {"import:f.json"})

    # --- --import-science-metrix mapping
    sm = [
        {"smsid": 100001, "issn": "0031-5850", "eissn": "1878-9080", "publicationName": "Persoonia",
         "scienceMatrixSubfieldId": "92", "scienceMetrixDomain": "Health Sciences",
         "scienceMetrixField": "Biomedical Research", "scienceMetrixSubfield": "Mycology"},
        {"smsid": 100002, "issn": "1111-1111", "eissn": None, "publicationName": "In the table",
         "scienceMatrixSubfieldId": 1, "scienceMetrixDomain": "d", "scienceMetrixField": "f",
         "scienceMetrixSubfield": "Optics"},
        {"smsid": 100003, "issn": "4444-4444", "eissn": "2222-2222", "publicationName":
         "eISSN in the table as an ISSN", "scienceMatrixSubfieldId": 1, "scienceMetrixDomain": "d",
         "scienceMetrixField": "f", "scienceMetrixSubfield": "Optics"},
        {"smsid": 832274, "issn": "", "eissn": None, "publicationName": "Delaware code annotated",
         "scienceMatrixSubfieldId": 77, "scienceMetrixDomain": "d", "scienceMetrixField": "f",
         "scienceMetrixSubfield": "Law"},
        {"smsid": 800009, "issn": "0031-5850", "eissn": None, "publicationName": "Persoonia again",
         "scienceMatrixSubfieldId": 92, "scienceMetrixDomain": "d", "scienceMetrixField": "f",
         "scienceMetrixSubfield": "Mycology"},
        {"smsid": 800010, "issn": "5555-555x", "eissn": None, "publicationName": "Taken id",
         "scienceMatrixSubfieldId": 77, "scienceMetrixDomain": "d", "scienceMetrixField": "f",
         "scienceMetrixSubfield": "Law"},
        {"smsid": 800011, "issn": "6666-6666", "eissn": None, "publicationName": "No subfield",
         "scienceMatrixSubfieldId": None, "scienceMetrixDomain": None, "scienceMetrixField": None,
         "scienceMetrixSubfield": None},
    ]
    rows, stats = science_metrix_rows(sm, {"11111111", "22222222"}, {100002, 800010})
    check("journal import: a journal whose ISSN and eISSN are both new is inserted under "
          "its own smsid, columns mapped, the string subfield id made an int",
          rows[0] == {"smsid": 100001, "publication_name": "Persoonia", "issn": "0031-5850",
                      "eissn": "1878-9080", "domain": "Health Sciences",
                      "field": "Biomedical Research", "subfield": "Mycology", "subfield_id": 92})
    check("journal import: skipped when its ISSN or its eISSN is already in the table, in "
          "either column, or earlier in the file",
          [r["publication_name"] for r in rows] == ["Persoonia", "Taken id"]
          and stats["skipped: ISSN already in the table"] == 3)
    check("journal import: rows with no ISSN or no subfield are left out",
          stats["skipped: no ISSN"] == 1 and stats["skipped: no subfield"] == 1)
    check("journal import: an smsid another journal holds gets the next free id above "
          "every id in the table and the file, and the ISSN is stored hyphenated uppercase",
          rows[1]["smsid"] == 832275 and rows[1]["issn"] == "5555-555X"
          and stats["smsid taken by another journal (new id assigned)"] == 1
          and stats["to insert"] == 2)
    conn = _Conn(999999)
    write(conn, rows, [])
    check("journal import: rows that carry an smsid are written with it, none renumbered",
          [j["smsid"] for s, p in conn.executed for j in (p or [])] == [100001, 832275]
          and not any("MAX(smsid)" in s for s, _p in conn.executed))

    print("\nSELFTEST", "PASS" if ok else "FAIL")
    return ok


if __name__ == "__main__":
    main()
