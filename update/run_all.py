import subprocess
import logging
import time
import boto3
import os
import sys
import psutil   # for memory logging (pip install psutil)
from botocore.config import Config        

LOG_FILE = os.environ['LOG_FILE']
S3_BUCKET = os.environ['S3_BUCKET']
S3_KEY_PREFIX = os.environ['S3_KEY_PREFIX']


# ------------- Logging Setup -------------
logger = logging.getLogger("cronjob")
logger.setLevel(logging.INFO)

fh = logging.FileHandler(LOG_FILE)
sh = logging.StreamHandler(sys.stdout)

formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
fh.setFormatter(formatter)
sh.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(sh)

# ------------- Memory Logging -------------
def log_memory_usage(label=""):
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / (1024 * 1024)
    logger.info(f"[MEMORY] {label} - RSS: {mem_mb:.2f} MB")

# ------------- Script Runner -------------
def run_script(name, cmd, timeout_seconds=None):
    start_ts = time.time()
    logger.info("")
    logger.info("======================================")
    logger.info(f"STARTING SCRIPT: {name}")
    logger.info(f"COMMAND: {cmd}")
    logger.info("======================================")

    log_memory_usage(f"Before running {name}")

    try:
        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1

        )

        # Stream logs live
       # for line in process.stdout:
       #     logger.info(f"{name}: {line.strip()}")
       # for line in process.stderr:
       #     logger.error(f"{name} [ERR]: {line.strip()}")
        start = time.time()
        assert process.stdout is not None
        for line in iter(process.stdout.readline, ""):
            logger.info(f"{name}: {line.rstrip()}")
            if timeout_seconds and (time.time() - start) > timeout_seconds:
                logger.error(f"⏱️ TIMEOUT: {name} exceeded {timeout_seconds}s; terminating.")
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    logger.error(f"Escalating to kill for {name}.")
                    process.kill()
                return False
        process.stdout.close()
        exit_code = process.wait()
        elapsed = time.time() - start_ts

        if exit_code != 0:
            #logger.error(f"❌ SCRIPT FAILED: {name} (exit code {exit_code})")
            logger.error(f"❌ SCRIPT FAILED: {name} (exit code {exit_code}) after {elapsed:.1f}s")
            return False

        logger.info(f"✅ SCRIPT COMPLETED: {name}")
        logger.info(f"✅ SCRIPT COMPLETED: {name} in {elapsed:.1f}s")
        log_memory_usage(f"After running {name}")
        return True

    except Exception as e:
        logger.exception(f"Exception while running {name}: {e}")
        return False

# ------------- S3 Upload -------------
def upload_log_to_s3():
    try:
        cfg = Config(connect_timeout=5, read_timeout=60, retries={"max_attempts": 10, "mode": "standard"})
        s3 = boto3.client("s3", config=cfg)
        s3 = boto3.client("s3")
        filename = f"{int(time.time())}-cronjob.log"
        s3_key = f"{S3_KEY_PREFIX}{filename}"

        logger.info(f"Uploading log to s3://{S3_BUCKET}/{S3_KEY_PREFIX}")
        s3.upload_file(LOG_FILE, S3_BUCKET, s3_key)
        logger.info("Log upload complete")

    except Exception as e:
        logger.error("Failed to upload logs to S3")
        logger.exception(e)

# ------------- AAR preflight: is the attribution table whole? -------------
AAR_GUARD_DDL = """
CREATE TABLE IF NOT EXISTS aar_lane_guard (
  metric      VARCHAR(64)  NOT NULL,
  value       BIGINT       NOT NULL,
  observed_at DATETIME     NOT NULL,
  PRIMARY KEY (metric)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def aar_lanes_safe():
    """Refuse to run the AAR lanes when person_article came back materially short.

    WHY THIS EXISTS. The AAR lanes read person_article to decide what is already
    attributed — aar_gate, aar_dismiss_byline_owner and aar_reconcile_open all key off it.
    If the reporting rebuild earlier in this same job read DynamoDB Analysis while
    reciter-inst-client was still rewriting it, person_article lands partial and every lane
    then reasons from a table missing attributions. The visible result is not an error: it
    is a queue full of authorships that are in fact already attributed, plus reconciliation
    deciding stored evidence is stale when only the input was. That is the shape of the May
    2026 incident and of the -30% gate-table drop (ReCiterDB #122).

    WHY IT IS A DATA CHECK AND NOT A LIVENESS CHECK. The obvious guard would ask Kubernetes
    whether an inst-client job is still Running. This pod cannot: it runs as the `default`
    ServiceAccount, which has no get/list on jobs, and the image ships no k8s client — only
    requests. Adding RBAC plus a dependency to detect a condition already visible in the
    data is the wrong trade. A short person_article is the symptom that actually matters,
    and it also catches upstream failures that have nothing to do with timing.

    HOW. Compare today's row count against the last count that PASSED. Below
    AAR_LANE_MIN_RATIO (default 0.90) the lanes are skipped and the baseline is deliberately
    NOT advanced, so a sustained partial state keeps failing instead of quietly becoming the
    new normal. AAR_LANE_GUARD=off overrides; a fresh DB just records a baseline and runs.

    Isolated like the lanes themselves: any failure here returns True (proceed). A broken
    guard must not be able to stop the nightly — it is a safety net, not a dependency."""
    if (os.getenv("AAR_LANE_GUARD") or "").strip().lower() == "off":
        logger.info("AAR guard: disabled by AAR_LANE_GUARD=off")
        return True
    try:
        from sqlalchemy import text
        import aar_db
        ratio = float(os.getenv("AAR_LANE_MIN_RATIO") or "0.90")
        with aar_db.engine().begin() as cx:
            cx.execute(text(AAR_GUARD_DDL))
            now = cx.execute(text("SELECT COUNT(*) FROM person_article")).scalar() or 0
            prev = cx.execute(text(
                "SELECT value FROM aar_lane_guard WHERE metric = 'person_article_rows'")).scalar()
            if prev and now < prev * ratio:
                logger.error(
                    f"AAR guard: person_article has {now:,} rows vs {prev:,} at the last good run "
                    f"({now / prev:.1%} < {ratio:.0%}) — SKIPPING every AAR lane. The reporting "
                    f"rebuild most likely read Analysis mid-rewrite; check whether a "
                    f"reciter-inst-client job was still running, and note a hung inst-client pod "
                    f"stays Running without progressing. Baseline left unchanged, so this keeps "
                    f"failing until the count recovers or AAR_LANE_GUARD=off.")
                return False
            cx.execute(text(
                "INSERT INTO aar_lane_guard (metric, value, observed_at) "
                "VALUES ('person_article_rows', :v, NOW()) "
                "ON DUPLICATE KEY UPDATE value = :v, observed_at = NOW()"), {"v": now})
            logger.info(f"AAR guard: person_article {now:,} rows"
                        + (f" (prev {prev:,})" if prev else " — first run, baseline recorded"))
        return True
    except Exception as e:
        logger.exception(f"AAR guard failed (proceeding — it must never block the nightly): {e}")
        return True


# ------------- lane cadence -------------
def lane_due(cadence, weekday, default):
    """Is a cadence-gated lane due today? `weekday` is Mon=0..Sun=6.

    Anything that is not exactly "daily" falls through to Sundays-only, so a typo in a
    CronJob env patch keeps the conservative schedule instead of silently going daily on an
    API budget nobody re-checked."""
    return (cadence or default).strip().lower() == "daily" or weekday == 6


# ------------- AAR Scopus lane (weekly by default, isolated) -------------
def run_scopus_lane_if_due():
    """Scopus not-in-PubMed authorship detector (AAR / PM#775).

    Cadence is env-controlled (AAR_SCOPUS_LANE_CADENCE: "weekly" default — Sundays only —
    or "daily"), mirroring AAR_PUBMED_LANE_CADENCE so it flips with a CronJob env patch
    rather than a rebuild; k8-buildspec only does `kubectl set image`, so env set on the
    live CronJob survives deploys. Default is weekly, so this change is inert until set.

    Daily is CORRECT but not FREE, and the two are worth separating:

      Correct, because overlapping ORIG-LOAD-DATE windows are idempotent via the author_key
      (see aar_universe_scopus.rolling_window) — the same property the PubMed lane already
      leans on every night.

      Not free, because the sweep is 135 AF-IDs in chunks of 40 at 25 results/page (COMPLETE
      view caps there) against Elsevier's weekly-rolling quota, so seven runs of the default
      76-day window is ~7x the calls. AAR_SCOPUS_SPAN_DAYS narrows the window when running
      daily: consecutive daily windows still overlap heavily, so coverage is unchanged while
      the call count falls back to roughly weekly cost. Setting cadence daily WITHOUT a span
      is the expensive combination — that is the knob to reach for.

    Keep the 14-day lag either way. It is not padding: it exists so Scopus<->PubMed links
    settle before the sweep reads them, and shortening it re-introduces the false
    not-in-PubMed rows the lag was added to prevent.

    Fully isolated from the reporting rebuild: keys-gated, timed out, and any failure is
    caught and logged so it can NEVER fail the nightly job."""
    try:
        import datetime as _datetime
        if not lane_due(os.getenv("AAR_SCOPUS_LANE_CADENCE"),
                        _datetime.datetime.utcnow().weekday(), "weekly"):
            logger.info("Scopus lane: not due (cadence=weekly, Sundays only) — skipped")
            return
        if not (os.getenv("SCOPUS_API_KEY") and os.getenv("SCOPUS_INST_TOKEN")):
            logger.warning("Scopus lane: SCOPUS_API_KEY/INST_TOKEN unset — skipped")
            return
        cmd = "python3 aar_universe_scopus.py --mode rolling --apply"
        span = (os.getenv("AAR_SCOPUS_SPAN_DAYS") or "").strip()
        if span.isdigit() and int(span) > 0:
            cmd += f" --span-days {span}"
            logger.info(f"Scopus lane: narrowed sweep window to {span} days (AAR_SCOPUS_SPAN_DAYS)")
        run_script("aarScopusLane", cmd,
                   timeout_seconds=int(os.getenv("SCOPUS_TIMEOUT_SECONDS", "3600")))
    except Exception as e:
        logger.exception(f"Scopus lane failed (ignored — reporting unaffected): {e}")


# ------------- AAR Scopus lane, Cornell Ithaca (env-gated, isolated) -------------
def run_scopus_ithaca_lane_if_due():
    """The same weekly Scopus sweep as run_scopus_lane_if_due(), pointed at the Cornell
    University (Ithaca) AF-ID family instead of the WCM one.

    Its own AF-ID list (update/scopus_afids_cornell_ithaca.csv, 16 AF-IDs) rather than 16
    more rows in scopus_afids.csv, deliberately. aar_universe_scopus.wcm_authorships()
    selects an author purely on "this author carries an afid in the family set", so
    appending would label every Ithaca author a WCM authorship; and one merged set would
    roughly double the volume a single Sunday sweep fetches under one
    SCOPUS_TIMEOUT_SECONDS budget (3600s).

    SHIPS OFF (AAR_SCOPUS_ITHACA_LANE unset or "off"), and that is the point of the env
    var, not an afterthought. aar_universe_scopus.py drops any authorship whose author
    matches nobody in reciterdb.identity — `top is None` -> `unmatched += 1; continue` —
    and no Cornell Ithaca people are in that table yet. Turning this on before the Ithaca
    identity load lands spends a rate-limited Elsevier key on a full sweep and writes
    zero rows: every authorship it finds is unmatched by construction.

    The precondition is TWO things, not one, and the second is the dangerous half.
    `--afid-list` changes which documents the sweep fetches; it does NOT change which
    roster they are matched against. Both `run()` and `run_backfill()` call a bare
    `IdentityIndex.load()`, so an Ithaca sweep is scored against whatever roster that
    default returns — today the flat pool, and the WCM roster once the campus-scope
    change lands. Matching a Cornell author against a roster that is not the Cornell
    roster does not merely find nothing: a Cornell surname that collides with a WCM
    person's yields a confident FALSE ATTRIBUTION to that WCM person, written to
    authorship_review as a real open row. So the lane must load the Ithaca roster
    explicitly before it is ever enabled.

    Sequence: (1) land whatever admits Cornell people to reciterdb.identity; (2) land the
    campus scoping AND thread it through this lane, so the Ithaca invocation loads the
    Ithaca roster rather than the default one; (3) confirm identity_index resolves a
    handful of known Ithaca authors; (4) run `aar_universe_scopus.py --mode rolling
    --afid-list scopus_afids_cornell_ithaca.csv` as a dry run and read the
    matched/unmatched counts; (5) only then patch the CronJob env to
    AAR_SCOPUS_ITHACA_LANE=on. An env patch on the live CronJob survives deploys
    (k8-buildspec only does `kubectl set image`), the same escape hatch as
    AAR_DRIFT_CADENCE and AAR_PUBMED_LANE_CADENCE.

    Same isolation contract as the WCM lane: gated to Sundays, skipped if its API keys
    are absent, and any failure is caught and logged so it can NEVER fail the nightly
    job."""
    try:
        import datetime as _datetime
        enabled = (os.getenv("AAR_SCOPUS_ITHACA_LANE") or "off").strip().lower()
        if enabled != "on":
            logger.info("Scopus Ithaca lane: AAR_SCOPUS_ITHACA_LANE=%s — skipped (set it "
                        "to 'on' only once Cornell Ithaca people are loaded into "
                        "reciterdb.identity; until then every authorship the sweep finds "
                        "is dropped as unmatched)", enabled)
            return
        if _datetime.datetime.utcnow().weekday() != 6:   # 6 = Sunday
            logger.info("Scopus Ithaca lane: not due (runs weekly on Sundays) — skipped")
            return
        if not (os.getenv("SCOPUS_API_KEY") and os.getenv("SCOPUS_INST_TOKEN")):
            logger.warning("Scopus Ithaca lane: SCOPUS_API_KEY/INST_TOKEN unset — skipped")
            return
        run_script("aarScopusLaneIthaca",
                   "python3 aar_universe_scopus.py --mode rolling "
                   "--afid-list scopus_afids_cornell_ithaca.csv --apply",
                   timeout_seconds=int(os.getenv("SCOPUS_ITHACA_TIMEOUT_SECONDS", "3600")))
    except Exception as e:
        logger.exception(f"Scopus Ithaca lane failed (ignored — reporting "
                         f"unaffected): {e}")


# ------------- COI refresh pass (weekly, isolated) -------------
def run_conflicts_refresh_if_due():
    """Weekly refill of reporting_conflicts rows that exist but are empty (#130).

    The nightly conflictsImport backfill selects on `a.pmid IS NULL`, so a row
    written empty (no coiStatement upstream at import time) is never revisited and
    a statement PubMed adds later stays invisible -- ~5,100 rows when measured.

    Weekly, not nightly: COI statements accrue over months, and a full pass re-reads
    ~87k pmids from DynamoDB. Isolated like the AAR lanes -- any failure is caught
    and logged so it can NEVER fail the nightly, since this only ever adds data that
    was missing and is safe to skip until next week."""
    try:
        import datetime as _datetime
        if _datetime.datetime.utcnow().weekday() != 6:   # 6 = Sunday
            logger.info("COI refresh: not due (runs weekly on Sundays) — skipped")
            return
        run_script("conflictsRefresh", "python3 conflictsImport.py --refresh-empty",
                   timeout_seconds=int(os.getenv("CONFLICTS_REFRESH_TIMEOUT_SECONDS", "3600")))
    except Exception as e:
        logger.exception(f"COI refresh failed (ignored — reporting unaffected): {e}")


# ------------- AAR PubMed lane (daily by default, isolated) -------------
def run_pubmed_lane_if_due():
    """PubMed orphan-authorship detector + IO/FB scoring (AAR).

    Runs EVERY night by default. Cadence is env-controlled (AAR_PUBMED_LANE_CADENCE:
    "daily" default, or "weekly" for the old Sunday-only behaviour) so it can be rolled
    back with a CronJob env patch instead of a rebuild -- k8-buildspec only does
    `kubectl set image`, so env set on the live CronJob survives deploys.

    Daily is safe and cheap: overlapping EDAT windows are harmless because the processed
    log prevents re-gating, per-run new-article work shrinks (~35/day vs ~240/week), and
    the re-check step makes no PubMed calls at all (DynamoDB BatchGetItem, 100 cwids/call).
    The 71-day window is kept as missed-run insurance so an outage self-heals.

    Same isolation contract as the Scopus lane: keys-gated, wrapped in
    try/except with a timeout, so any failure is logged and can NEVER fail the nightly
    reporting rebuild. Ledger/processed_log state lives in S3 (--s3-state) because the
    CronJob has no persistent filesystem; upserts land in reciterdb.authorship_review
    (source='pubmed')."""
    try:
        import datetime as _datetime
        if not lane_due(os.getenv("AAR_PUBMED_LANE_CADENCE"),
                        _datetime.datetime.utcnow().weekday(), "daily"):
            logger.info("PubMed lane: not due (cadence=weekly, Sundays only) — skipped")
            return
        if not os.getenv("PUBMED_API_KEY"):
            logger.warning("PubMed lane: PUBMED_API_KEY unset — skipped")
            return
        if not (os.getenv("AAR_S3_BUCKET") or os.getenv("S3_BUCKET")):
            logger.warning("PubMed lane: AAR_S3_BUCKET/S3_BUCKET unset (needed for --s3-state) — skipped")
            return
        # 5400 -> 14400 (#186): the first run after the 40-day recency floor is
        # removed processes ~40 days of new PMIDs (~5-6x a normal week), and a timeout
        # leaves DB rows written but the S3 ledger un-pushed, so the run repeats.
        run_script("aarPubmedLane", "python3 aar_orchestrator.py --mode recurring --s3-state",
                   timeout_seconds=int(os.getenv("PUBMED_LANE_TIMEOUT_SECONDS", "14400")))
    except Exception as e:
        logger.exception(f"PubMed lane failed (ignored — reporting unaffected): {e}")


# ------------- External-source article projection (ReCiterDB #101) -------------
def run_external_article_etl():
    """Project the ExternalArticle DynamoDB table -> reciterdb.external_article.

    Isolated (try/except + timeout) so a scan/load hiccup can never fail the nightly
    reporting rebuild. Runs every night BEFORE `nightlyIndexing` (see main()) so the
    reporting SP's STEP 6b sees freshly-loaded external rows and unions them into the
    person-publication tables (ReCiterDB #101, Option B). Robust to an empty/absent
    DynamoDB table. Kept isolated -- NOT a fatal `scripts` entry -- so an external-
    source hiccup degrades to stale/empty external pubs, never a failed nightly."""
    try:
        run_script("externalArticles", "python3 retrieveExternalArticles.py",
                   timeout_seconds=int(os.getenv("EXTERNAL_ARTICLE_TIMEOUT_SECONDS", "600")))
    except Exception as e:
        logger.exception(f"External-article ETL failed (ignored — reporting unaffected): {e}")


# ------------- AAR nightly closer: dismiss already-attributed open rows (#186) -------------
def run_aar_close_attributed():
    """Nightly DB-side closer for open AAR rows ReCiter has already attributed.

    Ships with the PubMed lane's 40-day recency floor removal (aar_universe.py):
    without a floor, some rows get emitted first and must self-retire later once
    reciterdb's own ground truth (person_article ACCEPTED / GoldStandard knownpmids)
    shows the article already attributed. Runs EVERY night, not Sunday-gated, so a
    row doesn't sit open for up to a week after it's already resolved. Same isolation
    contract as the lanes: DB-creds-gated (same env names aar_db.py reads), each call
    wrapped so a failure is logged and can NEVER fail the nightly reporting rebuild.

    Two independent tools, in order (their own docstrings document the write path,
    the status='open' race guard, the collation trap, and the note/CONCAT_WS 'auto:'
    reason convention -- ReCiterDB #186):
      1. aar_reconcile_open.py --class-a-only  -- the row's OWN stored proposal
         already holds the pmid.
      2. aar_dismiss_byline_owner.py           -- the BYLINE's real owner already
         holds the pmid, regardless of what the row proposes."""
    try:
        if not (os.getenv("DB_HOST") and os.getenv("DB_NAME")
                and os.getenv("DB_USERNAME") and os.getenv("DB_PASSWORD")):
            logger.warning("AAR closer: DB_HOST/DB_NAME/DB_USERNAME/DB_PASSWORD unset — skipped")
            return
        timeout = int(os.getenv("AAR_CLOSER_TIMEOUT_SECONDS", "1800"))
        # Explicit --ledger per invocation: aar_reconcile_open's default path is one
        # file, and _write_ledger truncates, so the drift step below would destroy this
        # run's ledger on any night both fire.
        run_script("aarReconcileOpenClassA",
                   "python3 aar_reconcile_open.py --apply --class-a-only "
                   "--ledger aar_reconcile_class_a_ledger.jsonl",
                   timeout_seconds=timeout)
        run_script("aarDismissBylineOwner",
                   "python3 aar_dismiss_byline_owner.py --apply",
                   timeout_seconds=timeout)
    except Exception as e:
        logger.exception(f"AAR closer failed (ignored — reporting unaffected): {e}")


# ------------- AAR producer-column drift reconciliation (env-gated, isolated) ---------
def run_aar_reconcile_drift_if_due():
    """Keep every OPEN row's stored proposal equal to what the producer would write for
    that authorship today (ReCiterDB #186, option 2).

    The producer writes a row's proposal once and can never revise it, so each matcher
    tightening strands its predecessors' suggestions and has needed a bespoke one-shot
    sweep -- #177, #181, #182, and now #203, whose own replay measured 3,970 rows seeing
    a candidate change against only 203 top picks moving. This step retires that pattern
    for the class where it is safe to do so unattended.

    --drift-only, deliberately: it queues ONLY rows whose top_cwid is unchanged and whose
    stored EVIDENCE columns are stale. No row's proposed PERSON is ever changed on this
    path -- stronger/sideways tier moves and the weaker hard-exclusion stay exactly where
    they were, manual and hand-reviewed. Curator-touched rows are untouched (the UPDATE
    re-checks status='open'), and one JSONL ledger is written per run.

    --lane pubmed, on cost: the pubmed replay is a batched efetch plus a threaded
    identity-only warm-up, ~15 minutes for the whole open queue. The scopus lane needs a
    live Scopus GET for every flagged row, and drift flags most of them -- 3,591 of 4,926
    open rows on 2026-09-04, measured at ~6s each, about six hours. That does not belong
    in a job that already runs 2.5 hours, and it shares a rate-limited API key with the
    Sunday Scopus lane. Reconcile the scopus lane by hand instead:
    `aar_reconcile_open.py --drift-only --lane scopus`, out of hours.

    SHIPS OFF (AAR_DRIFT_CADENCE unset or "off"), and this is the point of the env var,
    not an afterthought. The class is safe per row but the BACKLOG is not small: the
    2026-09-04 dry runs measured DRIFT_ONLY at 5,308 of 11,624 open pubmed rows and, on
    a re-run the same day, 5,222 of 11,335 -- about 46% of the lane, not the ~60% an
    earlier draft of this note claimed. Nothing has
    ever reconciled it against #159 (temporal penalty), #171/#173/#174, #185, #201 or
    #203. Turning this on for the first time is a one-off catch-up of thousands of rows
    and wants a human reading the ledger, not a Sunday cron discovering it. Sequence:
    run `aar_reconcile_open.py --drift-only` as a dry run, read the ledger, run it with
    --apply, then patch the CronJob env to AAR_DRIFT_CADENCE=weekly so the far smaller
    steady-state trickle is picked up automatically. "weekly" (Sundays) or "daily" both
    work; weekly is the intended setting -- this is the full CLASS-B replay (an efetch of
    every open pubmed row's article, an identity-only S3 warm-up, and a live Scopus
    re-verify for flagged rows), where the closer next door is a couple of SQL/DynamoDB
    batch reads, and a drifted row shows a curator a stale confidence or an obsolete dept
    chip, never the wrong person. An env patch on the live CronJob survives deploys
    (k8-buildspec only does `kubectl set image`), same escape hatch as
    AAR_PUBMED_LANE_CADENCE.

    Same isolation contract as the lanes: gated on cadence, on DB creds and on
    PUBMED_API_KEY (the replay re-fetches every open pubmed row's article), wrapped so a
    failure is logged and can NEVER fail the nightly reporting rebuild."""
    try:
        import datetime as _datetime
        cadence = (os.getenv("AAR_DRIFT_CADENCE") or "off").strip().lower()
        if cadence not in ("daily", "weekly"):
            logger.info("AAR drift reconciliation: AAR_DRIFT_CADENCE=%s — skipped (set "
                        "it to weekly once the one-off catch-up pass has been applied "
                        "and its ledger read; see #186)", cadence)
            return
        if cadence == "weekly" and _datetime.datetime.utcnow().weekday() != 6:  # 6 = Sunday
            logger.info("AAR drift reconciliation: not due (cadence=weekly, Sundays only) — skipped")
            return
        if not (os.getenv("DB_HOST") and os.getenv("DB_NAME")
                and os.getenv("DB_USERNAME") and os.getenv("DB_PASSWORD")):
            logger.warning("AAR drift reconciliation: DB_HOST/DB_NAME/DB_USERNAME/"
                           "DB_PASSWORD unset — skipped")
            return
        if not os.getenv("PUBMED_API_KEY"):
            logger.warning("AAR drift reconciliation: PUBMED_API_KEY unset (the replay "
                           "re-fetches every open pubmed row's article) — skipped")
            return
        # Its own --ledger, not the shared default: the nightly closer next door runs
        # the same script and _write_ledger truncates.
        run_script("aarReconcileDrift",
                   "python3 aar_reconcile_open.py --apply --drift-only --lane pubmed "
                   "--ledger aar_reconcile_drift_ledger.jsonl",
                   timeout_seconds=int(os.getenv("AAR_DRIFT_TIMEOUT_SECONDS", "3600")))
    except Exception as e:
        logger.exception(f"AAR drift reconciliation failed (ignored — reporting "
                         f"unaffected): {e}")


# ------------- Main Flow -------------
def main():
    scripts = [
        ("executeFeatureGenerator", "python3 executeFeatureGenerator.py"),
        ("retrieveArticles", "python3 retrieveArticles.py"),
        ("retrieveNIH", "python3 retrieveNIH.py"),
        ("retrieveReporter", "python3 retrieveReporter.py"),
        ("nightlyIndexing", "bash run_nightly_indexing.sh"),
        ("abstractImport", "python3 abstractImport.py"),
        ("conflictsImport", "python3 conflictsImport.py")
    ]

    overall_success = True

    # External-source projection MUST run before nightlyIndexing so the reporting SP
    # (STEP 6b) unions freshly-loaded external_article rows (ReCiterDB #101). Isolated,
    # so a failure here degrades to stale/empty external pubs, never a failed nightly.
    run_external_article_etl()

    for name, cmd in scripts:
        #ok = run_script(name, cmd)
        ok = run_script(name, cmd, timeout_seconds=int(os.getenv("SCRIPT_TIMEOUT_SECONDS", "15000")))
        if not ok:
            overall_success = False
            logger.error("Stopping pipeline due to script failure.")
            break

    # Post-reporting projections/lanes — run only if the reporting rebuild succeeded,
    # each isolated so it can never fail the nightly.
    # The AAR lanes all reason from person_article; skip them wholesale if the rebuild above
    # produced a short one rather than let every lane draw conclusions from it.
    if overall_success and aar_lanes_safe():
        run_scopus_lane_if_due()              # weekly (Sun) unless AAR_SCOPUS_LANE_CADENCE=daily
        run_scopus_ithaca_lane_if_due()       # OFF unless AAR_SCOPUS_ITHACA_LANE=on: the same
                                              # weekly (Sun) Scopus lane over the Cornell Ithaca
                                              # AF-ID family; needs the Ithaca identity load first
        run_pubmed_lane_if_due()              # DAILY by default (AAR_PUBMED_LANE_CADENCE)
        run_conflicts_refresh_if_due()        # weekly (Sun): refill empty COI rows (#130)
        run_aar_close_attributed()            # nightly: dismiss already-attributed open AAR rows (#186)
        run_aar_reconcile_drift_if_due()      # OFF unless AAR_DRIFT_CADENCE is set: refresh open AAR
                                              # rows whose stored evidence columns no longer match
                                              # what the matcher would write (#186)

    upload_log_to_s3()

    if not overall_success:
        logger.error("One or more scripts failed ❌")
        sys.exit(1)

    logger.info("All scripts completed successfully 🎉")
    sys.exit(0)

if __name__ == "__main__":
    main()
