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

# ------------- AAR Scopus lane (weekly, isolated) -------------
def run_scopus_lane_if_due():
    """Weekly Scopus not-in-PubMed authorship detector (AAR / PM#775).

    Fully isolated from the reporting rebuild: gated to Sundays, skipped if its API
    keys are absent, and any failure is caught and logged so it can NEVER fail the
    nightly job. A pre-migration DB (missing authorship_review columns) surfaces here
    as a swallowed script failure, not a pipeline abort."""
    try:
        import datetime as _datetime
        if _datetime.datetime.utcnow().weekday() != 6:   # 6 = Sunday
            logger.info("Scopus lane: not due (runs weekly on Sundays) — skipped")
            return
        if not (os.getenv("SCOPUS_API_KEY") and os.getenv("SCOPUS_INST_TOKEN")):
            logger.warning("Scopus lane: SCOPUS_API_KEY/INST_TOKEN unset — skipped")
            return
        run_script("aarScopusLane", "python3 aar_universe_scopus.py --mode rolling --apply",
                   timeout_seconds=int(os.getenv("SCOPUS_TIMEOUT_SECONDS", "3600")))
    except Exception as e:
        logger.exception(f"Scopus lane failed (ignored — reporting unaffected): {e}")


# ------------- AAR PubMed lane (weekly, isolated) -------------
def run_pubmed_lane_if_due():
    """Weekly PubMed orphan-authorship detector + IO/FB scoring (AAR).

    Same isolation contract as the Scopus lane: Sunday-gated, keys-gated, wrapped in
    try/except with a timeout, so any failure is logged and can NEVER fail the nightly
    reporting rebuild. Ledger/processed_log state lives in S3 (--s3-state) because the
    CronJob has no persistent filesystem; upserts land in reciterdb.authorship_review
    (source='pubmed')."""
    try:
        import datetime as _datetime
        if _datetime.datetime.utcnow().weekday() != 6:   # 6 = Sunday
            logger.info("PubMed lane: not due (runs weekly on Sundays) — skipped")
            return
        if not os.getenv("PUBMED_API_KEY"):
            logger.warning("PubMed lane: PUBMED_API_KEY unset — skipped")
            return
        if not (os.getenv("AAR_S3_BUCKET") or os.getenv("S3_BUCKET")):
            logger.warning("PubMed lane: AAR_S3_BUCKET/S3_BUCKET unset (needed for --s3-state) — skipped")
            return
        run_script("aarPubmedLane", "python3 aar_orchestrator.py --mode recurring --s3-state",
                   timeout_seconds=int(os.getenv("PUBMED_LANE_TIMEOUT_SECONDS", "5400")))
    except Exception as e:
        logger.exception(f"PubMed lane failed (ignored — reporting unaffected): {e}")


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

    for name, cmd in scripts:
        #ok = run_script(name, cmd)
        ok = run_script(name, cmd, timeout_seconds=int(os.getenv("SCRIPT_TIMEOUT_SECONDS", "15000")))
        if not ok:
            overall_success = False
            logger.error("Stopping pipeline due to script failure.")
            break

    # AAR lanes — run only if the reporting rebuild succeeded, weekly, each isolated.
    if overall_success:
        run_scopus_lane_if_due()
        run_pubmed_lane_if_due()

    upload_log_to_s3()

    if not overall_success:
        logger.error("One or more scripts failed ❌")
        sys.exit(1)

    logger.info("All scripts completed successfully 🎉")
    sys.exit(0)

if __name__ == "__main__":
    main()
