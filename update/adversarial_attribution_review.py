#!/usr/bin/env python3
"""
Adversarial Attribution Review — detector (Phase 1).

Finds (uid, pmid) pairs that ReCiter is near-certain about on IDENTITY but the
feedback-dominated production score buries below the storage threshold, and that
no curator has accepted/rejected yet. These are likely MISSED authorships.

Mechanism (see docs/ADVERSARIAL_ATTRIBUTION_REVIEW_PLAN.md):
    final = min(feedback_calibrated, identity_calibrated * 33) * 100
    flag  = identity_only_score >= IO_CUTOFF
            AND final < STORAGE_THRESHOLD
            AND userAssertion == 'PENDING'   (not already accepted/rejected)

Data sources (read-only):
    - s3://feedbackscoring/{uid}-feedbackIdentityScoringInput.json  (feature vectors;
      the feedback input is a superset that also carries all identity base features)
    - local production model set in ReCiter---Scoring/app/models/

This is the same detector the weekly job uses; --sample runs it as an ROI probe.

Usage:
    python adversarial_attribution_review.py --sample 1000 [--io-cutoff 90] [--seed 42]
    python adversarial_attribution_review.py --uids stw2006 meb7002   # validation mode
"""
import argparse, io, json, os, random, sys, warnings, hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

warnings.filterwarnings("ignore")
import boto3
import numpy as np
import pandas as pd
import joblib

HERE = os.path.dirname(os.path.abspath(__file__))
# In-cluster (reciterdb image): the 6 models are baked at update/aar_models/ and
# preprocessing.py is vendored alongside this file. Fall back to the local
# ReCiter---Scoring checkout for Mac runs.
SCORING_REPO = os.path.expanduser("~/Dropbox/GitHub/ReCiter---Scoring/app")
MODELS = os.path.join(HERE, "aar_models") if os.path.isdir(os.path.join(HERE, "aar_models")) \
    else os.path.join(SCORING_REPO, "models")
sys.path.insert(0, HERE if os.path.exists(os.path.join(HERE, "preprocessing.py")) else SCORING_REPO)
from preprocessing import (  # noqa: E402
    FEEDBACK_IDENTITY_BASE_FEATURES, FEEDBACK_IDENTITY_FEATURES,
    IDENTITY_ONLY_BASE_FEATURES, IDENTITY_ONLY_FEATURES,
    compute_derived_features_feedback_identity,
    compute_derived_features_identity_only,
)

BUCKET = "feedbackscoring"
SUFFIX = "-feedbackIdentityScoringInput.json"
SAFETY_NET_MULT = 33          # final = min(fb, io*33)
STORAGE_THRESHOLD = 30        # reciter.minimumStorageThreshold

_s3 = boto3.client("s3", region_name="us-east-1")

# ---- load models once (thread-safe for inference) -------------------------
def _load(name):
    return joblib.load(os.path.join(MODELS, name))

FB_MODEL, FB_SCALER, FB_CALIB = (_load("feedbackIdentityModel.joblib"),
                                 _load("feedbackIdentityScaler.joblib"),
                                 _load("feedbackIdentityCalibrator.joblib"))
IO_MODEL, IO_SCALER, IO_CALIB = (_load("identityOnlyModel.joblib"),
                                 _load("identityOnlyScaler.joblib"),
                                 _load("identityOnlyCalibrator.joblib"))

def _file_sha(name):
    h = hashlib.sha256()
    with open(os.path.join(MODELS, name), "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:12]

MODEL_HASHES = {n: _file_sha(n) for n in (
    "feedbackIdentityModel.joblib", "identityOnlyModel.joblib")}

# ---- scoring ---------------------------------------------------------------
def _score(df, model, scaler, calib, base, deriver, feats):
    d = df.copy()
    for col in base:
        if col not in d.columns:
            d[col] = 0
    d[base] = d[base].fillna(0)
    d = deriver(d)
    X = scaler.transform(d[feats].values)
    raw = model.predict_proba(X)[:, 1]
    return calib.predict(raw)

PMID_KEYS = ("articleId", "id", "pmid", "pmidString")

def score_user(uid):
    """Download a user's feedback input, score both pipelines, return flagged rows.

    Fully defensive: never raises, so one malformed file can't abort the run."""
    key = uid + SUFFIX
    try:
        obj = _s3.get_object(Bucket=BUCKET, Key=key)
        articles = json.loads(obj["Body"].read())
    except _s3.exceptions.NoSuchKey:
        return ("missing", uid, [])
    except Exception as e:                       # noqa: BLE001
        return ("error", uid, [f"fetch:{e}"])
    try:
        if not isinstance(articles, list) or not articles:
            return ("empty", uid, [])
        df = pd.DataFrame(articles)
        pmid_key = next((k for k in PMID_KEYS if k in df.columns), None)
        if pmid_key is None:
            return ("error", uid, [f"no pmid field; cols={list(df.columns)[:6]}"])
        io_cal = _score(df, IO_MODEL, IO_SCALER, IO_CALIB,
                        IDENTITY_ONLY_BASE_FEATURES, compute_derived_features_identity_only,
                        IDENTITY_ONLY_FEATURES)
        fb_cal = _score(df, FB_MODEL, FB_SCALER, FB_CALIB,
                        FEEDBACK_IDENTITY_BASE_FEATURES, compute_derived_features_feedback_identity,
                        FEEDBACK_IDENTITY_FEATURES)
    except Exception as e:                        # noqa: BLE001
        return ("error", uid, [f"score:{e}"])
    io_score = io_cal * 100.0
    final = np.minimum(fb_cal, io_cal * SAFETY_NET_MULT) * 100.0
    df = df.assign(io_score=io_score, fb_score=fb_cal * 100.0, final_score=final)
    rows = []
    for _, r in df.iterrows():
        try:
            pmid = int(r[pmid_key])
        except (ValueError, TypeError):
            continue
        rows.append({
            "uid": uid,
            "pmid": pmid,
            "userAssertion": str(r.get("userAssertion")),
            "io_score": round(float(r["io_score"]), 2),
            "fb_score": round(float(r["fb_score"]), 2),
            "final_score": round(float(r["final_score"]), 2),
            "nameMatchFirst": float(r.get("nameMatchFirstScore", 0) or 0),
            "nameMatchLast": float(r.get("nameMatchLastScore", 0) or 0),
            "affilMatchType": float(r.get("targetAuthorInstitutionalAffiliationMatchTypeScore", 0) or 0),
            "relPosMatch": float(r.get("relationshipPositiveMatchScore", 0) or 0),
        })
    return ("ok", uid, rows)

def is_flagged(row, io_cutoff):
    return (row["userAssertion"] == "PENDING"
            and row["io_score"] >= io_cutoff
            and row["final_score"] < STORAGE_THRESHOLD)

# ---- sampling frame --------------------------------------------------------
def list_feedback_uids():
    uids = []
    paginator = _s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        for o in page.get("Contents", []):
            k = o["Key"]
            if k.endswith(SUFFIX):
                uids.append(k[:-len(SUFFIX)])
    return uids

# ---- main ------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=0, help="random sample size of feedback users")
    ap.add_argument("--uids", nargs="*", default=None, help="explicit uids (validation mode)")
    ap.add_argument("--io-cutoff", type=float, default=90.0)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--outdir", default=None)
    args = ap.parse_args()

    if args.uids:
        targets = args.uids
        frame_size = len(targets)
    else:
        print("Listing feedback-input sampling frame ...", flush=True)
        frame = list_feedback_uids()
        frame_size = len(frame)
        random.seed(args.seed)
        targets = random.sample(frame, min(args.sample, frame_size))
        print(f"  frame={frame_size} feedback users; sampling {len(targets)}", flush=True)

    all_rows, stats = [], {"ok": 0, "missing": 0, "empty": 0, "error": 0}
    errors = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(score_user, u): u for u in targets}
        for i, fut in enumerate(as_completed(futs), 1):
            status, uid, payload = fut.result()
            stats[status] = stats.get(status, 0) + 1
            if status == "ok":
                all_rows.extend(payload)
            elif status == "error":
                errors.append((uid, payload))
            if i % 100 == 0:
                print(f"  scored {i}/{len(targets)} users ...", flush=True)

    flagged = [r for r in all_rows if is_flagged(r, args.io_cutoff)]
    users_scanned = stats["ok"]
    users_with_flag = len(set(r["uid"] for r in flagged))

    # io-band distribution
    def band(v):
        return "99-100" if v >= 99 else "95-99" if v >= 95 else "90-95" if v >= 90 else "<90"
    bands = {"90-95": 0, "95-99": 0, "99-100": 0}
    for r in flagged:
        bands[band(r["io_score"])] += 1

    rate_per_user = users_with_flag / users_scanned if users_scanned else 0
    flags_per_user = len(flagged) / users_scanned if users_scanned else 0

    summary = {
        "run_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "validation" if args.uids else "sample-probe",
        "io_cutoff": args.io_cutoff,
        "storage_threshold": STORAGE_THRESHOLD,
        "model_hashes": MODEL_HASHES,
        "sampling_frame_feedback_users": frame_size,
        "users_targeted": len(targets),
        "scan_stats": stats,
        "flagged_pairs": len(flagged),
        "users_with_at_least_one_flag": users_with_flag,
        "pct_users_with_flag": round(100 * rate_per_user, 2),
        "flags_per_scanned_user": round(flags_per_user, 4),
        "io_band_distribution": bands,
        "EXTRAPOLATION_to_full_15353_feedback_users": {
            "est_users_with_flag": round(rate_per_user * 15353),
            "est_total_flagged_backlog": round(flags_per_user * 15353),
        } if not args.uids else None,
    }

    outdir = args.outdir or os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "analysis", "adversarial_attribution_review",
        datetime.now(timezone.utc).strftime("%Y-%m-%d") + ("-validation" if args.uids else "-probe"))
    os.makedirs(outdir, exist_ok=True)

    flagged.sort(key=lambda r: r["io_score"], reverse=True)
    pd.DataFrame(flagged).to_csv(os.path.join(outdir, "flagged.csv"), index=False)
    with open(os.path.join(outdir, "summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    if errors:
        with open(os.path.join(outdir, "errors.txt"), "w") as fh:
            for uid, p in errors:
                fh.write(f"{uid}\t{p}\n")

    print("\n==== SUMMARY ====")
    print(json.dumps(summary, indent=2))
    print(f"\nOutputs -> {outdir}")
    if args.uids:
        print("\nValidation rows:")
        for r in flagged:
            print(" ", r)

if __name__ == "__main__":
    main()
