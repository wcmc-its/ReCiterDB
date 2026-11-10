"""Cornell Ithaca HR appointments: the live feed behind identity_cornell.

identity_cornell is a one-time load of IthacaResearchFaculty.xlsx. Since
2026-11-09 Cornell (Cindy Sedlacek) pushes a monthly Workday appointment extract
to s3://cornell-data-inbound-<account>/<prefix>/cindy/ on the 7th, one row per
appointment (Primary / Joint / Dual / Administrative). This module mirrors the
newest one into two tables and puts a view over the roster:

  identity_cornell_appointment  the file, row for row
  identity_cornell_hr           one row per netid, derived from the above
  identity_cornell_current      identity_cornell with HR values layered on top,
                                plus the HR people the spreadsheet never had

Both roster consumers -- buildIdentity.cornell_ithaca() here and
scripts/sync_cornell_ithaca_identities.py (DynamoDB Identity) in ReCiter
Research -- read the view, so the spreadsheet stays exactly as loaded and an
empty HR table makes the view identical to it.

Separate tables rather than identity columns for the Jenzabar reason: the
identity upsert is COALESCE-guarded and cumulative, so it can never turn an
ended appointment off.
"""
import csv
import datetime
import io
import logging
import os
import re

logger = logging.getLogger(__name__)

APPOINTMENT_TABLE = "identity_cornell_appointment"
HR_TABLE = "identity_cornell_hr"
VIEW = "identity_cornell_current"

BUCKET = os.environ.get("CORNELL_HR_BUCKET", "cornell-data-inbound-665083158573")
# ponytail: "live" until the amended data request lands; Cindy's pushes go to
# test/ today, so in the cron this finds nothing and the view equals the roster.
PREFIX = os.environ.get("CORNELL_HR_PREFIX", "live")
FILE_RE = re.compile(r"/\d{6}_Appointment_Dataset_[^/]*\.txt$")

# The extract's header, verbatim. A different header refuses the load: a renamed
# or reordered column would otherwise land silently in the wrong field.
HR_COLUMNS = [
    "legal_name", "legal_name_last", "legal_name_first", "legal_name_middle",
    "netid", "employee_id", "appointment_type", "appointment_slot_number",
    "auh_unit_code", "auh_unit_name", "auh_discipline_code", "auh_discipline_name",
    "unit_code", "unit_name", "track_type_category_group", "track_type_category",
    "track_type", "academic_rank", "academic_tenure_status", "track_end_date",
    "academic_appointment_end_date", "highest_degree",
    "years_since_highest_degree_received", "year_of_highest_degree", "tenure_home",
]

MIN_PEOPLE = 2000          # first live file: 3,649. Far below = truncated extract.
MAX_BLANK_NETID = 0.01

# Which appointment wins when a person has no Primary row (5 people on 11-09).
TYPE_RANK = {"Primary": 0, "Joint": 1, "Dual": 2, "Administrative": 3}
# Units that count as an extra department. Administrative appointments are
# offices (Provost, a Dean's office), not places a byline names.
EXTRA_UNIT_TYPES = ("Joint", "Dual")

APPOINTMENT_DDL = f"""CREATE TABLE IF NOT EXISTS {APPOINTMENT_TABLE} (
  {", ".join(f"{c} VARCHAR(200) NULL" for c in HR_COLUMNS)},
  source_key VARCHAR(300) NOT NULL,
  loaded_at DATETIME NOT NULL,
  KEY idx_netid (netid)
)"""

HR_DDL = f"""CREATE TABLE IF NOT EXISTS {HR_TABLE} (
  netid VARCHAR(100) NOT NULL PRIMARY KEY,
  name VARCHAR(200), name_first VARCHAR(100), name_last VARCHAR(100),
  name_middle VARCHAR(100),
  primary_department VARCHAR(200),
  primary_title VARCHAR(200),
  other_departments VARCHAR(1000),
  track_group VARCHAR(100),
  is_faculty BOOLEAN NOT NULL DEFAULT FALSE,
  is_postdoc BOOLEAN NOT NULL DEFAULT FALSE,
  is_researcher BOOLEAN NOT NULL DEFAULT FALSE,
  source_key VARCHAR(300) NOT NULL,
  loaded_at DATETIME NOT NULL
)"""

# Every column either consumer selects or filters on. HR overrides department and
# title, and switches on the role flags HR asserts; it never switches one off,
# because the file is academic appointments only and says nothing about staff,
# students or emeriti. Being in the file means a current appointment, hence
# is_active_employee and is_academic.
VIEW_DDL = f"""CREATE OR REPLACE VIEW {VIEW} AS
SELECT c.netid, c.name, c.name_first, c.name_last,
       c.email, c.email_netid, c.email_alt_cu,
       COALESCE(h.primary_department, c.primary_department) AS primary_department,
       c.primary_job_title,
       COALESCE(h.primary_title, c.primary_business_title) AS primary_business_title,
       h.other_departments,
       c.affiliations_all,
       (c.is_active_employee OR h.netid IS NOT NULL) AS is_active_employee,
       (c.is_faculty OR COALESCE(h.is_faculty, 0)) AS is_faculty,
       c.is_rte_faculty, c.is_new_faculty,
       (c.is_academic OR h.netid IS NOT NULL) AS is_academic,
       c.is_rsrch_teach_title,
       (c.is_researcher OR COALESCE(h.is_researcher, 0)) AS is_researcher,
       c.is_proj_researcher,
       (c.is_postdoc OR COALESCE(h.is_postdoc, 0)) AS is_postdoc,
       c.is_student_grad_all, c.is_student_undergrad_all, c.is_incubator_assoc,
       c.is_staff, c.is_rsrch_center_empl,
       c.update_date
FROM identity_cornell c
LEFT JOIN {HR_TABLE} h ON h.netid = c.netid
UNION ALL
SELECT h.netid, h.name, h.name_first, h.name_last,
       NULL, CONCAT(h.netid, '@cornell.edu'), NULL,
       h.primary_department, NULL, h.primary_title, h.other_departments,
       NULL,
       1, h.is_faculty, 0, 0, 1, 0, h.is_researcher, 0, h.is_postdoc,
       0, 0, 0, 0, 0,
       DATE(h.loaded_at)
FROM {HR_TABLE} h
LEFT JOIN identity_cornell c ON c.netid = h.netid
WHERE c.netid IS NULL"""

HR_COLS = ["netid", "name", "name_first", "name_last", "name_middle",
           "primary_department", "primary_title", "other_departments", "track_group",
           "is_faculty", "is_postdoc", "is_researcher"]


def ensure_schema(cur):
    """Both tables and the view. Idempotent; empty tables make the view == roster."""
    for ddl in (APPOINTMENT_DDL, HR_DDL, VIEW_DDL):
        cur.execute(ddl)


def parse(text):
    """The extract's text -> list of dicts. Refuses a header it doesn't know."""
    reader = csv.reader(io.StringIO(text.lstrip("﻿")), delimiter="\t")
    header = [h.strip() for h in next(reader, [])]
    if header != HR_COLUMNS:
        raise ValueError(f"unexpected header: {header[:6]}... ({len(header)} columns)")
    rows = []
    for n, rec in enumerate(reader, start=2):
        if not any(v.strip() for v in rec):
            continue
        if len(rec) != len(HR_COLUMNS):
            raise ValueError(f"line {n}: {len(rec)} fields, expected {len(HR_COLUMNS)}")
        rows.append({c: (v.strip() or None) for c, v in zip(HR_COLUMNS, rec)})
    return rows


def _tenure_home_name(value):
    """'AG-SIPS - School of Integrative Plant Science' -> the name half."""
    if not value:
        return None
    code, sep, name = value.partition(" - ")
    return name.strip() if sep else value.strip()


def derive(rows):
    """Appointment rows -> {netid: identity_cornell_hr row}.

    Department is the tenure home when there is one (deans and others whose HR
    unit isn't their academic one), else the main appointment's unit. The main
    appointment is the Primary row with the lowest slot, or failing that the
    best-ranked other type.
    """
    by_netid = {}
    for r in rows:
        if r["netid"]:
            by_netid.setdefault(r["netid"].lower(), []).append(r)
    out = {}
    for netid, appts in by_netid.items():
        appts.sort(key=lambda r: (TYPE_RANK.get(r["appointment_type"], 9),
                                  int(r["appointment_slot_number"] or 99)))
        main = appts[0]
        dept = _tenure_home_name(main["tenure_home"]) or main["unit_name"]
        extra = list(dict.fromkeys(
            r["unit_name"] for r in appts[1:]
            if r["appointment_type"] in EXTRA_UNIT_TYPES and r["unit_name"]
            and r["unit_name"] != dept))
        group = main["track_type_category_group"] or ""
        out[netid] = {
            "netid": netid,
            "name": main["legal_name"],
            "name_first": main["legal_name_first"],
            "name_last": main["legal_name_last"],
            "name_middle": main["legal_name_middle"],
            "primary_department": dept,
            "primary_title": main["academic_rank"],
            "other_departments": "|".join(extra)[:1000] or None,
            "track_group": group or None,
            "is_faculty": any(r["track_type_category_group"] == "Faculty" for r in appts),
            "is_postdoc": any(r["track_type_category_group"] == "Post Doctoral" for r in appts),
            "is_researcher": any(r["track_type_category"] == "Research/Extension" for r in appts),
        }
    return out


def check(rows, people):
    """Refuse an extract that looks truncated or keyless."""
    blank = sum(1 for r in rows if not r["netid"])
    if not rows or blank / len(rows) > MAX_BLANK_NETID:
        raise ValueError(f"{blank} of {len(rows)} rows have no netid")
    if len(people) < MIN_PEOPLE:
        raise ValueError(f"only {len(people)} people, floor is {MIN_PEOPLE}")


def latest_key(s3, bucket=BUCKET, prefix=PREFIX):
    """Newest Appointment_Dataset file under <prefix>/, or None. Skips the
    row-count summary Cindy sends alongside (same name, 83 bytes) by size."""
    best = None
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        for o in page.get("Contents", []):
            # ponytail: size floor tells the summary from the data; a real
            # extract is ~1.2 MB. Ask Cindy to rename the summary if this bites.
            if FILE_RE.search("/" + o["Key"]) and o["Size"] > 10_000:
                if best is None or o["LastModified"] > best["LastModified"]:
                    best = o
    return best["Key"] if best else None


def load(conn, dry_run=False, s3=None):
    """Mirror the newest extract. Returns rows written (0 = nothing new)."""
    if s3 is None:
        import boto3
        s3 = boto3.client("s3")
    key = latest_key(s3)
    if not key:
        logger.info("%s: no extract under s3://%s/%s/ - nothing to load",
                    APPOINTMENT_TABLE, BUCKET, PREFIX)
        return 0
    with conn.cursor() as cur:
        ensure_schema(cur)
        cur.execute(f"SELECT MAX(source_key) FROM {HR_TABLE}")
        current = cur.fetchone()[0]
    if current == key:
        logger.info("%s: %s already loaded", APPOINTMENT_TABLE, key)
        return 0
    text = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8")
    rows = parse(text)
    people = derive(rows)
    check(rows, people)
    logger.info("%s: %s -> %d appointments, %d people (%d faculty, %d postdoc)",
                APPOINTMENT_TABLE, key, len(rows), len(people),
                sum(p["is_faculty"] for p in people.values()),
                sum(p["is_postdoc"] for p in people.values()))
    if dry_run:
        logger.info("%s: --dry-run, not written", APPOINTMENT_TABLE)
        return len(rows)
    now = datetime.datetime.utcnow().replace(microsecond=0)
    with conn.cursor() as cur:
        # DELETE, not TRUNCATE, so a failed INSERT rolls back to last month's rows.
        cur.execute(f"DELETE FROM {APPOINTMENT_TABLE}")
        cur.executemany(
            f"INSERT INTO {APPOINTMENT_TABLE} ({', '.join(HR_COLUMNS)}, source_key, loaded_at) "
            f"VALUES ({', '.join(['%s'] * (len(HR_COLUMNS) + 2))})",
            [tuple(r[c] for c in HR_COLUMNS) + (key, now) for r in rows])
        cur.execute(f"DELETE FROM {HR_TABLE}")
        cur.executemany(
            f"INSERT INTO {HR_TABLE} ({', '.join(HR_COLS)}, source_key, loaded_at) "
            f"VALUES ({', '.join(['%s'] * (len(HR_COLS) + 2))})",
            [tuple(p[c] for c in HR_COLS) + (key, now) for p in people.values()])
    conn.commit()
    logger.info("%s: reloaded from %s", HR_TABLE, key)
    return len(rows)
