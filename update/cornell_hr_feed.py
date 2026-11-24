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
# Units that count as an extra department (a second Primary too). Administrative appointments are
# offices (Provost, a Dean's office), not places a byline names.
EXTRA_UNIT_TYPES = ("Primary", "Joint", "Dual")

# --- department names -------------------------------------------------------------
# One Cornell department reaches us under several names, at the same time: the 2026-11
# Workday extract shortened most of them ("Electrical and Computer Eng") while everyone
# it does not cover keeps the directory's ("Electrical & Computer Engineering"), and
# papers use a third. The string is a matcher key (venue_fit.DeptVenueFit, the
# identity_index department n-gram), so the spellings are grouped here and nowhere else.
# Three questions, three functions:
#
#   canonical_dept   what to show and store: one name per group (buildIdentity writes it,
#                    IdentityIndex reads it).
#   venue_dept       whose papers teach the venue-fit table: the group's, POOLED, and for
#                    a section too small to score, its parent unit's. A partition, so no
#                    paper is counted under two departments and a department with many
#                    spellings gets no extra tries at a high score.
#   dept_names       what a byline may call it: the canonical name plus other names papers
#                    use. ANY of them matches, as in ReCiter's OrganizationalUnitSynonyms.
#
# Groups come from prod (2026-11-23): the stored top_dept of accepted authorships beside
# the same person's department today. A name that covers several departments ("College
# Academic Activity - Endowed": Johnson and Hotel) is a PARENT, never a synonym. The
# tables this module loads keep the raw HR value; only consumers fold it.
#
# canonical name: other names HR / the directory / stored rows use for the same unit.
DEPT_GROUPS = {
    # Generic HR units that exist only at Cornell Tech (primary_college = 'TECH').
    "Cornell Tech": ("Faculty and Research", "Tech Instruction", "JTC Instruction"),
    # Engineering
    "Applied & Engineering Physics": ("Applied and Engineering Physics",),
    "Biological & Environmental Engineering": (
        "CALS Biological & Environmental Engr", "Bio and Environmental Engineer"),
    "Chemical & Biomolecular Engineering": ("Chemical and Biomolecular Eng",),
    "Civil & Environmental Engineering": ("Civil and Environmental Eng",),
    "Earth & Atmospheric Sciences": (
        "Earth and Atmospheric Sciences", "Earth and Atmospheric Sci (Ag)",
        "CALS Earth and Atmospheric Science"),
    "Electrical & Computer Engineering": ("Electrical and Computer Eng",),
    "Materials Science & Engineering": (
        "Materials Science Engineering", "Materials Science and Eng"),
    "Mechanical & Aerospace Engineering": ("Mechanical and Aerospace Eng",),
    "Operations Research & Information Engineering": (
        "Operations Research & Information Engr", "Oper Research and Info Eng"),
    # Agriculture & Life Sciences, Arts & Sciences
    "Animal Science": ("CALS Animal Science",),
    "Communication": ("CALS Communication",),
    "Computational Biology": ("CALS Computational Biology",),
    "Cornell Cooperative Extension": (
        "CALS Cornell Cooperative Extension", "Coop Ext Area Spec (Ag)"),
    "Ecology & Evolutionary Biology": (
        "Ecology and Evolutionary Biology", "CALS Ecology & Evolutionary Biology",
        "Ecology and Evolutionary Biology (Ag)", "Ecology and Evolutionary Biology (As)"),
    "Entomology": ("CALS Entomology", "Entomology (Geneva)"),
    "Food Science": ("CALS Food Science", "Food Science (Geneva)"),
    "Global Development": ("CALS Global Development",),
    "Horticulture": ("Horticultural Sci (Geneva)",),
    "Integrated Pest Management": (
        "CALS Integrated Pest Management", "Coop Ext Pest Mgmt (Ag)"),
    "Lab of Ornithology": ("CALS Lab of O - Administration",),
    "Landscape Architecture": ("CALS Landscape Architecture", "Landscape Architecture (Ag)"),
    "Microbiology": ("CALS Microbiology", "Microbiology (Ag)"),
    "Molecular Biology & Genetics": (
        "Molecular Biology and Genetics", "CALS Molecular Biology & Genetics",
        "Molecular Biology and Genetics (Ag)", "Molecular Biology and Genetics (As)"),
    "Natural Resources": ("CALS Natural Resources",),
    "Neurobiology & Behavior": (
        "Neurobiology and Behavior", "CALS Neurobiology & Behavior",
        "Neurobiology and Behavior (Ag)"),
    "Nutritional Sciences": (
        "CALS Nutritional Sciences", "Nutritional Sciences (Ag)", "Nutritional Sciences (He)"),
    "Plant Biology": ("Plant Biology (Ag)",),
    "Plant Pathology & Plant-Microbe Biology": (
        "Plant Pathology and Plant-Micro", "Plant Pathology (Geneva)"),
    "School of Integrative Plant Science": ("CALS School of Integrative Plant Science",),
    "Africana Studies and Research Center": ("Africana Studies and Research",),
    "History of Art and Visual Studies": ("History of Art",),
    "Performing and Media Arts": ("Theatre Arts",),
    "Knight Institute for Writing": ("John S. Knight Writing Institute", "Writing Program"),
    "Statistics & Data Science": (
        "Statistics and Data Science", "Department of Statistics & Data Science",
        "Statistical Science"),
    # Veterinary Medicine
    "Baker Institute": ("Jame Baker Institute for Animal Medicine",),
    "Biomedical & Translational Sciences": (
        "Biomedical and Translational Sciences", "Biomedical Sciences", "Molecular Medicine"),
    "Population Medicine & Diagnostic Sciences": (
        "Population Medicine & Diagnostic Science", "Pop Medicine & Diagnstc Sci"),
    # Human Ecology, Policy, ILR, Business, Law
    "Bronfenbrenner Center for Translational Research": (
        "Bronfenbrenner Ctr forTranslational Rsch",),
    "Design & Environmental Analysis": ("Design and Environment Analysis",),
    "Fiber Science & Apparel Design": ("Fiber Sci and Apparel Design",),
    "Psychology (Human Ecology)": ("CHE-Psychology", "CHE - Psychology"),
    "Public Policy": ("Policy Analysis and Management",),
    "Yang-Tan Institute": ("Yang Tan Institute", "Employment and Disability Inst"),
    "Applied Economics & Management": (
        "Applied Economics and Management", "CALS AEM", "Dyson - Teaching",
        "Dyson - Research", "Dyson - Extension", "College Academic Activity \u2013 Contract"),
    "Hotel Administration": ("SHA Teaching",),
    "Johnson Graduate School of Management": ("Johnson Grad School Mgmt", "Johnson Instruction"),
    "Law School": ("Law School Dean's Office", "Law Instruction"),
    # Laboratories and centers
    "Cornell Center for Materials Research": ("Ctr Materials Research",),
    "Cornell Laboratory for Accelerator-based Sciences & Education": (
        "Cornell Lab Accelerator Sciences & Ed",
        "Cornell Lab for Accelerator-based ScienceS & Ed"),
    "Cornell NanoScale Science & Technology Facility": (
        "NanoScale Science & Technology Facility", "Cornell Nanofab Facility"),
    "Institute of Biotechnology": (
        "Institute Biotechnology & Life Science", "Biotechnology Center"),
    "Laboratory of Atomic & Solid State Physics": (
        "Lab. of Atomic & Solid State Physics", "Lab Atomic and Solid State Phys"),
    "Radiophysics & Space Research": (
        "Radiophysics and Space Research", "Center Radiophysics and Space Research"),
    "Weill Institute for Cell & Molecular Biology": (
        "Weill Institute", "Inst Cell and Molecular Biology"),
}

# canonical name -> the unit whose papers stand in for it. Before the Workday names these
# people were stored under the parent, so this keeps the training they already had.
DEPT_PARENTS = {
    **dict.fromkeys(("Horticulture", "Plant Biology", "Plant Breeding and Genetics",
                     "Plant Pathology & Plant-Microbe Biology", "Soil and Crop Sciences"),
                    "School of Integrative Plant Science"),
    **dict.fromkeys(("Design & Environmental Analysis", "Fiber Science & Apparel Design"),
                    "Human Centered Design"),
    **dict.fromkeys(("Hotel Administration", "Johnson Graduate School of Management"),
                    "College Academic Activity \u2013 Endowed"),
}

# canonical name -> further names papers use. HR abbreviations are not listed: no byline
# says "Pop Medicine & Diagnstc Sci". "&"/"and" and a plural "s" are the matcher's job.
# A parent that a byline can name ("School of Integrative Plant Science") is added by
# dept_names itself.
BYLINE_NAMES = {
    "Biomedical & Translational Sciences": ("Biomedical Sciences", "Molecular Medicine"),
    "Laboratory of Atomic & Solid State Physics": ("LASSP",),
    "Psychology (Human Ecology)": ("Psychology",),
    "Public Policy": ("Policy Analysis and Management",),
    "Weill Institute for Cell & Molecular Biology": ("Weill Institute",),
    "Yang-Tan Institute": ("Employment and Disability Institute",),
}


def _dept_norm(dept):
    return " ".join(dept.split()).lower()


_CANONICAL = {_dept_norm(m): c for c, members in DEPT_GROUPS.items() for m in members}


def canonical_dept(dept):
    """The department to show and store: a DEPT_GROUPS member -> its canonical name (case
    and runs of whitespace ignored), anything else unchanged, byte for byte. Non-strings
    (None) pass through."""
    if not isinstance(dept, str):
        return dept
    return _CANONICAL.get(_dept_norm(dept), dept)


def venue_dept(dept):
    """The department whose papers score `dept` in the venue-fit table: its canonical
    name, or that name's parent unit."""
    dept = canonical_dept(dept)
    return DEPT_PARENTS.get(dept, dept)


def dept_names(dept):
    """Every name a byline may use for `dept`, canonical first; () for an empty one."""
    name = canonical_dept(dept)
    if not name:
        return ()
    return tuple(dict.fromkeys((name, *BYLINE_NAMES.get(name, ()), venue_dept(name))))


# Collation pinned to identity_cornell's (utf8mb4_unicode_ci); a schema default
# that differs (MySQL 8: utf8mb4_0900_ai_ci) breaks the view's join and UNION.
APPOINTMENT_DDL = f"""CREATE TABLE IF NOT EXISTS {APPOINTMENT_TABLE} (
  {", ".join(f"{c} VARCHAR(200) NULL" for c in HR_COLUMNS)},
  source_key VARCHAR(300) NOT NULL,
  loaded_at DATETIME NOT NULL,
  KEY idx_netid (netid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"""

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"""

# Every column either consumer selects or filters on. HR overrides department and
# title, and switches on the role flags HR asserts; it never switches one off,
# because the file is academic appointments only and says nothing about staff,
# students or emeriti. Being in the file means a current appointment, hence
# is_active_employee and is_academic.
# The spreadsheet also carries netids with no name at all (they arrived with only an ORCID
# iD). The HR name is all or nothing, so the two sources' halves are never mixed.
BLANK_NAME = ("h.netid IS NOT NULL AND COALESCE(c.name_first, '') = '' "
              "AND COALESCE(c.name_last, '') = ''")
VIEW_DDL = f"""CREATE OR REPLACE VIEW {VIEW} AS
SELECT c.netid,
       IF({BLANK_NAME}, h.name, c.name) AS name,
       IF({BLANK_NAME}, h.name_first, c.name_first) AS name_first,
       IF({BLANK_NAME}, h.name_last, c.name_last) AS name_last,
       c.email, COALESCE(c.email_netid, CONCAT(h.netid, '@cornell.edu')) AS email_netid,
       c.email_alt_cu,
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
    if not dry_run:
        # Creates the view on the first run after deploy, so both consumers have
        # it (== the spreadsheet) before any extract lands under the prefix.
        with conn.cursor() as cur:
            ensure_schema(cur)
    key = latest_key(s3)
    if not key:
        logger.info("%s: no extract under s3://%s/%s/ - nothing to load",
                    APPOINTMENT_TABLE, BUCKET, PREFIX)
        return 0
    current = None
    with conn.cursor() as cur:
        try:
            cur.execute(f"SELECT MAX(source_key) FROM {HR_TABLE}")
            current = cur.fetchone()[0]
        except Exception:                           # noqa: BLE001 - dry run before first load
            if not dry_run:
                raise
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
