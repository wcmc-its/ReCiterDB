#!/usr/bin/env python3
"""v3.6 external_record DDL: migration and fresh-build files agree, and stay safe to re-run.

setup/alter_external_record_v3.6.sql brings existing databases up to the schema in
setup/table_external_record*.sql / table_external_rejection.sql /
table_external_dup_verdict.sql. Two copies of one CREATE drift, so this checks they are
identical, idempotent (IF NOT EXISTS, no DROP/TRUNCATE), pinned to utf8mb4_unicode_ci
(a general_ci table made STEP 6b inject zero rows, v1.8), and that none of the four
durable tables is in a truncate list.

Offline: no database. Run: python3 update/test_external_record_ddl.py
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SETUP = os.path.join(ROOT, "setup")
TABLES = ["external_record", "external_record_event", "external_rejection", "external_dup_verdict"]
STATUSES = "ENUM('PENDING','ACCEPTED','REJECTED','REMOVED','MERGED','AUTOMERGED','SUPERSEDED')"


def read(*p):
    with open(os.path.join(*p)) as f:
        return f.read()


def create_block(sql, table):
    m = re.search(rf"^CREATE TABLE IF NOT EXISTS `{table}` \(.*?^\) ENGINE=[^\n]*", sql, re.M | re.S)
    return m.group(0) if m else None


migration = read(SETUP, "alter_external_record_v3.6.sql")
code = re.sub(r"--[^\n]*", "", migration)
fails = []

for t in TABLES:
    fresh = create_block(read(SETUP, f"table_{t}.sql"), t)
    migr = create_block(migration, t)
    if not fresh:
        fails.append(f"setup/table_{t}.sql has no CREATE TABLE IF NOT EXISTS `{t}`")
        continue
    if migr != fresh:
        fails.append(f"{t}: migration CREATE differs from setup/table_{t}.sql")
    if not fresh.rstrip().endswith("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"):
        fails.append(f"{t}: not pinned to utf8mb4_unicode_ci")

if re.search(r"\b(DROP|TRUNCATE|DELETE)\b", code, re.I):
    fails.append("migration must not DROP/TRUNCATE/DELETE")
if re.search(r"\bCREATE TABLE\b(?! IF NOT EXISTS)", code):
    fails.append("every CREATE TABLE must be IF NOT EXISTS")

rec = create_block(migration, "external_record") or ""
for needle in [
    "UNIQUE KEY `uq_er_uid_article` (`uid`, `article_id`)",
    "KEY `ix_er_uid_status` (`uid`, `status`)",
    "KEY `ix_er_doi_norm` (`doi_norm`)",
    "KEY `ix_er_uid_title_year` (`uid`, `title_year_key`)",
    "KEY `ix_er_pmid` (`pmid`)",
    f"`status`                 {STATUSES} NOT NULL",
    f"`prev_status`            {STATUSES} NULL",
    "ENUM('SCOPUS','WOS','OPENALEX','CROSSREF','MANUAL','ORCID')",
    "`article_id`             VARCHAR(96)",   # matches external_article.article_id
]:
    if needle not in rec:
        fails.append(f"external_record missing: {needle}")
dv = create_block(migration, "external_dup_verdict") or ""
# PM orders the pair with a plain JS sort (code-point order); a collation-order CHECK
# would refuse pairs whose DOIs contain '_' or '(' (unicode_ci sorts them before digits).
if "CHECK (CAST(`key_a` AS BINARY) < CAST(`key_b` AS BINARY))" not in dv:
    fails.append("external_dup_verdict: the ordered-pair CHECK must compare in binary order")
ev = create_block(migration, "external_record_event") or ""
if ev.count(STATUSES) != 2:
    fails.append("external_record_event from/to_status must use the external_record status ENUM")

# The durable tables must never join a nightly truncate list.
for name in os.listdir(os.path.join(ROOT, "update")):
    if not name.endswith(".py") or name == os.path.basename(__file__):
        continue
    src = read(ROOT, "update", name)
    for t in TABLES:
        if re.search(rf"TRUNCATE[^\n]*\b{t}\b", src, re.I) or re.search(rf"all_tables[^\]]*['\"]{t}['\"]", src, re.S):
            fails.append(f"update/{name} truncates {t}")

# doi_norm mirrors PM's DOI normalisation: check the regex on the cases it must handle.
m = re.search(r"REGEXP_REPLACE\(LOWER\(TRIM\(`doi`\)\), '([^']+)', ''\)", rec)
if not m:
    fails.append("doi_norm expression not found")
else:
    pattern = m.group(1).replace("\\\\", "\\").replace("[[:space:]]", r"\s")
    for raw, want in [("10.1016/J.SURG.2026.110815", "10.1016/j.surg.2026.110815"),
                      ("https://doi.org/10.1/AB", "10.1/ab"),
                      ("http://dx.doi.org/10.1/ab", "10.1/ab"),
                      ("doi: 10.5/X", "10.5/x"),
                      (" 10.7/y ", "10.7/y")]:
        got = re.sub(pattern, "", raw.strip().lower())
        if got != want:
            fails.append(f"doi_norm({raw!r}) = {got!r}, want {want!r}")

for f in fails:
    print("FAIL", f)
if fails:
    sys.exit(1)
print("PASS test_external_record_ddl")
