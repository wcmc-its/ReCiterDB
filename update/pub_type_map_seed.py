#!/usr/bin/env python3
"""Generate setup/seed_pub_type_map.sql from Publication Manager's src/lib/pubTypes.ts.

pub_type_map (v3.7) maps a source's raw work type onto PM's 18-type taxonomy, so the
reporting SP can fill analysis_summary_article.pubType / pubTypeGroup / pubTypeCounted.
The SP is SQL and can't import pubTypes.ts, so this script copies its CROSSREF, OPENALEX
and SCOPUS maps (and PUB_TYPE_GROUPS) into a seed file. pubTypes.ts stays the owner: when
it changes, regenerate the seed and commit both. PM's check script asserts they agree.

The RECITER map is the one part that lives here: it maps the publicationTypeCanonical
values ReCiter puts on PubMed rows (STEP 3), which pubTypes.ts doesn't cover. Five of them
have no slot in the 18 types and follow plan decision D1 (Case Report, Guideline and Article
-> Academic Article, Comment -> Letter, Editorial Article -> Editorial).

Keys are normalized the way pubTypes.ts normSourceType does it (trim, lower case, runs of
spaces/underscores -> '-'); the SP normalizes the raw value the same way before the join.
Errata, retractions, presentations and out-of-scope types get pub_type NULL and counted 0
(decision D4: they stay visible in Reports, only later counts skip them). A raw type with
no key at all is "unmapped": the SP leaves pubType NULL and counts it.

Usage:
  python3 update/pub_type_map_seed.py --pubtypes <PM>/src/lib/pubTypes.ts            # print SQL
  python3 update/pub_type_map_seed.py --pubtypes <PM>/src/lib/pubTypes.ts --write    # rewrite seed
  python3 update/pub_type_map_seed.py --pubtypes <PM>/src/lib/pubTypes.ts --check    # exit 1 on drift
  python3 update/pub_type_map_seed.py --selftest
"""
import argparse
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SEED_PATH = os.path.join(ROOT, "setup", "seed_pub_type_map.sql")

SOURCES = ("CROSSREF", "OPENALEX", "SCOPUS")  # pubTypes.ts maps copied into the seed
NOT_COUNTED = {"PRESENTATION", "OUT_OF_SCOPE", "ERRATUM", "RETRACTION"}

# ReCiter publicationTypeCanonical (PubMed rows) -> PM type. None = not counted (D4).
RECITER = {
    "Academic Article": "Academic Article",
    "Article": "Academic Article",
    "Case Report": "Academic Article",
    "Guideline": "Academic Article",
    "Review": "Review",
    "Letter": "Letter",
    "Comment": "Letter",
    "Editorial Article": "Editorial",
    "Conference Paper": "Conference Paper",
    "Preprint": "Preprint",
    "Erratum": None,
    "Retraction": None,
}


def norm(t):
    """pubTypes.ts normSourceType."""
    return re.sub(r"[\s_]+", "-", str(t or "").strip().lower())


def parse_groups(ts):
    """PUB_TYPE_GROUPS -> {type: group_key}."""
    block = re.search(r"export const PUB_TYPE_GROUPS = \[(.*?)\] as const;", ts, re.S)
    if not block:
        raise ValueError("PUB_TYPE_GROUPS not found")
    out = {}
    for key, types in re.findall(r'key:\s*"(\w+)".*?types:\s*\[([^\]]*)\]', block.group(1), re.S):
        for t in re.findall(r'"([^"]+)"', types):
            out[t] = key
    if len(out) != 18:
        raise ValueError(f"expected 18 types in PUB_TYPE_GROUPS, found {len(out)}")
    return out


def parse_map(ts, name):
    """const NAME: Record<string, SourceTypeMapping> = { key: pub("Type"), key: CONST, ... };"""
    block = re.search(rf"const {name}: Record<string, SourceTypeMapping> = \{{(.*?)\n\}};", ts, re.S)
    if not block:
        raise ValueError(f"map {name} not found")
    out = {}
    for line in block.group(1).splitlines():
        line = line.split("//", 1)[0].strip()
        if not line:
            continue
        m = re.match(r'^(?:"([^"]+)"|([A-Za-z0-9_-]+))\s*:\s*(.+?),?$', line)
        if not m:
            raise ValueError(f"{name}: can't parse line {line!r}")
        key = m.group(1) or m.group(2)
        val = m.group(3).strip()
        p = re.match(r'^pub\(\s*"([^"]+)"', val)
        if p:
            out[key] = p.group(1)
        elif val in NOT_COUNTED:
            out[key] = None
        else:
            raise ValueError(f"{name}: unknown mapping {val!r} for {key!r}")
    return out


def build_rows(ts):
    """[(source, source_type, pub_type, pub_type_group, counted)], sorted."""
    groups = parse_groups(ts)
    maps = {s: parse_map(ts, s) for s in SOURCES}
    maps["RECITER"] = {norm(k): v for k, v in RECITER.items()}
    rows = []
    for src, m in maps.items():
        for key, typ in m.items():
            if norm(key) != key:
                raise ValueError(f"{src}: key {key!r} is not normalized")
            if typ is not None and typ not in groups:
                raise ValueError(f"{src}:{key} maps to {typ!r}, not one of the 18 types")
            rows.append((src, key, typ, groups.get(typ), 0 if typ is None else 1))
    return sorted(rows)


def q(v):
    if v is None:
        return "NULL"
    if isinstance(v, int):
        return str(v)
    return "'" + v.replace("\\", "\\\\").replace("'", "''") + "'"


def render(rows):
    body = ",\n".join(f"  ({', '.join(q(c) for c in r)})" for r in rows)
    return f"""-- =============================================================================
-- Seed: pub_type_map (v3.7). GENERATED by update/pub_type_map_seed.py from Publication
-- Manager src/lib/pubTypes.ts (CROSSREF / OPENALEX / SCOPUS maps + PUB_TYPE_GROUPS) and
-- the script's RECITER map. Do not edit by hand: change pubTypes.ts, then regenerate with
--   python3 update/pub_type_map_seed.py --pubtypes <PM>/src/lib/pubTypes.ts --write
--
-- Apply after setup/alter_analysis_summary_article_pubtype_v3.7.sql, on both reciterdb
-- instances. Idempotent: replaces the whole map in one transaction.
-- =============================================================================

START TRANSACTION;
DELETE FROM pub_type_map;
INSERT INTO pub_type_map (source, source_type, pub_type, pub_type_group, counted) VALUES
{body};
COMMIT;

SELECT source, COUNT(*) AS n_keys, SUM(pub_type IS NULL) AS n_not_counted FROM pub_type_map GROUP BY source;
"""


SELFTEST_TS = '''
export const PUB_TYPE_GROUPS = [
  { key: "journal", label: "J", types: ["Academic Article", "Review", "Letter", "Editorial"] },
  { key: "conf", label: "C", types: ["Conference Paper", "Conference Abstract", "Conference Proceedings"] },
  { key: "book", label: "B", types: ["Book", "Edited Book", "Book Chapter", "Reference Entry"] },
  {
    key: "other",
    label: "O",
    types: ["Preprint", "Report", "Educational Material", "Non-Peer-Reviewed Writing", "Dataset", "Dissertation", "Software"],
  },
] as const;
const CROSSREF: Record<string, SourceTypeMapping> = {
  "journal-article": pub("Academic Article"),
  book: pub("Book"),
  grant: OUT_OF_SCOPE,
};

const OPENALEX: Record<string, SourceTypeMapping> = {
  article: pub("Academic Article"),
  erratum: ERRATUM,
};

// Scopus subtypeDescription
const SCOPUS: Record<string, SourceTypeMapping> = {
  "conference-paper": pub("Conference Paper"),
  cp: pub("Conference Paper"), // code
  retracted: RETRACTION,
};
'''


def selftest():
    rows = build_rows(SELFTEST_TS)
    d = {(r[0], r[1]): r[2:] for r in rows}
    assert d[("CROSSREF", "journal-article")] == ("Academic Article", "journal", 1)
    assert d[("CROSSREF", "grant")] == (None, None, 0)
    assert d[("OPENALEX", "erratum")] == (None, None, 0)
    assert d[("SCOPUS", "cp")] == ("Conference Paper", "conf", 1)
    assert d[("SCOPUS", "retracted")] == (None, None, 0)
    assert d[("RECITER", "case-report")] == ("Academic Article", "journal", 1)
    assert d[("RECITER", "editorial-article")] == ("Editorial", "journal", 1)
    assert d[("RECITER", "comment")] == ("Letter", "journal", 1)
    assert d[("RECITER", "erratum")] == (None, None, 0)
    assert norm(" Book_Chapter ") == "book-chapter" and norm("Short  Survey") == "short-survey"
    assert "'O''Brien'" == q("O'Brien")
    sql = render(rows)
    assert sql.count("(") >= len(rows) and "DELETE FROM pub_type_map;" in sql
    try:
        build_rows(SELFTEST_TS.replace('pub("Book")', 'pub("Novel")'))
        raise AssertionError("an unknown type must fail")
    except ValueError:
        pass
    print(f"PASS pub_type_map_seed selftest ({len(rows)} rows)")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pubtypes", help="path to PM src/lib/pubTypes.ts")
    ap.add_argument("--write", action="store_true", help=f"rewrite {os.path.relpath(SEED_PATH, ROOT)}")
    ap.add_argument("--check", action="store_true", help="exit 1 if the seed file differs from pubTypes.ts")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        selftest()
        return 0
    if not a.pubtypes:
        ap.error("--pubtypes is required")
    sql = render(build_rows(open(a.pubtypes).read()))
    if a.check:
        current = open(SEED_PATH).read() if os.path.exists(SEED_PATH) else ""
        if current != sql:
            print(f"DRIFT: {SEED_PATH} differs from {a.pubtypes}; rerun with --write")
            return 1
        print("OK: seed matches pubTypes.ts")
        return 0
    if a.write:
        with open(SEED_PATH, "w") as f:
            f.write(sql)
        print(f"wrote {SEED_PATH}")
        return 0
    sys.stdout.write(sql)
    return 0


if __name__ == "__main__":
    sys.exit(main())
