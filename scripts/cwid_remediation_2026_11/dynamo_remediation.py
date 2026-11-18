#!/usr/bin/env python3
"""
CWID remediation, DynamoDB half (2026-11). Dry-run by default; --apply writes.

Driven by mapping.json next to this file. For every entry:

  MAP        bad uid -> canonical uid
               1. GoldStandard: union bad knownpmids/rejectedpmids into the canonical
                  item. A pair is ABORTED (nothing written, nothing deleted) when the
                  union would make a pmid both known and rejected. The canonical item's
                  other attributes are preserved (UpdateItem, not Put), and the write is
                  conditional on the item still holding exactly what was snapshotted
                  (GoldStandard has no version attribute, so the snapshot IS the version:
                  knownpmids, rejectedpmids and size(auditLog) must be unchanged).
               2. Identity: copy the bad item to the canonical key with uid replaced, but
                  only when the canonical item is missing. IDENTITY_REPLACE entries
                  (kym9003 <- Kym9003) replace the canonical item's whole `identity` map
                  with the bad item's, because the canonical one is another person's
                  (Annie Rohan's name, relationships and institutions). Fields only the
                  canonical map has are kept and listed in the dry run for a hand check;
                  every field that changes is listed too.
               3. FeedbackLog and ExternalArticle rows: copied to the canonical uid (same
                  sort key, never overwriting). The bad-key FeedbackLog rows are kept as
                  audit history.
               4. Delete the bad Identity, GoldStandard, Analysis and ESearchResult items
                  (exact-key DeleteItem), the ArticleProvenance / PmidProvenance rows
                  under the bad uid that are in the snapshot (rows written after it are
                  reported and left), and s3://reciter-dynamodb/AnalysisOutput/<bad>.
  DELETE     no real CWID: step 4 only. FeedbackLog / ExternalArticle rows are reported,
             never deleted.
  UNDERSCORE legacy PM external login with no ED CWID, on the explicit UNDERSCORE_DELETE
             list only (_jfr9006, _pan9087, _xyz1234): treated as DELETE when its
             GoldStandard holds no assertions, otherwise NEEDS-DECISION and left alone.
             An UNDERSCORE entry not on that list is treated as KEEP.
  KEEP       underscore test/external identities that hold assertions (_pab4001 and 13
             more, decided 11-18): never read, merged, deleted or re-classified.

Snapshots are stored in DynamoDB's typed form ({"S": ...}, {"SS": [...]}, {"N": "1"};
binary as base64), so --restore puts back string/number sets as sets, not lists.

Before anything else every affected item, for the bad AND canonical forms, is written
to snapshot_<UTC>.json (dry-run too: it is a local file). Table key schemas are read
with DescribeTable, never assumed. With --apply, each pair re-reads its canonical items
and is re-planned just before it is applied, so two bad uids that share a canonical
(DLAMB, dlamb -> djl2008) do not write against a stale copy. The canonical uids of the
pairs that applied are written to rerun_uids_<UTC>.txt for rerun_canonical.sh.

Usage:
  unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
  python3 dynamo_remediation.py --selftest
  python3 dynamo_remediation.py                 # dry-run: snapshot + per-uid plan
  python3 dynamo_remediation.py --apply         # writes (asks for confirmation)
  python3 dynamo_remediation.py --only DLAMB --only dlamb --apply
  python3 dynamo_remediation.py --restore snapshots/snapshot_<UTC>.json [--only X] --apply
"""
import argparse
import base64
import copy
import datetime as dt
import json
import os
import sys
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))
REGION = "us-east-1"
S3_BUCKET = "reciter-dynamodb"
S3_PREFIX = "AnalysisOutput/"
SINGLE_ITEM_TABLES = ["Identity", "GoldStandard", "Analysis", "ESearchResult"]
MULTI_ITEM_TABLES = ["FeedbackLog", "ArticleProvenance", "PmidProvenance", "ExternalArticle"]
DELETE_SINGLE = ["Identity", "GoldStandard", "Analysis", "ESearchResult"]
DELETE_MULTI = ["ArticleProvenance", "PmidProvenance"]
COPY_MULTI = ["FeedbackLog", "ExternalArticle"]
# canonical uid -> bad uid whose whole Identity is the right person (ED-confirmed);
# the canonical kym9003 item is Annie Rohan's CTSC record (she is really ajr7002)
IDENTITY_REPLACE = {"kym9003": "Kym9003"}
# the only underscore ids that may be deleted (and only while they hold no assertions)
UNDERSCORE_DELETE = {"_jfr9006", "_pan9087", "_xyz1234"}
# the underscore ids that hold assertions: kept as is, unconditionally (decided 11-18)
KEEP_EXPECTED = {"_joc9251", "_pab4001", "_pah4001", "_dar7342", "_djulius", "_huangfud",
                 "_jpp2757", "_lal2055", "_lsw8099", "_olo2760", "_pac2224", "_tap2707",
                 "_tem2089", "_uk28"}
SNAPSHOT_FORMAT = "dynamodb-typed-v2"
AUDIT_ACTOR = {"uid": "cwid-remediation-2026-11", "userVerbose": "CWID remediation 2026-11"}


# --------------------------------------------------------------------------- pure logic
def pmids(item, attr):
    """GoldStandard list attribute -> list of ints in stored order ([] when missing)."""
    if not item or not item.get(attr):
        return []
    return [int(x) for x in item[attr]]


def merge_gold(bad, canon):
    """Union a bad GoldStandard item into the canonical one.

    Returns dict(known, rejected, add_known, add_rejected, conflicts, preexisting).
    known/rejected keep the canonical order and append new pmids in the bad item's
    order. conflicts = pmids the union makes both known and rejected that were not
    already both in the canonical item; a non-empty set means ABORT the pair.
    """
    ck, cr = pmids(canon, "knownpmids"), pmids(canon, "rejectedpmids")
    bk, br = pmids(bad, "knownpmids"), pmids(bad, "rejectedpmids")
    add_k = [p for p in dict.fromkeys(bk) if p not in set(ck)]
    add_r = [p for p in dict.fromkeys(br) if p not in set(cr)]
    known, rejected = ck + add_k, cr + add_r
    preexisting = set(ck) & set(cr)
    conflicts = (set(known) & set(rejected)) - preexisting
    return {"known": known, "rejected": rejected, "add_known": add_k, "add_rejected": add_r,
            "conflicts": sorted(conflicts), "preexisting": sorted(preexisting)}


def gold_condition(item):
    """Optimistic-lock condition: the GoldStandard item still equals the snapshot."""
    parts, values = [], {}
    for attr, ph in (("knownpmids", ":ok"), ("rejectedpmids", ":or")):
        if item is None or attr not in item:
            parts.append(f"attribute_not_exists({attr})")
        else:
            parts.append(f"{attr} = {ph}")
            values[ph] = item[attr]
    if item is None or "auditLog" not in item:
        parts.append("attribute_not_exists(auditLog)")
    else:
        parts.append("size(auditLog) = :on")
        values[":on"] = len(item["auditLog"])
    if item is None:
        parts.append("attribute_not_exists(uid)")
    return " AND ".join(parts), values


def audit_entries(add_known, add_rejected, now):
    out = []
    for action, ids in (("ACCEPTED", add_known), ("REJECTED", add_rejected)):
        if ids:
            out.append({**AUDIT_ACTOR, "dateTime": now, "action": action,
                        "pmids": [Decimal(p) for p in ids]})
    return out


def renamed_identity(item, canon):
    new = copy.deepcopy(item)  # keeps sets as sets (a JSON round-trip would make them lists)
    new["uid"] = canon
    if isinstance(new.get("identity"), dict):
        new["identity"]["uid"] = canon
    return new


def replaced_identity(bad_item, canon_item, canon):
    """IDENTITY_REPLACE: the canonical item's `identity` map becomes the bad item's
    (uid renamed). A field only the canonical map has (non-empty there, missing or empty
    in the bad map) is kept. Returns (new_identity_map, changed_fields, kept_fields)."""
    new = renamed_identity(bad_item, canon)["identity"]
    old = (canon_item or {}).get("identity") or {}
    empty = (None, "", [], {})
    kept = sorted(k for k, v in old.items() if v not in empty and new.get(k) in empty)
    for k in kept:
        new[k] = copy.deepcopy(old[k])
    changed = sorted(k for k in set(old) | set(new)
                     if json.dumps(old.get(k), default=_dec, sort_keys=True)
                     != json.dumps(new.get(k), default=_dec, sort_keys=True))
    return new, changed, kept


def norm_name(x):
    import unicodedata
    x = unicodedata.normalize("NFKD", x or "")
    return "".join(ch for ch in x if ch.isalnum()).lower()


def surname_ok(ed_last, *identity_items):
    """ED surname check for a MAP: the first Identity present (bad, else canonical) must
    carry ED's surname (accents, case, spaces and hyphens ignored; either may contain the
    other, for double-barrelled names). No Identity at all -> True (nothing to compare)."""
    wants = [norm_name(w) for w in (ed_last if isinstance(ed_last, list) else [ed_last])]
    for item in identity_items:
        if item:
            last = norm_name(((item.get("identity") or {}).get("primaryName") or {}).get("lastName"))
            return bool(last) and any(last == w or last in w or w in last for w in wants)
    return True


def resolve_action(entry, gold_bad=None):
    """KEEP stays KEEP. UNDERSCORE -> DELETE only for an id on UNDERSCORE_DELETE whose
    GoldStandard holds no assertions; on the list with assertions -> NEEDS-DECISION; not
    on the list -> KEEP (fail safe: an underscore id is never deleted by classification)."""
    if entry["action"] == "KEEP":
        return "KEEP"
    if entry["action"] != "UNDERSCORE":
        return entry["action"]
    if entry["bad"] not in UNDERSCORE_DELETE:
        return "KEEP"
    has = pmids(gold_bad, "knownpmids") or pmids(gold_bad, "rejectedpmids")
    return "NEEDS-DECISION" if has else "DELETE"


def _dec(o):
    if isinstance(o, Decimal):
        return int(o) if o == o.to_integral_value() else float(o)
    if isinstance(o, (set, frozenset)):
        return sorted(o, key=str)
    if isinstance(o, (bytes, bytearray)):
        return o.decode("latin-1")
    raise TypeError(type(o))


def _redec(o):
    if isinstance(o, float):
        return Decimal(str(o))
    if isinstance(o, int) and not isinstance(o, bool):
        return Decimal(o)
    if isinstance(o, dict):
        return {k: _redec(v) for k, v in o.items()}
    if isinstance(o, list):
        return [_redec(v) for v in o]
    return o


# ------------------------------------------------------------- typed snapshot form
def _av_to_json(av):
    """Low-level AttributeValue (TypeSerializer output) -> JSON-safe (B/BS as base64)."""
    (t, v), = av.items()
    if t == "B":
        return {"B": base64.b64encode(bytes(v.value if hasattr(v, "value") else v)).decode()}
    if t == "BS":
        return {"BS": [base64.b64encode(bytes(x.value if hasattr(x, "value") else x)).decode() for x in v]}
    if t == "M":
        return {"M": {k: _av_to_json(x) for k, x in v.items()}}
    if t == "L":
        return {"L": [_av_to_json(x) for x in v]}
    return {t: v}


def _av_from_json(av):
    """Inverse of _av_to_json: the low-level AttributeValue client.put_item takes."""
    (t, v), = av.items()
    if t == "B":
        return {"B": base64.b64decode(v)}
    if t == "BS":
        return {"BS": [base64.b64decode(x) for x in v]}
    if t == "M":
        return {"M": {k: _av_from_json(x) for k, x in v.items()}}
    if t == "L":
        return {"L": [_av_from_json(x) for x in v]}
    return {t: v}


def item_to_typed(item):
    """resource-style item (Decimal, set, Binary) -> {attr: JSON-safe AttributeValue}."""
    if item is None:
        return None
    from boto3.dynamodb.types import TypeSerializer
    ser = TypeSerializer()
    return {k: _av_to_json(ser.serialize(v)) for k, v in item.items()}


def typed_to_client(item):
    """Snapshot item -> low-level item for client.put_item / delete_item Key."""
    return {k: _av_from_json(v) for k, v in item.items()}


def typed_to_python(item):
    from boto3.dynamodb.types import TypeDeserializer
    de = TypeDeserializer()
    return {k: de.deserialize(v) for k, v in typed_to_client(item).items()}


def snapshot_to_typed(snap):
    """{uid: {table: item | [items] | None, s3_AnalysisOutput: ...}} -> typed copy."""
    out = {}
    for uid, tabs in snap.items():
        o = {}
        for t, v in tabs.items():
            if t in SINGLE_ITEM_TABLES:
                o[t] = item_to_typed(v)
            elif t in MULTI_ITEM_TABLES:
                o[t] = [item_to_typed(r) for r in v]
            else:
                o[t] = v
        out[uid] = o
    return out


def selftest():
    D = Decimal
    # plain union, canonical order kept, new pmids appended in bad order
    m = merge_gold({"knownpmids": [D(3), D(1)], "rejectedpmids": [D(9)]},
                   {"knownpmids": [D(1), D(2)]})
    assert m["known"] == [1, 2, 3] and m["add_known"] == [3], m
    assert m["rejected"] == [9] and m["add_rejected"] == [9] and m["conflicts"] == [], m
    # canonical missing entirely
    m = merge_gold({"knownpmids": [D(5)]}, None)
    assert m["known"] == [5] and m["rejected"] == [] and m["conflicts"] == []
    # bad known vs canonical rejected -> conflict, abort
    m = merge_gold({"knownpmids": [D(7)]}, {"rejectedpmids": [D(7)]})
    assert m["conflicts"] == [7], m
    # bad rejected vs canonical known -> conflict
    m = merge_gold({"rejectedpmids": [D(8)]}, {"knownpmids": [D(8)]})
    assert m["conflicts"] == [8], m
    # bad item itself contradictory -> conflict
    m = merge_gold({"knownpmids": [D(4)], "rejectedpmids": [D(4)]}, {})
    assert m["conflicts"] == [4], m
    # a contradiction the canonical ALREADY had is reported, not a new conflict
    m = merge_gold({"knownpmids": [D(6)]}, {"knownpmids": [D(6)], "rejectedpmids": [D(6)]})
    assert m["conflicts"] == [] and m["preexisting"] == [6] and m["add_known"] == [], m
    # nothing to add
    m = merge_gold({"knownpmids": [D(1)]}, {"knownpmids": [D(1)]})
    assert m["add_known"] == [] and m["add_rejected"] == []
    # both empty / missing
    m = merge_gold(None, None)
    assert m == {"known": [], "rejected": [], "add_known": [], "add_rejected": [],
                 "conflicts": [], "preexisting": []}
    # duplicate pmids inside the bad list are added once
    m = merge_gold({"knownpmids": [D(2), D(2)]}, {"knownpmids": [D(1)]})
    assert m["known"] == [1, 2], m
    # optimistic-lock condition
    c, v = gold_condition(None)
    assert "attribute_not_exists(uid)" in c and v == {}
    c, v = gold_condition({"uid": "x", "knownpmids": [D(1)], "auditLog": [{}, {}]})
    assert "knownpmids = :ok" in c and "attribute_not_exists(rejectedpmids)" in c
    assert v[":on"] == 2 and v[":ok"] == [D(1)]
    # audit entries only for non-empty adds
    a = audit_entries([1], [], "t")
    assert len(a) == 1 and a[0]["action"] == "ACCEPTED" and a[0]["pmids"] == [D(1)]
    assert audit_entries([], [], "t") == []
    # rename replaces both uid fields, leaves the rest, does not mutate the source
    src = {"uid": "Kym9003", "identity": {"uid": "Kym9003", "primaryName": {"lastName": "M"}}}
    r = renamed_identity(src, "kym9003")
    assert r["uid"] == "kym9003" and r["identity"]["uid"] == "kym9003"
    assert r["identity"]["primaryName"] == {"lastName": "M"} and src["uid"] == "Kym9003"
    # IDENTITY_REPLACE: the bad map wins field by field; canonical-only fields are kept
    rohan = {"uid": "kym9003", "identity": {
        "uid": "kym9003", "primaryName": {"firstName": "Annie", "lastName": "Rohan"},
        "knownRelationships": [{"type": "CTSC", "name": {"lastName": "Chambers"}}, {"type": "CTSC"}],
        "institutions": ["Stony Brook University ", "New York Presbyterian Hospital"],
        "primaryEmail": "Kym9003@nyp.org", "title": None, "emails": ["a@b"]}}
    merandy = {"uid": "Kym9003", "identity": {
        "uid": "Kym9003", "primaryName": {"firstName": "Kyle", "lastName": "Merandy"},
        "knownRelationships": [{"type": "CTSC", "name": {"lastName": "Lee"}}],
        "institutions": ["New York Presbyterian Hospital"], "emails": ["a@b"]}}
    new, changed, kept = replaced_identity(merandy, rohan, "kym9003")
    assert new["uid"] == "kym9003" and new["primaryName"]["lastName"] == "Merandy", new
    assert len(new["knownRelationships"]) == 1 and new["institutions"] == ["New York Presbyterian Hospital"]
    assert kept == ["primaryEmail"] and new["primaryEmail"] == "Kym9003@nyp.org", kept
    assert changed == ["institutions", "knownRelationships", "primaryName"], changed
    assert merandy["identity"]["uid"] == "Kym9003" and rohan["identity"]["primaryName"]["lastName"] == "Rohan"
    # underscore resolution: only the explicit list is ever deleted
    u = {"action": "UNDERSCORE", "bad": "_xyz1234"}
    assert resolve_action(u, None) == "DELETE"
    assert resolve_action(u, {"knownpmids": []}) == "DELETE"
    assert resolve_action(u, {"rejectedpmids": [D(1)]}) == "NEEDS-DECISION"
    assert resolve_action({"action": "UNDERSCORE", "bad": "_pab4001"}, None) == "KEEP"
    assert resolve_action({"action": "KEEP", "bad": "_pab4001"}, None) == "KEEP"
    assert resolve_action({"action": "KEEP", "bad": "_tem2089"}, {"knownpmids": []}) == "KEEP"
    assert resolve_action({"action": "MAP", "bad": "X"}, {"knownpmids": [D(1)]}) == "MAP"
    # a KEEP entry plans to nothing and needs no snapshot of its uid
    kp = plan_entry({"bad": "_pab4001", "canon": None, "action": "KEEP"}, {})
    assert kp["action"] == "KEEP" and kp["steps"] == [] and kp["abort"] is None, kp
    # typed snapshot round-trip: string/number sets stay sets (ArticleProvenance.ads,
    # Analysis relationshipType are SS), numbers stay numbers, binary survives
    item = {"uid": "x", "ads": {"a", "b"}, "ns": {D(1), D(2)}, "n": D("3.5"), "flag": True,
            "nul": None, "bin": b"\x00\xff", "m": {"relationshipType": {"MENTOR"},
                                                 "l": [D(1), "s", {"z"}]}}
    snap_json = json.loads(json.dumps(snapshot_to_typed({"x": {"ArticleProvenance": [item],
                                                              "Analysis": item,
                                                              "s3_AnalysisOutput": None}})))
    row = snap_json["x"]["ArticleProvenance"][0]
    assert sorted(row["ads"]["SS"]) == ["a", "b"], row["ads"]
    assert snap_json["x"]["Analysis"]["m"]["M"]["relationshipType"] == {"SS": ["MENTOR"]}
    low = typed_to_client(row)
    assert set(low["ads"]["SS"]) == {"a", "b"} and set(low["ns"]["NS"]) == {"1", "2"}
    assert low["bin"] == {"B": b"\x00\xff"}, low["bin"]
    back = typed_to_python(row)
    assert back["ads"] == {"a", "b"} and isinstance(back["ads"], set), back
    assert back["ns"] == {D(1), D(2)} and back["n"] == D("3.5") and back["flag"] is True
    assert back["nul"] is None and bytes(back["bin"].value) == b"\x00\xff"
    assert back["m"]["relationshipType"] == {"MENTOR"} and back["m"]["l"][2] == {"z"}
    # renaming an Identity keeps its sets as sets
    r = renamed_identity({"uid": "A", "identity": {"uid": "A", "tags": {"t"}}}, "a")
    assert r["identity"]["tags"] == {"t"}, r
    # ED surname guard (the 11-18 underscore near-miss: _pah4001 is Paul A. Harris,
    # ED pah4001 is Parviz Hejazi Pastor)
    harris = {"identity": {"primaryName": {"lastName": "Harris"}}}
    assert not surname_ok("Hejazi Pastor", harris, None)
    assert surname_ok("Nuñez", {"identity": {"primaryName": {"lastName": "Nunez"}}})
    assert surname_ok("Chanler-Berat", {"identity": {"primaryName": {"lastName": "Chanler Berat"}}})
    assert surname_ok("Lamb", None, {"identity": {"primaryName": {"lastName": "LAMB"}}})
    assert surname_ok("Kim", None, None)
    assert surname_ok(["Shahrokni", "Shakrokni"], {"identity": {"primaryName": {"lastName": "Shakrokni"}}})
    assert not surname_ok("Kim", {"identity": {"primaryName": {}}})
    # mapping.json sanity: every MAP has a canonical, canonicals are canonical, no dupes
    entries = load_mapping()
    bads = [e["bad"] for e in entries]
    assert len(bads) == len(set(bads)), "duplicate bad uid in mapping.json"
    keep = {e["bad"] for e in entries if e["action"] == "KEEP"}
    assert keep == KEEP_EXPECTED, sorted(keep ^ KEEP_EXPECTED)
    under = {e["bad"] for e in entries if e["action"] == "UNDERSCORE"}
    assert under == UNDERSCORE_DELETE, sorted(under ^ UNDERSCORE_DELETE)
    for e in entries:
        if e["bad"].startswith("_"):
            assert e["action"] in ("KEEP", "UNDERSCORE"), e
    for e in entries:
        if e["action"] == "MAP":
            c = e["canon"]
            assert c and c == c.strip().lower() and " " not in c and "@" not in c, e
            assert c != e["bad"], e
            assert e.get("ed_last"), f"MAP without an ED surname: {e}"
        else:
            assert e["canon"] is None, e
    # mysql_remediation.sql's rm_map rows must match mapping.json exactly
    sql_rows = sql_map_rows(os.path.join(HERE, "mysql_remediation.sql"))
    want = {(e["bad"], e["canon"], e["action"]) for e in entries if e["action"] in ("MAP", "DELETE")}
    assert sql_rows == want, (sorted(sql_rows ^ want, key=str))
    print("selftest OK")


def sql_map_rows(path):
    """(bad, canon, action) tuples from the rm_map INSERT block of the SQL file."""
    import re
    text = open(path).read()
    block = text.split("-- BEGIN rm_map", 1)[1].split("-- END rm_map", 1)[0]
    rows = set()
    for m in re.finditer(r"\(\s*'((?:[^'\\]|''|\\.)*)'\s*,\s*(NULL|'([^']*)')\s*,\s*'(MAP|DELETE)'\s*\)", block):
        bad = m.group(1).replace("''", "'")
        rows.add((bad, m.group(3) if m.group(2) != "NULL" else None, m.group(4)))
    return rows


def load_mapping(path=None):
    with open(path or os.path.join(HERE, "mapping.json")) as f:
        return json.load(f)["entries"]


# --------------------------------------------------------------------------- AWS side
class Ddb:
    def __init__(self, region):
        import boto3
        self.res = boto3.resource("dynamodb", region_name=region)
        self.client = self.res.meta.client
        self.s3 = boto3.client("s3", region_name=region)
        self.keys = {}
        for t in SINGLE_ITEM_TABLES + MULTI_ITEM_TABLES:
            ks = self.client.describe_table(TableName=t)["Table"]["KeySchema"]
            h = next(k["AttributeName"] for k in ks if k["KeyType"] == "HASH")
            r = next((k["AttributeName"] for k in ks if k["KeyType"] == "RANGE"), None)
            self.keys[t] = (h, r)
        for t in SINGLE_ITEM_TABLES:
            if self.keys[t][1] is not None:
                sys.exit(f"{t} has a range key {self.keys[t][1]}; this script assumes hash-only")
        for t in MULTI_ITEM_TABLES:
            if self.keys[t][1] is None:
                sys.exit(f"{t} has no range key; this script assumes hash+range")

    def get(self, table, uid):
        h, _ = self.keys[table]
        return self.res.Table(table).get_item(Key={h: uid}, ConsistentRead=True).get("Item")

    def query(self, table, uid):
        from boto3.dynamodb.conditions import Key
        h, _ = self.keys[table]
        t, out, kw = self.res.Table(table), [], {"KeyConditionExpression": Key(h).eq(uid)}
        while True:
            r = t.query(**kw)
            out += r["Items"]
            if "LastEvaluatedKey" not in r:
                return out
            kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]

    def s3_analysis_output(self, uid):
        """Exact-key AnalysisOutput object for uid (prefix listing, exact match filter)."""
        key = S3_PREFIX + uid
        r = self.s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=key)
        hit = [o for o in r.get("Contents", []) if o["Key"] == key]
        if not hit:
            return None
        head = self.s3.head_object(Bucket=S3_BUCKET, Key=key)
        return {"key": key, "size": hit[0]["Size"], "last_modified": str(hit[0]["LastModified"]),
                "version_id": head.get("VersionId")}

    def snapshot_uid(self, uid, s3_dir, with_s3=True):
        snap = {t: self.get(t, uid) for t in SINGLE_ITEM_TABLES}
        snap.update({t: self.query(t, uid) for t in MULTI_ITEM_TABLES})
        o = self.s3_analysis_output(uid) if with_s3 else None
        if o:
            # keep the bytes: an object written before versioning has VersionId "null"
            os.makedirs(s3_dir, exist_ok=True)
            local = os.path.join(s3_dir, "AnalysisOutput__" + uid.encode().hex())
            self.s3.download_file(S3_BUCKET, o["key"], local)
            o["local_copy"] = local
        snap["s3_AnalysisOutput"] = o
        return snap


def gs_summary(item):
    if not item:
        return "none"
    return f"{len(pmids(item, 'knownpmids'))}k/{len(pmids(item, 'rejectedpmids'))}r"


def ident_name(item):
    if not item:
        return "none"
    n = (item.get("identity") or {}).get("primaryName") or {}
    return " ".join(x for x in (n.get("firstName"), n.get("middleName"), n.get("lastName")) if x)


def plan_entry(e, snap):
    """Build the per-uid plan from the snapshot. Pure: no AWS calls."""
    bad, canon = e["bad"], e["canon"]
    if resolve_action(e) == "KEEP":
        return {"bad": bad, "canon": canon, "action": "KEEP", "steps": [], "warnings": [], "abort": None}
    b = snap[bad]
    c = snap.get(canon) if canon else None
    action = resolve_action(e, b["GoldStandard"])
    p = {"bad": bad, "canon": canon, "action": action, "steps": [], "warnings": [], "abort": None}
    if action == "NEEDS-DECISION":
        p["warnings"].append(f"GoldStandard holds assertions ({gs_summary(b['GoldStandard'])}); "
                             f"Identity name: {ident_name(b['Identity'])}. Not touched.")
        return p
    if action == "MAP":
        if e.get("ed_last") and not surname_ok(e["ed_last"], b["Identity"], c["Identity"]):
            p["abort"] = (f"surname check failed: ED {canon} is {e['ed_last']!r}, Identity says "
                          f"{ident_name(b['Identity'] or c['Identity'])!r}; not the same person?")
            return p
        if b["GoldStandard"]:
            m = merge_gold(b["GoldStandard"], c["GoldStandard"])
            p["gold"] = m
            if m["conflicts"]:
                p["abort"] = f"GoldStandard union would make pmids both known and rejected: {m['conflicts']}"
                return p
            if m["preexisting"]:
                p["warnings"].append(f"canonical GoldStandard already has known+rejected pmids "
                                     f"{m['preexisting']} (left as is)")
            if c["GoldStandard"] is None and not (m["known"] or m["rejected"]):
                p["steps"].append(("noop", f"GoldStandard: {bad!r} holds no assertions, nothing to carry"))
            elif c["GoldStandard"] is None:
                p["steps"].append(("gs_create", f"GoldStandard: create {canon} from {bad!r} "
                                   f"({len(m['known'])}k/{len(m['rejected'])}r)"))
            elif m["add_known"] or m["add_rejected"]:
                p["steps"].append(("gs_update", f"GoldStandard: {canon} +{len(m['add_known'])}k "
                                   f"+{len(m['add_rejected'])}r {m['add_known'] + m['add_rejected']}"))
            else:
                p["steps"].append(("noop", f"GoldStandard: {canon} already holds every bad assertion"))
        if b["Identity"] and not c["Identity"]:
            p["steps"].append(("ident_copy", f"Identity: create {canon} as a copy of {bad!r} "
                               f"(name: {ident_name(b['Identity'])})"))
        elif not b["Identity"] and not c["Identity"]:
            p["warnings"].append(f"no Identity under either form; the rerun of {canon} will fail "
                                 f"until one exists")
        if IDENTITY_REPLACE.get(canon) == bad and c["Identity"] and b["Identity"]:
            _, changed, kept = replaced_identity(b["Identity"], c["Identity"], canon)
            p["steps"].append(("ident_replace", f"Identity: {canon} identity map {ident_name(c['Identity'])!r} "
                               f"-> {bad!r}'s {ident_name(b['Identity'])!r}; fields changed: {changed}"))
            if kept:
                old = c["Identity"].get("identity") or {}
                p["warnings"].append(f"{canon}: kept canonical-only fields (check by hand): "
                                     + json.dumps({k: old[k] for k in kept}, default=_dec))
        for t in COPY_MULTI:
            if b[t]:
                p["steps"].append((f"copy_{t}", f"{t}: copy {len(b[t])} row(s) to {canon} "
                                   f"(skip any sort key already present)"))
    else:  # DELETE
        g = b["GoldStandard"]
        if g and (pmids(g, "knownpmids") or pmids(g, "rejectedpmids")):
            p["warnings"].append(f"deleting GoldStandard with assertions {gs_summary(g)} (decided: no real CWID)")
        for t in COPY_MULTI:
            if b[t]:
                p["warnings"].append(f"{t}: {len(b[t])} row(s) under {bad!r} are kept, not deleted")
    for t in DELETE_SINGLE:
        if b[t]:
            p["steps"].append((f"del_{t}", f"{t}: delete {bad!r}"))
    for t in DELETE_MULTI:
        if b[t]:
            p["steps"].append((f"del_{t}", f"{t}: delete {len(b[t])} row(s) under {bad!r}"))
    if b["s3_AnalysisOutput"]:
        o = b["s3_AnalysisOutput"]
        p["steps"].append(("del_s3", f"S3: delete s3://{S3_BUCKET}/{o['key']} ({o['size']} B, "
                           f"version {o['version_id']})"))
    return p


def apply_plan(ddb, e, p, snap, now):
    bad, canon = e["bad"], e["canon"]
    b = snap[bad]
    c = snap.get(canon) if canon else None
    kinds = {k for k, _ in p["steps"]}
    T = ddb.res.Table

    if "gs_create" in kinds:
        m = p["gold"]
        item = {k: v for k, v in b["GoldStandard"].items()}
        item["uid"] = canon
        item["knownpmids"] = [Decimal(x) for x in m["known"]]
        if m["rejected"]:
            item["rejectedpmids"] = [Decimal(x) for x in m["rejected"]]
        else:
            item.pop("rejectedpmids", None)
        item["auditLog"] = list(item.get("auditLog") or []) + audit_entries(m["add_known"], m["add_rejected"], now)
        T("GoldStandard").put_item(Item=item, ConditionExpression="attribute_not_exists(uid)")
    if "gs_update" in kinds:
        m = p["gold"]
        cond, vals = gold_condition(c["GoldStandard"])
        vals.update({":k": [Decimal(x) for x in m["known"]], ":a": audit_entries(m["add_known"], m["add_rejected"], now),
                     ":e": []})
        expr = "SET knownpmids = :k, auditLog = list_append(if_not_exists(auditLog, :e), :a)"
        if m["rejected"]:
            expr += ", rejectedpmids = :r"
            vals[":r"] = [Decimal(x) for x in m["rejected"]]
        T("GoldStandard").update_item(Key={"uid": canon}, UpdateExpression=expr,
                                      ConditionExpression=cond, ExpressionAttributeValues=vals)
    if "ident_copy" in kinds:
        T("Identity").put_item(Item=renamed_identity(b["Identity"], canon),
                               ConditionExpression="attribute_not_exists(uid)")
    if "ident_replace" in kinds:
        new, _, _ = replaced_identity(b["Identity"], c["Identity"], canon)
        # conditional on the canonical map still being the one planned against
        T("Identity").update_item(
            Key={"uid": canon}, UpdateExpression="SET #i = :n",
            ConditionExpression="attribute_exists(uid) AND #i = :old",
            ExpressionAttributeNames={"#i": "identity"},
            ExpressionAttributeValues={":n": new, ":old": c["Identity"]["identity"]})
    for t in COPY_MULTI:
        if f"copy_{t}" in kinds:
            h, r = ddb.keys[t]
            for row in b[t]:
                new = dict(row)
                new[h] = canon
                try:
                    T(t).put_item(Item=new, ConditionExpression=f"attribute_not_exists({r})")
                except ddb.client.exceptions.ConditionalCheckFailedException:
                    print(f"     {t}: {canon}/{row[r]} already present, kept existing")
    # deletes: only after every copy above succeeded (any exception stops this uid)
    if "del_GoldStandard" in kinds:
        cond, vals = gold_condition(b["GoldStandard"])
        cond = "attribute_exists(uid) AND " + cond
        kw = {"ConditionExpression": cond}
        if vals:
            kw["ExpressionAttributeValues"] = vals
        T("GoldStandard").delete_item(Key={"uid": bad}, **kw)
    for t in ("Identity", "Analysis", "ESearchResult"):
        if f"del_{t}" in kinds:
            T(t).delete_item(Key={ddb.keys[t][0]: bad})
    for t in DELETE_MULTI:
        if f"del_{t}" in kinds:
            # only the rows in the snapshot: anything written since has no backup
            h, r = ddb.keys[t]
            with T(t).batch_writer() as w:
                for row in b[t]:
                    w.delete_item(Key={h: row[h], r: row[r]})
            left = len(ddb.query(t, bad))
            if left:
                print(f"     {t}: {left} row(s) under {bad!r} written after the snapshot, left in place")
    if "del_s3" in kinds:
        ddb.s3.delete_object(Bucket=S3_BUCKET, Key=S3_PREFIX + bad)


def restore(ddb, snapshot_path, only, apply):
    """Put the snapshot's items back: every bad-form item and row, the canonical
    GoldStandard / Identity exactly as snapshotted (a canonical item that did not exist is
    deleted), canonical FeedbackLog/ExternalArticle rows the remediation copied are removed,
    and the S3 AnalysisOutput object is re-uploaded from the local copy.
    WARNING: canonical GoldStandard returns to its snapshot state, so curation done on it
    after the remediation is lost. Use --only to restore single uids."""
    doc = json.load(open(snapshot_path))
    if doc.get("format") != SNAPSHOT_FORMAT:
        sys.exit(f"{snapshot_path}: not a {SNAPSHOT_FORMAT} snapshot. Older snapshots stored sets "
                 f"as lists and would restore SS/NS attributes as L; refusing.")
    data = doc["uids"]
    cl = ddb.client
    entries = [e for e in load_mapping() if (not only or e["bad"] in only)]
    for e in entries:
        bad, canon = e["bad"], e["canon"]
        if bad not in data or resolve_action(e) == "KEEP":
            continue
        b = data[bad]
        print(f"restore {bad!r}" + (f" (and {canon})" if canon else ""))
        if not apply:
            continue
        # low-level client with the typed snapshot: SS/NS/BS come back as sets
        for t in SINGLE_ITEM_TABLES:
            if b[t]:
                cl.put_item(TableName=t, Item=typed_to_client(b[t]))
        for t in MULTI_ITEM_TABLES:
            for row in b[t]:
                cl.put_item(TableName=t, Item=typed_to_client(row))
        if canon and canon in data:
            c = data[canon]
            for t in ("GoldStandard", "Identity"):
                if c[t]:
                    cl.put_item(TableName=t, Item=typed_to_client(c[t]))
                else:
                    cl.delete_item(TableName=t, Key={ddb.keys[t][0]: {"S": canon}})
            for t in COPY_MULTI:
                h, r = ddb.keys[t]
                had = {json.dumps(row[r], sort_keys=True) for row in c[t]}
                for row in b[t]:
                    if json.dumps(row[r], sort_keys=True) not in had:
                        cl.delete_item(TableName=t, Key={h: {"S": canon}, r: _av_from_json(row[r])})
        o = b.get("s3_AnalysisOutput")
        if o and o.get("local_copy"):
            ddb.s3.upload_file(o["local_copy"], S3_BUCKET, o["key"])
    if not apply:
        print("dry-run: pass --apply to restore")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write (default is a read-only dry-run)")
    ap.add_argument("--only", action="append", default=[], help="limit to these bad uids (repeatable)")
    ap.add_argument("--snapshot-dir", default=os.path.join(HERE, "snapshots"))
    ap.add_argument("--region", default=REGION)
    ap.add_argument("--yes", action="store_true", help="skip the --apply confirmation prompt")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--restore", metavar="SNAPSHOT_JSON",
                    help="roll back from a snapshot (with --apply; --only to limit)")
    args = ap.parse_args()
    if args.selftest:
        return selftest()
    if args.restore:
        return restore(Ddb(args.region), args.restore, args.only, args.apply)

    entries = load_mapping()
    if args.only:
        unknown = set(args.only) - {e["bad"] for e in entries}
        if unknown:
            sys.exit(f"--only: not in mapping.json: {sorted(unknown)}")
        entries = [e for e in entries if e["bad"] in args.only]

    ddb = Ddb(args.region)
    print("key schemas (DescribeTable):",
          ", ".join(f"{t}={h}{'+' + r if r else ''}" for t, (h, r) in ddb.keys.items()))

    active = [e for e in entries if resolve_action(e) != "KEEP"]  # KEEP ids are never read
    uids = list(dict.fromkeys([e["bad"] for e in active] + [e["canon"] for e in active if e["canon"]]))
    now = dt.datetime.now(dt.timezone.utc)
    os.makedirs(args.snapshot_dir, exist_ok=True)
    s3_dir = os.path.join(args.snapshot_dir, f"s3_{now:%Y%m%dT%H%M%SZ}")
    snap = {u: ddb.snapshot_uid(u, s3_dir) for u in uids}
    path = os.path.join(args.snapshot_dir, f"snapshot_{now:%Y%m%dT%H%M%SZ}.json")
    with open(path, "w") as f:
        json.dump({"format": SNAPSHOT_FORMAT, "taken_utc": now.isoformat(),
                   "mode": "apply" if args.apply else "dry-run",
                   "region": args.region, "uids": snapshot_to_typed(snap)}, f, indent=1)
    print(f"snapshot: {path} ({len(uids)} uids, "
          f"{sum(len(v) for s in snap.values() for k, v in s.items() if isinstance(v, list))} multi-table rows)")

    plans = [plan_entry(e, snap) for e in entries]
    totals = {}
    for p in plans:
        tag = "ABORT" if p["abort"] else p["action"]
        totals[tag] = totals.get(tag, 0) + 1
        if p["action"] == "KEEP":
            continue
        print(f"\n[{tag}] {p['bad']!r} -> {p['canon'] or '-'}")
        if p["abort"]:
            print(f"   ABORT: {p['abort']}")
        for _, s in p["steps"]:
            print(f"   - {s}")
        for w in p["warnings"]:
            print(f"   ! {w}")
    print("\nsummary:", ", ".join(f"{k}={v}" for k, v in sorted(totals.items())))
    canon = sorted({p["canon"] for p in plans if p["action"] == "MAP" and not p["abort"]})
    print(f"canonical uids to re-run ({len(canon)}): {' '.join(canon)}")
    kept = [p["bad"] for p in plans if p["action"] == "KEEP"]
    if kept:
        print(f"KEEP, not touched ({len(kept)}): {' '.join(kept)}")
    nd = [p["bad"] for p in plans if p["action"] == "NEEDS-DECISION"]
    if nd:
        print(f"NEEDS-DECISION ({len(nd)}): {' '.join(nd)}")

    if not args.apply:
        print("\ndry-run: nothing written. Re-run with --apply to write.")
        return
    if not args.yes:
        if input(f"\nApply to DynamoDB/S3 in {args.region}? type 'apply': ").strip() != "apply":
            sys.exit("aborted")
    stamp = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"
    failed, aborted, rerun = [], [], set()
    for e, p in zip(entries, plans):
        if p["abort"] or p["action"] in ("KEEP", "NEEDS-DECISION") or not p["steps"]:
            continue
        try:
            live = snap
            if e["canon"]:
                # re-read the canonical side: an earlier pair may have written it
                live = dict(snap)
                live[e["canon"]] = ddb.snapshot_uid(e["canon"], None, with_s3=False)
                p2 = plan_entry(e, live)
                if p2["abort"]:
                    aborted.append(p["bad"])
                    print(f"ABORT {p['bad']!r} on re-plan: {p2['abort']}")
                    continue
                if [s for _, s in p2["steps"]] != [s for _, s in p["steps"]]:
                    print(f"   re-planned {p['bad']!r} against the live {e['canon']}:")
                    for _, s in p2["steps"]:
                        print(f"   - {s}")
                p = p2
            apply_plan(ddb, e, p, live, stamp)
            print(f"applied {p['bad']!r} -> {p['canon'] or '-'}")
            if p["action"] == "MAP":
                rerun.add(p["canon"])
        except Exception as ex:  # one uid failing must not stop the others
            failed.append(p["bad"])
            print(f"FAILED {p['bad']!r}: {ex}")
    rerun_path = os.path.join(args.snapshot_dir, f"rerun_uids_{now:%Y%m%dT%H%M%SZ}.txt")
    with open(rerun_path, "w") as f:
        f.write("".join(u + "\n" for u in sorted(rerun)))
    print(f"\ndone. failed: {failed or 'none'}. aborted on re-plan: {aborted or 'none'}.")
    print(f"canonical uids applied ({len(rerun)}), for rerun_canonical.sh: {rerun_path}")
    print(f"Snapshot for rollback: {path}")
    if failed or aborted:
        sys.exit(1)


if __name__ == "__main__":
    main()
