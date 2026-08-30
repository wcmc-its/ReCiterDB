"""Check that a DynamoDB NULL attribute cannot silently drop a person from the CSVs.

A NULL attribute deserializes to None with the key PRESENT, so `.get(k, default)` returns
None and the default never fires. Iterating that raises, process_person_temp()'s per-person
`except` swallows it, and the person vanishes from person_temp.csv — which is what removed
names for 174 identities, because update_person() fills names by INNER JOIN against that file.

Run:  python3 update/test_person_temp_null_attributes.py
"""
import csv
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dataTransformer import process_person_temp, process_person_person_type  # noqa: E402


def rows_for(identities):
    with tempfile.TemporaryDirectory() as d:
        process_person_temp(identities, d)
        with open(os.path.join(d, "person_temp.csv")) as f:
            return list(csv.DictReader(f))


def person(uid, **identity):
    return {"uid": uid, "identity": {"primaryName": {"firstName": "Ada", "lastName": "Lovelace"},
                                     **identity}}


def main():
    # The exact shapes seen in prod. `emails: NULL` alone accounted for 170 of the 174 drops;
    # `knownRelationships: NULL` for the rest. brf9046 carried both.
    cases = [
        ("emails NULL",                       person("a1", emails=None)),
        ("knownRelationships NULL",           person("a2", knownRelationships=None)),
        ("both NULL (the brf9046 shape)",     person("a3", emails=None, knownRelationships=None)),
        ("keys absent entirely",              person("a4")),
        ("identity itself NULL",              {"uid": "a5", "identity": None}),
        ("primaryName NULL",                  {"uid": "a6", "identity": {"primaryName": None}}),
        ("a NULL inside knownRelationships",  person("a7", knownRelationships=[None, {"uid": "x"}])),
        ("populated, the control",            person("a8", emails=["ada@med.cornell.edu"],
                                                     knownRelationships=[{"uid": "x"}])),
    ]

    out = rows_for([c[1] for c in cases])
    got = {r["personIdentifier"] for r in out}
    failures = []

    for label, ident in cases:
        uid = ident["uid"]
        if uid not in got:
            failures.append(f"{label}: {uid} was DROPPED from person_temp.csv")
    print(f"kept {len(got)}/{len(cases)} identities")

    by_uid = {r["personIdentifier"]: r for r in out}

    # A dropped person is the whole bug, but a person kept WITHOUT a name is just as useless
    # to update_person, so assert the names actually survive.
    for uid in ("a1", "a2", "a3", "a4", "a7", "a8"):
        r = by_uid.get(uid)
        if r and not (r["firstName"] and r["lastName"]):
            failures.append(f"{uid}: kept but name is blank ({r['firstName']!r} {r['lastName']!r})")

    # The two genuinely nameless shapes should still be kept, just empty — never dropped.
    for uid in ("a5", "a6"):
        if uid in by_uid and (by_uid[uid]["firstName"] or by_uid[uid]["lastName"]):
            failures.append(f"{uid}: expected a blank name, got one")

    if by_uid.get("a8", {}).get("primaryEmail") != "ada@med.cornell.edu":
        failures.append("a8: preferred-domain email selection regressed")
    if by_uid.get("a7", {}).get("knownRelationshipCount") != "1":
        failures.append(f"a7: expected knownRelationshipCount 1, got "
                        f"{by_uid.get('a7', {}).get('knownRelationshipCount')!r}")

    # process_person_person_type carries the same trap. 2,448 live identities have
    # personTypes: NULL. No ROWS are lost when it fires (those people have no types to emit),
    # so counting output rows cannot tell fixed from broken — the only observable difference
    # is whether the per-identity `except` swallowed anything. Assert on that instead.
    pt_cases = [
        ("personTypes NULL",    {"uid": "p1", "identity": {"personTypes": None}}),
        ("identity NULL",       {"uid": "p2", "identity": None}),
        ("key absent",          {"uid": "p3", "identity": {}}),
        ("populated, control",  {"uid": "p4", "identity": {"personTypes": ["faculty", "postdoc"]}}),
    ]
    import dataTransformer
    swallowed = []
    real_log_error = dataTransformer.log_error
    dataTransformer.log_error = lambda pid, msg: swallowed.append((pid, msg))
    try:
        with tempfile.TemporaryDirectory() as d:
            dataTransformer.process_person_person_type([c[1] for c in pt_cases], d)
            with open(os.path.join(d, "person_person_type.csv")) as f:
                pt_rows = list(csv.DictReader(f))
    finally:
        dataTransformer.log_error = real_log_error

    emitted = [r["personIdentifier"] for r in pt_rows]
    if emitted != ["p4", "p4"]:
        failures.append(f"person_person_type: expected exactly p4's two types, got {emitted}")
    for pid, msg in swallowed:
        failures.append(f"person_person_type: {pid} raised and was swallowed -- {msg}")
    print(f"person_person_type: {len(pt_rows)} rows emitted, {len(swallowed)} swallowed exception(s)")

    for f in failures:
        print(f"  FAIL {f}")
    print("PASS — no identity is dropped by a NULL attribute" if not failures
          else f"\n{len(failures)} failure(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
