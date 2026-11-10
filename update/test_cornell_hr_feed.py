"""Self-check for cornell_hr_feed: python3 test_cornell_hr_feed.py [real_extract.txt]"""
import datetime
import sys

import cornell_hr_feed as f

H = "\t".join(f.HR_COLUMNS)


def line(netid, typ, slot="1", unit="Sociology", group="Faculty", cat="Faculty Professorial",
         rank="Professor", tenure=""):
    vals = dict.fromkeys(f.HR_COLUMNS, "")
    vals.update(legal_name="Ann B Cee", legal_name_first="Ann", legal_name_last="Cee",
                legal_name_middle="B", netid=netid, appointment_type=typ,
                appointment_slot_number=slot, unit_name=unit,
                track_type_category_group=group, track_type_category=cat,
                academic_rank=rank, tenure_home=tenure)
    return "\t".join(vals[c] for c in f.HR_COLUMNS)


def test_parse_and_derive():
    text = "\r\n".join([
        H,
        line("ABC1", "Primary", unit="Plant Pathology", tenure="AG-SIPS - School of Integrative Plant Science"),
        line("abc1", "Joint", unit="Biomedical Sciences"),
        line("abc1", "Administrative", unit="Provosts Office"),
        line("pd2", "Primary", unit="Chemistry", group="Post Doctoral", cat="Post Doctoral", rank="Postdoc"),
        line("np3", "Dual", unit="Law School", rank="Lecturer"),
        line("np3", "Administrative", unit="Provosts Office"),
        "",
    ])
    rows = f.parse("﻿" + text)
    assert len(rows) == 6 and rows[0]["academic_tenure_status"] is None
    p = f.derive(rows)
    a = p["abc1"]
    assert a["primary_department"] == "School of Integrative Plant Science"   # tenure home wins
    assert a["other_departments"] == "Biomedical Sciences"                    # joint kept, admin dropped
    assert a["primary_title"] == "Professor" and a["is_faculty"] and not a["is_postdoc"]
    assert p["pd2"]["is_postdoc"] and p["pd2"]["primary_department"] == "Chemistry"
    assert p["np3"]["primary_department"] == "Law School"                     # no Primary row: Dual wins
    assert p["np3"]["other_departments"] is None


def test_refusals():
    for bad in ["wrong\theader", H + "\r\n" + "x\ty"]:
        try:
            f.parse(bad)
            raise AssertionError("accepted: " + bad[:20])
        except ValueError:
            pass
    rows = f.parse(H + "\n" + line("a1", "Primary"))
    try:
        f.check(rows, f.derive(rows))
        raise AssertionError("accepted a 1-person extract")
    except ValueError:
        pass


def test_latest_key_skips_summary():
    t = datetime.datetime(2026, 11, 9, 23)
    objs = [{"Key": "live/cindy/2026-11-09/223851_Appointment_Dataset_OSP_Weill_20261109.txt", "Size": 83, "LastModified": t},
            {"Key": "live/cindy/2026-11-09/232306_Appointment_Dataset_OSP_Weill_20261109.txt", "Size": 1217003, "LastModified": t},
            {"Key": "live/cindy/2026-12-07/010000_Appointment_Dataset_OSP_Weill_20261207.txt", "Size": 1300000,
             "LastModified": t + datetime.timedelta(days=28)},
            {"Key": "live/cindy/2026-12-07/010500_grad_students.txt", "Size": 900000,
             "LastModified": t + datetime.timedelta(days=29)}]

    class S3:
        def get_paginator(self, _):
            return self

        def paginate(self, **_):
            return [{"Contents": objs}]

    assert f.latest_key(S3(), "b", "live").endswith("010000_Appointment_Dataset_OSP_Weill_20261207.txt")


def real(path):
    rows = f.parse(open(path, encoding="utf-8").read())
    p = f.derive(rows)
    f.check(rows, p)
    print(f"{path}: {len(rows)} rows, {len(p)} people, "
          f"{sum(x['is_faculty'] for x in p.values())} faculty, "
          f"{sum(x['is_postdoc'] for x in p.values())} postdoc, "
          f"{sum(x['is_researcher'] for x in p.values())} researcher, "
          f"{sum(1 for x in p.values() if x['other_departments'])} with joint/dual units, "
          f"{sum(1 for x in p.values() if not x['primary_department'])} without a department")


if __name__ == "__main__":
    test_parse_and_derive()
    test_refusals()
    test_latest_key_skips_summary()
    print("ok")
    if len(sys.argv) > 1:
        real(sys.argv[1])
