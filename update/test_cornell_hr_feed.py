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
        line("tp4", "Primary", unit="Communication"),
        line("tp4", "Primary", slot="2", unit="Nutritional Sciences"),
        "",
    ])
    rows = f.parse("﻿" + text)
    assert len(rows) == 8 and rows[0]["academic_tenure_status"] is None
    p = f.derive(rows)
    a = p["abc1"]
    assert a["primary_department"] == "School of Integrative Plant Science"   # tenure home wins
    assert a["other_departments"] == "Biomedical Sciences"                    # joint kept, admin dropped
    assert a["primary_title"] == "Professor" and a["is_faculty"] and not a["is_postdoc"]
    assert p["pd2"]["is_postdoc"] and p["pd2"]["primary_department"] == "Chemistry"
    assert p["np3"]["primary_department"] == "Law School"                     # no Primary row: Dual wins
    assert p["np3"]["other_departments"] is None
    assert p["tp4"]["primary_department"] == "Communication"                  # slot 1 is main
    assert p["tp4"]["other_departments"] == "Nutritional Sciences"            # second Primary kept


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


def test_canonical_dept():
    for unit in ("Faculty and Research", "Tech Instruction", "JTC Instruction",
                 " faculty  AND research ", "Cornell Tech"):
        assert f.canonical_dept(unit) == "Cornell Tech", unit
    # everything else is returned untouched, whitespace included
    for dept in ("Computer Science", "Faculty Affairs", "Research", " Physics ", ""):
        assert f.canonical_dept(dept) == dept, dept
    assert f.canonical_dept(None) is None


def test_dept_groups():
    """Structure first: the table is edited by hand and an overlap would silently pool two
    departments."""
    members = [f._dept_norm(m) for ms in f.DEPT_GROUPS.values() for m in ms]
    assert len(members) == len(set(members)), "a name is in two groups"
    canon = {f._dept_norm(c) for c in f.DEPT_GROUPS}
    assert not canon & set(members), "a canonical name is a member of another group"
    for table in (f.DEPT_PARENTS, f.BYLINE_NAMES):
        for name in table:
            assert f.canonical_dept(name) == name, name     # keyed by canonical name
    for parent in f.DEPT_PARENTS.values():
        assert f.canonical_dept(parent) == parent and parent not in f.DEPT_PARENTS, parent
    for name in members + list(canon):
        assert f.canonical_dept(f.canonical_dept(name)) == f.canonical_dept(name), name

    # both HR spellings and the canonical name are one department, for venues and display
    for d in ("Electrical & Computer Engineering", "Electrical and Computer Eng"):
        assert f.canonical_dept(d) == f.venue_dept(d) == "Electrical & Computer Engineering"
    assert f.canonical_dept("CALS Food Science") == f.canonical_dept("Food Science") == "Food Science"
    # a section keeps its own name, borrows its parent's papers, and answers to both names
    assert f.canonical_dept("Plant Biology (Ag)") == "Plant Biology"
    assert f.venue_dept("Plant Biology (Ag)") == "School of Integrative Plant Science"
    assert f.dept_names("Plant Biology (Ag)") == (
        "Plant Biology", "School of Integrative Plant Science")
    # a unit covering several departments is a parent, never a synonym
    endowed = "College Academic Activity \u2013 Endowed"
    assert f.canonical_dept(endowed) == endowed
    assert f.canonical_dept("Johnson Grad School Mgmt") != f.canonical_dept("Hotel Administration")
    assert f.venue_dept("Johnson Grad School Mgmt") == f.venue_dept("SHA Teaching") == endowed
    # the two Psychology departments stay apart
    assert f.canonical_dept("CHE-Psychology") != f.canonical_dept("Psychology")
    assert f.dept_names("CHE-Psychology") == ("Psychology (Human Ecology)", "Psychology")
    assert f.dept_names("Physics") == ("Physics",)
    assert f.dept_names("") == f.dept_names(None) == ()


if __name__ == "__main__":
    test_canonical_dept()
    test_dept_groups()
    test_parse_and_derive()
    test_refusals()
    test_latest_key_skips_summary()
    print("ok")
    if len(sys.argv) > 1:
        real(sys.argv[1])
