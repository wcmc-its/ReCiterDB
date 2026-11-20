"""Self-check: python3 test_grad_school_cwid.py (or pytest)"""
import buildIdentity as b


def _row(**kw):
    raw = {c: None for _, c in b.GRAD_SCHOOL_COLUMNS}
    raw.update(jid="7", **kw)
    return dict(zip([c for _, c in b.GRAD_SCHOOL_COLUMNS], b._grad_school_row(raw)))


def test_cwid_trimmed_and_lowercased():
    assert _row(cwid=" Mbd4001 ")["cwid"] == "mbd4001"
    assert _row(cwid="abc1234")["cwid"] == "abc1234"


def test_blank_cwid_is_null_and_other_columns_keep_case():
    r = _row(cwid="  ", lastName=" McDonald ")
    assert r["cwid"] is None and r["lastName"] == "McDonald" and r["jid"] == 7


if __name__ == "__main__":
    test_cwid_trimmed_and_lowercased()
    test_blank_cwid_is_null_and_other_columns_keep_case()
    print("ok")
