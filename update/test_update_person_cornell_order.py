"""The Cornell Ithaca stub insert must run only in the final update_person pass. In Step 1
person_new is empty, so its NOT EXISTS excluded nobody and Cornell people with an Analysis
row got two person rows (844 cwids on prod, 2026-11-16)."""
import os
import sys
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import updateReciterDB as u  # noqa: E402


def run(add_cornell):
    sent = []
    with mock.patch.object(u, "execute_with_reconnect", lambda c, sql: sent.append(sql) or c):
        u.update_person(mock.MagicMock(), add_cornell=add_cornell)
    return sent


def test_step1_does_not_insert_cornell_stubs():
    assert not any("cornell-ithaca" in s for s in run(False))


def test_final_pass_inserts_then_updates_names():
    sent = run(True)
    assert "cornell-ithaca" in sent[0] and sent[1].lstrip().startswith("UPDATE")


def test_call_update_person_only_passes_add_cornell():
    with mock.patch.object(u, "establish_connection"), mock.patch.object(u, "update_person") as up:
        u.call_update_person_only()
    assert up.call_args.kwargs.get("add_cornell") is True


if __name__ == "__main__":
    test_step1_does_not_insert_cornell_stubs()
    test_final_pass_inserts_then_updates_names()
    test_call_update_person_only_passes_add_cornell()
    print("ok")
