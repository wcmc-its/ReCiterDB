#!/usr/bin/env python3
"""Pure-logic regression tests for the atomic table-swap nightly loader.

No database connection is made or required -- these exercise only the two
functions that decide *which* table name a statement targets and *what* the
final RENAME TABLE statement looks like:

1. shadow_table_name() -- routes LOAD DATA / UPDATE targets to `<table>_new`
   when ATOMIC_SWAP is on and the table participates in the swap, and leaves
   everything else (including person_temp) untouched.
2. build_swap_rename_sql() -- constructs the single atomic RENAME TABLE
   statement that promotes every `_new` shadow table into its live name.

Run: python3 test_atomic_swap.py
"""
import updateReciterDB as u


def test_shadow_table_name_off_by_default():
    # ATOMIC_SWAP defaults to False (no ATOMIC_SWAP=1 in this process's env).
    assert u.ATOMIC_SWAP is False
    for t in u.SWAP_TABLES:
        assert u.shadow_table_name(t) == t, f"expected no routing for {t} when ATOMIC_SWAP is off"
    assert u.shadow_table_name('person_temp') == 'person_temp'


def test_shadow_table_name_routes_swap_tables_when_on():
    original = u.ATOMIC_SWAP
    try:
        u.ATOMIC_SWAP = True
        for t in u.SWAP_TABLES:
            assert u.shadow_table_name(t) == f"{t}_new", f"{t} did not route to its shadow table"
    finally:
        u.ATOMIC_SWAP = original


def test_shadow_table_name_never_routes_person_temp():
    # person_temp is internal staging, explicitly excluded from the swap set,
    # and must never be routed to person_temp_new even when swap mode is on.
    original = u.ATOMIC_SWAP
    try:
        u.ATOMIC_SWAP = True
        assert 'person_temp' not in u.SWAP_TABLES
        assert u.shadow_table_name('person_temp') == 'person_temp'
    finally:
        u.ATOMIC_SWAP = original


def test_shadow_table_name_ignores_unknown_tables_when_on():
    # A table outside the swap set (e.g. analysis_summary_* tables owned by a
    # different job) must pass through unchanged even in swap mode.
    original = u.ATOMIC_SWAP
    try:
        u.ATOMIC_SWAP = True
        assert u.shadow_table_name('analysis_summary_person') == 'analysis_summary_person'
    finally:
        u.ATOMIC_SWAP = original


def test_swap_tables_excludes_person_temp():
    assert 'person_temp' not in u.SWAP_TABLES
    # And covers exactly the 10 real data tables named in updateReciterDB's
    # own all_tables list (person_temp aside).
    assert len(u.SWAP_TABLES) == 10
    assert len(set(u.SWAP_TABLES)) == 10, "SWAP_TABLES must not contain duplicates"


def test_build_swap_rename_sql_is_single_statement():
    sql = u.build_swap_rename_sql()
    assert sql.startswith("RENAME TABLE ")
    assert sql.endswith(";")
    # Exactly one statement -- no stray semicolons that would split this into
    # multiple non-atomic RENAMEs.
    assert sql.count(';') == 1


def test_build_swap_rename_sql_covers_every_swap_table_both_directions():
    sql = u.build_swap_rename_sql()
    for t in u.SWAP_TABLES:
        assert f"`{t}` TO `{t}_backup`" in sql, f"{t} -> {t}_backup clause missing"
        assert f"`{t}_new` TO `{t}`" in sql, f"{t}_new -> {t} clause missing"
    # And nothing else snuck in.
    assert "person_temp" not in sql


def test_build_swap_rename_sql_matches_ticket_form():
    # Verbatim shape requested: RENAME TABLE person TO person_backup,
    # person_new TO person, person_article TO person_article_backup, ... ;
    # Matches the naming precedent in setup/populateAnalysisSummaryTables_v2.sql
    # ("7. Atomic table swap") -- `_backup`, not `_old`.
    sql = u.build_swap_rename_sql()
    person_idx = sql.index("`person` TO `person_backup`")
    person_new_idx = sql.index("`person_new` TO `person`")
    person_article_idx = sql.index("`person_article` TO `person_article_backup`")
    assert person_idx < person_new_idx < person_article_idx, (
        "clause order must be: person->person_backup, person_new->person, "
        "person_article->person_article_backup, ..."
    )


def test_build_swap_rename_sql_maps_each_live_table_to_backup_and_each_new_table_to_live():
    # Explicit statement of the RENAME's meaning for every one of the 10
    # SWAP_TABLES: `<table>` -> `<table>_backup` and `<table>_new` -> `<table>`,
    # all within the single statement (not per-table separate RENAMEs).
    sql = u.build_swap_rename_sql()
    assert sql.count("RENAME TABLE") == 1
    for t in u.SWAP_TABLES:
        live_to_backup = f"`{t}` TO `{t}_backup`"
        new_to_live = f"`{t}_new` TO `{t}`"
        assert live_to_backup in sql, f"missing live->backup mapping for {t}"
        assert new_to_live in sql, f"missing new->live mapping for {t}"
        # The live->backup clause for this table must precede its new->live
        # clause, matching the precedent's per-table pair ordering.
        assert sql.index(live_to_backup) < sql.index(new_to_live), (
            f"{t}: expected `{t}` TO `{t}_backup` before `{t}_new` TO `{t}`"
        )


if __name__ == "__main__":
    test_shadow_table_name_off_by_default()
    test_shadow_table_name_routes_swap_tables_when_on()
    test_shadow_table_name_never_routes_person_temp()
    test_shadow_table_name_ignores_unknown_tables_when_on()
    test_swap_tables_excludes_person_temp()
    test_build_swap_rename_sql_is_single_statement()
    test_build_swap_rename_sql_covers_every_swap_table_both_directions()
    test_build_swap_rename_sql_matches_ticket_form()
    test_build_swap_rename_sql_maps_each_live_table_to_backup_and_each_new_table_to_live()
    print("OK: shadow-table routing and RENAME TABLE construction verified (no DB).")
