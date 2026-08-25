#!/usr/bin/env python3
"""Pure-logic regression tests for the person_* table shadow-build / atomic-
swap design.

No database connection is made or required. Table lifecycle -- creating the
`_new` shadow tables, dropping stale `_backup` tables, and the atomic RENAME
itself -- lives entirely in the stored procedures defined by
setup/person_table_swap.sql (prepare_person_shadow_tables() and
swap_person_tables()), not in Python. These tests therefore split into two
groups:

1. updateReciterDB.py's shadow_table_name() -- routes LOAD DATA / UPDATE
   targets to `<table>_new` unconditionally for the 10 swap-managed tables
   (no flag), and leaves person_temp untouched. Also asserts no ATOMIC_SWAP
   symbol remains anywhere in the module.
2. setup/person_table_swap.sql -- parsed with a small DELIMITER-aware
   splitter that mimics how the mysql CLI consumes this file, to verify
   prepare_person_shadow_tables() drops each `_new` table before recreating
   it, and swap_person_tables() renames all 10 tables in both directions as
   a single RENAME TABLE statement.

Run: python3 test_person_table_swap.py
"""
import os
import re

import updateReciterDB as u

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL_FILE = os.path.join(REPO_ROOT, "setup", "person_table_swap.sql")


# ------------------------------------------------------------------------------
# Group 1: updateReciterDB.py shadow routing
# ------------------------------------------------------------------------------

def test_shadow_table_name_routes_swap_tables_unconditionally():
    for t in u.SWAP_TABLES:
        assert u.shadow_table_name(t) == f"{t}_new", f"{t} did not route to its shadow table"


def test_shadow_table_name_never_routes_person_temp():
    # person_temp is internal staging, explicitly excluded from the swap set,
    # and must never be routed to person_temp_new.
    assert 'person_temp' not in u.SWAP_TABLES
    assert u.shadow_table_name('person_temp') == 'person_temp'


def test_shadow_table_name_ignores_unknown_tables():
    # A table outside the swap set (e.g. analysis_summary_* tables owned by a
    # different job) must pass through unchanged.
    assert u.shadow_table_name('analysis_summary_person') == 'analysis_summary_person'


def test_swap_tables_excludes_person_temp_and_has_ten_entries():
    assert 'person_temp' not in u.SWAP_TABLES
    assert len(u.SWAP_TABLES) == 10
    assert len(set(u.SWAP_TABLES)) == 10, "SWAP_TABLES must not contain duplicates"


def test_no_atomic_swap_symbol_remains_in_module():
    # No env-flag gate anywhere -- table lifecycle is unconditional and lives
    # in setup/person_table_swap.sql now.
    assert not hasattr(u, 'ATOMIC_SWAP')
    assert not hasattr(u, '_SWAP_WARNING_EMITTED')
    assert not hasattr(u, 'build_swap_rename_sql')
    module_path = os.path.join(REPO_ROOT, "update", "updateReciterDB.py")
    with open(module_path) as f:
        source = f.read()
    assert 'ATOMIC_SWAP' not in source, "ATOMIC_SWAP symbol/reference still present in updateReciterDB.py"


def test_missing_procedure_raises_named_exception_type():
    # ShadowTableProcedureMissing must exist and be raisable/catchable as its
    # own type (main()'s except clause depends on this to avoid swallowing
    # the failure like other errors).
    assert issubclass(u.ShadowTableProcedureMissing, RuntimeError)


# ------------------------------------------------------------------------------
# Group 2: setup/person_table_swap.sql, parsed like the mysql CLI would
# ------------------------------------------------------------------------------

def parse_mysql_statements(sql_text):
    """Minimal simulation of how the mysql command-line client tokenizes a
    DELIMITER-wrapped .sql file: a `DELIMITER <tok>` line (alone on its own
    line) changes the active statement terminator; everything else
    accumulates until the active delimiter is seen, at which point that
    accumulated text (with the delimiter stripped) becomes one statement.
    Internal `;` characters are preserved untouched while delimiter is `//`,
    exactly like the real client -- that's the behavior this file depends on
    to keep each procedure body one statement.
    """
    delimiter = ';'
    statements = []
    buffer = ''
    for line in sql_text.split('\n'):
        stripped = line.strip()
        if stripped.upper().startswith('DELIMITER '):
            delimiter = stripped.split(None, 1)[1].strip()
            continue
        if stripped.startswith('--'):
            # Pure `--` comment line (this file has no trailing same-line
            # comments after code): the mysql CLI's parser tracks comment
            # state and never treats a delimiter character inside a comment
            # as a statement terminator, so a `;` here (e.g. in a header
            # comment like "Invoke with: CALL foo();") must not split or
            # terminate a statement. Drop the line rather than scan it.
            continue
        buffer += line + '\n'
        if buffer.rstrip().endswith(delimiter):
            stmt = buffer.rstrip()[: -len(delimiter)].strip()
            if stmt:
                statements.append(stmt)
            buffer = ''
    trailing = buffer.strip()
    if trailing:
        statements.append(trailing)
    return statements


def _read_sql():
    with open(SQL_FILE) as f:
        return f.read()


def test_sql_file_exists():
    assert os.path.isfile(SQL_FILE), f"expected {SQL_FILE} to exist"


def test_sql_parses_into_exactly_four_statements_with_internal_semicolons_preserved():
    statements = parse_mysql_statements(_read_sql())
    # DROP PROCEDURE / CREATE PROCEDURE, twice (prepare_person_shadow_tables,
    # swap_person_tables). Each CREATE PROCEDURE body is full of `;`-terminated
    # DROP TABLE / CREATE TABLE lines that must NOT have split the statement,
    # since the active delimiter at that point is `//`, not `;`.
    assert len(statements) == 4, f"expected 4 statements, got {len(statements)}:\n" + \
        "\n---\n".join(statements)
    kinds = [
        statements[0].upper().startswith("DROP PROCEDURE IF EXISTS `RECITERDB`.`PREPARE_PERSON_SHADOW_TABLES`"),
        statements[1].upper().startswith("CREATE DEFINER") and "PREPARE_PERSON_SHADOW_TABLES" in statements[1].upper(),
        statements[2].upper().startswith("DROP PROCEDURE IF EXISTS `RECITERDB`.`SWAP_PERSON_TABLES`"),
        statements[3].upper().startswith("CREATE DEFINER") and "SWAP_PERSON_TABLES" in statements[3].upper() and "PREPARE_PERSON_SHADOW_TABLES" not in statements[3].upper(),
    ]
    assert all(kinds), f"statement shapes did not match expected order: {kinds}"
    # Each CREATE PROCEDURE body still contains its internal `;`-terminated
    # lines (DROP/CREATE TABLE, and for swap_person_tables the RENAME TABLE
    # statement itself) -- proof they were preserved rather than split off
    # when the active delimiter was `//`, not `;`.
    assert statements[1].count(';') >= 10, "prepare_person_shadow_tables body lost its internal semicolons"
    assert statements[3].count(';') >= 10, "swap_person_tables body lost its internal semicolons"


def test_prepare_person_shadow_tables_drops_before_creating_each_table():
    statements = parse_mysql_statements(_read_sql())
    prepare_body = statements[1]
    for t in u.SWAP_TABLES:
        drop_stmt = f"DROP TABLE IF EXISTS {t}_new"
        create_stmt = f"CREATE TABLE {t}_new LIKE {t}"
        assert drop_stmt in prepare_body, f"missing drop of {t}_new in prepare_person_shadow_tables"
        assert create_stmt in prepare_body, f"missing create of {t}_new in prepare_person_shadow_tables"
        assert prepare_body.index(drop_stmt) < prepare_body.index(create_stmt), (
            f"{t}_new: DROP must precede CREATE -- a stale `_new` from a crashed "
            "run must never be silently reused (it may carry a stale schema if a "
            "migration has since altered the live table)"
        )


def test_swap_person_tables_renames_all_ten_tables_both_directions_as_one_statement():
    statements = parse_mysql_statements(_read_sql())
    swap_body = statements[3]
    # Exactly one RENAME TABLE keyword -- the promotion must be a single
    # atomic statement, not 10 separate per-table renames.
    assert swap_body.upper().count("RENAME TABLE") == 1, (
        "swap_person_tables must contain exactly one RENAME TABLE statement"
    )
    for t in u.SWAP_TABLES:
        live_to_backup = f"{t} TO {t}_backup"
        new_to_live = f"{t}_new TO {t}"
        assert live_to_backup in swap_body, f"missing live->backup mapping for {t}"
        assert new_to_live in swap_body, f"missing new->live mapping for {t}"
        assert swap_body.index(live_to_backup) < swap_body.index(new_to_live), (
            f"{t}: expected `{t} TO {t}_backup` before `{t}_new TO {t}`"
        )
    # And nothing else snuck into the rename mapping.
    assert "person_temp" not in swap_body


def test_swap_person_tables_drops_stale_backup_before_renaming():
    statements = parse_mysql_statements(_read_sql())
    swap_body = statements[3]
    rename_idx = swap_body.upper().index("RENAME TABLE")
    for t in u.SWAP_TABLES:
        drop_stmt = f"DROP TABLE IF EXISTS {t}_backup"
        assert drop_stmt in swap_body, f"missing drop of stale {t}_backup in swap_person_tables"
        assert swap_body.index(drop_stmt) < rename_idx, (
            f"{t}_backup must be dropped before the RENAME so a stale backup "
            "from the prior run can't collide with the incoming rename target"
        )


def test_swap_person_tables_does_not_drop_the_backup_tables_it_just_created():
    # The `_backup` tables the RENAME produces are the rollback window for
    # restore_person_tables_from_backup() -- this procedure must not drop
    # them after renaming, only before (for the *previous* run's backups).
    statements = parse_mysql_statements(_read_sql())
    swap_body = statements[3]
    rename_idx = swap_body.upper().index("RENAME TABLE")
    after_rename = swap_body[rename_idx:]
    assert "DROP TABLE" not in after_rename.upper(), (
        "swap_person_tables must not drop any `_backup` table after the RENAME"
    )


if __name__ == "__main__":
    test_shadow_table_name_routes_swap_tables_unconditionally()
    test_shadow_table_name_never_routes_person_temp()
    test_shadow_table_name_ignores_unknown_tables()
    test_swap_tables_excludes_person_temp_and_has_ten_entries()
    test_no_atomic_swap_symbol_remains_in_module()
    test_missing_procedure_raises_named_exception_type()
    test_sql_file_exists()
    test_sql_parses_into_exactly_four_statements_with_internal_semicolons_preserved()
    test_prepare_person_shadow_tables_drops_before_creating_each_table()
    test_swap_person_tables_renames_all_ten_tables_both_directions_as_one_statement()
    test_swap_person_tables_drops_stale_backup_before_renaming()
    test_swap_person_tables_does_not_drop_the_backup_tables_it_just_created()
    print("OK: shadow-table routing and setup/person_table_swap.sql structure verified (no DB).")
