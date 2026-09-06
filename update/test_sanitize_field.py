#!/usr/bin/env python3
"""Regression test: sanitize_field must blank every literal-null spelling, and
must not eat legitimate volume/issue/pages values.

ReCiterDB #197. The upstream JSON stringifies absent values inconsistently:
uppercase 'NULL' on some paths, lowercase 'null' on others (Java/Jackson). The
original predicate compared `value == 'NULL'` -- exact case -- so lowercase
'null' slipped through into person_article.volume, where a downstream
`if (volume)` guard sees a truthy string and renders it (SPS#2580).

The opposite failure is worse: a looser rule ("starts with n", "contains null",
sweeping 'N/A' / 'none' / '-') destroys real corpus data. The values in
KEEP below are genuine volume/issue/pages strings and must survive verbatim.

Run: python3 test_sanitize_field.py
"""
import csv
import io

from dataTransformer import (NULL_MARKER, numeric_or_null, sanitize_field,
                             write_csv_rows)

BLANK = [None, 'NULL', 'null', 'Null', 'nULL', '  NULL  ', ' null', '', '   ', '\t']
KEEP = ['Suppl', 'Spec No', 'Suppl Web Exclusives', 'IX', 'PP', 'IV', 'XXIX',
        'DECIPHeR', 'N/A', 'none', 'None', '-', '12', '188-197', '5']


def main():
    for v in BLANK:
        assert sanitize_field(v) == '', f"expected blank for {v!r}, got {sanitize_field(v)!r}"
    for v in KEEP:
        assert sanitize_field(v) == v.strip(), f"{v!r} was altered to {sanitize_field(v)!r}"

    # non-strings still pass through stringified, and are never treated as null
    assert sanitize_field(0) == '0'
    assert sanitize_field(12) == '12'
    # embedded newlines/CRs still stripped (CSV safety, pre-existing behaviour)
    assert sanitize_field('a\r\nb') == 'ab'

    # ---- numeric_or_null: absent must not become a confident 0 -------------------
    # person_article had 0 NULLs in 858,946 rows because an empty CSV field loads as 0
    # in a numeric column, so "never scored" and "scored zero" were the same value.
    for v in BLANK:
        assert numeric_or_null(v) == NULL_MARKER, \
            f"expected the NULL marker for {v!r}, got {numeric_or_null(v)!r}"
    # a genuine zero is a real measurement and must survive as 0
    assert numeric_or_null(0) == '0'
    assert numeric_or_null(0.0) == '0.0'
    assert numeric_or_null('0.0') == '0.0'
    assert numeric_or_null(97.35) == '97.35'

    # ---- the marker must reach the file UNQUOTED, or LOAD DATA reads it as a string --
    # csv.QUOTE_MINIMAL only quotes fields containing the delimiter, quotechar or a
    # newline. If a future change quotes everything, "\\N" becomes the two-character
    # string and every NULL silently turns back into 0.
    import tempfile, os
    fd, path = tempfile.mkstemp(suffix='.csv')
    os.close(fd)
    try:
        write_csv_rows(path, [[numeric_or_null(None), numeric_or_null(0.0), 'text']])
        with open(path) as f:
            line = f.read().strip()
    finally:
        os.unlink(path)
    assert line == '\\N,0.0,text', f"CSV line must carry an unquoted \\N, got {line!r}"
    assert '"' not in line, f"the NULL marker must not be quoted: {line!r}"

    print(f"OK: {len(BLANK)} null spellings blanked, {len(KEEP)} real values preserved, "
          f"numeric_or_null emits an unquoted {NULL_MARKER} for absent and keeps a real 0")


if __name__ == '__main__':
    main()
