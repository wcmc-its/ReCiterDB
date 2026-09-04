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
from dataTransformer import sanitize_field

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

    print(f"OK: {len(BLANK)} null spellings blanked, {len(KEEP)} real values preserved")


if __name__ == '__main__':
    main()
