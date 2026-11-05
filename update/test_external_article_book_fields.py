#!/usr/bin/env python3
"""retrieveExternalArticles v3.5: book/CV fields, decoded titles, and the fallback byline.

Run: python3 update/test_external_article_book_fields.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from retrieveExternalArticles import byline_and_position, to_row  # noqa: E402

raw = {"ISBN": ["9781421455150", "9781421455143"], "publisher": "Johns Hopkins University Press",
       "author": [{"given": "Gary Alan", "family": "Koretzky"}],
       "editor": [{"given": "Ada", "family": "Lovelace"}]}
book = to_row({"uid": "gak2008", "articleId": "CROSSREF:10.56021/9781421455143", "sourceType": "CROSSREF",
               "title": "Your Immune System", "authors": ["Gary Alan Koretzky"], "rawRecord": json.dumps(raw)},
              {"gak2008": "Koretzky"})
assert book["isbn"] == "9781421455150,9781421455143"
assert book["publisher"] == "Johns Hopkins University Press"
assert json.loads(book["editors"]) == ["Ada Lovelace"]
assert (book["byline"], book["author_position"]) == ("((Koretzky GA))", "first")

# Scopus row: empty rawRecord, flat name strings, ISBN from authorship_review, &amp; decoded.
scopus = to_row({"uid": "gak2008", "articleId": "SCOPUS:85217350239", "sourceType": "SCOPUS", "rawRecord": "{}",
                 "title": "Firestein &amp; Kelley&#8217;s Textbook <i>of</i> Rheumatology",
                 "authors": ["Gary S. Firestein", "Iain B. McInnes", "Gary A. Koretzky", "James R. O'Dell Jr."]},
                {"gak2008": "Koretzky"}, {"SCOPUS:85217350239": "9780323935401"})
assert scopus["title"] == "Firestein & Kelley’s Textbook of Rheumatology", scopus["title"]
assert scopus["isbn"] == "9780323935401" and scopus["publisher"] is None and scopus["editors"] is None
assert scopus["byline"] == "Firestein GS, McInnes IB, ((Koretzky GA)), O'Dell JR", scopus["byline"]
assert scopus["author_position"] is None  # middle author

assert byline_and_position(["A Smith", "B Jones"], "jones") == ("Smith A, ((Jones B))", "last")
assert byline_and_position(["A Smith"], None) == ("Smith A", None)  # person not found: unwrapped
assert byline_and_position([], "Smith") == (None, None)
assert byline_and_position(None, "Smith") == (None, None)
bare = to_row({"uid": "x", "articleId": "OPENALEX:W1", "sourceType": "OPENALEX"})  # old rows still map
assert bare["byline"] is None and bare["isbn"] is None and bare["title"] is None
many = [f"Given{i} Surname{i}" for i in range(300)] + ["Gary Koretzky"]
long_line, _ = byline_and_position(many, "Koretzky")
assert len(long_line) <= 1000 and long_line.endswith(", ..., ((Koretzky G)), et al."), long_line[-60:]
assert ", Surname" in long_line and not long_line.split(", ..., ")[0].endswith(("Surname", "S"))
short_cut, _ = byline_and_position(many[:200], None)
assert len(short_cut) <= 1000 and short_cut.endswith(", et al.") and "((" not in short_cut
print("PASS test_external_article_book_fields")
