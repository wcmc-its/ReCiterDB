#!/usr/bin/env python3
"""retrieveExternalArticles v3.5: book/CV fields, decoded titles, and the fallback byline.

Run: python3 update/test_external_article_book_fields.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from retrieveExternalArticles import byline_and_position, scopus_venue, to_row  # noqa: E402

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
edited = to_row({"uid": "aa1", "articleId": "CROSSREF:10.1/ed", "sourceType": "CROSSREF", "rawRecord": json.dumps(
    {"editor": [{"given": "Ada", "family": "Lovelace"}, {"given": "Charles", "family": "Babbage"}]})}, {"aa1": "Babbage"})
assert (edited["byline"], edited["author_position"]) == ("Lovelace A, ((Babbage C)), editors", "last"), edited["byline"]

# Scopus venue punctuation (Scopus's API now strips it): Scimago by ISSN when the names agree.
sci = {"1552-5260": "Alzheimer's and Dementia", "2213-8587": "Lancet Diabetes and Endocrinology,The",
       "0000-0001": "Some Other Journal"}
assert scopus_venue("Alzheimer S and Dementia", "1552-5260", sci) == "Alzheimer's and Dementia"
assert scopus_venue("Lancet Diabetes and Endocrinology", "2213-8587", sci) == "Lancet Diabetes and Endocrinology"
assert scopus_venue("Journal of Men S Health", "0000-0001", sci) == "Journal of Men's Health"  # ISSN disagrees
assert scopus_venue("Firestein Kelley S Textbook of Rheumatology 2 Volume Set") == \
    "Firestein Kelley's Textbook of Rheumatology 2 Volume Set"
assert scopus_venue("Vitamin D S") == "Vitamin D S" and scopus_venue(None) is None
row = to_row({"uid": "x", "articleId": "SCOPUS:1", "sourceType": "SCOPUS", "journalOrVenue": "Alzheimer S and Dementia"},
             {}, {}, {"SCOPUS:1": "1552-5260"}, sci)
assert row["journal_or_venue"] == "Alzheimer's and Dementia"
assert to_row({"uid": "x", "articleId": "OPENALEX:W1", "sourceType": "OPENALEX", "journalOrVenue": "Men S Health"}
              )["journal_or_venue"] == "Men S Health"  # only Scopus rows are repaired
print("PASS test_external_article_book_fields")
