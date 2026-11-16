"""watchlist_patterns.py: every false-positive guard carried over from the Section 1286
screen, a positive case per category, the usual-suspects and country lists, the funding
patterns, and parity with the original script's entity table when it is on this machine.

Run: python3 -m pytest update/test_watchlist_patterns.py -q
"""
import os
import re
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import watchlist_patterns as wp  # noqa: E402

ORIGINAL = os.path.expanduser("~/Dropbox/Projects/ReCiter Research/scripts/section1286_match.py")


def hits(aff, list_code=wp.HOUSE):
    return {entity for code, _, _, entity in wp.match_affiliation(aff) if code == list_code}


# ---- house-1286-nominations: shape ----------------------------------------------
def test_house_list_shape():
    house = [e for e in wp.ENTITIES if e[0] == wp.HOUSE]
    by_cat = {}
    for _, cat, n, name, *_ in house:
        by_cat.setdefault(cat, []).append(n)
    # 62 list numbers for laboratories, of which 4, 12, 15 and 55 are merged into 3, 11, 14, 28
    assert {c: len(v) for c, v in by_cat.items()} == {
        "CAS institute": 14, "Laboratory": 58, "SASTIND university": 58}
    assert sorted(by_cat["Laboratory"]) == [n for n in range(1, 63) if n not in (4, 12, 15, 55)]
    assert sorted(by_cat["CAS institute"]) == list(range(1, 15))
    assert sorted(by_cat["SASTIND university"]) == list(range(1, 59))
    assert len({e[3] for e in house}) == len(house)


# ---- house-1286-nominations: a positive case per category ------------------------
@pytest.mark.parametrize("aff, entity", [
    ("Dalian Institute of Chemical Physics, Chinese Academy of Sciences, Dalian 116023, China",
     "Dalian Institute of Chemical Physics"),
    ("Wuhan National Laboratory for Optoelectronics, Huazhong University of Science and Technology, Wuhan, China",
     "Wuhan National Laboratory for Optoelectronics (WNLO)"),
    ("Department of Urology, Peking University Third Hospital, Beijing, China", "Peking University"),
    ("Zhejiang University School of Medicine, Hangzhou, China", "Zhejiang University"),
])
def test_house_positive(aff, entity):
    assert entity in hits(aff)


def test_match_returns_category_and_list_number():
    got = [m for m in wp.match_affiliation("Xidian University, Xi'an, China") if m[0] == wp.HOUSE]
    assert got == [(wp.HOUSE, "SASTIND university", 56, "Xidian University")]


def test_normalization_accents_apostrophes_case_whitespace():
    assert "Xi'an Jiaotong University" in hits("Xi’an  Jiaotong\nUniversity, Xi’an, China")
    assert "Xi'an Jiaotong University" in hits("XIAN JIAOTONG UNIVERSITY")
    assert "Sun Yat-sen University" in hits("Sun Yat‐sen University, Guangzhou")
    assert wp.norm(None) == ""


# ---- house-1286-nominations: false-positive guards ---------------------------------
def test_guard_taiwan_tsing_hua():
    assert "Tsinghua University" in hits("School of Medicine, Tsinghua University, Beijing, China")
    assert hits("National Tsing Hua University, Hsinchu, Taiwan") == set()
    assert hits("National Tsinghua University, Hsinchu, Taiwan") == set()
    assert hits("Tsinghua University, Beijing; National Tsing Hua University, Hsinchu") == set()


def test_guard_taiwan_sun_yat_sen():
    assert "Sun Yat-sen University" in hits("Sun Yat-sen University Cancer Center, Guangzhou, China")
    assert hits("National Sun Yat-sen University, Kaohsiung, Taiwan") == set()


def test_guard_soochow_taipei():
    assert "Soochow University" in hits("Soochow University, Suzhou, Jiangsu, China")
    for place in ("Taipei", "Taiwan", "Kaohsiung", "Hsinchu"):
        assert hits(f"Department of Microbiology, Soochow University, {place}") == set()


def test_guard_southeast_university_requires_nanjing():
    assert "Southeast University" in hits("School of Public Health, Southeast University, Nanjing, China")
    assert hits("Southeast University, Dhaka, Bangladesh") == set()
    assert hits("Southeast University, China") == set()


def test_guard_xian_jiaotong_liverpool():
    assert "Xi'an Jiaotong University" in hits("Xi'an Jiaotong University Health Science Center, Xi'an, China")
    assert hits("Xi'an Jiaotong-Liverpool University, Suzhou, China") == set()
    assert hits("Xian Jiaotong - Liverpool University") == set()


@pytest.mark.parametrize("aff", [
    "Zhejiang University of Technology, Hangzhou, China",
    "Shandong University of Traditional Chinese Medicine, Jinan, China",
    "Hebei University of Technology, Tianjin, China",
    "Anhui University of Chinese Medicine, Hefei, China",
    "Tianjin University of Traditional Chinese Medicine",
    "Wuhan University, of Science and Technology",
    "Sichuan University for Nationalities",
    "Zhejiang University City College, Hangzhou",
    "Shanghai University School of Medicine",
    "Shanghai University of Traditional Chinese Medicine",
    "North Hebei University, Zhangjiakou",
])
def test_guard_sister_schools(aff):
    assert hits(aff) == set()


def test_guard_sister_school_with_its_own_entry():
    assert hits("Hebei University of Science and Technology, Shijiazhuang") == {
        "Hebei University of Science and Technology"}
    assert hits("Wuhan University of Technology") == {"Wuhan University of Technology"}
    assert hits("Hebei University, Baoding") == {"Hebei University"}
    assert hits("Shanghai Jiao Tong University School of Medicine") == {"Shanghai Jiao Tong University"}


@pytest.mark.parametrize("institute", [
    "Institute of Automation", "Institute of High Energy Physics", "Institute of Mechanics",
    "Institute of Metal Research", "Institute of Semiconductors",
    "Technical Institute of Physics and Chemistry",
])
def test_guard_generic_institutes_require_cas(institute):
    assert hits(f"{institute}, Chinese Academy of Sciences, Beijing") == {institute}
    assert hits(f"{institute}, CAS, Beijing 100190") == {institute}
    assert hits(f"{institute}, University of Bremen, Germany") == set()
    assert hits(f"{institute}, Beijing, China") == set()


def test_cas_institutes_with_a_place_name_need_no_cas():
    assert hits("Shanghai Institute of Ceramics, Shanghai") == {"Shanghai Institute of Ceramics"}


@pytest.mark.parametrize("aff, entity", [
    ("State Key Laboratory of Powder Metallurgy, Central South University, Changsha",
     "State Key Laboratory of Powder Metallurgy"),
    ("Laboratory of Transient Physics, Nanjing", "National Key Laboratory of Transient Physics"),
    ("State Key Laboratory of Solidification Processing, Xi'an", "State Key Laboratory of Solidification Processing"),
    ("Purple Mountain Laboratories, Nanjing", "Purple Mountain Laboratory (PML)"),
    ("Key Laboratory for Space Biosciences and Biotechnology, Xi'an",
     "Key Laboratory for Space Biosciences and Biotechnology"),
])
def test_guard_labs_require_china(aff, entity):
    assert hits(aff) == set()
    assert entity in hits(aff + ", China")
    assert entity in hits(aff + ", P.R.C")
    assert entity in hits(aff + ", PR China")


def test_house_entities_do_not_leak_into_other_lists():
    assert hits("Harbin University of Science and Technology, Harbin", wp.USUAL) == set()
    assert hits("Harbin Institute of Technology, Harbin") == set()


# ---- usual-suspects ------------------------------------------------------------------
@pytest.mark.parametrize("aff, entity", [
    ("School of Biological Science and Medical Engineering, Beihang University, Beijing", "Beihang University"),
    ("Beijing University of Aeronautics and Astronautics, Beijing 100191", "Beihang University"),
    ("School of Life Science, Beijing Institute of Technology, Beijing", "Beijing Institute of Technology"),
    ("Harbin Engineering University, Harbin", "Harbin Engineering University"),
    ("Harbin Institute of Technology (Shenzhen), Shenzhen", "Harbin Institute of Technology"),
    ("Nanjing University of Aeronautics & Astronautics", "Nanjing University of Aeronautics and Astronautics"),
    ("Nanjing University of Science and Technology, Nanjing", "Nanjing University of Science and Technology"),
    ("Northwestern Polytechnical University, Xi'an 710072", "Northwestern Polytechnical University"),
    ("School of Life Sciences, Northwestern Polytechnic University, Xi'an, Shaanxi, China",
     "Northwestern Polytechnical University"),
    ("National University of Defense Technology, Changsha", "National University of Defense Technology"),
    ("National University of Defence Technology, Changsha", "National University of Defense Technology"),
    ("Institute of Fluid Physics, China Academy of Engineering Physics, Mianyang",
     "China Academy of Engineering Physics"),
    ("Beijing Institute of Radiation Medicine, Academy of Military Medical Sciences, Beijing",
     "Academy of Military Medical Sciences"),
    ("Fourth Military Medical University, Xi'an, China", "PLA medical universities"),
    ("Xinqiao Hospital, Army Medical University, Chongqing, China", "PLA medical universities"),
    ("Naval Medical University, Shanghai, P.R. China", "PLA medical universities"),
    ("Medical School of Chinese PLA, Beijing, China", "PLA medical universities"),
    ("Department of Cardiology, Chinese PLA General Hospital, Beijing, China", "PLA hospitals"),
    ("General Hospital of the People's Liberation Army, Beijing, China", "PLA hospitals"),
    ("The 960th Hospital of the PLA Joint Logistics Support Force, Jinan, China", "PLA hospitals"),
    # names with no namesake outside China need no country in the string (OpenAlex
    # institution names and many PubMed affiliations carry none)
    ("Chinese PLA General Hospital, Beijing 100853", "PLA hospitals"),
    ("General Hospital of Chinese People's Liberation Army, Beijing", "PLA hospitals"),
    ("Chinese PLA General Hospital", "PLA hospitals"),
    ("Fourth Military Medical University, Xi'an 710032", "PLA medical universities"),
    ("Second Military Medical University, Shanghai", "PLA medical universities"),
    ("Fourth Military Medical University", "PLA medical universities"),
    ("Medical School of Chinese PLA, Beijing", "PLA medical universities"),
    ("Third Military Medical University (Army Medical University), Chongqing", "PLA medical universities"),
    # the generic names count when the clause says Chinese or names a Chinese city
    ("Xijing Hospital, Air Force Medical University, Xi'an", "PLA medical universities"),
    ("Changhai Hospital, Naval Medical University, Shanghai 200433", "PLA medical universities"),
    ("PLA Rocket Force Characteristic Medical Center, Beijing", "PLA hospitals"),
    # Seven Sons short forms
    ("Beihang, Beijing", "Beihang University"),
    ("Beihang Univ., Beijing", "Beihang University"),
    ("BUAA, Beijing, China", "Beihang University"),
    ("Northwestern Polytechnical Univ, Xi'an", "Northwestern Polytechnical University"),
    ("Northwest Polytechnical University, Xi'an, China", "Northwestern Polytechnical University"),
])
def test_usual_suspects_positive(aff, entity):
    assert entity in hits(aff, wp.USUAL)


@pytest.mark.parametrize("aff", [
    "Northwestern Polytechnic University, Fremont, CA, USA",                 # unrelated US namesake
    "Northwestern Polytechnic University, Fremont, California; visiting, Xi'an, China",
    "Northwestern Polytechnic University",                                   # no place: not assumed
    "Northwestern University Feinberg School of Medicine, Chicago",
    "Nanjing University of Information Science and Technology, Nanjing, China",
    "Nanjing University, Nanjing, China",
    "Chinese Academy of Engineering, Beijing, China",
    "Beijing Institute of Genomics, Chinese Academy of Sciences",
    "Harbin Medical University, Harbin, China",
    "Southern Medical University, Guangzhou, China",                         # civilian since 2004
    "Army Medical University",                                               # no China in the string
    "Army Medical College, Rawalpindi, Pakistan",
    "Military Medical Academy, Saint Petersburg, Russia",
    "Hospital Universitari Germans Trias i Pujol, Pla de l'Estany, Spain",   # Catalan "Pla"
    "Hospital Clinic, Barcelona; Pla Director, China programme",             # "Pla" is not the acronym
    "PLA General Hospital",                                                  # no China in the string
    "Department of Polymer Science (PLA scaffolds), Hospital for Special Surgery, New York",
    "National Defense Medical Center, Taipei, Taiwan",
    # the China guard is bounded: Indochina, Chinatown and UPRC are not China
    "Royal Army Medical University, Indochina",
    "Army Medical University, Chinatown, NY",
    "Hospital, UPRC campus, PLA",
    # the guard must hold in the SAME ';' clause as the name
    "Dept of Polymers (PLA scaffolds), Hospital for Special Surgery, New York; Peking University, China",
    "Dept of Polymers (PLA scaffolds), Hospital for Special Surgery, Shanghai, China",   # acronym and hospital in different comma parts
    "Northwestern Polytechnic University, CA 94539, USA; Fudan University, China",
    "Northwestern Polytechnic University, CA 94539, USA, visiting Xi'an",
    "Army Medical University; Fudan University, Shanghai, China",
    "BUAA",                                                                  # the acronym alone: not assumed
    "Northwestern Polytechnical Institute of Nowhere",
])
def test_usual_suspects_namesakes(aff):
    assert hits(aff, wp.USUAL) == set()


def test_usual_suspects_have_no_list_number_and_one_hit_per_entity():
    got = [m for m in wp.match_affiliation("Northwestern Polytechnical University; Northwestern "
                                           "Polytechnic University, Xi'an, China") if m[0] == wp.USUAL]
    assert got == [(wp.USUAL, "Seven Sons of National Defence", None, "Northwestern Polytechnical University")]
    assert {e[3] for e in wp.ENTITIES if e[0] == wp.USUAL} == {
        "Beihang University", "Beijing Institute of Technology", "Harbin Engineering University",
        "Harbin Institute of Technology", "Nanjing University of Aeronautics and Astronautics",
        "Nanjing University of Science and Technology", "Northwestern Polytechnical University",
        "National University of Defense Technology", "China Academy of Engineering Physics",
        "Academy of Military Medical Sciences", "PLA medical universities", "PLA hospitals"}


# ---- country-of-concern ----------------------------------------------------------------
@pytest.mark.parametrize("aff, countries", [
    ("Fudan University, Shanghai, China", {"China"}),
    ("Fudan University, Shanghai, P.R. China", {"China"}),
    ("Shanghai, People's Republic of China", {"China"}),
    ("Institute of Zoology, Chinese Academy of Sciences, Beijing", {"China"}),
    ("The University of Hong Kong, Hong Kong", {"China"}),
    ("University of Macau, Macao SAR", {"China"}),
    ("Academia Sinica, Taipei, Taiwan, Republic of China", set()),          # Taiwan guard
    ("Academia Sinica, Taipei, Taiwan; The Chinese University of Hong Kong", {"China"}),
    ("Chinatown Health Clinic, New York, NY, USA", set()),
    ("Lomonosov Moscow State University, Moscow, Russia", {"Russia"}),
    ("Sechenov University, Moscow, Russian Federation", {"Russia"}),
    ("Tehran University of Medical Sciences, Tehran, Iran", {"Iran"}),
    ("Editorial office, Iranian Journal of Public Health, Iran", set()),
    ("Kim Il Sung University, Pyongyang", {"North Korea"}),
    ("Pyongyang University of Science and Technology, DPR Korea", {"North Korea"}),
    ("Seoul National University, Seoul, South Korea", set()),
    ("Instituto Pedro Kouri, Havana, Cuba", {"Cuba"}),
    ("Universidad Central de Venezuela, Caracas", {"Venezuela"}),
    ("Weill Cornell Medicine, New York, NY 10065, USA", set()),
    ("Joint lab: Moscow, Russia and Beijing, China", {"China", "Russia"}),
    # US towns and street names that are a country word
    ("Cuba, NM 87013", set()), ("Havana, Illinois", set()), ("Havana Street, Aurora, Colorado", set()),
    ("Russia, Ohio", set()), ("China Grove, NC, USA", set()), ("Hong Kong Road Clinic, Singapore", set()),
    # ... but a state that starts the next institution's name does not hide the country
    ("Fudan University, Shanghai, China, MD Anderson Cancer Center, Houston", {"China"}),
    ("Peking University, Beijing, China, Washington University, St Louis", {"China"}),
    # Taiwan by city, and as "Republic of China"
    ("China Medical University Hospital, Taichung 40447", set()),
    ("Taipei Veterans General Hospital, Taipei, Republic of China", set()),
    ("Academia Sinica, Republic of China", set()),
    ("Fudan University, Shanghai, People's Republic of China", {"China"}),
    # a guard word in one clause does not hide a hit in another
    ("Columbia University, New York; Instituto Finlay, Havana, Cuba", {"Cuba"}),
    ("National Taiwan University, Taipei, Taiwan; Fudan University, Shanghai, China", {"China"}),
    ("Tehran, Islamic Republic of Iran; Iranian Journal of X editorial board", {"Iran"}),
    ("Columbia University, New York, Havana", set()),
])
def test_country_of_concern(aff, countries):
    assert hits(aff, wp.COUNTRY) == countries


@pytest.mark.parametrize("aff, sensitive", [
    # DynamoDB joins an author's affiliations with ", ": a whole-string guard then spans institutions
    ("Soochow University, Suzhou, China, Kaohsiung Medical University, Kaohsiung, Taiwan", True),
    ("Tsinghua University, Beijing, China, National Tsing Hua University, Hsinchu, Taiwan", True),
    ("Peking University, Beijing, China, Academia Sinica, Taipei, Taiwan", True),
    ("Southeast University, Dhaka, Bangladesh, Nanjing Medical University, Nanjing, China", True),
    ("State Key Laboratory of Powder Metallurgy, Changsha", True),            # a required-term entity matched
    ("Columbia University, New York, NY, Instituto Finlay, Havana, Cuba", True),
    ("Peking University, Beijing, China", False),                             # no whole-string guard involved
    ("Fudan University, Shanghai, China", False),
    ("Weill Cornell Medicine, New York, NY, USA", False),
    ("", False), (None, False),
])
def test_guard_sensitive(aff, sensitive):
    assert wp.guard_sensitive(aff) is sensitive


def test_joined_string_is_wrong_and_the_separate_strings_are_right():
    """Why collectAffiliations.py refetches a guard-sensitive DynamoDB article from efetch."""
    def both(affs):
        return {(c, e) for a in affs for c, _, _, e in wp.match_affiliation(a) if c != wp.USUAL}

    a, b = "Soochow University, Suzhou, China", "Kaohsiung Medical University, Kaohsiung, Taiwan"
    assert both([a + ", " + b]) == set()
    assert both([a, b]) == {(wp.HOUSE, "Soochow University"), (wp.COUNTRY, "China")}
    a, b = "Southeast University, Dhaka, Bangladesh", "Nanjing Medical University, Nanjing, China"
    assert (wp.HOUSE, "Southeast University") in both([a + ", " + b])          # false hit when joined
    assert both([a, b]) == {(wp.COUNTRY, "China")}


def test_country_rows_are_their_own_list():
    got = wp.match_affiliation("Peking University, Beijing, China")
    assert (wp.HOUSE, "SASTIND university", 29, "Peking University") in got
    assert (wp.COUNTRY, "Country", None, "China") in got
    assert wp.match_affiliation("Weill Cornell Medicine, New York, USA") == []
    assert wp.match_affiliation("") == [] and wp.match_affiliation(None) == []


# ---- funding-flags -----------------------------------------------------------------------
@pytest.mark.parametrize("agency, grant_id, programs", [
    ("China Scholarship Council", "201906230123", ["China Scholarship Council (CSC)"]),
    ("CSC, China", "", ["China Scholarship Council (CSC)"]),
    ("Scholarship fund", "China Scholarship Council No. 2019", ["China Scholarship Council (CSC)"]),
    ("Computer Sciences Corporation (CSC)", "", []),
    ("National Natural Science Foundation of China", "81700000", []),
    ("NSFC Excellent Young Scientists Fund (Overseas)", "", ["Overseas Excellent Young Scientists Fund"]),
    ("Shanghai Pujiang Program", "21PJ1401000", ["Shanghai Magnolia / Pujiang (Category D)"]),
    ("NHLBI NIH HHS", "Pujiang", []),                       # the grant id counts for CSC only
    ("Shenzhen Peacock Plan", "KQTD2016", ["Peacock Team Project"]),
    ("Wuhan East Lake 3551 Optics Valley Talent Program", "",
     ["Optics Valley 3551 / Global Talent Hunting"]),
    ("Postdoctoral Innovative Talent Support Program", "BX2021", ["Postdoctoral Innovation Talent Support Program"]),
    ("NIH HHS", "R01 CA123456", []),
    (None, None, []),
])
def test_funding_patterns(agency, grant_id, programs):
    assert wp.match_funder(agency, grant_id) == programs


def test_funding_program_count():
    assert len(wp.FUND) == 20 and len({n for n, _ in wp.FUND}) == 20


def test_name_key():
    assert wp.name_key("Yi-Ming", "Wang") == wp.name_key("Yi Ming", "Wang") == ("wang", "yiming")
    assert wp.name_key("José", "Muñoz") == ("munoz", "jose")
    assert wp.name_key("Y", "Wang") != wp.name_key("Yi-Ming", "Wang")


# ---- parity with the original script -------------------------------------------------------
def _original():
    src = open(ORIGINAL).read().split("# --- load")[0].replace("import pandas as pd", "")
    ns, argv = {"__name__": "section1286_match"}, sys.argv
    sys.argv = ["x", "unused-workdir", "unused.xlsx"]
    try:
        exec(compile(src, ORIGINAL, "exec"), ns)
    finally:
        sys.argv = argv
    return ns


def _synthetic(ns):
    """A few thousand strings built from every entity name: plain, with the place and
    qualifier words the guards look at, and with the punctuation norm() folds."""
    names = {re.sub(r"\s*\(.*?\)", "", name).strip() for _, _, name, *_ in ns["E"]}
    for _, _, name, *_ in ns["E"]:
        names.add(name)
        names.update(p.strip() for p in re.split(r"\s+/\s+", name))
    templates = [
        "{n}", "{n}, China", "Department of Chemistry, {n}, Beijing 100084, P.R. China", "{n}, PRC",
        "{n}, Nanjing, China", "{n}, Wuhan, China", "{n}, Taipei, Taiwan", "{n}, Hsinchu", "{n}, Kaohsiung",
        "National {n}", "North {n}", "{n} of Technology", "{n} of Science and Technology, China",
        "{n}, of Chinese Medicine", "{n} for Nationalities", "{n} School of Medicine", "{n} City College",
        "{n}-Liverpool University", "{n} - Liverpool University, Suzhou", "{n}, Chinese Academy of Sciences",
        "{n}, CAS, Beijing", "Key Laboratory of {n}, China", "State Key Lab for {n}, P. R. C",
        "Key Laboratory of Science and Technology on {n}, Xi'an, China", "{n} Laboratory, China",
        "{n} Lab, Germany", "{n}; National Tsing Hua University", "HUST, {n}, Wuhan",
    ]
    out = []
    for n in sorted(names):
        for t in templates:
            s = t.format(n=n)
            out.extend((s, s.upper(), s.lower(), s.replace("'", "’").replace("-", "‐").replace(" ", "  ")))
    return out


@pytest.mark.skipif(not os.path.exists(ORIGINAL), reason="the original section1286_match.py is not on this machine")
def test_parity_with_original_script():
    ns = _original()

    def pat(rx):
        return (rx.pattern, rx.flags) if rx else None

    ours = [(cat, n, name, pat(rx), pat(req), pat(excl))
            for code, cat, n, name, rx, req, excl in wp.ENTITIES if code == wp.HOUSE]
    theirs = [(cat, n, name, pat(rx), pat(req), pat(excl)) for cat, n, name, rx, req, excl in ns["E"]]
    assert ours == theirs
    assert [(n, pat(rx)) for n, rx in wp.FUND] == [(n, pat(rx)) for n, rx in ns["FUND"]]

    strings = _synthetic(ns)
    assert len(strings) > 300
    matched = 0
    for s in strings:
        want = list(ns["match_aff"](s))
        got = [(cat, n, name) for code, cat, n, name in wp.match_affiliation(s) if code == wp.HOUSE]
        assert got == want, s
        matched += bool(want)
    assert matched > 300 and matched < len(strings)       # both hits and guarded misses are exercised

    for first, last in (("Yi-Ming", "Wang"), ("José", "O’Neil"), ("", "Consortium"), (None, None)):
        assert wp.name_key(first, last) == ns["name_key"](first, last)
    for s in ("Xi’an  Jiaotong‐Liverpool", "Zürich – Genève", None, 5):
        assert wp.norm(s) == ns["norm"](s)
