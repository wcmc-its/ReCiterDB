"""watchlist_patterns.py: research-security watch lists as regexes. Pure: no I/O, no DB.

Used by collectAffiliations.py to screen every co-author affiliation string and every
grant on an article. A hit is a screening result, not a determination.

Lists (list_code):

  house-1286-nominations  The 134 entities nominated in the House Select Committee's
                          2026-10-01 letter (14 CAS institutes, 62 laboratories with the
                          duplicate names 3/4, 11/12, 14/15, 28/55 merged, 58 SASTIND
                          universities). Ported unchanged, with every false-positive
                          guard, from ReCiter Research/scripts/section1286_match.py;
                          test_watchlist_patterns.py holds the two tables equal.
  usual-suspects          A short curated list (seed only; see the marked block below).
  country-of-concern      Any affiliation in China (incl. Hong Kong and Macau), Russia,
                          Iran, North Korea, Cuba or Venezuela. entity = the country.
                          Started as a port of Scholars-Profile-System
                          scripts/bulk-data-rule/coauthor_affil.py; the guards now run per
                          ';'-separated clause and skip US place names, so the two differ
                          on purpose. A far wider net than the entity lists: keep it out
                          of entity totals.
  funding-flags           China Scholarship Council and named talent programs in grant
                          metadata (match_funder). Paper-level: says nothing about which
                          author held the award.

  match_affiliation(aff) -> [(list_code, category, list_no, entity), ...]
  guard_sensitive(aff) -> True when a whole-string guard took part in the answer, so a
                          string holding several affiliations cannot be trusted
  match_funder(agency, grant_id) -> [program, ...]

Adding a list or fixing a pattern needs no refetch: collectAffiliations.py rebuilds
watchlist_match and watchlist_funding_flag from the stored strings every night.
"""
import re
import unicodedata

HOUSE = 'house-1286-nominations'
USUAL = 'usual-suspects'
COUNTRY = 'country-of-concern'
FUNDING = 'funding-flags'

CAS = r'(Chinese Academy of Sciences|\bCAS\b)'
NOT_OF = r'(?![\s,]+(of|for)\b)'  # "Hebei University" must not swallow "Hebei University of Technology"

# (list_code, category, list number or None, entity, regex, extra-required regex or None,
#  exclusion regex or None). House entries test all three against the whole normalized
#  string, as the original script does. Usual-suspects entries test them against one
#  ';'-separated clause at a time: all three must hold in the same clause.
ENTITIES = []


def _add(list_code, cat, n, name, rx, req=None, excl=None):
    ENTITIES.append((list_code, cat, n, name, re.compile(rx, re.I),
                     re.compile(req, re.I) if req else None,
                     re.compile(excl, re.I) if excl else None))


# ---- house-1286-nominations ---------------------------------------------------
def add(cat, n, name, rx, req=None, excl=None):
    _add(HOUSE, cat, n, name, rx, req, excl)


C = 'CAS institute'
add(C, 1, 'Changchun Institute of Applied Chemistry', r'Changchun Institute of Applied Chemistry')
add(C, 2, 'Changchun Institute of Optics, Fine Mechanics and Physics', r'Changchun Institute of Optics')
add(C, 3, 'Dalian Institute of Chemical Physics', r'Dalian Institute of Chemical Physics')
add(C, 4, 'Hefei Institutes of Physical Science', r'Hefei Institutes? (of|for) Physical Sciences?')
add(C, 5, 'Institute of Automation', r'Institute of Automation', CAS)
add(C, 6, 'Institute of High Energy Physics', r'Institute of High Energy Physics', CAS)
add(C, 7, 'Institute of Mechanics', r'\bInstitute of Mechanics', CAS)
add(C, 8, 'Institute of Metal Research', r'Institute of Metal Research', CAS)
add(C, 9, 'Institute of Semiconductors', r'Institute of Semiconductors', CAS)
add(C, 10, 'Lanzhou Institute of Chemical Physics', r'Lanzhou Institute of Chemical Physics')
add(C, 11, 'Ningbo Institute of Materials Technology and Engineering', r'Ningbo Institute of Materials? Technology')
add(C, 12, 'Shanghai Institute of Ceramics', r'Shanghai Institute of Ceramics')
add(C, 13, 'Shenzhen Institutes of Advanced Technology', r'Shenzhen Institutes? of Advanced Technology')
add(C, 14, 'Technical Institute of Physics and Chemistry', r'Technical Institute of Physics and Chemistry', CAS)

L, CN = 'Laboratory', r'China|P\.?\s?R\.?\s?C\b'
KL = r'Key Lab\w*\s+(of|for|on)\s+(Science and Technology on\s+)?'
labs = [
    (1, 'Wuhan National Laboratory for Optoelectronics (WNLO)', r'Wuhan National (Laboratory|Research Center) for Optoelectronics|\bWNLO\b'),
    (2, 'Purple Mountain Laboratory (PML)', r'Purple Mountain Lab'),
    (3, 'Sci. & Tech. on Advanced High Temperature Structural Materials Lab (#3/#4)', r'Advanced High[\s-]Temperature Structural Materials'),
    (5, 'National Key Laboratory of Advanced Composite Materials', KL + r'Advanced Composites? Materials|Advanced Composites? Materials (Key )?Lab'),
    (6, 'National Key Laboratory of Fire Control Technology', r'Fire Control Technology'),
    (7, 'National Key Laboratory for High Energy Density Beam Processing Technology', r'High Energy Density Beam Processing'),
    (8, 'Key Laboratory of Inertial Technology', KL + r'Inertial Technology'),
    (9, 'National Key Laboratory of Sci. & Tech. on Aero-Engine Aero-Thermodynamics', r'Aero-?\s?Engine Aero-?\s?Thermodynamics'),
    (10, 'Science and Technology on Aircraft Control Laboratory', r'Aircraft Control Lab|' + KL + r'Aircraft Control'),
    (11, 'Reliability and Environmental Engineering Sci. & Tech. Lab (#11/#12)', r'Reliability and Environmental Engineering'),
    (13, 'Science and Technology on Metrology and Calibration Laboratory', r'Metrology and Calibration (Technology )?Lab|' + KL + r'Metrology and Calibration'),
    (14, 'State Key Laboratory of Explosion Science and Technology / Safety Protection (#14/#15)', r'Explosion Science'),
    (16, 'National Key Lab of Sci. & Tech. on Materials under Shock and Impact', r'Materials under Shock and Impact'),
    (17, 'Science and Technology on Electromechanical Dynamic Control Laboratory', r'Electromechanical Dynamic Control'),
    (18, 'National Key Laboratory of Sci. & Tech. on Space Microwave', KL + r'Space Microwave|Space Microwave (Technology )?Lab'),
    (19, 'National Key Laboratory of Sci. & Tech. on Aerospace Intelligence Control', r'Aerospace Intelligen\w* Control'),
    (20, 'Science and Technology on Space System Simulation Laboratory', r'Space System Simulation'),
    (21, 'State Key Laboratory of Powder Metallurgy', KL + r'Powder Metallurgy'),
    (22, 'Science and Technology on Electronic Information Control Laboratory', r'Electronic Information Control'),
    (23, 'Science and Technology on Communication Information Security Control Laboratory', r'Communication Information Security Control'),
    (24, 'National Key Laboratory of Electromagnetic Environment', KL + r'Electromagnetic Environment'),
    (25, 'Key Laboratory of Electronic Measurement Technology', KL + r'Electronic Measurement Technology'),
    (26, 'National Key Laboratory of Application Specific Integrated Circuit', KL + r'(Application Specific Integrated Circuits?|ASIC)\b'),
    (27, 'National Key Laboratory of Computational Physics', KL + r'Computational Physics'),
    (28, 'Sci. & Tech. on Plasma Physics / Plasma Dynamics Laboratory (#28/#55)', r'Science and Technology on Plasma (Physics|Dynamics)|' + KL + r'(Aero\w*\s+)?Plasma Dynamics'),
    (29, 'Science and Technology on High Power Microwave Laboratory', KL + r'High Power Microwave|High Power Microwave (Technology )?Lab'),
    (30, 'Science and Technology on Surface Physics and Chemistry Laboratory', r'Surface Physics and Chemistry (Lab|Key)|' + KL + r'Surface Physics and Chemistry'),
    (31, 'Science and Technology on Sonar Laboratory', r'on Sonar Lab|Sonar (Technology )?Lab|' + KL + r'Sonar'),
    (32, 'State Key Laboratory for Marine Corrosion and Protection', r'Marine Corrosion and Protection'),
    (33, 'Science and Technology on Electromagnetic Compatibility Laboratory', KL + r'Electromagnetic Compatibility|Electromagnetic Compatibility Lab'),
    (34, 'National Key Lab of Sci. & Tech. on Underwater Acoustic Antagonizing', r'Underwater Acoustic (Antagonizing|Technology)'),
    (35, 'Fundamental Science on Nuclear Safety and Simulation Technology Laboratory', r'Nuclear Safety and Simulation'),
    (36, 'Science and Technology on Autonomous Underwater Vehicle Laboratory', KL + r'(Autonomous )?Underwater Vehicles?|Autonomous Underwater Vehicle Lab'),
    (37, 'National Key Lab of Sci. & Tech. on Advanced Composites in Special Environments', r'Composites? in Special Environments?'),
    (38, 'National Key Laboratory of Sci. & Tech. on Tunable Laser', KL + r'Tunable Laser|Tunable Laser (Technology )?Lab'),
    (39, 'National Key Laboratory for Precision Hot Processing of Metals', r'Precision Hot Processing'),
    (40, 'National Key Laboratory of Sci. & Tech. on Rotorcraft Aeromechanics', r'Rotorcraft Aeromechanics'),
    (41, 'National Key Laboratory of Sci. & Tech. on Helicopter Transmission', r'Helicopter Transmission'),
    (42, 'National Key Laboratory of Transient Physics', r'Transient Physics'),
    (43, 'Sci. & Tech. on Advanced Ceramic Fibres and Composites Laboratory', r'Ceramic Fib\w+ and Composites'),
    (44, 'Sci. & Tech. on Parallel and Distributed Processing Laboratory', r'Parallel and Distributed Processing'),
    (45, 'National Key Laboratory of Automatic Target Recognition', r'Automatic Target Recognition'),
    (46, 'National Key Laboratory of Unmanned Aerial Vehicle Technology', KL + r'(Unmanned Aerial Vehicle|UAV) (Special )?Technolog'),
    (47, 'Key Laboratory for Space Biosciences and Biotechnology', r'Space Bioscience\w* (and|&) Biotechnolog'),
    (48, 'State Key Laboratory of Solidification Processing', r'Solidification Processing'),
    (49, 'National Key Lab of Sci. & Tech. on Aerodynamic Design and Research', r'Aerodynamic Design and Research'),
    (50, 'National Key Laboratory of Aerospace Flight Dynamics', r'Aerospace Flight Dynamics'),
    (51, 'National Key Laboratory of Combustion, Flow and Thermo-Structure', r'Combustion,? Flow,? and Thermo-?\s?Structure'),
    (52, 'National Key Laboratory for Underwater Information Processing and Control', r'Underwater Information Processing'),
    (53, 'National Key Lab of Thermostructure Composite Materials', r'Thermo-?\s?structure Composite'),
    (54, 'Radiochemistry and Radiation Chemistry Key Laboratory of Fundamental Science', r'Radiochemistry and Radiation Chemistry'),
    (56, 'National Key Laboratory of Human Factors Engineering', KL + r'Human Factors Engineering'),
    (57, 'National Key Lab of Sci. & Tech. on Vessel Integrated Power System', r'Vessel Integrated Power'),
    (58, 'National Key Lab of Sci. & Tech. on Micro/Nano Fabrication', KL + r'Micro\s?/\s?Nano\s?(Fabrication|Technology)'),
    (59, 'Nuclear Waste and Environmental Safety Key Laboratory of Defense', r'Nuclear Waste and Environmental Safety'),
    (60, 'National Key Laboratory of Sci. & Tech. on Communications', r'Science and Technology on Communications|Communication Anti-?\s?jamming'),
    (61, 'Vacuum Electronics National Laboratory', r'Vacuum Electronics National Lab'),
    (62, 'Key Laboratory of Wide Band Gap Semiconductor Technology', r'Wide[\s-]?Band[\s-]?Gap Semiconductor (Technology|Materials)'),
]
for n, name, rx in labs:
    add(L, n, name, rx, CN)

U = 'SASTIND university'
TW = r'Taiwan|Taipei|Kaohsiung|Hsinchu'
unis = [
    (1, 'Anhui University', r'Anhui University' + NOT_OF),
    (2, 'Beijing University of Chemical Technology', r'Beijing University of Chemical Technology'),
    (3, 'Changchun University of Science and Technology', r'Changchun University of Science and Technology'),
    (4, 'Dalian University of Technology', r'Dalian University of Technology'),
    (5, 'East China University of Technology', r'East China University of Technology'),
    (6, 'Fuzhou University', r'Fuzhou University' + NOT_OF),
    (7, 'Guilin University of Electronic Technology', r'Guilin University of Electronic Technology'),
    (8, 'Hangzhou Dianzi University', r'Hangzhou Dianzi University'),
    (9, 'Harbin University of Science and Technology', r'Harbin University of Science and Technology'),
    (10, 'Hebei University', r'(?<!North )Hebei University' + NOT_OF),
    (11, 'Hebei University of Science and Technology', r'Hebei University of Science and Technology'),
    (12, 'Hefei University of Technology', r'Hefei University of Technology'),
    (13, 'Heilongjiang Institute of Technology', r'Heilongjiang Institute of Technology'),
    (14, 'Heilongjiang University', r'Heilongjiang University' + NOT_OF),
    (15, 'Henan University of Science and Technology', r'Henan University of Science and Technology'),
    (16, 'Huazhong University of Science and Technology', r'Huazhong University of Science (and|&) Technology|\bHUST\b(?=.*(Wuhan|China))'),
    (17, 'Hunan University', r'Hunan University' + NOT_OF),
    (18, 'Hunan University of Science and Technology', r'Hunan University of Science and Technology'),
    (19, 'Jiangsu University of Science and Technology', r'Jiangsu University of Science and Technology'),
    (20, 'Jilin University', r'Jilin University' + NOT_OF),
    (21, 'Kunming University of Science and Technology', r'Kunming University of Science and Technology'),
    (22, 'Lanzhou University', r'Lanzhou University' + NOT_OF),
    (23, 'Lanzhou University of Technology', r'Lanzhou University of Technology'),
    (24, 'Nanchang Hangkong University', r'Nanchang Hangkong University'),
    (25, 'Nanjing Tech University', r'Nanjing Tech University|Nanjing University of Technology'),
    (26, 'North China University of Science and Technology', r'North China University of Science and Technology'),
    (27, 'North China Institute of Aerospace Engineering', r'North China Institute of Aerospace Engineering'),
    (28, 'North University of China', r'North University of China'),
    (29, 'Peking University', r'Peking University' + NOT_OF),
    (30, 'Shandong University', r'Shandong University' + NOT_OF),
    (31, 'Shandong University of Technology', r'Shandong University of Technology'),
    (32, 'Shanghai Jiao Tong University', r'Shanghai Jiao\s?Tong University'),
    (33, 'Shanghai University', r'Shanghai University' + NOT_OF + r'(?! School of Medicine)'),
    (34, 'Shenyang Aerospace University', r'Shenyang Aerospace University'),
    (35, 'Shenyang Ligong University', r'Shenyang Ligong University'),
    (36, 'Shijiazhuang Tiedao University', r'Shijiazhuang Tiedao University'),
    (37, 'Sichuan University', r'Sichuan University' + NOT_OF),
    (38, 'Soochow University', r'Soochow University', None, TW),
    (39, 'South China University of Technology', r'South China University of Technology'),
    (40, 'Southeast University', r'Southeast University', r'Nanjing'),
    (41, 'Southwest University of Science and Technology', r'Southwest University of Science and Technology'),
    (42, 'Sun Yat-sen University', r'(?<!National )Sun Yat[\s-]?sen University'),
    (43, 'Tianjin Polytechnic University (now Tiangong University)', r'Tianjin Polytechnic University|Tiangong University'),
    (44, 'Tianjin University', r'Tianjin University' + NOT_OF),
    (45, 'Tsinghua University', r'Tsinghua University', None, r'National Tsing\s?Hua'),
    (46, 'University of Electronic Science and Technology of China', r'University of Electronic Science and Technology of China'),
    (47, 'University of Science and Technology Beijing', r'University of Science and Technology,? Beijing'),
    (48, 'University of Shanghai for Science and Technology', r'University of Shanghai for Science and Technology'),
    (49, 'University of South China', r'University of South China'),
    (50, 'Wuhan University', r'Wuhan University' + NOT_OF),
    (51, 'Wuhan University of Technology', r'Wuhan University of Technology'),
    (52, "Xi'an Jiaotong University", r"Xi'?\s?an Jiao\s?tong University(?!-?\s?Liverpool)"),
    (53, "Xi'an Technological University", r"Xi'?\s?an Technological University"),
    (54, 'Xiamen University', r'Xiamen University' + NOT_OF),
    (55, 'Xiangtan University', r'Xiangtan University'),
    (56, 'Xidian University', r'Xidian University'),
    (57, 'Yanshan University', r'Yanshan University'),
    (58, 'Zhejiang University', r'Zhejiang University' + NOT_OF + r'(?! City College)'),
]
for u in unis:
    add(U, *u)


# ---- usual-suspects (SEED ONLY) -------------------------------------------------
# Entities of concern regardless of any one letter. No list numbers.
def usual(cat, name, rx, req=None, excl=None):
    _add(USUAL, cat, None, name, rx, req, excl)


# Bounded, unlike the house list's CN: "Indochina", "Chinatown" and "UPRC" must not count.
# The PRC arm is case-sensitive.
_CHINA = r"\bChina\b|(?-i:\bP\.?\s?R\.?\s?C\b)"
# For names that only mean the Chinese institution when the clause places it in China.
CN_PLACE = _CHINA + r"|\bChinese\b|\bBeijing\b|\bShanghai\b|\bXi'?\s?an\b|\bChongqing\b"

S7 = 'Seven Sons of National Defence'
usual(S7, 'Beihang University', r'\bBeihang\b|Beijing University of Aeronautics (and|&) Astronautics')
usual(S7, 'Beihang University', r'(?-i:\bBUAA\b)', CN_PLACE)
usual(S7, 'Beijing Institute of Technology', r'\bBeijing Institute of Technology')
usual(S7, 'Harbin Engineering University', r'\bHarbin Engineering University')
usual(S7, 'Harbin Institute of Technology', r'\bHarbin Institute of Technology')
usual(S7, 'Nanjing University of Aeronautics and Astronautics', r'\bNanjing University of Aeronautics (and|&) Astronautics')
# "Nanjing University of Information Science and Technology" is a different school and does not match.
usual(S7, 'Nanjing University of Science and Technology', r'\bNanjing University of Science (and|&) Technology')
# Northwestern Polytechnic University (Fremont, California) is an unrelated namesake. The
# "Polytechnical" spelling is Xi'an's; the "Polytechnic" spelling counts only when the same
# clause places it in Xi'an / Shaanxi / China and does not place it in the United States.
usual(S7, 'Northwestern Polytechnical University', r'\bNorthwest(ern)? Polytechnical Univ')
usual(S7, 'Northwestern Polytechnical University', r'\bNorthwestern Polytechnic University',
      r"\bXi'?\s?an\b|\bShaanxi\b|" + _CHINA,
      r'Fremont|California|(?-i:\bCA\b)|(?-i:\bUSA\b)|United States')

D = 'Defence university / research academy'
usual(D, 'National University of Defense Technology', r'\bNational University of Defen[cs]e Technology')
# "Chinese Academy of Engineering" alone is the civilian academy and does not match.
usual(D, 'China Academy of Engineering Physics', r'\bChin(a|ese) Academy of Engineering Physics')
usual(D, 'Academy of Military Medical Sciences', r'\bAcademy of Military Medical Sciences?')

P = 'PLA medical'
# (?-i:...) keeps the acronym case-sensitive: "Pla" is a Catalan place-name word and a
# surname. Each PLA entity has two entries. Names with no namesake outside China match on
# their own: "(Second|Third|Fourth) Military Medical University" and anything saying
# "Chinese PLA" / "Chinese People's Liberation Army". The bare forms ("PLA", "People's
# Liberation Army", "Army Medical University") need CN_PLACE in the same clause.
_CPLA = r"Chinese (People'?s Liberation Army|(?-i:PLA\b))"
_PLA_WORDS = r"People'?s Liberation Army"
_PLA_ACR = r"(?-i:\bPLA\b)"
_PLA = r"(" + _PLA_WORDS + r"|" + _PLA_ACR + r")"
_HOSP = r"(Hospital|Medical Cent(er|re))"
# The Second / Third / Fourth Military Medical Universities, under their current names too
# (Naval / Army / Air Force Medical University), and the PLA's own medical school. The
# former First Military Medical University is the civilian Southern Medical University
# since 2004 and is not matched.
usual(P, 'PLA medical universities',
      r'\b(Second|Third|Fourth) Military Medical University'
      r'|Medical (School|College) of (the )?' + _CPLA +
      r'|' + _CPLA + r' Medical (School|College|University)')
usual(P, 'PLA medical universities',
      r'\b(Naval|Army|Air Force) (Military )?Medical University'
      r'|Medical (School|College) of (the )?' + _PLA +
      r'|' + _PLA + r' Medical (School|College|University)', CN_PLACE)
# The gap between the PLA token and the hospital word stays inside one clause, and inside
# one comma-delimited part for the bare acronym (PLA is also polylactic acid).
usual(P, 'PLA hospitals',
      _CPLA + r'[^;]{0,80}' + _HOSP + r'|' + _HOSP + r'[^;]{0,80}' + _CPLA)
usual(P, 'PLA hospitals',
      _PLA_WORDS + r'[^;]{0,80}' + _HOSP + r'|' + _HOSP + r'[^;]{0,80}' + _PLA_WORDS +
      r'|' + _PLA_ACR + r'[^;,]{0,60}' + _HOSP + r'|' + _HOSP + r'[^;,]{0,60}' + _PLA_ACR, CN_PLACE)

# >>> ADD HERE, FROM THE PUBLISHED LISTS (not from memory): the academic and research
# >>> institutions on the designated Section 1286 list and on the BIS Entity List.
# >>> One usual(category, entity, regex, required=None, excluded=None) line each, plus a
# >>> positive and a namesake case in test_watchlist_patterns.py.


# ---- funding-flags --------------------------------------------------------------
# Paper-level flags from grant metadata (agency text). Not attributable to a person.
FUND = [
    ('China Scholarship Council (CSC)', r'China Scholarship Council|\bCSC\b.*China|China.*\bCSC\b'),
    ('Overseas Excellent Young Scientists Fund', r'Excellent Young Scien\w+.*Overseas|Overseas.*Excellent Young'),
    ('Postdoctoral Innovation Talent Support Program', r'Postdoctoral Innovati\w+ Talent'),
    ('Research Fund for International Scientists', r'Research Fund for International (Young |Senior |Excellent )?Scien'),
    ('International Outstanding Young Scientists Program', r'International Outstanding Young Scien'),
    ('Qiming Plan', r'Qiming'),
    ('Peacock Team Project', r'Peacock'),
    ('Shanghai Magnolia / Pujiang (Category D)', r'Magnolia|Baiyulan|Pujiang'),
    ('Oriental Talent Plan', r'Oriental Talent|Oriental Ying\s?cai'),
    ('Foreign Scholar "Huizhi" Project', r'Huizhi'),
    ('Yicheng Talent AI Super Individual', r'Yicheng'),
    ('Chip Elite Plan', r'Chip Elite|Xinying'),
    ('AI Guangbo 300 / AI Photon Plan', r'Guangbo|AI Photon'),
    ('Optics Valley 3551 / Global Talent Hunting', r'3551|Optics Valley|Global Talent Hunting'),
    ('Gusu Innovation and Entrepreneurship Leading Talent Plan', r'Gusu'),
    ('Suzhou Young Scientists', r'Suzhou Young Scien'),
    ('Taihu Talent Plan', r'Taihu'),
    ('Zijinshan Talent Plan', r'Zijinshan'),
    ('West Lake Pearl Project', r'West\s?Lake Pearl'),
    ('Yongjiang Talent Program', r'Yongjiang'),
]
FUND = [(n, re.compile(r, re.I)) for n, r in FUND]


# ---- country-of-concern ---------------------------------------------------------
COUNTRY_CATEGORY = 'Country'


_US_STATES = ("Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|Georgia|"
              "Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|"
              "Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|"
              "New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|"
              "Rhode Island|South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|"
              "West Virginia|Wisconsin|Wyoming")
_US_CODES = ("AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|"
             "NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC")
# A country word that is a US town ("Cuba, NM", "Russia, Ohio") or part of a street or
# town name ("China Grove", "Havana Street", "Hong Kong Road") is not the country. The
# state must end its comma part ("..., China, MD Anderson" and "..., China, Washington
# University" still count).
_NOT_US_PLACE = (r"(?!\s+(Grove|Street|St|Road|Rd|Avenue|Ave|Blvd|Boulevard|Drive|Lane)\b)"
                 r"(?!\s*,\s*((?-i:" + _US_CODES + r")|" + _US_STATES + r")\b(?=\s*(\d{5}|,|\.?\s*$)))")
_RX_CHINA = re.compile(r"(\bChina\b" + _NOT_US_PLACE + r")|p\.?\s*r\.?\s*china|people'?s republic of china"
                       r"|chinese academy", re.I)
_RX_HK = re.compile(r"(hong kong" + _NOT_US_PLACE + r")|\bmaca[uo]\b", re.I)
# Taiwan: by name, by its main university cities, or as "Republic of China" (not "People's ...").
_RX_TAIWAN = re.compile(r"taiwan|\btaipei\b|\btaichung\b|\bkaohsiung\b|\bhsinchu\b"
                        r"|(?<!people's )(?<!peoples )republic of china", re.I)
_RX_RUSSIA = re.compile(r"(\brussia\b" + _NOT_US_PLACE + r")|russian federation", re.I)
_RX_IRAN = re.compile(r"\biran\b", re.I)
_RX_NK = re.compile(r"north korea|d\.?p\.?r\.?\s*korea|pyongyang|democratic people'?s republic of korea", re.I)
_RX_CUBA = re.compile(r"(\bcuba\b|havana)" + _NOT_US_PLACE, re.I)
_RX_VENEZUELA = re.compile(r"\bvenezuela\b|caracas", re.I)
_RX_IRAN_JOURNAL = re.compile(r"iranian journal", re.I)
_RX_COLUMBIA = re.compile(r"columbia", re.I)
_COUNTRY_ORDER = ("China", "Russia", "Iran", "North Korea", "Cuba", "Venezuela")


def _clauses(a):
    return [c for c in a.split(';') if c.strip()] or [a]


def countries_of_concern(aff):
    """Countries of concern named in one affiliation string, in a fixed order. Each
    ';'-separated clause is judged on its own, so a guard word in one clause (Taiwan,
    Columbia, "Iranian Journal") does not hide a real hit in another. The Taiwan guard
    applies to the China / PRC / 'Chinese Academy' tokens only, so Hong Kong and Macau
    count regardless."""
    found = set()
    for c in _clauses(aff):
        if (_RX_CHINA.search(c) and not _RX_TAIWAN.search(c)) or _RX_HK.search(c):
            found.add("China")
        if _RX_RUSSIA.search(c):
            found.add("Russia")
        if _RX_IRAN.search(c) and not _RX_IRAN_JOURNAL.search(c):
            found.add("Iran")
        if _RX_NK.search(c):
            found.add("North Korea")
        if _RX_CUBA.search(c) and not _RX_COLUMBIA.search(c):
            found.add("Cuba")
        if _RX_VENEZUELA.search(c):
            found.add("Venezuela")
    return [c for c in _COUNTRY_ORDER if c in found]


# ---- matching ---------------------------------------------------------------------
def norm(s):
    s = unicodedata.normalize('NFKD', s if isinstance(s, str) else '')
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    return re.sub(r'\s+', ' ', s.replace('’', "'").replace('‘', "'").replace('`', "'").replace('‐', '-').replace('–', '-'))


def name_key(first, last):
    # full given name, letters only ("Yi-Ming" == "Yi Ming" == "Yiming"); initials-only forenames won't match (conservative)
    return (re.sub(r'[^a-z]', '', norm(last).lower()), re.sub(r'[^a-z]', '', norm(first).lower()))


def _hit(list_code, a, clauses, rx, req, excl):
    if list_code == USUAL:
        return any(rx.search(c) and (req is None or req.search(c)) and (excl is None or not excl.search(c))
                   for c in clauses)
    return bool(rx.search(a) and (req is None or req.search(a)) and (excl is None or not excl.search(a)))


def match_affiliation(aff):
    """Every (list_code, category, list_no, entity) one affiliation string hits, each at
    most once. Case-insensitive, accent- and apostrophe-normalized."""
    a = norm(aff)
    clauses = _clauses(a)
    out, seen = [], set()
    for list_code, cat, n, name, rx, req, excl in ENTITIES:
        if (list_code, name) in seen:
            continue
        if _hit(list_code, a, clauses, rx, req, excl):
            seen.add((list_code, name))
            out.append((list_code, cat, n, name))
    for country in countries_of_concern(a):
        out.append((COUNTRY, COUNTRY_CATEGORY, None, country))
    return out


def guard_sensitive(aff):
    """True when a whole-string (or whole-clause) guard took part in screening this string:
    an entity whose name matched carries a required or an excluded term, or a country
    token shares a clause with its guard word. The answer is then only right if the
    string is ONE affiliation. DynamoDB joins all of an author's affiliations with ", "
    into one string, so collectAffiliations.py refetches such an article from efetch,
    which keeps them apart."""
    a = norm(aff)
    if any((req or excl) and rx.search(a) for _, _, _, _, rx, req, excl in ENTITIES):
        return True
    return any((_RX_CHINA.search(c) and _RX_TAIWAN.search(c))
               or (_RX_IRAN.search(c) and _RX_IRAN_JOURNAL.search(c))
               or (_RX_CUBA.search(c) and _RX_COLUMBIA.search(c)) for c in _clauses(a))


def match_funder(agency, grant_id=None):
    """Program names flagged by one grant. Agency text for every program; the grant id as
    well for the China Scholarship Council only, as in the original screen."""
    agency, grant_id = norm(agency), norm(grant_id)
    return [name for name, rx in FUND
            if rx.search(agency) or (name.startswith('China Scholarship') and rx.search(grant_id))]
