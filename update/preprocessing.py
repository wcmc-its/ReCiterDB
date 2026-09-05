#!/usr/bin/env python3
"""
preprocessing.py - Shared feature computation for ReCiter scoring models.

This module contains:
- Feature column definitions for both model types (v3.2: 72 feedback, 47 identity-only)
- Wilson score interval calculation
- Derived feature computation (confidence-aware features)

Used by: verify_setup.py (inference pipeline), main.py (Lambda handler)

v3.2 changes from v3.1:
- Feedback model: same 72 total features, different composition (removed 9 zero-weight,
  added 9 new: 5 BibCoupling/textSim interactions, 2 evidence extensions, 2 identity interactions)
- Identity-only model: 47 features (was 42), added 5 identity-specific derived features
- No ablation mechanism (hardcoded per-model feature lists)
"""

import os
import re
import json
import logging
from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats


# =============================================================================
# CONFIGURATION
# =============================================================================

# Thresholds for identity signal strength (environment-configurable)
STRONG_EMAIL = float(os.getenv("STRONG_EMAIL", "0.90"))
STRONG_ORCID = float(os.getenv("STRONG_ORCID", "0.90"))
STRONG_AFFIL = float(os.getenv("STRONG_AFFIL", "0.95"))

# Features to clip at zero (non-negative only).
#
# These features have 0% negative values in ACCEPTED articles but significant
# negative values in REJECTED articles, creating a perfect separation at zero.
# Without clipping, the model learns a hard binary threshold: any negative
# value -> reject with maximum confidence, regardless of all other evidence.
#
# Clipping removes the cliff while preserving positive-value signal. The
# absence signal is captured instead by informedAbsenceCount/Intensity.
CLIP_AT_ZERO_FEATURES = [
    'feedbackScoreCoAuthorName',     # 0.00% acc negative, 62.66% rej negative
    'feedbackScoreYear',             # 0.00% acc negative, 17.94% rej negative
    'feedbackScoreOrcidCoAuthor',    # 0.00% acc negative,  0.63% rej negative
]

_log = logging.getLogger(__name__)


# =============================================================================
# NAME FREQUENCY DATA (loaded once at import time)
# =============================================================================

def _load_name_frequency():
    """Load name frequency table from data/name_frequency.json.
    Returns (table_dict, median_score) or ({}, 0.0) if unavailable."""
    # Container path first (/var/task/data/), then local dev path (../data/)
    freq_path = Path(__file__).parent / 'data' / 'name_frequency.json'
    if not freq_path.exists():
        freq_path = Path(__file__).parent.parent / 'data' / 'name_frequency.json'
    if freq_path.exists():
        with open(freq_path, 'r') as f:
            table = json.load(f)
        scores = [v['score'] for v in table.values()]
        median = sorted(scores)[len(scores) // 2] if scores else 0.0
        _log.info(f"Loaded name frequency table: {len(table):,} names (median score={median:.4f})")
        return table, median
    return {}, 0.0


_NAME_FREQ_TABLE, _NAME_FREQ_MEDIAN = _load_name_frequency()


def _name_frequency_score(first_name):
    """Look up IDF-like frequency score for a first name.

    For compound names (e.g., "Jean-Pierre", "Sae hee"), splits into tokens,
    discards single-char initials, and averages the scores.
    Returns median score for unknown names or 0.0 if no frequency table loaded.
    """
    if not _NAME_FREQ_TABLE or not first_name or not isinstance(first_name, str):
        return 0.0

    first_name = first_name.strip().lower().replace('.', '')
    tokens = [t for t in re.split(r'[\s\-]+', first_name) if len(t) > 1]

    if not tokens:
        return 0.0

    scores = [_NAME_FREQ_TABLE[t]['score'] if t in _NAME_FREQ_TABLE else _NAME_FREQ_MEDIAN
              for t in tokens]
    return sum(scores) / len(scores)


def _first_name_length(first_name):
    """Return character count of identity first name after normalization.

    For compound names like "Jean-Pierre", returns total string length (12),
    not token average. Returns 0.0 if name missing or empty.
    """
    if not first_name or not isinstance(first_name, str):
        return 0.0
    cleaned = first_name.strip().lower().replace('.', '')
    return float(len(cleaned)) if cleaned else 0.0


def _levenshtein_distance(s1: str, s2: str) -> int:
    """Compute Levenshtein edit distance between two strings. Pure Python."""
    if len(s1) < len(s2):
        return _levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)
    prev = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr = [i + 1]
        for j, c2 in enumerate(s2):
            curr.append(min(prev[j + 1] + 1, curr[j] + 1, prev[j] + (c1 != c2)))
        prev = curr
    return prev[-1]


def _jaro_winkler_similarity(s1: str, s2: str, p: float = 0.1) -> float:
    """Compute Jaro-Winkler similarity between two strings. Pure Python.

    Returns a value in [0, 1] where 1 means identical strings.
    The prefix bonus weight p is clamped to max 0.25 per the standard.
    """
    if s1 == s2:
        return 1.0
    len1, len2 = len(s1), len(s2)
    if len1 == 0 or len2 == 0:
        return 0.0

    # Match window
    match_dist = max(len1, len2) // 2 - 1
    if match_dist < 0:
        match_dist = 0

    s1_matches = [False] * len1
    s2_matches = [False] * len2
    matches = 0
    transpositions = 0

    for i in range(len1):
        start = max(0, i - match_dist)
        end = min(i + match_dist + 1, len2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    # Count transpositions
    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    jaro = (matches / len1 + matches / len2 + (matches - transpositions / 2) / matches) / 3.0

    # Winkler prefix bonus (up to 4 chars)
    prefix = 0
    for i in range(min(4, len1, len2)):
        if s1[i] == s2[i]:
            prefix += 1
        else:
            break

    return jaro + prefix * min(p, 0.25) * (1.0 - jaro)


# =============================================================================
# FEATURE COLUMN DEFINITIONS (v3.2 - hardcoded per-model lists)
# =============================================================================

# Feedback + Identity model: 32 base features
# (feedbackScoreEmail, feedbackScoreYear, grantMatchScore excluded -- zero-weight in v3.2)
FEEDBACK_IDENTITY_BASE_FEATURES = [
    # 13 feedback score features
    'feedbackScoreCites', 'feedbackScoreCoAuthorName',
    'feedbackScoreInstitution', 'feedbackScoreJournal', 'feedbackScoreJournalSubField',
    'feedbackScoreKeyword', 'feedbackScoreTextSimilarity', 'feedbackScoreJournalTitleSimilarity',
    'feedbackScoreOrcid', 'feedbackScoreOrcidCoAuthor',
    'feedbackScoreOrganization', 'feedbackScoreTargetAuthorName',
    'feedbackScoreBibliographicCoupling',
    # 17 identity features (grantMatchScore excluded from feedback model)
    'articleCountScore', 'authorCountScore', 'discrepancyDegreeYearScore', 'emailMatchScore',
    'genderScoreIdentityArticleDiscrepancy', 'journalSubfieldScore',
    'nameMatchFirstScore', 'nameMatchLastScore', 'nameMatchMiddleScore', 'nameMatchModifierScore',
    'organizationalUnitMatchingScore', 'scopusNonTargetAuthorInstitutionalAffiliationScore',
    'targetAuthorInstitutionalAffiliationMatchTypeScore',
    'pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore',
    'relationshipPositiveMatchScore', 'relationshipNegativeMatchScore', 'relationshipIdentityCount',
    # Feedback history
    'countAccepted', 'countRejected',
]

# Identity-Only model: 18 base features (no feedback scores, no countAccepted/countRejected)
# NOTE: grantMatchScore is INCLUDED here (active in identity-only model)
IDENTITY_ONLY_BASE_FEATURES = [
    'articleCountScore', 'authorCountScore', 'discrepancyDegreeYearScore', 'emailMatchScore',
    'genderScoreIdentityArticleDiscrepancy', 'grantMatchScore', 'journalSubfieldScore',
    'nameMatchFirstScore', 'nameMatchLastScore', 'nameMatchMiddleScore', 'nameMatchModifierScore',
    'organizationalUnitMatchingScore',
    'scopusNonTargetAuthorInstitutionalAffiliationScore',
    'targetAuthorInstitutionalAffiliationMatchTypeScore',
    'pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore',
    'relationshipPositiveMatchScore', 'relationshipNegativeMatchScore', 'relationshipIdentityCount',
]

# Derived features for Feedback+Identity model (18 feedback-only + 22 identity-shared = 40 total)
DERIVED_FEATURES_FEEDBACK = [
    # 18 feedback-only derived features
    'acceptanceRateLowerBound',
    'feedbackConfidence',
    'uncertainRejectionRisk',
    'feedbackDensity',
    'feedbackIdentityInteraction',
    'informedAbsenceCount',
    'informedAbsenceIntensity',
    'nameConflictConfirmed',
    # TextSimilarity interaction features (5)
    'hasTextEvidence',
    'textSimNoCoauthorInteraction',
    'textSimNewJournalInteraction',
    'textSimFeedbackConfInteraction',
    'textSimAffilGapInteraction',
    # BibCoupling interaction features (3)
    'bibCouplingFeedbackConfInteraction',
    'bibCouplingHighConfOnly',
    'hasBibCouplingSignal',
    # Extended evidence aggregation features (2)
    'worstSingleEvidenceExtended',
    'netEvidenceCountExtended',
    # 22 identity-shared features included in feedback model
    'identityStrength',
    'netEvidenceCount',
    'ambiguityRisk',
    'nameInstitutionInteraction',
    'worstSingleEvidence',
    'nameQualityMin',
    'firstNameFrequencyScore',
    'firstNameLength',
    'nameFrequencyMatchInteraction',
    'nameLengthMatchInteraction',
    'firstMiddleMatchInteraction',
    'nameMatchMiddleAgreement',
    'nameJaroWinkler',
    'nameEditDistanceNorm',
    'forenameLengthRatio',
    'firstMiddleCoverage',
    'nameMatchTypeOrdinal',
    'hasEmail',
    'hasGender',
    'hasOrgUnit',
    'nameAffilStrength',
    'evidenceConsistency',
]

# Derived features for Identity-Only model (29 identity-shared features)
DERIVED_FEATURES_IDENTITY_ONLY = [
    'identityStrength',
    'netEvidenceCount',
    'ambiguityRisk',
    'nameInstitutionInteraction',
    'worstSingleEvidence',
    'nameQualityMin',
    'firstNameFrequencyScore',
    'nameGenderConflict',
    'firstNameLength',
    'nameFrequencyMatchInteraction',
    'nameLengthMatchInteraction',
    'firstMiddleMatchInteraction',
    'nameMatchMiddleAgreement',
    'nameJaroWinkler',
    'nameEditDistanceNorm',
    'forenameLengthRatio',
    'firstMiddleCoverage',
    'nameMatchTypeOrdinal',
    'hasEmail',
    'hasDegreeYear',
    'hasGrants',
    'hasRelationships',
    'hasGender',
    'hasOrgUnit',
    'nameAffilStrength',
    'evidenceConsistency',
    # Phase 9 Round 2 identity-specific interactions (3)
    'nameMatchConsistency',
    'affilRelationshipInteraction',
    'scopusPubmedAffilAgreement',
]

# Complete feature lists (direct concatenation, no filter)
FEEDBACK_IDENTITY_FEATURES = FEEDBACK_IDENTITY_BASE_FEATURES + DERIVED_FEATURES_FEEDBACK
IDENTITY_ONLY_FEATURES = IDENTITY_ONLY_BASE_FEATURES + DERIVED_FEATURES_IDENTITY_ONLY

# Module-level assertions to catch feature count drift
assert len(FEEDBACK_IDENTITY_FEATURES) == 72, \
    f"Expected 72 feedback features, got {len(FEEDBACK_IDENTITY_FEATURES)}"
assert len(IDENTITY_ONLY_FEATURES) == 47, \
    f"Expected 47 identity-only features, got {len(IDENTITY_ONLY_FEATURES)}"

# Backward compatibility alias
DERIVED_FEATURES = DERIVED_FEATURES_FEEDBACK


# =============================================================================
# STATISTICAL FUNCTIONS
# =============================================================================

def wilson_lower_bound(successes: float, failures: float, confidence: float = 0.95) -> float:
    """
    Wilson score interval lower bound for success rate.

    Key properties:
    - With no data (0,0): returns 0.5 (maximum uncertainty)
    - With small samples: pulls toward 0.5 (low confidence)
    - With large samples: approaches raw proportion (high confidence)
    - No arbitrary thresholds or cliff effects

    Args:
        successes: Number of successes (acceptances)
        failures: Number of failures (rejections)
        confidence: Confidence level for interval (default 95%)

    Returns:
        Lower bound of confidence interval for success rate [0, 1]
    """
    n = successes + failures
    if n == 0:
        return 0.5  # No data = maximum uncertainty

    z = stats.norm.ppf(1 - (1 - confidence) / 2)
    p = successes / n

    denominator = 1 + z**2 / n
    centre = p + z**2 / (2 * n)
    spread = z * np.sqrt((p * (1 - p) + z**2 / (4 * n)) / n)

    lower = (centre - spread) / denominator
    return max(0.0, min(1.0, lower))


# =============================================================================
# DERIVED FEATURE COMPUTATION
# =============================================================================

# Features used for evidence counting and worst-single-evidence
_POSITIVE_EVIDENCE_FEATURES = [
    'nameMatchFirstScore', 'emailMatchScore', 'grantMatchScore',
    'journalSubfieldScore', 'organizationalUnitMatchingScore',
    'targetAuthorInstitutionalAffiliationMatchTypeScore',
    'pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore',
    'scopusNonTargetAuthorInstitutionalAffiliationScore',
    'relationshipPositiveMatchScore',
    'genderScoreIdentityArticleDiscrepancy',
]

_NEGATIVE_EVIDENCE_FEATURES = [
    'nameMatchFirstScore', 'nameMatchMiddleScore', 'nameMatchModifierScore',
    'targetAuthorInstitutionalAffiliationMatchTypeScore',
    'discrepancyDegreeYearScore', 'genderScoreIdentityArticleDiscrepancy',
    'relationshipNegativeMatchScore',
    'articleCountScore',
]

_WORST_EVIDENCE_FEATURES = [
    'nameMatchFirstScore', 'nameMatchMiddleScore',
    'targetAuthorInstitutionalAffiliationMatchTypeScore',
    'discrepancyDegreeYearScore', 'genderScoreIdentityArticleDiscrepancy',
]

# Extended evidence lists for netEvidenceCountExtended (Phase 9).
# Adds textSimilarity and bibCoupling to the positive/negative signal count.
_EXTENDED_POSITIVE_EVIDENCE_FEATURES = list(_POSITIVE_EVIDENCE_FEATURES) + [
    'feedbackScoreTextSimilarity',
    'feedbackScoreBibliographicCoupling',
]
_EXTENDED_NEGATIVE_EVIDENCE_FEATURES = list(_NEGATIVE_EVIDENCE_FEATURES) + [
    'feedbackScoreTextSimilarity',
    'feedbackScoreBibliographicCoupling',
]


def _compute_identity_engineered_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute engineered identity features shared by both models.

    These features synthesize raw identity signals into higher-level concepts.
    All features are computed regardless of which model will use them --
    extra columns are harmless; the feature list controls what goes to the scaler.

    Requires 'identityStrength' to already be computed on df.
    """
    # 1. netEvidenceCount: positive signals minus negative signals
    pos_count = sum(
        (df[f] > 0).astype(int) for f in _POSITIVE_EVIDENCE_FEATURES
    )
    neg_count = sum(
        (df[f] < 0).astype(int) for f in _NEGATIVE_EVIDENCE_FEATURES
    )
    df['netEvidenceCount'] = pos_count - neg_count

    # 2. ambiguityRisk: high articleCountScore + low identityStrength
    df['ambiguityRisk'] = df['articleCountScore'] * (1 - df['identityStrength'])

    # 3. nameInstitutionInteraction: name * best affiliation
    best_affil = np.maximum(
        df['targetAuthorInstitutionalAffiliationMatchTypeScore'],
        df['pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore']
    )
    df['nameInstitutionInteraction'] = df['nameMatchFirstScore'] * best_affil

    # 4. worstSingleEvidence: min of key identity features
    df['worstSingleEvidence'] = np.minimum.reduce([
        df[f] for f in _WORST_EVIDENCE_FEATURES
    ])

    # 5. nameQualityMin: weakest name component
    df['nameQualityMin'] = np.minimum.reduce([
        df['nameMatchFirstScore'],
        df['nameMatchLastScore'],
        df['nameMatchMiddleScore']
    ])

    # 6. firstNameFrequencyScore: IDF-like score from name frequency table (person-level)
    if _NAME_FREQ_TABLE and 'identityFirstName' in df.columns:
        df['firstNameFrequencyScore'] = df['identityFirstName'].map(_name_frequency_score)
    else:
        df['firstNameFrequencyScore'] = 0.0

    # 7. nameGenderConflict: wrong first name AND wrong gender
    nf_shared = df['nameMatchFirstScore']
    gd_shared = df['genderScoreIdentityArticleDiscrepancy']
    name_gender_both = (nf_shared < -2.0) & (gd_shared < 0)
    df['nameGenderConflict'] = np.where(name_gender_both, nf_shared.abs() * gd_shared.clip(upper=0).abs(), 0.0)

    # 8. firstNameLength: raw character count of identity first name
    if 'identityFirstName' in df.columns:
        df['firstNameLength'] = df['identityFirstName'].map(_first_name_length)
    else:
        df['firstNameLength'] = 0.0

    # 9. nameFrequencyMatchInteraction: match score weighted by name rarity
    df['nameFrequencyMatchInteraction'] = (
        df['nameMatchFirstScore'] * df['firstNameFrequencyScore']
    )

    # 10. nameLengthMatchInteraction: match score weighted by name length
    df['nameLengthMatchInteraction'] = (
        df['nameMatchFirstScore'] * np.log1p(df['firstNameLength'])
    )

    # 11. firstMiddleMatchInteraction: first x middle name score product
    df['firstMiddleMatchInteraction'] = (
        df['nameMatchFirstScore'] * df['nameMatchMiddleScore']
    )

    # 12. nameMatchMiddleAgreement: sign concordance indicator
    df['nameMatchMiddleAgreement'] = (
        np.sign(df['nameMatchFirstScore']) * np.sign(df['nameMatchMiddleScore'])
    )

    # --- Continuous name similarity features ---
    has_article_first = 'articleAuthorFirstName' in df.columns
    has_identity_middle = 'identityMiddleName' in df.columns
    has_match_type = 'nameMatchFirstType' in df.columns

    if has_article_first and 'identityFirstName' in df.columns:
        id_first = df['identityFirstName'].fillna('').astype(str).str.strip().str.lower()
        art_first = df['articleAuthorFirstName'].fillna('').astype(str).str.strip().str.lower()

        # 13. nameJaroWinkler
        df['nameJaroWinkler'] = [
            _jaro_winkler_similarity(a, b) for a, b in zip(id_first, art_first)
        ]

        # 14. nameEditDistanceNorm
        df['nameEditDistanceNorm'] = [
            1.0 - _levenshtein_distance(a, b) / max(len(a), len(b), 1)
            for a, b in zip(id_first, art_first)
        ]

        # 15. forenameLengthRatio
        id_len = id_first.str.len().clip(lower=1)
        art_len = art_first.str.len()
        df['forenameLengthRatio'] = art_len / id_len
    else:
        df['nameJaroWinkler'] = 0.0
        df['nameEditDistanceNorm'] = 0.0
        df['forenameLengthRatio'] = 0.0

    # 16. firstMiddleCoverage
    if has_article_first and 'identityFirstName' in df.columns and has_identity_middle:
        id_first = df['identityFirstName'].fillna('').astype(str).str.strip().str.lower()
        id_middle = df['identityMiddleName'].fillna('').astype(str).str.strip().str.lower()
        art_first = df['articleAuthorFirstName'].fillna('').astype(str).str.strip().str.lower()
        combined_len = id_first.str.len() + id_middle.str.len()
        art_len = art_first.str.len().clip(lower=1)
        df['firstMiddleCoverage'] = combined_len / art_len
    else:
        df['firstMiddleCoverage'] = 0.0

    # 17. nameMatchTypeOrdinal
    if has_match_type:
        _MATCH_TYPE_MAP = {
            'full-exact': 5, 'inferredInitials-exact': 4, 'full-fuzzy': 3,
            'noMatch': 2, 'conflictingAllButInitials': 1, 'conflictingEntirely': 0,
        }
        df['nameMatchTypeOrdinal'] = (
            df['nameMatchFirstType'].fillna('').astype(str).map(_MATCH_TYPE_MAP).fillna(2.0)
        )
    else:
        df['nameMatchTypeOrdinal'] = 0.0

    # 18-23. Sparse identity indicators
    df['hasEmail'] = (df['emailMatchScore'] != 0).astype(float)
    df['hasDegreeYear'] = (df['discrepancyDegreeYearScore'] != 0).astype(float)
    df['hasGrants'] = (df['grantMatchScore'] != 0).astype(float)
    df['hasRelationships'] = (df['relationshipIdentityCount'] > 0).astype(float)
    df['hasGender'] = (df['genderScoreIdentityArticleDiscrepancy'] != 0).astype(float)
    df['hasOrgUnit'] = (df['organizationalUnitMatchingScore'] != 0).astype(float)

    # --- Phase 9 candidate features (identity-shared) ---

    # 24. nameAffilStrength: name match weighted by overall identity strength
    df['nameAffilStrength'] = df['nameMatchFirstScore'] * df['identityStrength']

    # 25. evidenceConsistency: std dev of positive evidence features
    df['evidenceConsistency'] = df[_POSITIVE_EVIDENCE_FEATURES].std(axis=1)

    # --- Phase 9 Round 2 identity-specific interactions ---
    # These are used by the identity-only model but computed for both
    # (extra columns are harmless).

    # 26. nameMatchConsistency: product of all three name components
    df['nameMatchConsistency'] = (
        df['nameMatchFirstScore'] * df['nameMatchLastScore'] * df['nameMatchMiddleScore']
    )

    # 27. affilRelationshipInteraction: best affiliation * relationship positive
    best_affil_r2 = np.maximum(
        df['targetAuthorInstitutionalAffiliationMatchTypeScore'],
        df['pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore']
    )
    df['affilRelationshipInteraction'] = (
        best_affil_r2.clip(lower=0) * df['relationshipPositiveMatchScore'].clip(lower=0)
    )

    # 28. scopusPubmedAffilAgreement: both affiliation sources agree
    df['scopusPubmedAffilAgreement'] = (
        df['scopusNonTargetAuthorInstitutionalAffiliationScore'].clip(lower=0)
        * df['pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore'].clip(lower=0)
    )

    return df


def compute_derived_features_feedback_identity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute derived features for Feedback + Identity model.

    Uses email, ORCID feedback score, and affiliation for identity strength.

    Args:
        df: DataFrame with base features already filled (NaN -> 0)

    Returns:
        DataFrame with derived features added
    """
    df = df.copy()

    # 0. Clip features with perfect separation at zero (prevent cliff effects)
    for col in CLIP_AT_ZERO_FEATURES:
        if col in df.columns:
            df[col] = df[col].clip(lower=0)

    # 1. Acceptance Rate Lower Bound (Wilson score)
    df['acceptanceRateLowerBound'] = df.apply(
        lambda row: wilson_lower_bound(
            row['countAccepted'],
            row['countRejected'],
            confidence=0.95
        ), axis=1
    )

    # 2. Feedback Confidence (log-scaled total feedback)
    total_feedback = df['countAccepted'] + df['countRejected']
    df['feedbackConfidence'] = np.log1p(total_feedback)

    # 3. Identity Strength (continuous, 0 to 1)
    # Uses three signals: email, ORCID feedback, affiliation
    email_strength = (df['emailMatchScore'].fillna(0).clip(0, 1) / STRONG_EMAIL).clip(0, 1)
    orcid_strength = (df['feedbackScoreOrcid'].fillna(0).clip(0, 1) / STRONG_ORCID).clip(0, 1)
    affil_strength = np.maximum(
        df['targetAuthorInstitutionalAffiliationMatchTypeScore'].fillna(0).clip(0, 1),
        df['pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore'].fillna(0).clip(0, 1)
    ) / STRONG_AFFIL
    affil_strength = affil_strength.clip(0, 1)

    # Take max of the three signals (any strong signal is good)
    df['identityStrength'] = np.maximum.reduce([email_strength, orcid_strength, affil_strength])

    # Compute shared identity engineered features
    df = _compute_identity_engineered_features(df)

    # 4. Uncertain Rejection Risk (continuous, 0 to 1)
    confidence_factor = (1 - np.exp(-total_feedback / 10))
    df['uncertainRejectionRisk'] = (
        (1 - df['acceptanceRateLowerBound']) *
        (1 - df['identityStrength']) *
        confidence_factor
    )

    # 5. Feedback Density (fraction of 15 feedback features that are non-zero)
    feedback_score_cols = [
        'feedbackScoreCites', 'feedbackScoreCoAuthorName', 'feedbackScoreEmail',
        'feedbackScoreInstitution', 'feedbackScoreJournal', 'feedbackScoreJournalSubField',
        'feedbackScoreKeyword', 'feedbackScoreTextSimilarity', 'feedbackScoreJournalTitleSimilarity',
        'feedbackScoreOrcid', 'feedbackScoreOrcidCoAuthor',
        'feedbackScoreOrganization', 'feedbackScoreTargetAuthorName', 'feedbackScoreYear',
        'feedbackScoreBibliographicCoupling'
    ]
    df['feedbackDensity'] = (df[feedback_score_cols] != 0).sum(axis=1) / len(feedback_score_cols)

    # 6. Feedback-Identity Interaction
    df['feedbackIdentityInteraction'] = df['feedbackDensity'] * df['identityStrength']

    # 7. Informed Absence Count: zero-valued feedback dimensions for established researchers
    ca = df['countAccepted'].fillna(0)
    zero_count = (df[feedback_score_cols] == 0).sum(axis=1)
    df['informedAbsenceCount'] = np.where(ca > 0, zero_count, 0).astype(float)

    # 8. Informed Absence Intensity: scales absence count by researcher history depth
    df['informedAbsenceIntensity'] = df['informedAbsenceCount'] * np.log1p(ca)

    # 9. Name Conflict Confirmed: identity name matching AND feedback name pattern agree
    nf = df['nameMatchFirstScore']
    ta = df['feedbackScoreTargetAuthorName']
    both_negative = (nf < -2.0) & (ta < 0)
    df['nameConflictConfirmed'] = np.where(both_negative, nf.abs() * ta.clip(upper=0).abs(), 0.0)

    # --- TextSimilarity interaction features ---
    text_sim = df['feedbackScoreTextSimilarity']

    # 10. hasTextEvidence: binary indicator
    df['hasTextEvidence'] = (text_sim != 0).astype(float)

    # 11. textSimNoCoauthorInteraction: text fills the gap when no coauthor evidence
    df['textSimNoCoauthorInteraction'] = text_sim * (df['feedbackScoreCoAuthorName'] == 0).astype(float)

    # 12. textSimNewJournalInteraction: text fills the gap when journal is unknown
    df['textSimNewJournalInteraction'] = text_sim * (df['feedbackScoreJournal'] == 0).astype(float)

    # 13. textSimFeedbackConfInteraction: more feedback = more reliable centroid
    df['textSimFeedbackConfInteraction'] = text_sim * df['feedbackConfidence']

    # 14. textSimAffilGapInteraction: text fills affiliation gap
    best_affil = np.maximum(
        df['targetAuthorInstitutionalAffiliationMatchTypeScore'],
        df['pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore']
    )
    df['textSimAffilGapInteraction'] = text_sim * (1 - best_affil.clip(0, 1))

    # --- Phase 9 BibCoupling interaction features ---
    bib_coupling = df['feedbackScoreBibliographicCoupling']

    # 15. bibCouplingFeedbackConfInteraction
    df['bibCouplingFeedbackConfInteraction'] = bib_coupling * df['feedbackConfidence']

    # 16. bibCouplingHighConfOnly: gated by minimum feedback threshold
    df['bibCouplingHighConfOnly'] = (
        bib_coupling * (df['feedbackConfidence'] > np.log1p(10)).astype(float)
    )

    # 17. hasBibCouplingSignal: binary presence indicator
    df['hasBibCouplingSignal'] = (bib_coupling != 0).astype(float)

    # 18. worstSingleEvidenceExtended: expanded with textSim dimension
    df['worstSingleEvidenceExtended'] = np.minimum(
        df['worstSingleEvidence'],
        np.where(text_sim != 0, text_sim, df['worstSingleEvidence'])
    )

    # 19. netEvidenceCountExtended: expanded evidence count with textSim + bibCoupling
    ext_pos_count = sum(
        (df[f] > 0).astype(int) for f in _EXTENDED_POSITIVE_EVIDENCE_FEATURES
    )
    ext_neg_count = sum(
        (df[f] < 0).astype(int) for f in _EXTENDED_NEGATIVE_EVIDENCE_FEATURES
    )
    df['netEvidenceCountExtended'] = ext_pos_count - ext_neg_count

    return df


def compute_derived_features_identity_only(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute derived features for Identity-Only model.

    This model is BLIND to feedback history (countAccepted, countRejected).
    Only computes identityStrength using email and affiliation signals.

    Args:
        df: DataFrame with base features already filled (NaN -> 0)

    Returns:
        DataFrame with derived features added
    """
    df = df.copy()

    # Identity Strength (continuous, 0 to 1)
    # Uses two signals: email, affiliation (no ORCID or feedback data)
    email_strength = (df['emailMatchScore'].fillna(0).clip(0, 1) / STRONG_EMAIL).clip(0, 1)
    affil_strength = np.maximum(
        df['targetAuthorInstitutionalAffiliationMatchTypeScore'].fillna(0).clip(0, 1),
        df['pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore'].fillna(0).clip(0, 1)
    ) / STRONG_AFFIL
    affil_strength = affil_strength.clip(0, 1)

    # Take max of the two signals
    df['identityStrength'] = np.maximum(email_strength, affil_strength)

    # Compute shared identity engineered features
    df = _compute_identity_engineered_features(df)

    # NOTE: No acceptanceRateLowerBound, feedbackConfidence, or uncertainRejectionRisk
    # because this model is blind to feedback history (countAccepted/countRejected)

    return df


# =============================================================================
# INFERENCE PREPROCESSING (no label required)
# =============================================================================

def preprocess_for_inference_feedback_identity(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess for inference (no userAssertion/label required)."""
    df = df.copy()
    for col in FEEDBACK_IDENTITY_BASE_FEATURES:
        if col not in df.columns:
            df[col] = 0.0
    df[FEEDBACK_IDENTITY_BASE_FEATURES] = df[FEEDBACK_IDENTITY_BASE_FEATURES].fillna(0)
    df = compute_derived_features_feedback_identity(df)
    return df


def preprocess_for_inference_identity_only(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess for inference (no userAssertion/label required)."""
    df = df.copy()
    for col in IDENTITY_ONLY_BASE_FEATURES:
        if col not in df.columns:
            df[col] = 0.0
    df[IDENTITY_ONLY_BASE_FEATURES] = df[IDENTITY_ONLY_BASE_FEATURES].fillna(0)
    df = compute_derived_features_identity_only(df)
    return df
