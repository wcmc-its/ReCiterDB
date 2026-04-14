-- =============================================================================
-- Migration: Add 4 new Feature Generator fields (v1.1)
-- =============================================================================
-- Adds columns introduced by ReCiter Feature Generator:
--   - datePublicationAddedToPMC          (top-level article field)
--   - feedbackScoreTextSimilarity        (evidence.feedbackEvidence)
--   - feedbackScoreJournalTitleSimilarity (evidence.feedbackEvidence)
--   - feedbackScoreBibliographicCoupling  (evidence.feedbackEvidence)
--
-- Safe to run on prod and dev. Uses IF NOT EXISTS-style guards via
-- information_schema check (no-op on re-run).
--
-- Run BEFORE deploying the updated Python ETL, otherwise LOAD DATA INFILE
-- will fail with "Unknown column" on the 4 new headers.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- person_article: + datePublicationAddedToPMC + 3 feedback scores
-- -----------------------------------------------------------------------------

SET @db = DATABASE();

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'person_article'
       AND column_name = 'datePublicationAddedToPMC') = 0,
    'ALTER TABLE person_article ADD COLUMN `datePublicationAddedToPMC` varchar(128) DEFAULT NULL AFTER `datePublicationAddedToEntrez`',
    'SELECT ''person_article.datePublicationAddedToPMC already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'person_article'
       AND column_name = 'feedbackScoreTextSimilarity') = 0,
    'ALTER TABLE person_article ADD COLUMN `feedbackScoreTextSimilarity` float DEFAULT NULL AFTER `feedbackScoreYear`',
    'SELECT ''person_article.feedbackScoreTextSimilarity already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'person_article'
       AND column_name = 'feedbackScoreJournalTitleSimilarity') = 0,
    'ALTER TABLE person_article ADD COLUMN `feedbackScoreJournalTitleSimilarity` float DEFAULT NULL AFTER `feedbackScoreTextSimilarity`',
    'SELECT ''person_article.feedbackScoreJournalTitleSimilarity already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'person_article'
       AND column_name = 'feedbackScoreBibliographicCoupling') = 0,
    'ALTER TABLE person_article ADD COLUMN `feedbackScoreBibliographicCoupling` float DEFAULT NULL AFTER `feedbackScoreJournalTitleSimilarity`',
    'SELECT ''person_article.feedbackScoreBibliographicCoupling already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- analysis_summary_article: + datePublicationAddedToPMC
-- (feedback scores NOT carried into summary — per-person-article only)
-- -----------------------------------------------------------------------------

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'analysis_summary_article'
       AND column_name = 'datePublicationAddedToPMC') = 0,
    'ALTER TABLE analysis_summary_article ADD COLUMN `datePublicationAddedToPMC` varchar(128) DEFAULT NULL AFTER `datePublicationAddedToEntrez`',
    'SELECT ''analysis_summary_article.datePublicationAddedToPMC already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------

SELECT table_name, column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND column_name IN (
    'datePublicationAddedToPMC',
    'feedbackScoreTextSimilarity',
    'feedbackScoreJournalTitleSimilarity',
    'feedbackScoreBibliographicCoupling')
ORDER BY table_name, ordinal_position;
