-- =============================================================================
-- v2.9 — authorship_review: close a row the moment its authorship is ACCEPTED
--        anywhere in Publication Manager (two triggers)
-- =============================================================================
-- WHY: /curate and /authorships both write the same GoldStandard, but only
-- /authorships resolves its own authorship_review row. A /curate accept left the
-- row open: nothing on PM's read path looks at knownpmids, the producer's
-- _recheck() closes only its CSV ledger, and the producer's new-row gate reads
-- analysis_summary_author, which trails GoldStandard by a ReCiter re-run plus a
-- nightly import. Measured 2026-09-16 (prod): of 11,442 rows paa2013 resolved via
-- /authorships, 600 had already been decided by drw2004 in /curate -- Drew first
-- 574 times (525 accepts), 370 of those on rows the producer created AFTER his
-- accept. Both PM write paths land in admin_feedback_log synchronously
-- (createFeedbackLog bulkCreate; authorships appendFeedbackLog), same database
-- as authorship_review, so the DB can close the loop itself.
--
-- ACCEPTED only. A rejection must not close a row (other candidates remain, and
-- /authorships' reject-others writes would ripple across same-paper authorships);
-- PM already renders a live `already rejected` pill for that case.
--
-- Trigger 1  afl_close_open_authorship  AFTER INSERT ON admin_feedback_log
--   Row already open when the accept lands (155 of the 525): close it, crediting
--   the accepted cwid, whether it was the lead or a listed candidate. An
--   /authorships assign's own ACCEPTED log row arrives after PM has already set
--   status='assigned', so on its own row this is a no-op.
--
-- Trigger 2  ar_born_closed  BEFORE INSERT ON authorship_review
--   Producer inserts a row for something already accepted (370 of the 525): it is
--   born status='accepted'. "Latest feedback wins" so a 2022 seed accept later
--   reversed does not close anything. The producer's upsert is
--   INSERT ... ON DUPLICATE KEY UPDATE over proposal columns only (aar_db.upsert),
--   so on an existing row the BEFORE INSERT values are discarded and curator
--   status is preserved as before.
--   ponytail: lead cwid only. Checking candidates too would need a lookup by
--   articleIdentifier alone, which admin_feedback_log has no index for
--   (idx_personIdentifier is (personIdentifier, articleIdentifier)); add
--   `INDEX (articleIdentifier)` and widen the EXISTS if candidate-accepted rows
--   show up born-open in any number.
--
-- Both lookups are indexed point reads: authorship_review.ix_pmid and
-- admin_feedback_log.idx_personIdentifier(personIdentifier, articleIdentifier).
--
-- Idempotent: DROP IF EXISTS before each CREATE. Revert = the two DROPs alone.
-- Dry-run 2026-09-16 on prod inside a rolled-back transaction: see
-- ReCiter Research/analysis/authorship_review_feedback_triggers_dryrun_2026-09-16.md

DROP TRIGGER IF EXISTS afl_close_open_authorship;
DROP TRIGGER IF EXISTS ar_born_closed;

DELIMITER $$

CREATE TRIGGER afl_close_open_authorship
AFTER INSERT ON admin_feedback_log FOR EACH ROW
BEGIN
  IF NEW.feedback = 'ACCEPTED' THEN
    UPDATE authorship_review
       SET status          = 'accepted',
           resolution_cwid = NEW.personIdentifier,
           resolved_at     = NOW(),
           note            = CONCAT_WS(' | ', NULLIF(note, ''), 'auto: accepted via feedback log')
     WHERE status = 'open'
       AND pmid = NEW.articleIdentifier
       AND (top_cwid = NEW.personIdentifier
            OR JSON_SEARCH(candidate_cwids_json, 'one', NEW.personIdentifier, NULL, '$[*].cwid') IS NOT NULL);
  END IF;
END$$

CREATE TRIGGER ar_born_closed
BEFORE INSERT ON authorship_review FOR EACH ROW
BEGIN
  IF NEW.status = 'open' AND NEW.top_cwid IS NOT NULL AND NEW.pmid IS NOT NULL AND (
       SELECT feedback FROM admin_feedback_log
        WHERE personIdentifier = NEW.top_cwid AND articleIdentifier = NEW.pmid
        ORDER BY createTimestamp DESC, feedbackID DESC
        LIMIT 1) = 'ACCEPTED' THEN
    SET NEW.status          = 'accepted',
        NEW.resolution_cwid = NEW.top_cwid,
        NEW.resolved_at     = NOW(),
        NEW.note            = CONCAT_WS(' | ', NULLIF(NEW.note, ''), 'auto: accepted via feedback log');
  END IF;
END$$

DELIMITER ;
