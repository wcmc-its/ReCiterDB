-- =============================================================================
-- Migration: drop the v1 bibliometric report procedures (v3.19)
-- =============================================================================
-- PM #1105 (5h P6). Publication Manager now produces only the .docx bibliometric
-- analysis, which reads the v3.11 flags and analysis_summary_person_peer; nothing
-- calls the v1 RTF procedure any more. generateBibliometricReportTest2 is an
-- uncommitted test copy present on prod only.
--
-- ORDER: apply only AFTER the Publication Manager release that removes the RTF route
--   is live; the previous release calls the procedure when its flag is off.
-- The admin_settings row bibliometricReport (that flag) is deleted here too.
--
-- NOT dropped: the v1 top-N columns on analysis_summary_person, and analysis_nih's
--   stored zeros. Other readers may use them.
--
-- Apply with:
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/drop_generateBibliometricReport_v3.19.sql
-- Rollback: reload generateBibliometricReport from createEventsProceduresReciterDb.sql
--   at 4a4fd3f.
-- =============================================================================

DROP PROCEDURE IF EXISTS `generateBibliometricReport`;
DROP PROCEDURE IF EXISTS `generateBibliometricReportTest2`;

DELETE FROM admin_settings WHERE viewName = 'bibliometricReport';

-- Check: both empty.
SELECT routine_name FROM information_schema.routines
 WHERE routine_schema = DATABASE() AND routine_name LIKE 'generateBibliometricReport%';
SELECT viewName FROM admin_settings WHERE viewName = 'bibliometricReport';
