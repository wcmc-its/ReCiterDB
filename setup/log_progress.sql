CREATE DEFINER=`admin`@`%` PROCEDURE `reciterdb`.`log_progress`(
    IN p_job_id VARCHAR(50),
    IN p_step VARCHAR(100),
    IN p_substep VARCHAR(200),
    IN p_status VARCHAR(20),
    IN p_rows INT,
    IN p_message VARCHAR(500)
)
BEGIN
    INSERT INTO analysis_job_log (job_id, step, substep, status, rows_affected, message)
    VALUES (p_job_id, p_step, p_substep, p_status, p_rows, p_message);
    -- Also output to console
    SELECT CONCAT(NOW(), ' | ', p_step, ' | ', p_substep, ' | ', p_status,
                  IFNULL(CONCAT(' | rows: ', p_rows), ''),
                  IFNULL(CONCAT(' | ', p_message), '')) AS progress;
END