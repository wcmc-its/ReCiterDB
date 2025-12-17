CREATE DEFINER=`admin`@`%` PROCEDURE `reciterdb`.`setup_job_logging`()
BEGIN
    CREATE TABLE IF NOT EXISTS `analysis_job_log` (
        `id` INT AUTO_INCREMENT PRIMARY KEY,
        `job_id` VARCHAR(50),
        `step` VARCHAR(100),
        `substep` VARCHAR(200),
        `status` VARCHAR(20),
        `rows_affected` INT,
        `message` VARCHAR(500),
        `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB;
END