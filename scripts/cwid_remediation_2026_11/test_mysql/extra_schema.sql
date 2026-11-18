-- tables the remediation touches that createDatabaseTableReciterDb.sql does not reach (it stops
-- at a syntax error further down on MariaDB 11.4); identity_grad_school_faculty is built by
-- update/buildIdentity.py, reduced here to the column the SQL reads
USE reciterdb;
CREATE TABLE IF NOT EXISTS `reporting_ad_hoc_feature_generator_execution` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) DEFAULT NULL,
  `frequency` varchar(128) DEFAULT NULL,
  `type` varchar(128) DEFAULT NULL,
  `createdAt` timestamp NOT NULL DEFAULT current_timestamp(),
  `updatedAt` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `idx_personIdentifier` (`personIdentifier`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
CREATE TABLE IF NOT EXISTS `reporting_ad_hoc_identity_creation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) DEFAULT NULL,
  `inPersonTable` varchar(128) DEFAULT NULL,
  `type` varchar(128) DEFAULT NULL,
  `firstName` varchar(128) DEFAULT NULL,
  `middleName` varchar(128) DEFAULT NULL,
  `lastName` varchar(128) DEFAULT NULL,
  `title` varchar(128) DEFAULT NULL,
  `primaryEmail` varchar(128) DEFAULT NULL,
  `email1` varchar(128) DEFAULT NULL,
  `email2` varchar(128) DEFAULT NULL,
  `degreeYearTerminal` varchar(128) DEFAULT NULL,
  `orgUnitLabel1` varchar(128) DEFAULT NULL,
  `orgUnitType1` varchar(128) DEFAULT NULL,
  `orgUnitLabel2` varchar(128) DEFAULT NULL,
  `orgUnitType2` varchar(128) DEFAULT NULL,
  `primaryOrganizationalUnit` varchar(128) DEFAULT NULL,
  `primaryInstitution` varchar(128) DEFAULT NULL,
  `institution1` varchar(128) DEFAULT NULL,
  `institution2` varchar(128) DEFAULT NULL,
  `institution3` varchar(128) DEFAULT NULL,
  `institution4` varchar(128) DEFAULT NULL,
  `institution5` varchar(128) DEFAULT NULL,
  `relationshipFirstName1` varchar(128) DEFAULT NULL,
  `relationshipLastName1` varchar(128) DEFAULT NULL,
  `relationshipPersonID1` varchar(128) DEFAULT NULL,
  `relationshipType1` varchar(128) DEFAULT NULL,
  `relationshipFirstName2` varchar(128) DEFAULT NULL,
  `relationshipLastName2` varchar(128) DEFAULT NULL,
  `relationshipPersonID2` varchar(128) DEFAULT NULL,
  `relationshipType2` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_personIdentifier` (`personIdentifier`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
CREATE TABLE IF NOT EXISTS identity_grad_school_faculty (jid INT PRIMARY KEY, cwid VARCHAR(64)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
