
CREATE DATABASE IF NOT EXISTS `reciterDB` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `reciterDB`.`admin_departments` (
  `departmentID` int(11) NOT NULL AUTO_INCREMENT,
  `institutionalDepartmentCode` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `departmentLabel` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `source` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `status` int(1) DEFAULT 1,
  `createTimestamp` timestamp NOT NULL DEFAULT current_timestamp(),
  `modifyTimestamp` timestamp NOT NULL ON UPDATE current_timestamp(),
  PRIMARY KEY (`departmentID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE IF NOT EXISTS `reciterDB`.`admin_feedback_log` (
  `feedbackID` int(11) NOT NULL AUTO_INCREMENT,
  `userID` int(11) DEFAULT NULL,
  `personIdentifier` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `articleIdentifier` int(11) DEFAULT NULL,
  `feedback` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `createTimestamp` timestamp NOT NULL DEFAULT current_timestamp(),
  `modifyTimestamp` timestamp NOT NULL ON UPDATE current_timestamp(),
  PRIMARY KEY (`feedbackID`),
  KEY `idx_userID` (`userID`) USING BTREE,
  KEY `idx_personIdentifier` (`personIdentifier`,`articleIdentifier`) USING BTREE,
  CONSTRAINT `admin_feedback_log_ibfk_1` FOREIGN KEY (`userID`) REFERENCES `admin_users` (`userID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE IF NOT EXISTS `reciterDB`.`admin_notification_log` (
  `notificationID` int(11) NOT NULL AUTO_INCREMENT,
  `userID` int(11) DEFAULT NULL,
  `articleIdentifier` int(11) DEFAULT NULL,
  `articleScore` int(11) DEFAULT NULL,
  `email` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `dateSent` datetime DEFAULT NULL,
  `createTimestamp` timestamp NOT NULL DEFAULT current_timestamp(),
  `modifyTimestamp` timestamp NOT NULL ON UPDATE current_timestamp(),
  PRIMARY KEY (`notificationID`),
  KEY `admin_notification_log_ibfk_1` (`userID`),
  CONSTRAINT `admin_notification_log_ibfk_1` FOREIGN KEY (`userID`) REFERENCES `admin_users` (`userID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE IF NOT EXISTS `reciterDB`.`admin_notification_preferences` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `userID` int(11) DEFAULT NULL,
  `minimumThreshold` int(11) DEFAULT NULL,
  `frequency` int(11) DEFAULT NULL,
  `accepted` int(1) DEFAULT 1,
  `status` int(1) DEFAULT 1,
  `createTimestamp` timestamp NOT NULL DEFAULT current_timestamp(),
  `modifyTimestamp` timestamp NOT NULL ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `admin_notification_preferences_ibfk_1` (`userID`),
  CONSTRAINT `admin_notification_preferences_ibfk_1` FOREIGN KEY (`userID`) REFERENCES `admin_users` (`userID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE IF NOT EXISTS `reciterDB`.`admin_roles` (
  `roleID` int(11) NOT NULL AUTO_INCREMENT,
  `roleLabel` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`roleID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE IF NOT EXISTS `reciterDB`.`admin_users` (
  `userID` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `nameFirst` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `nameMiddle` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `nameLast` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `email` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `status` int(1) DEFAULT 1,
  `createTimestamp` timestamp NOT NULL DEFAULT current_timestamp(),
  `modifyTimestamp` timestamp NOT NULL ON UPDATE current_timestamp(),
  PRIMARY KEY (`userID`),
  KEY `idx_personIdentifier` (`personIdentifier`) USING BTREE,
  KEY `idx_email` (`email`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE IF NOT EXISTS `reciterDB`.`admin_users_departments` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `userID` int(11) DEFAULT NULL,
  `departmentID` int(11) DEFAULT NULL,
  `createTimestamp` timestamp NOT NULL DEFAULT current_timestamp(),
  `modifyTimestamp` timestamp NOT NULL ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `admin_users_departments_ibfk_1` (`departmentID`),
  KEY `idx_userID` (`userID`) USING BTREE,
  CONSTRAINT `admin_users_departments_ibfk_1` FOREIGN KEY (`departmentID`) REFERENCES `admin_departments` (`departmentID`),
  CONSTRAINT `admin_users_departments_ibfk_2` FOREIGN KEY (`userID`) REFERENCES `admin_users` (`userID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE IF NOT EXISTS `reciterDB`.`admin_users_roles` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `userID` int(11) DEFAULT NULL,
  `roleID` int(11) DEFAULT NULL,
  `createTimestamp` timestamp NOT NULL DEFAULT current_timestamp(),
  `modifyTimestamp` timestamp NOT NULL ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `admin_users_roles_ibfk_1` (`userID`),
  KEY `admin_users_roles_ibfk_2` (`roleID`),
  CONSTRAINT `admin_users_roles_ibfk_1` FOREIGN KEY (`userID`) REFERENCES `admin_users` (`userID`),
  CONSTRAINT `admin_users_roles_ibfk_2` FOREIGN KEY (`roleID`) REFERENCES `admin_roles` (`roleID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `reciterDB`.`analysis_altmetric` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `doi` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `pmid` int(11) DEFAULT NULL,
  `altmetric_jid` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `context-all-count` int(11) DEFAULT NULL,
  `context-all-mean` float(10,4) DEFAULT NULL,
  `context-all-rank` int(11) DEFAULT NULL,
  `context-all-pct` int(11) DEFAULT NULL,
  `context-all-higher_than` int(11) DEFAULT NULL,
  `context-similar_age_3m-count` int(11) DEFAULT 0,
  `context-similar_age_3m-mean` float(10,4) DEFAULT 0.0000,
  `context-similar_age_3m-rank` int(11) DEFAULT 0,
  `context-similar_age_3m-pct` int(11) DEFAULT 0,
  `context-similar_age_3m-higher_than` int(11) DEFAULT 0,
  `altmetric_id` int(11) DEFAULT NULL,
  `cited_by_msm_count` int(11) DEFAULT NULL,
  `is_oa` varchar(12) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `cited_by_posts_count` int(11) DEFAULT NULL,
  `cited_by_tweeters_count` int(11) DEFAULT NULL,
  `cited_by_feeds_count` int(11) DEFAULT NULL,
  `cited_by_fbwalls_count` int(11) DEFAULT NULL,
  `cited_by_rh_count` int(11) DEFAULT NULL,
  `cited_by_accounts_count` int(11) DEFAULT NULL,
  `last_updated` int(11) DEFAULT NULL,
  `score` float(10,4) DEFAULT NULL,
  `history-1y` float(10,4) DEFAULT NULL,
  `history-6m` float(10,4) DEFAULT NULL,
  `history-3m` float(10,4) DEFAULT NULL,
  `history-1m` float(10,4) DEFAULT NULL,
  `history-1w` float(10,4) DEFAULT NULL,
  `history-at` float(10,4) DEFAULT NULL,
  `added_on` int(11) DEFAULT NULL,
  `published_on` int(11) DEFAULT NULL,
  `readers-mendeley` int(11) DEFAULT NULL,
  `createTimestamp` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `y` (`doi`) USING BTREE,
  KEY `x` (`pmid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `reciterDB`.`analysis_override_author_position` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `pmid` int(11) DEFAULT NULL,
  `position` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_pmid` (`pmid`) USING BTREE,
  KEY `idx_personIdentifier` (`personIdentifier`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `reciterDB`.`analysis_nih` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `pmid` int(11) DEFAULT 0,
  `year` int(11) DEFAULT NULL,
  `is_research_article` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_clinical` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `relative_citation_ratio` float(6,2) DEFAULT NULL,
  `nih_percentile` float(5,2) DEFAULT NULL,
  `citation_count` int(11) DEFAULT NULL,
  `citations_per_year` float(7,3) DEFAULT NULL,
  `expected_citations_per_year` float(7,3) DEFAULT NULL,
  `field_citation_rate` float(7,3) DEFAULT NULL,
  `provisional` varchar(12) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `doi` varchar(150) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `human` float(4,2) DEFAULT NULL,
  `animal` float(4,2) DEFAULT NULL,
  `molecular_cellular` float(4,2) DEFAULT NULL,
  `apt` float(4,2) DEFAULT NULL,
  `x_coord` float(5,4) DEFAULT NULL,
  `y_coord` float(5,4) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `sdfsdfsdf` (`pmid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `reciterDB`.`analysis_nih_cites` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `citing_pmid` int(11) DEFAULT 0,
  `cited_pmid` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `y` (`cited_pmid`) USING BTREE,
  KEY `x` (`citing_pmid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci; 


CREATE TABLE IF NOT EXISTS `reciterDB`.`analysis_nih_cites_clin` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `citing_pmid` int(11) DEFAULT 0,
  `cited_pmid` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `y` (`cited_pmid`) USING BTREE,
  KEY `x` (`citing_pmid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci; 



CREATE TABLE IF NOT EXISTS `reciterDB`.`analysis_special_characters` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `specialCharacter` varchar(3) DEFAULT NULL,
  `RTFescape` varchar(10) DEFAULT NULL,
  `characterName` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_character` (`specialCharacter`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE IF NOT EXISTS `reciterDB`.`analysis_summary_article` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `pmid` int(11) DEFAULT 0,
  `pmcid` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `publicationDateDisplay` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `publicationDateStandardized` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `datePublicationAddedToEntrez` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `articleTitle` varchar(5000) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `articleTitleRTF` varchar(2000) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `publicationTypeCanonical` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `publicationTypeNIH` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `journalTitleVerbose` varchar(2000) COLLATE utf8mb4_unicode_ci DEFAULT 'NULL',
  `issn` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `journalImpactScore1` float(6,3) DEFAULT NULL,
  `journalImpactScore2` float(6,3) DEFAULT NULL,
  `articleYear` int(11) DEFAULT 0,
  `doi` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `volume` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT 'NULL',
  `issue` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT 'NULL',
  `pages` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT 'NULL',
  `citationCountScopus` int(11) DEFAULT NULL,
  `citationCountNIH` int(11) DEFAULT NULL,
  `percentileNIH` float(5,2) DEFAULT NULL,
  `relativeCitationRatioNIH` float(6,2) DEFAULT NULL,
  `readersMendeley` int(11) DEFAULT NULL,
  `trendingPubsScore` float(7,2) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `x` (`doi`) USING BTREE,
  KEY `z` (`pmid`) USING BTREE,
  KEY `w` (`issn`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `reciterDB`.`analysis_summary_author` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `pmid` int(11) DEFAULT 0,
  `authors` varchar(1000) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `authorsRTF` varchar(1000) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `authorPosition` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `z` (`pmid`) USING BTREE,
  KEY `y` (`personIdentifier`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `reciterDB`.`analysis_summary_person` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `nameFirst` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `nameMiddle` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `nameLast` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `facultyRank` varchar(25) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `department` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `hindexNIH` int(11) DEFAULT NULL,
  `h5indexNIH` int(11) DEFAULT NULL,
  `hindexScopus` int(11) DEFAULT NULL,
  `h5indexScopus` int(11) DEFAULT NULL,
  `countAll` int(11) DEFAULT 0,
  `countFirst` int(11) DEFAULT 0,
  `countSenior` int(11) DEFAULT 0,
  `top10PercentileAll` float(6,3) DEFAULT NULL,
  `top10RankAll` int(11) DEFAULT NULL,
  `top10DenominatorAll` int(11) DEFAULT NULL,
  `top5PercentileAll` float(7,3) DEFAULT NULL,
  `top5RankAll` int(11) DEFAULT NULL,
  `top5DenominatorAll` int(11) DEFAULT NULL,
  `top10PercentileFirst` float(6,3) DEFAULT NULL,
  `top10RankFirst` int(11) DEFAULT NULL,
  `top10DenominatorFirst` int(11) DEFAULT NULL,
  `top5PercentileFirst` float(7,3) DEFAULT NULL,
  `top5RankFirst` int(11) DEFAULT NULL,
  `top5DenominatorFirst` int(11) DEFAULT NULL,
  `top10PercentileSenior` float(6,3) DEFAULT NULL,
  `top10RankSenior` int(11) DEFAULT NULL,
  `top10DenominatorSenior` int(11) DEFAULT NULL,
  `top5PercentileSenior` float(7,3) DEFAULT NULL,
  `top5RankSenior` int(11) DEFAULT NULL,
  `top5DenominatorSenior` int(11) DEFAULT NULL,
  `top10PercentileFirstSenior` float(6,3) DEFAULT NULL,
  `top10RankFirstSenior` int(11) DEFAULT NULL,
  `top10DenominatorFirstSenior` int(11) DEFAULT NULL,
  `top5PercentileFirstSenior` float(7,3) DEFAULT NULL,
  `top5RankFirstSenior` int(11) DEFAULT NULL,
  `top5DenominatorFirstSenior` int(11) DEFAULT NULL,
  `hindexStatus` int(11) DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `idx_personIdentifier` (`personIdentifier`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `reciterDB`.`analysis_temp_article` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `pmid` int(11) DEFAULT NULL,
  `position` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `personIdentifier` (`pmid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `reciterDB`.`analysis_temp_hindex` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) DEFAULT NULL,
  `citation_count` int(11) DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `personIdentifier` (`personIdentifier`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `reciterDB`.`analysis_temp_output_article` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `pmid` int(11) DEFAULT NULL,
  `authors` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `authorsRTF` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `pmid` (`pmid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `reciterDB`.`analysis_temp_output_author` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT '0',
  `pmid` int(11) DEFAULT NULL,
  `citationRTF` varchar(3000) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `rowRTF` blob DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `pmid` (`pmid`) USING BTREE,
  KEY `personIdentifier` (`personIdentifier`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `reciterDB`.`identity` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cwid` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `surname` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `middleName` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `givenName` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `primaryTitle` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `primaryAcademicDepartment` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `primaryAcademicDivision` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `primaryProgram` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `fullTimeFaculty` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `postdoc` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `studentMDNYC` varchar(12) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `studentMDQatar` varchar(12) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `studentMDPhD` varchar(12) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `studentPhDTriI` varchar(12) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `studentPhDWeill` varchar(12) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `partTimeFaculty` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `voluntaryFaculty` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `emeritusFaculty` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `adjunctFaculty` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `residentNYP` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `fellow` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `faculty` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `nonFaculty` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `inactiveFaculty` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `alumniMD` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `alumniMDPHD` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `alumniPHD` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `inactiveNonAlumniStudent` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `startDateWCMFaculty` int(11) DEFAULT NULL,
  `endDateWCMFaculty` int(11) DEFAULT NULL,
  `startDateWCMStudent` int(11) DEFAULT NULL,
  `endDateWCMStudent` int(11) DEFAULT NULL,
  `popsProfile` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `directoryProfile` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `vivoProfile` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `facultyRank` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `primaryOrg` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `notes` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `createTimestamp` timestamp NULL DEFAULT current_timestamp(),
  `modifyTimestamp` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `dfsdfsdf` (`cwid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `reciterDB`.`journal_impact_alternative` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `journalTitle` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `issn` varchar(12) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `eissn` varchar(12) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `impactScore1` float(7,3) DEFAULT NULL,
  `impactScore2` float(6,5) DEFAULT NULL,
  `rank` int(11) DEFAULT NULL,
  `cites` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_EISSN` (`eissn`) USING BTREE,
  KEY `idx_FullJournalTitle` (`journalTitle`) USING BTREE,
  KEY `idx_issn` (`issn`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE IF NOT EXISTS `reciterDB`.`journal_impact_scimago` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `sourceID` varchar(128) DEFAULT NULL,
  `rank` int(11) DEFAULT NULL,
  `title` varchar(1000) DEFAULT NULL,
  `issn` varchar(128) DEFAULT NULL,
  `issn1` varchar(12) DEFAULT NULL,
  `issn2` varchar(12) DEFAULT NULL,
  `issn3` varchar(12) DEFAULT NULL,
  `sjr` float(8,5) DEFAULT NULL,
  `sjrBestQuartlie` varchar(10) DEFAULT NULL,
  `type` varchar(30) DEFAULT NULL,
  `hindex` int(11) DEFAULT NULL,
  `totalDocs` int(11) DEFAULT NULL,
  `totalDocs3Years` int(11) DEFAULT NULL,
  `citableDocs3Years` int(11) DEFAULT NULL,
  `totalRefs` int(11) DEFAULT NULL,
  `totalCites3Years` int(11) DEFAULT NULL,
  `citesPerDoc2Years` float(6,2) DEFAULT NULL,
  `refPerDoc` float(6,2) DEFAULT NULL,
  `country` varchar(128) DEFAULT NULL,
  `region` varchar(128) DEFAULT NULL,
  `publisher` varchar(400) DEFAULT NULL,
  `coverage` varchar(400) DEFAULT NULL,
  `categories` varchar(1000) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `index1` (`issn1`),
  KEY `index2` (`issn2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `reciterDB`.`journal_nlm` (
  `nlmabbreviation` varchar(200) DEFAULT NULL,
  `nlmfulltitle` varchar(300) DEFAULT NULL,
  `nlmissn` varchar(9) DEFAULT NULL,
  `nlmeissn` varchar(9) DEFAULT NULL,
  `nlmisoabbreviation` varchar(100) DEFAULT NULL,
  `nlmcatalog` varchar(12) NOT NULL DEFAULT '0',
  PRIMARY KEY (`nlmcatalog`),
  KEY `nlm` (`nlmissn`) USING BTREE,
  KEY `idx_nlmeissn` (`nlmeissn`) USING BTREE,
  KEY `idx_nlmfulltitle` (`nlmfulltitle`) USING BTREE,
  KEY `idx_nlmabbreviation` (`nlmabbreviation`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE IF NOT EXISTS `reciterDB`.`person` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `firstName` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `middleName` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `lastName` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `title` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `primaryEmail` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `primaryOrganizationalUnit` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `primaryInstitution` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `dateAdded` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `dateUpdated` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `precision` float DEFAULT 0,
  `recall` float DEFAULT 0,
  `countSuggestedArticles` int(11) DEFAULT 0,
  `countPendingArticles` int(11) DEFAULT 0,
  `overallAccuracy` float DEFAULT 0,
  `mode` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`,`personIdentifier`),
  KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `reciterDB`.`person_article` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `pmid` int(11) DEFAULT 0,
  `pmcid` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `totalArticleScoreStandardized` int(11) DEFAULT 0,
  `totalArticleScoreNonStandardized` float DEFAULT 0,
  `userAssertion` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `publicationDateDisplay` varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `publicationDateStandardized` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `publicationTypeCanonical` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `scopusDocID` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `journalTitleVerbose` varchar(2000) COLLATE utf8mb4_unicode_ci DEFAULT 'NULL',
  `articleTitle` varchar(5000) COLLATE utf8mb4_unicode_ci DEFAULT '''NULL''',
  `feedbackScoreAccepted` float DEFAULT NULL,
  `feedbackScoreRejected` float DEFAULT NULL,
  `feedbackScoreNull` float DEFAULT NULL,
  `articleAuthorNameFirstName` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT '''NULL''',
  `articleAuthorNameLastName` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT '''NULL''',
  `institutionalAuthorNameFirstName` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT '''NULL''',
  `institutionalAuthorNameMiddleName` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT '''NULL''',
  `institutionalAuthorNameLastName` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT '''NULL''',
  `nameMatchFirstScore` float DEFAULT 0,
  `nameMatchFirstType` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT 'NULL',
  `nameMatchMiddleScore` float DEFAULT 0,
  `nameMatchMiddleType` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT 'NULL',
  `nameMatchLastScore` float DEFAULT 0,
  `nameMatchLastType` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT 'NULL',
  `nameMatchModifierScore` float DEFAULT 0,
  `nameScoreTotal` float DEFAULT 0,
  `emailMatch` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `emailMatchScore` float DEFAULT NULL,
  `journalSubfieldScienceMetrixLabel` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT 'NULL',
  `journalSubfieldScienceMetrixID` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `journalSubfieldDepartment` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `journalSubfieldScore` float DEFAULT 0,
  `relationshipEvidenceTotalScore` float DEFAULT 0,
  `relationshipMinimumTotalScore` float DEFAULT 0,
  `relationshipNonMatchCount` int(11) DEFAULT 0,
  `relationshipNonMatchScore` float DEFAULT 0,
  `articleYear` int(11) DEFAULT 0,
  `identityBachelorYear` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT 'NULL',
  `discrepancyDegreeYearBachelor` int(11) DEFAULT 0,
  `discrepancyDegreeYearBachelorScore` float DEFAULT 0,
  `identityDoctoralYear` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `discrepancyDegreeYearDoctoral` int(11) DEFAULT 0,
  `discrepancyDegreeYearDoctoralScore` float DEFAULT 0,
  `genderScoreArticle` float DEFAULT 0,
  `genderScoreIdentity` float DEFAULT 0,
  `genderScoreIdentityArticleDiscrepancy` float DEFAULT 0,
  `personType` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `personTypeScore` float DEFAULT NULL,
  `countArticlesRetrieved` int(11) DEFAULT 0,
  `articleCountScore` float DEFAULT 0,
  `targetAuthorInstitutionalAffiliationArticlePubmedLabel` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore` float DEFAULT NULL,
  `scopusNonTargetAuthorInstitutionalAffiliationSource` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT '''''''NULL''''''',
  `scopusNonTargetAuthorInstitutionalAffiliationScore` float DEFAULT 0,
  `totalArticleScoreWithoutClustering` float DEFAULT 0,
  `clusterScoreAverage` float DEFAULT 0,
  `clusterReliabilityScore` float DEFAULT 0,
  `clusterScoreModificationOfTotalScore` float DEFAULT 0,
  `datePublicationAddedToEntrez` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `clusterIdentifier` int(11) DEFAULT NULL,
  `doi` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `issn` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `issue` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT 'NULL',
  `journalTitleISOabbreviation` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `pages` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT 'NULL',
  `timesCited` int(11) DEFAULT NULL,
  `volume` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT 'NULL',
  PRIMARY KEY (`id`),
  KEY `idx_issn` (`issn`) USING BTREE,
  KEY `idx_scopusDocID` (`scopusDocID`) USING BTREE,
  KEY `idx_doi` (`doi`) USING BTREE,
  KEY `idx_pmid` (`pmid`) USING BTREE,
  KEY `personIdentifier` (`personIdentifier`,`pmid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE IF NOT EXISTS `reciterDB`.`person_article_author` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `pmid` int(11) DEFAULT 0,
  `authorFirstName` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT '''NULL''',
  `authorLastName` varchar(150) COLLATE utf8mb4_unicode_ci DEFAULT '''NULL''',
  `targetAuthor` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT '''NULL''',
  `rank` int(11) DEFAULT 0,
  `orcid` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `personIdentifier` (`personIdentifier`,`pmid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE IF NOT EXISTS `reciterDB`.`person_article_department` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `pmid` int(11) DEFAULT 0,
  `identityOrganizationalUnit` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `articleAffiliation` varchar(10000) COLLATE utf8mb4_unicode_ci DEFAULT 'NULL',
  `organizationalUnitType` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `organizationalUnitMatchingScore` float DEFAULT 0,
  `organizationalUnitModifier` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `organizationalUnitModifierScore` float DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `personIdentifier` (`personIdentifier`,`pmid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE IF NOT EXISTS `reciterDB`.`person_article_grant` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `pmid` int(11) DEFAULT 0,
  `articleGrant` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `grantMatchScore` float DEFAULT 0,
  `institutionGrant` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE IF NOT EXISTS `reciterDB`.`person_article_keyword` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `pmid` int(11) DEFAULT 0,
  `keyword` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `sdfsdfsdf` (`personIdentifier`,`pmid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE IF NOT EXISTS `reciterDB`.`person_article_relationship` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `pmid` int(11) DEFAULT 0,
  `relationshipNameArticleFirstName` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT '''NULL''',
  `relationshipNameArticleLastName` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT '''NULL''',
  `relationshipNameIdentityFirstName` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT '''NULL''',
  `relationshipNameIdentityLastName` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT '''NULL''',
  `relationshipType` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `relationshipMatchType` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `relationshipMatchingScore` float DEFAULT 0,
  `relationshipVerboseMatchModifierScore` float DEFAULT 0,
  `relationshipMatchModifierMentor` float DEFAULT NULL,
  `relationshipMatchModifierMentorSeniorAuthor` float DEFAULT NULL,
  `relationshipMatchModifierManager` float DEFAULT NULL,
  `relationshipMatchModifierManagerSeniorAuthor` float DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `personIdentifier` (`personIdentifier`,`pmid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE IF NOT EXISTS `reciterDB`.`person_article_scopus_non_target_author_affiliation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `pmid` int(11) DEFAULT NULL,
  `nonTargetAuthorInstitutionLabel` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT 'NULL',
  `nonTargetAuthorInstitutionID` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT 'NULL',
  `nonTargetAuthorInstitutionCount` int(11) DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `personIdentifier` (`personIdentifier`,`pmid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE IF NOT EXISTS `reciterDB`.`person_article_scopus_target_author_affiliation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `pmid` int(11) DEFAULT 0,
  `targetAuthorInstitutionalAffiliationSource` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT 'NULL',
  `scopusTargetAuthorInstitutionalAffiliationIdentity` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `targetAuthorInstitutionalAffiliationArticleScopusLabel` varchar(2000) COLLATE utf8mb4_unicode_ci DEFAULT '''NULL''',
  `targetAuthorInstitutionalAffiliationArticleScopusAffiliationId` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT 'NULL',
  `targetAuthorInstitutionalAffiliationMatchType` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `targetAuthorInstitutionalAffiliationMatchTypeScore` float DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `personIdentifier` (`personIdentifier`,`pmid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



CREATE TABLE IF NOT EXISTS `reciterDB`.`person_person_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `personType` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_personIdentifier` (`personIdentifier`) USING BTREE,
  KEY `idx_personType` (`personType`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `reciterDB`.`journal_science_metrix` (
  `smsid` int(11) NOT NULL DEFAULT 0,
  `publication_name` varchar(500) DEFAULT NULL,
  `issn` varchar(25) DEFAULT NULL,
  `issncut` varchar(25) DEFAULT NULL,
  `eissn` varchar(25) DEFAULT NULL,
  `domain` varchar(100) DEFAULT NULL,
  `field` varchar(100) DEFAULT NULL,
  `subfield` varchar(100) DEFAULT NULL,
  `subfield_id` int(10) DEFAULT NULL,
  `nlmabbreviation` varchar(300) DEFAULT NULL,
  PRIMARY KEY (`smsid`),
  KEY `scopus_document_pk` (`smsid`) USING BTREE,
  KEY `issn` (`issn`,`eissn`) USING BTREE,
  KEY `idx_subfield` (`subfield`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

