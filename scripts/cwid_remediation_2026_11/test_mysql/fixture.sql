USE reciterdb;
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE admin_users_roles; TRUNCATE admin_users_departments; TRUNCATE admin_notification_preferences;
TRUNCATE admin_notification_log; TRUNCATE admin_feedback_log; TRUNCATE admin_users; TRUNCATE admin_roles;
TRUNCATE admin_departments; TRUNCATE grant_provenance; TRUNCATE authorship_review; TRUNCATE admin_orcid;
TRUNCATE reporting_ad_hoc_feature_generator_execution; TRUNCATE reporting_ad_hoc_identity_creation;
SET FOREIGN_KEY_CHECKS = 1;
INSERT INTO admin_roles (roleID, roleLabel) VALUES (1,'Superuser'),(2,'Curator_All'),(3,'Reporter_All'),(4,'Curator_Self');
INSERT INTO admin_departments (departmentID, departmentLabel) VALUES (1, 'Medicine');
INSERT INTO admin_users (userID, personIdentifier, nameFirst, nameLast, email, status, proxy_person_ids) VALUES
 -- defect 3: keeper (lowest userID) is inactive with no email/name; the merged-away one is the live account
 (13,    'mrj4001', NULL, NULL, NULL, 0, NULL),
 (40078, 'MRJ4001', 'Mahender', 'Jangari', 'mrj4001@med.cornell.edu', 1, NULL),
 -- mapped lone account, renamed
 (20, 'Abl9002', 'A', 'B', 'abl@x', 1, NULL),
 -- defect 4: proxy list with four mapped ids and two DELETE-class ids
 (30, 'pxy1001', 'P', 'X', 'p@x', 1, '["Abl9002","DLAMB","dlamb","SHP2010","HAF9018","NA","na","keep1"]'),
 -- defect 5: junk accounts; ReCiterBot is referenced only as an impersonator, NewName123 by nothing
 (50, 'ReCiterBot', NULL, NULL, NULL, 1, NULL),
 (51, 'NewName123', NULL, NULL, NULL, 1, NULL),
 -- defect 6: bare uc... ids are CWIDs (lowercased); uc<letters>_ ids are external (case kept)
 (60, 'Uca2001', NULL, NULL, NULL, 1, NULL),
 (61, 'ucsf_AbC', NULL, NULL, NULL, 1, NULL),
 (62, 'UCSF_Q9', NULL, NULL, NULL, 1, NULL),
 (63, 'ucb9001 ', NULL, NULL, NULL, 1, NULL),
 (64, 'ucla_Q1', NULL, NULL, NULL, 1, NULL),
 (65, 'Ucd9001', NULL, NULL, NULL, 1, NULL),
 (66, 'ucA2002', NULL, NULL, NULL, 1, NULL),
 -- underscore test identity: untouched
 (70, '_pab4001', NULL, NULL, NULL, 1, NULL);
INSERT INTO admin_users_roles (userID, roleID) VALUES (13,2),(40078,2),(40078,1),(20,4),(50,1),(51,1),(70,2);
INSERT INTO admin_feedback_log (userID, personIdentifier, articleIdentifier, feedback, impersonatedByUserID) VALUES
 (13, 'abc1234', 1, 'ACCEPTED', 50),
 (40078, 'Abl9002', 2, 'REJECTED', NULL);
INSERT INTO grant_provenance (personIdentifier, pmid, core_project_num) VALUES ('Abl9002', 1, 'R01X'), ('abl9002', 2, 'R01X'), ('NA', 3, 'R01X');
INSERT INTO authorship_review (author_key, candidate_cwids_json, top_cwid, reviewer) VALUES
 ('k1', '[{"cwid": "Abl9002"}, {"cwid": "DLAMB"}, {"cwid": "dlamb"}, {"cwid": "SHP2010"}, {"cwid": "HAF9018"}]', 'DLAMB', 'SHP2010'),
 ('k2', '[{"cwid": "_pab4001"}, {"cwid": "ucsf_AbC"}]', '_pab4001', NULL);
INSERT INTO reporting_ad_hoc_feature_generator_execution (personIdentifier, frequency, `type`) VALUES
 ('Ucb9001', 'd', 't'), ('ucsf_Mixed', 'd', 't'), ('fh_ABC', 'd', 't'), (' usc_X ', 'd', 't'), ('UCSF_Z', 'd', 't'), ('_pab4001', 'd', 't'), ('ucB9001', 'd', 'u');
INSERT INTO reporting_ad_hoc_identity_creation (personIdentifier) VALUES ('Ucx1234 '), ('ucdavis_Q'), ('fredhutch_AA');
