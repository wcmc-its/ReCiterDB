USE reciterdb;
-- every row returned is a FAILED expectation
SELECT 'FAIL d3 keeper status/email/name' AS f, u.* FROM admin_users u WHERE userID = 13
  AND NOT (status = 1 AND email = 'mrj4001@med.cornell.edu' AND nameFirst = 'Mahender' AND nameLast = 'Jangari' AND BINARY personIdentifier = 'mrj4001');
SELECT 'FAIL d3 loser still present' AS f, userID FROM admin_users WHERE userID = 40078;
SELECT 'FAIL d3 keeper roles' AS f, GROUP_CONCAT(roleID ORDER BY roleID) r FROM admin_users_roles WHERE userID = 13 HAVING r <> '1,2' OR r IS NULL;
SELECT 'FAIL d4 proxy' AS f, proxy_person_ids FROM admin_users WHERE userID = 30
  AND BINARY JSON_COMPACT(proxy_person_ids) <> BINARY '["abl9002","djl2008","djl2008","shp2010","haf9018","keep1"]';
SELECT 'FAIL d4 candidates' AS f, candidate_cwids_json FROM authorship_review WHERE author_key = 'k1'
  AND BINARY candidate_cwids_json <> BINARY '[{"cwid": "abl9002"}, {"cwid": "djl2008"}, {"cwid": "djl2008"}, {"cwid": "shp2010"}, {"cwid": "haf9018"}]';
SELECT 'FAIL top/reviewer' AS f, top_cwid, reviewer FROM authorship_review WHERE author_key = 'k1' AND NOT (BINARY top_cwid = 'djl2008' AND BINARY reviewer = 'shp2010');
SELECT 'FAIL underscore review row' AS f, candidate_cwids_json FROM authorship_review WHERE author_key = 'k2'
  AND BINARY candidate_cwids_json <> BINARY '[{"cwid": "_pab4001"}, {"cwid": "ucsf_AbC"}]';
SELECT 'FAIL d5 ReCiterBot kept inactive' AS f FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM admin_users WHERE userID = 50 AND status = 0);
SELECT 'FAIL d5 ReCiterBot roles removed' AS f FROM admin_users_roles WHERE userID = 50;
SELECT 'FAIL d5 NewName123 deleted' AS f FROM admin_users WHERE userID = 51;
SELECT 'FAIL d6 admin_users ids' AS f, GROUP_CONCAT(CONCAT(userID, '=[', personIdentifier, ']') ORDER BY userID) g
  FROM admin_users WHERE userID BETWEEN 60 AND 70
  HAVING BINARY g <> BINARY '60=[uca2001],61=[ucsf_AbC],62=[ucsf_q9],63=[ucb9001],64=[ucla_Q1],65=[ucd9001],66=[uca2002],70=[_pab4001]';
SELECT 'FAIL d6 reporting fge' AS f, GROUP_CONCAT(CONCAT('[', personIdentifier, ']') ORDER BY id) g FROM reporting_ad_hoc_feature_generator_execution
  HAVING BINARY g <> BINARY '[ucb9001],[ucsf_Mixed],[fh_ABC],[usc_X],[ucsf_z],[_pab4001],[ucb9001]';
SELECT 'FAIL d6 reporting ic' AS f, GROUP_CONCAT(CONCAT('[', personIdentifier, ']') ORDER BY id) g FROM reporting_ad_hoc_identity_creation
  HAVING BINARY g <> BINARY '[ucx1234],[ucdavis_Q],[fredhutch_AA]';
SELECT 'FAIL grant_provenance' AS f, GROUP_CONCAT(personIdentifier) g FROM grant_provenance HAVING BINARY g <> BINARY 'abl9002,abl9002' OR g IS NULL;
SELECT 'FAIL feedback_log' AS f, GROUP_CONCAT(CONCAT(userID,':',personIdentifier,':',COALESCE(impersonatedByUserID,'-')) ORDER BY feedbackID) g FROM admin_feedback_log
  HAVING BINARY g <> BINARY '13:abc1234:50,13:abl9002:-';
SELECT 'expectations checked' AS done;
