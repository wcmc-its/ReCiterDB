LOCK TABLES `admin_roles` WRITE;
INSERT INTO `admin_roles` (`roleID`, `roleLabel`) VALUES 
  (1,'Superuser'),
  (2,'Curator_All'),
  (3,'Reporter_All'),
  (4,'Curator_Self'),
  (5,'Curator_Department'),
  (6,'Curator_Department_Delegate');
UNLOCK TABLES;