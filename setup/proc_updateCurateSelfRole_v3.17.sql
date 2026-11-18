-- =============================================================================
-- Reload: updateCurateSelfRole (v3.17, CWID canonicalization)
-- =============================================================================
-- The procedure is defined in setup/createEventsProceduresReciterDb.sql; this file is
-- a byte-for-byte copy of that definition so prod can reload just this one routine
-- (update/test_cwid.py fails if the two drift). Same deploy pattern as #268: apply to
-- prod BEFORE the merge, since nothing in the image reloads routines.
--
-- What changed:
--   * New accounts get the CANONICAL personIdentifier: trimmed, lowercased unless an
--     external-cohort id (usc_, fh_, fredhutch_, uc*_ such as ucsf_/ucla_), one account
--     per canonical id. Ids still invalid after trimming (inner space, '@') get none.
--   * Role 4 and role 3 inserts are DISTINCT and NOT EXISTS per (userID, roleID), so
--     case-only duplicates in person no longer give one account the same role twice,
--     and an account with a blank personIdentifier gets no new roles.
--
-- BEHAVIOUR CHANGE: none for people whose person row is already canonical.
-- ORDER: before alter_admin_users_unique_keys_v3.17.sql (the old body can insert the
--   duplicates those keys refuse, which would make the procedure error).
--
-- Apply with:
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/proc_updateCurateSelfRole_v3.17.sql
-- Check:
--   SHOW CREATE PROCEDURE updateCurateSelfRole\G   (look for "group by c.canon")
-- Rollback: reload the definition from createEventsProceduresReciterDb.sql at 560a397.
-- =============================================================================

DROP PROCEDURE IF EXISTS `updateCurateSelfRole`;
DELIMITER //
CREATE DEFINER=`admin`@`%` PROCEDURE `updateCurateSelfRole`()
BEGIN

-- Update person info for "curate self" users from the "person" table

update admin_users a 
join person p on p.personIdentifier = a.personIdentifier
set a.nameFirst = p.firstName
where (a.nameFirst is null or a.nameFirst = '') and p.firstName is not null;

update admin_users a 
join person p on p.personIdentifier = a.personIdentifier
set a.nameLast = p.lastName
where (a.nameLast is null or a.nameLast = '') and p.lastName is not null;

update admin_users a 
join person p on p.personIdentifier = a.personIdentifier
set a.nameMiddle = p.middleName
where (a.nameMiddle is null or a.nameMiddle = '') and p.middleName is not null;

update admin_users a 
join person p on p.personIdentifier = a.personIdentifier
set a.email = p.primaryEmail
where (a.email is null or a.email = '') and p.primaryEmail is not null;

-- Add new users from person table. Cornell Ithaca people (never run: dateAdded null)
-- are in person only so Find People lists them; they get no account or roles.
--
-- The account is keyed by the CANONICAL personIdentifier (trimmed, lowercased unless an
-- external-cohort id such as fredhutch_Rachel.Rau; same rule as update/cwid.py), one row
-- per canonical id. Copying person's raw value made `Abl9002` accounts, and person rows
-- that differ only in case (`Abl9002` + `abl9002`) became two candidates for one account.
-- An id that is still not a plain id after trimming (inner space, '@') gets no account.

insert into admin_users (personIdentifier, nameFirst, nameMiddle, nameLast, email)
select c.canon, max(c.firstName), max(c.middleName), max(c.lastName), max(c.primaryEmail)
from (
  select case when trim(p.personIdentifier) regexp '^(usc|fh|fredhutch|uc[a-z]*)_'
              then trim(p.personIdentifier)
              else lower(trim(p.personIdentifier)) end as canon,
         p.firstName, p.middleName, p.lastName, p.primaryEmail
  from person p
  where p.primaryEmail not in (select email from admin_users where email is not null and email != '')
  and not (p.dateAdded is null and p.personIdentifier in (select personIdentifier from person_person_type where personType = 'cornell-ithaca'))
) c
where c.canon regexp '^[A-Za-z0-9._-]+$'
and not exists (select 1 from admin_users u where trim(u.personIdentifier) = c.canon)
group by c.canon;

-- Create roles for them. DISTINCT + NOT EXISTS: one (userID, roleID) row per account
-- however many person rows match it (case-only duplicates in person used to give the
-- same account the role twice), and never a role for an account with a blank id.

insert into admin_users_roles (userID, roleID)
select distinct a.userID, 4
from person p
join admin_users a on a.personIdentifier = trim(p.personIdentifier)
where trim(a.personIdentifier) <> ''
and not exists (select 1 from admin_users_roles r where r.userID = a.userID and r.roleID = 4)
and not (p.dateAdded is null and p.personIdentifier in (select personIdentifier from person_person_type where personType = 'cornell-ithaca'));

-- Let's give self-curators the ability to generate reports for everyone

insert into admin_users_roles (userID, roleID)
select distinct a.userID, 3
from person p
join admin_users a on a.personIdentifier = trim(p.personIdentifier)
where trim(a.personIdentifier) <> ''
and not exists (select 1 from admin_users_roles r where r.userID = a.userID and r.roleID = 3)
and not (p.dateAdded is null and p.personIdentifier in (select personIdentifier from person_person_type where personType = 'cornell-ithaca'));


END;
//
DELIMITER ;
