"""Shape check for the Cornell Ithaca person rows (updateReciterDB.cornell_ithaca_people_sql).
The behavior itself was checked on the dev DB with temporary tables; this pins the
parts PM relies on: dateAdded and the metrics left NULL, idempotent, cornell-ithaca only."""
import re

from updateReciterDB import cornell_ithaca_people_sql

sql = " ".join(cornell_ithaca_people_sql("person_new", "person_person_type_new").split())
assert "INSERT INTO `person_new` (personIdentifier, `precision`, recall," in sql
assert "dateAdded" not in sql, "dateAdded must stay NULL: PM reads it as 'never run'"
assert "SELECT DISTINCT i.personIdentifier, NULL, NULL, NULL, NULL, NULL" in sql
assert "JOIN `person_person_type_new` t" in sql and "t.personType = 'cornell-ithaca'" in sql
assert re.search(r"WHERE NOT EXISTS \(SELECT 1 FROM `person_new` p WHERE p.personIdentifier = i.personIdentifier\)", sql)
print("ok")
