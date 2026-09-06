# Limiting blast radius on the Identity Authority database

Written 2026-09-06, before any credential exists, because that is the only moment
this is cheap.

## Why this is not theoretical

This port has, in one day, produced these mistakes:

- Both draft writers were written against SQL Server. The database is MariaDB.
  Twenty-one `MERGE` statements that cannot parse.
- A guard was defined, exercised only in a self-test, and never called -- while
  three source docstrings claimed it was protecting them.
- Two rounds of generated source functions failed adversarial review, the second
  introducing 14 regressions, all of them hard aborts on unmeasured premises.
- Four factual assumptions were asserted and later corrected by measurement:
  the CSID's meaning, its placement, the org unit identifier, and the base DNs.

Every one was caught before touching data, by review or by a guard. But the
guards were written by the same process that made the mistakes, which is exactly
the property that makes them insufficient on their own. **A GRANT is enforced by
something the agent cannot reason its way past.** That is the difference.

## The shape: two users, not one

Create two database accounts, not one.

```sql
-- Read-only. Everything except the nightly write uses this: schema queries,
-- --dry-run, the diff against Splunk, every ad-hoc investigation.
CREATE USER 'ia_ro'@'<cluster-cidr>' IDENTIFIED BY '<password>';
GRANT SELECT ON `<ia_database>`.* TO 'ia_ro'@'<cluster-cidr>';

-- Writer. Referenced ONLY by the cronjob, never mounted into an interactive or
-- development pod. Note what is absent: no DELETE, no DROP, no ALTER, no CREATE.
CREATE USER 'ia_rw'@'<cluster-cidr>' IDENTIFIED BY '<password>';
GRANT SELECT, INSERT, UPDATE ON `<ia_database>`.* TO 'ia_rw'@'<cluster-cidr>';
```

Scope the host to the cluster's egress CIDR rather than `%`.

### Why no DELETE on the writer

The job ships `IA_DELETE_MODE=flag`, which is an `UPDATE`, so it does not need
`DELETE` to do its job. Withholding the grant means the single most destructive
failure mode -- the one the Splunk job actually exhibits, where one failed feeder
nominates an entire type for deletion -- is impossible at the database level
rather than merely guarded at three places in the application.

If `conf-db_outputs` later shows the original really does delete rows, granting
`DELETE` becomes a deliberate, reviewable change with a date attached. That is a
second gate, and it costs nothing to have.

### Why no DDL on the writer

`_org_unit` does not exist yet and the job emits columns some tables do not have.
Without `CREATE`/`ALTER` the job cannot paper over a schema mismatch by altering
the table -- it aborts naming the missing columns, which is what
`upsert()` already does. Schema changes stay a human action.

## Kubernetes side

Two secrets, and the write one is referenced by exactly one object:

| secret | grants | referenced by |
|---|---|---|
| `identity-authority-secrets-ro` | SELECT | development and probe pods |
| `identity-authority-secrets` | SELECT, INSERT, UPDATE | the `reciterdb-identity-authority` cronjob, nothing else |

Use `kubectl create secret`, never `apply`: apply copies the values into the
`last-applied-configuration` annotation in cleartext. `reciterdb-secrets` carries
its values that way today, which is how an API key ended up echoed to a terminal
during this work.

## What still needs a human, by design

- Applying DDL, including the `_org_unit` table.
- Creating and rotating both credentials.
- Granting `DELETE`, if it is ever justified.
- Removing `--dry-run` from the cronjob command.
- Flipping `IA_DELETE_MODE` off `flag`.

## Backstop

Before the first write of any kind, confirm the restore path: that the IA
database is backed up, at what frequency, and that a restore has actually been
tested. Every control above reduces the chance of a bad write; only a restore
undoes one.

## What this does not solve

Read-only access still reads production data, which includes PII. The sensitivity
rule stands regardless of grants: never echo values of `mail`, `mobile`, `phone`,
`dob`, `postalCode`, `street`. Segregation limits damage, not exposure.
