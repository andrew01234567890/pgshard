-- A barrier reserves its name before touching any group: the row is inserted
-- uncertified first and certified only when every group's restore point is
-- archived. A failed attempt keeps the row (with its error) so the same
-- physical WAL restore-point name can never be created twice, which would
-- make name-based recovery ambiguous across groups.

SET LOCAL ROLE pgshard_system;

ALTER TABLE pgshard.restore_points
    ADD COLUMN attempt_error text NOT NULL DEFAULT '';

RESET ROLE;
