-- A role setting scoped to a database outlived the database. DROP DATABASE
-- deletes pgshard.databases and cascades to tables and grants, but nothing
-- reached role_settings, and desired-state SQL could name a database that
-- never existed. The next role materialization then issues
-- ALTER ROLE ... IN DATABASE for a database that is not there, which fails
-- the whole pass for that group -- and, since its generation stays stale,
-- every later pass fails the same way. One dead row stops role
-- reconciliation for good.
--
-- A foreign key would be the natural way to say this, but the global scope
-- is spelled '' rather than NULL, and '' is a value a foreign key would
-- insist on finding. Changing that representation means changing the
-- primary key and every query that reads it; these two triggers give the
-- same guarantee where the damage is done.

DELETE FROM pgshard.role_settings s
    WHERE s.database <> ''
      AND NOT EXISTS (SELECT 1 FROM pgshard.databases d WHERE d.name = s.database);

CREATE FUNCTION pgshard.role_setting_database_exists() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF NEW.database <> '' AND NOT EXISTS (SELECT 1 FROM pgshard.databases WHERE name = NEW.database) THEN
        RAISE EXCEPTION 'no database named % for a setting of role %', NEW.database, NEW.rolname
            USING ERRCODE = 'foreign_key_violation',
                  HINT = 'create the database first, or leave database empty for a cluster-wide setting';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER role_setting_database_exists
    BEFORE INSERT OR UPDATE ON pgshard.role_settings
    FOR EACH ROW EXECUTE FUNCTION pgshard.role_setting_database_exists();

CREATE FUNCTION pgshard.drop_role_settings_of_database() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    DELETE FROM pgshard.role_settings WHERE database = OLD.name;
    RETURN OLD;
END
$$;

CREATE TRIGGER drop_role_settings
    AFTER DELETE ON pgshard.databases
    FOR EACH ROW EXECUTE FUNCTION pgshard.drop_role_settings_of_database();
