-- A grant's privileges are rendered into GRANT statements by concatenation,
-- and pgshard_admin -- deliberately below superuser -- may write this table
-- directly. An element that is not a privilege word is therefore a way to
-- put arbitrary SQL into a statement the controller runs on every shard as
-- the administration role.
--
-- The Go path validates too. This is the half that holds when the row does
-- not come through it.
CREATE FUNCTION pgshard.grant_privileges_are_words() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    p    text;
    kind text := lower(btrim(NEW.object_kind));
    ok   text[];
BEGIN
    ok := CASE kind
        WHEN 'table'    THEN ARRAY['ALL','SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER','MAINTAIN']
        WHEN 'sequence' THEN ARRAY['ALL','SELECT','UPDATE','USAGE']
        WHEN 'schema'   THEN ARRAY['ALL','CREATE','USAGE']
        WHEN 'database' THEN ARRAY['ALL','CONNECT','CREATE','TEMPORARY','TEMP']
        WHEN 'function' THEN ARRAY['ALL','EXECUTE']
        WHEN 'parameter' THEN ARRAY['ALL','SET','ALTER SYSTEM']
        WHEN 'large object' THEN ARRAY['ALL','SELECT','UPDATE']
        WHEN 'tablespace' THEN ARRAY['ALL','CREATE']
        ELSE ARRAY['ALL','USAGE']
    END;
    -- A column grant renders one clause per privilege, and only the column
    -- privileges make sense there.
    IF NEW.column_name <> '' THEN
        ok := ARRAY['ALL','SELECT','INSERT','UPDATE','REFERENCES'];
    END IF;
    FOREACH p IN ARRAY NEW.privileges LOOP
        IF upper(btrim(p)) <> ALL (ok) THEN
            RAISE EXCEPTION 'privilege % is not a % privilege', quote_literal(p), kind
                USING ERRCODE = 'check_violation',
                      HINT = 'privileges are single words rendered into a GRANT statement; see pgshard.grants';
        END IF;
    END LOOP;
    RETURN NEW;
END
$$;

CREATE TRIGGER grant_privileges_are_words
    BEFORE INSERT OR UPDATE ON pgshard.grants
    FOR EACH ROW EXECUTE FUNCTION pgshard.grant_privileges_are_words();
