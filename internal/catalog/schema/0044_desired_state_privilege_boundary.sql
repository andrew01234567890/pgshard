-- pgshard_admin is deliberately below superuser on the shards, and the
-- controller applies these tables on every group as a superuser. Three rows
-- crossed that boundary: a membership naming a superuser role, a grant
-- naming a system catalog, and a per-role setting overriding a fence.
--
-- The Go path validates too. This is the half that holds when the row does
-- not come through it.

CREATE FUNCTION pgshard.role_member_is_not_superuser() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    -- The role being GRANTED, not the member receiving it. Checking only
    -- the member left the shortest escalation in the system: insert the
    -- bootstrap superuser into pgshard.roles, then a membership row naming
    -- it, and the controller hands it out on every group.
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = NEW.rolname AND rolsuper) THEN
        RAISE EXCEPTION 'role % is a superuser and may not be granted through pgshard.role_members', quote_literal(NEW.rolname)
            USING ERRCODE = 'check_violation',
                  HINT = 'the controller applies memberships on every group as a superuser';
    END IF;
    -- These carry the ability to read and write server files and to run
    -- programs as the server account, which is superuser by another name.
    IF NEW.rolname = ANY (ARRAY['pg_execute_server_program', 'pg_read_server_files', 'pg_write_server_files']) THEN
        RAISE EXCEPTION 'role % may not be granted through pgshard.role_members', quote_literal(NEW.rolname)
            USING ERRCODE = 'check_violation',
                  HINT = 'this predefined role is equivalent to superuser on the host';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER role_member_is_not_superuser
    BEFORE INSERT OR UPDATE ON pgshard.role_members
    FOR EACH ROW EXECUTE FUNCTION pgshard.role_member_is_not_superuser();

CREATE FUNCTION pgshard.grant_object_is_not_protected() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    s text := lower(btrim(NEW.object_schema));
BEGIN
    -- pg_authid holds every role's SCRAM verifier, which is the cluster's
    -- whole authentication secret. pgshard is the boundary itself: a grant
    -- on these tables would let their own contents widen who may write them.
    IF s = ANY (ARRAY['pg_catalog', 'information_schema', 'pgshard'])
       OR s LIKE 'pg\_toast%' OR s LIKE 'pg\_temp%' THEN
        RAISE EXCEPTION 'schema % may not be named in pgshard.grants', quote_literal(NEW.object_schema)
            USING ERRCODE = 'check_violation',
                  HINT = 'the controller applies grants on every group as a superuser';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER grant_object_is_not_protected
    BEFORE INSERT OR UPDATE ON pgshard.grants
    FOR EACH ROW EXECUTE FUNCTION pgshard.grant_object_is_not_protected();

CREATE FUNCTION pgshard.role_setting_is_not_protected() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    n text := lower(btrim(NEW.name));
BEGIN
    -- A per-role setting overrides the cluster-wide one, so this is how a
    -- role keeps writing through the write pause that cutover, rollback and
    -- barrier restore points are built on. The pgshard namespace carries the
    -- fences the router and the placement triggers read.
    IF n = ANY (ARRAY['default_transaction_read_only', 'transaction_read_only', 'session_replication_role'])
       OR n LIKE 'pgshard.%' THEN
        RAISE EXCEPTION 'setting % may not be set per role', quote_literal(NEW.name)
            USING ERRCODE = 'check_violation',
                  HINT = 'it overrides a control-plane guarantee for that role alone';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER role_setting_is_not_protected
    BEFORE INSERT OR UPDATE ON pgshard.role_settings
    FOR EACH ROW EXECUTE FUNCTION pgshard.role_setting_is_not_protected();
