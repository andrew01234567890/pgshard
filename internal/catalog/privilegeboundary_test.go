package catalog

import (
	"errors"
	"testing"
)

// TestAGrantMayNotNameASystemCatalog: the privileges being words is not
// enough, because the OBJECT is concatenated into the statement too and the
// controller runs it on every group as a superuser. pg_catalog.pg_authid
// holds every role's SCRAM verifier.
func TestAGrantMayNotNameASystemCatalog(t *testing.T) {
	for _, ok := range []string{"", "public", "app", "reporting"} {
		if err := CheckGrantObject(ok); err != nil {
			t.Errorf("schema %q was refused: %v", ok, err)
		}
	}
	for _, bad := range []string{"pg_catalog", "PG_CATALOG", " information_schema ", "pgshard", "pg_toast", "pg_temp_3"} {
		if err := CheckGrantObject(bad); !errors.Is(err, ErrProtectedSchema) {
			t.Errorf("schema %q reached a GRANT: %v", bad, err)
		}
	}
}

// TestAPerRoleSettingMayNotOverrideAFence: a per-role setting beats the
// cluster-wide one, so default_transaction_read_only=off on one role is a
// way to keep writing through the write pause that cutover, rollback and
// barrier restore points are built on.
func TestAPerRoleSettingMayNotOverrideAFence(t *testing.T) {
	for _, ok := range []string{"work_mem", "statement_timeout", "search_path"} {
		if err := CheckRoleSetting(ok); err != nil {
			t.Errorf("setting %q was refused: %v", ok, err)
		}
	}
	for _, bad := range []string{"default_transaction_read_only", "DEFAULT_TRANSACTION_READ_ONLY", "transaction_read_only", "session_replication_role", "pgshard.maintenance"} {
		if err := CheckRoleSetting(bad); !errors.Is(err, ErrProtectedSetting) {
			t.Errorf("setting %q reached an ALTER ROLE: %v", bad, err)
		}
	}
}
