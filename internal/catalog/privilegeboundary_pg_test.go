package catalog

import (
	"context"
	"strings"
	"testing"
)

// TestTheDesiredStateRefusesAnEscalationRow: pgshard_admin is deliberately
// below superuser on the shards, and the controller applies these tables on
// every group AS a superuser. Three row shapes crossed that boundary, and
// the shortest of them -- a membership naming the bootstrap superuser --
// made any pgshard_admin holder a superuser everywhere. The Go path refuses
// them too; this is the half that holds when the row does not come through
// it.
func TestTheDesiredStateRefusesAnEscalationRow(t *testing.T) {
	requireDocker(t)
	ctx := context.Background()
	conn := connect(t, startPostgres(t, candidateImages[0]))
	if err := Migrate(ctx, conn); err != nil {
		t.Fatal(err)
	}
	exec := func(sql string, args ...any) error {
		_, err := conn.Exec(ctx, sql, args...)
		return err
	}
	// The superuser this container runs as, whatever it is called.
	var super string
	if err := conn.QueryRow(ctx, `SELECT rolname FROM pg_roles WHERE rolsuper ORDER BY oid LIMIT 1`).Scan(&super); err != nil {
		t.Fatal(err)
	}
	if err := exec(`INSERT INTO pgshard.databases (name) VALUES ('app') ON CONFLICT DO NOTHING`); err != nil {
		t.Fatalf("seeding the database a grant refers to: %v", err)
	}
	for _, r := range []string{super, "attacker"} {
		if err := exec(`INSERT INTO pgshard.roles (rolname) VALUES ($1) ON CONFLICT DO NOTHING`, r); err != nil {
			t.Fatalf("seeding %s: %v", r, err)
		}
	}

	if err := exec(`INSERT INTO pgshard.role_members (rolname, member) VALUES ($1, 'attacker')`, super); err == nil {
		t.Fatal("a membership granting the bootstrap superuser was accepted; the controller would apply it on every group")
	} else if !strings.Contains(err.Error(), "superuser") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}

	if err := exec(`INSERT INTO pgshard.role_members (rolname, member) VALUES ('pg_execute_server_program', 'attacker')`); err == nil {
		t.Fatal("pg_execute_server_program is superuser by another name and was accepted")
	}

	if err := exec(`INSERT INTO pgshard.grants (rolname, database, object_kind, object_schema, object_name, privileges)
	                VALUES ('attacker', 'app', 'table', 'pg_catalog', 'pg_authid', '{SELECT}')`); err == nil {
		t.Fatal("a grant on pg_authid was accepted; it leaks every role's SCRAM verifier")
	} else if !strings.Contains(err.Error(), "pg_catalog") {
		// Without a databases row this used to fail on the foreign key
		// instead, which passes the assertion while proving nothing.
		t.Fatalf("refused for the wrong reason: %v", err)
	}

	if err := exec(`INSERT INTO pgshard.role_settings (rolname, database, name, value)
	                VALUES ('attacker', '', 'default_transaction_read_only', 'off')`); err == nil {
		t.Fatal("a per-role override of the write pause was accepted")
	} else if !strings.Contains(err.Error(), "default_transaction_read_only") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}

	// The boundary must not swallow the ordinary rows it exists to carry.
	if err := exec(`INSERT INTO pgshard.role_members (rolname, member) VALUES ('attacker', 'attacker') ON CONFLICT DO NOTHING`); err != nil {
		t.Fatalf("an ordinary membership was refused: %v", err)
	}
	if err := exec(`INSERT INTO pgshard.grants (rolname, database, object_kind, object_schema, object_name, privileges)
	                VALUES ('attacker', 'app', 'table', 'public', 'orders', '{SELECT}')`); err != nil {
		t.Fatalf("an ordinary grant was refused: %v", err)
	}
	if err := exec(`INSERT INTO pgshard.role_settings (rolname, database, name, value)
	                VALUES ('attacker', '', 'work_mem', '64MB')`); err != nil {
		t.Fatalf("an ordinary role setting was refused: %v", err)
	}
}
