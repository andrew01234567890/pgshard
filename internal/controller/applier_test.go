package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// memStore keeps migrations in memory.
type memStore struct {
	mu         sync.Mutex
	migrations []catalog.DDLMigration
	shards     []int32
	execs      []string
	saves      int
}

func (s *memStore) Pending(context.Context) ([]catalog.DDLMigration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []catalog.DDLMigration
	for _, m := range s.migrations {
		if m.State == catalog.MigrationQueued || m.State == catalog.MigrationRunning {
			out = append(out, cloneMigration(m))
		}
	}
	return out, nil
}

func cloneMigration(m catalog.DDLMigration) catalog.DDLMigration {
	per := map[string]catalog.ShardMigration{}
	for k, v := range m.PerShard {
		per[k] = v
	}
	m.PerShard = per
	return m
}

func (s *memStore) Save(_ context.Context, m catalog.DDLMigration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	for i := range s.migrations {
		if s.migrations[i].ID == m.ID {
			s.migrations[i] = cloneMigration(m)
			return nil
		}
	}
	return fmt.Errorf("unknown migration %s", m.ID)
}

func (s *memStore) Shards(context.Context, string) ([]int32, error) { return s.shards, nil }

func (s *memStore) Exec(_ context.Context, sql string, args ...any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.execs = append(s.execs, fmt.Sprintf("%s %v", strings.Join(strings.Fields(sql), " "), args))
	return nil
}

func (s *memStore) get(t *testing.T, id string) catalog.DDLMigration {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.migrations {
		if m.ID == id {
			return cloneMigration(m)
		}
	}
	t.Fatalf("migration %s missing", id)
	return catalog.DDLMigration{}
}

// fakeShards scripts every shard: exec decides the outcome of a statement,
// exists answers object checks, invalid answers invalid-index checks.
type fakeShards struct {
	mu       sync.Mutex
	ran      map[int32][]string
	super    map[int32][]string
	dbs      map[int32][]string
	logins   map[int32][]string
	exec     func(shard int32, sql string) error
	exists   func(shard int32, kind, name string) bool
	invalid  func(shard int32, name string) bool
	rolsuper func(shard int32, name string) bool
	check    func(shard int32, kind, table, name string) bool
	dialErr  func(shard int32) error
}

func newFakeShards() *fakeShards {
	return &fakeShards{ran: map[int32][]string{}, super: map[int32][]string{}, dbs: map[int32][]string{}, logins: map[int32][]string{}}
}

func (f *fakeShards) DialDatabase(_ context.Context, _ string, id int32, _ string) (ShardConn, error) {
	if f.dialErr != nil {
		if err := f.dialErr(id); err != nil {
			return nil, err
		}
	}
	return &fakeConn{f: f, id: id, superuser: true}, nil
}

func (f *fakeShards) DialDatabaseAs(_ context.Context, _ string, id int32, db, user, password string) (ShardConn, error) {
	f.mu.Lock()
	f.dbs[id] = append(f.dbs[id], db)
	f.logins[id] = append(f.logins[id], user+"/"+password)
	f.mu.Unlock()
	if f.dialErr != nil {
		if err := f.dialErr(id); err != nil {
			return nil, err
		}
	}
	return &fakeConn{f: f, id: id}, nil
}

// statements lists what ran on the DDL session of a shard.
func (f *fakeShards) statements(id int32) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ran[id]...)
}

// superuserStatements lists what ran on the superuser session of a shard.
func (f *fakeShards) superuserStatements(id int32) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.super[id]...)
}

type fakeConn struct {
	f         *fakeShards
	id        int32
	superuser bool
}

func (c *fakeConn) Exec(_ context.Context, sql string, _ ...any) (pgconnTag, error) {
	c.f.mu.Lock()
	if c.superuser {
		c.f.super[c.id] = append(c.f.super[c.id], sql)
	} else {
		c.f.ran[c.id] = append(c.f.ran[c.id], sql)
	}
	exec := c.f.exec
	c.f.mu.Unlock()
	if c.superuser {
		return pgconn.CommandTag{}, nil
	}
	switch {
	case sql == "BEGIN", sql == "COMMIT", sql == "ROLLBACK", strings.HasPrefix(sql, "SET lock_timeout"), strings.HasPrefix(sql, "SET ROLE"):
		return pgconn.CommandTag{}, nil
	}
	if exec != nil {
		if err := exec(c.id, sql); err != nil {
			return pgconn.CommandTag{}, err
		}
	}
	return pgconn.CommandTag{}, nil
}

func (c *fakeConn) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	name, _ := args[0].(string)
	if strings.Contains(sql, "WHERE i.indisvalid") {
		c.f.mu.Lock()
		c.f.ran[c.id] = append(c.f.ran[c.id], "check index_valid "+name)
		c.f.mu.Unlock()
		v := c.f.check != nil && c.f.check(c.id, "index_valid", "", name)
		return &boolRows{vals: []bool{v}}, nil
	}
	if len(args) == 3 {
		table, _ := args[0].(string)
		obj, _ := args[2].(string)
		c.f.mu.Lock()
		c.f.ran[c.id] = append(c.f.ran[c.id], "check "+checkKind(sql)+" "+table+" "+obj)
		c.f.mu.Unlock()
		v := c.f.check != nil && c.f.check(c.id, checkKind(sql), table, obj)
		return &boolRows{vals: []bool{v}}, nil
	}
	switch {
	case strings.Contains(sql, "rolsuper"):
		v := c.f.rolsuper != nil && c.f.rolsuper(c.id, name)
		return &boolRows{vals: []bool{v}}, nil
	case strings.Contains(sql, "indisvalid"):
		v := c.f.invalid != nil && c.f.invalid(c.id, name)
		return &boolRows{vals: []bool{v}}, nil
	case strings.Contains(sql, "pg_class"), strings.Contains(sql, "pg_namespace"), strings.Contains(sql, "pg_roles"),
		strings.Contains(sql, "pg_database"), strings.Contains(sql, "pg_type"):
		kind := "relation"
		switch {
		case strings.Contains(sql, "pg_roles"):
			kind = "role"
		case strings.Contains(sql, "pg_database"):
			kind = "database"
		case strings.Contains(sql, "FROM pg_namespace"):
			kind = "schema"
		}
		v := c.f.exists != nil && c.f.exists(c.id, kind, name)
		return &boolRows{vals: []bool{v}}, nil
	}
	return nil, fmt.Errorf("unexpected query %q", sql)
}

func (c *fakeConn) Close(context.Context) error { return nil }

// checkKind names a step-check query by its shape.
func checkKind(sql string) string {
	switch {
	case strings.Contains(sql, "contype = 'n' AND k.convalidated"):
		return "notnull_valid"
	case strings.Contains(sql, "contype = 'n'"):
		return "notnull"
	case strings.Contains(sql, "conname = $3 AND convalidated"):
		return "constraint_valid"
	case strings.Contains(sql, "conname = $3"):
		return "constraint"
	case strings.Contains(sql, "inhdetachpending"):
		return "detach_pending"
	case strings.Contains(sql, "pg_inherits"):
		return "detached"
	}
	return "?"
}

// boolRows is a one-column pgx.Rows of booleans.
type boolRows struct {
	vals []bool
	i    int
}

func (r *boolRows) Close()                                       {}
func (r *boolRows) Err() error                                   { return nil }
func (r *boolRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *boolRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *boolRows) Next() bool                                   { r.i++; return r.i <= len(r.vals) }
func (r *boolRows) Scan(dest ...any) error {
	*(dest[0].(*bool)) = r.vals[r.i-1]
	return nil
}
func (r *boolRows) Values() ([]any, error) { return []any{r.vals[r.i-1]}, nil }
func (r *boolRows) RawValues() [][]byte    { return nil }
func (r *boolRows) Conn() *pgx.Conn        { return nil }

func pgErr(code, msg string) error {
	return &pgconn.PgError{Severity: "ERROR", Code: code, Message: msg}
}

type applierFixture struct {
	store  *memStore
	shards *fakeShards
	app    *Applier
	sleeps []time.Duration
	clock  time.Time
}

func newApplierFixture(t *testing.T) *applierFixture {
	t.Helper()
	f := &applierFixture{store: &memStore{shards: []int32{0, 1, 2}}, shards: newFakeShards(), clock: time.Unix(1_700_000_000, 0)}
	f.app = &Applier{Store: f.store, Shards: f.shards, Logger: slog.New(slog.DiscardHandler),
		Backoff: Backoff{Min: 500 * time.Millisecond, Max: 4 * time.Second, Total: 30 * time.Second},
		Sleep: func(_ context.Context, d time.Duration) error {
			f.sleeps = append(f.sleeps, d)
			f.clock = f.clock.Add(d)
			return nil
		},
		Now: func() time.Time { return f.clock }}
	return f
}

func (f *applierFixture) queue(m catalog.DDLMigration) string {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	m.ID = fmt.Sprintf("m%d", len(f.store.migrations)+1)
	if m.State == "" {
		m.State = catalog.MigrationQueued
	}
	if m.Strategy == "" {
		m.Strategy = "direct"
	}
	if m.Database == "" {
		m.Database = "app"
	}
	f.store.migrations = append(f.store.migrations, m)
	return m.ID
}

func (f *applierFixture) run(t *testing.T) {
	t.Helper()
	if _, err := f.app.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func states(m catalog.DDLMigration) string {
	var parts []string
	for _, k := range sortedShardKeys(m.PerShard) {
		s := m.PerShard[k]
		parts = append(parts, fmt.Sprintf("%s=%s/%d", k, s.State, s.Attempts))
	}
	return strings.Join(parts, " ")
}

func TestApplierRunsOnEveryTargetInsideATransaction(t *testing.T) {
	f := newApplierFixture(t)
	all := f.queue(catalog.DDLMigration{Statement: "create table t (id int)", Kind: "CREATE TABLE", Scope: "all",
		Meta: catalog.MigrationMeta{RunAs: "app", Object: catalog.MigrationObject{Kind: "relation", Name: "t", Expect: "present"}}})
	home := f.queue(catalog.DDLMigration{Statement: "create table u (id int)", Kind: "CREATE TABLE", Scope: "home", HomeShard: 1})
	f.run(t)
	m := f.store.get(t, all)
	if m.State != catalog.MigrationComplete || states(m) != "0=applied/1 1=applied/1 2=applied/1" {
		t.Fatalf("all: %s %s", m.State, states(m))
	}
	for id := int32(0); id < 3; id++ {
		got := strings.Join(f.shards.statements(id), ";")
		if !strings.Contains(got, "SET lock_timeout = '2000ms';SET ROLE \"app\";BEGIN;create table t (id int);COMMIT") {
			t.Fatalf("shard %d ran %q", id, got)
		}
		if f.shards.dbs[id][0] != "app" {
			t.Fatalf("shard %d dialed database %q", id, f.shards.dbs[id][0])
		}
	}
	m = f.store.get(t, home)
	if m.State != catalog.MigrationComplete || states(m) != "1=applied/1" {
		t.Fatalf("home: %s %s", m.State, states(m))
	}
	if n := len(f.shards.statements(0)); n != 5 {
		t.Fatalf("home-scoped DDL reached shard 0: %v", f.shards.statements(0))
	}
	if len(f.sleeps) != 0 {
		t.Fatalf("slept %v", f.sleeps)
	}
}

func TestApplierRetriesLockTimeoutsWithBackoff(t *testing.T) {
	f := newApplierFixture(t)
	fails := 3
	f.shards.exec = func(shard int32, _ string) error {
		if shard == 1 && fails > 0 {
			fails--
			return pgErr("55P03", "canceling statement due to lock timeout")
		}
		return nil
	}
	id := f.queue(catalog.DDLMigration{Statement: "alter table t add column x int", Kind: "ALTER TABLE", Scope: "all"})
	f.run(t)
	m := f.store.get(t, id)
	if m.State != catalog.MigrationComplete || states(m) != "0=applied/1 1=applied/4 2=applied/1" {
		t.Fatalf("%s %s %+v", m.State, states(m), m.PerShard)
	}
	if fmt.Sprint(f.sleeps) != "[500ms 1s 2s]" {
		t.Fatalf("backoff %v", f.sleeps)
	}
	if s := f.shards.statements(1); strings.Count(strings.Join(s, ";"), "ROLLBACK") != 3 {
		t.Fatalf("shard 1 ran %v", s)
	}
	if m.PerShard["1"].Error != "" {
		t.Fatalf("applied shard keeps error %q", m.PerShard["1"].Error)
	}
}

func TestApplierGivesUpAfterTheBackoffBudget(t *testing.T) {
	f := newApplierFixture(t)
	f.shards.exec = func(shard int32, _ string) error {
		if shard == 0 {
			return pgErr("55P03", "canceling statement due to lock timeout")
		}
		return nil
	}
	id := f.queue(catalog.DDLMigration{Statement: "alter table t add column x int", Kind: "ALTER TABLE", Scope: "all"})
	f.run(t)
	m := f.store.get(t, id)
	if m.State != catalog.MigrationFailed || m.PerShard["0"].State != catalog.ShardFailed || m.PerShard["0"].SQLState != "55P03" {
		t.Fatalf("%s %s %+v", m.State, states(m), m.PerShard)
	}
	// 0.5+1+2+4+4+4+4+4+4 = 27.5s is still inside the 30s budget; the
	// attempt after the next 4s wait is the last.
	if fmt.Sprint(f.sleeps) != "[500ms 1s 2s 4s 4s 4s 4s 4s 4s 4s]" || m.PerShard["0"].Attempts != 11 {
		t.Fatalf("backoff %v attempts %d", f.sleeps, m.PerShard["0"].Attempts)
	}
	if !strings.HasPrefix(m.Error, "shard 0: ") {
		t.Fatalf("error %q", m.Error)
	}
	if states(m) != "0=failed/11 1=applied/1 2=applied/1" {
		t.Fatalf("other shards must still be applied: %s", states(m))
	}
}

func TestApplierRetriesUnreachableShards(t *testing.T) {
	f := newApplierFixture(t)
	down := 2
	f.shards.dialErr = func(shard int32) error {
		if shard == 2 && down > 0 {
			down--
			return errors.New("connection refused")
		}
		return nil
	}
	id := f.queue(catalog.DDLMigration{Statement: "create schema s", Kind: "CREATE SCHEMA", Scope: "all"})
	f.run(t)
	if m := f.store.get(t, id); m.State != catalog.MigrationComplete || m.PerShard["2"].Attempts != 3 {
		t.Fatalf("%s %s", m.State, states(m))
	}
}

func TestApplierHardFailureLeavesOtherShardsApplied(t *testing.T) {
	f := newApplierFixture(t)
	f.shards.exec = func(shard int32, _ string) error {
		if shard == 1 {
			return pgErr("42P07", `relation "t" already exists`)
		}
		return nil
	}
	id := f.queue(catalog.DDLMigration{Statement: "create table t (id int)", Kind: "CREATE TABLE", Scope: "all",
		Meta: catalog.MigrationMeta{Object: catalog.MigrationObject{Kind: "relation", Name: "t", Expect: "present"}}})
	f.run(t)
	m := f.store.get(t, id)
	if m.State != catalog.MigrationFailed || states(m) != "0=applied/1 1=failed/1 2=applied/1" {
		t.Fatalf("%s %s", m.State, states(m))
	}
	if s := m.PerShard["1"]; s.SQLState != "42P07" || !strings.Contains(s.Error, "already exists") {
		t.Fatalf("shard 1: %+v", s)
	}
	if m.Error != `shard 1: ERROR: relation "t" already exists (SQLSTATE 42P07)` {
		t.Fatalf("error %q", m.Error)
	}
	if len(f.sleeps) != 0 {
		t.Fatalf("hard failures must not be retried: %v", f.sleeps)
	}
	f.run(t)
	if m2 := f.store.get(t, id); m2.PerShard["1"].Attempts != 1 {
		t.Fatal("a failed migration was driven again")
	}
}

func TestApplierResumesARunningMigrationIdempotently(t *testing.T) {
	f := newApplierFixture(t)
	f.shards.exists = func(shard int32, kind, name string) bool { return shard == 1 && kind == "relation" && name == "t" }
	id := f.queue(catalog.DDLMigration{Statement: "create table t (id int)", Kind: "CREATE TABLE", Scope: "all", State: catalog.MigrationRunning,
		Meta: catalog.MigrationMeta{Object: catalog.MigrationObject{Kind: "relation", Name: "t", Expect: "present"}},
		PerShard: map[string]catalog.ShardMigration{
			"0": {State: catalog.ShardApplied, Attempts: 1},
			"1": {State: catalog.ShardRunning, Attempts: 1},
			"2": {State: catalog.ShardPending},
		}})
	f.run(t)
	m := f.store.get(t, id)
	if m.State != catalog.MigrationComplete || states(m) != "0=applied/1 1=applied/2 2=applied/1" {
		t.Fatalf("%s %s", m.State, states(m))
	}
	if s := f.shards.statements(0); len(s) != 0 {
		t.Fatalf("applied shard 0 was touched: %v", s)
	}
	if s := strings.Join(f.shards.statements(1), ";"); strings.Contains(s, "create table") {
		t.Fatalf("shard 1 already had the table but ran %q", s)
	}
	if s := strings.Join(f.shards.statements(2), ";"); !strings.Contains(s, "create table") {
		t.Fatalf("shard 2 ran %q", s)
	}
	// A pending shard is never guarded by the existence check: an object
	// created out of band is a hard failure, not a silent success.
	f.shards.exists = func(int32, string, string) bool { return true }
	f.shards.exec = func(_ int32, _ string) error { return pgErr("42P07", "exists") }
	id2 := f.queue(catalog.DDLMigration{Statement: "create table t (id int)", Kind: "CREATE TABLE", Scope: "home",
		Meta: catalog.MigrationMeta{Object: catalog.MigrationObject{Kind: "relation", Name: "t", Expect: "present"}}})
	f.run(t)
	if m := f.store.get(t, id2); m.State != catalog.MigrationFailed {
		t.Fatalf("out-of-band duplicate: %s %s", m.State, states(m))
	}
}

func TestApplierSkipsShardsWithoutTheObjectUnderExistingScope(t *testing.T) {
	f := newApplierFixture(t)
	f.shards.exec = func(shard int32, _ string) error {
		if shard != 0 {
			return pgErr("42704", `index "i" does not exist`)
		}
		return nil
	}
	id := f.queue(catalog.DDLMigration{Statement: "drop index i", Kind: "DROP INDEX", Scope: "existing"})
	f.run(t)
	m := f.store.get(t, id)
	if m.State != catalog.MigrationComplete || states(m) != "0=applied/1 1=skipped/1 2=skipped/1" {
		t.Fatalf("%s %s", m.State, states(m))
	}
	if !strings.Contains(m.PerShard["1"].Error, "does not exist") {
		t.Fatalf("skipped shard keeps the reason: %+v", m.PerShard["1"])
	}
	f.shards.exec = func(int32, string) error { return pgErr("42704", `index "j" does not exist`) }
	id = f.queue(catalog.DDLMigration{Statement: "drop index j", Kind: "DROP INDEX", Scope: "existing"})
	f.run(t)
	m = f.store.get(t, id)
	if m.State != catalog.MigrationFailed || m.PerShard["0"].State != catalog.ShardFailed || !strings.Contains(m.Error, "does not exist") {
		t.Fatalf("missing everywhere: %s %s %q", m.State, states(m), m.Error)
	}
}

func TestApplierRebuildsAnInvalidConcurrentIndexOnce(t *testing.T) {
	f := newApplierFixture(t)
	first := true
	f.shards.exec = func(shard int32, sql string) error {
		if shard == 0 && strings.HasPrefix(sql, "create index concurrently") && first {
			first = false
			return pgErr("23505", "duplicate key")
		}
		return nil
	}
	f.shards.invalid = func(shard int32, name string) bool { return shard == 0 && name == "i" }
	id := f.queue(catalog.DDLMigration{Statement: "create index concurrently i on t (x)", Kind: "CREATE INDEX", Scope: "all", Strategy: "concurrent",
		Meta: catalog.MigrationMeta{Object: catalog.MigrationObject{Kind: "relation", Name: "i", Expect: "present"}}})
	f.run(t)
	m := f.store.get(t, id)
	if m.State != catalog.MigrationComplete || states(m) != "0=applied/1 1=applied/1 2=applied/1" {
		t.Fatalf("%s %s %+v", m.State, states(m), m.PerShard)
	}
	got := strings.Join(f.shards.statements(0), ";")
	want := "SET lock_timeout = '2000ms';create index concurrently i on t (x);DROP INDEX CONCURRENTLY IF EXISTS \"i\";create index concurrently i on t (x)"
	if got != want {
		t.Fatalf("shard 0 ran %q", got)
	}
	if strings.Contains(strings.Join(f.shards.statements(1), ";"), "BEGIN") {
		t.Fatal("CONCURRENTLY ran inside a transaction")
	}
	// A second failure is final.
	f.shards.exec = func(_ int32, _ string) error { return pgErr("23505", "duplicate key") }
	id = f.queue(catalog.DDLMigration{Statement: "create index concurrently i on t (x)", Kind: "CREATE INDEX", Scope: "home", Strategy: "concurrent",
		Meta: catalog.MigrationMeta{Object: catalog.MigrationObject{Kind: "relation", Name: "i", Expect: "present"}}})
	f.run(t)
	if m := f.store.get(t, id); m.State != catalog.MigrationFailed || m.PerShard["0"].SQLState != "23505" {
		t.Fatalf("%s %+v", m.State, m.PerShard)
	}
}

func TestApplierMirrorsRolesAndDatabases(t *testing.T) {
	f := newApplierFixture(t)
	f.queue(catalog.DDLMigration{Statement: "create role r password 'SCRAM-SHA-256$x'", Kind: "CREATE ROLE", Scope: "all",
		Meta: catalog.MigrationMeta{Role: "r", RoleOp: "create", Verifier: "SCRAM-SHA-256$x", Object: catalog.MigrationObject{Kind: "role", Name: "r", Expect: "present"}}})
	f.queue(catalog.DDLMigration{Statement: "alter role r login", Kind: "ALTER ROLE", Scope: "all", Meta: catalog.MigrationMeta{Role: "r", RoleOp: "alter"}})
	f.queue(catalog.DDLMigration{Statement: "create database d", Kind: "CREATE DATABASE", Scope: "all",
		Meta: catalog.MigrationMeta{RunAs: "app", Database: "d", DatabaseOp: "create", Object: catalog.MigrationObject{Kind: "database", Name: "d", Expect: "present"}}})
	f.queue(catalog.DDLMigration{Statement: "drop database d", Kind: "DROP DATABASE", Scope: "all",
		Meta: catalog.MigrationMeta{Database: "d", DatabaseOp: "drop", Object: catalog.MigrationObject{Kind: "database", Name: "d", Expect: "absent"}}})
	f.queue(catalog.DDLMigration{Statement: "drop role r", Kind: "DROP ROLE", Scope: "all",
		Meta: catalog.MigrationMeta{Role: "r", RoleOp: "drop", Object: catalog.MigrationObject{Kind: "role", Name: "r", Expect: "absent"}}})
	f.run(t)
	got := strings.Join(f.store.execs, "\n")
	for _, want := range []string{
		"INSERT INTO pgshard.roles (rolname, verifier, login, createdb, createrole, inherit, connection_limit, valid_until) VALUES ($1, nullif($2, ''), coalesce($3, true),",
		"[r SCRAM-SHA-256$x <nil> <nil> <nil> <nil> <nil> <nil>]",
		"INSERT INTO pgshard.databases (name) VALUES ($1) ON CONFLICT (name) DO NOTHING [d]",
		"DELETE FROM pgshard.databases WHERE name = $1 [d]",
		"DELETE FROM pgshard.roles WHERE rolname = $1 [r]",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("catalog statements:\n%s\nmissing %q", got, want)
		}
	}
	if len(f.store.execs) != 4 {
		t.Fatalf("ALTER ROLE without a password must not touch the catalog: %v", f.store.execs)
	}
	for id := int32(0); id < 3; id++ {
		for _, db := range f.shards.dbs[id] {
			if db != "" {
				t.Fatalf("role/database DDL dialed database %q on shard %d", db, id)
			}
		}
	}
	if got := strings.Join(f.shards.statements(0), ";"); !strings.Contains(got, "SET ROLE \"app\";create database d;SET lock_timeout = '2000ms';drop database d") {
		t.Fatalf("CREATE DATABASE must run outside a transaction as the client role: %q", got)
	}
}

func TestApplierRunsMigrationsOldestFirstAndFailedOnesNeverAgain(t *testing.T) {
	f := newApplierFixture(t)
	a := f.queue(catalog.DDLMigration{Statement: "create table a (id int)", Kind: "CREATE TABLE", Scope: "home"})
	b := f.queue(catalog.DDLMigration{Statement: "create table b (id int)", Kind: "CREATE TABLE", Scope: "home"})
	f.run(t)
	if s := f.shards.statements(0); !strings.Contains(strings.Join(s, ";"), "create table a (id int);COMMIT;SET lock_timeout = '2000ms';BEGIN;create table b") {
		t.Fatalf("order %v", s)
	}
	for _, id := range []string{a, b} {
		if f.store.get(t, id).State != catalog.MigrationComplete {
			t.Fatal(id)
		}
	}
	saves := f.store.saves
	f.run(t)
	if f.store.saves != saves {
		t.Fatal("finished migrations were saved again")
	}
}

func TestApplierStopsWhenTheContextEnds(t *testing.T) {
	f := newApplierFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	f.app.Sleep = func(context.Context, time.Duration) error { cancel(); return ctx.Err() }
	f.shards.exec = func(int32, string) error { return pgErr("55P03", "lock timeout") }
	id := f.queue(catalog.DDLMigration{Statement: "alter table t add column x int", Kind: "ALTER TABLE", Scope: "all"})
	if _, err := f.app.RunOnce(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
	m := f.store.get(t, id)
	if m.State != catalog.MigrationRunning || m.PerShard["0"].State != catalog.ShardRetrying {
		t.Fatalf("a cancelled pass must leave the migration resumable: %s %s", m.State, states(m))
	}
}

func TestApplierRunsClientStatementsOnANonSuperuserSession(t *testing.T) {
	f := newApplierFixture(t)
	id := f.queue(catalog.DDLMigration{Statement: "create table t (id int)", Kind: "CREATE TABLE", Scope: "home", HomeShard: 1,
		Meta: catalog.MigrationMeta{RunAs: "app"}})
	f.run(t)
	if m := f.store.get(t, id); m.State != catalog.MigrationComplete {
		t.Fatalf("%s %s", m.State, states(m))
	}
	super := strings.Join(f.shards.superuserStatements(1), ";")
	for _, want := range []string{
		`CREATE ROLE "pgshard_ddl" LOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION`,
		`ALTER ROLE "pgshard_ddl" LOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOBYPASSRLS NOREPLICATION PASSWORD '`,
		`FROM pg_auth_members WHERE member = 'pgshard_ddl'::regrole`,
		`GRANT "app" TO "pgshard_ddl" WITH SET TRUE, INHERIT FALSE`,
		`REVOKE "app" FROM "pgshard_ddl"`,
	} {
		if !strings.Contains(super, want) {
			t.Fatalf("superuser session ran %q, want %q", super, want)
		}
	}
	if strings.Contains(super, "create table") {
		t.Fatalf("client DDL ran on the superuser session: %q", super)
	}
	login := f.shards.logins[1][0]
	if !strings.HasPrefix(login, "pgshard_ddl/") || len(login) < len("pgshard_ddl/")+32 {
		t.Fatalf("DDL session logged in as %q", login)
	}
	if got := strings.Join(f.shards.statements(1), ";"); !strings.Contains(got, `SET ROLE "app";BEGIN;create table t (id int);COMMIT`) {
		t.Fatalf("DDL session ran %q", got)
	}
	// The role is provisioned once per shard; membership is granted on every step.
	f.queue(catalog.DDLMigration{Statement: "create table u (id int)", Kind: "CREATE TABLE", Scope: "home", HomeShard: 1,
		Meta: catalog.MigrationMeta{RunAs: "app"}})
	f.run(t)
	if n := strings.Count(strings.Join(f.shards.superuserStatements(1), ";"), "CREATE ROLE"); n != 1 {
		t.Fatalf("role provisioned %d times", n)
	}
	if f.shards.logins[1][0] != f.shards.logins[1][1] {
		t.Fatalf("password changed between steps: %v", f.shards.logins[1])
	}

	f.shards.rolsuper = func(_ int32, name string) bool { return name == "postgres" }
	id = f.queue(catalog.DDLMigration{Statement: "create table v (id int)", Kind: "CREATE TABLE", Scope: "home", HomeShard: 1,
		Meta: catalog.MigrationMeta{RunAs: "postgres"}})
	f.run(t)
	m := f.store.get(t, id)
	if m.State != catalog.MigrationFailed || m.PerShard["1"].SQLState != "42501" {
		t.Fatalf("superuser client: %s %+v", m.State, m.PerShard)
	}
	if strings.Contains(strings.Join(f.shards.superuserStatements(1), ";"), `GRANT "postgres"`) {
		t.Fatal("a superuser was granted to the DDL role")
	}
}

// membershipBalanced reports that every GRANT of a role to pgshard_ddl on
// the superuser session was followed by its REVOKE, and returns how many.
func membershipBalanced(stmts []string, role string) (int, bool) {
	grant, revoke := `GRANT "`+role+`" TO "pgshard_ddl" WITH SET TRUE, INHERIT FALSE`, `REVOKE "`+role+`" FROM "pgshard_ddl"`
	held, n := false, 0
	for _, st := range stmts {
		switch st {
		case grant:
			if held {
				return n, false
			}
			held = true
			n++
		case revoke:
			if !held {
				return n, false
			}
			held = false
		}
	}
	return n, !held
}

func TestApplierRevokesTheMembershipAfterEveryShardStep(t *testing.T) {
	f := newApplierFixture(t)
	f.shards.exec = func(shard int32, _ string) error {
		if shard == 2 {
			return pgErr("42P07", "relation exists")
		}
		return nil
	}
	steps := []catalog.MigrationStep{{SQL: "alter table t add column x int"}, {SQL: "alter table t add column y int"}}
	id := f.queue(catalog.DDLMigration{Statement: "alter table t add column x int, add column y int", Kind: "ALTER TABLE", Scope: "all", Strategy: "multistep",
		Meta: catalog.MigrationMeta{RunAs: "app", Steps: steps}})
	f.run(t)
	m := f.store.get(t, id)
	if m.State != catalog.MigrationFailed || m.PerShard["2"].State != catalog.ShardFailed || m.PerShard["0"].State != catalog.ShardApplied {
		t.Fatalf("%s %s", m.State, states(m))
	}
	for shard, want := range map[int32]int{0: 2, 1: 2, 2: 1} {
		n, ok := membershipBalanced(f.shards.superuserStatements(shard), "app")
		if !ok || n != want {
			t.Fatalf("shard %d: %d grants, balanced=%v: %q", shard, n, ok, f.shards.superuserStatements(shard))
		}
	}

	f.shards.exec = nil
	id = f.queue(catalog.DDLMigration{Statement: "create table u (id int)", Kind: "CREATE TABLE", Scope: "all", Meta: catalog.MigrationMeta{RunAs: "app"}})
	f.run(t)
	if m := f.store.get(t, id); m.State != catalog.MigrationComplete {
		t.Fatalf("%s %s", m.State, states(m))
	}
	for shard := int32(0); shard < 3; shard++ {
		if _, ok := membershipBalanced(f.shards.superuserStatements(shard), "app"); !ok {
			t.Fatalf("shard %d: %q", shard, f.shards.superuserStatements(shard))
		}
	}
}

func TestApplierRevokesLeftoverMembershipsWhenItStarts(t *testing.T) {
	f := newApplierFixture(t)
	f.queue(catalog.DDLMigration{Statement: "create table t (id int)", Kind: "CREATE TABLE", Scope: "home", HomeShard: 1, State: catalog.MigrationRunning,
		Meta:     catalog.MigrationMeta{RunAs: "app"},
		PerShard: map[string]catalog.ShardMigration{"1": {State: catalog.ShardRunning, Attempts: 1}}})
	f.run(t)
	super := f.shards.superuserStatements(1)
	sweep, grant := -1, -1
	for i, st := range super {
		switch {
		case strings.Contains(st, `FROM pg_auth_members WHERE member = 'pgshard_ddl'::regrole`):
			sweep = i
		case strings.HasPrefix(st, `GRANT "app"`):
			grant = i
		}
	}
	if sweep < 0 || grant < 0 || sweep > grant {
		t.Fatalf("leftover memberships are revoked before the first grant: %q", super)
	}
	if _, ok := membershipBalanced(super, "app"); !ok {
		t.Fatalf("resumed step left a membership: %q", super)
	}
}

func TestApplierResumeRebuildsAnInvalidIndexAndSkipsAValidOne(t *testing.T) {
	f := newApplierFixture(t)
	f.shards.exists = func(_ int32, kind, name string) bool { return kind == "relation" && name == "i" }
	f.shards.invalid = func(shard int32, name string) bool { return shard == 1 && name == "i" }
	id := f.queue(catalog.DDLMigration{Statement: "create index concurrently i on t (x)", Kind: "CREATE INDEX", Scope: "all", Strategy: "concurrent", State: catalog.MigrationRunning,
		Meta: catalog.MigrationMeta{Object: catalog.MigrationObject{Kind: "relation", Name: "i", Expect: "present"}},
		PerShard: map[string]catalog.ShardMigration{
			"0": {State: catalog.ShardApplied, Attempts: 1},
			"1": {State: catalog.ShardRunning, Attempts: 1},
			"2": {State: catalog.ShardRunning, Attempts: 1},
		}})
	f.run(t)
	m := f.store.get(t, id)
	if m.State != catalog.MigrationComplete || states(m) != "0=applied/1 1=applied/2 2=applied/2" {
		t.Fatalf("%s %s", m.State, states(m))
	}
	got := strings.Join(f.shards.statements(1), ";")
	if !strings.Contains(got, `DROP INDEX CONCURRENTLY IF EXISTS "i";create index concurrently i on t (x)`) {
		t.Fatalf("invalid index on shard 1 was not rebuilt: %q", got)
	}
	if got := strings.Join(f.shards.statements(2), ";"); strings.Contains(got, "create index") || strings.Contains(got, "DROP INDEX") {
		t.Fatalf("valid index on shard 2 was touched: %q", got)
	}
}

func pkSteps() []catalog.MigrationStep {
	return []catalog.MigrationStep{
		{SQL: `ALTER TABLE t ADD CONSTRAINT t_id_not_null NOT NULL id NOT VALID`, Skip: catalog.MigrationCheck{Kind: "notnull", Table: "t", Name: "id"}},
		{SQL: `ALTER TABLE t VALIDATE CONSTRAINT t_id_not_null`, Skip: catalog.MigrationCheck{Kind: "notnull_valid", Table: "t", Name: "id"},
			OnFail: `ALTER TABLE t DROP CONSTRAINT IF EXISTS t_id_not_null`},
		{SQL: `CREATE UNIQUE INDEX CONCURRENTLY t_pkey ON t (id)`, Concurrent: true, Index: "t_pkey", Skip: catalog.MigrationCheck{Kind: "index_valid", Table: "t", Name: "t_pkey"}},
		{SQL: `ALTER TABLE t ADD CONSTRAINT t_pkey PRIMARY KEY USING INDEX t_pkey`, Skip: catalog.MigrationCheck{Kind: "constraint", Table: "t", Name: "t_pkey"}},
	}
}

func multistep(steps []catalog.MigrationStep) catalog.DDLMigration {
	return catalog.DDLMigration{Statement: "alter table t add primary key (id)", Kind: "ALTER TABLE", Scope: "all", Strategy: "multistep",
		Meta: catalog.MigrationMeta{Steps: steps, RunAs: "app"}}
}

func TestApplierRunsMultistepStepsInOrderEachUnderItsOwnTransaction(t *testing.T) {
	f := newApplierFixture(t)
	f.store.shards = []int32{0}
	id := f.queue(multistep(pkSteps()))
	f.run(t)
	m := f.store.get(t, id)
	if m.State != catalog.MigrationComplete || states(m) != "0=applied/4" || m.PerShard["0"].Step != 4 {
		t.Fatalf("%s %s step=%d", m.State, states(m), m.PerShard["0"].Step)
	}
	got := strings.Join(f.shards.statements(0), "\n")
	want := strings.Join([]string{
		"SET lock_timeout = '2000ms'", "SET ROLE \"app\"", "check notnull t id",
		"BEGIN", pkSteps()[0].SQL, "COMMIT",
		"SET lock_timeout = '2000ms'", "SET ROLE \"app\"", "check notnull_valid t id",
		"BEGIN", pkSteps()[1].SQL, "COMMIT",
		"SET lock_timeout = '2000ms'", "SET ROLE \"app\"", "check index_valid t_pkey",
		pkSteps()[2].SQL,
		"SET lock_timeout = '2000ms'", "SET ROLE \"app\"", "check constraint t t_pkey",
		"BEGIN", pkSteps()[3].SQL, "COMMIT",
	}, "\n")
	if got != want {
		t.Fatalf("statements:\n%s\nwant:\n%s", got, want)
	}
}

func TestApplierResumesAMultistepMigrationAtItsStep(t *testing.T) {
	f := newApplierFixture(t)
	f.store.shards = []int32{0}
	m := multistep(pkSteps())
	m.State = catalog.MigrationRunning
	m.PerShard = map[string]catalog.ShardMigration{"0": {State: catalog.ShardRunning, Attempts: 2, Step: 2}}
	id := f.queue(m)
	f.shards.check = func(_ int32, kind, _, name string) bool { return kind == "index_valid" && name == "t_pkey" }
	f.run(t)
	m = f.store.get(t, id)
	if m.State != catalog.MigrationComplete || states(m) != "0=applied/4" {
		t.Fatalf("%s %s", m.State, states(m))
	}
	got := strings.Join(f.shards.statements(0), "\n")
	if strings.Contains(got, "NOT NULL") || strings.Contains(got, "CREATE UNIQUE INDEX") || !strings.Contains(got, "PRIMARY KEY USING INDEX") {
		t.Fatalf("resume ran earlier steps again or skipped the last:\n%s", got)
	}
}

func TestApplierMultistepRetriesALockedStepAndKeepsTheStep(t *testing.T) {
	f := newApplierFixture(t)
	f.store.shards = []int32{0}
	locked := 2
	f.shards.exec = func(_ int32, sql string) error {
		if strings.HasPrefix(sql, "ALTER TABLE t VALIDATE") && locked > 0 {
			locked--
			return pgErr("55P03", "canceling statement due to lock timeout")
		}
		return nil
	}
	id := f.queue(multistep(pkSteps()))
	f.run(t)
	m := f.store.get(t, id)
	if m.State != catalog.MigrationComplete || states(m) != "0=applied/6" || len(f.sleeps) != 2 {
		t.Fatalf("%s %s sleeps=%v", m.State, states(m), f.sleeps)
	}
	if n := strings.Count(strings.Join(f.shards.statements(0), "\n"), "VALIDATE CONSTRAINT"); n != 3 {
		t.Fatalf("validate ran %d times", n)
	}
}

func TestApplierMultistepValidateFailureDropsTheConstraintAndFailsTheShard(t *testing.T) {
	f := newApplierFixture(t)
	f.shards.exec = func(shard int32, sql string) error {
		if shard == 1 && strings.HasPrefix(sql, "ALTER TABLE t VALIDATE") {
			return pgErr("23502", `column "id" of relation "t" contains null values`)
		}
		return nil
	}
	id := f.queue(multistep(pkSteps()))
	f.run(t)
	m := f.store.get(t, id)
	if m.State != catalog.MigrationFailed || states(m) != "0=applied/4 1=failed/2 2=applied/4" {
		t.Fatalf("%s %s", m.State, states(m))
	}
	if s := m.PerShard["1"]; s.Step != 1 || s.SQLState != "23502" || !strings.Contains(m.Error, "shard 1: ") {
		t.Fatalf("shard 1 %+v error %q", s, m.Error)
	}
	got := strings.Join(f.shards.statements(1), "\n")
	if !strings.Contains(got, "ROLLBACK\nALTER TABLE t DROP CONSTRAINT IF EXISTS t_id_not_null") || strings.Contains(got, "CREATE UNIQUE INDEX") {
		t.Fatalf("shard 1 statements:\n%s", got)
	}
}

func TestApplierMultistepIndexFailureRebuildsOnceThenDropsTheInvalidIndex(t *testing.T) {
	f := newApplierFixture(t)
	f.store.shards = []int32{0}
	f.shards.exec = func(_ int32, sql string) error {
		if strings.HasPrefix(sql, "CREATE UNIQUE INDEX") {
			return pgErr("23505", "duplicate key")
		}
		return nil
	}
	f.shards.invalid = func(_ int32, name string) bool { return name == "t_pkey" }
	id := f.queue(multistep(pkSteps()))
	f.run(t)
	m := f.store.get(t, id)
	if m.State != catalog.MigrationFailed || m.PerShard["0"].Step != 2 || m.PerShard["0"].SQLState != "23505" {
		t.Fatalf("%s %+v", m.State, m.PerShard["0"])
	}
	got := strings.Join(f.shards.statements(0), "\n")
	if strings.Count(got, "CREATE UNIQUE INDEX") != 2 || strings.Count(got, `DROP INDEX CONCURRENTLY IF EXISTS "t_pkey"`) != 3 || strings.Contains(got, "PRIMARY KEY") {
		t.Fatalf("statements:\n%s", got)
	}
}

func TestApplierMultistepSkipsStepsAlreadyDone(t *testing.T) {
	f := newApplierFixture(t)
	f.store.shards = []int32{0}
	f.shards.check = func(_ int32, kind, _, _ string) bool {
		return kind == "notnull" || kind == "notnull_valid" || kind == "constraint"
	}
	id := f.queue(multistep(pkSteps()))
	f.run(t)
	m := f.store.get(t, id)
	if m.State != catalog.MigrationComplete || states(m) != "0=applied/4" {
		t.Fatalf("%s %s", m.State, states(m))
	}
	got := strings.Join(f.shards.statements(0), "\n")
	if strings.Contains(got, "NOT NULL") || strings.Contains(got, "PRIMARY KEY") || strings.Count(got, "CREATE UNIQUE INDEX") != 1 || strings.Contains(got, "BEGIN") {
		t.Fatalf("statements:\n%s", got)
	}
}

func TestApplierStepChecksCoverEveryKind(t *testing.T) {
	f := newApplierFixture(t)
	f.store.shards = []int32{0}
	kinds := []string{"constraint", "constraint_valid", "notnull", "notnull_valid", "index_valid", "detached", "detach_pending"}
	var steps []catalog.MigrationStep
	for _, k := range kinds {
		steps = append(steps, catalog.MigrationStep{SQL: "select 1 -- " + k, Skip: catalog.MigrationCheck{Kind: k, Table: "t", Name: "x"}})
	}
	f.shards.check = func(_ int32, kind, _, _ string) bool { return kind != "?" }
	id := f.queue(multistep(steps))
	f.run(t)
	m := f.store.get(t, id)
	if m.State != catalog.MigrationComplete || m.PerShard["0"].Step != len(kinds) {
		t.Fatalf("%s %+v", m.State, m.PerShard["0"])
	}
	got := strings.Join(f.shards.statements(0), "\n")
	for _, k := range kinds {
		if !strings.Contains(got, "check "+k+" ") {
			t.Fatalf("check %s never ran:\n%s", k, got)
		}
	}
	if strings.Contains(got, "select 1") {
		t.Fatalf("a step ran although its check held:\n%s", got)
	}
	bad := multistep([]catalog.MigrationStep{{SQL: "select 1", Skip: catalog.MigrationCheck{Kind: "bogus"}}})
	id = f.queue(bad)
	f.run(t)
	if m := f.store.get(t, id); m.State != catalog.MigrationFailed || !strings.Contains(m.Error, "unknown step check") {
		t.Fatalf("bogus check: %s %q", m.State, m.Error)
	}
}
