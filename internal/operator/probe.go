package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	"github.com/andrew01234567890/pgshard/internal/placement"
)

// PrimaryState is what the operator learned from one probe of a group primary.
type PrimaryState struct {
	// Streaming holds the application_name of every standby with a streaming
	// walsender.
	Streaming        map[string]bool
	SyncStandbyNames string
}

// StandbyState is what the operator learned from one probe of a member
// during failover.
type StandbyState struct {
	InRecovery bool
	// Streaming is true while a WAL receiver is still connected to a primary.
	Streaming bool
	// FlushLSN is pg_last_wal_receive_lsn(): the WAL durably received.
	FlushLSN uint64
}

// SettingState is one row of pg_settings the operator cares about.
type SettingState struct {
	Value string
	// Context is the GUC context: postmaster values need a restart, the
	// rest apply on reload.
	Context        string
	PendingRestart bool
}

// Prober talks SQL to group members. It is an interface so envtest can
// substitute a fake; the real one dials with pgx.
type Prober interface {
	Probe(ctx context.Context, dsn string) (PrimaryState, error)
	ProbeStandby(ctx context.Context, dsn string) (StandbyState, error)
	SetSyncStandbyNames(ctx context.Context, dsn, value string) error
	MigrateCatalog(ctx context.Context, dsn string) error
	SetRouterPassword(ctx context.Context, dsn, password string) error
	// PublishShardStatus upserts pgshard.shard_status for the given shard
	// groups in one statement; it never lowers primary_epoch. Non-serving
	// groups stay provisioning.
	PublishShardStatus(ctx context.Context, dsn string, rows []ShardStatus) error
	// ShardSets lists the catalog shard sets with their ranges.
	ShardSets(ctx context.Context, dsn string) ([]ShardSetInfo, error)
	// MaterializeShardSet writes a new shard set of equal ranges in state.
	MaterializeShardSet(ctx context.Context, dsn, name string, generation int64, state string, ranges placement.RangeSet, major int) error
	// DropShardSet removes a shard set with its ranges and status rows.
	DropShardSet(ctx context.Context, dsn, name string) error
	// SetShardSetMajor stamps the PostgreSQL major a set's groups run.
	SetShardSetMajor(ctx context.Context, dsn, name string, major int) error
	// SetWorkflowRollback asks a switched upgrade workflow to return
	// serving to the old groups.
	SetWorkflowRollback(ctx context.Context, dsn, workflowID string) error
	// ReshardWorkflow returns the active reshard workflow of a shard set;
	// an empty ID means none exists.
	ReshardWorkflow(ctx context.Context, dsn, shardSet string) (WorkflowInfo, error)
	// PlacementWorkflows lists the table placement workflows that are
	// active, plus the ones that ended in the last day.
	PlacementWorkflows(ctx context.Context, dsn string) ([]PlacementWorkflowInfo, error)
	// SetReshardCutoverSpec mirrors spec.resharding and the proceed
	// annotation into the workflow spec the controller's cutover reads.
	SetReshardCutoverSpec(ctx context.Context, dsn, workflowID, pauseBefore string, proceed []string, retireAfterSeconds int64) error
	// EnsureSlots creates the missing physical slots in want on the primary
	// and drops an inactive slot named drop (the primary's own, inherited
	// from its time as a standby, which would otherwise pin WAL forever).
	EnsureSlots(ctx context.Context, dsn string, want []string, drop string) error
	// Settings reads pg_settings for names.
	Settings(ctx context.Context, dsn string, names []string) (map[string]SettingState, error)
	// ServerMajor reads the server's PostgreSQL major version.
	ServerMajor(ctx context.Context, dsn string) (int, error)
	// EnsureCatalogCopy sets up the logical copy of the catalog schema
	// from the old-major to the new-major catalog primary: publication on
	// the source, truncate plus subscription with the initial copy on the
	// target. Idempotent.
	EnsureCatalogCopy(ctx context.Context, srcDSN, tgtDSN string) error
	// CatalogCopyCaughtUp reports whether the catalog copy subscription
	// consumed the source's current WAL position; lag describes the rest.
	CatalogCopyCaughtUp(ctx context.Context, srcDSN string) (bool, string, error)
	// CutoverCatalog fences the old catalog primary (read-only default,
	// backends terminated), waits for the subscription to drain, carries
	// the sequence positions over and drops the subscription so the new
	// primary owns the catalog.
	CutoverCatalog(ctx context.Context, srcDSN, tgtDSN string) error
	// RollbackCatalog reverses a cutover: it fences the new catalog,
	// replays what it accepted back into the old group over the reverse
	// subscription armed at cutover, carries the sequences back and lets
	// the old group serve again.
	RollbackCatalog(ctx context.Context, oldDSN, newDSN string) error
	// DropCatalogRollback removes the reverse slot and publication from the
	// serving catalog once the rollback window has closed.
	DropCatalogRollback(ctx context.Context, dsn string) error
	// DisableCatalogRollback stops the reverse subscription on the old
	// group so an abandoned rollback stops applying and frees the slot.
	DisableCatalogRollback(ctx context.Context, dsn string) error
	// ReleaseCatalog undoes the cutover fence for a rollback.
	ReleaseCatalog(ctx context.Context, dsn string) error
}

// DSN builds the connection URL for a group's -rw Service.
func DSN(host, namespace, password string) string {
	return HostDSN(fmt.Sprintf("%s.%s.svc", host, namespace), password)
}

// HostDSN builds the connection URL for one host (a Service name or pod IP).
func HostDSN(host, password string) string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(superuserName, password),
		Host:     fmt.Sprintf("%s:%d", host, postgresPort),
		Path:     "/postgres",
		RawQuery: "sslmode=disable&connect_timeout=5",
	}
	return u.String()
}

// PgxProber is the production Prober.
type PgxProber struct{}

// probePrimarySQL reads everything a primary probe needs in one row: that
// the server answers at all, that it is not in recovery, which standbys are
// streaming, and the synchronous standby list. It used to be four
// statements -- SELECT 1, pg_is_in_recovery(), the pg_stat_replication scan
// and SHOW -- and the operator runs this for every member of every group on
// every pass, so it was four round trips where one does.
//
// The aggregate is legal beside the two function calls because neither
// refers to a column of pg_stat_replication; with no rows the filtered
// array_agg is NULL, which coalesce turns into the empty array.
const probePrimarySQL = `SELECT pg_is_in_recovery(),
	current_setting('synchronous_standby_names'),
	coalesce(array_agg(application_name) FILTER (WHERE state = 'streaming'), '{}')
	FROM pg_stat_replication`

// Probe reads liveness and replication state from the primary.
func (PgxProber) Probe(ctx context.Context, dsn string) (PrimaryState, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return PrimaryState{}, err
	}
	defer func() { _ = conn.Close(ctx) }()
	var recovery bool
	var syncNames string
	var streaming []string
	if err := conn.QueryRow(ctx, probePrimarySQL).Scan(&recovery, &syncNames, &streaming); err != nil {
		return PrimaryState{}, err
	}
	if recovery {
		return PrimaryState{}, fmt.Errorf("primary service points at a standby")
	}
	st := PrimaryState{Streaming: make(map[string]bool, len(streaming)), SyncStandbyNames: syncNames}
	for _, name := range streaming {
		st.Streaming[name] = true
	}
	return st, nil
}

// ProbeStandby reads recovery and WAL receiver state from one member.
func (PgxProber) ProbeStandby(ctx context.Context, dsn string) (StandbyState, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return StandbyState{}, err
	}
	defer func() { _ = conn.Close(ctx) }()
	var st StandbyState
	var lsn *int64
	err = conn.QueryRow(ctx, `SELECT pg_is_in_recovery(),
		EXISTS (SELECT 1 FROM pg_stat_wal_receiver WHERE status = 'streaming'),
		CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn() ELSE pg_current_wal_flush_lsn() END - '0/0'::pg_lsn`).
		Scan(&st.InRecovery, &st.Streaming, &lsn)
	if err != nil {
		return StandbyState{}, err
	}
	if lsn != nil {
		st.FlushLSN = uint64(*lsn)
	}
	return st, nil
}

// ShardStatus is one shard group's fence as published to the catalog.
type ShardStatus struct {
	Group    Group
	Epoch    int64
	Endpoint string
}

// PublishShardStatus upserts the shards' fences into pgshard.shard_status.
// The whole set goes in one statement on one connection: a pass published
// every group of the cluster separately, so a large topology paid a
// connect and a round trip per shard.
func (PgxProber) PublishShardStatus(ctx context.Context, dsn string, rows []ShardStatus) error {
	rows = latestPerShard(rows)
	if len(rows) == 0 {
		return nil
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	sets := make([]string, len(rows))
	ids := make([]int, len(rows))
	names := make([]string, len(rows))
	states := make([]string, len(rows))
	epochs := make([]int64, len(rows))
	endpoints := make([]string, len(rows))
	for i, row := range rows {
		state := "serving"
		if row.Group.NonServing {
			state = "provisioning"
		}
		if row.Group.Retired {
			state = "retired"
		}
		sets[i], ids[i], names[i] = row.Group.ShardSet(), row.Group.ShardID, row.Group.Name()
		states[i], epochs[i], endpoints[i] = state, row.Epoch, row.Endpoint
	}
	// The epoch guard alone still rewrote an unchanged row on every pass,
	// because an EQUAL epoch satisfies <=. Each write fires notify_serving,
	// and every router and pooler watcher answers it by opening a catalog
	// connection and reloading ranges, statuses, databases, tables,
	// rewrites, fences and sequences -- so a healthy cluster reconciling
	// every 30s paid a full reload wave per shard for no change at all.
	_, err = conn.Exec(ctx, `INSERT INTO pgshard.shard_status (shard_set, shard_id, group_name, serving_state, primary_epoch, primary_endpoint)
		SELECT * FROM unnest($1::text[], $2::integer[], $3::text[], $4::text[], $5::bigint[], $6::text[])
		ON CONFLICT (shard_set, shard_id) DO UPDATE
		SET group_name = EXCLUDED.group_name, primary_epoch = EXCLUDED.primary_epoch,
		    primary_endpoint = EXCLUDED.primary_endpoint, updated_at = now()
		WHERE pgshard.shard_status.primary_epoch <= EXCLUDED.primary_epoch
		  AND (pgshard.shard_status.group_name IS DISTINCT FROM EXCLUDED.group_name
		    OR pgshard.shard_status.primary_epoch IS DISTINCT FROM EXCLUDED.primary_epoch
		    OR pgshard.shard_status.primary_endpoint IS DISTINCT FROM EXCLUDED.primary_endpoint)`,
		sets, ids, names, states, epochs, endpoints)
	return err
}

// latestPerShard keeps one row per shard: PostgreSQL refuses an ON
// CONFLICT DO UPDATE that would touch the same row twice in one command.
// The row kept is the one the sequence of single-row upserts would have
// left behind -- the highest epoch, since the guard refuses to lower one,
// and the last of equal epochs.
func latestPerShard(rows []ShardStatus) []ShardStatus {
	out := make([]ShardStatus, 0, len(rows))
	at := map[[2]any]int{}
	for _, row := range rows {
		key := [2]any{row.Group.ShardSet(), row.Group.ShardID}
		i, seen := at[key]
		if !seen {
			at[key] = len(out)
			out = append(out, row)
			continue
		}
		if row.Epoch >= out[i].Epoch {
			out[i] = row
		}
	}
	return out
}

// ShardSetInfo is one catalog shard set with its ranges in key order.
type ShardSetInfo struct {
	Name       string
	Generation int64
	State      string
	Ranges     placement.RangeSet
	// PGMajor is the PostgreSQL major stamped on the set; zero when the
	// catalog predates upgrades.
	PGMajor int
}

// WorkflowInfo is the catalog workflow row of a reshard.
type WorkflowInfo struct {
	ID      string
	State   string
	Stage   string
	Message string
	// CutoverPauseMS is status.cutover.pause_ms: fence raised to new map
	// published; zero before the switch.
	CutoverPauseMS int64
	// JournalIDs are the resharding journal ids the cutover wrote. A
	// non-empty value means the switch passed its point of no return, which
	// is the single most important thing a responder needs to know and was
	// visible only by connecting to the catalog directly.
	JournalIDs []string
}

// PlacementWorkflowInfo is one pgshard.workflows row of kind
// table_placement.
type PlacementWorkflowInfo struct {
	ID            string
	Table         string
	From, To      string
	State         string
	Stage         string
	Message       string
	PauseMS       int64
	FromPlacement string
}

// PlacementWorkflows implements Prober.
func (PgxProber) PlacementWorkflows(ctx context.Context, dsn string) ([]PlacementWorkflowInfo, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT id::text,
			spec->>'database' || '.' || (spec->>'schema_name') || '.' || (spec->>'table_name'),
			spec->'from'->>'placement', coalesce(spec->'from'->>'shard_key', ''),
			spec->'to'->>'placement', coalesce(spec->'to'->>'shard_key', ''),
			state, coalesce(status->>'stage', ''), coalesce(status->>'message', ''), coalesce((status->'placement'->>'pause_ms')::bigint, 0)
		FROM pgshard.workflows
		WHERE kind = 'table_placement' AND (state IN ('pending', 'running', 'paused') OR updated_at > now() - interval '1 day')
		ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PlacementWorkflowInfo
	for rows.Next() {
		var w PlacementWorkflowInfo
		var fromP, fromK, toP, toK string
		if err := rows.Scan(&w.ID, &w.Table, &fromP, &fromK, &toP, &toK, &w.State, &w.Stage, &w.Message, &w.PauseMS); err != nil {
			return nil, err
		}
		w.From, w.To = describePlacement(fromP, fromK), describePlacement(toP, toK)
		out = append(out, w)
	}
	return out, rows.Err()
}

func describePlacement(placement, key string) string {
	if placement == "sharded" {
		return "sharded(" + key + ")"
	}
	return placement
}

// ShardSets lists the catalog shard sets and their ranges.
func (PgxProber) ShardSets(ctx context.Context, dsn string) ([]ShardSetInfo, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	sets, err := catalog.ListShardSets(ctx, conn)
	if err != nil {
		return nil, err
	}
	ranges, err := catalog.ListAllShardRanges(ctx, conn)
	if err != nil {
		return nil, err
	}
	bySet := map[string][]catalog.ShardRange{}
	for _, r := range ranges {
		bySet[r.ShardSet] = append(bySet[r.ShardSet], r)
	}
	out := make([]ShardSetInfo, 0, len(sets))
	for _, s := range sets {
		info := ShardSetInfo{Name: s.Name, Generation: s.Generation, State: s.State, Ranges: catalog.RangeSet(bySet[s.Name])}
		if s.PGMajor != nil {
			info.PGMajor = *s.PGMajor
		}
		out = append(out, info)
	}
	return out, nil
}

// MaterializeShardSet writes a new shard set in one transaction.
func (PgxProber) MaterializeShardSet(ctx context.Context, dsn, name string, generation int64, state string, ranges placement.RangeSet, major int) error {
	return inTx(ctx, dsn, func(tx pgx.Tx) error {
		return catalog.MaterializeShardSet(ctx, tx, name, generation, state, ranges, major)
	})
}

// DropShardSet removes a shard set in one transaction.
func (PgxProber) DropShardSet(ctx context.Context, dsn, name string) error {
	return inTx(ctx, dsn, func(tx pgx.Tx) error { return catalog.DropShardSet(ctx, tx, name) })
}

// ReshardWorkflow reads the newest active reshard workflow of shardSet.
// ReshardWorkflow returns the newest reshard or upgrade workflow targeting
// shardSet. An online major upgrade is recorded with kind 'upgrade' and
// reuses the reshard cutover, so filtering on 'reshard' alone left the
// operator unable to see the workflow whose proceed gates, rollback request,
// completion and retirement it is meant to mirror.
func (PgxProber) ReshardWorkflow(ctx context.Context, dsn, shardSet string) (WorkflowInfo, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return WorkflowInfo{}, err
	}
	defer func() { _ = conn.Close(ctx) }()
	var w WorkflowInfo
	err = conn.QueryRow(ctx, `SELECT id::text, state, coalesce(status->>'stage', ''), coalesce(status->>'message', ''),
			coalesce((status->'cutover'->>'pause_ms')::bigint, 0), journal_ids FROM pgshard.workflows
		WHERE kind IN ('reshard', 'upgrade') AND spec->>'shard_set' = $1
		ORDER BY created_at DESC LIMIT 1`, shardSet).Scan(&w.ID, &w.State, &w.Stage, &w.Message, &w.CutoverPauseMS, &w.JournalIDs)
	if errors.Is(err, pgx.ErrNoRows) {
		return WorkflowInfo{}, nil
	}
	return w, err
}

// CertifiedBarrier reports whether a restore point of that name exists and
// was certified. A barrier attempt that created the physical restore point
// on every group and then failed certification leaves a name that restores
// cleanly with no error, landing the cluster on a point recorded as NOT
// two-phase-consistent -- so this has to be asked of the live source before
// the restore, never of the restored catalog afterwards: certified is
// WAL-logged after the catalog group's own restore point, so a catalog
// recovered to that name always reads back uncertified, even for a good
// barrier.
func (PgxProber) CertifiedBarrier(ctx context.Context, dsn, password, name string) (bool, error) {
	// The operator has no PGPASSWORD for an arbitrary cluster's superuser,
	// so the password comes from that cluster's secret and is set on the
	// parsed config rather than written into the DSN, which reaches logs
	// and error messages.
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return false, err
	}
	cfg.Password = password
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return false, err
	}
	defer func() { _ = conn.Close(ctx) }()
	var certified bool
	err = conn.QueryRow(ctx, `SELECT certified FROM pgshard.restore_points
		WHERE name = $1 ORDER BY created_at DESC LIMIT 1`, name).Scan(&certified)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return certified, err
}

// SetShardSetMajor stamps the PostgreSQL major of one shard set.
func (PgxProber) SetShardSetMajor(ctx context.Context, dsn, name string, major int) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	return catalog.SetShardSetMajor(ctx, conn, name, major)
}

// SetWorkflowRollback asks a switched upgrade workflow to roll back.
func (PgxProber) SetWorkflowRollback(ctx context.Context, dsn, workflowID string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, `UPDATE pgshard.workflows SET spec = spec || '{"rollback": true}'::jsonb, updated_at = now() WHERE id = $1::uuid`, workflowID)
	return err
}

// SetReshardCutoverSpec merges the cutover keys into the workflow spec.
func (PgxProber) SetReshardCutoverSpec(ctx context.Context, dsn, workflowID, pauseBefore string, proceed []string, retireAfterSeconds int64) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	if pauseBefore == "none" {
		pauseBefore = ""
	}
	if proceed == nil {
		proceed = []string{}
	}
	body, err := json.Marshal(map[string]any{"pause_before": pauseBefore, "proceed": proceed, "retire_after_seconds": retireAfterSeconds})
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, `UPDATE pgshard.workflows SET spec = spec || $2::jsonb, updated_at = now() WHERE id = $1::uuid`, workflowID, body)
	return err
}

func inTx(ctx context.Context, dsn string, fn func(pgx.Tx) error) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// EnsureSlots creates missing physical slots and drops the primary's own.
func (PgxProber) EnsureSlots(ctx context.Context, dsn string, want []string, drop string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, name := range want {
		if _, err := conn.Exec(ctx, `SELECT pg_create_physical_replication_slot($1, true)
			WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, name); err != nil {
			return fmt.Errorf("create slot %s: %w", name, err)
		}
	}
	if drop == "" {
		return nil
	}
	_, err = conn.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
		WHERE slot_name = $1 AND NOT active`, drop)
	return err
}

// Settings reads the named rows of pg_settings.
func (PgxProber) Settings(ctx context.Context, dsn string, names []string) (map[string]SettingState, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, "SELECT name, setting, context, pending_restart FROM pg_settings WHERE name = ANY($1)", names)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]SettingState{}
	for rows.Next() {
		var name string
		var st SettingState
		if err := rows.Scan(&name, &st.Value, &st.Context, &st.PendingRestart); err != nil {
			return nil, err
		}
		out[name] = st
	}
	return out, rows.Err()
}

// SetSyncStandbyNames applies synchronous_standby_names via ALTER SYSTEM and reloads.
func (PgxProber) SetSyncStandbyNames(ctx context.Context, dsn, value string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, "ALTER SYSTEM SET synchronous_standby_names = "+quoteLiteral(value)); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, "SELECT pg_reload_conf()")
	return err
}

// MigrateCatalog applies the embedded catalog migrations.
func (PgxProber) MigrateCatalog(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	return catalog.Migrate(ctx, conn)
}

// SetRouterPassword gives the router's login role the password the operator
// generated for this cluster. It runs on every pass: the role is the same
// for every cluster and its password is not, so the catalog has to be told
// which one, and saying it again is how a restored or rebuilt catalog gets
// back in step with the Secret.
func (PgxProber) SetRouterPassword(ctx context.Context, dsn, password string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	// ALTER ROLE takes no parameters, so the password is a quoted literal.
	_, err = conn.Exec(ctx, `ALTER ROLE `+catalog.RouterRole+` WITH LOGIN PASSWORD `+quoteLiteral(password))
	return err
}

func quoteLiteral(s string) string {
	var b []byte
	b = append(b, '\'')
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			b = append(b, '\'')
		}
		b = append(b, s[i])
	}
	return string(append(b, '\''))
}

// proberCallTimeout bounds every SQL call a reconcile makes. A wedged server
// (for example a commit stuck waiting for a synchronous standby that cannot
// reconnect) must fail the call and requeue, never block a reconcile forever.
const proberCallTimeout = 15 * time.Second

// catalogCallTimeout covers a cutover or rollback, which fences a catalog
// and then waits for a drain. The generic deadline would cancel it well
// inside catalogFenceTimeout and leave the catalog fenced between tries.
const catalogCallTimeout = catalogFenceTimeout + 30*time.Second

// boundedProber caps each call on the wrapped Prober at Timeout.
type boundedProber struct {
	Inner   Prober
	Timeout time.Duration
}

func (b boundedProber) bound(ctx context.Context) (context.Context, context.CancelFunc) {
	return b.boundBy(ctx, proberCallTimeout)
}

// boundBy applies a per-call deadline, honouring an explicit Timeout when
// one is configured.
func (b boundedProber) boundBy(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if b.Timeout > 0 {
		d = b.Timeout
	}
	return context.WithTimeout(ctx, d)
}

func (b boundedProber) Probe(ctx context.Context, dsn string) (PrimaryState, error) {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.Probe(ctx, dsn)
}

func (b boundedProber) ProbeStandby(ctx context.Context, dsn string) (StandbyState, error) {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.ProbeStandby(ctx, dsn)
}

func (b boundedProber) SetSyncStandbyNames(ctx context.Context, dsn, value string) error {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.SetSyncStandbyNames(ctx, dsn, value)
}

func (b boundedProber) MigrateCatalog(ctx context.Context, dsn string) error {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.MigrateCatalog(ctx, dsn)
}

func (b boundedProber) SetRouterPassword(ctx context.Context, dsn, password string) error {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.SetRouterPassword(ctx, dsn, password)
}

func (b boundedProber) PublishShardStatus(ctx context.Context, dsn string, rows []ShardStatus) error {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.PublishShardStatus(ctx, dsn, rows)
}

func (b boundedProber) ShardSets(ctx context.Context, dsn string) ([]ShardSetInfo, error) {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.ShardSets(ctx, dsn)
}

func (b boundedProber) MaterializeShardSet(ctx context.Context, dsn, name string, generation int64, state string, ranges placement.RangeSet, major int) error {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.MaterializeShardSet(ctx, dsn, name, generation, state, ranges, major)
}

func (b boundedProber) DropShardSet(ctx context.Context, dsn, name string) error {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.DropShardSet(ctx, dsn, name)
}

func (b boundedProber) SetShardSetMajor(ctx context.Context, dsn, name string, major int) error {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.SetShardSetMajor(ctx, dsn, name, major)
}

func (b boundedProber) SetWorkflowRollback(ctx context.Context, dsn, workflowID string) error {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.SetWorkflowRollback(ctx, dsn, workflowID)
}

func (b boundedProber) ServerMajor(ctx context.Context, dsn string) (int, error) {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.ServerMajor(ctx, dsn)
}

func (b boundedProber) EnsureCatalogCopy(ctx context.Context, srcDSN, tgtDSN string) error {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.EnsureCatalogCopy(ctx, srcDSN, tgtDSN)
}

func (b boundedProber) CatalogCopyCaughtUp(ctx context.Context, srcDSN string) (bool, string, error) {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.CatalogCopyCaughtUp(ctx, srcDSN)
}

func (b boundedProber) CutoverCatalog(ctx context.Context, srcDSN, tgtDSN string) error {
	ctx, cancel := b.boundBy(ctx, catalogCallTimeout)
	defer cancel()
	return b.Inner.CutoverCatalog(ctx, srcDSN, tgtDSN)
}

func (b boundedProber) RollbackCatalog(ctx context.Context, oldDSN, newDSN string) error {
	ctx, cancel := b.boundBy(ctx, catalogCallTimeout)
	defer cancel()
	return b.Inner.RollbackCatalog(ctx, oldDSN, newDSN)
}

func (b boundedProber) DropCatalogRollback(ctx context.Context, dsn string) error {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.DropCatalogRollback(ctx, dsn)
}

func (b boundedProber) DisableCatalogRollback(ctx context.Context, dsn string) error {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.DisableCatalogRollback(ctx, dsn)
}

func (b boundedProber) ReleaseCatalog(ctx context.Context, dsn string) error {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.ReleaseCatalog(ctx, dsn)
}

func (b boundedProber) ReshardWorkflow(ctx context.Context, dsn, shardSet string) (WorkflowInfo, error) {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.ReshardWorkflow(ctx, dsn, shardSet)
}

func (b boundedProber) PlacementWorkflows(ctx context.Context, dsn string) ([]PlacementWorkflowInfo, error) {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.PlacementWorkflows(ctx, dsn)
}

func (b boundedProber) SetReshardCutoverSpec(ctx context.Context, dsn, workflowID, pauseBefore string, proceed []string, retireAfterSeconds int64) error {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.SetReshardCutoverSpec(ctx, dsn, workflowID, pauseBefore, proceed, retireAfterSeconds)
}

func (b boundedProber) EnsureSlots(ctx context.Context, dsn string, want []string, drop string) error {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.EnsureSlots(ctx, dsn, want, drop)
}

func (b boundedProber) Settings(ctx context.Context, dsn string, names []string) (map[string]SettingState, error) {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	return b.Inner.Settings(ctx, dsn, names)
}
