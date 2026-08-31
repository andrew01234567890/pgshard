// Package agent implements pgshard-agent: the PID 1 supervisor that
// bootstraps, fences and drives one PostgreSQL instance.
package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/andrew01234567890/pgshard/internal/agent/backup"
)

// Role is the bootstrap role of an instance.
type Role string

// Bootstrap roles.
const (
	RolePrimary Role = "primary"
	RoleStandby Role = "standby"
)

// Config is the agent configuration loaded from --config.
type Config struct {
	Cluster string `json:"cluster"`
	Shard   string `json:"shard"`
	// Member is this instance's name; also its application_name and slot suffix.
	Member string `json:"member"`
	// PodName is the Lease holder identity; defaults to Member.
	PodName string `json:"podName"`
	Role    Role   `json:"role"`
	PGData  string `json:"pgdata"`
	BinDir  string `json:"binDir"`
	// PasswordFile holds the postgres superuser password (initdb --pwfile).
	PasswordFile string `json:"passwordFile"`
	// PrimaryConninfo is the source used for cloning and rewinding.
	PrimaryConninfo string `json:"primaryConninfo"`
	// PodCIDR is the network allowed to authenticate with scram over TCP.
	PodCIDR string `json:"podCIDR"`
	// PeerFailsafeURLs are the other members' /failsafe endpoints.
	PeerFailsafeURLs []string `json:"peerFailsafeURLs"`

	Port     int    `json:"port"`
	HTTPAddr string `json:"httpAddr"`
	GRPCAddr string `json:"grpcAddr"`

	Postgres PostgresSettings `json:"postgres"`
	TLS      TLSFiles         `json:"tls"`
	Lease    LeaseConfig      `json:"lease"`

	// MaxLagBytes is the replay lag above which a standby reports not ready.
	MaxLagBytes int64 `json:"maxLagBytes"`
	// IsolationGrace is how long a primary must reach neither the kube API
	// nor any peer before it self-fences; zero means 30s.
	IsolationGrace Duration `json:"isolationGrace"`
	// ShutdownTimeout bounds a smart shutdown before falling back to fast.
	ShutdownTimeout Duration `json:"shutdownTimeout"`
	// OverrideFile is a rendered postgresql.conf fragment (the operator's
	// derived tuning) copied into PGDATA as pgshard.override.conf.
	OverrideFile string `json:"overrideFile,omitempty"`
	// SettingsHash identifies the operator-rendered settings; Reload reports
	// it back so the operator can tell a stale volume from an applied change.
	SettingsHash string `json:"settingsHash,omitempty"`
	// Backup, when set, turns on WAL archiving to the pgBackRest repository
	// and lets the primary take backups.
	Backup *backup.Settings `json:"backup,omitempty"`
	// Restore, on a primary with an empty data directory, bootstraps from
	// the repository (Backup must be set) instead of initdb.
	Restore *backup.RestoreOptions `json:"restore,omitempty"`
	// RecloneFromRepo makes a member that must be rebuilt (a rejoin whose
	// pg_rewind failed) restore from the repository instead of running
	// pg_basebackup against the primary; the operator sets it once a
	// completed backup exists.
	RecloneFromRepo bool `json:"recloneFromRepo,omitempty"`
	// NonServing marks a reshard target that is not part of the serving
	// shard map yet: WAL archiving stays off until it is.
	NonServing bool `json:"nonServing,omitempty"`

	// AuthTokenFile holds the control-plane token this agent accepts, as its
	// own secret rather than something derived from the superuser password.
	// Empty falls back to the derived token alone, which is what an agent
	// deployed before that Secret existed does.
	AuthTokenFile string `json:"authTokenFile,omitempty"`

	// path is where the config was loaded from; Refresh rereads it.
	path string

	// mu guards the fields Refresh replaces while the instance runs:
	// Postgres.Parameters, OverrideFile, SettingsHash, Backup and
	// RecloneFromRepo. Everything else is written once, at load. Readers
	// that run outside the agent server's own lock take it through an
	// accessor; today that is the backup policy.
	mu sync.RWMutex
}

// PostgresSettings are the operator-provided values in postgresql.conf.
type PostgresSettings struct {
	MaxPreparedTransactions     int `json:"maxPreparedTransactions"`
	MaxReplicationSlots         int `json:"maxReplicationSlots"`
	MaxWALSenders               int `json:"maxWalSenders"`
	MaxActiveReplicationOrigins int `json:"maxActiveReplicationOrigins"`
	// MaxLogicalReplicationWorkers must exceed the number of subscriptions
	// this group holds: every subscription keeps an apply worker for its
	// whole life, and table sync draws from the same pool. A reshard target
	// subscribes to every source it takes range from, so a merge exhausts
	// PostgreSQL's default of 4 with apply workers alone and no table ever
	// finishes its initial copy.
	MaxLogicalReplicationWorkers int `json:"maxLogicalReplicationWorkers"`
	// MaxSyncWorkersPerSubscription bounds how much of that pool one
	// subscription may take for table sync.
	MaxSyncWorkersPerSubscription int `json:"maxSyncWorkersPerSubscription"`
	// MaxWorkerProcesses caps every background worker; PostgreSQL silently
	// starts fewer logical workers than asked for when this is lower.
	MaxWorkerProcesses      int    `json:"maxWorkerProcesses"`
	SynchronousStandbyNames string `json:"synchronousStandbyNames"`
	// WALKeepSize retains WAL past the last checkpoint so pg_rewind can find
	// the divergence point without an archive; PostgreSQL size syntax.
	WALKeepSize string `json:"walKeepSize"`
	// RestoreCommand fetches archived WAL; when set pg_rewind may use it.
	RestoreCommand string `json:"restoreCommand"`
	// Parameters are user settings appended to postgresql.conf; a name the
	// agent owns is ignored.
	Parameters map[string]string `json:"parameters,omitempty"`
}

// TLSFiles points at the server certificate; empty disables ssl.
type TLSFiles struct {
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
	CAFile   string `json:"caFile"`
}

// LeaseConfig controls the coordination.k8s.io Lease guarding the primary.
type LeaseConfig struct {
	Enabled   bool     `json:"enabled"`
	Namespace string   `json:"namespace"`
	Duration  Duration `json:"duration"`
	Renew     Duration `json:"renew"`
	Retry     Duration `json:"retry"`
}

// Duration is a time.Duration that unmarshals from a Go duration string.
type Duration time.Duration

// UnmarshalJSON parses "15s"-style strings.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// MarshalJSON writes the Go duration string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// LoadConfig reads and validates a JSON config file.
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	c.path = path
	return &c, nil
}

// Refresh rereads the file the config was loaded from and takes over the
// settings that may change while the instance runs: the user parameters
// and the override file. Everything else stays as loaded at start.
func (c *Config) Refresh() error {
	if c.path == "" {
		return nil
	}
	fresh, err := LoadConfig(c.path)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Postgres.Parameters = fresh.Postgres.Parameters
	c.OverrideFile = fresh.OverrideFile
	c.SettingsHash = fresh.SettingsHash
	c.Backup = fresh.Backup
	c.RecloneFromRepo = fresh.RecloneFromRepo
	return nil
}

// BackupPolicy is the backup settings in force, or nil when the cluster has
// no policy.
//
// Read through this rather than through the field. Refresh replaces it from
// whichever goroutine served the reload, while the backup RPCs and the
// stanza loop read it without the server lock; reading the field twice --
// once for the nil check and once to dereference -- could find a policy and
// then nil, which ends the agent, and the agent is PID 1 in the PostgreSQL
// pod.
func (c *Config) BackupPolicy() *backup.Settings {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Backup
}

func (c *Config) applyDefaults() {
	if c.PodName == "" {
		c.PodName = c.Member
	}
	if c.BinDir == "" {
		c.BinDir = "/usr/local/pgsql/bin"
	}
	if c.Port == 0 {
		c.Port = 5432
	}
	if c.HTTPAddr == "" {
		c.HTTPAddr = ":8080"
	}
	if c.GRPCAddr == "" {
		c.GRPCAddr = ":9090"
	}
	if c.Postgres.MaxPreparedTransactions == 0 {
		c.Postgres.MaxPreparedTransactions = 100
	}
	if c.Postgres.MaxReplicationSlots == 0 {
		c.Postgres.MaxReplicationSlots = 16
	}
	if c.Postgres.MaxWALSenders == 0 {
		c.Postgres.MaxWALSenders = 16
	}
	if c.Postgres.MaxActiveReplicationOrigins == 0 {
		c.Postgres.MaxActiveReplicationOrigins = 16
	}
	if c.Postgres.MaxLogicalReplicationWorkers == 0 {
		// Above the slot count, not equal to it: every subscription holds an
		// apply worker for its whole life, so a pool the same size as the
		// slots leaves nothing for table sync and the initial copy never
		// finishes.
		c.Postgres.MaxLogicalReplicationWorkers = c.Postgres.MaxReplicationSlots + 8
	}
	if c.Postgres.MaxSyncWorkersPerSubscription == 0 {
		c.Postgres.MaxSyncWorkersPerSubscription = 2
	}
	if c.Postgres.MaxWorkerProcesses == 0 {
		c.Postgres.MaxWorkerProcesses = c.Postgres.MaxLogicalReplicationWorkers + 8
	}
	if c.Postgres.WALKeepSize == "" {
		c.Postgres.WALKeepSize = "512MB"
	}
	if c.MaxLagBytes == 0 {
		c.MaxLagBytes = 64 << 20
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = Duration(30 * time.Second)
	}
	if c.Lease.Duration == 0 {
		c.Lease.Duration = Duration(15 * time.Second)
	}
	if c.Lease.Renew == 0 {
		c.Lease.Renew = Duration(10 * time.Second)
	}
	if c.Lease.Retry == 0 {
		c.Lease.Retry = Duration(2 * time.Second)
	}
}

// Validate rejects configs that cannot bootstrap an instance.
func (c *Config) Validate() error {
	var errs []error
	req := func(v, name string) {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	req(c.Cluster, "cluster")
	req(c.Shard, "shard")
	req(c.Member, "member")
	req(c.PGData, "pgdata")
	req(c.PasswordFile, "passwordFile")
	switch c.Role {
	case RolePrimary:
	case RoleStandby:
		req(c.PrimaryConninfo, "primaryConninfo")
	default:
		errs = append(errs, fmt.Errorf("role must be %q or %q, got %q", RolePrimary, RoleStandby, c.Role))
	}
	if (c.TLS.CertFile == "") != (c.TLS.KeyFile == "") {
		errs = append(errs, errors.New("tls.certFile and tls.keyFile must be set together"))
	}
	if c.Lease.Enabled && c.Lease.Namespace == "" {
		errs = append(errs, errors.New("lease.namespace is required when lease.enabled"))
	}
	if c.Backup != nil {
		if err := c.Backup.WithDefaults().Validate(); err != nil {
			errs = append(errs, fmt.Errorf("backup: %w", err))
		}
	}
	if c.Restore != nil {
		if c.Backup == nil {
			errs = append(errs, errors.New("restore requires backup (the repository settings)"))
		}
		if c.Restore.Stanza == "" {
			errs = append(errs, errors.New("restore.stanza is required"))
		}
		if c.Restore.Type == backup.TargetStandby {
			errs = append(errs, errors.New("restore.type standby is not a bootstrap target"))
		}
		if err := c.Restore.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("restore: %w", err))
		}
	}
	if c.RecloneFromRepo && c.Backup == nil {
		errs = append(errs, errors.New("recloneFromRepo requires backup"))
	}
	return errors.Join(errs...)
}

// SlotName is the physical replication slot this member streams from; slot
// names only allow [a-z0-9_], so other characters in Member become '_'.
func (c *Config) SlotName() string {
	return "pgshard_" + strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			return r
		case r >= 'A' && r <= 'Z':
			return r + ('a' - 'A')
		default:
			return '_'
		}
	}, c.Member)
}

// LeaseName is the Lease guarding this shard's primary.
func (c *Config) LeaseName() string { return c.Cluster + "-" + c.Shard + "-primary" }
