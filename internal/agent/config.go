// Package agent implements pgshard-agent: the PID 1 supervisor that
// bootstraps, fences and drives one PostgreSQL instance.
package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
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
	// ShutdownTimeout bounds a smart shutdown before falling back to fast.
	ShutdownTimeout Duration `json:"shutdownTimeout"`
}

// PostgresSettings are the operator-provided values in postgresql.conf.
type PostgresSettings struct {
	MaxPreparedTransactions     int    `json:"maxPreparedTransactions"`
	MaxReplicationSlots         int    `json:"maxReplicationSlots"`
	MaxWALSenders               int    `json:"maxWalSenders"`
	MaxActiveReplicationOrigins int    `json:"maxActiveReplicationOrigins"`
	SynchronousStandbyNames     string `json:"synchronousStandbyNames"`
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
	return &c, nil
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
