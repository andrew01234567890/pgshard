// Package backup renders and drives pgBackRest for one PostgreSQL instance.
package backup

import (
	"errors"
	"fmt"
	"strings"
)

// Repository types pgBackRest supports as repo1-type.
const (
	TypeS3    = "s3"
	TypeAzure = "azure"
	TypeGCS   = "gcs"
	TypePosix = "posix"
	TypeSFTP  = "sftp"
)

// Default paths inside the member container.
const (
	DefaultConfigPath = "/etc/pgbackrest/pgbackrest.conf"
	DefaultSpoolPath  = "/var/lib/pgbackrest/spool"
	DefaultLogPath    = "/var/log/pgbackrest"
	DefaultRepoPath   = "/pgshard"
)

// Credential file names expected in Repo.CredentialsDir; they mirror the
// keys of the Kubernetes Secret the operator mounts there.
const (
	CredS3Key        = "key"
	CredS3KeySecret  = "keySecret"
	CredAzureAccount = "account"
	CredAzureKey     = "key"
	CredGCSKeyFile   = "key.json"
	CredGCSToken     = "token"
	CredSFTPKey      = "privateKey"
	CredPassphrase   = "passphrase"
)

// Settings is the agent's view of a backup policy for one instance.
type Settings struct {
	// Stanza is <cluster>-<group>-pg<major>.
	Stanza string `json:"stanza"`
	Repo   Repo   `json:"repo"`
	// CipherPassFile holds the aes-256-cbc passphrase; empty disables
	// repository encryption.
	CipherPassFile string `json:"cipherPassFile,omitempty"`
	RetentionFull  int    `json:"retentionFull,omitempty"`
	RetentionDiff  int    `json:"retentionDiff,omitempty"`
	// LogLevel is the pgbackrest console/file log level.
	LogLevel   string `json:"logLevel,omitempty"`
	ProcessMax int    `json:"processMax,omitempty"`
	ConfigPath string `json:"configPath,omitempty"`
	SpoolPath  string `json:"spoolPath,omitempty"`
	LogPath    string `json:"logPath,omitempty"`
}

// Repo locates the repository (repo1-*).
type Repo struct {
	Type string `json:"type"`
	// Bucket is the S3/GCS bucket or the Azure container.
	Bucket   string `json:"bucket,omitempty"`
	Endpoint string `json:"endpoint,omitempty"`
	Region   string `json:"region,omitempty"`
	// Path is repo1-path, the prefix inside the store.
	Path string `json:"path,omitempty"`
	// URIStyle is host or path (S3 and Azure).
	URIStyle  string `json:"uriStyle,omitempty"`
	VerifyTLS *bool  `json:"verifyTLS,omitempty"`
	CAFile    string `json:"caFile,omitempty"`
	// KeyType selects the credential mechanism: s3 shared|web-id|auto,
	// azure shared|sas, gcs service|token|auto.
	KeyType string `json:"keyType,omitempty"`
	// CredentialsDir is the directory holding the credential files.
	CredentialsDir string `json:"credentialsDir,omitempty"`
	// SFTP host settings.
	Host     string `json:"host,omitempty"`
	HostUser string `json:"hostUser,omitempty"`
	HostPort int    `json:"hostPort,omitempty"`
	// HostKeyCheck is the pgbackrest sftp host key check type.
	HostKeyCheck string `json:"hostKeyCheck,omitempty"`
}

// StanzaName builds the stanza for a group and PostgreSQL major.
func StanzaName(cluster, group string, major int) string {
	return fmt.Sprintf("%s-%s-pg%d", cluster, group, major)
}

// WithDefaults fills the unset paths and options.
func (s Settings) WithDefaults() Settings {
	if s.ConfigPath == "" {
		s.ConfigPath = DefaultConfigPath
	}
	if s.SpoolPath == "" {
		s.SpoolPath = DefaultSpoolPath
	}
	if s.LogPath == "" {
		s.LogPath = DefaultLogPath
	}
	if s.LogLevel == "" {
		s.LogLevel = "info"
	}
	if s.ProcessMax == 0 {
		s.ProcessMax = 2
	}
	if s.RetentionFull == 0 {
		s.RetentionFull = 2
	}
	if s.Repo.Path == "" {
		s.Repo.Path = DefaultRepoPath
	}
	if s.Repo.KeyType == "" {
		switch s.Repo.Type {
		case TypeS3, TypeAzure:
			s.Repo.KeyType = "shared"
		case TypeGCS:
			s.Repo.KeyType = "service"
		}
	}
	if s.Repo.Type == TypeSFTP && s.Repo.HostKeyCheck == "" {
		s.Repo.HostKeyCheck = "strict"
	}
	return s
}

// Validate rejects settings pgbackrest could not use.
func (s Settings) Validate() error {
	var errs []error
	if s.Stanza == "" {
		errs = append(errs, errors.New("stanza is required"))
	}
	if !strings.HasPrefix(s.Repo.Path, "/") {
		errs = append(errs, errors.New("repo.path must be absolute"))
	}
	r := s.Repo
	switch r.Type {
	case TypeS3:
		if r.Bucket == "" || r.Endpoint == "" || r.Region == "" {
			errs = append(errs, errors.New("s3 repo needs bucket, endpoint and region"))
		}
		if r.KeyType == "shared" && r.CredentialsDir == "" {
			errs = append(errs, errors.New("s3 shared credentials need credentialsDir"))
		}
	case TypeAzure:
		if r.Bucket == "" || r.CredentialsDir == "" {
			errs = append(errs, errors.New("azure repo needs bucket (container) and credentialsDir"))
		}
	case TypeGCS:
		if r.Bucket == "" {
			errs = append(errs, errors.New("gcs repo needs bucket"))
		}
		if (r.KeyType == "service" || r.KeyType == "token") && r.CredentialsDir == "" {
			errs = append(errs, errors.New("gcs service and token credentials need credentialsDir"))
		}
	case TypePosix:
	case TypeSFTP:
		if r.Host == "" || r.HostUser == "" || r.CredentialsDir == "" {
			errs = append(errs, errors.New("sftp repo needs host, hostUser and credentialsDir"))
		}
	default:
		errs = append(errs, fmt.Errorf("unsupported repo type %q", r.Type))
	}
	return errors.Join(errs...)
}
