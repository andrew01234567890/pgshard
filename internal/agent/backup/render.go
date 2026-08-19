package backup

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Render produces pgbackrest.conf for the instance at pgdata listening on
// port. Credential values are read from the credentials directory so the
// rendered file is self-contained for archive_command.
func Render(s Settings, pgdata string, port int) (string, error) {
	s = s.WithDefaults()
	if err := s.Validate(); err != nil {
		return "", err
	}
	var g []kv
	add := func(k, v string) { g = append(g, kv{k, v}) }
	add("repo1-type", s.Repo.Type)
	add("repo1-path", s.Repo.Path)
	add("repo1-bundle", "y")
	add("repo1-block", "y")
	add("repo1-retention-full", fmt.Sprint(s.RetentionFull))
	if s.RetentionDiff > 0 {
		add("repo1-retention-diff", fmt.Sprint(s.RetentionDiff))
	}
	if s.CipherPassFile != "" {
		pass, err := readTrim(s.CipherPassFile)
		if err != nil {
			return "", fmt.Errorf("cipher pass: %w", err)
		}
		add("repo1-cipher-type", "aes-256-cbc")
		add("repo1-cipher-pass", pass)
	}
	store, err := renderStore(s.Repo)
	if err != nil {
		return "", err
	}
	g = append(g, store...)
	add("archive-async", "y")
	add("spool-path", s.SpoolPath)
	add("log-path", s.LogPath)
	add("log-level-console", s.LogLevel)
	add("log-level-file", s.LogLevel)
	add("process-max", fmt.Sprint(s.ProcessMax))
	add("start-fast", "y")
	add("delta", "y")

	var b strings.Builder
	b.WriteString("# Managed by pgshard-agent. Edits are overwritten.\n[global]\n")
	for _, e := range g {
		fmt.Fprintf(&b, "%s=%s\n", e.k, e.v)
	}
	fmt.Fprintf(&b, "\n[%s]\npg1-path=%s\npg1-port=%d\npg1-socket-path=/tmp\npg1-user=postgres\n", s.Stanza, pgdata, port)
	return b.String(), nil
}

type kv struct{ k, v string }

func renderStore(r Repo) ([]kv, error) {
	var out []kv
	add := func(k, v string) { out = append(out, kv{k, v}) }
	cred := func(name string) (string, error) { return readTrim(filepath.Join(r.CredentialsDir, name)) }
	switch r.Type {
	case TypeS3:
		add("repo1-s3-bucket", r.Bucket)
		add("repo1-s3-endpoint", r.Endpoint)
		add("repo1-s3-region", r.Region)
		if r.URIStyle != "" {
			add("repo1-s3-uri-style", r.URIStyle)
		}
		add("repo1-s3-key-type", r.KeyType)
		if r.KeyType == "shared" {
			key, err := cred(CredS3Key)
			if err != nil {
				return nil, err
			}
			secret, err := cred(CredS3KeySecret)
			if err != nil {
				return nil, err
			}
			add("repo1-s3-key", key)
			add("repo1-s3-key-secret", secret)
		}
	case TypeAzure:
		account, err := cred(CredAzureAccount)
		if err != nil {
			return nil, err
		}
		key, err := cred(CredAzureKey)
		if err != nil {
			return nil, err
		}
		add("repo1-azure-account", account)
		add("repo1-azure-container", r.Bucket)
		add("repo1-azure-key", key)
		add("repo1-azure-key-type", r.KeyType)
		if r.Endpoint != "" {
			add("repo1-azure-endpoint", r.Endpoint)
		}
		if r.URIStyle != "" {
			add("repo1-azure-uri-style", r.URIStyle)
		}
	case TypeGCS:
		add("repo1-gcs-bucket", r.Bucket)
		add("repo1-gcs-key-type", r.KeyType)
		switch r.KeyType {
		case "service":
			add("repo1-gcs-key", filepath.Join(r.CredentialsDir, CredGCSKeyFile))
		case "token":
			token, err := cred(CredGCSToken)
			if err != nil {
				return nil, err
			}
			add("repo1-gcs-key", token)
		}
		if r.Endpoint != "" {
			add("repo1-gcs-endpoint", r.Endpoint)
		}
	case TypePosix:
	case TypeSFTP:
		add("repo1-sftp-host", r.Host)
		add("repo1-sftp-host-user", r.HostUser)
		if r.HostPort != 0 {
			add("repo1-sftp-host-port", fmt.Sprint(r.HostPort))
		}
		add("repo1-sftp-private-key-file", filepath.Join(r.CredentialsDir, CredSFTPKey))
		add("repo1-sftp-host-key-check-type", r.HostKeyCheck)
	}
	switch r.Type {
	case TypeS3, TypeAzure, TypeGCS:
		if r.VerifyTLS != nil && !*r.VerifyTLS {
			add("repo1-storage-verify-tls", "n")
		}
		if r.CAFile != "" {
			add("repo1-storage-ca-file", r.CAFile)
		}
	}
	return out, nil
}

func readTrim(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

// ArchiveCommand is the postgresql.conf archive_command for the stanza.
func ArchiveCommand(s Settings) string {
	s = s.WithDefaults()
	return fmt.Sprintf("pgbackrest --config=%s --stanza=%s archive-push %%p", s.ConfigPath, s.Stanza)
}

// RestoreCommand is the postgresql.conf restore_command for the stanza.
func RestoreCommand(s Settings) string {
	s = s.WithDefaults()
	return fmt.Sprintf("pgbackrest --config=%s --stanza=%s archive-get %%f \"%%p\"", s.ConfigPath, s.Stanza)
}

// WriteConfig renders the file to s.ConfigPath (mode 0600) and creates the
// spool and log directories.
func WriteConfig(s Settings, pgdata string, port int) error {
	s = s.WithDefaults()
	body, err := Render(s, pgdata, port)
	if err != nil {
		return err
	}
	for _, d := range []string{filepath.Dir(s.ConfigPath), s.SpoolPath, s.LogPath} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			return err
		}
	}
	tmp := s.ConfigPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(body), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.ConfigPath)
}
