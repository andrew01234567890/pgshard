package pgtune

import (
	"fmt"
	"sort"
	"strings"
)

// unsafeKeys can never be overridden: they weaken durability, replication or
// authentication guarantees the rest of pgshard relies on.
var unsafeKeys = map[string]string{
	"fsync":                         "disabling fsync loses committed data on crash",
	"full_page_writes":              "disabling full-page writes corrupts pages on torn writes",
	"wal_level":                     "logical decoding requires wal_level=logical",
	"max_prepared_transactions":     "two-phase commit requires prepared transactions",
	"ssl":                           "TLS is required between processes",
	"data_checksums":                "checksums are decided at initdb time",
	"password_encryption":           "scram-sha-256 is the only accepted password scheme",
	"standard_conforming_strings":   "the router parses shard keys out of string literals with it on",
	"track_commit_timestamp":        "commit timestamps are required for conflict resolution",
	"max_replication_slots":         "derived from replicas and slots",
	"max_wal_senders":               "derived from replicas and slots",
	"idle_replication_slot_timeout": "abandoned slots must be dropped before they fill the disk",
	"max_slot_wal_keep_size":        "derived from disk size",
	"listen_addresses":              "owned by the agent",
	"hba_file":                      "owned by the agent",
	"ident_file":                    "owned by the agent",
	"unix_socket_directories":       "owned by the agent",
	"data_directory":                "owned by the agent",
	"config_file":                   "owned by the agent",
	"archive_command":               "owned by the agent",
	"restore_command":               "owned by the agent",
	"primary_conninfo":              "owned by the agent",
	"primary_slot_name":             "owned by the agent",
	"synchronous_standby_names":     "owned by the agent",
	"include":                       "includes are not settings",
	"include_if_exists":             "includes are not settings",
	"include_dir":                   "includes are not settings",
}

// UnsafeKeys lists every override key Derive rejects.
func UnsafeKeys() []string {
	keys := make([]string, 0, len(unsafeKeys))
	for k := range unsafeKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func applyOverrides(s *Settings, overrides map[string]string) error {
	names := make([]string, 0, len(overrides))
	for k := range overrides {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, name := range names {
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" {
			return fmt.Errorf("%w: empty setting name", ErrUnsafeOverride)
		}
		if why, bad := unsafeKeys[key]; bad {
			return fmt.Errorf("%w: %s: %s", ErrUnsafeOverride, key, why)
		}
		value := overrides[name]
		if key == "synchronous_commit" && value != "remote_apply" {
			return fmt.Errorf("%w: synchronous_commit: only remote_apply is stronger than the on floor", ErrUnsafeOverride)
		}
		reason := "operator override"
		replaced := false
		for i := range *s {
			if (*s)[i].Name != key {
				continue
			}
			(*s)[i].Value = value
			(*s)[i].Reason = reason
			replaced = true
		}
		if !replaced {
			*s = append(*s, Setting{Name: key, Value: value, Reason: reason})
		}
	}
	return nil
}
