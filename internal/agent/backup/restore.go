package backup

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// TargetType is the pgbackrest restore --type.
type TargetType string

// Restore target types.
const (
	// TargetDefault recovers to the end of the archived WAL.
	TargetDefault   TargetType = "default"
	TargetImmediate TargetType = "immediate"
	TargetTime      TargetType = "time"
	TargetLSN       TargetType = "lsn"
	TargetName      TargetType = "name"
	TargetXID       TargetType = "xid"
	// TargetStandby restores a data directory that will follow a primary.
	TargetStandby TargetType = "standby"
)

// RestoreOptions parametrise one pgbackrest restore.
type RestoreOptions struct {
	// Stanza is the repository stanza restored from; empty means the
	// runner's own stanza.
	Stanza string `json:"stanza,omitempty"`
	// BackupID pins the backup set (--set); required for name, xid and
	// immediate targets, which pgbackrest cannot select a set for.
	BackupID string     `json:"backupId,omitempty"`
	Type     TargetType `json:"type,omitempty"`
	// Target is the time, lsn, name or xid value.
	Target string `json:"target,omitempty"`
	// TargetTLI selects the timeline to follow (--target-timeline).
	TargetTLI int64 `json:"targetTLI,omitempty"`
	// Exclusive stops recovery just before the target.
	Exclusive bool `json:"exclusive,omitempty"`
	// Delta reuses files already in PGDATA that match the backup.
	Delta bool `json:"delta,omitempty"`
}

// Validate rejects option combinations pgbackrest would refuse or that
// would recover somewhere other than intended.
func (o RestoreOptions) Validate() error {
	switch o.Type {
	case "", TargetDefault, TargetStandby:
		if o.Target != "" {
			return fmt.Errorf("restore type %q takes no target", o.typeOrDefault())
		}
	case TargetImmediate:
		if o.Target != "" {
			return errors.New("restore type immediate takes no target")
		}
		if o.BackupID == "" {
			return errors.New("restore type immediate requires a backup id")
		}
	case TargetTime, TargetLSN:
		if o.Target == "" {
			return fmt.Errorf("restore type %s requires a target", o.Type)
		}
	case TargetName, TargetXID:
		if o.Target == "" {
			return fmt.Errorf("restore type %s requires a target", o.Type)
		}
		if o.BackupID == "" {
			return fmt.Errorf("restore type %s requires a backup id", o.Type)
		}
	default:
		return fmt.Errorf("unknown restore type %q", o.Type)
	}
	if o.Exclusive && (o.Type == TargetImmediate || o.Type == TargetDefault || o.Type == "" || o.Type == TargetStandby) {
		return fmt.Errorf("exclusive needs a time, lsn, name or xid target")
	}
	if o.TargetTLI < 0 {
		return errors.New("target timeline must be positive")
	}
	return nil
}

func (o RestoreOptions) typeOrDefault() TargetType {
	if o.Type == "" {
		return TargetDefault
	}
	return o.Type
}

// RestoreArgs builds the pgbackrest restore command line for the settings.
func RestoreArgs(s Settings, o RestoreOptions) ([]string, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	s = s.WithDefaults()
	stanza := o.Stanza
	if stanza == "" {
		stanza = s.Stanza
	}
	args := []string{"--config=" + s.ConfigPath, "--stanza=" + stanza, "--log-level-console=" + s.LogLevel}
	if o.Delta {
		args = append(args, "--delta")
	} else {
		args = append(args, "--no-delta")
	}
	if o.BackupID != "" {
		args = append(args, "--set="+o.BackupID)
	}
	args = append(args, "--type="+string(o.typeOrDefault()))
	if o.Target != "" {
		args = append(args, "--target="+o.Target)
	}
	if o.TargetTLI > 0 {
		args = append(args, "--target-timeline="+strconv.FormatInt(o.TargetTLI, 10))
	}
	if o.Exclusive {
		args = append(args, "--target-exclusive")
	}
	return append(args, "restore"), nil
}

// RecoverySettings are the postgresql.conf settings that drive recovery to
// the target after a restore. restore_command is left to the caller.
func RecoverySettings(o RestoreOptions) map[string]string {
	set := map[string]string{"recovery_target_action": "promote"}
	switch o.typeOrDefault() {
	case TargetImmediate:
		set["recovery_target"] = "immediate"
	case TargetTime:
		set["recovery_target_time"] = o.Target
	case TargetLSN:
		set["recovery_target_lsn"] = o.Target
	case TargetName:
		set["recovery_target_name"] = o.Target
	case TargetXID:
		set["recovery_target_xid"] = o.Target
	}
	if o.Exclusive {
		set["recovery_target_inclusive"] = "off"
	}
	if o.TargetTLI > 0 {
		set["recovery_target_timeline"] = strconv.FormatInt(o.TargetTLI, 10)
	}
	return set
}

// RecoverySettingNames lists the settings RecoverySettings may emit.
var RecoverySettingNames = []string{
	"recovery_target", "recovery_target_action", "recovery_target_inclusive", "recovery_target_lsn",
	"recovery_target_name", "recovery_target_time", "recovery_target_timeline", "recovery_target_xid",
}

// RestoreCommandFor is restore_command for an arbitrary stanza in the
// rendered configuration.
func RestoreCommandFor(s Settings, stanza string) string {
	s = s.WithDefaults()
	return fmt.Sprintf("pgbackrest --config=%s --stanza=%s archive-get %%f \"%%p\"", s.ConfigPath, stanza)
}

// Restore runs pgbackrest restore into the configured pg1-path.
func (r *Runner) Restore(ctx context.Context, o RestoreOptions) ([]string, error) {
	args, err := RestoreArgs(r.Settings, o)
	if err != nil {
		return nil, err
	}
	var tail []string
	onLine := func(l string) {
		if r.Log != nil {
			r.Log.Info("pgbackrest", "cmd", "restore", "line", l)
		}
		tail = append(tail, l)
		if len(tail) > logTail {
			tail = tail[len(tail)-logTail:]
		}
	}
	if err := r.Exec(ctx, args, onLine); err != nil {
		return tail, fmt.Errorf("pgbackrest restore: %w: %s", err, lastLine(tail))
	}
	return tail, nil
}

// HasCompletedBackup reports whether the stanza holds at least one backup.
func (r *Runner) HasCompletedBackup(ctx context.Context) (bool, error) {
	st, err := r.Info(ctx)
	if err != nil {
		return false, err
	}
	return len(st.Backups) > 0, nil
}

// String renders the options for logs.
func (o RestoreOptions) String() string {
	parts := []string{"type=" + string(o.typeOrDefault())}
	if o.Stanza != "" {
		parts = append(parts, "stanza="+o.Stanza)
	}
	if o.BackupID != "" {
		parts = append(parts, "set="+o.BackupID)
	}
	if o.Target != "" {
		parts = append(parts, "target="+o.Target)
	}
	if o.TargetTLI > 0 {
		parts = append(parts, "timeline="+strconv.FormatInt(o.TargetTLI, 10))
	}
	if o.Exclusive {
		parts = append(parts, "exclusive")
	}
	if o.Delta {
		parts = append(parts, "delta")
	}
	return strings.Join(parts, " ")
}
