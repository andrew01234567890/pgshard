package agent

import (
	"context"
	"errors"
	"fmt"
	"os/exec"

	"github.com/andrew01234567890/pgshard/internal/catalog"
	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/schemacopy"
)

// MaterializeSchema copies the schema of one database from a source into
// the local database of the same name.
func (s *Server) MaterializeSchema(ctx context.Context, req *pgshardv1.MaterializeSchemaRequest) (*pgshardv1.MaterializeSchemaResponse, error) {
	resp := &pgshardv1.MaterializeSchemaResponse{Epoch: s.epoch.Current()}
	// A copy can outlive a failover of its own target, so it proves the
	// caller is talking to the member that is still serving before it
	// starts, as every other mutating call does. An absent epoch is
	// refused rather than read as zero: a caller that does not know the
	// epoch cannot be shown to mean this member.
	if req.Epoch == nil {
		resp.Error = pgErr(errors.New("materialize schema: no epoch in the request; the caller must name the epoch it believes this member serves at"))
		return resp, nil
	}
	if err := s.fenceCurrent(req.GetEpoch()); err != nil {
		resp.Error = pgErr(err)
		return resp, nil
	}
	ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	resp.Error = pgErr(s.inst.MaterializeSchema(ctx, req.GetSourceConninfo(), req.GetDatabase()))
	return resp, nil
}

// MaterializeSchema runs pg_dump --schema-only on source and pipes it into
// psql against the local database. The source password must be resolvable
// through the conninfo or the agent's pgpass file.
func (in *Instance) MaterializeSchema(ctx context.Context, source, database string) error {
	if err := catalog.CheckDatabaseName(database); err != nil {
		return fmt.Errorf("invalid database name %q", database)
	}
	local := fmt.Sprintf("host=/tmp port=%d user=postgres dbname=%s", in.cfg.Port, database)
	dump := in.sup.Command(ctx, "pg_dump", schemacopy.DumpArgs(source)...)
	restore := in.sup.Command(ctx, "psql", schemacopy.RestoreArgs(local)...)
	return schemacopy.Run(dump, restore, func(cmd *exec.Cmd) (func(), error) {
		if err := in.sup.StartTracked(cmd); err != nil {
			return nil, err
		}
		pid := cmd.Process.Pid
		return func() { in.sup.Untrack(pid) }, nil
	})
}
