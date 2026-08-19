package agent

import (
	"context"
	"fmt"
	"strings"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
	"github.com/andrew01234567890/pgshard/internal/schemacopy"
)

// MaterializeSchema copies the schema of one database from a source into
// the local database of the same name.
func (s *Server) MaterializeSchema(ctx context.Context, req *pgshardv1.MaterializeSchemaRequest) (*pgshardv1.MaterializeSchemaResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	return &pgshardv1.MaterializeSchemaResponse{Error: pgErr(s.inst.MaterializeSchema(ctx, req.GetSourceConninfo(), req.GetDatabase()))}, nil
}

// MaterializeSchema runs pg_dump --schema-only on source and pipes it into
// psql against the local database. The source password must be resolvable
// through the conninfo or the agent's pgpass file.
func (in *Instance) MaterializeSchema(ctx context.Context, source, database string) error {
	if database == "" || strings.ContainsAny(database, "'\\ ") {
		return fmt.Errorf("invalid database name %q", database)
	}
	local := fmt.Sprintf("host=/tmp port=%d user=postgres dbname=%s", in.cfg.Port, database)
	dump := in.sup.Command(ctx, "pg_dump", schemacopy.DumpArgs(source)...)
	restore := in.sup.Command(ctx, "psql", schemacopy.RestoreArgs(local)...)
	return schemacopy.Run(dump, restore, func(pid int) func() {
		in.sup.Track(pid)
		return func() { in.sup.Untrack(pid) }
	})
}
