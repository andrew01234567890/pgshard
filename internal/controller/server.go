package controller

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// Server serves pgshard.v1.Controller from the workflows table.
type Server struct {
	pgshardv1.UnimplementedControllerServer
	Pool *pgxpool.Pool
	// Resolver serves ResolveTransactions; nil answers Unimplemented.
	Resolver *Resolver
	// Barrier serves CreateBarrier; nil answers Unimplemented.
	Barrier *Barrier
	// Streams serves CreateStream/DropStream; nil answers Unimplemented.
	Streams *StreamAdmin
}

// CreateBarrier runs one barrier to completion.
func (s *Server) CreateBarrier(ctx context.Context, req *pgshardv1.CreateBarrierRequest) (*pgshardv1.CreateBarrierResponse, error) {
	if s.Barrier == nil {
		return nil, status.Error(codes.Unimplemented, "the controller has no shard access configured for barriers")
	}
	rp, err := s.Barrier.Run(ctx, req.GetName())
	switch {
	case errors.Is(err, ErrBarrierName), errors.Is(err, ErrBarrierExists):
		return nil, status.Error(codes.InvalidArgument, err.Error())
	case err != nil:
		return &pgshardv1.CreateBarrierResponse{Error: &pgshardv1.Error{Message: err.Error()}}, nil
	}
	return &pgshardv1.CreateBarrierResponse{Barrier: restorePointProto(rp)}, nil
}

// ListBarriers lists recorded restore points, newest first.
func (s *Server) ListBarriers(ctx context.Context, req *pgshardv1.ListBarriersRequest) (*pgshardv1.ListBarriersResponse, error) {
	points, err := ListRestorePoints(ctx, s.Pool, req.GetCertifiedOnly())
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	resp := &pgshardv1.ListBarriersResponse{}
	for _, rp := range points {
		resp.Barriers = append(resp.Barriers, restorePointProto(rp))
	}
	return resp, nil
}

func restorePointProto(rp RestorePoint) *pgshardv1.Barrier {
	out := &pgshardv1.Barrier{Id: rp.ID, Name: rp.Name, RestorePoint: RestorePointName(rp.Name), ShardMapGeneration: rp.ShardMapGeneration,
		Certified: rp.Certified, CreatedAt: rp.CreatedAt.Unix()}
	for _, g := range rp.Groups {
		out.Groups = append(out.Groups, &pgshardv1.GroupRestorePoint{Group: g.Group, Lsn: g.LSN, Timeline: g.Timeline, WalSegment: g.WALSegment})
	}
	return out
}

// ResolveTransactions runs one resolver pass on demand.
func (s *Server) ResolveTransactions(ctx context.Context, req *pgshardv1.ResolveTransactionsRequest) (*pgshardv1.ResolveTransactionsResponse, error) {
	if s.Resolver == nil {
		return nil, status.Error(codes.Unimplemented, "the controller has no shard access configured for the resolver")
	}
	out, err := s.Resolver.Resolve(ctx, req.GetShardSet())
	resp := &pgshardv1.ResolveTransactionsResponse{Committed: uint32(out.Committed), RolledBack: uint32(out.RolledBack), Unresolved: uint32(out.Unresolved)}
	if err != nil {
		resp.Error = &pgshardv1.Error{Message: err.Error()}
	}
	return resp, nil
}

var kindToProto = map[string]pgshardv1.WorkflowKind{
	KindReshard:        pgshardv1.WorkflowKind_WORKFLOW_KIND_RESHARD,
	KindTablePlacement: pgshardv1.WorkflowKind_WORKFLOW_KIND_REKEY,
}

var stateToProto = map[string]pgshardv1.WorkflowState{
	StatePending:   pgshardv1.WorkflowState_WORKFLOW_STATE_PENDING,
	StateRunning:   pgshardv1.WorkflowState_WORKFLOW_STATE_RUNNING,
	StatePaused:    pgshardv1.WorkflowState_WORKFLOW_STATE_PAUSED,
	StateCompleted: pgshardv1.WorkflowState_WORKFLOW_STATE_COMPLETED,
	StateFailed:    pgshardv1.WorkflowState_WORKFLOW_STATE_FAILED,
	StateCancelled: pgshardv1.WorkflowState_WORKFLOW_STATE_CANCELLED,
}

// protoState maps catalog states onto the API; provisioning is a running
// reshard whose targets are still being created.
func protoState(state string) pgshardv1.WorkflowState {
	if state == StateProvisioning {
		return pgshardv1.WorkflowState_WORKFLOW_STATE_RUNNING
	}
	return stateToProto[state]
}

func lookupKey[K comparable, V comparable](m map[K]V, want V) (K, bool) {
	for k, v := range m {
		if v == want {
			return k, true
		}
	}
	var zero K
	return zero, false
}

type workflowRow struct {
	ID     string
	Kind   string
	State  string
	Status []byte
	Error  *string
}

func (w workflowRow) proto() *pgshardv1.Workflow {
	out := &pgshardv1.Workflow{Id: w.ID, Kind: kindToProto[w.Kind], State: protoState(w.State)}
	var st struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(w.Status, &st) == nil {
		out.Message = st.Message
	}
	if w.Error != nil {
		out.Error = &pgshardv1.Error{Message: *w.Error}
	}
	return out
}

const workflowColumns = `id::text, kind, state, status, error`

// ListWorkflows returns workflows, optionally filtered by kind and state.
func (s *Server) ListWorkflows(ctx context.Context, req *pgshardv1.ListWorkflowsRequest) (*pgshardv1.ListWorkflowsResponse, error) {
	var kind, state *string
	if req.GetKind() != pgshardv1.WorkflowKind_WORKFLOW_KIND_UNSPECIFIED {
		k, ok := lookupKey(kindToProto, req.GetKind())
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "unsupported workflow kind %s", req.GetKind())
		}
		kind = &k
	}
	if req.GetState() != pgshardv1.WorkflowState_WORKFLOW_STATE_UNSPECIFIED {
		st, ok := lookupKey(stateToProto, req.GetState())
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "unsupported workflow state %s", req.GetState())
		}
		state = &st
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+workflowColumns+` FROM pgshard.workflows
		WHERE ($1::text IS NULL OR kind = $1) AND ($2::text IS NULL OR state = $2) ORDER BY created_at, id`, kind, state)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	list, err := pgx.CollectRows(rows, pgx.RowToStructByPos[workflowRow])
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	resp := &pgshardv1.ListWorkflowsResponse{}
	for _, w := range list {
		resp.Workflows = append(resp.Workflows, w.proto())
	}
	return resp, nil
}

func (s *Server) getWorkflow(ctx context.Context, id string) (*pgshardv1.Workflow, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+workflowColumns+` FROM pgshard.workflows WHERE id::text = $1`, id)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	w, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByPos[workflowRow])
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Errorf(codes.NotFound, "workflow %q not found", id)
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return w.proto(), nil
}

// GetWorkflow returns one workflow by id.
func (s *Server) GetWorkflow(ctx context.Context, req *pgshardv1.GetWorkflowRequest) (*pgshardv1.GetWorkflowResponse, error) {
	w, err := s.getWorkflow(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &pgshardv1.GetWorkflowResponse{Workflow: w}, nil
}

// PauseWorkflow moves a pending or running workflow to paused, remembering
// the state to resume into.
func (s *Server) PauseWorkflow(ctx context.Context, req *pgshardv1.PauseWorkflowRequest) (*pgshardv1.PauseWorkflowResponse, error) {
	if err := s.transition(ctx, req.GetId(), `
		UPDATE pgshard.workflows
		SET status = status || jsonb_build_object('paused_from', state), state = $2, updated_at = now()
		WHERE id::text = $1 AND state IN ($3, $4)`, StatePaused, StatePending, StateRunning); err != nil {
		return nil, err
	}
	w, err := s.getWorkflow(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &pgshardv1.PauseWorkflowResponse{Workflow: w}, nil
}

// ResumeWorkflow returns a paused workflow to the state it was paused from.
func (s *Server) ResumeWorkflow(ctx context.Context, req *pgshardv1.ResumeWorkflowRequest) (*pgshardv1.ResumeWorkflowResponse, error) {
	if err := s.transition(ctx, req.GetId(), `
		UPDATE pgshard.workflows
		SET state = coalesce(status->>'paused_from', $3), status = status - 'paused_from', updated_at = now()
		WHERE id::text = $1 AND state = $2`, StatePaused, StatePending); err != nil {
		return nil, err
	}
	w, err := s.getWorkflow(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &pgshardv1.ResumeWorkflowResponse{Workflow: w}, nil
}

func (s *Server) transition(ctx context.Context, id, sql string, args ...any) error {
	tag, err := s.Pool.Exec(ctx, sql, append([]any{id}, args...)...)
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	if tag.RowsAffected() == 0 {
		w, err := s.getWorkflow(ctx, id)
		if err != nil {
			return err
		}
		return status.Errorf(codes.FailedPrecondition, "workflow %s is %s", id, w.GetState())
	}
	return nil
}

// ListRoleStatus implements pgshardv1.ControllerServer.
func (s *Server) ListRoleStatus(ctx context.Context, req *pgshardv1.ListRoleStatusRequest) (*pgshardv1.ListRoleStatusResponse, error) {
	rows, err := ListRoleStatus(ctx, s.Pool, req.GetRole())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "role status: %v", err)
	}
	resp := &pgshardv1.ListRoleStatusResponse{}
	for _, r := range rows {
		details, _ := json.Marshal(r.Details)
		resp.Statuses = append(resp.Statuses, &pgshardv1.RoleStatus{Role: r.Role, Group: r.Group, State: roleState(r.State),
			DetailsJson: string(details), RolesGeneration: r.Generation, CheckedAtUnix: r.CheckedAt.Unix()})
	}
	return resp, nil
}

func roleState(s string) pgshardv1.RoleStatus_State {
	switch s {
	case RoleInSync:
		return pgshardv1.RoleStatus_STATE_IN_SYNC
	case RoleDrifted:
		return pgshardv1.RoleStatus_STATE_DRIFTED
	case RoleMissing:
		return pgshardv1.RoleStatus_STATE_MISSING
	case RoleUnmanaged, RoleUnmanagedSuperuser:
		return pgshardv1.RoleStatus_STATE_UNMANAGED
	}
	return pgshardv1.RoleStatus_STATE_UNSPECIFIED
}
