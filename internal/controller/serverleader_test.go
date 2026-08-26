package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pgshardv1 "github.com/andrew01234567890/pgshard/internal/gen/pgshard/v1"
)

// TestAFollowerRefusesMutatingRPCs: the Controller service is registered on
// every replica and its handlers did not consult leadership, so a caller
// reaching a follower drove mutating work from a second controller -- the
// concurrency the background loops are gated against, entered through the
// request path instead.
//
// The pool points at a closed port, so the refusal has to happen before any
// query: a read-only RPC reaching the database fails as a connection error,
// which is how this tells "refused for not being leader" apart from "ran".
func TestAFollowerRefusesMutatingRPCs(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://127.0.0.1:1/pgshard?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	follower := &Server{Pool: pool, Resolver: &Resolver{Pool: pool}, Barrier: &Barrier{Store: &PGBarrierStore{Pool: pool}}, Leader: func() bool { return false }}

	mutating := map[string]func() error{
		"CreateBarrier": func() error {
			_, err := follower.CreateBarrier(ctx, &pgshardv1.CreateBarrierRequest{Name: "b"})
			return err
		},
		"ResolveTransactions": func() error {
			_, err := follower.ResolveTransactions(ctx, &pgshardv1.ResolveTransactionsRequest{})
			return err
		},
		"PauseWorkflow": func() error {
			_, err := follower.PauseWorkflow(ctx, &pgshardv1.PauseWorkflowRequest{Id: "00000000-0000-0000-0000-000000000000"})
			return err
		},
		"ResumeWorkflow": func() error {
			_, err := follower.ResumeWorkflow(ctx, &pgshardv1.ResumeWorkflowRequest{Id: "00000000-0000-0000-0000-000000000000"})
			return err
		},
	}
	for name, call := range mutating {
		err := call()
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), "not the leader") {
			t.Errorf("%s on a follower: %v, want FailedPrecondition naming leadership", name, err)
		}
	}

	// Read-only RPCs stay answerable on a follower, so status and topology
	// reads still scale across replicas. They reach the database and fail
	// there, which is what proves they were not refused.
	readOnly := map[string]func() error{
		"ListWorkflows": func() error {
			_, err := follower.ListWorkflows(ctx, &pgshardv1.ListWorkflowsRequest{})
			return err
		},
		"ListBarriers": func() error {
			_, err := follower.ListBarriers(ctx, &pgshardv1.ListBarriersRequest{})
			return err
		},
	}
	for name, call := range readOnly {
		err := call()
		if err != nil && strings.Contains(status.Convert(err).Message(), "not the leader") {
			t.Errorf("%s was refused on a follower; reads must stay answerable everywhere: %v", name, err)
		}
	}

	// A leader accepts them, so a gate that refused everything cannot pass.
	leader := &Server{Pool: pool, Resolver: &Resolver{Pool: pool}, Leader: func() bool { return true }}
	if _, err := leader.PauseWorkflow(ctx, &pgshardv1.PauseWorkflowRequest{Id: "00000000-0000-0000-0000-000000000000"}); err != nil &&
		strings.Contains(status.Convert(err).Message(), "not the leader") {
		t.Errorf("the leader refused its own mutating RPC: %v", err)
	}
}
