package v1alpha1

import (
	"context"
	"strings"
	"testing"
)

func TestPgShardRestoreValidatorRequiresExactOrderedTopology(t *testing.T) {
	t.Parallel()
	topology := webhookRestoreTopology()
	restore := &PgShardRestore{Spec: PgShardRestoreSpec{
		Manifest:            RestoreManifest{Topology: topology},
		DestinationTopology: topology.DeepCopy(),
	}}
	validator := &PgShardRestoreValidator{}
	if _, err := validator.ValidateCreate(context.Background(), restore); err != nil {
		t.Fatal(err)
	}

	for _, mutate := range []func(*RestoreTopology){
		func(candidate *RestoreTopology) {
			candidate.Shards[0].End = "9223372036854775807"
			candidate.Shards[1].Start = "9223372036854775807"
		},
		func(candidate *RestoreTopology) {
			candidate.Shards[0], candidate.Shards[1] = candidate.Shards[1], candidate.Shards[0]
		},
	} {
		candidate := restore.DeepCopy()
		mutate(candidate.Spec.DestinationTopology)
		_, err := validator.ValidateCreate(context.Background(), candidate)
		if err == nil || !strings.Contains(err.Error(), "RestoreTopologyMismatch") {
			t.Fatalf("topology drift error = %v", err)
		}
	}
}

func TestPgShardRestoreValidatorRejectsSpecUpdates(t *testing.T) {
	t.Parallel()
	topology := webhookRestoreTopology()
	oldRestore := &PgShardRestore{Spec: PgShardRestoreSpec{
		Manifest:            RestoreManifest{Topology: topology},
		DestinationTopology: topology.DeepCopy(),
	}}
	newRestore := oldRestore.DeepCopy()
	newRestore.Spec.DestinationDatabase = "different"
	_, err := (&PgShardRestoreValidator{}).ValidateUpdate(context.Background(), oldRestore, newRestore)
	if err == nil || !strings.Contains(err.Error(), "restore specification is immutable") {
		t.Fatalf("spec update error = %v", err)
	}
}

func webhookRestoreTopology() RestoreTopology {
	return RestoreTopology{
		PostgreSQLMajor: "18",
		HashVersion:     1,
		HashSeed:        "7",
		ShardCount:      2,
		Shards: []RestoreShardRange{
			{Ordinal: 0, Start: "0", End: "9223372036854775808"},
			{Ordinal: 1, Start: "9223372036854775808", End: "18446744073709551616"},
		},
	}
}
