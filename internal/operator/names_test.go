package operator

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

func TestSyncStandbyNamesOrdersStreamingFirst(t *testing.T) {
	g := Group{Cluster: "c", Kind: "catalog", Replicas: 4}
	cases := []struct {
		name      string
		numSync   int
		streaming map[string]bool
		want      string
	}{
		{"none streaming keeps ordinal order", 1, nil, `ANY 1 ("c-catalog-1", "c-catalog-2", "c-catalog-3")`},
		{"streaming member moves first", 1, map[string]bool{"c-catalog-3": true}, `ANY 1 ("c-catalog-3", "c-catalog-1", "c-catalog-2")`},
		{"two streaming stay in ordinal order", 1, map[string]bool{"c-catalog-2": true, "c-catalog-3": true}, `ANY 1 ("c-catalog-2", "c-catalog-3", "c-catalog-1")`},
		{"numSync clamps to standby count", 5, nil, `ANY 3 ("c-catalog-1", "c-catalog-2", "c-catalog-3")`},
		{"zero means asynchronous", 0, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SyncStandbyNames(g, "c-catalog-0", tc.numSync, tc.streaming); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
	if got := SyncStandbyNames(Group{Cluster: "c", Kind: "catalog", Replicas: 1}, "c-catalog-0", 1, nil); got != "" {
		t.Fatalf("single member group must have no sync standbys, got %q", got)
	}
	if got := SyncStandbyNames(g, "c-catalog-2", 1, nil); got != `ANY 1 ("c-catalog-0", "c-catalog-1", "c-catalog-3")` {
		t.Fatalf("after failover the old primary must be listed and the new one excluded, got %q", got)
	}
}

func TestReplicaMinAvailable(t *testing.T) {
	for replicas, want := range map[int]int{1: 0, 2: 0, 3: 1, 4: 2, 5: 3} {
		if got := ReplicaMinAvailable(replicas); got != want {
			t.Errorf("replicas=%d: got %d want %d", replicas, got, want)
		}
	}
}

func TestGroupsAndNames(t *testing.T) {
	two := 2
	c := &pgshardv1alpha1.PgShardCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo"},
		Spec: pgshardv1alpha1.PgShardClusterSpec{
			PostgreSQL:       pgshardv1alpha1.PostgreSQLSpec{Major: 18},
			Catalog:          pgshardv1alpha1.CatalogSpec{Replicas: 3, Storage: pgshardv1alpha1.StorageSpec{Size: resource.MustParse("1Gi")}},
			Shards:           &two,
			ReplicasPerShard: 5,
			Storage:          pgshardv1alpha1.StorageSpec{Size: resource.MustParse("2Gi")},
		},
	}
	gs := Groups(c)
	if len(gs) != 3 {
		t.Fatalf("want catalog + 2 shards, got %d groups", len(gs))
	}
	if gs[0].Prefix() != "demo-catalog" || gs[0].Replicas != 3 || gs[0].Storage.Size.String() != "1Gi" {
		t.Errorf("catalog group wrong: %+v", gs[0])
	}
	if gs[2].Prefix() != "demo-shard-1" || gs[2].Replicas != 5 || gs[2].Storage.Size.String() != "2Gi" {
		t.Errorf("shard group wrong: %+v", gs[2])
	}
	if gs[1].MemberName(2) != "demo-shard-0-2" || gs[1].ServiceRW() != "demo-shard-0-rw" || gs[1].ServiceRO() != "demo-shard-0-ro" {
		t.Errorf("names wrong: %s %s %s", gs[1].MemberName(2), gs[1].ServiceRW(), gs[1].ServiceRO())
	}
	c.Spec.Shards = nil
	if got := len(Groups(c)); got != 2 {
		t.Errorf("shards default 1: got %d groups", got)
	}
	if Image(c) != "ghcr.io/andrew01234567890/pgshard-postgres:18" {
		t.Errorf("default image: %s", Image(c))
	}
	c.Spec.PostgreSQL.Image = "example/pg:x"
	if Image(c) != "example/pg:x" {
		t.Errorf("image override ignored: %s", Image(c))
	}
}

func TestQuoteLiteral(t *testing.T) {
	if got := quoteLiteral(`ANY 1 ("a", "b'c")`); got != `'ANY 1 ("a", "b''c")'` {
		t.Fatalf("got %s", got)
	}
}

func TestSynchronizedStandbySlotsListsStreamingSyncCandidatesOnly(t *testing.T) {
	g := Group{Cluster: "c", Kind: "catalog", Replicas: 3}
	streaming := map[string]bool{"c-catalog-0": true, "c-catalog-2": true}
	if got := SynchronizedStandbySlots(g, "c-catalog-0", 1, streaming); !reflect.DeepEqual(got, []string{SlotName("c-catalog-2")}) {
		t.Fatalf("got %v", got)
	}
	if got := SynchronizedStandbySlots(g, "c-catalog-0", 0, streaming); got != nil {
		t.Fatalf("asynchronous group must list no slots, got %v", got)
	}
	if got := SynchronizedStandbySlots(g, "c-catalog-0", 1, nil); got != nil {
		t.Fatalf("nothing streaming must list no slots, got %v", got)
	}
}

func TestServingShardsPrefersCatalogOverSpec(t *testing.T) {
	four := 4
	c := &pgshardv1alpha1.PgShardCluster{ObjectMeta: metav1.ObjectMeta{Name: "c"}}
	if ServingShards(c) != 1 {
		t.Fatalf("default must be 1, got %d", ServingShards(c))
	}
	c.Spec.Shards = &four
	if ServingShards(c) != 4 {
		t.Fatalf("spec.shards before the catalog exists, got %d", ServingShards(c))
	}
	c.Status.EffectiveShards = 2
	if ServingShards(c) != 2 || len(Groups(c)) != 3 {
		t.Fatalf("effective shards must win once materialized: %d groups=%d", ServingShards(c), len(Groups(c)))
	}
	for _, g := range Groups(c)[1:] {
		if g.Generation != 1 || g.NonServing || g.ShardSet() != "default" || g.Labels()[LabelShardSet] != "default" {
			t.Errorf("serving group: %+v", g)
		}
	}
	if TargetGroups(c) != nil {
		t.Fatal("no targets without status.reshard")
	}
	c.Status.Reshard = &pgshardv1alpha1.ClusterReshardStatus{Name: "c-reshard-g3", ShardSet: "g3", Generation: 3, Shards: 4}
	targets := TargetGroups(c)
	if len(targets) != 4 {
		t.Fatalf("targets: %d", len(targets))
	}
	g := targets[3]
	if g.Name() != "shard-3-g3" || g.Prefix() != "c-shard-3-g3" || g.MemberName(0) != "c-shard-3-g3-0" || g.ShardSet() != "g3" ||
		!g.NonServing || g.Replicas != 3 || g.Labels()[LabelShardSet] != "g3" || g.Labels()[LabelGroup] != "shard-3-g3" {
		t.Errorf("target group: %+v name=%s", g, g.Name())
	}
	if Groups(c)[1].Name() == targets[0].Name() {
		t.Error("generation 1 and 3 of shard 0 must not collide")
	}
	if ReshardName("c", 3) != "c-reshard-g3" {
		t.Errorf("ReshardName: %s", ReshardName("c", 3))
	}
}
