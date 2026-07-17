package v1alpha1

import (
	"context"
	"os"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestDevelopmentSampleIsExplicitAndSemanticallyValid(t *testing.T) {
	t.Parallel()
	contents, err := os.ReadFile("../../config/samples/pgshard_v1alpha1_development.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cluster := &PgShardCluster{}
	if err := yaml.UnmarshalStrict(contents, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.APIVersion != GroupVersion.String() || cluster.Kind != "PgShardCluster" || cluster.Name != "development" {
		t.Fatalf("development sample identity = %#v", cluster.TypeMeta)
	}
	warnings, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 0 {
		t.Fatalf("development sample warnings = %q", warnings)
	}
	if cluster.Spec.Pooler.Scaling.Mode != ScalingFixed || cluster.Spec.Pooler.Scaling.Fixed == nil || cluster.Spec.Pooler.Scaling.Fixed.Replicas != 1 {
		t.Fatalf("development pooler scaling = %#v", cluster.Spec.Pooler.Scaling)
	}
	if _, err := cluster.ResolvedPostgreSQLSettings(); err != nil {
		t.Fatal(err)
	}
}

func TestSingleMemberSampleIsExplicitAndSemanticallyValid(t *testing.T) {
	t.Parallel()
	contents, err := os.ReadFile("../../config/samples/pgshard_v1alpha1_single_member.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cluster := &PgShardCluster{}
	if err := yaml.UnmarshalStrict(contents, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.APIVersion != GroupVersion.String() || cluster.Kind != "PgShardCluster" || cluster.Name != "single-member" {
		t.Fatalf("single-member sample identity = %#v", cluster.TypeMeta)
	}
	warnings, err := (&PgShardClusterValidator{}).ValidateCreate(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 1 || warnings[0] != "single-member topology has no standby or failover, and restarting its primary interrupts the shard" {
		t.Fatalf("single-member sample warnings = %q", warnings)
	}
	if cluster.Spec.Shards != 2 || cluster.Spec.MembersPerShard != 1 || cluster.Spec.Durability != DurabilityAsynchronous {
		t.Fatalf("single-member sample topology = %#v", cluster.Spec)
	}
	if _, err := cluster.ResolvedPostgreSQLSettings(); err != nil {
		t.Fatal(err)
	}
}
