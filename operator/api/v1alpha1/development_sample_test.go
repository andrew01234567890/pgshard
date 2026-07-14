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
