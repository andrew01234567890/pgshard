package controller

import (
	"os"
	"path/filepath"
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/operator/api/v1alpha1"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting"
	"sigs.k8s.io/yaml"
)

// The shipped manager runs with the mutating webhook disabled, so the API
// server's structural defaulting is all that fills an omitted optional block.
// Structural defaulting does not descend into an absent object, so every
// optional block reconciliation depends on needs its own default — otherwise a
// spec the API server accepts can never converge.
//
// This runs the API server's own defaulting against the generated CRD schema
// rather than inspecting the schema, so it fails for the same reason a real
// cluster would.
func TestShippedSamplesReconcileAfterAPIServerDefaulting(t *testing.T) {
	structural := generatedClusterSchema(t)
	samples, err := filepath.Glob(filepath.Join("..", "..", "config", "samples", "*.yaml"))
	if err != nil {
		t.Fatalf("list samples: %v", err)
	}
	if len(samples) == 0 {
		t.Fatal("no samples found")
	}
	for _, sample := range samples {
		t.Run(filepath.Base(sample), func(t *testing.T) {
			raw, err := os.ReadFile(sample)
			if err != nil {
				t.Fatalf("read sample: %v", err)
			}
			object := map[string]any{}
			if err := yaml.Unmarshal(raw, &object); err != nil {
				t.Fatalf("decode sample: %v", err)
			}

			defaulting.Default(object, structural)

			defaulted, err := yaml.Marshal(object)
			if err != nil {
				t.Fatalf("encode defaulted sample: %v", err)
			}
			cluster := &pgshardv1alpha1.PgShardCluster{}
			if err := yaml.Unmarshal(defaulted, cluster); err != nil {
				t.Fatalf("decode defaulted sample: %v", err)
			}
			if err := pgshardv1alpha1.ValidateClusterForReconciliation(cluster); err != nil {
				t.Fatalf("sample cannot reconcile after API-server defaulting: %v", err)
			}
		})
	}
}

// A spec that omits the optional blocks must still reconcile. The shipped
// samples carry most of them, so they cannot prove any individual block is
// defaulted — this strips them from a real sample and requires the result to
// converge, which is what a user writing a lean spec would hit.
func TestSpecWithoutOptionalBlocksReconcilesAfterAPIServerDefaulting(t *testing.T) {
	structural := generatedClusterSchema(t)
	raw, err := os.ReadFile(filepath.Join("..", "..", "config", "samples", "pgshard_v1alpha1_development.yaml"))
	if err != nil {
		t.Fatalf("read sample: %v", err)
	}
	object := map[string]any{}
	if err := yaml.Unmarshal(raw, &object); err != nil {
		t.Fatalf("decode sample: %v", err)
	}
	spec, ok := object["spec"].(map[string]any)
	if !ok {
		t.Fatal("sample has no spec")
	}
	for _, optional := range []string{"pooler", "services", "observability"} {
		delete(spec, optional)
	}

	defaulting.Default(object, structural)

	defaulted, err := yaml.Marshal(object)
	if err != nil {
		t.Fatalf("encode defaulted spec: %v", err)
	}
	cluster := &pgshardv1alpha1.PgShardCluster{}
	if err := yaml.Unmarshal(defaulted, cluster); err != nil {
		t.Fatalf("decode defaulted spec: %v", err)
	}
	if err := pgshardv1alpha1.ValidateClusterForReconciliation(cluster); err != nil {
		t.Fatalf("a spec omitting the optional blocks cannot reconcile: %v", err)
	}
	if cluster.Spec.Observability.Prometheus == nil || !*cluster.Spec.Observability.Prometheus {
		t.Fatal("the documented prometheus default is unreachable when observability is omitted")
	}
}

func generatedClusterSchema(t *testing.T) *structuralschema.Structural {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "bases", "pgshard.io_pgshardclusters.yaml"))
	if err != nil {
		t.Fatalf("read generated CRD: %v", err)
	}
	definition := &apiextensionsv1.CustomResourceDefinition{}
	if err := yaml.Unmarshal(raw, definition); err != nil {
		t.Fatalf("decode generated CRD: %v", err)
	}
	if len(definition.Spec.Versions) == 0 || definition.Spec.Versions[0].Schema == nil {
		t.Fatal("generated CRD has no schema")
	}
	internal := &apiextensions.JSONSchemaProps{}
	if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(
		definition.Spec.Versions[0].Schema.OpenAPIV3Schema, internal, nil,
	); err != nil {
		t.Fatalf("convert schema: %v", err)
	}
	structural, err := structuralschema.NewStructural(internal)
	if err != nil {
		t.Fatalf("build structural schema: %v", err)
	}
	return structural
}
