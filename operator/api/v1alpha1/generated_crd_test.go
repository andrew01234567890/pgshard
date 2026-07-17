package v1alpha1

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsvalidation "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

func TestGeneratedCRDPassesAPIServerValidation(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob("../../config/crd/bases/pgshard.io_*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no generated pgshard CRDs found")
	}
	for _, path := range paths {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			t.Parallel()
			contents, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := utilyaml.ToJSON(contents)
			if err != nil {
				t.Fatal(err)
			}
			external := &apiextensionsv1.CustomResourceDefinition{}
			if err := json.Unmarshal(encoded, external); err != nil {
				t.Fatal(err)
			}
			scheme := runtime.NewScheme()
			if err := apiextensions.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			if err := apiextensionsv1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			internal := &apiextensions.CustomResourceDefinition{}
			if err := scheme.Convert(external, internal, nil); err != nil {
				t.Fatal(err)
			}
			// The CRD storage strategy populates this status field after creation. The
			// static schema/CEL validator expects that API-server-owned value to exist.
			internal.Status.StoredVersions = []string{"v1alpha1"}
			if errors := apiextensionsvalidation.ValidateCustomResourceDefinition(context.Background(), internal); len(errors) != 0 {
				t.Fatalf("generated CRD is rejected by kube-apiserver validation: %v", errors.ToAggregate())
			}
		})
	}
}
