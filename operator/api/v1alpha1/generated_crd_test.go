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
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
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

func TestGeneratedCRDConstrainsDatabaseNamesWithoutTheWebhook(t *testing.T) {
	t.Parallel()
	// The name is interpolated into database genesis SQL, including bodies
	// delimited by fixed dollar-quote tags that a string-literal escape cannot
	// close over. The structural schema has to carry that grammar so the
	// constraint survives an unreachable admission webhook.
	validator := clusterSchemaValidator(t)
	tests := map[string]struct {
		name     string
		accepted bool
	}{
		"ordinary label":            {name: "app", accepted: true},
		"hyphenated label":          {name: "billing-eu-west-1", accepted: true},
		"dollar quote tag":          {name: "x$pgshard_legacy_topology$x"},
		"genesis dollar quote tag":  {name: "x$pgshard_database_genesis_postcondition$x"},
		"single quote":              {name: "a'b"},
		"backslash":                 {name: `a\b`},
		"semicolon":                 {name: "a;b"},
		"upper case":                {name: "App"},
		"underscore":                {name: "my_app"},
		"leading hyphen":            {name: "-app"},
		"statement terminator line": {name: "app\nDROP DATABASE app"},
	}
	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			var rejection *field.Error
			for _, err := range apiservervalidation.ValidateCustomResource(nil, clusterWithDatabase(test.name), validator) {
				if err.Field == "spec.databases[0].name" {
					rejection = err
				}
			}
			if test.accepted && rejection != nil {
				t.Fatalf("database name %q was rejected by the structural schema: %v", test.name, rejection)
			}
			if !test.accepted && rejection == nil {
				t.Fatalf("database name %q reaches constructed SQL without any API server constraint", test.name)
			}
		})
	}
}

func clusterSchemaValidator(t *testing.T) apiservervalidation.SchemaValidator {
	t.Helper()
	contents, err := os.ReadFile("../../config/crd/bases/pgshard.io_pgshardclusters.yaml")
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
	// Conversion hoists a schema shared by every served version to the spec and
	// clears the per-version copies.
	schema := internal.Spec.Validation
	for _, version := range internal.Spec.Versions {
		if version.Name == "v1alpha1" && version.Schema != nil {
			schema = version.Schema
		}
	}
	if schema == nil {
		t.Fatal("the generated PgShardCluster CRD has no v1alpha1 schema")
	}
	validator, _, err := apiservervalidation.NewSchemaValidator(schema.OpenAPIV3Schema)
	if err != nil {
		t.Fatal(err)
	}
	return validator
}

func clusterWithDatabase(name string) map[string]any {
	return map[string]any{
		"apiVersion": "pgshard.io/v1alpha1",
		"kind":       "PgShardCluster",
		"metadata":   map[string]any{"name": "demo", "namespace": "database"},
		"spec": map[string]any{
			"shards":          int64(1),
			"membersPerShard": int64(1),
			"databases":       []any{map[string]any{"name": name, "shards": int64(1), "cells": []any{int64(0)}}},
		},
	}
}
