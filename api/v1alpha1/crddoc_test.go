package v1alpha1_test

import (
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// TestTheCRDReferenceDescribesTheServedAPI compares docs/crd.md with the
// generated CRDs. The reference is written by hand, so it drifts silently:
// a field added to a Go type reaches the served API through code generation
// and never reaches the document, and a manifest author working from the
// document submits a resource the API server rejects, or misses a state the
// controller can be in.
//
// It checks what a reader acts on and the schema constrains: every required
// spec field and every enumerated value, by name, in that kind's own
// section. What a field means, and states the schema does not enumerate --
// a phase field typed as a plain string, say -- are still on the author.
func TestTheCRDReferenceDescribesTheServedAPI(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "crd.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(doc)
	sections := docSections(text, []string{"PgShardCluster", "PgShardGroup", "PgShardBackupPolicy", "PgShardBackup", "PgShardRestore", "PgShardReshard"})

	files, err := filepath.Glob(filepath.Join("..", "..", "config", "crd", "bases", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no generated CRDs found: %v", err)
	}
	var missing []string
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var crd struct {
			Spec struct {
				Names struct {
					Kind string `json:"kind"`
				} `json:"names"`
				Versions []struct {
					Name   string `json:"name"`
					Schema struct {
						OpenAPIV3Schema map[string]any `json:"openAPIV3Schema"`
					} `json:"schema"`
				} `json:"versions"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal(raw, &crd); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		kind := crd.Spec.Names.Kind
		// Each kind is described under its own heading, and a field is only
		// documented if it appears there: naming it under another kind is
		// how a reference drifts into being wrong rather than incomplete.
		section, ok := sections[kind]
		if !ok {
			missing = append(missing, kind+" has no section in docs/crd.md")
			continue
		}
		for _, v := range crd.Spec.Versions {
			for _, want := range wanted(v.Schema.OpenAPIV3Schema) {
				if !names(section, want.token) {
					missing = append(missing, kind+" "+want.what+" "+want.token)
				}
			}
		}
	}
	sort.Strings(missing)
	missing = slices.Compact(missing)
	if len(missing) > 0 {
		t.Fatalf("docs/crd.md does not describe the served API; %d item(s) missing:\n  %s",
			len(missing), strings.Join(missing, "\n  "))
	}
}

// docSections splits the reference into one text per kind, keyed by the kind
// its heading names.
func docSections(text string, kinds []string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(text, "\n## ") {
		for _, k := range kinds {
			if strings.HasPrefix(part, k+" ") || strings.HasPrefix(part, k+"\n") {
				out[k] = part
			}
		}
	}
	return out
}

// names reports whether a section names token as a word: the document
// writes fields both as dotted paths and inside a brace list, so the leaf
// is what can be looked for either way.
func names(section, token string) bool {
	for from := 0; from < len(section); {
		rel := strings.Index(section[from:], token)
		if rel < 0 {
			return false
		}
		i := from + rel
		before, after := byte(' '), byte(' ')
		if i > 0 {
			before = section[i-1]
		}
		if i+len(token) < len(section) {
			after = section[i+len(token)]
		}
		if !wordByte(before) && !wordByte(after) {
			return true
		}
		from = i + 1
	}
	return false
}

// wordByte reports whether b continues an identifier. A dot does not: the
// document writes fields both as spec.postgresql.major and as
// postgresql{major}, and both name the same field.
func wordByte(b byte) bool {
	return b == '_' || b == '-' || (b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

type docItem struct{ what, token string }

// wanted lists what the reference must name for a reader to write a valid
// manifest and recognise the states a controller reports: every required
// field, and every enumerated value. Optional fields are a summary's to
// choose; a required one it omits produces a resource the API server
// rejects, and an enum value it omits is a state that reads as a bug.
func wanted(root map[string]any) []docItem {
	var out []docItem
	props, _ := root["properties"].(map[string]any)
	if spec, _ := props["spec"].(map[string]any); spec != nil {
		// clusterSpec is a whole PgShardCluster spec embedded in a restore.
		// The reference documents it by pointing at that kind rather than
		// restating it, which is the right shape for a summary.
		if sp, ok := spec["properties"].(map[string]any); ok {
			delete(sp, "clusterSpec")
		}
		out = append(out, required("spec", spec)...)
	}
	// True/False/Unknown come from the embedded metav1.Condition schema on
	// every kind; they are Kubernetes' own vocabulary, not this API's.
	seen := map[string]bool{"True": true, "False": true, "Unknown": true}
	for _, v := range enums(root) {
		if !seen[v] {
			seen[v] = true
			out = append(out, docItem{"enum value", v})
		}
	}
	return out
}

// required walks a schema naming every property its parent lists as
// required, as the document writes it: a backticked dotted path.
func required(prefix string, schema map[string]any) []docItem {
	props, _ := schema["properties"].(map[string]any)
	must := map[string]bool{}
	if list, ok := schema["required"].([]any); ok {
		for _, r := range list {
			if name, ok := r.(string); ok {
				must[name] = true
			}
		}
	}
	var out []docItem
	for name, raw := range props {
		child, _ := raw.(map[string]any)
		path := prefix + "." + name
		if must[name] {
			out = append(out, docItem{"required field", name})
		}
		if child != nil {
			out = append(out, required(path, child)...)
		}
	}
	return out
}

// enums collects every enum value in a schema, at any depth.
func enums(schema map[string]any) []string {
	var out []string
	if vals, ok := schema["enum"].([]any); ok {
		for _, v := range vals {
			if s, ok := v.(string); ok {
				out = append(out, s)
			}
		}
	}
	for _, key := range []string{"properties", "items", "additionalProperties"} {
		child, ok := schema[key].(map[string]any)
		if !ok {
			continue
		}
		if key == "properties" {
			for _, raw := range child {
				if m, ok := raw.(map[string]any); ok {
					out = append(out, enums(m)...)
				}
			}
			continue
		}
		out = append(out, enums(child)...)
	}
	return out
}
