package metrics

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/yaml"
)

// alertRulesPath is the file the operator ships to Prometheus.
const alertRulesPath = "../../config/monitoring/alerts.yaml"

// The file is a PrometheusRule custom resource, so the groups sit under
// spec, not at the top level.
type ruleFile struct {
	Spec struct {
		Groups []struct {
			Name  string `json:"name"`
			Rules []struct {
				Alert       string            `json:"alert"`
				Expr        string            `json:"expr"`
				For         string            `json:"for"`
				Labels      map[string]string `json:"labels"`
				Annotations map[string]string `json:"annotations"`
			} `json:"rules"`
		} `json:"groups"`
	} `json:"spec"`
}

func loadRules(t *testing.T) ruleFile {
	t.Helper()
	b, err := os.ReadFile(filepath.Clean(alertRulesPath))
	if err != nil {
		t.Fatal(err)
	}
	var f ruleFile
	if err := yaml.Unmarshal(b, &f); err != nil {
		t.Fatalf("%s does not parse: %v", alertRulesPath, err)
	}
	if len(f.Spec.Groups) == 0 {
		t.Fatalf("%s declares no groups under spec", alertRulesPath)
	}
	return f
}

// registered is every pgshard_* series name the processes export.
//
// Taken from each collector's DESCRIPTION, not from a scrape: a *Vec with no
// labels observed yet gathers nothing, so a scrape-based check reports six
// perfectly good metrics as missing and would fail on correct code. The
// descriptors are the declaration, which is what an alert names.
func registered(t *testing.T) map[string]bool {
	t.Helper()
	zero := func() float64 { return 0 }
	regs := []*prometheus.Registry{
		NewRegistry("router"), NewRegistry("pooler"), NewRegistry("agent"),
		NewRegistry("controller"), NewRegistry("operator"),
	}
	sets := []any{
		NewRouter(regs[0], zero, zero),
		NewPooler(regs[1], zero, zero, zero),
		NewAgent(regs[2], zero, zero),
		NewController(regs[3]),
		NewOperator(regs[4]),
	}

	out := map[string]bool{}
	add := func(name string) {
		out[name] = true
		// A histogram or summary is scraped as _bucket/_sum/_count and a
		// counter as _total; an alert may name any of them.
		for _, suffix := range []string{"_bucket", "_sum", "_count", "_total"} {
			out[name+suffix] = true
		}
		out[strings.TrimSuffix(name, "_total")] = true
	}
	// A GaugeFunc is registered inline and kept on no field, so reflection
	// cannot see it -- but it always yields a sample, so a scrape can.
	// Neither source is complete alone.
	for _, reg := range regs {
		families, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range families {
			add(f.GetName())
		}
	}
	for _, set := range sets {
		for _, c := range collectorsOf(set) {
			descs := make(chan *prometheus.Desc, 64)
			go func() { c.Describe(descs); close(descs) }()
			for d := range descs {
				if m := fqNameRe.FindStringSubmatch(d.String()); m != nil {
					add(m[1])
				}
			}
		}
	}
	if len(out) == 0 {
		t.Fatal("no collectors found on the metric sets; the reflection below stopped working")
	}
	return out
}

// collectorsOf is every exported field of a metric set that is a collector.
// Reflection rather than a list, because a list is one more thing to forget
// when a metric is added -- which is the failure this whole file is about.
func collectorsOf(set any) []prometheus.Collector {
	v := reflect.ValueOf(set)
	for v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil
	}
	var out []prometheus.Collector
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		if !f.CanInterface() {
			continue
		}
		if c, ok := f.Interface().(prometheus.Collector); ok && c != nil {
			out = append(out, c)
		}
	}
	return out
}

var fqNameRe = regexp.MustCompile(`fqName: "([a-zA-Z0-9_]+)"`)

var seriesRe = regexp.MustCompile(`pgshard_[a-z0-9_]+`)

// TestEveryAlertedMetricIsExported is the check nothing else performs.
//
// Until this test there was NOTHING that read config/monitoring/alerts.yaml:
// no Go file, no Makefile target, no workflow. A rule naming a metric no
// process exports is not an error anyone sees -- it is an alert that never
// fires, which is indistinguishable from a system behaving itself. This
// repository has shipped a documented metric that was never exported before.
func TestEveryAlertedMetricIsExported(t *testing.T) {
	have := registered(t)
	var missing []string
	for _, g := range loadRules(t).Spec.Groups {
		for _, r := range g.Rules {
			for _, s := range seriesRe.FindAllString(r.Expr, -1) {
				if !have[s] {
					missing = append(missing, r.Alert+": "+s)
				}
			}
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("alert rules name %d metric(s) no process exports:\n  %s",
			len(missing), strings.Join(missing, "\n  "))
	}
}

// TestEveryAlertHasSeverityAndDescription keeps a rule from shipping without
// what a person paged by it needs.
func TestEveryAlertHasSeverityAndDescription(t *testing.T) {
	for _, g := range loadRules(t).Spec.Groups {
		for _, r := range g.Rules {
			switch {
			case r.Alert == "":
				t.Errorf("group %s has a rule with no alert name", g.Name)
			case r.Labels["severity"] == "":
				t.Errorf("%s has no severity label", r.Alert)
			case strings.TrimSpace(r.Annotations["summary"]) == "":
				t.Errorf("%s has no summary", r.Alert)
			case strings.TrimSpace(r.Annotations["description"]) == "":
				t.Errorf("%s has no description", r.Alert)
			}
		}
	}
}
