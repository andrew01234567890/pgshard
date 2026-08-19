package operator

import (
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/pgtune"
)

func TestNeedsRestartOnlyForPostmasterOrUnknownSettings(t *testing.T) {
	live := map[string]SettingState{
		"log_min_duration_statement": {Context: "superuser"},
		"work_mem":                   {Context: "user"},
		"archive_command":            {Context: "sighup"},
		"max_connections":            {Context: "postmaster"},
	}
	cases := []struct {
		changed []string
		want    bool
	}{
		{[]string{"log_min_duration_statement"}, false},
		{[]string{"work_mem", "archive_command"}, false},
		{[]string{"work_mem", "max_connections"}, true},
		{[]string{"no_such_guc"}, true},
		{nil, false},
	}
	for _, tc := range cases {
		if got := needsRestart(tc.changed, live); got != tc.want {
			t.Errorf("needsRestart(%v) = %v want %v", tc.changed, got, tc.want)
		}
	}
}

func TestRolloutOrderPutsStandbysFirstAndPrimaryLast(t *testing.T) {
	got := rolloutOrder([]string{"g-0", "g-2", "g-1"}, "g-0")
	if want := []string{"g-1", "g-2", "g-0"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("order %v want %v", got, want)
	}
	if got := rolloutOrder([]string{"g-2"}, "g-0"); !reflect.DeepEqual(got, []string{"g-2"}) {
		t.Fatalf("order without primary %v", got)
	}
}

func TestClassifyPodDistinguishesReloadFromRestart(t *testing.T) {
	c := newCluster("cp")
	g := Groups(c)[1]
	tpl := Template(c, nil)
	pod := Renderer{}.Pod(c, g, 1, RoleReplica, "cp-shard-0-1", tpl)
	if s := classifyPod(pod, tpl, false); s.restart || s.reload {
		t.Fatalf("fresh pod must not be stale: %+v", s)
	}
	c2 := c.DeepCopy()
	c2.Spec.PostgreSQL.Parameters = map[string]string{"work_mem": "16MB"}
	tpl2 := Template(c2, nil)
	if s := classifyPod(pod, tpl2, false); !s.reload || s.restart {
		t.Fatalf("settings-only change without the restart flag must reload: %+v", s)
	}
	if s := classifyPod(pod, tpl2, true); !s.restart || s.reload {
		t.Fatalf("settings change with the restart flag must restart: %+v", s)
	}
	c3 := c.DeepCopy()
	c3.Spec.PostgreSQL.Image = "example.invalid/pg:19"
	if s := classifyPod(pod, Template(c3, nil), false); !s.restart {
		t.Fatalf("image change must restart: %+v", s)
	}
	c4 := c.DeepCopy()
	c4.Annotations = map[string]string{AnnotationRestart: "now"}
	if s := classifyPod(pod, Template(c4, nil), false); !s.restart {
		t.Fatalf("restart token must restart: %+v", s)
	}
	c5 := c.DeepCopy()
	c5.Spec.Resources = corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")}}
	if s := classifyPod(pod, Template(c5, nil), false); !s.restart {
		t.Fatalf("resources change must restart: %+v", s)
	}
	if classifyPod(nil, tpl2, true) != (memberStaleness{}) {
		t.Fatal("a missing pod is not stale")
	}
}

func TestTemplateHashesSplitPodShapeFromSettings(t *testing.T) {
	c := newCluster("th")
	first := Template(c, nil).Hash()
	if Template(c.DeepCopy(), nil).Hash() != first {
		t.Fatal("hash must be deterministic")
	}
	tuned := pgtune.Settings{{Name: "shared_buffers", Value: "1GB"}}
	if Template(c, nil).Hash() != Template(c, tuned).Hash() {
		t.Fatal("settings must not move the pod hash; their own hash tracks them")
	}
	if Template(c, tuned).SettingsHash() == Template(c, nil).SettingsHash() {
		t.Fatal("settings hash must follow the derived settings")
	}
	// Overrides win over parameters, as the include order in postgresql.conf does.
	c.Spec.PostgreSQL.Parameters = map[string]string{"shared_buffers": "64MB"}
	if got := Template(c, tuned).Settings["shared_buffers"]; got != "1GB" {
		t.Fatalf("override must win: %q", got)
	}
}

func TestClassifyStorage(t *testing.T) {
	pvc := func(class *string, size string) *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: class,
			Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)}},
		}}
	}
	std := ptr.To("standard")
	cases := []struct {
		name       string
		pvc        *corev1.PersistentVolumeClaim
		want       pgshardv1alpha1.StorageSpec
		expandable bool
		exp        storageChange
	}{
		{"same", pvc(std, "1Gi"), pgshardv1alpha1.StorageSpec{Size: resource.MustParse("1Gi"), StorageClassName: std}, true, storageUnchanged},
		{"nil desired class is no opinion", pvc(std, "1Gi"), pgshardv1alpha1.StorageSpec{Size: resource.MustParse("1Gi")}, true, storageUnchanged},
		{"grow expandable", pvc(std, "1Gi"), pgshardv1alpha1.StorageSpec{Size: resource.MustParse("2Gi"), StorageClassName: std}, true, storageExpand},
		{"grow not expandable", pvc(std, "1Gi"), pgshardv1alpha1.StorageSpec{Size: resource.MustParse("2Gi"), StorageClassName: std}, false, storageRebuild},
		{"class change", pvc(std, "1Gi"), pgshardv1alpha1.StorageSpec{Size: resource.MustParse("1Gi"), StorageClassName: ptr.To("fast")}, true, storageRebuild},
		{"class change and grow", pvc(std, "1Gi"), pgshardv1alpha1.StorageSpec{Size: resource.MustParse("2Gi"), StorageClassName: ptr.To("fast")}, true, storageRebuild},
		{"shrink", pvc(std, "2Gi"), pgshardv1alpha1.StorageSpec{Size: resource.MustParse("1Gi"), StorageClassName: std}, true, storageShrink},
		{"class set where none was", pvc(nil, "1Gi"), pgshardv1alpha1.StorageSpec{Size: resource.MustParse("1Gi"), StorageClassName: std}, true, storageRebuild},
	}
	for _, tc := range cases {
		if got := classifyStorage(tc.pvc, tc.want, tc.expandable); got != tc.exp {
			t.Errorf("%s: got %s want %s", tc.name, got, tc.exp)
		}
	}
}

func TestNextPVCName(t *testing.T) {
	if got := nextPVCName("c-shard-0-1", "c-shard-0-1"); got != "c-shard-0-1-v2" {
		t.Fatalf("first successor %q", got)
	}
	if got := nextPVCName("c-shard-0-1", "c-shard-0-1-v2"); got != "c-shard-0-1-v3" {
		t.Fatalf("second successor %q", got)
	}
	if got := nextPVCName("c-shard-0-1", "c-shard-0-1-vx"); got != "c-shard-0-1-v2" {
		t.Fatalf("garbage suffix %q", got)
	}
}

func TestTuningDropsAgentOwnedSettingsAndNeedsMemory(t *testing.T) {
	c := newCluster("tune")
	c.Spec.PostgreSQL.Parameters = nil
	g := Groups(c)[1]
	if s, err := Tuning(c, g); err != nil || s != nil {
		t.Fatalf("no memory: %v %v", s, err)
	}
	c.Spec.Resources = corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi"), corev1.ResourceCPU: resource.MustParse("2")}}
	s, err := Tuning(c, g)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, x := range s {
		names[x.Name] = true
	}
	for _, owned := range []string{"ssl", "wal_level", "max_replication_slots", "max_wal_senders", "synchronous_commit", "max_prepared_transactions"} {
		if names[owned] {
			t.Errorf("override must not carry the agent-owned %s", owned)
		}
	}
	for _, want := range []string{"shared_buffers", "work_mem", "max_connections", "effective_cache_size"} {
		if !names[want] {
			t.Errorf("override must derive %s", want)
		}
	}
	if !contains(OverrideConf(s), "shared_buffers = '512MB'") {
		t.Errorf("shared_buffers must be a quarter of 2Gi: %s", OverrideConf(s))
	}
	c.Spec.Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")}
	s, _ = Tuning(c, g)
	if !contains(OverrideConf(s), "shared_buffers = '1GB'") {
		t.Errorf("limits must win over requests: %s", OverrideConf(s))
	}
	c.Spec.Resources.Requests[corev1.ResourceMemory] = resource.MustParse("128Mi")
	c.Spec.Resources.Limits = nil
	if _, err := Tuning(c, g); err == nil {
		t.Fatal("128Mi must fail to derive")
	}
	if OverrideConf(nil) != "" {
		t.Fatal("no settings renders no override")
	}
}

func TestConfigMapCarriesOverrideAndSettings(t *testing.T) {
	c := newCluster("cm")
	c.Spec.PostgreSQL.Parameters = map[string]string{"log_min_duration_statement": "250ms"}
	c.Spec.Resources = corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")}}
	g := Groups(c)[1]
	tuning, err := Tuning(c, g)
	if err != nil {
		t.Fatal(err)
	}
	cm := Renderer{}.ConfigMap(c, g, g.MemberName(0), tuning)
	if !contains(cm.Data[overrideConfKey], "shared_buffers = '512MB'") {
		t.Fatalf("override missing: %q", cm.Data[overrideConfKey])
	}
	if !contains(cm.Data[agentConfigKey(g.MemberName(1))], `"overrideFile": "/etc/pgshard/pgshard.override.conf"`) {
		t.Fatalf("agent config must point at the override: %s", cm.Data[agentConfigKey(g.MemberName(1))])
	}
	if !contains(cm.Data[agentConfigKey(g.MemberName(1))], `"settingsHash": "`+Template(c, tuning).SettingsHash()+`"`) {
		t.Fatalf("agent config must carry the settings hash: %s", cm.Data[agentConfigKey(g.MemberName(1))])
	}
	got := ConfigMapSettings(cm)
	if got["shared_buffers"] != "512MB" || got["log_min_duration_statement"] != "250ms" {
		t.Fatalf("settings map %v", got)
	}
	plain := Renderer{}.ConfigMap(c, g, g.MemberName(0), nil)
	if _, ok := plain.Data[overrideConfKey]; ok {
		t.Fatal("no override key without derived settings")
	}
	if contains(plain.Data[agentConfigKey(g.MemberName(1))], "overrideFile") {
		t.Fatal("agent config must not point at an absent override")
	}
	if len(ConfigMapSettings(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "empty"}})) != 0 {
		t.Fatal("empty ConfigMap has no settings")
	}
}

func TestChangedSettings(t *testing.T) {
	got := changedSettings(map[string]string{"a": "1", "b": "2", "c": "3"}, map[string]string{"a": "1", "b": "9", "d": "4"})
	want := map[string]bool{"b": true, "c": true, "d": true}
	if len(got) != len(want) {
		t.Fatalf("changed %v", got)
	}
	for _, n := range got {
		if !want[n] {
			t.Fatalf("unexpected %q in %v", n, got)
		}
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }
