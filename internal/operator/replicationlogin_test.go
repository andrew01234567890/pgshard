package operator

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/agent"
	"github.com/andrew01234567890/pgshard/internal/catalog"
)

// A member streams from its primary as pgshard_replication, and takes that
// password from its own mounted Secret. primary_conninfo is written into
// every standby's postgresql.auto.conf and travels in every clone, so the
// credential that reaches the most places is the one that can do the least.
func TestAMemberStreamsAsTheReplicationRole(t *testing.T) {
	c := newCluster("rep")
	g := Groups(c)[0]
	cm := Renderer{}.ConfigMap(c, g, g.MemberName(0), nil, nil, false, true)
	var cfg agent.Config
	if err := json.Unmarshal([]byte(cm.Data[agentConfigKey(g.MemberName(0))]), &cfg); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cfg.PrimaryConninfo, "user="+catalog.ReplicationRole) {
		t.Errorf("primary_conninfo is %q, want it to name %s", cfg.PrimaryConninfo, catalog.ReplicationRole)
	}
	if strings.Contains(cfg.PrimaryConninfo, "user="+superuserName) {
		t.Errorf("primary_conninfo still names the superuser: %q", cfg.PrimaryConninfo)
	}
	if cfg.ReplicationPasswordFile != replicationDir+"/"+secretKey {
		t.Errorf("replication password file %q, want it under the mounted Secret", cfg.ReplicationPasswordFile)
	}
	if strings.Contains(cfg.PrimaryConninfo, "password") {
		t.Errorf("primary_conninfo carries a password: %q", cfg.PrimaryConninfo)
	}

	pod := Renderer{}.Pod(c, g, 0, RolePrimary, "pvc", Template(c, g, nil, nil))
	if pod.Annotations[AnnotationReplicationLogin] != "true" {
		t.Error("the pod does not record that it admits the replication role, so no standby will ever be pointed at it as one")
	}
	var volume bool
	for _, v := range pod.Spec.Volumes {
		if v.Name == replicationVolume {
			volume = true
			if v.Secret == nil || v.Secret.SecretName != ReplicationSecretName(c.Name) {
				t.Errorf("the replication volume is %+v, want the %s Secret", v.Secret, ReplicationSecretName(c.Name))
			}
		}
	}
	if !volume {
		t.Error("the replication Secret is not a volume on the member pod")
	}
	var mounted bool
	for _, ct := range pod.Spec.Containers {
		if ct.Name != "postgres" {
			continue
		}
		for _, m := range ct.VolumeMounts {
			if m.Name == replicationVolume && m.MountPath == replicationDir {
				mounted = true
			}
		}
	}
	if !mounted {
		t.Errorf("the agent does not mount the replication Secret, so it cannot write the pgpass entry")
	}
	// The pooler has no business with it: it never opens a replication
	// connection, and a Secret it does not need is a Secret its compromise
	// does not yield.
	for _, ct := range pod.Spec.Containers {
		if ct.Name == "postgres" {
			continue
		}
		for _, m := range ct.VolumeMounts {
			if m.Name == replicationVolume {
				t.Errorf("%s mounts the replication Secret", ct.Name)
			}
		}
	}
}

// The switch to the replication role is staged behind the primary, and this
// is why: pg_hba rejects an identity it does not list, members roll one at a
// time with the primary LAST, and the rollout holds as soon as the sync set
// is too small. Pointing every standby at the role in the same pass that
// introduces it therefore restarts a standby that cannot stream, drops the
// sync set, and holds the roll before it ever reaches the primary that would
// have admitted it -- a cluster wedged mid-upgrade, with no way out but
// hand-editing pg_hba.
//
// So a member dials its primary as the replication role only once the
// PRIMARY's running pod says it admits one, and as the superuser until then.
func TestAStandbyOnlyStreamsAsTheRoleOnceItsPrimaryAdmitsIt(t *testing.T) {
	c := newCluster("stage")
	g := Groups(c)[0]
	for _, admits := range []bool{false, true} {
		cm := Renderer{}.ConfigMap(c, g, g.MemberName(0), nil, nil, false, admits)
		var cfg agent.Config
		if err := json.Unmarshal([]byte(cm.Data[agentConfigKey(g.MemberName(1))]), &cfg); err != nil {
			t.Fatal(err)
		}
		want := superuserName
		if admits {
			want = catalog.ReplicationRole
		}
		if !strings.Contains(cfg.PrimaryConninfo, "user="+want) {
			t.Errorf("primary admits=%v: conninfo is %q, want user=%s", admits, cfg.PrimaryConninfo, want)
		}
	}

	// And the flip has to roll the pods: it changes what a member presents
	// to its primary, and pods are immutable, so a template that hashed the
	// same either way would leave every member streaming as the superuser
	// for as long as nothing else happened to restart it.
	off, on := Template(c, g, nil, nil), Template(c, g, nil, nil)
	on.Replication = true
	if off.Hash() == on.Hash() {
		t.Error("the template hash does not change with the replication role, so nothing rolls the members onto it")
	}
}
