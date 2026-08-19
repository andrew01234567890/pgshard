package operator

import (
	"testing"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
)

func TestNewSchemeRegistersPgshardKinds(t *testing.T) {
	s, err := NewScheme()
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"PgShardCluster", "PgShardGroup", "PgShardBackupPolicy", "PgShardBackup", "PgShardRestore", "PgShardReshard"} {
		if !s.Recognizes(pgshardv1alpha1.GroupVersion.WithKind(kind)) {
			t.Errorf("scheme does not recognize %s", kind)
		}
	}
	if !s.Recognizes(pgshardv1alpha1.GroupVersion.WithKind("PgShardClusterList")) {
		t.Error("list kind not registered")
	}
}
