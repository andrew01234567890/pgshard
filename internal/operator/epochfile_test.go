package operator

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/andrew01234567890/pgshard/internal/agent"
)

// TestTheAgentKeepsItsEpochOnTheVolumeOutsidePGDATA: the agent's fencing
// epoch has to survive a reclone emptying PGDATA, or an agent that dies
// mid-clone restarts at epoch 0 and obeys any stale Promote. Left unset the
// agent falls back to PGDATA/pgshard/epoch, so the operator has to name the
// file -- on the data volume, and outside PGDATA.
func TestTheAgentKeepsItsEpochOnTheVolumeOutsidePGDATA(t *testing.T) {
	c := newCluster("epoch")
	g := Groups(c)[0]
	cm := Renderer{}.ConfigMap(c, g, g.MemberName(0), nil, nil, false, true)
	var cfg agent.Config
	if err := json.Unmarshal([]byte(cm.Data[agentConfigKey(g.MemberName(0))]), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.EpochFile == "" {
		t.Fatal("no epochFile rendered: the agent keeps its fence inside PGDATA, where a reclone removes it")
	}
	if !strings.HasPrefix(cfg.EpochFile, dataMountPath+"/") {
		t.Errorf("epochFile %q is not on the data volume mounted at %s, so it does not survive the pod", cfg.EpochFile, dataMountPath)
	}
	if rel, err := filepath.Rel(cfg.PGData, cfg.EpochFile); err == nil && !strings.HasPrefix(rel, "..") {
		t.Errorf("epochFile %q is inside pgdata %q", cfg.EpochFile, cfg.PGData)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("the rendered agent config does not validate: %v", err)
	}
}
