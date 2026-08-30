package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pgshardv1alpha1 "github.com/andrew01234567890/pgshard/api/v1alpha1"
	"github.com/andrew01234567890/pgshard/internal/controller"
)

// Workflow kinds the reshards panel reads from pgshard.workflows.
const (
	WorkflowKindReshard   = controller.KindReshard
	WorkflowKindPlacement = controller.KindTablePlacement
	WorkflowKindUpgrade   = "upgrade"
)

// WorkflowRecord is one row of pgshard.workflows with its JSON documents
// left opaque; the panel interprets what it knows and ignores the rest.
type WorkflowRecord struct {
	ID         string
	Kind       string
	State      string
	Spec       json.RawMessage
	Status     json.RawMessage
	JournalIDs []string
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Error      string
}

// WorkflowSource lists the workflows of the catalog database. A
// CatalogSource that also implements it feeds the reshards panel.
type WorkflowSource interface {
	Workflows(ctx context.Context) ([]WorkflowRecord, error)
}

// Progress is one copy progress measurement, as the controller aggregates
// it in workflows.status.progress (and per target under status.targets
// when the copier reports that granularity).
type Progress struct {
	Subscriptions int   `json:"subscriptions"`
	TablesTotal   int   `json:"tablesTotal"`
	TablesReady   int   `json:"tablesReady"`
	LagBytes      int64 `json:"lagBytes"`
	LagKnown      bool  `json:"lagKnown"`
	Paused        int   `json:"paused"`
	// Percent is tables ready over total, 0 with no tables.
	Percent int `json:"percent"`
}

// ReshardTarget is one target shard: readiness from the record, copy
// progress from the workflow.
type ReshardTarget struct {
	ShardID    int       `json:"shardId"`
	Group      string    `json:"group"`
	Ready      bool      `json:"ready"`
	Primary    string    `json:"primary,omitempty"`
	RangeStart int64     `json:"rangeStart"`
	RangeEnd   int64     `json:"rangeEnd"`
	Progress   *Progress `json:"progress,omitempty"`
}

// StageEvent is one entry of the stage timeline.
type StageEvent struct {
	Stage string    `json:"stage"`
	At    time.Time `json:"at"`
	Note  string    `json:"note,omitempty"`
}

// VerifyResult is the cutover verify report.
type VerifyResult struct {
	Tables     int       `json:"tables"`
	Rows       int64     `json:"rows"`
	Mismatches []string  `json:"mismatches"`
	CheckedAt  time.Time `json:"checkedAt"`
}

// Cutover is the switch record of a reshard workflow.
type Cutover struct {
	SourceSet   string        `json:"sourceSet,omitempty"`
	Step        string        `json:"step,omitempty"`
	Attempts    int           `json:"attempts"`
	Gate        string        `json:"gate,omitempty"`
	FencedAt    *time.Time    `json:"fencedAt,omitempty"`
	SwitchedAt  *time.Time    `json:"switchedAt,omitempty"`
	FlippedAt   *time.Time    `json:"flippedAt,omitempty"`
	ReleasedAt  *time.Time    `json:"releasedAt,omitempty"`
	Pause       string        `json:"pause,omitempty"`
	Fence       string        `json:"fence,omitempty"`
	RetireAt    *time.Time    `json:"retireAt,omitempty"`
	PauseBefore string        `json:"pauseBefore,omitempty"`
	Proceed     []string      `json:"proceed,omitempty"`
	Aborts      []string      `json:"aborts,omitempty"`
	Verify      *VerifyResult `json:"verify,omitempty"`
}

// Reshard is one PgShardReshard joined with its workflow.
type Reshard struct {
	Namespace     string          `json:"namespace"`
	Name          string          `json:"name"`
	Cluster       string          `json:"cluster"`
	FromGen       int64           `json:"fromGeneration"`
	ToGen         int64           `json:"targetGeneration"`
	ToShards      int             `json:"targetShards"`
	TargetSet     string          `json:"targetShardSet"`
	Phase         string          `json:"phase"`
	Stage         string          `json:"stage,omitempty"`
	State         string          `json:"workflowState,omitempty"`
	WorkflowID    string          `json:"workflowId,omitempty"`
	StartedAt     *time.Time      `json:"startedAt,omitempty"`
	UpdatedAt     *time.Time      `json:"updatedAt,omitempty"`
	TargetsReady  int             `json:"targetsReady"`
	Targets       []ReshardTarget `json:"targets"`
	Progress      *Progress       `json:"progress,omitempty"`
	CutoverPause  string          `json:"cutoverPause,omitempty"`
	JournalIDs    []string        `json:"journalIds"`
	Message       string          `json:"message,omitempty"`
	Error         string          `json:"error,omitempty"`
	Cutover       *Cutover        `json:"cutover,omitempty"`
	Timeline      []StageEvent    `json:"timeline"`
	CancelledAt   *time.Time      `json:"cancelledAt,omitempty"`
	CancelReason  string          `json:"cancelReason,omitempty"`
	WorkflowFound bool            `json:"workflowFound"`
}

// Placement is a table_placement workflow as the UI shows it.
type Placement struct {
	ID        string    `json:"id"`
	Database  string    `json:"database"`
	Table     string    `json:"table"`
	From      string    `json:"from"`
	To        string    `json:"to"`
	State     string    `json:"state"`
	Stage     string    `json:"stage,omitempty"`
	Progress  *Progress `json:"progress,omitempty"`
	Message   string    `json:"message,omitempty"`
	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// Upgrade is an upgrade workflow; the panel shows what it records.
type Upgrade struct {
	ID        string    `json:"id"`
	State     string    `json:"state"`
	Stage     string    `json:"stage,omitempty"`
	From      string    `json:"from,omitempty"`
	To        string    `json:"to,omitempty"`
	Message   string    `json:"message,omitempty"`
	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// ClusterReshards is the reshards panel of one cluster.
type ClusterReshards struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	Shards    int       `json:"shards"`
	Active    string    `json:"activeReshard,omitempty"`
	Reshards  []Reshard `json:"reshards"`
}

// ReshardsPage is the /reshards document.
type ReshardsPage struct {
	Clusters     []ClusterReshards `json:"clusters"`
	Placements   []Placement       `json:"placements"`
	Upgrades     []Upgrade         `json:"upgrades"`
	CatalogError string            `json:"catalogError,omitempty"`
}

// ReshardCards are the overview cards of the clusters list.
type ReshardCards struct {
	InProgress       int    `json:"reshardsInProgress"`
	LastCutoverPause string `json:"lastCutoverPause,omitempty"`
	LastCutover      string `json:"lastCutover,omitempty"`
}

var terminalPhases = map[string]bool{
	pgshardv1alpha1.ReshardPhaseCompleted: true,
	pgshardv1alpha1.ReshardPhaseCancelled: true,
	pgshardv1alpha1.ReshardPhaseFailed:    true,
}

// BuildReshardsPage assembles the reshards panel for every cluster in namespace.
func BuildReshardsPage(ctx context.Context, c client.Reader, src CatalogSource, namespace, cluster string) (*ReshardsPage, error) {
	var clusters pgshardv1alpha1.PgShardClusterList
	if err := c.List(ctx, &clusters, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	clusters.Items = onlyCluster(clusters.Items, cluster)
	page := &ReshardsPage{Clusters: []ClusterReshards{}, Placements: []Placement{}, Upgrades: []Upgrade{}}
	workflows, err := loadWorkflows(ctx, src)
	if err != nil {
		page.CatalogError = err.Error()
	}
	reshards, err := ListReshards(ctx, c, namespace, cluster, workflows)
	if err != nil {
		return nil, err
	}
	for i := range clusters.Items {
		pc := &clusters.Items[i]
		cr := ClusterReshards{Namespace: pc.Namespace, Name: pc.Name, Reshards: []Reshard{}}
		if pc.Spec.Shards != nil {
			cr.Shards = *pc.Spec.Shards
		}
		if pc.Status.Reshard != nil {
			cr.Active = pc.Status.Reshard.Name
		}
		for _, r := range reshards {
			if r.Namespace == pc.Namespace && r.Cluster == pc.Name {
				cr.Reshards = append(cr.Reshards, r)
			}
		}
		page.Clusters = append(page.Clusters, cr)
	}
	sort.Slice(page.Clusters, func(i, j int) bool {
		a, b := page.Clusters[i], page.Clusters[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	for _, w := range workflows {
		switch w.Kind {
		case WorkflowKindPlacement:
			page.Placements = append(page.Placements, convertPlacement(w))
		case WorkflowKindUpgrade:
			page.Upgrades = append(page.Upgrades, convertUpgrade(w))
		}
	}
	return page, nil
}

// BuildReshardCards derives the overview cards from the reshards in namespace.
func BuildReshardCards(ctx context.Context, c client.Reader, namespace, cluster string) (*ReshardCards, error) {
	reshards, err := ListReshards(ctx, c, namespace, cluster, nil)
	if err != nil {
		return nil, err
	}
	cards := &ReshardCards{}
	var latest *Reshard
	for i := range reshards {
		r := &reshards[i]
		if !terminalPhases[r.Phase] {
			cards.InProgress++
		}
		if r.CutoverPause != "" && latest == nil {
			latest = r
		}
	}
	if latest != nil {
		cards.LastCutoverPause = latest.CutoverPause
		cards.LastCutover = latest.Namespace + "/" + latest.Name
	}
	return cards, nil
}

// ListReshards returns every PgShardReshard in namespace, newest first,
// joined with workflows by status.workflowId.
func ListReshards(ctx context.Context, c client.Reader, namespace, cluster string, workflows []WorkflowRecord) ([]Reshard, error) {
	var list pgshardv1alpha1.PgShardReshardList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	byID := indexWorkflows(workflows)
	out := make([]Reshard, 0, len(list.Items))
	for i := range list.Items {
		rs := convertReshard(&list.Items[i], byID)
		if cluster != "" && rs.Cluster != cluster {
			continue
		}
		out = append(out, rs)
	}
	sort.Slice(out, func(i, j int) bool { return newerFirst(out[i].StartedAt, out[i].Name, out[j].StartedAt, out[j].Name) })
	return out, nil
}

// GetReshard reads one PgShardReshard and its workflow.
func GetReshard(ctx context.Context, c client.Reader, src CatalogSource, namespace, name, cluster string) (*Reshard, error) {
	var r pgshardv1alpha1.PgShardReshard
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &r); err != nil {
		return nil, err
	}
	workflows, err := loadWorkflows(ctx, src)
	out := convertReshard(&r, indexWorkflows(workflows))
	if cluster != "" && out.Cluster != cluster {
		return nil, notServed("reshard", name)
	}
	if err != nil {
		out.Error = joinErr(out.Error, "catalog: "+err.Error())
	}
	return &out, nil
}

func loadWorkflows(ctx context.Context, src CatalogSource) ([]WorkflowRecord, error) {
	ws, ok := src.(WorkflowSource)
	if !ok {
		return nil, nil
	}
	return ws.Workflows(ctx)
}

func indexWorkflows(workflows []WorkflowRecord) map[string]WorkflowRecord {
	byID := make(map[string]WorkflowRecord, len(workflows))
	for _, w := range workflows {
		byID[w.ID] = w
	}
	return byID
}

func joinErr(a, b string) string {
	if a == "" {
		return b
	}
	return a + "; " + b
}

// reshardStatus is the subset of workflows.status the panel reads.
type reshardStatus struct {
	Stage    string                             `json:"stage"`
	Message  string                             `json:"message"`
	Reason   string                             `json:"reason"`
	Progress *controller.CopyProgress           `json:"progress"`
	Targets  map[string]controller.CopyProgress `json:"targets"`
	History  []StageEvent                       `json:"history"`
	Cutover  *struct {
		SourceSet  string     `json:"source_set"`
		Step       string     `json:"step"`
		Attempts   int        `json:"attempts"`
		Gate       string     `json:"gate"`
		FencedAt   *time.Time `json:"fenced_at"`
		SwitchedAt *time.Time `json:"switched_at"`
		FlippedAt  *time.Time `json:"flipped_at"`
		ReleasedAt *time.Time `json:"released_at"`
		PauseMS    int64      `json:"pause_ms"`
		FenceMS    int64      `json:"fence_ms"`
		Aborts     []string   `json:"aborts"`
		Verify     *struct {
			Tables     int       `json:"tables"`
			Rows       int64     `json:"rows"`
			Mismatches []string  `json:"mismatches"`
			CheckedAt  time.Time `json:"CheckedAt"`
		} `json:"verify"`
	} `json:"cutover"`
}

type reshardSpec struct {
	PauseBefore        string   `json:"pause_before"`
	Proceed            []string `json:"proceed"`
	RetireAfterSeconds int64    `json:"retire_after_seconds"`
}

func convertReshard(r *pgshardv1alpha1.PgShardReshard, byID map[string]WorkflowRecord) Reshard {
	created := r.CreationTimestamp.Time
	out := Reshard{
		Namespace: r.Namespace, Name: r.Name, Cluster: r.Spec.ClusterName,
		FromGen: r.Spec.FromGeneration, ToGen: r.Spec.TargetGeneration, ToShards: r.Spec.TargetShards, TargetSet: r.Spec.TargetShardSet,
		Phase: r.Status.Phase, WorkflowID: r.Status.WorkflowID, Message: r.Status.Message,
		JournalIDs: append([]string{}, r.Status.JournalIDs...), Targets: []ReshardTarget{}, Timeline: []StageEvent{},
	}
	if !created.IsZero() {
		out.StartedAt = &created
	}
	if out.Phase == "" {
		out.Phase = pgshardv1alpha1.ReshardPhasePending
	}
	if r.Status.CutoverPause != nil {
		out.CutoverPause = r.Status.CutoverPause.Duration.String()
	}
	ranges := map[int]pgshardv1alpha1.ReshardRange{}
	for _, rg := range r.Spec.TargetRanges {
		ranges[rg.ShardID] = rg
	}
	targetsByID := map[int]pgshardv1alpha1.ReshardTargetStatus{}
	for _, t := range r.Status.Targets {
		targetsByID[t.ShardID] = t
	}
	ids := make([]int, 0, len(ranges)+len(targetsByID))
	for id := range ranges {
		ids = append(ids, id)
	}
	for id := range targetsByID {
		if _, ok := ranges[id]; !ok {
			ids = append(ids, id)
		}
	}
	sort.Ints(ids)
	for _, id := range ids {
		t := ReshardTarget{ShardID: id, RangeStart: ranges[id].RangeStart, RangeEnd: ranges[id].RangeEnd}
		if st, ok := targetsByID[id]; ok {
			t.Group, t.Ready, t.Primary = st.Group, st.Ready, st.Primary
			if st.Ready {
				out.TargetsReady++
			}
		}
		out.Targets = append(out.Targets, t)
	}
	for _, cond := range r.Status.Conditions {
		if cond.Status == "True" && !cond.LastTransitionTime.IsZero() {
			out.Timeline = append(out.Timeline, StageEvent{Stage: cond.Type, At: cond.LastTransitionTime.Time, Note: cond.Message})
		}
	}
	if w, ok := byID[r.Status.WorkflowID]; ok && r.Status.WorkflowID != "" {
		applyWorkflow(&out, w)
	}
	sort.SliceStable(out.Timeline, func(i, j int) bool { return out.Timeline[i].At.Before(out.Timeline[j].At) })
	return out
}

func applyWorkflow(out *Reshard, w WorkflowRecord) {
	out.WorkflowFound = true
	out.State = w.State
	out.Error = w.Error
	updated := w.UpdatedAt
	out.UpdatedAt = &updated
	if len(out.JournalIDs) == 0 {
		out.JournalIDs = append(out.JournalIDs, w.JournalIDs...)
	}
	var st reshardStatus
	if err := json.Unmarshal(w.Status, &st); err != nil {
		out.Error = joinErr(out.Error, "workflow status: "+err.Error())
		return
	}
	var spec reshardSpec
	_ = json.Unmarshal(w.Spec, &spec)
	out.Stage = st.Stage
	if out.Message == "" {
		out.Message = st.Message
	}
	if st.Progress != nil {
		out.Progress = convertProgress(*st.Progress)
	}
	for i := range out.Targets {
		if p, ok := st.Targets[strconv.Itoa(out.Targets[i].ShardID)]; ok {
			out.Targets[i].Progress = convertProgress(p)
		}
	}
	out.Timeline = append(out.Timeline, StageEvent{Stage: "created", At: w.CreatedAt})
	out.Timeline = append(out.Timeline, st.History...)
	switch w.State {
	case controller.StateCancelled:
		out.CancelledAt = &updated
		out.CancelReason = firstNonEmpty(st.Reason, st.Message)
	case controller.StateFailed:
		out.Timeline = append(out.Timeline, StageEvent{Stage: controller.StageFailed, At: updated, Note: w.Error})
	}
	if c := st.Cutover; c != nil {
		cv := &Cutover{SourceSet: c.SourceSet, Step: c.Step, Attempts: c.Attempts, Gate: c.Gate,
			FencedAt: c.FencedAt, SwitchedAt: c.SwitchedAt, FlippedAt: c.FlippedAt, ReleasedAt: c.ReleasedAt,
			PauseBefore: spec.PauseBefore, Proceed: spec.Proceed, Aborts: c.Aborts}
		if c.PauseMS > 0 {
			cv.Pause = (time.Duration(c.PauseMS) * time.Millisecond).String()
			if out.CutoverPause == "" {
				out.CutoverPause = cv.Pause
			}
		}
		if c.FenceMS > 0 {
			cv.Fence = (time.Duration(c.FenceMS) * time.Millisecond).String()
		}
		if c.SwitchedAt != nil {
			retire := c.SwitchedAt.Add(retireAfter(spec.RetireAfterSeconds))
			cv.RetireAt = &retire
		}
		if c.Verify != nil {
			cv.Verify = &VerifyResult{Tables: c.Verify.Tables, Rows: c.Verify.Rows, Mismatches: append([]string{}, c.Verify.Mismatches...), CheckedAt: c.Verify.CheckedAt}
		}
		for _, ev := range []struct {
			stage string
			at    *time.Time
		}{{"fenced", c.FencedAt}, {"flipped", c.FlippedAt}, {"released", c.ReleasedAt}, {controller.StageSwitched, c.SwitchedAt}} {
			if ev.at != nil {
				out.Timeline = append(out.Timeline, StageEvent{Stage: ev.stage, At: *ev.at})
			}
		}
		out.Cutover = cv
	}
	if len(st.History) == 0 && st.Stage != "" {
		out.Timeline = append(out.Timeline, StageEvent{Stage: st.Stage, At: updated, Note: "current"})
	}
}

func retireAfter(seconds int64) time.Duration {
	if seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return controller.DefaultRetireAfter
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func convertProgress(p controller.CopyProgress) *Progress {
	out := &Progress{Subscriptions: p.Subscriptions, TablesTotal: p.TablesTotal, TablesReady: p.TablesReady, Paused: p.Paused}
	if p.LagBytes >= 0 {
		out.LagKnown, out.LagBytes = true, p.LagBytes
	}
	out.Percent = percent(p.TablesReady, p.TablesTotal)
	return out
}

func percent(done, total int) int {
	if total <= 0 || done <= 0 {
		return 0
	}
	if done >= total {
		return 100
	}
	return done * 100 / total
}

type placementSpec struct {
	Database   string `json:"database"`
	SchemaName string `json:"schema_name"`
	TableName  string `json:"table_name"`
	From       struct {
		Placement string  `json:"placement"`
		ShardKey  *string `json:"shard_key"`
	} `json:"from"`
	To struct {
		Placement string  `json:"placement"`
		ShardKey  *string `json:"shard_key"`
	} `json:"to"`
}

func describePlacement(placement string, key *string) string {
	if key != nil && *key != "" {
		return placement + "(" + *key + ")"
	}
	return placement
}

func convertPlacement(w WorkflowRecord) Placement {
	out := Placement{ID: w.ID, State: w.State, Error: w.Error, CreatedAt: w.CreatedAt, UpdatedAt: w.UpdatedAt}
	var spec placementSpec
	if err := json.Unmarshal(w.Spec, &spec); err != nil {
		out.Error = joinErr(out.Error, "workflow spec: "+err.Error())
	}
	out.Database = spec.Database
	out.Table = spec.SchemaName + "." + spec.TableName
	if spec.SchemaName == "" && spec.TableName == "" {
		out.Table = ""
	}
	out.From = describePlacement(spec.From.Placement, spec.From.ShardKey)
	out.To = describePlacement(spec.To.Placement, spec.To.ShardKey)
	var st reshardStatus
	if err := json.Unmarshal(w.Status, &st); err != nil {
		out.Error = joinErr(out.Error, "workflow status: "+err.Error())
	}
	out.Stage, out.Message = st.Stage, st.Message
	if st.Progress != nil {
		out.Progress = convertProgress(*st.Progress)
	}
	return out
}

func convertUpgrade(w WorkflowRecord) Upgrade {
	out := Upgrade{ID: w.ID, State: w.State, Error: w.Error, CreatedAt: w.CreatedAt, UpdatedAt: w.UpdatedAt}
	var spec struct {
		From json.RawMessage `json:"from"`
		To   json.RawMessage `json:"to"`
	}
	_ = json.Unmarshal(w.Spec, &spec)
	out.From, out.To = rawText(spec.From), rawText(spec.To)
	var st reshardStatus
	_ = json.Unmarshal(w.Status, &st)
	out.Stage, out.Message = st.Stage, st.Message
	return out
}

func rawText(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var n json.Number
	if json.Unmarshal(raw, &n) == nil {
		return n.String()
	}
	if len(raw) == 0 {
		return ""
	}
	return fmt.Sprint(string(raw))
}
