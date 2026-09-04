# Chaos experiments

Chaos tests run Chaos Mesh experiments against a kind cluster and evaluate the
oracles in `test/e2e/oracle` while the fault is active. Install Chaos Mesh with
`hack/chaos/install.sh`; run with `go test -tags chaos ./test/chaos/...`.

## Experiment catalogue

Status legend: `implemented` has a manifest and a test under `test/chaos`;
`planned` is a target for a later milestone and has no manifest yet. Update
this column as experiments land.

An experiment asserts an INVARIANT, not recovery. A test that waits for the
pods to come back passes against a cluster that came back having lost data,
which is the failure worth catching and the one a liveness check cannot see.
`pod-kill-primary` is the worked example: it records what the system
acknowledged, injects the fault while writing, and afterwards requires every
acknowledged row to exist exactly once.

Two guards keep such a test from passing vacuously, and a new experiment
needs its own versions of both. The fault must be shown to have landed --
`pod-kill-primary` requires the primary label to move to a different pod, so a
kill that never happened fails rather than passes. And the workload must be
shown to have written on BOTH sides of the fault, or the run proves only that
a healthy cluster accepts writes.

| Experiment | Status | Chaos Mesh kind | Target | Invariants checked |
| --- | --- | --- | --- | --- |
| podchaos-noop | implemented | PodChaos | dummy pod | experiment injects; pipeline works |
| pod-kill-primary | implemented | PodChaos | shard primary pod | no acknowledged commit lost, no row in two places, promotion happened |
| pod-kill-replica | planned | PodChaos | shard replica pod | ledger total, replica catches up, primary unaffected |
| pod-kill-router | planned | PodChaos | router pod | in-flight txn atomicity, client reconnect, no lost commits |
| pod-kill-controller | planned | PodChaos | shard controller pod | primary lease unchanged, no spurious failover |
| pod-kill-operator | planned | PodChaos | operator pod | data plane unaffected, reconcile resumes |
| partition-primary-operator | planned | NetworkChaos (partition) | primary <-> operator | primary keeps serving, no double promotion after heal |
| partition-catalog | planned | NetworkChaos (partition) | catalog <-> everything | routers serve from cache, DDL blocked, recovers on heal |
| router-pooler-loss | planned | NetworkChaos (loss) | router <-> pooler | retries bounded, no duplicated writes |
| router-pooler-delay | planned | NetworkChaos (delay) | router <-> pooler | latency SLO breach reported, no correctness violation |
| io-latency | planned | IOChaos | PostgreSQL data dir | commits durable, ledger total |
| cpu-stress | planned | StressChaos | shard pods | ledger total, no false failover |
| mem-stress | planned | StressChaos | shard pods | OOM handled, no data loss |
| time-skew | planned | TimeChaos | controller / router pods | leases not misjudged, 2PC timeouts sane |

## Invariant checkers

* `oracle.Ledger` - the sum of balances across all shards equals the initial total.
* `oracle.RowSetEquality` - two sources (primary/replica, source/target during resharding) hold identical row sets.
* Further oracles (lease uniqueness, prepared-transaction drain, replication lag) are added alongside the components they check.

## Layout

* `podchaos-noop.yaml` - smoke experiment: kills a dummy pod; proves the pipeline works.
* `noop_test.go` - applies the manifest and waits for the experiment to inject.
* `killprimary_test.go` - kills the shard-0 primary under a write workload and
  asserts no acknowledged commit was lost. It provisions a real HA cluster
  (`replicasPerShard: 3`, `catalog.replicas: 3`): the CRD refuses fewer, and a
  single-replica group cannot fail over at all, so an experiment against one
  would only measure how long a rebuild takes.

The workload and its checker live in `test/e2e/workload` so the experiments
below do not each rebuild them. `AckedLedger` records, per stream, the highest
id whose INSERT was ACKNOWLEDGED -- statements killed in flight have no answer
and are deliberately not asserted about, because the system never promised
them. `workload.Verify` reports a lost acknowledged commit and a row existing
in more than one place separately: uniqueness is enforced per shard, so a copy
that landed on the right shard and a wrong one is invisible to a count taken
from the owner.
