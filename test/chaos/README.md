# Chaos experiments

Chaos tests run Chaos Mesh experiments against a kind cluster and evaluate the
oracles in `test/e2e/oracle` while the fault is active. Install Chaos Mesh with
`hack/chaos/install.sh`; run with `go test -tags chaos ./test/chaos/...`.

## Experiment catalogue

Status legend: `implemented` has a manifest and a test under `test/chaos`;
`planned` is a target for a later milestone and has no manifest yet. Only
`podchaos-noop` exists today; update this column as experiments land.

| Experiment | Status | Chaos Mesh kind | Target | Invariants checked |
| --- | --- | --- | --- | --- |
| podchaos-noop | implemented | PodChaos | dummy pod | experiment injects; pipeline works |
| pod-kill-primary | planned | PodChaos | shard primary pod | ledger total, rowset equality, failover within SLO, no split brain |
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
