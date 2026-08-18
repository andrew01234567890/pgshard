# Chaos experiments

Chaos tests run Chaos Mesh experiments against a kind cluster and evaluate the
oracles in `test/e2e/oracle` while the fault is active. Install Chaos Mesh with
`hack/chaos/install.sh`; run with `go test -tags chaos ./test/chaos/...`.

## Experiment catalogue

| Experiment | Chaos Mesh kind | Target | Invariants checked |
| --- | --- | --- | --- |
| pod-kill-primary | PodChaos | shard primary pod | ledger total, rowset equality, failover within SLO, no split brain |
| pod-kill-replica | PodChaos | shard replica pod | ledger total, replica catches up, primary unaffected |
| pod-kill-router | PodChaos | router pod | in-flight txn atomicity, client reconnect, no lost commits |
| pod-kill-controller | PodChaos | shard controller pod | primary lease unchanged, no spurious failover |
| pod-kill-operator | PodChaos | operator pod | data plane unaffected, reconcile resumes |
| partition-primary-operator | NetworkChaos (partition) | primary <-> operator | primary keeps serving, no double promotion after heal |
| partition-catalog | NetworkChaos (partition) | catalog <-> everything | routers serve from cache, DDL blocked, recovers on heal |
| router-pooler-loss | NetworkChaos (loss) | router <-> pooler | retries bounded, no duplicated writes |
| router-pooler-delay | NetworkChaos (delay) | router <-> pooler | latency SLO breach reported, no correctness violation |
| io-latency | IOChaos | PostgreSQL data dir | commits durable, ledger total |
| cpu-stress | StressChaos | shard pods | ledger total, no false failover |
| mem-stress | StressChaos | shard pods | OOM handled, no data loss |
| time-skew | TimeChaos | controller / router pods | leases not misjudged, 2PC timeouts sane |

## Invariant checkers

* `oracle.Ledger` - the sum of balances across all shards equals the initial total.
* `oracle.RowSetEquality` - two sources (primary/replica, source/target during resharding) hold identical row sets.
* Further oracles (lease uniqueness, prepared-transaction drain, replication lag) are added alongside the components they check.

## Layout

* `podchaos-noop.yaml` - smoke experiment: kills a dummy pod; proves the pipeline works.
* `noop_test.go` - applies the manifest and waits for the experiment to inject.
