# Autoscaling the router

The router is stateless: any instance can serve any session, so the tier
scales horizontally on CPU.

## Configuration

```yaml
spec:
  router:
    minReplicas: 2
    maxReplicas: 10
    hpa:
      cpuUtilization: 70   # percent, 1..100
```

The operator owns a `<cluster>-router` Deployment, a ClusterIP Service on
5432, an `autoscaling/v2` HorizontalPodAutoscaler between `minReplicas` and
`maxReplicas`, and a PodDisruptionBudget (`minAvailable: 1`).

## Why scale-down is safe

- **Drain.** On SIGTERM a router flips `/readyz` to 503 while keeping the
  listener open for `--drain-delay` (5s) so endpoints stop sending new
  connections, then closes the listener, lets open transactions and
  in-flight statements finish, and force-closes the rest after
  `--drain-timeout` (30s). Idle sessions get `FATAL 57P01`; clients with
  reconnect logic ride through a scale-down.
- **Cancel forwarding.** Behind the Service a client's `CancelRequest` can
  land on any pod; routers forward unmatched cancel keys to their peers
  (discovered through the headless peer Service), so query cancel works at
  any replica count.
- **In-doubt transactions.** A router killed mid two-phase commit leaves
  nothing stuck: the durable decision log plus the controller's resolver
  finish the transaction ([transactions.md](transactions.md)).

## Sizing notes

- Sessions are cheap until pinned; `SET` and named prepared statements pin
  a pooler backend per shard touched. The real ceiling is usually the
  poolers' `--max-backends` per shard, not router CPU.
- Scatter fan-out is bounded per router by `--scatter-max-streams` (4096)
  and per statement by `--scatter-max-shards`.
- Watch `pgshard_router_twopc_in_doubt_total` and connection counts on the
  `/metrics` endpoint of the health listener.
