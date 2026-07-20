# pgshard_fence

`pgshard_fence` is a PostgreSQL 18 `shared_preload_libraries` extension for the
writable bootstrap source. Every postmaster starts disarmed. The agent's
owner-only Unix socket plus exact `local postgres postgres peer` HBA rule is
the control-plane authentication boundary. That session installs the canonical
writable-generation bytes and an absolute Linux `CLOCK_BOOTTIME` deadline;
PostgreSQL echoes both values and the agent rejects a non-exact ACK.

Within one postmaster lifetime the identity cannot change, the deadline cannot
move backwards, and an expired fence cannot be rearmed. Lease renewals may only
extend the deadline for the same identity. New ordinary sessions and every SQL
executor or utility statement boundary fail closed while disarmed or expired.
Prepared transactions, normal autovacuum scheduling, parallel workers, and
dynamic background workers are disabled by the agent for this supervised
writable role.
PostgreSQL can still launch anti-wraparound autovacuum workers even with
`autovacuum=off`; that core exception is part of the boundary below.

## PostgreSQL core boundary

An extension has no supported hook that can synchronously interrupt every
already-running backend, idle transaction, or physical walsender at an exact
`CLOCK_BOOTTIME` deadline. PostgreSQL's public timeout API is wall-clock based,
so converting this suspend-aware deadline to `TimestampTz` would weaken the
guarantee across suspend or wall-clock adjustment. This slice therefore does
not claim that target-local fencing alone closes that gap: the agent still
fences and reaps the complete postmaster process tree before its Lease safety
margin, while the extension prevents new sessions and post-deadline SQL
statement starts. Core auxiliary work, including anti-wraparound autovacuum,
also has no extension hook at which this fence can prevent every WAL-writing
operation. Exact in-process interruption and auxiliary-process enforcement
require a small PostgreSQL 18 core patch (or new core hooks) and remain future
work.
