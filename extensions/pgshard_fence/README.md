# pgshard_fence

`pgshard_fence` is the PostgreSQL 18 writable-authority fence used by the
bootstrap source. Every postmaster starts disarmed. The agent's owner-only Unix
socket plus the exact `local postgres postgres peer` HBA rule is the trusted
control-plane boundary. One retained control session installs the canonical
writable-generation bytes and an absolute Linux `CLOCK_BOOTTIME` deadline;
PostgreSQL echoes both values and the agent rejects a non-exact ACK.

Within one postmaster lifetime the identity cannot change, the deadline cannot
move backwards, and an expired fence cannot be rearmed. Only the session that
first installs authority may renew it, and renewals may only extend the deadline
for the same identity. That session's exit irreversibly expires the fence and
increments a shared fence epoch and wakes every other live PostgreSQL process
latch.

## Patched PostgreSQL core

The runtime image builds PostgreSQL 18.4 from upstream commit
`f5cc81719e6da4cbdb1f797c48b693e91018153a` (tag `REL_18_4`) using the official
`postgresql-18.4.tar.gz` release archive. The archive is pinned by
SHA-256
`450aa8f2da06c46f8221916e82ae06b04fb1040f8f00643dbf8b7d663caac0b9`,
and the exact patch is
`patches/postgresql/f5cc81719e6da4cbdb1f797c48b693e91018153a-pgshard-fence.patch`.
The build rejects an archive hash/version mismatch, a patch failure or fuzz, or
a server missing the private fence ABI symbol. The extension also refuses to
build or load without private core ABI 1. Loading the built extension into the
unpatched PostgreSQL 18.4 image is a tested startup failure.
The source-built server retains the base image's exact downstream version
suffix so that image's pinned `initdb` accepts it; the source commit, archive
hash, patch, and private ABI marker are the server's provenance, not that
downstream suffix. Build packages come only from the base image's recorded
Debian snapshots at `20260713T000000Z`; the mutable PGDG package source is
removed before the first package-index update.

The private ABI adds three enforcement points:

1. `CHECK_FOR_INTERRUPTS()` and direct `ProcessInterrupts()` calls invoke the
   fence hook. Every enforced process owns an absolute `CLOCK_BOOTTIME` POSIX
   timer. Deadline expiry or an owner-loss broadcast wakes waits and terminates
   active statements, idle transactions, physical walsenders, autovacuum
   workers, and background workers at their next interrupt-safe boundary.
2. The postmaster consults the fence before launching autovacuum or background
   workers. This includes the emergency anti-wraparound launcher that
   PostgreSQL may request while `autovacuum=off`. Core launcher/worker startup
   rechecks the fence before any background-worker extension entry point,
   closing the pre-entry launch race even for workers that never connect to a
   database. Such workers retain PostgreSQL's original `SIGUSR1` and global
   ProcSignal-barrier contract; the fence never enrolls them as barrier
   participants.
3. `XLogInsert()` denies every new primary-side WAL record without current
   authority. Because many callers have already changed a protected page and
   entered a critical section by then, denial deliberately raises `PANIC` so
   crash recovery restores a valid state instead of continuing without the
   required WAL.

The timer uses `CLOCK_BOOTTIME`, so suspend time is part of the authority
deadline and wall-clock changes cannot extend it. A renewal broadcasts an
immediate recheck so processes replace an older timer deadline without waiting
for unrelated activity. The agent still fences and reaps the complete
postmaster process tree before its Lease safety margin as defense in depth.

## Explicit boundaries

- Enforcement is at interrupt-safe boundaries. PostgreSQL deliberately defers
  errors while interrupts are held or a critical section is active; work that
  already crossed a fence check may finish that bounded section first.
- A third-party no-database background worker that never services its process
  latch or calls `CHECK_FOR_INTERRUPTS()` cannot be forced to exit safely from
  a signal handler. It is not allowed to block PostgreSQL's global ProcSignal
  barriers, every attempted WAL insertion remains core-fenced, and the agent's
  bounded whole-process-tree reap remains the final termination boundary.
- Startup, `RecoveryInProgress()`, and the checkpointer's narrowly scoped
  end-of-recovery WAL are allowed so crash recovery and its final checkpoint
  can complete. Ordinary connections remain rejected during a disarmed
  recovery/startup.
- Supervised replication standbys treat promotion (`RecoveryEnded`) as
  terminal and the agent reaps the server. This fence does not authorize a
  promoted standby or exempt its later online checkpoint; continuation is not
  claimed.
- The peer-authenticated retained control backend is trusted and exempt so it
  can create the extension, install authority, and renew it. Compromise of the
  PostgreSQL operating-system account or this private socket is outside this
  boundary.
- Normal control-backend exit runs PostgreSQL's shared-memory exit callback and
  broadcasts immediate owner loss. An uncatchable backend `SIGKILL` skips that
  callback; the retained agent session detects the loss and reaps PostgreSQL,
  while the already-installed absolute deadline remains the final fail-closed
  bound.
- A denied auxiliary WAL insert uses crash recovery rather than pretending the
  caller can safely unwind. Immediate PostgreSQL shutdown is therefore the
  expected disarmed shutdown mode; a clean shutdown checkpoint is itself a new
  WAL record and remains fenced.
- The timer implementation is Linux/POSIX-specific and reserves `SIGRTMIN` in
  PostgreSQL server processes. A PostgreSQL upgrade requires pinning a new
  source commit and archive hash, rebasing this patch without fuzz, rebuilding
  the extension, and rerunning the live ABI, expiry, recovery, and process
  tests.
