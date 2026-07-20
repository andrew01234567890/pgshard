/*
 * pgshard_fence: PostgreSQL 18 shared-preload writable-authority target.
 *
 * Each postmaster starts disarmed. Only the postgres/postgres local peer
 * control backend may install an exact canonical identity and absolute Linux
 * CLOCK_BOOTTIME deadline. The matching private core ABI interrupts running
 * processes and rejects new primary WAL while disarmed or expired.
 */

#include "postgres.h"

#include <signal.h>
#include <sys/socket.h>
#include <time.h>

#include "access/htup_details.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq/auth.h"
#include "libpq/hba.h"
#include "libpq/libpq-be.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/wait_classes.h"

PG_MODULE_MAGIC;

#define PGSHARD_FENCE_MAX_IDENTITY_BYTES 1024
#define PGSHARD_FENCE_DEADLINE_BYTES 8
#define PGSHARD_FENCE_TIMER_SIGNAL SIGRTMIN

#if !defined(PGSHARD_FENCE_CORE_ABI) || PGSHARD_FENCE_CORE_ABI != 1
#error "pgshard_fence requires the pinned pgshard PostgreSQL core ABI 1"
#endif

typedef struct PgshardFenceState
{
	pg_atomic_uint64 epoch;
	bool	armed;
	bool	expired;
	uint16	identity_length;
	int32	control_pid;
	uint64	deadline_boottime_ns;
	unsigned char identity[PGSHARD_FENCE_MAX_IDENTITY_BYTES];
} PgshardFenceState;

static PgshardFenceState *fence_state = NULL;
static LWLock *fence_lock = NULL;
static bool control_backend = false;
static bool control_owner = false;
static bool control_exit_callback_registered = false;
static bool fence_timer_created = false;
static timer_t fence_timer;
static uint64 fence_timer_deadline = 0;
static uint64 fence_epoch_seen = 0;
static volatile sig_atomic_t fence_timer_pending = false;
static BackgroundWorkerHandle *barrier_test_worker_handle = NULL;

static shmem_request_hook_type previous_shmem_request_hook = NULL;
static shmem_startup_hook_type previous_shmem_startup_hook = NULL;
static ClientAuthentication_hook_type previous_client_authentication_hook = NULL;
static ExecutorStart_hook_type previous_executor_start_hook = NULL;
static ProcessUtility_hook_type previous_process_utility_hook = NULL;

void		_PG_init(void);
Datum		pgshard_fence_install(PG_FUNCTION_ARGS);
Datum		pgshard_fence_test_request_autovacuum(PG_FUNCTION_ARGS);
Datum		pgshard_fence_test_start_background_worker(PG_FUNCTION_ARGS);
Datum		pgshard_fence_test_start_barrier_worker(PG_FUNCTION_ARGS);
Datum		pgshard_fence_test_stop_barrier_worker(PG_FUNCTION_ARGS);
PGDLLEXPORT void pgshard_fence_test_background_worker_main(Datum main_arg);
PGDLLEXPORT void pgshard_fence_test_barrier_worker_main(Datum main_arg);

PG_FUNCTION_INFO_V1(pgshard_fence_install);
PG_FUNCTION_INFO_V1(pgshard_fence_test_request_autovacuum);
PG_FUNCTION_INFO_V1(pgshard_fence_test_start_background_worker);
PG_FUNCTION_INFO_V1(pgshard_fence_test_start_barrier_worker);
PG_FUNCTION_INFO_V1(pgshard_fence_test_stop_barrier_worker);

static void pgshard_fence_shmem_request(void);
static void pgshard_fence_shmem_startup(void);
static void pgshard_fence_client_authentication(Port *port, int status);
static void pgshard_fence_executor_start(QueryDesc *query_desc, int eflags);
static void pgshard_fence_process_utility(PlannedStmt *pstmt,
										 const char *query_string,
										 bool read_only_tree,
										 ProcessUtilityContext context,
										 ParamListInfo params,
										 QueryEnvironment *query_env,
										 DestReceiver *dest,
										 QueryCompletion *qc);
static void pgshard_fence_require_authority(void);
static PgshardFenceAuthorityStatus pgshard_fence_authority_status(uint64 *deadline,
															 uint64 *epoch);
static void pgshard_fence_interrupt(void);
static PgshardFenceAuthorityStatus pgshard_fence_wal_insert_allowed(void);
static bool pgshard_fence_process_is_enforced(void);
static void pgshard_fence_arm_timer(uint64 deadline);
static void pgshard_fence_timer_handler(SIGNAL_ARGS);
static void pgshard_fence_control_exit(int code, Datum arg);
static void pgshard_fence_signal_processes(void);
static Datum pgshard_fence_test_start_worker(const char *function_name,
											 const char *worker_name,
											 BackgroundWorkerHandle **saved_handle);
static bool pgshard_fence_is_control_backend(Port *port, int status);
static bool pgshard_fence_boottime_now(uint64 *result);
static bool pgshard_fence_decode_deadline(bytea *value, uint64 *result);
static bytea *pgshard_fence_copy_bytea(const unsigned char *bytes, Size length);
static bytea *pgshard_fence_deadline_bytea(uint64 value);

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pgshard_fence must be loaded through shared_preload_libraries")));
	if (PgshardFenceCoreAbi != PGSHARD_FENCE_CORE_ABI ||
		PgshardFenceInterruptHook != NULL || PgshardFenceWalInsertHook != NULL)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pgshard_fence requires an unused PostgreSQL core fence ABI %d",
						PGSHARD_FENCE_CORE_ABI)));

	PgshardFenceInterruptHook = pgshard_fence_interrupt;
	PgshardFenceWalInsertHook = pgshard_fence_wal_insert_allowed;
	pqsignal(PGSHARD_FENCE_TIMER_SIGNAL, pgshard_fence_timer_handler);
	/* The absolute fence must fire before arbitrary bgworker entry code. */
	sigdelset(&BlockSig, PGSHARD_FENCE_TIMER_SIGNAL);
	sigdelset(&StartupBlockSig, PGSHARD_FENCE_TIMER_SIGNAL);

	previous_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pgshard_fence_shmem_request;
	previous_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgshard_fence_shmem_startup;
	previous_client_authentication_hook = ClientAuthentication_hook;
	ClientAuthentication_hook = pgshard_fence_client_authentication;
	previous_executor_start_hook = ExecutorStart_hook;
	ExecutorStart_hook = pgshard_fence_executor_start;
	previous_process_utility_hook = ProcessUtility_hook;
	ProcessUtility_hook = pgshard_fence_process_utility;
}

static void
pgshard_fence_shmem_request(void)
{
	if (previous_shmem_request_hook)
		previous_shmem_request_hook();
	RequestAddinShmemSpace(MAXALIGN(sizeof(PgshardFenceState)));
	RequestNamedLWLockTranche("pgshard_fence", 1);
}

static void
pgshard_fence_shmem_startup(void)
{
	bool		found;

	if (previous_shmem_startup_hook)
		previous_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	fence_state = ShmemInitStruct("pgshard_fence state",
								  sizeof(PgshardFenceState),
								  &found);
	fence_lock = &GetNamedLWLockTranche("pgshard_fence")[0].lock;
	if (!found)
	{
		MemSet(fence_state, 0, sizeof(PgshardFenceState));
		pg_atomic_init_u64(&fence_state->epoch, 1);
	}
	LWLockRelease(AddinShmemInitLock);
}

static void
pgshard_fence_client_authentication(Port *port, int status)
{
	if (previous_client_authentication_hook)
		previous_client_authentication_hook(port, status);

	if (status != STATUS_OK)
		return;

	control_backend = pgshard_fence_is_control_backend(port, status);
	pgshard_fence_require_authority();
}

static bool
pgshard_fence_is_control_backend(Port *port, int status)
{
	return status == STATUS_OK &&
		port != NULL &&
		port->hba != NULL &&
		port->raddr.addr.ss_family == AF_UNIX &&
		port->hba->auth_method == uaPeer &&
		MyClientConnectionInfo.authn_id != NULL &&
		strcmp(MyClientConnectionInfo.authn_id, "postgres") == 0 &&
		port->user_name != NULL && strcmp(port->user_name, "postgres") == 0 &&
		port->database_name != NULL && strcmp(port->database_name, "postgres") == 0;
}

static void
pgshard_fence_executor_start(QueryDesc *query_desc, int eflags)
{
	pgshard_fence_require_authority();
	if (previous_executor_start_hook)
		previous_executor_start_hook(query_desc, eflags);
	else
		standard_ExecutorStart(query_desc, eflags);
}

static void
pgshard_fence_process_utility(PlannedStmt *pstmt,
								  const char *query_string,
								  bool read_only_tree,
								  ProcessUtilityContext context,
								  ParamListInfo params,
								  QueryEnvironment *query_env,
								  DestReceiver *dest,
								  QueryCompletion *qc)
{
	pgshard_fence_require_authority();
	if (previous_process_utility_hook)
		previous_process_utility_hook(pstmt, query_string, read_only_tree,
								  context, params, query_env, dest, qc);
	else
		standard_ProcessUtility(pstmt, query_string, read_only_tree,
								context, params, query_env, dest, qc);
}

static void
pgshard_fence_require_authority(void)
{
	uint64		deadline;
	uint64		epoch;

	if (control_backend)
		return;
	if (pgshard_fence_authority_status(&deadline, &epoch) !=
		PGSHARD_FENCE_AUTHORITY_ALLOWED)
		ereport(FATAL,
				(errcode(ERRCODE_ADMIN_SHUTDOWN),
				 errmsg("pgshard writable authority is not installed or has expired")));
	fence_epoch_seen = epoch;
	pgshard_fence_arm_timer(deadline);
}

static PgshardFenceAuthorityStatus
pgshard_fence_authority_status(uint64 *deadline, uint64 *epoch)
{
	bool		armed;
	bool		expired;
	uint64		installed_deadline;
	uint64		installed_epoch;
	uint64		now;

	if (fence_state == NULL || fence_lock == NULL)
		return PGSHARD_FENCE_AUTHORITY_DENIED;

	/* The postmaster cannot queue for an LWLock because it has no PGPROC. */
	if (MyProc == NULL)
	{
		if (!LWLockConditionalAcquire(fence_lock, LW_SHARED))
			return PGSHARD_FENCE_AUTHORITY_RETRY;
	}
	else
		LWLockAcquire(fence_lock, LW_SHARED);
	armed = fence_state->armed;
	expired = fence_state->expired;
	installed_deadline = fence_state->deadline_boottime_ns;
	installed_epoch = pg_atomic_read_u64(&fence_state->epoch);
	if (!pgshard_fence_boottime_now(&now))
	{
		LWLockRelease(fence_lock);
		return PGSHARD_FENCE_AUTHORITY_DENIED;
	}
	LWLockRelease(fence_lock);
	if (!armed || expired || now >= installed_deadline)
		return PGSHARD_FENCE_AUTHORITY_DENIED;
	if (deadline != NULL)
		*deadline = installed_deadline;
	if (epoch != NULL)
		*epoch = installed_epoch;
	return PGSHARD_FENCE_AUTHORITY_ALLOWED;
}

static void
pgshard_fence_interrupt(void)
{
	uint64		deadline;
	uint64		epoch;
	uint64		shared_epoch;

	if (InterruptHoldoffCount != 0 || CritSectionCount != 0 ||
		ClientAuthInProgress || control_backend ||
		!pgshard_fence_process_is_enforced())
		return;
	shared_epoch = fence_state == NULL ? 0 :
		pg_atomic_read_u64(&fence_state->epoch);
	if (fence_timer_created && !fence_timer_pending &&
		shared_epoch == fence_epoch_seen)
		return;

	fence_timer_pending = false;
	if (pgshard_fence_authority_status(&deadline, &epoch) !=
		PGSHARD_FENCE_AUTHORITY_ALLOWED)
	{
		/* Preserve the launcher's shared-memory PID cleanup on normal exit. */
		if (AmAutoVacuumLauncherProcess())
			PgshardFenceAutoVacLauncherShutdown();
		ereport(FATAL,
				(errcode(ERRCODE_ADMIN_SHUTDOWN),
				 errmsg("pgshard writable authority is not installed or has expired")));
	}
	fence_epoch_seen = epoch;
	pgshard_fence_arm_timer(deadline);
}

static PgshardFenceAuthorityStatus
pgshard_fence_wal_insert_allowed(void)
{
	if (control_backend)
		return PGSHARD_FENCE_AUTHORITY_ALLOWED;
	return pgshard_fence_authority_status(NULL, NULL);
}

static bool
pgshard_fence_process_is_enforced(void)
{
	return AmRegularBackendProcess() || AmAutoVacuumLauncherProcess() ||
		AmAutoVacuumWorkerProcess() || AmBackgroundWorkerProcess() ||
		AmWalSenderProcess() || AmLogicalSlotSyncWorkerProcess();
}

static void
pgshard_fence_arm_timer(uint64 deadline)
{
	struct sigevent event;
	struct itimerspec when;
	uint64		seconds = deadline / UINT64CONST(1000000000);

	if (!fence_timer_created)
	{
		MemSet(&event, 0, sizeof(event));
		event.sigev_notify = SIGEV_SIGNAL;
		event.sigev_signo = PGSHARD_FENCE_TIMER_SIGNAL;
		if (timer_create(CLOCK_BOOTTIME, &event, &fence_timer) != 0)
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("could not create pgshard CLOCK_BOOTTIME fence timer: %m")));
		fence_timer_created = true;
	}
	if (fence_timer_deadline == deadline)
		return;

	MemSet(&when, 0, sizeof(when));
	when.it_value.tv_sec = (time_t) seconds;
	when.it_value.tv_nsec = (long) (deadline % UINT64CONST(1000000000));
	if (when.it_value.tv_sec < 0 || (uint64) when.it_value.tv_sec != seconds)
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pgshard fence deadline exceeds the platform timer range")));
	if (timer_settime(fence_timer, TIMER_ABSTIME, &when, NULL) != 0)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not arm pgshard CLOCK_BOOTTIME fence timer: %m")));
	fence_timer_deadline = deadline;
}

static void
pgshard_fence_timer_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	fence_timer_pending = true;
	InterruptPending = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

static bool
pgshard_fence_boottime_now(uint64 *result)
{
	struct timespec now;
	uint64		seconds;

	if (clock_gettime(CLOCK_BOOTTIME, &now) != 0 ||
		now.tv_sec < 0 || now.tv_nsec < 0 || now.tv_nsec >= 1000000000L)
		return false;
	seconds = (uint64) now.tv_sec;
	if (seconds > (PG_UINT64_MAX - (uint64) now.tv_nsec) / UINT64CONST(1000000000))
		return false;
	*result = seconds * UINT64CONST(1000000000) + (uint64) now.tv_nsec;
	return true;
}

Datum
pgshard_fence_install(PG_FUNCTION_ARGS)
{
	bytea	   *identity = PG_GETARG_BYTEA_PP(0);
	bytea	   *deadline_value = PG_GETARG_BYTEA_PP(1);
	Size		identity_length = VARSIZE_ANY_EXHDR(identity);
	unsigned char *identity_bytes = (unsigned char *) VARDATA_ANY(identity);
	uint64		deadline;
	uint64		now;
	unsigned char acknowledged_identity[PGSHARD_FENCE_MAX_IDENTITY_BYTES];
	uint16		acknowledged_identity_length;
	uint64		acknowledged_deadline;
	TupleDesc	tuple_desc;
	HeapTuple	tuple;
	Datum		values[2];
	bool		nulls[2] = {false, false};

	if (!control_backend || MyProcPort == NULL || !superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("pgshard fence installation requires the local peer-authenticated postgres control session")));
	if (fence_state == NULL || fence_lock == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pgshard fence shared state is unavailable")));
	if (identity_length == 0 || identity_length > PGSHARD_FENCE_MAX_IDENTITY_BYTES)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pgshard fence identity must contain between 1 and %d bytes",
						PGSHARD_FENCE_MAX_IDENTITY_BYTES)));
	if (!pgshard_fence_decode_deadline(deadline_value, &deadline))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pgshard fence deadline must be an exact 8-byte CLOCK_BOOTTIME value")));
	if (!control_exit_callback_registered)
	{
		before_shmem_exit(pgshard_fence_control_exit, (Datum) 0);
		control_exit_callback_registered = true;
	}

	LWLockAcquire(fence_lock, LW_EXCLUSIVE);
	if (!pgshard_fence_boottime_now(&now))
	{
		fence_state->expired = true;
		LWLockRelease(fence_lock);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot read CLOCK_BOOTTIME for pgshard fence installation")));
	}
	if (fence_state->armed && now >= fence_state->deadline_boottime_ns)
		fence_state->expired = true;
	if (fence_state->expired)
	{
		LWLockRelease(fence_lock);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pgshard fence cannot be rearmed after authority expiry")));
	}
	if (fence_state->armed && fence_state->control_pid != MyProcPid)
	{
		LWLockRelease(fence_lock);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pgshard fence is owned by another retained control session")));
	}
	if (fence_state->armed &&
		(fence_state->identity_length != identity_length ||
		 memcmp(fence_state->identity, identity_bytes, identity_length) != 0))
	{
		LWLockRelease(fence_lock);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pgshard fence identity is immutable for the postmaster lifetime")));
	}
	if (fence_state->armed && deadline < fence_state->deadline_boottime_ns)
	{
		LWLockRelease(fence_lock);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pgshard fence deadline cannot regress")));
	}
	if (deadline <= now)
	{
		LWLockRelease(fence_lock);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pgshard fence deadline has already expired")));
	}
	if (!fence_state->armed)
	{
		memcpy(fence_state->identity, identity_bytes, identity_length);
		fence_state->identity_length = (uint16) identity_length;
		fence_state->control_pid = MyProcPid;
		fence_state->armed = true;
	}
	fence_state->deadline_boottime_ns = deadline;
	(void) pg_atomic_add_fetch_u64(&fence_state->epoch, 1);
	acknowledged_identity_length = fence_state->identity_length;
	memcpy(acknowledged_identity, fence_state->identity,
		   acknowledged_identity_length);
	acknowledged_deadline = fence_state->deadline_boottime_ns;
	LWLockRelease(fence_lock);
	control_owner = true;
	pgshard_fence_signal_processes();
	SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);
	SendPostmasterSignal(PMSIGNAL_BACKGROUND_WORKER_CHANGE);

	if (get_call_result_type(fcinfo, NULL, &tuple_desc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "pgshard_fence_install return type must be a row type");
	tuple_desc = BlessTupleDesc(tuple_desc);
	values[0] = PointerGetDatum(pgshard_fence_copy_bytea(
									  acknowledged_identity,
									  acknowledged_identity_length));
	values[1] = PointerGetDatum(pgshard_fence_deadline_bytea(acknowledged_deadline));
	tuple = heap_form_tuple(tuple_desc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/* Undeclared, superuser-only live-test probe for the core emergency path. */
Datum
pgshard_fence_test_request_autovacuum(PG_FUNCTION_ARGS)
{
	if (!control_backend || MyProcPort == NULL || !superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("pgshard emergency-autovacuum test requires the trusted control session")));
	SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER);
	PG_RETURN_VOID();
}

/* Undeclared, owner-only live-test probe for dynamic no-database workers. */
Datum
pgshard_fence_test_start_background_worker(PG_FUNCTION_ARGS)
{
	(void) fcinfo;
	return pgshard_fence_test_start_worker(
		"pgshard_fence_test_background_worker_main",
		"pgshard fence test worker", NULL);
}

/* Undeclared probe for a worker that never processes ProcSignal barriers. */
Datum
pgshard_fence_test_start_barrier_worker(PG_FUNCTION_ARGS)
{
	(void) fcinfo;
	return pgshard_fence_test_start_worker(
		"pgshard_fence_test_barrier_worker_main",
		"pgshard fence barrier test worker", &barrier_test_worker_handle);
}

static Datum
pgshard_fence_test_start_worker(const char *function_name,
								const char *worker_name,
								BackgroundWorkerHandle **saved_handle)
{
	BackgroundWorker worker = {0};
	BackgroundWorkerHandle *handle = NULL;
	BgwHandleStatus status;
	MemoryContext old_context = NULL;
	pid_t		pid;

	if (!control_backend || !control_owner || MyProcPort == NULL || !superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("pgshard background-worker test requires the retained control owner")));
	if (saved_handle != NULL && *saved_handle != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("pgshard fence barrier test worker is already registered")));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	strcpy(worker.bgw_library_name, "pgshard_fence");
	strlcpy(worker.bgw_function_name, function_name,
			sizeof(worker.bgw_function_name));
	strlcpy(worker.bgw_name, worker_name, sizeof(worker.bgw_name));
	strlcpy(worker.bgw_type, worker_name, sizeof(worker.bgw_type));
	worker.bgw_notify_pid = MyProcPid;
	if (saved_handle != NULL)
		old_context = MemoryContextSwitchTo(TopMemoryContext);
	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		if (old_context != NULL)
			MemoryContextSwitchTo(old_context);
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not register pgshard fence test worker")));
	}
	if (old_context != NULL)
	{
		MemoryContextSwitchTo(old_context);
		*saved_handle = handle;
	}
	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED)
	{
		if (saved_handle != NULL)
		{
			pfree(handle);
			*saved_handle = NULL;
		}
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("pgshard fence test worker did not start")));
	}
	PG_RETURN_INT32(pid);
}

Datum
pgshard_fence_test_stop_barrier_worker(PG_FUNCTION_ARGS)
{
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;

	(void) fcinfo;
	if (!control_backend || !control_owner || MyProcPort == NULL || !superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("pgshard barrier-worker stop requires the retained control owner")));
	if (barrier_test_worker_handle == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pgshard fence barrier test worker is not registered")));
	handle = barrier_test_worker_handle;
	TerminateBackgroundWorker(handle);
	status = WaitForBackgroundWorkerShutdown(handle);
	barrier_test_worker_handle = NULL;
	pfree(handle);
	if (status != BGWH_STOPPED)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pgshard fence barrier test worker did not stop")));
	PG_RETURN_VOID();
}

/* Dynamic no-database worker used only by the live core-fence test. */
void
pgshard_fence_test_background_worker_main(Datum main_arg)
{
	(void) main_arg;
	BackgroundWorkerUnblockSignals();
	elog(LOG, "pgshard fence test background worker entered extension code");
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 60000L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
	}
}

/* A no-database worker that responds to SIGTERM but never joins barriers. */
void
pgshard_fence_test_barrier_worker_main(Datum main_arg)
{
	(void) main_arg;
	BackgroundWorkerUnblockSignals();
	elog(LOG, "pgshard fence barrier test worker entered extension code");
	for (;;)
	{
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 60000L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		if (ProcDiePending)
			proc_exit(0);
	}
}

static void
pgshard_fence_control_exit(int code, Datum arg)
{
	bool		changed = false;

	(void) code;
	(void) arg;

	if (!control_owner || fence_state == NULL || fence_lock == NULL)
		return;

	LWLockAcquire(fence_lock, LW_EXCLUSIVE);
	if (fence_state->armed && fence_state->control_pid == MyProcPid &&
		!fence_state->expired)
	{
		fence_state->expired = true;
		(void) pg_atomic_add_fetch_u64(&fence_state->epoch, 1);
		changed = true;
	}
	LWLockRelease(fence_lock);
	if (changed)
		pgshard_fence_signal_processes();
}

static void
pgshard_fence_signal_processes(void)
{
	ProcNumber	proc_number;

	if (ProcGlobal == NULL)
		return;
	for (proc_number = 0; proc_number < ProcGlobal->allProcCount; proc_number++)
	{
		volatile PGPROC *proc = &ProcGlobal->allProcs[proc_number];
		pid_t		pid = proc->pid;

		if (pid > 0 && pid != MyProcPid)
			SetLatch(&ProcGlobal->allProcs[proc_number].procLatch);
	}
}

static bool
pgshard_fence_decode_deadline(bytea *value, uint64 *result)
{
	const unsigned char *bytes;
	int			i;
	uint64		decoded = 0;

	if (VARSIZE_ANY_EXHDR(value) != PGSHARD_FENCE_DEADLINE_BYTES)
		return false;
	bytes = (const unsigned char *) VARDATA_ANY(value);
	for (i = 0; i < PGSHARD_FENCE_DEADLINE_BYTES; i++)
		decoded = (decoded << 8) | bytes[i];
	*result = decoded;
	return true;
}

static bytea *
pgshard_fence_copy_bytea(const unsigned char *bytes, Size length)
{
	bytea	   *result = (bytea *) palloc(VARHDRSZ + length);

	SET_VARSIZE(result, VARHDRSZ + length);
	memcpy(VARDATA(result), bytes, length);
	return result;
}

static bytea *
pgshard_fence_deadline_bytea(uint64 value)
{
	unsigned char bytes[PGSHARD_FENCE_DEADLINE_BYTES];
	int			i;

	for (i = PGSHARD_FENCE_DEADLINE_BYTES - 1; i >= 0; i--)
	{
		bytes[i] = (unsigned char) (value & 0xff);
		value >>= 8;
	}
	return pgshard_fence_copy_bytea(bytes, PGSHARD_FENCE_DEADLINE_BYTES);
}
