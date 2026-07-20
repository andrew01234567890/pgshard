/*
 * pgshard_fence: PostgreSQL 18 shared-preload writable-authority target.
 *
 * Each postmaster starts disarmed. Only the postgres/postgres local peer
 * control backend may install an exact canonical identity and absolute Linux
 * CLOCK_BOOTTIME deadline. Ordinary backends are rejected at authentication
 * and at every executor/utility statement boundary while disarmed or expired.
 */

#include "postgres.h"

#include <sys/socket.h>
#include <time.h>

#include "access/htup_details.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq/auth.h"
#include "libpq/hba.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

#define PGSHARD_FENCE_MAX_IDENTITY_BYTES 1024
#define PGSHARD_FENCE_DEADLINE_BYTES 8

typedef struct PgshardFenceState
{
	bool	armed;
	bool	expired;
	uint16	identity_length;
	uint64	deadline_boottime_ns;
	unsigned char identity[PGSHARD_FENCE_MAX_IDENTITY_BYTES];
} PgshardFenceState;

static PgshardFenceState *fence_state = NULL;
static LWLock *fence_lock = NULL;
static bool control_backend = false;

static shmem_request_hook_type previous_shmem_request_hook = NULL;
static shmem_startup_hook_type previous_shmem_startup_hook = NULL;
static ClientAuthentication_hook_type previous_client_authentication_hook = NULL;
static ExecutorStart_hook_type previous_executor_start_hook = NULL;
static ProcessUtility_hook_type previous_process_utility_hook = NULL;

void		_PG_init(void);
Datum		pgshard_fence_install(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pgshard_fence_install);

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
static bool pgshard_fence_authority_current(void);
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
		MemSet(fence_state, 0, sizeof(PgshardFenceState));
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
	if (control_backend)
		return;
	if (!pgshard_fence_authority_current())
		ereport(FATAL,
				(errcode(ERRCODE_ADMIN_SHUTDOWN),
				 errmsg("pgshard writable authority is not installed or has expired")));
}

static bool
pgshard_fence_authority_current(void)
{
	bool		current;
	uint64		now;

	if (fence_state == NULL || fence_lock == NULL)
		return false;

	LWLockAcquire(fence_lock, LW_EXCLUSIVE);
	if (!pgshard_fence_boottime_now(&now))
	{
		fence_state->expired = true;
		LWLockRelease(fence_lock);
		return false;
	}
	if (fence_state->armed && now >= fence_state->deadline_boottime_ns)
		fence_state->expired = true;
	current = fence_state->armed && !fence_state->expired;
	LWLockRelease(fence_lock);
	return current;
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
		fence_state->armed = true;
	}
	fence_state->deadline_boottime_ns = deadline;
	acknowledged_identity_length = fence_state->identity_length;
	memcpy(acknowledged_identity, fence_state->identity,
		   acknowledged_identity_length);
	acknowledged_deadline = fence_state->deadline_boottime_ns;
	LWLockRelease(fence_lock);

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
