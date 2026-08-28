#ifndef ILIOS_H
#define ILIOS_H

#include <stdint.h>
#include <float.h>
#include <cassandra.h>
#include <uv.h>
#include "ruby.h"
#include "ruby/thread.h"
#include "ruby/encoding.h"

#define DEFAULT_PAGE_SIZE 10000

// Cluster exposes an allocator, so allocate and dup both produce an instance
// that never ran initialize. Reject it here instead of handing NULL to the
// driver, which segfaults.
#define GET_CLUSTER(obj, var)   do { \
    GET_UNINITIALIZED_CLUSTER(obj, var); \
    if ((var)->cluster == NULL) { \
        rb_raise(rb_eRuntimeError, "uninitialized Ilios::Cassandra::Cluster"); \
    } \
} while (0)
#define GET_UNINITIALIZED_CLUSTER(obj, var) TypedData_Get_Struct(obj, CassandraCluster, &cassandra_cluster_data_type, var)
#define GET_SESSION(obj, var)   TypedData_Get_Struct(obj, CassandraSession, &cassandra_session_data_type, var)
#define GET_STATEMENT(obj, var) TypedData_Get_Struct(obj, CassandraStatement, &cassandra_statement_data_type, var)
#define GET_RESULT(obj, var)    TypedData_Get_Struct(obj, CassandraResult, &cassandra_result_data_type, var)
#define GET_FUTURE(obj, var)    TypedData_Get_Struct(obj, CassandraFuture, &cassandra_future_data_type, var)
#define CREATE_CLUSTER(var)     TypedData_Make_Struct(cCluster, CassandraCluster, &cassandra_cluster_data_type, var)
#define CREATE_SESSION(var)     TypedData_Make_Struct(cSession, CassandraSession, &cassandra_session_data_type, var)
#define CREATE_STATEMENT(var)   TypedData_Make_Struct(cStatement, CassandraStatement, &cassandra_statement_data_type, var)
#define CREATE_RESULT(var)      TypedData_Make_Struct(cResult, CassandraResult, &cassandra_result_data_type, var)
#define CREATE_FUTURE(var)      TypedData_Make_Struct(cFuture, CassandraFuture, &cassandra_future_data_type, var)

typedef enum {
  prepare_async,
  execute_async
} future_kind;

// Node in the callback dispatch lists (defined in future.c).
typedef struct future_dispatch_node future_dispatch_node;

typedef enum {
  idempotency_unset,
  idempotency_false,
  idempotency_true
} statement_idempotency;

typedef struct
{
    CassCluster* cluster;
    VALUE keyspace;
} CassandraCluster;
typedef struct
{
    CassSession* session;
    VALUE cluster_obj;
} CassandraSession;

typedef struct
{
    // Scratch statement used for validating values in Statement#bind.
    // It is never handed to the driver: each execution gets its own
    // CassStatement built from `prepared` and `bound_values`, because the
    // driver encodes values asynchronously on its IO thread and re-binding
    // an in-flight statement is a use-after-free (issue #12).
    CassStatement* statement;
    const CassPrepared* prepared;
    VALUE session_obj;
    VALUE bound_values;
    int page_size;
    statement_idempotency idempotent;
} CassandraStatement;

typedef struct
{
    const CassResult *result;
    CassFuture *future;
    // The CassStatement this result was executed with (owned, freed on destroy).
    // Not to be confused with statement_obj, the Ruby Statement object.
    CassStatement *executed_statement;
    VALUE statement_obj;
} CassandraResult;

typedef struct
{
    CassFuture *future;
    // The CassStatement this future's execution was submitted with (owned,
    // freed on destroy unless handed over to the result). Not to be confused
    // with statement_obj, the Ruby Statement object.
    CassStatement *executed_statement;
    future_kind kind;

    VALUE session_obj;
    VALUE statement_obj;
    VALUE on_success_block;
    VALUE on_failure_block;
    VALUE proc_mutex;

    uv_sem_t sem;
    bool already_waited;
    bool yielded;
    // A native completion callback was registered with the cpp-driver and
    // the future was handed to the dispatcher, which posts the semaphore
    // once the callbacks were delivered.
    bool dispatch_registered;
    // The future's node in the dispatch lists while the dispatcher still
    // owns its completion; NULL once the completion was delivered (or when
    // the future was never handed over). Guarded by proc_mutex.
    future_dispatch_node *dispatch_node;
} CassandraFuture;

extern const rb_data_type_t cassandra_cluster_data_type;
extern const rb_data_type_t cassandra_session_data_type;
extern const rb_data_type_t cassandra_statement_data_type;
extern const rb_data_type_t cassandra_result_data_type;
extern const rb_data_type_t cassandra_future_data_type;

extern VALUE mIlios;
extern VALUE mCassandra;
extern VALUE cCluster;
extern VALUE cSession;
extern VALUE cStatement;
extern VALUE cResult;
extern VALUE cFuture;
extern VALUE eConnectError;
extern VALUE eExecutionError;
extern VALUE eStatementError;

extern VALUE cSet;

extern VALUE id_to_time;
extern VALUE id_to_a;
extern VALUE id_new;
extern VALUE id_alive;
extern VALUE id_report_on_exception;
extern VALUE id_code;
extern VALUE sym_unsupported_column_type;

extern void Init_cluster(void);
extern void Init_session(void);
extern void Init_statement(void);
extern void Init_result(void);
extern void Init_future(void);

extern VALUE future_create(CassFuture *future, VALUE session, VALUE statement, future_kind kind);
extern void nogvl_future_wait(CassFuture *future);
extern CassFuture *nogvl_session_prepare(CassSession* session, VALUE query);
extern CassFuture *nogvl_session_execute(CassSession* session, CassStatement* statement);
extern void nogvl_sem_wait(uv_sem_t *sem);

extern void statement_default_config(CassandraStatement *cassandra_statement);
extern CassStatement *statement_build_for_execution(CassandraStatement *cassandra_statement);
extern void result_await(CassandraResult *cassandra_result);

// Builds an exception of `exception_class` carrying `message` and `@code`
// (readable via #code) set to the CassError value.
extern VALUE ilios_error_new(VALUE exception_class, VALUE message, CassError error_code);

// Builds an exception of `exception_class` from `future`'s error: the
// driver's server-reported message (falling back to the generic
// cass_error_desc when the driver returns none), formatted as
// "<prefix>: <message>" when `prefix` is non-NULL, or the bare message when
// `prefix` is NULL. `future` must still be alive (not yet cass_future_free'd)
// when this is called.
extern VALUE ilios_future_error_new(VALUE exception_class, const char *prefix, CassFuture *future);


#endif // ILIOS_H
