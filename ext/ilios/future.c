#include "ilios.h"

typedef struct dispatch_node
{
    VALUE future_obj;
    struct dispatch_node *prev;
    struct dispatch_node *next;
} dispatch_node;

typedef struct
{
    dispatch_node *head;
    dispatch_node *tail;
} dispatch_list;

/*
 * Completion dispatch state shared by every future.
 *
 * The cpp-driver invokes future_completed_native_cb on one of its own IO
 * threads (no GVL), which only moves the future's node from `pending` to
 * `completed` and signals `cond`. A single Ruby dispatcher thread drains
 * `completed` and yields the registered callbacks with the GVL held, in
 * completion order.
 *
 * Critical sections guarded by `mutex` never call Ruby APIs: a thread
 * holding the GVL therefore cannot trigger GC while holding `mutex`, so the
 * GC mark function below can take `mutex` without risking a deadlock; the
 * only contention is an IO thread's O(1) list operation.
 */
static struct
{
    uv_mutex_t mutex;
    uv_cond_t cond;
    dispatch_list pending;   /* native callback registered, not yet completed */
    dispatch_list completed; /* completed, waiting to be yielded (FIFO) */
} dispatch;

/* Created lazily, recreated when found dead (fork, Thread#kill, fatal error). */
static VALUE dispatcher_thread;

typedef struct
{
    bool interrupted;
} dispatch_wait_ctx;

static void future_mark(void *ptr);
static void future_destroy(void *ptr);
static size_t future_memsize(const void *ptr);
static void future_compact(void *ptr);

const rb_data_type_t cassandra_future_data_type = {
    "Ilios::Cassandra::Future",
    {
        future_mark,
        future_destroy,
        future_memsize,
        future_compact,
    },
    0, 0,
    RUBY_TYPED_FREE_IMMEDIATELY | RUBY_TYPED_WB_PROTECTED | RUBY_TYPED_FROZEN_SHAREABLE,
};

static void dispatch_list_append(dispatch_list *list, dispatch_node *node)
{
    node->prev = list->tail;
    node->next = NULL;
    if (list->tail) {
        list->tail->next = node;
    } else {
        list->head = node;
    }
    list->tail = node;
}

static void dispatch_list_remove(dispatch_list *list, dispatch_node *node)
{
    if (node->prev) {
        node->prev->next = node->next;
    } else {
        list->head = node->next;
    }
    if (node->next) {
        node->next->prev = node->prev;
    } else {
        list->tail = node->prev;
    }
    node->prev = NULL;
    node->next = NULL;
}

static void dispatch_registry_mark(void *ptr)
{
    dispatch_node *node;

    // rb_gc_mark (not the movable variant) pins the futures: their raw
    // VALUEs are stored in native lists and passed to the cpp-driver as
    // user data, which the GC could not update when compaction moves them.
    uv_mutex_lock(&dispatch.mutex);
    for (node = dispatch.pending.head; node; node = node->next) {
        rb_gc_mark(node->future_obj);
    }
    for (node = dispatch.completed.head; node; node = node->next) {
        rb_gc_mark(node->future_obj);
    }
    uv_mutex_unlock(&dispatch.mutex);
}

static const rb_data_type_t dispatch_registry_data_type = {
    "Ilios::Cassandra::Future::dispatch_registry",
    {
        dispatch_registry_mark,
        NULL,
        NULL,
        NULL,
    },
    0, 0,
    0,
};

// Runs on a cpp-driver IO thread (or on the registering thread when the
// future resolved before cass_future_set_callback returned), without the
// GVL: no Ruby API may be called here.
static void future_completed_native_cb(CassFuture *future, void *data)
{
    dispatch_node *node = (dispatch_node *)data;

    (void)future;
    uv_mutex_lock(&dispatch.mutex);
    dispatch_list_remove(&dispatch.pending, node);
    dispatch_list_append(&dispatch.completed, node);
    uv_cond_signal(&dispatch.cond);
    uv_mutex_unlock(&dispatch.mutex);
}

static void *dispatch_wait_for_completion(void *ptr)
{
    dispatch_wait_ctx *ctx = (dispatch_wait_ctx *)ptr;

    uv_mutex_lock(&dispatch.mutex);
    while (dispatch.completed.head == NULL && !ctx->interrupted) {
        uv_cond_wait(&dispatch.cond, &dispatch.mutex);
    }
    uv_mutex_unlock(&dispatch.mutex);
    return NULL;
}

static void dispatch_wait_ubf(void *ptr)
{
    dispatch_wait_ctx *ctx = (dispatch_wait_ctx *)ptr;

    uv_mutex_lock(&dispatch.mutex);
    ctx->interrupted = true;
    uv_cond_broadcast(&dispatch.cond);
    uv_mutex_unlock(&dispatch.mutex);
}

static dispatch_node *dispatch_pop_completed(void)
{
    dispatch_node *node;

    uv_mutex_lock(&dispatch.mutex);
    node = dispatch.completed.head;
    if (node) {
        dispatch_list_remove(&dispatch.completed, node);
    }
    uv_mutex_unlock(&dispatch.mutex);
    return node;
}

static void future_result_success_yield(CassandraFuture *cassandra_future)
{
    VALUE obj;

    if (cassandra_future->on_success_block) {
        if (rb_proc_arity(cassandra_future->on_success_block)) {
            switch (cassandra_future->kind) {
            case prepare_async:
                {
                    CassandraStatement *cassandra_statement;
                    VALUE cassandra_statement_obj;

                    cassandra_statement_obj = CREATE_STATEMENT(cassandra_statement);
                    cassandra_statement->prepared = cass_future_get_prepared(cassandra_future->future);
                    cassandra_statement->statement = cass_prepared_bind(cassandra_statement->prepared);
                    cassandra_statement->session_obj = cassandra_future->session_obj;

                    statement_default_config(cassandra_statement);

                    obj = cassandra_statement_obj;
                }
                break;
            case execute_async:
                {
                    CassandraResult *cassandra_result;
                    VALUE cassandra_result_obj;

                    cassandra_result_obj = CREATE_RESULT(cassandra_result);
                    cassandra_result->result = cass_future_get_result(cassandra_future->future);
                    cassandra_result->statement_obj = cassandra_future->statement_obj;
                    // Hand over the executed CassStatement so Result#next_page
                    // can reuse it and it gets freed exactly once.
                    cassandra_result->executed_statement = cassandra_future->executed_statement;
                    cassandra_future->executed_statement = NULL;

                    obj = cassandra_result_obj;
                }
                break;
            }

            rb_proc_call_with_block(cassandra_future->on_success_block, 1, &obj, Qnil);
        } else {
            rb_proc_call_with_block(cassandra_future->on_success_block, 0, NULL, Qnil);
        }
    }
}

static void future_result_failure_yield(CassandraFuture *cassandra_future)
{
    if (cassandra_future->on_failure_block) {
        if (rb_proc_arity(cassandra_future->on_failure_block)) {
            VALUE error = ilios_future_error_new(eExecutionError, NULL, cassandra_future->future);

            rb_proc_call_with_block(cassandra_future->on_failure_block, 1, &error, Qnil);
        } else {
            rb_proc_call_with_block(cassandra_future->on_failure_block, 0, NULL, Qnil);
        }
    }
}

static VALUE future_result_yielder_synchronize(VALUE future)
{
    CassandraFuture *cassandra_future;

    GET_FUTURE(future, cassandra_future);

    if (!cassandra_future->yielded) {
        if (cass_future_error_code(cassandra_future->future) == CASS_OK) {
            if (cassandra_future->on_success_block) {
                cassandra_future->yielded = true;
                future_result_success_yield(cassandra_future);
            }
        } else {
            if (cassandra_future->on_failure_block) {
                cassandra_future->yielded = true;
                future_result_failure_yield(cassandra_future);
            }
        }
    }
    cassandra_future->already_waited = true;
    return Qnil;
}

static VALUE dispatch_yield_body(VALUE future)
{
    CassandraFuture *cassandra_future;

    GET_FUTURE(future, cassandra_future);
    return rb_mutex_synchronize(cassandra_future->proc_mutex, future_result_yielder_synchronize, future);
}

// Yields the callbacks of one completed future and posts its semaphore so
// #await returns only after the callbacks ran.
static void dispatch_process_node(dispatch_node *node)
{
    // Keep the future in a local: while the node is off both lists this
    // machine stack reference is what pins the future.
    volatile VALUE future = node->future_obj;
    CassandraFuture *cassandra_future;
    int state = 0;

    rb_protect(dispatch_yield_body, (VALUE)future, &state);

    GET_FUTURE(future, cassandra_future);
    uv_sem_post(&cassandra_future->sem);
    xfree(node);

    if (state) {
        VALUE errinfo = rb_errinfo();

        rb_set_errinfo(Qnil);
        if (RTEST(rb_obj_is_kind_of(errinfo, rb_eStandardError))) {
            // A raising callback must not stop callback delivery for the
            // other futures: report it like report_on_exception would and
            // keep dispatching.
            rb_io_write(rb_stderr, rb_funcall(errinfo, rb_intern("full_message"), 0));
        } else {
            // Everything else (Thread#kill, SystemExit, fatal errors)
            // unwinds this thread; it is recreated lazily on the next
            // registration or #await.
            rb_jump_tag(state);
        }
    }
}

static VALUE dispatcher_thread_main(void *arg)
{
    (void)arg;
    while (1) {
        dispatch_wait_ctx ctx = { false };
        dispatch_node *node;

        rb_thread_call_without_gvl(dispatch_wait_for_completion, &ctx, dispatch_wait_ubf, &ctx);
        node = dispatch_pop_completed();
        if (node) {
            dispatch_process_node(node);
        }
        rb_thread_check_ints();
    }
    return Qnil;
}

static void dispatcher_ensure_thread(void)
{
    if (!RTEST(dispatcher_thread) || !RTEST(rb_funcall(dispatcher_thread, id_alive, 0))) {
        dispatcher_thread = rb_thread_create(dispatcher_thread_main, NULL);
        rb_funcall(dispatcher_thread, id_report_on_exception, 1, Qtrue);
    }
}

// Hands the future over to the dispatcher: pins it in the pending list and
// registers the native completion callback with the cpp-driver.
static void future_register_dispatch(VALUE future, CassandraFuture *cassandra_future)
{
    dispatch_node *node = ALLOC(dispatch_node);
    CassError rc;

    node->future_obj = future;
    node->prev = NULL;
    node->next = NULL;

    uv_mutex_lock(&dispatch.mutex);
    dispatch_list_append(&dispatch.pending, node);
    uv_mutex_unlock(&dispatch.mutex);

    dispatcher_ensure_thread();

    rc = cass_future_set_callback(cassandra_future->future, future_completed_native_cb, node);
    if (rc != CASS_OK) {
        // Cannot happen: dispatch_registered guards against registering the
        // native callback twice. Unpin and fail loudly rather than leak.
        uv_mutex_lock(&dispatch.mutex);
        dispatch_list_remove(&dispatch.pending, node);
        uv_mutex_unlock(&dispatch.mutex);
        xfree(node);
        rb_raise(eExecutionError, "Failed to register completion callback: %s", cass_error_desc(rc));
    }
}

VALUE future_create(CassFuture *future, VALUE session, VALUE statement, future_kind kind)
{
    CassandraFuture *cassandra_future;
    VALUE cassandra_future_obj;

    cassandra_future_obj = CREATE_FUTURE(cassandra_future);
    cassandra_future->kind = kind;
    cassandra_future->future = future;
    cassandra_future->executed_statement = NULL;
    cassandra_future->session_obj = session;
    cassandra_future->statement_obj = statement;
    cassandra_future->proc_mutex = rb_mutex_new();
    uv_sem_init(&cassandra_future->sem, 0);
    cassandra_future->already_waited = false;
    cassandra_future->yielded = false;
    cassandra_future->dispatch_registered = false;

    return cassandra_future_obj;
}

static VALUE future_on_success_synchronize(VALUE future)
{
    CassandraFuture *cassandra_future;
    bool register_native_callback = false;

    GET_FUTURE(future, cassandra_future);

    if (!cassandra_future->on_failure_block) {
        // Register the native completion callback only once per future
        register_native_callback = true;
    }

    RB_OBJ_WRITE(future, &cassandra_future->on_success_block, rb_block_proc());

    if (cass_future_ready(cassandra_future->future)) {
        // The dispatcher posts the semaphore itself once it has yielded;
        // posting here too would let #await return before that happens.
        if (!cassandra_future->dispatch_registered) {
            uv_sem_post(&cassandra_future->sem);
        }
        if (!cassandra_future->yielded &&
            cass_future_error_code(cassandra_future->future) == CASS_OK) {
            cassandra_future->yielded = true;
            future_result_success_yield(cassandra_future);
        }
        return future;
    }

    if (register_native_callback) {
        cassandra_future->dispatch_registered = true;
        future_register_dispatch(future, cassandra_future);
    }

    return future;
}

/**
 * Run block when future resolves to a value.
 * The callback is invoked on a background dispatcher thread as soon as the
 * future completes: callbacks of different futures run in completion order,
 * not in registration order.
 *
 * @yieldparam value [Cassandra::Statement, Cassandra::Result] A value.
 *   Yields +Cassandra::Statement+ object when future was created by +Cassandra::Session#prepare_async+.
 *   Yields +Cassandra::Result+ object when future was created by +Cassandra::Session#execute_async+.
 * @return [Cassandra::Future] self.
 * @raise [Cassandra::ExecutionError] If this method will be called twice.
 * @raise [ArgumentError] If no block was given.
 */
static VALUE future_on_success(VALUE self)
{
    CassandraFuture *cassandra_future;

    GET_FUTURE(self, cassandra_future);

    if (cassandra_future->on_success_block) {
        rb_raise(eExecutionError, "It should not call twice");
    }
    if (!rb_block_given_p()) {
        rb_raise(rb_eArgError, "no block given");
    }

    return rb_mutex_synchronize(cassandra_future->proc_mutex, future_on_success_synchronize, self);
}

static VALUE future_on_failure_synchronize(VALUE future)
{
    CassandraFuture *cassandra_future;
    bool register_native_callback = false;

    GET_FUTURE(future, cassandra_future);

    if (!cassandra_future->on_success_block) {
        // Register the native completion callback only once per future
        register_native_callback = true;
    }

    RB_OBJ_WRITE(future, &cassandra_future->on_failure_block, rb_block_proc());

    if (cass_future_ready(cassandra_future->future)) {
        if (!cassandra_future->dispatch_registered) {
            uv_sem_post(&cassandra_future->sem);
        }
        if (!cassandra_future->yielded &&
            cass_future_error_code(cassandra_future->future) != CASS_OK) {
            cassandra_future->yielded = true;
            future_result_failure_yield(cassandra_future);
        }
        return future;
    }

    if (register_native_callback) {
        cassandra_future->dispatch_registered = true;
        future_register_dispatch(future, cassandra_future);
    }

    return future;
}

/**
 * Run block when future resolves to error.
 * The callback is invoked on a background dispatcher thread as soon as the
 * future completes: callbacks of different futures run in completion order,
 * not in registration order.
 *
 * @yieldparam error [Cassandra::ExecutionError] The failure reason. Only
 *   yielded when the block accepts an argument; a zero-arity block is still
 *   called with no arguments.
 * @return [Cassandra::Future] self.
 * @raise [Cassandra::ExecutionError] If this method will be called twice.
 * @raise [ArgumentError] If no block was given.
 */
static VALUE future_on_failure(VALUE self)
{
    CassandraFuture *cassandra_future;

    GET_FUTURE(self, cassandra_future);

    if (cassandra_future->on_failure_block) {
        rb_raise(eExecutionError, "It should not call twice");
    }
    if (!rb_block_given_p()) {
        rb_raise(rb_eArgError, "no block given");
    }

    return rb_mutex_synchronize(cassandra_future->proc_mutex, future_on_failure_synchronize, self);
}

/**
 * Wait to complete a future's statement.
 * Returns after the registered callbacks finished running.
 *
 * @return [Cassandra::Future] self.
 */
static VALUE future_await(VALUE self)
{
    CassandraFuture *cassandra_future;
    bool dispatch_registered;

    GET_FUTURE(self, cassandra_future);

    rb_mutex_lock(cassandra_future->proc_mutex);
    if (cassandra_future->already_waited) {
        rb_mutex_unlock(cassandra_future->proc_mutex);
        return self;
    }
    cassandra_future->already_waited = true;
    dispatch_registered = cassandra_future->dispatch_registered;
    rb_mutex_unlock(cassandra_future->proc_mutex);

    if (dispatch_registered && rb_thread_current() == dispatcher_thread) {
        // Called from inside a callback: blocking on the semaphore would
        // deadlock because this thread is the one that posts it. Yield the
        // completed futures ourselves until this future's callbacks ran.
        while (uv_sem_trywait(&cassandra_future->sem) != 0) {
            dispatch_wait_ctx ctx = { false };
            dispatch_node *node;

            rb_thread_call_without_gvl(dispatch_wait_for_completion, &ctx, dispatch_wait_ubf, &ctx);
            node = dispatch_pop_completed();
            if (node) {
                dispatch_process_node(node);
            }
            rb_thread_check_ints();
        }
        return self;
    }

    nogvl_future_wait(cassandra_future->future);
    // Re-read after the wait so callbacks registered meanwhile are covered.
    if (cassandra_future->on_success_block || cassandra_future->on_failure_block) {
        if (cassandra_future->dispatch_registered) {
            // The dispatcher may have died (fork, Thread#kill); make sure
            // someone will post the semaphore.
            dispatcher_ensure_thread();
        }
        nogvl_sem_wait(&cassandra_future->sem);
    }
    return self;
}

static void future_mark(void *ptr)
{
    CassandraFuture *cassandra_future = (CassandraFuture *)ptr;
    rb_gc_mark_movable(cassandra_future->session_obj);
    rb_gc_mark_movable(cassandra_future->statement_obj);
    rb_gc_mark_movable(cassandra_future->on_success_block);
    rb_gc_mark_movable(cassandra_future->on_failure_block);
    rb_gc_mark_movable(cassandra_future->proc_mutex);
}

static void future_destroy(void *ptr)
{
    CassandraFuture *cassandra_future = (CassandraFuture *)ptr;

    if (cassandra_future->future) {
        cass_future_free(cassandra_future->future);
    }
    if (cassandra_future->executed_statement) {
        // Safe even if the request is still in flight: the driver's request
        // holds its own reference to the statement internals.
        cass_statement_free(cassandra_future->executed_statement);
    }
    uv_sem_destroy(&cassandra_future->sem);
    xfree(cassandra_future);
}

static size_t future_memsize(const void *ptr)
{
    return sizeof(CassandraFuture);
}

static void future_compact(void *ptr)
{
    CassandraFuture *cassandra_future = (CassandraFuture *)ptr;

    cassandra_future->session_obj = rb_gc_location(cassandra_future->session_obj);
    cassandra_future->statement_obj = rb_gc_location(cassandra_future->statement_obj);
    cassandra_future->on_success_block = rb_gc_location(cassandra_future->on_success_block);
    cassandra_future->on_failure_block = rb_gc_location(cassandra_future->on_failure_block);
    cassandra_future->proc_mutex = rb_gc_location(cassandra_future->proc_mutex);
}

void Init_future(void)
{
    VALUE registry;

    rb_undef_alloc_func(cFuture);

    rb_define_method(cFuture, "on_success", future_on_success, 0);
    rb_define_method(cFuture, "on_failure", future_on_failure, 0);
    rb_define_method(cFuture, "await", future_await, 0);

    uv_mutex_init(&dispatch.mutex);
    uv_cond_init(&dispatch.cond);

    // Hidden object whose only job is to mark (and thereby pin) the futures
    // currently owned by the dispatch lists.
    registry = TypedData_Wrap_Struct(0, &dispatch_registry_data_type, &dispatch);
    rb_gc_register_mark_object(registry);

    dispatcher_thread = Qnil;
    rb_gc_register_address(&dispatcher_thread);
}
