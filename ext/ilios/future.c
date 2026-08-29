#include "ilios.h"

#include <pthread.h>

// Longest a blocked #await goes between dispatcher liveness re-checks.
#define DISPATCH_AWAIT_RECHECK_INTERVAL_NS (500ULL * 1000 * 1000)

typedef struct future_dispatch_node
{
    VALUE future_obj;
    struct future_dispatch_node *prev;
    struct future_dispatch_node *next;
} future_dispatch_node;

typedef struct
{
    future_dispatch_node *head;
    future_dispatch_node *tail;
} dispatch_list;

/*
 * Completion dispatch state shared by every future. A cpp-driver IO thread
 * (no GVL) moves a future's node from `pending` to `completed` and
 * broadcasts `cond`; a single Ruby dispatcher thread drains `completed` and
 * yields the callbacks under the GVL in completion order. Threads blocked
 * in #await sleep on the same `cond`.
 *
 * Invariant: no Ruby API is called while `mutex` is held, so a GVL-holding
 * thread cannot trigger GC inside a critical section, which lets the GC
 * mark function below take `mutex` without risking a deadlock.
 */
static struct
{
    uv_mutex_t mutex;
    uv_cond_t cond;
    dispatch_list pending;   /* native callback registered, not yet completed */
    dispatch_list completed; /* completed, waiting to be yielded (FIFO) */
    /* Bumped by the dying dispatcher before it broadcasts. Awaiters compare
     * it against the value they entered their wait with, so a death is part
     * of the wait predicate instead of a wakeup they cannot tell apart from
     * a spurious one. */
    uint64_t dispatcher_deaths;
} dispatch;

/* Created lazily, recreated when found dead (fork, Thread#kill, fatal error). */
static VALUE dispatcher_thread;
/* Serializes dispatcher creation so it stays a single thread. */
static VALUE dispatcher_mutex;
/* Set by the running dispatcher itself, cleared by its ensure handler and by
 * the atfork child handler — so a stale true is impossible. Read under GVL. */
static bool dispatcher_running;
/* Incremented in the forked child: futures stamped with an older generation
 * belong to the parent's driver threads and cannot be used any more. */
static uint32_t dispatch_fork_generation;

typedef struct
{
    const bool *delivered;
    /* Value of dispatch.dispatcher_deaths when the awaiter last verified the
     * dispatcher, sampled before that check so no death can slip in between.
     * The counter is monotonic, so even a die-then-recreate is visible. */
    uint64_t deaths;
    bool interrupted;
    bool done;
} dispatch_await_ctx;

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

static void dispatch_list_append(dispatch_list *list, future_dispatch_node *node)
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

static void dispatch_list_prepend(dispatch_list *list, future_dispatch_node *node)
{
    node->prev = NULL;
    node->next = list->head;
    if (list->head) {
        list->head->prev = node;
    } else {
        list->tail = node;
    }
    list->head = node;
}

static void dispatch_list_remove(dispatch_list *list, future_dispatch_node *node)
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
    future_dispatch_node *node;

    // rb_gc_mark (not the movable variant) pins the futures. Movable marking
    // would be possible — the cpp-driver's user data is the node pointer, not
    // the VALUE — but it would need a compact callback rewriting every
    // node->future_obj through rb_gc_location while holding dispatch.mutex,
    // in a list a driver IO thread can be splicing at the same time. Pinning
    // a handful of in-flight futures is deliberately preferred over that.
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

// Runs on a cpp-driver IO thread — or, when the future resolved before
// cass_future_set_callback returned, on the registering thread (which
// holds the GVL). Treat it as a no-GVL context either way: no Ruby API
// may be called here.
static void future_completed_native_cb(CassFuture *future, void *data)
{
    future_dispatch_node *node = (future_dispatch_node *)data;

    (void)future;
    uv_mutex_lock(&dispatch.mutex);
    dispatch_list_remove(&dispatch.pending, node);
    dispatch_list_append(&dispatch.completed, node);
    uv_cond_broadcast(&dispatch.cond);
    uv_mutex_unlock(&dispatch.mutex);
}

static void *dispatch_wait_for_completion(void *ptr)
{
    bool *interrupted = (bool *)ptr;

    uv_mutex_lock(&dispatch.mutex);
    while (dispatch.completed.head == NULL && !*interrupted) {
        uv_cond_wait(&dispatch.cond, &dispatch.mutex);
    }
    uv_mutex_unlock(&dispatch.mutex);
    return NULL;
}

// Unblock function shared by the no-GVL waits below; `ptr` is the wait's
// `interrupted` flag.
static void dispatch_interrupt_ubf(void *ptr)
{
    bool *interrupted = (bool *)ptr;

    uv_mutex_lock(&dispatch.mutex);
    *interrupted = true;
    uv_cond_broadcast(&dispatch.cond);
    uv_mutex_unlock(&dispatch.mutex);
}

// One bounded wait of an awaiting thread. The delivered flag is read under
// `dispatch.mutex`, so the dispatcher's set-then-broadcast cannot slip
// between the check and the wait; broadcasts for other futures are absorbed
// here without re-taking the GVL.
static void *dispatch_await_wait(void *ptr)
{
    dispatch_await_ctx *ctx = (dispatch_await_ctx *)ptr;
    uint64_t deadline = uv_hrtime() + DISPATCH_AWAIT_RECHECK_INTERVAL_NS;

    uv_mutex_lock(&dispatch.mutex);
    while (!ctx->interrupted) {
        uint64_t now;

        if (*ctx->delivered) {
            ctx->done = true;
            break;
        }
        // A dispatcher died since the caller last verified one was alive:
        // nobody may be left to set the flag, so return to the GVL now and
        // let the caller re-check instead of sitting out the deadline below.
        // The baseline was sampled before that check, so a death racing it
        // cannot hide here.
        if (dispatch.dispatcher_deaths != ctx->deaths) {
            break;
        }
        now = uv_hrtime();
        if (now >= deadline) {
            break;
        }
        // Timed rather than untimed on purpose: the deadline is the safety
        // net for anything the predicate above cannot observe.
        uv_cond_timedwait(&dispatch.cond, &dispatch.mutex, deadline - now);
    }
    uv_mutex_unlock(&dispatch.mutex);
    return NULL;
}

// How many dispatcher threads have died so far. Monotonic, so comparing two
// samples tells an awaiter whether a dispatcher died in between even when a
// replacement was already created.
static uint64_t dispatch_deaths_count(void)
{
    uint64_t deaths;

    uv_mutex_lock(&dispatch.mutex);
    deaths = dispatch.dispatcher_deaths;
    uv_mutex_unlock(&dispatch.mutex);
    return deaths;
}

// Whether the dispatcher finished running this future's callbacks.
static bool future_delivered_p(CassandraFuture *cassandra_future)
{
    bool delivered;

    uv_mutex_lock(&dispatch.mutex);
    delivered = cassandra_future->delivered;
    uv_mutex_unlock(&dispatch.mutex);
    return delivered;
}

static future_dispatch_node *dispatch_pop_completed(void)
{
    future_dispatch_node *node;

    uv_mutex_lock(&dispatch.mutex);
    node = dispatch.completed.head;
    if (node) {
        dispatch_list_remove(&dispatch.completed, node);
    }
    uv_mutex_unlock(&dispatch.mutex);
    return node;
}

// The value a succeeded future hands to its callbacks: the bound Statement
// for a prepare_async future, the Result for an execute_async one. Consumes
// the executed CassStatement, so it must be called at most once per future
// (delivery is at-most-once, and on_complete cannot coexist with
// on_success).
static VALUE future_build_success_value(CassandraFuture *cassandra_future)
{
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

            return cassandra_statement_obj;
        }
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

            return cassandra_result_obj;
        }
    }
    return Qnil; /* unreachable: kind is one of the two above */
}

static void future_result_success_yield(CassandraFuture *cassandra_future)
{
    if (cassandra_future->on_success_block) {
        if (rb_proc_arity(cassandra_future->on_success_block)) {
            VALUE obj = future_build_success_value(cassandra_future);

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

// Yields (value, nil) when the future succeeded and (nil, error) when it
// failed — always exactly two arguments, whatever the block's arity, so that
// a lambda callback must accept both. Only called with the block present.
static void future_result_complete_yield(CassandraFuture *cassandra_future)
{
    VALUE args[2];

    if (cass_future_error_code(cassandra_future->future) == CASS_OK) {
        args[0] = future_build_success_value(cassandra_future);
        args[1] = Qnil;
    } else {
        args[0] = Qnil;
        args[1] = ilios_future_error_new(eExecutionError, NULL, cassandra_future->future);
    }
    rb_proc_call_with_block(cassandra_future->on_complete_block, 2, args, Qnil);
}

static VALUE future_result_yielder_synchronize(VALUE future)
{
    CassandraFuture *cassandra_future;

    GET_FUTURE(future, cassandra_future);

    // Delivery has begun: registrations from now on yield inline again.
    cassandra_future->dispatch_state = dispatch_delivering;

    if (!cassandra_future->yielded) {
        // on_complete is exclusive with on_success/on_failure, so at most one
        // of these branches has a block to run.
        if (cassandra_future->on_complete_block) {
            cassandra_future->yielded = true;
            future_result_complete_yield(cassandra_future);
        } else if (cass_future_error_code(cassandra_future->future) == CASS_OK) {
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
    return Qnil;
}

static VALUE dispatch_yield_body(VALUE future)
{
    CassandraFuture *cassandra_future;

    GET_FUTURE(future, cassandra_future);
    return rb_mutex_synchronize(cassandra_future->proc_mutex, future_result_yielder_synchronize, future);
}

static VALUE dispatch_report_error(VALUE errinfo)
{
    rb_io_write(rb_stderr, rb_funcall(errinfo, id_full_message, 0));
    return Qnil;
}

// Reports a StandardError raised by a callback and swallows it; re-raises
// everything else, unwinding the calling thread.
static void dispatch_handle_callback_unwind(int state)
{
    VALUE errinfo = rb_errinfo();

    if (RTEST(rb_obj_is_kind_of(errinfo, rb_eStandardError))) {
        int report_state = 0;

        rb_set_errinfo(Qnil);
        // A raising callback must not stop delivery for the other
        // futures: report it and keep dispatching. The report is
        // protected too — a broken $stderr must not kill the dispatcher.
        rb_protect(dispatch_report_error, errinfo, &report_state);
        if (report_state) {
            rb_set_errinfo(Qnil);
        }
    } else {
        // Thread#kill, SystemExit etc. unwind this thread (recreated
        // lazily); errinfo stays in place because rb_jump_tag does not
        // carry the payload itself.
        rb_jump_tag(state);
    }
}

// Yields the callbacks of one completed future, then marks it delivered so
// #await returns only after the callbacks ran, and wakes the awaiters.
static void dispatch_process_node(future_dispatch_node *node)
{
    // While the node is off both lists, this stack reference is what pins
    // the future.
    volatile VALUE future = node->future_obj;
    CassandraFuture *cassandra_future;
    int state = 0;

    rb_protect(dispatch_yield_body, (VALUE)future, &state);

    GET_FUTURE((VALUE)future, cassandra_future);
    // The yielder flips dispatch_state on this thread when delivery begins,
    // so dispatch_owned here means an unwind hit before any callback ran.
    // Read without proc_mutex on purpose: every writer of dispatch_state runs
    // under the GVL, and this thread is the only one that writes the
    // owned->delivering transition, so no other thread can change what this
    // read observes while we hold the GVL here.
    if (cassandra_future->dispatch_state == dispatch_owned) {
        // Put the completion back for the next dispatcher round rather than
        // marking it delivered — #await must not return with the stored
        // blocks silently dropped. Back at the HEAD, the position it was
        // popped from: appending would demote this future behind every
        // completion queued meanwhile and break completion-order delivery
        // for it.
        uv_mutex_lock(&dispatch.mutex);
        dispatch_list_prepend(&dispatch.completed, node);
        uv_mutex_unlock(&dispatch.mutex);
    } else {
        // The awaiters' signal: unlike the ownership flip (which happens
        // when delivery begins), this is set only after the callbacks ran.
        // "Ran" includes "started and unwound": a non-StandardError raised
        // inside a completion this callback pumped through a nested #await
        // propagates through the enclosing callback's frame and lands here
        // too. Delivery is at-most-once by design — the partially executed
        // callback is deliberately not retried — so the future is marked
        // delivered and #await is released rather than left hanging.
        uv_mutex_lock(&dispatch.mutex);
        cassandra_future->delivered = true;
        uv_cond_broadcast(&dispatch.cond);
        uv_mutex_unlock(&dispatch.mutex);
        xfree(node);
    }

    if (state) {
        dispatch_handle_callback_unwind(state);
    }
}

// One round of the dispatcher's loop; also used by #await when it runs on
// the dispatcher thread.
static void dispatch_wait_and_process_one(void)
{
    future_dispatch_node *node = dispatch_pop_completed();

    if (node == NULL) {
        bool interrupted = false;

        rb_thread_call_without_gvl(dispatch_wait_for_completion, &interrupted, dispatch_interrupt_ubf, &interrupted);
        node = dispatch_pop_completed();
    }
    if (node) {
        dispatch_process_node(node);
    }
    rb_thread_check_ints();
}

static VALUE dispatcher_thread_body(VALUE arg)
{
    (void)arg;
    while (1) {
        dispatch_wait_and_process_one();
    }
    return Qnil;
}

static VALUE dispatcher_thread_cleanup(VALUE arg)
{
    (void)arg;
    dispatcher_running = false;
    // Publish the death into the awaiters' wait predicate before waking
    // them: seeing the bumped counter is what lets them leave the no-GVL
    // wait right away and recreate the dispatcher, instead of treating the
    // broadcast as a spurious wakeup and sleeping out their recheck interval.
    uv_mutex_lock(&dispatch.mutex);
    dispatch.dispatcher_deaths++;
    uv_cond_broadcast(&dispatch.cond);
    uv_mutex_unlock(&dispatch.mutex);
    return Qnil;
}

static VALUE dispatcher_thread_main(void *arg)
{
    (void)arg;
    dispatcher_running = true;
    return rb_ensure(dispatcher_thread_body, Qnil, dispatcher_thread_cleanup, Qnil);
}

static VALUE dispatcher_ensure_thread_body(VALUE arg)
{
    (void)arg;
    if (!RTEST(dispatcher_thread) || !RTEST(rb_funcall(dispatcher_thread, id_alive, 0))) {
        dispatcher_thread = rb_thread_create(dispatcher_thread_main, NULL);
        rb_funcall(dispatcher_thread, id_report_on_exception, 1, Qtrue);
    }
    return Qnil;
}

static void dispatcher_ensure_thread(void)
{
    // Fast path for the hot registration/await calls.
    if (dispatcher_running) {
        return;
    }
    // Serialized: two threads finding the dispatcher dead must not both
    // create one (concurrent callbacks; an orphan would also defeat
    // #await's dispatcher-thread check).
    rb_mutex_synchronize(dispatcher_mutex, dispatcher_ensure_thread_body, Qnil);
}

// Hands the future over to the dispatcher: pins it in the pending list and
// registers the native completion callback with the cpp-driver. Called
// under proc_mutex.
static void future_register_dispatch(VALUE future, CassandraFuture *cassandra_future)
{
    future_dispatch_node *node;
    CassError rc;

    // Fallible parts (Ruby calls, allocation) first: an exception, including
    // an interrupt hitting a checkpoint, must leave no half-registered state.
    dispatcher_ensure_thread();
    node = ALLOC(future_dispatch_node);
    node->future_obj = future;
    node->prev = NULL;
    node->next = NULL;

    uv_mutex_lock(&dispatch.mutex);
    dispatch_list_append(&dispatch.pending, node);
    uv_mutex_unlock(&dispatch.mutex);

    rc = cass_future_set_callback(cassandra_future->future, future_completed_native_cb, node);
    if (rc != CASS_OK) {
        // Cannot happen: dispatch_state guards against registering the
        // native callback twice. Unpin and fail loudly rather than leak.
        uv_mutex_lock(&dispatch.mutex);
        dispatch_list_remove(&dispatch.pending, node);
        uv_mutex_unlock(&dispatch.mutex);
        xfree(node);
        rb_raise(eExecutionError, "Failed to register completion callback: %s", cass_error_desc(rc));
    }

    cassandra_future->dispatch_state = dispatch_owned;
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
    cassandra_future->yielded = false;
    cassandra_future->dispatch_state = dispatch_not_registered;
    cassandra_future->delivered = false;
    cassandra_future->fork_generation = dispatch_fork_generation;

    return cassandra_future_obj;
}

// Futures created before a fork are bound to the parent's driver threads,
// which do not exist in the child: fail fast instead of hanging.
static void future_check_fork_generation(CassandraFuture *cassandra_future)
{
    if (cassandra_future->fork_generation != dispatch_fork_generation) {
        rb_raise(eExecutionError, "The future belongs to the parent process and cannot be used after fork");
    }
}

// Hands an unresolved future to the dispatcher on the first registration.
// Runs BEFORE the block is stored: the handover can raise, and a raise must
// not leave a stored block that nobody will ever deliver. Delivery cannot
// overtake the store — it synchronizes on proc_mutex, which the caller holds.
static void future_ensure_dispatch_registration(VALUE future, CassandraFuture *cassandra_future)
{
    if (cassandra_future->dispatch_state == dispatch_not_registered &&
        !cass_future_ready(cassandra_future->future)) {
        future_register_dispatch(future, cassandra_future);
    }
}

// Which outcomes a registered callback handles.
typedef enum {
    // on_success: the value, only when the future succeeded.
    callback_kind_success,
    // on_failure: the error, only when the future failed.
    callback_kind_failure,
    // on_complete: (value, nil) or (nil, error), whatever the outcome.
    callback_kind_complete
} future_callback_kind;

// Where the block of a given callback kind is stored on the future.
static VALUE *future_callback_slot(CassandraFuture *cassandra_future, future_callback_kind kind)
{
    switch (kind) {
    case callback_kind_success:
        return &cassandra_future->on_success_block;
    case callback_kind_failure:
        return &cassandra_future->on_failure_block;
    case callback_kind_complete:
        return &cassandra_future->on_complete_block;
    }
    return NULL; /* unreachable: kind is one of the three above */
}

// Rejects a registration that would leave the future with two callbacks
// claiming the same outcome. Called under proc_mutex, before the block is
// stored and before anything fallible runs, so a rejection leaves the future
// exactly as it was.
static void future_check_callback_conflict(CassandraFuture *cassandra_future, future_callback_kind kind)
{
    if (*future_callback_slot(cassandra_future, kind)) {
        rb_raise(eExecutionError, "It should not call twice");
    }
    // on_complete already covers both outcomes, so combining it with the
    // outcome-specific callbacks is rejected in either direction rather than
    // given some subtle precedence rule.
    if (kind == callback_kind_complete) {
        if (cassandra_future->on_success_block || cassandra_future->on_failure_block) {
            rb_raise(eExecutionError, "on_complete cannot be combined with on_success or on_failure");
        }
    } else if (cassandra_future->on_complete_block) {
        rb_raise(eExecutionError, "%s cannot be combined with on_complete",
                 kind == callback_kind_success ? "on_success" : "on_failure");
    }
}

// Shared registration tail, called under proc_mutex after the block was
// stored. Yields inline when the future is resolved and no longer owned by
// the dispatcher; `kind` picks which outcomes the block handles.
static void future_registration_finish(CassandraFuture *cassandra_future, future_callback_kind kind)
{
    if (!cass_future_ready(cassandra_future->future)) {
        // Unresolved: already handed to the dispatcher by
        // future_ensure_dispatch_registration.
        return;
    }
    if (cassandra_future->dispatch_state == dispatch_owned) {
        // The dispatcher will see the block just stored. Yielding inline
        // here — user code under proc_mutex — would deadlock with the
        // dispatcher blocking on this mutex as soon as the block waited
        // for another future.
        return;
    }
    if (cassandra_future->yielded) {
        return;
    }
    // The outcome is queried only where it decides something: the
    // on_complete branch runs either way, and its yielder determines the
    // outcome itself, so asking the driver here too would take the future's
    // internal lock a second time for nothing.
    switch (kind) {
    case callback_kind_success:
        if (cass_future_error_code(cassandra_future->future) == CASS_OK) {
            cassandra_future->yielded = true;
            future_result_success_yield(cassandra_future);
        }
        break;
    case callback_kind_failure:
        if (cass_future_error_code(cassandra_future->future) != CASS_OK) {
            cassandra_future->yielded = true;
            future_result_failure_yield(cassandra_future);
        }
        break;
    case callback_kind_complete:
        cassandra_future->yielded = true;
        future_result_complete_yield(cassandra_future);
        break;
    }
}

typedef struct
{
    VALUE future;
    future_callback_kind kind;
} future_registration_args;

static VALUE future_registration_synchronize(VALUE arg)
{
    future_registration_args *args = (future_registration_args *)arg;
    CassandraFuture *cassandra_future;
    VALUE block;

    GET_FUTURE(args->future, cassandra_future);

    // Checked under proc_mutex so a concurrent registration cannot slip in
    // between the check and the store.
    future_check_callback_conflict(cassandra_future, args->kind);

    block = rb_block_proc();
    future_ensure_dispatch_registration(args->future, cassandra_future);
    RB_OBJ_WRITE(args->future, future_callback_slot(cassandra_future, args->kind), block);
    future_registration_finish(cassandra_future, args->kind);
    return args->future;
}

static VALUE future_register_callback(VALUE self, future_callback_kind kind)
{
    CassandraFuture *cassandra_future;
    future_registration_args args = { self, kind };

    GET_FUTURE(self, cassandra_future);

    future_check_fork_generation(cassandra_future);
    if (!rb_block_given_p()) {
        rb_raise(rb_eArgError, "no block given");
    }

    // Registering on a future whose own callback is running on this very
    // thread: the dispatcher holds proc_mutex while yielding, and a nested
    // #await inside that callback drains other completions on the same
    // thread, so their callbacks can land here. Re-locking would raise
    // ThreadError, which the dispatcher reports and swallows — losing both
    // the registration and the rest of the pumped callback. An outer frame
    // of this same stack already holds the lock, so the mutual exclusion the
    // critical section needs is in force: run it directly.
    if (RTEST(rb_funcall(cassandra_future->proc_mutex, id_owned_p, 0))) {
        return future_registration_synchronize((VALUE)&args);
    }

    return rb_mutex_synchronize(cassandra_future->proc_mutex, future_registration_synchronize, (VALUE)&args);
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
 * @raise [Cassandra::ExecutionError] If this method will be called twice, or
 *   if +on_complete+ is already registered on this future.
 * @raise [ArgumentError] If no block was given.
 */
static VALUE future_on_success(VALUE self)
{
    return future_register_callback(self, callback_kind_success);
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
 * @raise [Cassandra::ExecutionError] If this method will be called twice, or
 *   if +on_complete+ is already registered on this future.
 * @raise [ArgumentError] If no block was given.
 */
static VALUE future_on_failure(VALUE self)
{
    return future_register_callback(self, callback_kind_failure);
}

/**
 * Run block when future resolves, whatever the outcome.
 * The block is always called with exactly two arguments: the value and +nil+
 * when the future succeeded, +nil+ and the error when it failed. There is no
 * arity special-casing, so a lambda callback must accept both arguments.
 * The callback is invoked on a background dispatcher thread as soon as the
 * future completes: callbacks of different futures run in completion order,
 * not in registration order.
 *
 * Exclusive with +on_success+ and +on_failure+ on the same future.
 *
 * @yieldparam value [Cassandra::Statement, Cassandra::Result, nil] The value, or +nil+ when the future failed.
 *   Yields +Cassandra::Statement+ object when future was created by +Cassandra::Session#prepare_async+.
 *   Yields +Cassandra::Result+ object when future was created by +Cassandra::Session#execute_async+.
 * @yieldparam error [Cassandra::ExecutionError, nil] The failure reason, or +nil+ when the future succeeded.
 * @return [Cassandra::Future] self.
 * @raise [Cassandra::ExecutionError] If this method will be called twice, or
 *   if +on_success+ or +on_failure+ is already registered on this future.
 * @raise [ArgumentError] If no block was given.
 */
static VALUE future_on_complete(VALUE self)
{
    return future_register_callback(self, callback_kind_complete);
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
    future_dispatch_state dispatch_state;

    GET_FUTURE(self, cassandra_future);

    future_check_fork_generation(cassandra_future);
    // #await from inside this future's own callback (callbacks run under
    // proc_mutex): the callback cannot wait for its own completion, and the
    // only work left on the future is the very caller — return right away.
    if (RTEST(rb_funcall(cassandra_future->proc_mutex, id_owned_p, 0))) {
        return self;
    }

    rb_mutex_lock(cassandra_future->proc_mutex);
    dispatch_state = cassandra_future->dispatch_state;
    rb_mutex_unlock(cassandra_future->proc_mutex);

    if (dispatch_state == dispatch_not_registered) {
        bool has_callback;

        // No dispatcher involved (yet): wait on the future itself, then
        // re-read under the mutex so registrations racing with this #await
        // are observed. For a dispatcher-involved future the delivered-flag
        // paths below cover completion too.
        nogvl_future_wait(cassandra_future->future);
        rb_mutex_lock(cassandra_future->proc_mutex);
        has_callback = cassandra_future->on_success_block || cassandra_future->on_failure_block ||
                       cassandra_future->on_complete_block;
        dispatch_state = cassandra_future->dispatch_state;
        rb_mutex_unlock(cassandra_future->proc_mutex);

        if (!has_callback) {
            return self;
        }
        if (dispatch_state == dispatch_not_registered) {
            // Inline-only callbacks already ran: registrations on a resolved
            // future yield under proc_mutex, and the re-read above is
            // serialized after them.
            return self;
        }
    }
    // Dispatcher involved. A callback is guaranteed to exist: registration
    // leaves dispatch_not_registered and stores the block inside the same
    // proc_mutex critical section.
    // Both `dispatcher_thread` and `dispatcher_running` below are plain
    // globals read under the GVL, which is what serializes them against the
    // dispatcher bookkeeping that writes them.
    if (rb_thread_current() == dispatcher_thread) {
        // Called from inside a callback: sleeping until the delivered flag
        // is set would deadlock because this thread is the one that sets
        // it. Yield the completed futures ourselves until this future's
        // callbacks ran.
        while (!future_delivered_p(cassandra_future)) {
            dispatch_wait_and_process_one();
        }
        return self;
    }
    // Already delivered — the common "#await after the callback ran" case.
    // Checked before touching the dispatcher so that a future with nothing
    // left to do does not resurrect a dead dispatcher thread.
    if (future_delivered_p(cassandra_future)) {
        return self;
    }
    // Wait for the dispatcher to deliver this future's callbacks, re-checking
    // on every wakeup that the dispatcher is still alive to mark futures
    // delivered (it is recreated when found dead).
    while (1) {
        // The death counter is sampled BEFORE the liveness check below, so a
        // dispatcher dying between the two is still ahead of the baseline the
        // wait predicate compares against and wakes this thread immediately.
        dispatch_await_ctx ctx = { &cassandra_future->delivered, dispatch_deaths_count(), false, false };

        dispatcher_ensure_thread();
        rb_thread_call_without_gvl(dispatch_await_wait, &ctx, dispatch_interrupt_ubf, &ctx.interrupted);
        if (ctx.done) {
            break;
        }
        rb_thread_check_ints();
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
    rb_gc_mark_movable(cassandra_future->on_complete_block);
    rb_gc_mark_movable(cassandra_future->proc_mutex);
}

static void future_destroy(void *ptr)
{
    CassandraFuture *cassandra_future = (CassandraFuture *)ptr;

    // Only the process incarnation that created these driver objects may
    // free them. In a forked child the atfork handler dropped the dispatch
    // lists, so parent-created futures become collectable here — but the
    // driver is not fork-safe: an IO thread of the parent may have been
    // mid-mutation of exactly these objects at fork time, and it does not
    // exist in the child to finish. Leak them deliberately; the child cannot
    // use the driver anyway, and the ruby-owned struct is still freed.
    if (cassandra_future->fork_generation == dispatch_fork_generation) {
        if (cassandra_future->future) {
            cass_future_free(cassandra_future->future);
        }
        if (cassandra_future->executed_statement) {
            // Safe even if the request is still in flight: the driver's
            // request holds its own reference to the statement internals.
            cass_statement_free(cassandra_future->executed_statement);
        }
    }
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
    cassandra_future->on_complete_block = rb_gc_location(cassandra_future->on_complete_block);
    cassandra_future->proc_mutex = rb_gc_location(cassandra_future->proc_mutex);
}

static void dispatch_atfork_child(void)
{
    // An IO thread may have held dispatch.mutex at fork (the forking thread
    // cannot: no Ruby API runs with it held), which would deadlock the
    // child's first GC in dispatch_registry_mark — reinitialize it. The
    // listed futures belonged to driver threads that do not exist here:
    // drop them (the nodes leak, the futures become collectable; the driver
    // itself is not usable across fork anyway).
    dispatch.pending.head = NULL;
    dispatch.pending.tail = NULL;
    dispatch.completed.head = NULL;
    dispatch.completed.tail = NULL;
    uv_mutex_init(&dispatch.mutex);
    uv_cond_init(&dispatch.cond);
    dispatcher_running = false;
    dispatch_fork_generation++;
}

void Init_future(void)
{
    VALUE registry;

    rb_undef_alloc_func(cFuture);

    rb_define_method(cFuture, "on_success", future_on_success, 0);
    rb_define_method(cFuture, "on_failure", future_on_failure, 0);
    rb_define_method(cFuture, "on_complete", future_on_complete, 0);
    rb_define_method(cFuture, "await", future_await, 0);

    uv_mutex_init(&dispatch.mutex);
    uv_cond_init(&dispatch.cond);
    pthread_atfork(NULL, NULL, dispatch_atfork_child);

    // Hidden object whose only job is to mark (and thereby pin) the futures
    // currently owned by the dispatch lists.
    registry = TypedData_Wrap_Struct(0, &dispatch_registry_data_type, &dispatch);
    rb_gc_register_mark_object(registry);

    dispatcher_thread = Qnil;
    rb_gc_register_address(&dispatcher_thread);
    dispatcher_mutex = rb_mutex_new();
    rb_gc_register_mark_object(dispatcher_mutex);
}
