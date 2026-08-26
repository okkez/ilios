#include "ilios.h"

#include <string.h>

VALUE mIlios;
VALUE mCassandra;
VALUE cCluster;
VALUE cSession;
VALUE cStatement;
VALUE cResult;
VALUE cFuture;
VALUE eConnectError;
VALUE eExecutionError;
VALUE eStatementError;

VALUE cSizedQueue;
VALUE cSet;

VALUE id_to_time;
VALUE id_to_a;
VALUE id_new;
VALUE id_push;
VALUE id_pop;
VALUE id_alive;
VALUE id_report_on_exception;
VALUE id_code;
VALUE sym_unsupported_column_type;

#if defined(HAVE_MALLOC_USABLE_SIZE)
#include <malloc.h>
#elif defined(HAVE_MALLOC_SIZE)
#include <malloc/malloc.h>
#endif

static ssize_t ilios_malloc_size(void *ptr)
{
#if defined(HAVE_MALLOC_USABLE_SIZE)
    return malloc_usable_size(ptr);
#elif defined(HAVE_MALLOC_SIZE)
    return malloc_size(ptr);
#else
    return 0;
#endif
}

static void *ilios_malloc(size_t size)
{
    rb_gc_adjust_memory_usage(size);
    return malloc(size);
}

static void *ilios_realloc(void *ptr, size_t size)
{
    ssize_t before_size = ilios_malloc_size(ptr);
    rb_gc_adjust_memory_usage(size - before_size);
    return realloc(ptr, size);
}

static void ilios_free(void *ptr)
{
    if (ptr) {
        ssize_t size = ilios_malloc_size(ptr);
        rb_gc_adjust_memory_usage(-size);
        free(ptr);
    }
}

/**
 *  Sets the log level.
 * Default is +LOG_ERROR+.
 *
 * @return [Cassandra] self.
 */
static VALUE cassandra_set_log_level(VALUE self, VALUE log_level)
{
    cass_log_set_level(NUM2INT(log_level));
    return self;
}

VALUE ilios_error_new(VALUE exception_class, VALUE message, CassError error_code)
{
    VALUE error = rb_exc_new_str(exception_class, message);

    rb_ivar_set(error, id_code, INT2NUM(error_code));

    return error;
}

VALUE ilios_future_error_new(VALUE exception_class, const char *prefix, CassFuture *future)
{
    CassError error_code;
    const char *message;
    size_t message_length;
    VALUE body;
    VALUE full_message;

    error_code = cass_future_error_code(future);
    cass_future_error_message(future, &message, &message_length);
    if (message_length == 0) {
        message = cass_error_desc(error_code);
        message_length = strlen(message);
    }

    // The driver hands back the server's raw bytes, which rb_str_new would
    // tag ASCII-8BIT; CQL identifiers and literals are UTF-8, so tag it
    // UTF-8 and keep the prefixed message in that encoding too. The message
    // can also carry driver-local text (strerror output, host names), which
    // is not guaranteed UTF-8, so replace invalid bytes rather than handing
    // back a string that raises ArgumentError on the first regexp match.
    body = rb_utf8_str_new(message, message_length);
    if (rb_enc_str_coderange(body) == ENC_CODERANGE_BROKEN) {
        body = rb_str_scrub(body, Qnil);
    }
    full_message = prefix ? rb_enc_sprintf(rb_utf8_encoding(), "%s: %"PRIsVALUE, prefix, body) : body;

    return ilios_error_new(exception_class, full_message, error_code);
}

void Init_ilios(void)
{
    rb_ext_ractor_safe(true);

    mIlios = rb_define_module("Ilios");
    mCassandra = rb_define_module_under(mIlios, "Cassandra");
    cCluster = rb_define_class_under(mCassandra, "Cluster", rb_cObject);
    cSession = rb_define_class_under(mCassandra, "Session", rb_cObject);
    cStatement = rb_define_class_under(mCassandra, "Statement", rb_cObject);
    cResult = rb_define_class_under(mCassandra, "Result", rb_cObject);
    cFuture = rb_define_class_under(mCassandra, "Future", rb_cObject);
    eConnectError = rb_define_class_under(mCassandra, "ConnectError", rb_eStandardError);
    eExecutionError = rb_define_class_under(mCassandra, "ExecutionError", rb_eStandardError);
    eStatementError = rb_define_class_under(mCassandra, "StatementError", rb_eStandardError);
    // @code is only set on errors built by ilios_error_new; the ones raised
    // elsewhere via plain rb_raise leave #code nil.
    rb_define_attr(eExecutionError, "code", 1, 0);
    rb_define_attr(eConnectError, "code", 1, 0);

    cSizedQueue = rb_const_get(rb_cThread, rb_intern("SizedQueue"));
    rb_require("set");
    cSet = rb_const_get(rb_cObject, rb_intern("Set"));
    rb_gc_register_mark_object(cSet);

    id_to_time = rb_intern("to_time");
    id_to_a = rb_intern("to_a");
    id_new = rb_intern("new");
    id_push = rb_intern("push");
    id_pop = rb_intern("pop");
    id_alive = rb_intern("alive?");
    id_report_on_exception = rb_intern("report_on_exception=");
    id_code = rb_intern("@code");
    sym_unsupported_column_type = ID2SYM(rb_intern("unsupported_column_type"));

    rb_define_module_function(mCassandra, "log_level", cassandra_set_log_level, 1);
    rb_define_const(mCassandra, "LOG_DISABLED", INT2NUM(CASS_LOG_DISABLED));
    rb_define_const(mCassandra, "LOG_CRITICAL", INT2NUM(CASS_LOG_CRITICAL));
    rb_define_const(mCassandra, "LOG_ERROR", INT2NUM(CASS_LOG_ERROR));
    rb_define_const(mCassandra, "LOG_WARN", INT2NUM(CASS_LOG_WARN));
    rb_define_const(mCassandra, "LOG_INFO", INT2NUM(CASS_LOG_INFO));
    rb_define_const(mCassandra, "LOG_DEBUG", INT2NUM(CASS_LOG_DEBUG));
    rb_define_const(mCassandra, "LOG_TRACE", INT2NUM(CASS_LOG_TRACE));

    Init_cluster();
    Init_session();
    Init_statement();
    Init_result();
    Init_future();

    cass_log_set_level(CASS_LOG_ERROR);

#if defined(HAVE_MALLOC_USABLE_SIZE) || defined(HAVE_MALLOC_SIZE)
    cass_alloc_set_functions(ilios_malloc, ilios_realloc, ilios_free);
#endif
}
