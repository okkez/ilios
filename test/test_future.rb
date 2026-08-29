# frozen_string_literal: true

require 'timeout'

require_relative 'helper'

class FutureTest < Minitest::Test
  def test_await
    count = 0
    uuids = {}

    prepare_future = Ilios::Cassandra.session.prepare_async(<<~CQL)
      INSERT INTO ilios.test (
        id,
        tinyint,
        smallint,
        int,
        bigint,
        float,
        double,
        boolean,
        text,
        timestamp,
        uuid
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    CQL

    prepare_future.on_success do |statement|
      futures = []

      # Regression test for a use-after-free in the cpp driver (issue#12):
      # re-binding the same prepared statement while previous asynchronous
      # executions are still in flight must not crash nor corrupt values.
      50.times do |i|
        uuids[i] = SecureRandom.uuid
        statement.bind(
          {
            id: i,
            tinyint: i,
            smallint: i,
            int: i,
            bigint: i,
            float: i,
            double: i,
            boolean: true,
            text: 'hello',
            timestamp: Time.now,
            uuid: uuids[i]
          }
        )
        result_future = Ilios::Cassandra.session.execute_async(statement)
        result_future.on_success do
          count += 1
        end
        futures << result_future
      end
      futures.each(&:await)
    end
    prepare_future.await

    assert_equal(50, count)

    statement = Ilios::Cassandra.session.prepare(<<~CQL)
      SELECT id, uuid FROM ilios.test WHERE id IN (#{(0...50).to_a.join(', ')});
    CQL
    rows = Ilios::Cassandra.session.execute(statement).to_a

    assert_equal(50, rows.size)
    rows.each do |row|
      assert_kind_of(String, row['uuid'])
      assert_equal(uuids[row['id']], row['uuid'])
    end
  end

  def test_on_success
    statement = Ilios::Cassandra.session.prepare('SELECT * FROM ilios.test;')
    future = Ilios::Cassandra.session.execute_async(statement)

    assert_raises(ArgumentError) { future.on_success }

    future.on_success {}

    assert_raises(Ilios::Cassandra::ExecutionError) { future.on_success {} }
  end

  def test_on_failure
    statement = Ilios::Cassandra.session.prepare('SELECT * FROM ilios.test;')
    future = Ilios::Cassandra.session.execute_async(statement)

    assert_raises(ArgumentError) { future.on_failure }

    future.on_failure {}

    assert_raises(Ilios::Cassandra::ExecutionError) { future.on_failure {} }
  end

  def test_complex_case
    statement = Ilios::Cassandra.session.prepare('SELECT * FROM ilios.test;')
    future = Ilios::Cassandra.session.execute_async(statement)

    count = 0

    future.on_failure do
      count += 1
    end

    sleep(2)

    future.on_success do
      count += 1
    end

    future.await

    assert_equal(1, count)
  end

  def test_on_failure_yields_execution_error
    # invalid query, registered before the future is ready, so the callback
    # runs on the dispatcher path (future_result_yielder_synchronize).
    future = Ilios::Cassandra.session.prepare_async('foo')

    error = nil
    future.on_failure do |err|
      error = err
    end
    future.await

    assert_kind_of(Ilios::Cassandra::ExecutionError, error)
    assert_kind_of(Integer, error.code)
    assert_match(/foo/, error.message)
    assert_equal(Encoding::UTF_8, error.message.encoding)
  end

  def test_on_failure_yields_execution_error_when_already_ready
    # invalid query; #await it first so the future is already ready by the
    # time on_failure is registered, exercising the inline path
    # (future_registration_finish's cass_future_ready branch) rather than
    # the dispatcher.
    future = Ilios::Cassandra.session.prepare_async('foo')
    future.await

    error = nil
    future.on_failure do |err|
      error = err
    end
    future.await

    assert_kind_of(Ilios::Cassandra::ExecutionError, error)
    assert_kind_of(Integer, error.code)
    assert_match(/foo/, error.message)
  end

  def test_callback_registration_does_not_block_with_many_unyielded_futures
    # The former SizedQueue-based pool blocked the registering thread once
    # ~105 registered futures were waiting to be yielded: five futures with
    # blocking callbacks (a deterministic stand-in for slow futures) pinned
    # all five pool threads and the queue filled up. Registration must stay
    # non-blocking no matter how many futures await delivery.
    gate = Queue.new
    statement = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')

    gated_futures = Array.new(5) do |i|
      statement.bind({ id: i + 100_000, text: 'gate' })
      future = Ilios::Cassandra.session.execute_async(statement)
      future.on_success { gate.pop }
      future
    end

    futures = []
    registration = Thread.new do
      1000.times do |i|
        statement.bind({ id: i + 101_000, text: 'flood' })
        future = Ilios::Cassandra.session.execute_async(statement)
        future.on_failure {} # never invoked; still occupies the delivery pipeline
        futures << future
      end
    end

    assert(registration.join(30), 'registering callbacks must not block')
  ensure
    5.times { gate << :go }
    registration&.join
    gated_futures&.each(&:await)
    futures&.each(&:await)
  end

  def test_fast_future_callback_is_not_blocked_by_slow_futures
    # With the former pool, five in-flight slow futures occupied all five
    # pool threads, and a fast future registered afterwards waited its FIFO
    # turn behind them (head-of-line blocking). Callbacks must be delivered
    # in completion order instead.
    insert = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')

    # The fast future gets its own session (its own connection), so that its
    # tiny response does not have to wait on the wire behind the megabytes of
    # insert payloads on the shared connection.
    fast_cluster = Ilios::Cassandra::Cluster.new
    fast_cluster.keyspace('ilios')
    fast_cluster.hosts([CASSANDRA_HOST])
    fast_session = fast_cluster.connect
    select = fast_session.prepare('SELECT id FROM ilios.test LIMIT 1;')
    # Warm up the dedicated connection so the raced select runs at full speed.
    fast_session.execute(select)

    # Bind once and reuse: each execution snapshots the bound values, and a
    # single bind keeps the submission phase short so the fast future is
    # submitted well before the first slow future completes.
    big_text = 'x' * (4 * 1024 * 1024)
    insert.bind({ id: 102_000, text: big_text })

    # The assertion is only meaningful when the fast future completes before
    # the slow ones; a server-side pause can occasionally break that premise,
    # so retry the race. The former FIFO pool never delivered :fast first,
    # with or without retries.
    first = nil
    slow_futures = nil
    fast_future = nil
    3.times do
      order = Queue.new
      slow_futures = Array.new(5) do
        future = Ilios::Cassandra.session.execute_async(insert)
        future.on_success { order << :slow }
        future
      end

      fast_future = fast_session.execute_async(select)
      fast_future.on_success { order << :fast }

      fast_future.await
      slow_futures.each(&:await)
      first = order.pop
      break if first == :fast
    end

    assert_equal(:fast, first)
  ensure
    # Session and Cluster expose no close/disconnect, so the dedicated
    # connection is simply dropped for the GC to reclaim; make sure none of
    # this test's callbacks is still in flight when it returns, including
    # when the assertion above failed.
    fast_future&.await
    slow_futures&.each(&:await)
  end

  def test_pending_futures_are_not_garbage_collected
    # A future with a registered callback must stay alive until the callback
    # ran, even when no Ruby code holds a reference to it any more.
    statement = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')

    count = 0
    50.times do |i|
      statement.bind({ id: i + 103_000, text: 'gc' })
      Ilios::Cassandra.session.execute_async(statement).on_success { count += 1 }
    end
    10.times { GC.start }

    deadline = Time.now + 30
    sleep(0.1) while count < 50 && Time.now < deadline

    assert_equal(50, count)
  end

  def test_await_inside_callback_does_not_deadlock
    # The callback runs on the callback-delivery thread; awaiting another
    # future from there must not deadlock.
    insert = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')
    insert.bind({ id: 104_000, text: 'x' * (1024 * 1024) })
    outer = Ilios::Cassandra.session.execute_async(insert)

    inner_result = nil
    outer.on_success do
      select = Ilios::Cassandra.session.prepare('SELECT id FROM ilios.test LIMIT 1;')
      inner = Ilios::Cassandra.session.execute_async(select)
      inner.on_success { inner_result = :done }
      inner.await
    end
    outer.await

    assert_equal(:done, inner_result)
  end

  def test_late_callback_on_future_owned_by_dispatcher_does_not_deadlock
    # A dispatcher-owned future (callback registered while pending) that
    # completed but was not delivered yet must not run a late-registered
    # callback inline: user code under the future's mutex deadlocked with
    # the dispatcher as soon as the callback waited for another future.
    gate = Queue.new
    done = Queue.new
    statement = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')

    statement.bind({ id: 106_000, text: 'gate' })
    gated = Ilios::Cassandra.session.execute_async(statement)
    gated.on_success { gate.pop }

    statement.bind({ id: 106_001, text: 'a' })
    future_a = Ilios::Cassandra.session.execute_async(statement)
    future_a.on_failure {}

    statement.bind({ id: 106_002, text: 'b' })
    future_b = Ilios::Cassandra.session.execute_async(statement)
    future_b.on_success {}

    # Let all three futures complete while the dispatcher is parked inside
    # the gated callback, so future_a is resolved but not yet delivered.
    sleep(1)
    releaser = Thread.new do
      sleep(0.5)
      gate << :go
    end

    future_a.on_success do
      future_b.await
      done << :ok
    end

    assert_equal(:ok, done.pop(timeout: 15), 'late-registered callback did not run within 15s (deadlock?)')
    releaser.join
    [gated, future_a, future_b].each(&:await)
  end

  def test_callback_may_register_on_the_future_being_delivered_around_it
    # While the dispatcher delivers `outer`'s callback it holds that future's
    # mutex, and a nested #await inside the callback drains other completed
    # futures on the same thread, so `pumped`'s callback runs inside
    # `outer`'s critical section. Registering another callback on `outer`
    # from there must not try to re-lock the mutex: that raised ThreadError,
    # which the dispatcher reports and swallows, losing the registration and
    # aborting the rest of `pumped`'s callback while still marking it
    # delivered.
    select = Ilios::Cassandra.session.prepare('SELECT id FROM ilios.test LIMIT 1;')
    outer = Ilios::Cassandra.session.execute_async(select)
    pumped_result = Queue.new

    pumped = nil
    inner = nil
    outer.on_success do
      pumped = Ilios::Cassandra.session.execute_async(select)
      pumped.on_success do
        outer.on_failure {}
        pumped_result << :ok
      rescue StandardError => e
        pumped_result << e
      end
      # Give `pumped` time to complete, so it is already queued when the
      # nested await below starts draining completions on this thread.
      sleep(0.5)

      inner = Ilios::Cassandra.session.execute_async(select)
      # A registered callback is what makes the await below take the
      # dispatcher path that drains completions.
      inner.on_success {}
      inner.await
    end
    outer.await

    assert_equal(:ok, pumped_result.pop(timeout: 15), 'the pumped callback did not run to completion')
    # The registration really took effect rather than being swallowed.
    assert_raises(Ilios::Cassandra::ExecutionError) { outer.on_failure {} }
    [pumped, inner].each(&:await)
  end

  def test_concurrent_awaits_all_return_after_the_callback_ran
    # Every #await must honor the "returns after the callbacks ran"
    # contract, including a second await racing the first one.
    statement = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')
    statement.bind({ id: 108_000, text: 'x' })
    future = Ilios::Cassandra.session.execute_async(statement)

    callback_ran = false
    future.on_success do
      sleep(0.2)
      callback_ran = true
    end

    awaiters = Array.new(2) do
      Thread.new do
        future.await
        callback_ran
      end
    end

    assert_equal([true, true], awaiters.map(&:value))
  end

  def test_await_inside_the_futures_own_callback_returns
    statement = Ilios::Cassandra.session.prepare('SELECT * FROM ilios.test;')
    future = Ilios::Cassandra.session.execute_async(statement)

    result = nil
    future.on_success do
      future.await
      result = :returned
    end
    future.await

    assert_equal(:returned, result)
  end

  def test_futures_from_the_parent_process_raise_in_a_forked_child
    skip('fork is unavailable') unless Process.respond_to?(:fork)

    statement = Ilios::Cassandra.session.prepare('SELECT * FROM ilios.test;')
    future = Ilios::Cassandra.session.execute_async(statement)
    future.on_failure {}

    pid = fork do
      future.await
      exit!(1)
    rescue Ilios::Cassandra::ExecutionError
      exit!(0)
    end
    watchdog = Thread.new do
      sleep(15)
      begin
        Process.kill(:KILL, pid)
      rescue StandardError
        nil
      end
    end
    _, status = Process.waitpid2(pid)
    watchdog.kill
    future.await

    assert_predicate(status, :success?)
  end

  def test_on_failure_with_zero_arity_block_still_works
    # invalid query so the failure path actually runs; a lambda instead of a
    # block so that passing the error to a zero-arity callback raises
    # ArgumentError instead of being silently ignored
    future = Ilios::Cassandra.session.prepare_async('foo')

    count = 0
    zero_arity_callback = -> { count += 1 }
    future.on_failure(&zero_arity_callback)
    future.await

    assert_equal(1, count)
  end

  def test_on_complete_yields_result_and_nil_on_success
    statement = Ilios::Cassandra.session.prepare('SELECT * FROM ilios.test;')
    future = Ilios::Cassandra.session.execute_async(statement)

    value = :unset
    error = :unset
    future.on_complete do |v, e|
      value = v
      error = e
    end
    future.await

    assert_kind_of(Ilios::Cassandra::Result, value)
    assert_nil(error)
  end

  def test_on_complete_yields_statement_for_prepare_async
    future = Ilios::Cassandra.session.prepare_async('SELECT * FROM ilios.test;')

    value = :unset
    error = :unset
    future.on_complete do |v, e|
      value = v
      error = e
    end
    future.await

    assert_kind_of(Ilios::Cassandra::Statement, value)
    assert_nil(error)
  end

  def test_on_complete_yields_nil_and_execution_error_on_failure
    future = Ilios::Cassandra.session.prepare_async('foo')

    value = :unset
    error = nil
    future.on_complete do |v, e|
      value = v
      error = e
    end
    future.await

    assert_nil(value)
    assert_kind_of(Ilios::Cassandra::ExecutionError, error)
    assert_kind_of(Integer, error.code)
    assert_match(/foo/, error.message)
    assert_equal(Encoding::UTF_8, error.message.encoding)
  end

  def test_on_complete_always_receives_two_arguments
    # A lambda has strict arity, so a callback invoked with a different number
    # of arguments raises ArgumentError instead of silently ignoring them:
    # this pins down "always exactly two arguments, no arity special-casing"
    # for both outcomes.
    success = Ilios::Cassandra.session.prepare_async('SELECT * FROM ilios.test;')
    failure = Ilios::Cassandra.session.prepare_async('foo')

    success_args = nil
    failure_args = nil
    success_callback = ->(value, error) { success_args = [value, error] }
    failure_callback = ->(value, error) { failure_args = [value, error] }
    success.on_complete(&success_callback)
    failure.on_complete(&failure_callback)
    [success, failure].each(&:await)

    assert_kind_of(Ilios::Cassandra::Statement, success_args.first)
    assert_nil(success_args.last)
    assert_nil(failure_args.first)
    assert_kind_of(Ilios::Cassandra::ExecutionError, failure_args.last)
  end

  def test_on_complete_runs_inline_when_already_resolved
    # #await first, so the future is already resolved when on_complete is
    # registered: the callback runs inline instead of on the dispatcher.
    future = Ilios::Cassandra.session.prepare_async('foo')
    future.await

    error = :unset
    future.on_complete { |_value, e| error = e }

    assert_kind_of(Ilios::Cassandra::ExecutionError, error)
    future.await
  end

  def test_on_complete_is_called_exactly_once
    statement = Ilios::Cassandra.session.prepare('SELECT * FROM ilios.test;')
    future = Ilios::Cassandra.session.execute_async(statement)

    count = 0
    future.on_complete { count += 1 }
    future.await
    future.await

    assert_equal(1, count)
  end

  def test_on_complete_cannot_be_registered_twice
    statement = Ilios::Cassandra.session.prepare('SELECT * FROM ilios.test;')
    future = Ilios::Cassandra.session.execute_async(statement)

    assert_raises(ArgumentError) { future.on_complete }

    future.on_complete {}

    assert_raises(Ilios::Cassandra::ExecutionError) { future.on_complete {} }
    future.await
  end

  def test_on_complete_cannot_be_combined_with_on_success_or_on_failure
    statement = Ilios::Cassandra.session.prepare('SELECT * FROM ilios.test;')
    futures = Array.new(3) { Ilios::Cassandra.session.execute_async(statement) }

    # on_complete first, then the outcome-specific callbacks.
    futures.first.on_complete {}

    assert_raises(Ilios::Cassandra::ExecutionError) { futures.first.on_success {} }
    assert_raises(Ilios::Cassandra::ExecutionError) { futures.first.on_failure {} }

    # ... and the other direction.
    futures[1].on_success {}

    assert_raises(Ilios::Cassandra::ExecutionError) { futures[1].on_complete {} }

    futures[2].on_failure {}

    assert_raises(Ilios::Cassandra::ExecutionError) { futures[2].on_complete {} }
    futures.each(&:await)
  end

  def test_await_returns_after_on_complete_ran
    statement = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')
    statement.bind({ id: 109_000, text: 'x' })
    future = Ilios::Cassandra.session.execute_async(statement)

    done = false
    future.on_complete do |_value, _error|
      sleep(0.2)
      done = true
    end
    future.await

    assert(done, '#await returned before the on_complete callback had run')
  end

  def test_registering_from_inside_on_complete_raises_instead_of_thread_error
    # The callback runs under the future's own mutex, so a registration from
    # inside it takes the reentrant (already-owned) path. It must reach the
    # mixing check and raise ExecutionError rather than ThreadError from
    # re-locking the mutex.
    statement = Ilios::Cassandra.session.prepare('SELECT * FROM ilios.test;')
    future = Ilios::Cassandra.session.execute_async(statement)

    seen = Queue.new
    future.on_complete do |_value, _error|
      future.on_success {}
      seen << :no_error
    rescue StandardError => e
      seen << e
    end
    future.await

    assert_kind_of(Ilios::Cassandra::ExecutionError, seen.pop(timeout: 15))
  end

  def test_on_complete_is_rejected_after_the_other_callback_already_ran
    # The mixing check must run before the inline-yield path, so that a late
    # registration on an already-delivered future is rejected instead of
    # yielding a second time.
    delivered = Ilios::Cassandra.session.prepare_async('SELECT * FROM ilios.test;')
    delivered.on_success {}
    delivered.await

    assert_raises(Ilios::Cassandra::ExecutionError) { delivered.on_complete {} }

    completed = Ilios::Cassandra.session.prepare_async('SELECT * FROM ilios.test;')
    count = 0
    completed.on_complete { count += 1 }
    completed.await

    assert_raises(Ilios::Cassandra::ExecutionError) { completed.on_success {} }
    assert_raises(Ilios::Cassandra::ExecutionError) { completed.on_failure {} }
    assert_equal(1, count)
  end
end

class FutureAllTest < Minitest::Test
  def test_yields_no_errors_when_every_future_succeeds
    statement = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')
    futures = Array.new(10) do |i|
      statement.bind({ id: i + 111_000, text: 'all' })
      Ilios::Cassandra.session.execute_async(statement)
    end

    aggregate = Ilios::Cassandra::Future.all(futures)
    errors = nil
    aggregate.on_complete { |errs| errors = errs }
    aggregate.await

    assert_empty(errors)
  end

  def test_collects_the_failures
    statement = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')
    statement.bind({ id: 112_000, text: 'ok' })
    good = Ilios::Cassandra.session.execute_async(statement)
    bad = Array.new(2) { Ilios::Cassandra.session.prepare_async('foo') }

    aggregate = Ilios::Cassandra::Future.all([good, *bad])
    errors = nil
    aggregate.on_complete { |errs| errors = errs }
    aggregate.await

    assert_equal(2, errors.size)
    errors.each do |error|
      assert_kind_of(Ilios::Cassandra::ExecutionError, error)
      assert_match(/foo/, error.message)
    end
  end

  def test_empty_futures_array_resolves_immediately
    aggregate = Ilios::Cassandra::Future.all([])
    aggregate.await

    errors = :unset
    aggregate.on_complete { |errs| errors = errs }

    assert_empty(errors)
  end

  def test_on_complete_runs_inline_when_already_resolved
    statement = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')
    futures = Array.new(3) do |i|
      statement.bind({ id: i + 113_000, text: 'resolved' })
      Ilios::Cassandra.session.execute_async(statement)
    end
    futures.each(&:await)

    aggregate = Ilios::Cassandra::Future.all(futures)
    aggregate.await

    errors = :unset
    aggregate.on_complete { |errs| errors = errs }

    assert_empty(errors)
  end

  def test_on_complete_accepts_only_one_callback
    aggregate = Ilios::Cassandra::Future.all([])

    assert_raises(ArgumentError) { aggregate.on_complete }

    aggregate.on_complete {}

    assert_raises(Ilios::Cassandra::ExecutionError) { aggregate.on_complete {} }
  end

  def test_on_complete_is_called_exactly_once
    statement = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')
    futures = Array.new(5) do |i|
      statement.bind({ id: i + 114_000, text: 'once' })
      Ilios::Cassandra.session.execute_async(statement)
    end

    aggregate = Ilios::Cassandra::Future.all(futures)
    count = 0
    aggregate.on_complete { |_errors| count += 1 }
    aggregate.await
    aggregate.await

    assert_equal(1, count)
  end

  def test_await_returns_after_the_callback_ran
    statement = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')
    statement.bind({ id: 115_000, text: 'x' })
    future = Ilios::Cassandra.session.execute_async(statement)

    aggregate = Ilios::Cassandra::Future.all([future])
    done = false
    aggregate.on_complete do |_errors|
      sleep(0.2)
      done = true
    end
    aggregate.await

    assert(done, '#await returned before the aggregate callback had run')
  end

  def test_await_inside_a_future_callback_does_not_deadlock
    # The whole aggregate is created and awaited on the dispatcher thread:
    # blocking on a ConditionVariable there would stall the process, since
    # this very thread is the one that would have to signal it.
    insert = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')
    insert.bind({ id: 116_000, text: 'x' })
    trigger = Ilios::Cassandra.session.execute_async(insert)

    done = Queue.new
    trigger.on_success do
      select = Ilios::Cassandra.session.prepare('SELECT id FROM ilios.test LIMIT 1;')
      inner = Array.new(3) { Ilios::Cassandra.session.execute_async(select) }
      aggregate = Ilios::Cassandra::Future.all(inner)
      errors = nil
      aggregate.on_complete { |errs| errors = errs }
      aggregate.await
      done << errors
    end
    trigger.await

    errors = done.pop(timeout: 15)

    refute_nil(errors, 'Future::All#await did not return on the dispatcher thread')
    assert_empty(errors)
  end

  def test_await_inside_the_aggregate_callback_returns
    # The aggregate callback runs on the dispatcher thread; #await from
    # inside it must recognize the calling thread as the one running the
    # callback and return instead of waiting for itself.
    statement = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')
    futures = Array.new(3) do |i|
      statement.bind({ id: i + 117_000, text: 'reentrant' })
      Ilios::Cassandra.session.execute_async(statement)
    end

    aggregate = Ilios::Cassandra::Future.all(futures)
    done = Queue.new
    aggregate.on_complete do |errors|
      aggregate.await
      done << errors
    end

    errors = done.pop(timeout: 15)

    refute_nil(errors, 'Future::All#await did not return from inside its own callback')
    assert_empty(errors)
    aggregate.await
  end

  def test_callback_can_await_the_aggregated_futures
    statement = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')
    futures = Array.new(5) do |i|
      statement.bind({ id: i + 118_000, text: 'batch' })
      Ilios::Cassandra.session.execute_async(statement)
    end

    aggregate = Ilios::Cassandra::Future.all(futures)
    done = Queue.new
    aggregate.on_complete do |errors|
      futures.each(&:await)
      done << errors
    end

    errors = done.pop(timeout: 15)

    refute_nil(errors, 'aggregate callback did not finish (swallowed error?)')
    assert_empty(errors)
    aggregate.await
  end

  def test_rejects_duplicate_futures_before_registering_anything
    statement = Ilios::Cassandra.session.prepare('SELECT * FROM ilios.test;')
    future = Ilios::Cassandra.session.execute_async(statement)

    assert_raises(ArgumentError) { Ilios::Cassandra::Future.all([future, future]) }
    # Nothing was registered, so the future is still free to take a callback.
    future.on_success {}
    future.await
  end

  def test_rejects_futures_that_already_carry_a_callback
    statement = Ilios::Cassandra.session.prepare('SELECT * FROM ilios.test;')
    future = Ilios::Cassandra.session.execute_async(statement)
    future.on_success {}

    assert_raises(Ilios::Cassandra::ExecutionError) { Ilios::Cassandra::Future.all([future]) }
    future.await
  end

  def test_rejects_futures_that_already_carry_an_on_complete_callback
    # The aggregate claims each future's on_complete slot, so a future that
    # already uses it is rejected too, by the double-registration check rather
    # than by the mixing check.
    statement = Ilios::Cassandra.session.prepare('SELECT * FROM ilios.test;')
    future = Ilios::Cassandra.session.execute_async(statement)
    future.on_complete { |_value, _error| }

    assert_raises(Ilios::Cassandra::ExecutionError) { Ilios::Cassandra::Future.all([future]) }
    future.await
  end

  def test_await_returns_after_the_callback_was_interrupted
    # An async exception thrown into the thread running the aggregate callback
    # must not strand the aggregate: the completion has to be published and
    # the awaiters woken even when the callback never returns normally.
    statement = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')
    futures = Array.new(2) do |i|
      statement.bind({ id: i + 119_000, text: 'interrupt' })
      Ilios::Cassandra.session.execute_async(statement)
    end
    # Resolve them first, so the callback fires inline on this thread and the
    # interrupt below is guaranteed to land inside it.
    futures.each(&:await)

    aggregate = Ilios::Cassandra::Future.all(futures)
    assert_raises(Timeout::Error) do
      Timeout.timeout(0.2) { aggregate.on_complete { |_errors| sleep(30) } }
    end

    assert_same(aggregate, Timeout.timeout(15) { aggregate.await })
  end

  def test_releases_the_aggregated_futures_once_resolved
    # A long-lived aggregate must not keep every member future (and its
    # driver-side result buffer) alive for its own lifetime.
    statement = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')
    futures = Array.new(3) do |i|
      statement.bind({ id: i + 120_000, text: 'release' })
      Ilios::Cassandra.session.execute_async(statement)
    end

    aggregate = Ilios::Cassandra::Future.all(futures)
    aggregate.on_complete { |_errors| }
    aggregate.await

    assert_empty(aggregate.instance_variable_get(:@futures))
    # Still honors its contract afterwards.
    assert_same(aggregate, aggregate.await)
  end
end
