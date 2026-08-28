# frozen_string_literal: true

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

    value = nil
    future.on_complete { |v, _e| value = v }
    future.await

    assert_kind_of(Ilios::Cassandra::Statement, value)
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
  end

  def test_on_complete_runs_inline_when_already_resolved
    future = Ilios::Cassandra.session.prepare_async('foo')
    future.await

    error = nil
    future.on_complete { |_v, e| error = e }
    future.await

    assert_kind_of(Ilios::Cassandra::ExecutionError, error)
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

  def test_on_complete_cannot_be_combined_with_other_callbacks
    statement = Ilios::Cassandra.session.prepare('SELECT * FROM ilios.test;')
    futures = Array.new(4) { Ilios::Cassandra.session.execute_async(statement) }

    futures.first.on_complete {}

    assert_raises(Ilios::Cassandra::ExecutionError) { futures.first.on_complete {} }
    assert_raises(Ilios::Cassandra::ExecutionError) { futures.first.on_success {} }
    assert_raises(Ilios::Cassandra::ExecutionError) { futures.first.on_failure {} }

    futures[1].on_success {}

    assert_raises(Ilios::Cassandra::ExecutionError) { futures[1].on_complete {} }

    futures[2].on_failure {}

    assert_raises(Ilios::Cassandra::ExecutionError) { futures[2].on_complete {} }
    assert_raises(ArgumentError) { futures[3].on_complete }

    futures.each(&:await)
  end

  def test_await_returns_after_on_complete_ran
    statement = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')
    statement.bind({ id: 109_000, text: 'x' })
    future = Ilios::Cassandra.session.execute_async(statement)

    done = false
    future.on_complete do
      sleep(0.2)
      done = true
    end
    future.await

    assert(done)
  end

  def test_future_all_yields_no_errors_when_all_futures_succeed
    statement = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')
    futures = Array.new(10) do |i|
      statement.bind({ id: i + 110_000, text: 'all' })
      Ilios::Cassandra.session.execute_async(statement)
    end

    aggregate = Ilios::Cassandra::Future.all(futures)
    errors = nil
    aggregate.on_complete { |errs| errors = errs }
    aggregate.await

    assert_empty(errors)
  end

  def test_future_all_collects_the_failures
    statement = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')
    statement.bind({ id: 111_000, text: 'ok' })
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

  def test_future_all_with_no_futures_resolves_immediately
    aggregate = Ilios::Cassandra::Future.all([])
    aggregate.await

    errors = nil
    aggregate.on_complete { |errs| errors = errs }

    assert_empty(errors)
  end

  def test_future_all_on_complete_accepts_only_one_callback
    aggregate = Ilios::Cassandra::Future.all([])
    aggregate.on_complete {}

    assert_raises(Ilios::Cassandra::ExecutionError) { aggregate.on_complete {} }
    assert_raises(ArgumentError) { Ilios::Cassandra::Future.all([]).on_complete }
  end

  def test_future_all_await_returns_after_the_callback_ran
    statement = Ilios::Cassandra.session.prepare('INSERT INTO ilios.test (id, text) VALUES (?, ?);')
    statement.bind({ id: 112_000, text: 'x' })
    future = Ilios::Cassandra.session.execute_async(statement)

    aggregate = Ilios::Cassandra::Future.all([future])
    done = false
    aggregate.on_complete do |_errs|
      sleep(0.2)
      done = true
    end
    aggregate.await

    assert(done)
  end

  def test_future_all_rejects_futures_that_already_have_callbacks
    statement = Ilios::Cassandra.session.prepare('SELECT * FROM ilios.test;')
    future = Ilios::Cassandra.session.execute_async(statement)
    future.on_success {}

    assert_raises(Ilios::Cassandra::ExecutionError) { Ilios::Cassandra::Future.all([future]) }
    future.await
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
end
