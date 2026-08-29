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
end
