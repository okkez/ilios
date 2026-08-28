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
    # (future_on_failure_synchronize's cass_future_ready branch) rather than
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
    # The former SizedQueue-based thread pool blocked the registering thread
    # once ~105 registered futures were waiting to be yielded: five futures
    # whose callbacks block (a deterministic stand-in for slow futures) pinned
    # all five pool threads, the queue filled up, and the next registration
    # blocked forever. Callback registration must stay non-blocking no matter
    # how many registered futures are still waiting to be yielded.
    gate = Queue.new
    statement = Ilios::Cassandra.session.prepare(<<~CQL)
      INSERT INTO ilios.test (id, text) VALUES (?, ?);
    CQL

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
    # With the former thread pool, five in-flight slow futures occupied all
    # five pool threads in nogvl_future_wait, and a fast future registered
    # afterwards had to wait in the FIFO queue behind them (head-of-line
    # blocking). Callbacks must be delivered in completion order instead.
    insert = Ilios::Cassandra.session.prepare(<<~CQL)
      INSERT INTO ilios.test (id, text) VALUES (?, ?);
    CQL

    # The fast future gets its own session (its own connection), so that its
    # tiny response does not have to wait on the wire behind the megabytes of
    # insert payloads on the shared connection.
    fast_cluster = Ilios::Cassandra::Cluster.new
    fast_cluster.keyspace('ilios')
    fast_cluster.hosts([CASSANDRA_HOST])
    fast_session = fast_cluster.connect
    select = fast_session.prepare('SELECT id FROM ilios.test LIMIT 1;')

    order = Queue.new
    big_text = 'x' * (4 * 1024 * 1024)
    # Bind once and reuse: each execution snapshots the bound values, and a
    # single bind keeps the submission phase short so the fast future is
    # submitted well before the first slow future completes.
    insert.bind({ id: 102_000, text: big_text })
    slow_futures = Array.new(5) do
      future = Ilios::Cassandra.session.execute_async(insert)
      future.on_success { order << :slow }
      future
    end

    fast_future = fast_session.execute_async(select)
    fast_future.on_success { order << :fast }

    fast_future.await
    slow_futures.each(&:await)

    assert_equal(:fast, order.pop)
  end

  def test_pending_futures_are_not_garbage_collected
    # A future with a registered callback must stay alive until the callback
    # ran, even when no Ruby code holds a reference to it any more.
    statement = Ilios::Cassandra.session.prepare(<<~CQL)
      INSERT INTO ilios.test (id, text) VALUES (?, ?);
    CQL

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
    insert = Ilios::Cassandra.session.prepare(<<~CQL)
      INSERT INTO ilios.test (id, text) VALUES (?, ?);
    CQL
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
