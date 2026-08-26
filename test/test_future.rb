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
    # runs on the thread pool path (future_result_yielder_synchronize).
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
    # the thread pool.
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
