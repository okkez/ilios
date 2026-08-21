# frozen_string_literal: true

require_relative 'helper'

class ClusterTest < Minitest::Test
  def test_hosts
    cluster = Ilios::Cassandra::Cluster.new

    assert_raises(TypeError) { cluster.hosts(Object.new) }
    assert_raises(TypeError) { cluster.hosts([1]) }
    assert_raises(ArgumentError) { cluster.hosts([]) }
    assert_kind_of(Ilios::Cassandra::Cluster, cluster.hosts([CASSANDRA_HOST]))
  end

  def test_port
    cluster = Ilios::Cassandra::Cluster.new

    assert_raises(TypeError) { cluster.port(Object.new) }
    assert_kind_of(Ilios::Cassandra::Cluster, cluster.port(9042))
  end

  def test_keyspace
    cluster = Ilios::Cassandra::Cluster.new

    assert_raises(TypeError) { cluster.keyspace(Object.new) }
    assert_kind_of(Ilios::Cassandra::Cluster, cluster.keyspace('ilios'))
  end

  def test_protocol_version
    cluster = Ilios::Cassandra::Cluster.new

    assert_raises(TypeError) { cluster.keyspace(Object.new) }
    assert_kind_of(Ilios::Cassandra::Cluster, cluster.protocol_version(Ilios::Cassandra::Cluster::PROTOCOL_VERSION_V4))
  end

  def test_connect_timeout
    cluster = Ilios::Cassandra::Cluster.new

    assert_raises(TypeError) { cluster.connect_timeout(Object.new) }
    assert_kind_of(Ilios::Cassandra::Cluster, cluster.connect_timeout(10_000))
  end

  def test_request_timeout
    cluster = Ilios::Cassandra::Cluster.new

    assert_raises(TypeError) { cluster.request_timeout(Object.new) }
    assert_kind_of(Ilios::Cassandra::Cluster, cluster.request_timeout(10_000))
  end

  def test_resolve_timeout
    cluster = Ilios::Cassandra::Cluster.new

    assert_raises(TypeError) { cluster.resolve_timeout(Object.new) }
    assert_kind_of(Ilios::Cassandra::Cluster, cluster.resolve_timeout(10_000))
  end

  def test_constant_speculative_execution_policy
    cluster = Ilios::Cassandra::Cluster.new

    assert_raises(TypeError) { cluster.constant_speculative_execution_policy(Object.new, Object.new) }
    assert_raises(ArgumentError) { cluster.constant_speculative_execution_policy(-1, 2) }
    assert_raises(ArgumentError) { cluster.constant_speculative_execution_policy(10_000, -1) }
    assert_kind_of(Ilios::Cassandra::Cluster, cluster.constant_speculative_execution_policy(10_000, 2))
  end

  def test_credentials
    cluster = Ilios::Cassandra::Cluster.new

    assert_raises(TypeError) { cluster.credentials(Object.new, 'password') }
    assert_raises(TypeError) { cluster.credentials('username', Object.new) }
    assert_kind_of(Ilios::Cassandra::Cluster, cluster.credentials('username', 'password'))

    # Credentials are ignored by a node using AllowAllAuthenticator;
    # setting them must not break the connection.
    cluster.hosts([CASSANDRA_HOST])

    assert_kind_of(Ilios::Cassandra::Session, cluster.connect)
  end

  def test_consistency
    cluster = Ilios::Cassandra::Cluster.new

    assert_raises(TypeError) { cluster.consistency(Object.new) }
    # CASS_CONSISTENCY_UNKNOWN (0xFFFF) is the only value the driver rejects
    assert_raises(ArgumentError) { cluster.consistency(0xFFFF) }
    assert_kind_of(Ilios::Cassandra::Cluster, cluster.consistency(Ilios::Cassandra::Cluster::CONSISTENCY_QUORUM))
  end

  def test_serial_consistency
    cluster = Ilios::Cassandra::Cluster.new

    assert_raises(TypeError) { cluster.serial_consistency(Object.new) }
    assert_raises(ArgumentError) { cluster.serial_consistency(0xFFFF) }
    assert_kind_of(Ilios::Cassandra::Cluster, cluster.serial_consistency(Ilios::Cassandra::Cluster::CONSISTENCY_LOCAL_SERIAL))
  end

  def test_connect_with_correct_credentials
    skip('auth-enabled Cassandra is not available') unless CASSANDRA_AUTH_AVAILABLE

    cluster = Ilios::Cassandra::Cluster.new
    cluster.hosts([CASSANDRA_HOST])
    cluster.port(CASSANDRA_AUTH_PORT)
    cluster.credentials('cassandra', 'cassandra')

    assert_kind_of(Ilios::Cassandra::Session, cluster.connect)
  end

  def test_connect_with_wrong_credentials
    skip('auth-enabled Cassandra is not available') unless CASSANDRA_AUTH_AVAILABLE

    cluster = Ilios::Cassandra::Cluster.new
    cluster.hosts([CASSANDRA_HOST])
    cluster.port(CASSANDRA_AUTH_PORT)
    cluster.credentials('cassandra', 'wrong-password')

    assert_raises(Ilios::Cassandra::ConnectError) { cluster.connect }
  end

  def test_connect_without_credentials_to_auth_node
    skip('auth-enabled Cassandra is not available') unless CASSANDRA_AUTH_AVAILABLE

    cluster = Ilios::Cassandra::Cluster.new
    cluster.hosts([CASSANDRA_HOST])
    cluster.port(CASSANDRA_AUTH_PORT)

    assert_raises(Ilios::Cassandra::ConnectError) { cluster.connect }
  end

  def test_connect
    cluster = Ilios::Cassandra::Cluster.new

    assert_raises(Ilios::Cassandra::ConnectError) { cluster.connect }

    cluster.hosts([CASSANDRA_HOST])

    assert_kind_of(Ilios::Cassandra::Session, cluster.connect)
  end
end
