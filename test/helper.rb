# frozen_string_literal: true

require 'date'
require 'ilios'
require 'minitest/autorun'
require 'securerandom'

CASSANDRA_HOST = ENV['CASSANDRA_HOST'] || '127.0.0.1'
CASSANDRA_AUTH_PORT = Integer(ENV['CASSANDRA_AUTH_PORT'] || 9043)

# Returns true when the auth-enabled node is reachable AND its default
# superuser (cassandra/cassandra) is ready to authenticate. The superuser is
# created ~10 seconds after the node boots, so retry for a while before
# giving up. When this returns false, authentication tests are skipped.
def wait_for_auth_cassandra
  require('socket')
  begin
    TCPSocket.new(CASSANDRA_HOST, CASSANDRA_AUTH_PORT).close
  rescue SystemCallError
    return false
  end

  deadline = Time.now + 180
  begin
    cluster = Ilios::Cassandra::Cluster.new
    cluster.hosts([CASSANDRA_HOST])
    cluster.port(CASSANDRA_AUTH_PORT)
    cluster.credentials('cassandra', 'cassandra')
    cluster.connect
    true
  rescue StandardError
    return false if Time.now > deadline

    sleep(5)
    retry
  end
end

module Ilios
  module Cassandra
    def self.session
      Thread.current[:ilios_cassandra_session] ||= begin
        cluster = Ilios::Cassandra::Cluster.new
        cluster.keyspace('ilios')
        cluster.hosts([CASSANDRA_HOST])
        cluster.connect
      end
    end
  end
end

def prepare_keyspace
  cluster = Ilios::Cassandra::Cluster.new
  cluster.hosts([CASSANDRA_HOST])

  session = cluster.connect
  statement = session.prepare(<<~CQL)
    CREATE KEYSPACE IF NOT EXISTS ilios
    WITH REPLICATION = {
      'class' : 'SimpleStrategy',
      'replication_factor' : 1
    };
  CQL
  session.execute(statement)
end

def prepare_table
  cluster = Ilios::Cassandra::Cluster.new
  cluster.keyspace('ilios')
  cluster.hosts([CASSANDRA_HOST])

  session = cluster.connect
  statement = session.prepare(<<~CQL)
    DROP TABLE IF EXISTS ilios.test;
  CQL
  session.execute(statement)

  statement = session.prepare(<<~CQL)
    CREATE TABLE IF NOT EXISTS ilios.test (
      id bigint,

      tinyint tinyint,
      smallint smallint,
      int int,
      bigint bigint,
      float float,
      double double,
      boolean boolean,
      text text,
      timestamp timestamp,
      uuid uuid,
      list list<int>,
      "set" set<text>,
      map map<text, bigint>,
      nested_list list<frozen<list<int>>>,
      nested_map map<text, frozen<set<int>>>,
      map_uuid_boolean map<uuid, boolean>,
      list_timestamp list<timestamp>,
      PRIMARY KEY (id)
    ) WITH compaction = { 'class' : 'LeveledCompactionStrategy' }
    AND gc_grace_seconds = 691200;
  CQL
  session.execute(statement)
end

def verify_gc_compaction
  # Asks the GC to move objects around, helping to find object movement bugs.
  GC.verify_compaction_references(expand_heap: true, toward: :empty)
end

at_exit do
  verify_gc_compaction
end

CASSANDRA_AUTH_AVAILABLE = wait_for_auth_cassandra

raise StandardError, 'auth-enabled Cassandra (CASSANDRA_AUTH_PORT) is not available on CI' if ENV['CI'] && !CASSANDRA_AUTH_AVAILABLE

prepare_keyspace
prepare_table

$default_test_log_level = Ilios::Cassandra::LOG_ERROR
Ilios::Cassandra.log_level($default_test_log_level)
