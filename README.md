# Ilios

[![Gem Version](https://badge.fury.io/rb/ilios.svg)](https://badge.fury.io/rb/ilios)
[![CI](https://github.com/Watson1978/ilios/actions/workflows/CI.yml/badge.svg)](https://github.com/Watson1978/ilios/actions/workflows/CI.yml)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/Watson1978/ilios)

Ilios that Cassandra driver written by C language for Ruby using [DataStax C/C++ Driver](https://github.com/datastax/cpp-driver).

## Installation

Install the gem and add to the application's Gemfile by executing:

```sh
$ bundle add ilios
```

If bundler is not being used to manage dependencies, install the gem by executing:

```sh
$ gem install ilios
```

This gem's installer will install the DataStax C/C++ Driver to the appropriate location automatically.
However, if you prefer to install the DataStax C/C++ Driver manually, you can do so by executing:

```sh
$ bundle config set --local build.ilios --with-libuv-dir=/path/to/libuv-installed-dir
$ bundle config set --local build.ilios --with-cassandra-driver-dir=/path/to/cassandra-cpp-driver-installed-dir
$ bundle add ilios
```

or

```sh
$ gem install ilios -- --with-libuv-dir=/path/to/libuv-installed-dir --with-cassandra-driver-dir=/path/to/cassandra-cpp-driver-installed-dir
```

## Requirements

- cmake (in order to build the DataStax C/C++ Driver and libuv)
- C/C++ compiler
- install_name_tool (macOS only)

## Supported

- Ruby 3.4 or later
- Cassandra 3.0 or later
- Linux and macOS platform

## Example
### Basic usage

Create the keyspace in advance using the `cqlsh` command.

```cql
CREATE KEYSPACE IF NOT EXISTS ilios
WITH REPLICATION = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 1
};
```

Then, you can run the following code.

```ruby
require 'ilios'

cluster = Ilios::Cassandra::Cluster.new
cluster.keyspace('ilios')
cluster.hosts(['127.0.0.1'])
session = cluster.connect

# Create the table
statement = session.prepare(<<~CQL)
  CREATE TABLE IF NOT EXISTS ilios.example (
    id bigint,
    message text,
    created_at timestamp,
    PRIMARY KEY (id)
  ) WITH compaction = { 'class' : 'LeveledCompactionStrategy' }
  AND gc_grace_seconds = 691200;
CQL
session.execute(statement)

# Insert the records
statement = session.prepare(<<~CQL)
  INSERT INTO ilios.example (
    id,
    message,
    created_at
  ) VALUES (?, ?, ?)
CQL

100.times do |i|
  statement.bind({
    id: i,
    message: 'Hello World',
    created_at: Time.now,
  })
  session.execute(statement)
end

# Select the records
statement = session.prepare(<<~CQL)
  SELECT * FROM ilios.example
CQL
statement.idempotent = true
statement.page_size = 25
result = session.execute(statement)
result.each do |row|
  p row
end

while(result.next_page)
  result.each do |row|
    p row
  end
end
```

### Collection types

`list`, `set` and `map` columns (including nested collections such as
`list<frozen<list<int>>>`) are supported.

```ruby
statement = session.prepare(<<~CQL)
  CREATE TABLE IF NOT EXISTS ilios.collection_example (
    id bigint,
    tags set<text>,
    scores list<int>,
    attributes map<text, bigint>,
    PRIMARY KEY (id)
  );
CQL
session.execute(statement)

statement = session.prepare(<<~CQL)
  INSERT INTO ilios.collection_example (
    id,
    tags,
    scores,
    attributes
  ) VALUES (?, ?, ?, ?)
CQL
statement.bind({
  id: 1,
  tags: Set['ruby', 'cassandra'],  # or an Array
  scores: [85, 92],
  attributes: { 'height' => 180 },
})
session.execute(statement)

statement = session.prepare(<<~CQL)
  SELECT * FROM ilios.collection_example
CQL
session.execute(statement).each do |row|
  row['tags']       # => Set["cassandra", "ruby"]
  row['scores']     # => [85, 92]
  row['attributes'] # => {"height" => 180}
end
```

Notes:

- A `set` column accepts both `Set` and `Array` on bind, and is always returned as a `Set`.
- A `list` column also accepts both `Array` and `Set` on bind, but is always returned as an `Array`. Binding a `Set` to a `list` column keeps the order `Set#to_a` returns.
- Cassandra stores an empty non-frozen collection as `null`, so inserting `[]`, `Set.new` or `{}` returns `nil` on select. This is Cassandra's data model, not an Ilios limitation.
- `nil` is not allowed as a collection element (Cassandra collections cannot contain `null`).
- `Symbol` is accepted for `text` (as well as `ascii` and `varchar`) columns and collection elements, and is stored (and returned) as a `String`.
- Because a `Symbol` map key is stored as its `String` equivalent, binding a map that contains both (for example `{ k1: 1, 'k1' => 2 }`) ends up as a single key on the server; the entry bound last wins.
- Binding a `String` containing a NUL character (`\0`) to a `text` (or `ascii` / `varchar`) column raises `ArgumentError` (known limitation).

### Authentication and cluster options

Username/password authentication (Cassandra's `PasswordAuthenticator`) and
common cluster options are configured on `Cluster` before `connect`:

```ruby
cluster = Ilios::Cassandra::Cluster.new
cluster.hosts(['127.0.0.1'])
cluster.credentials('username', 'password')
cluster.consistency(:quorum) # or Ilios::Cassandra::Cluster::CONSISTENCY_QUORUM
cluster.num_threads_io(4)
cluster.exponential_reconnect(2_000, 60_000)
cluster.tcp_keepalive(true, 60)
cluster.load_balance_dc_aware('dc1')
session = cluster.connect
```

See the RBS signatures in `sig/ilios.rbs` for the full list of options.

### Synchronous API
`Ilios::Cassandra::Session#prepare` and `Ilios::Cassandra::Session#execute` are provided as synchronous API.

```ruby
statement = session.prepare(<<~CQL)
  SELECT * FROM ilios.example
CQL
result = session.execute(statement)
```

### Asynchronous API
`Ilios::Cassandra::Session#prepare_async` and `Ilios::Cassandra::Session#execute_async` are provided as asynchronous API.

```ruby
prepare_future = session.prepare_async(<<~CQL)
  INSERT INTO ilios.example (
    id,
    message,
    created_at
  ) VALUES (?, ?, ?)
CQL

prepare_future.on_success { |statement|
  futures = []

  10.times do |i|
    statement.bind({
      id: i,
      message: 'Hello World',
      created_at: Time.now,
    })
    result_future = session.execute_async(statement)
    result_future.on_success { |result|
      p result
      p "success"
    }
    # `error` is an `Ilios::Cassandra::ExecutionError` (a StandardError with
    # a #code Integer attribute); the block is optional-arity, so `{ p "fail" }`
    # still works without it.
    result_future.on_failure { |error|
      p error.message
      p error.code
    }

    futures << result_future
  end
  futures.each(&:await)
}

prepare_future.await
```

Notes:

- Callbacks are delivered by a single background dispatcher thread in **completion order**: the order in which callbacks of different futures run is not guaranteed, and in particular is not the registration order.
- Registering a callback never blocks on other futures, no matter how many of them are still in flight. The one case where it waits is a second registration on the *same* future while that future's callback is running: the two are serialized, so it blocks for the duration of that callback.
- Blocking inside a callback delays the delivery of other futures' callbacks. Calling `Future#await` inside a callback is safe (the dispatcher keeps delivering completions while waiting, and awaiting the future from its own callback returns immediately), but a callback must not otherwise block waiting for another callback to run — e.g. popping a queue that only another callback pushes — because no other callback can run until the current one returns: that pattern deadlocks.
- An exception raised by a callback is reported to `$stderr` and does not stop callback delivery for other futures.
- There is no backpressure: when callbacks are delivered more slowly than futures complete, undelivered completions queue up in memory. (The previous thread pool implicitly capped this by blocking registration at ~105 in-flight futures.)
- The underlying DataStax C/C++ driver is not fork-safe: after `fork`, the child process must not use inherited ilios objects — nor rely on garbage-collecting them, since freeing an inherited `Session` waits forever for driver threads that do not exist in the child. Using an inherited `Future` in the child raises `ExecutionError`.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/Watson1978/ilios.
