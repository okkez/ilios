# Change Log

## Unreleased

- Support username/password authentication with `Cluster#credentials` (#33)
- Add cluster options: consistency, serial_consistency, num_threads_io, queue_size_io, core_connections_per_host, constant_reconnect, exponential_reconnect, tcp_nodelay, tcp_keepalive, connection_heartbeat_interval, connection_idle_timeout, load_balance_round_robin, load_balance_dc_aware, token_aware_routing, latency_aware_routing and use_schema (#33)
- Accept Symbol consistency levels (e.g. `:quorum`) in `Cluster#consistency` and `Cluster#serial_consistency` (#33)

## 1.1.0

- Support Cassandra collection types (`list`, `set` and `map`, including nested collections). `list` maps to `Array`, `set` to `Set`, `map` to `Hash` (#28)
- Accept `Symbol` values for `text`/`ascii`/`varchar` columns and collection elements (#28)
- Require Ruby 3.4 or later (#28)
- Fix `Ilios::Cassandra::Future#await` returning before the registered callback ran (#29)

## 1.0.6

- Fix uninitialized memory being returned for an undecodable column value (#27)
- Fix segmentation fault when a Result is used after Result#next_page fails (#26)
- Fix uninitialized memory being bound for a malformed UUID string (#25)

## 1.0.5

- Fix use-after-free when re-binding a statement with in-flight async executions (#24)

## 1.0.4

-  Fix macOS build failure with Apple Clang on macOS 26+ (#23)

## 1.0.3

- Return 0 from ilios_malloc_size when no size API is available
- Add proper unblock function for nogvl_sem_wait
- Register thread-pool slots with GC only once
- Guard future callback dispatch against double invocation
- Use write barriers when mutating WB_PROTECTED structs
- Retain Statement object in async execute future
- Update libuv to v1.52.1 (#22)
- Remove unnecessary parentheses
- Remove extconf_compile_commands_json from runtime dependency
- Revert "Fix build error with -c++11-extensions"
- Revert "Disable -Werror on macOS"
- Disable -Werror on macOS
- Fix build error with -c++11-extensions
- Add workaround to avoid build error on macOS
- Use extconf_compile_commands_json gem for clangd LSP

## 1.0.2

- Fix install error with CMake 4.x

## 1.0.1

- Update libuv version to 1.50.0 (#18)
- Add Ruby 3.4 support (#17)

## 1.0.0

- Support for
  - Ruby 3.1 or later
  - Cassandra 3.0 or later
- Used libraries
  - DataStax C/C++ Driver 2.17.1
  - libuv 1.48.0
