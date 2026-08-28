# Change Log

## Unreleased

- Add `Future#on_complete`, which handles both outcomes with a single callback: the block receives `(value, nil)` on success and `(nil, ExecutionError)` on failure, exactly once. It cannot be combined with `on_success`/`on_failure` on the same future
- Add `Ilios::Cassandra::Future.all`, returning an aggregate that resolves once all given futures completed; its `on_complete` receives the failures (an empty array when everything succeeded) and `#await` waits for all futures and the callback

- Deliver future callbacks event-driven via `cass_future_set_callback` instead of a fixed thread pool polling a bounded queue. Registering `Future#on_success` / `Future#on_failure` no longer blocks once ~105 registered futures are in flight, and a slow future no longer delays the callbacks of faster futures completing behind it (head-of-line blocking). Callbacks now run on a single dispatcher thread in completion order; an exception raised by a callback is reported to `$stderr` and no longer kills a delivery thread. Public API and `Future#await` semantics are unchanged. Behavioral note: since all callbacks share one dispatcher thread, a callback that blocks waiting for **another callback** to run now deadlocks, where the former 5-thread pool happened to tolerate a few such callbacks; `Future#await` inside callbacks remains safe (see README)
- `Future#await` called from inside the future's own callback returns immediately instead of raising `ThreadError`, and a `Future` inherited across `fork` raises `ExecutionError` instead of hanging

## 1.2.0

- Fix cluster setters silently ignoring invalid values in `Cluster#port`, `#protocol_version`, `#connect_timeout`, `#request_timeout` and `#resolve_timeout` (#36)
- Fix segfault when a `Cluster` created by `allocate` or `dup` is used without running `initialize` (#37)
- Yield the failure reason as an `Ilios::Cassandra::ExecutionError` to `Future#on_failure` blocks that accept an argument, including variadic blocks (e.g. `{ |*args| }`); zero-arity blocks are unchanged (#40)
- The synchronous API (`Session#prepare`, `Session#execute`, `Result#next_page`, `Cluster#connect`) now raises errors carrying the server-reported message and a `#code` Integer, same as `Future#on_failure` (#40)
- `Ilios::Cassandra::StatementError` also carries a `#code` Integer, so `#code` is answered by every error class the driver raises (#40)
- Fix `Future#on_success` RBS signature to allow the `Statement` yielded by `prepare_async` (#42)

## 1.1.2

- Support username/password authentication with `Cluster#credentials` (#33)
- Add cluster options: consistency, serial_consistency, num_threads_io, queue_size_io, core_connections_per_host, constant_reconnect, exponential_reconnect, tcp_nodelay, tcp_keepalive, connection_heartbeat_interval, connection_idle_timeout, load_balance_round_robin, load_balance_dc_aware, token_aware_routing, latency_aware_routing and use_schema (#33)
- Accept Symbol consistency levels (e.g. `:quorum`) in `Cluster#consistency` and `Cluster#serial_consistency` (#33)
- Fix infinite loop when binding a `list`/`set` whose element appends to the array while it is being converted (#34)
- Fix infinite loop in `Cluster#hosts` when an element appends to the array while it is being converted (#34)

## 1.1.1

- Fix out-of-bounds read when binding a `list`/`set` whose element mutates the array while it is being converted (#31)
- Verify the sha256 checksum of the libuv and cpp-driver archives downloaded at build time (#32)

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
