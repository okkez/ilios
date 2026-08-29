# frozen_string_literal: true

module Ilios
  module Cassandra
    # Asynchronous future returned by Session#prepare_async and
    # Session#execute_async. The class itself is defined by the C extension;
    # this file adds the Ruby-level aggregation API on top of it.
    class Future
      # Aggregate over several futures that resolves once every one of them
      # completed. Created through Ilios::Cassandra::Future.all, which
      # registers +on_complete+ on each future, so none of them may already
      # carry a callback.
      #
      # Registration is not transactional: when one of the futures rejects the
      # registration, the futures registered before it stay bound to the
      # aggregate that is then thrown away. Their callbacks still fire, but
      # nothing observes the result.
      class All
        # Installed in place of the aggregated futures once no #await can need
        # them any more, so a long-lived aggregate stops pinning them.
        EMPTY_FUTURES = [].freeze
        private_constant :EMPTY_FUTURES

        # @param futures [Array[Ilios::Cassandra::Future]] The futures to aggregate.
        # @raise [ArgumentError] If the same future is given more than once.
        # @raise [Ilios::Cassandra::ExecutionError] If one of the futures already carries a callback.
        def initialize(futures)
          # Array#to_a returns the receiver itself, so take a copy: a caller
          # mutating the array it passed in must not desync the countdown from
          # the futures #await walks.
          futures = futures.to_a.dup.freeze
          # Rejected before anything is registered: two on_complete
          # registrations on one future would raise halfway through and leave
          # the aggregate waiting for a countdown that can never reach zero.
          raise(ArgumentError, 'duplicate future given') if futures.uniq.size != futures.size

          @futures = futures
          @mutex = Mutex.new
          @cond = ConditionVariable.new
          @remaining = futures.size
          @errors = []
          @callback = nil
          @callback_finished = false
          # The thread currently running the aggregate callback. Doubles as
          # the "already claimed" marker: together with @callback_finished it
          # says whether the callback still has to run, so there is no third
          # flag that could fall out of step with these two.
          @callback_thread = nil
          # Iterates the local, not @futures: a future that already resolved
          # yields inline right here, which can count the aggregate down to
          # zero and release @futures mid-loop.
          futures.each do |future|
            future.on_complete { |_value, error| future_completed(error) }
          end
        end

        # Runs block once every aggregated future completed. The block
        # receives the failures: an empty array when all of them succeeded,
        # otherwise the Ilios::Cassandra::ExecutionError of each failed future.
        # They come in completion order, which has nothing to do with the order
        # of the futures given to Future.all, so the array cannot be indexed to
        # find out which future failed.
        #
        # The block runs exactly once: inline on the calling thread when the
        # aggregate already resolved, otherwise on the dispatcher thread that
        # delivers the last completion. An exception raised by the block
        # follows that split. It propagates out of this method in the inline
        # case, and is reported to $stderr and swallowed in the dispatcher
        # case, exactly as for Ilios::Cassandra::Future's own callbacks.
        #
        # @yieldparam errors [Array[Ilios::Cassandra::ExecutionError]] The failures.
        # @return [Ilios::Cassandra::Future::All] self.
        # @raise [Ilios::Cassandra::ExecutionError] If this method will be called twice.
        # @raise [ArgumentError] If no block was given.
        def on_complete(&block)
          raise(ArgumentError, 'no block given') unless block

          run_callback_after do
            @mutex.synchronize do
              raise(Ilios::Cassandra::ExecutionError, 'It should not call twice') if @callback

              @callback = block
            end
          end
          self
        end

        # Waits until every aggregated future completed and, when an
        # on_complete callback is registered, until that callback returned.
        #
        # Calling it on the dispatcher thread, that is from inside a future
        # callback, is safe in the cases it exists for: waiting for the
        # futures is delegated to their own #await, which keeps delivering
        # completions on that thread instead of blocking it, and a call made
        # from inside this aggregate's own callback returns right away. It is
        # not safe when the aggregate callback happens to be running
        # concurrently on *another* thread, an inline fire from #on_complete
        # racing this call for instance. #await then blocks until that
        # callback returns, and if the callback in turn waits for a completion
        # only this dispatcher thread could have delivered, the two deadlock.
        # That is the general "a callback must not block waiting for another
        # callback" rule from the README rather than anything specific to the
        # aggregate.
        #
        # @return [Ilios::Cassandra::Future::All] self.
        def await
          futures = @mutex.synchronize do
            # Nothing left to wait for. Skipping the walk is also what lets a
            # resolved aggregate drop its futures, see release_futures_if_done.
            break nil if resolved_for?(Thread.current)

            @futures
          end
          return self unless futures

          futures.each(&:await)
          @mutex.synchronize do
            @cond.wait(@mutex) until resolved_for?(Thread.current)
          end
          self
        end

        private

        # Whether #await may return for the given thread. Called under the
        # mutex.
        #
        # @param thread [Thread] The awaiting thread.
        # @return [bool] True when nothing is left to wait for.
        def resolved_for?(thread)
          return false if @remaining.nonzero?

          @callback.nil? || @callback_finished || @callback_thread == thread
        end

        # Whether the aggregate is done for every thread, not only for the one
        # running the callback. Called under the mutex.
        #
        # @return [bool] True when no #await can need the futures any more.
        def fully_resolved?
          @remaining.zero? && (@callback.nil? || @callback_finished)
        end

        # Drops the reference to the aggregated futures once no #await needs
        # to walk them, so a long-lived aggregate stops pinning their
        # driver-side result buffers. Called under the mutex.
        #
        # @return [void]
        def release_futures_if_done
          @futures = EMPTY_FUTURES if fully_resolved?
        end

        # Counts one future down. Called from that future's on_complete, on
        # the dispatcher thread.
        #
        # @param error [Ilios::Cassandra::ExecutionError, nil] The failure, or nil when the future succeeded.
        # @return [void]
        def future_completed(error)
          run_callback_after do
            @mutex.synchronize do
              @errors << error if error
              @remaining -= 1
              next if @remaining.nonzero?

              release_futures_if_done
              @cond.broadcast
            end
          end
        end

        # Applies a state change and then runs the aggregate callback, exactly
        # once, when that change resolved the aggregate.
        #
        # Async exceptions are deferred across the whole sequence, because
        # every gap in it is a permanent hang: an interrupt (Timeout.timeout,
        # Thread#raise) landing between the state change and the dispatch, or
        # between claiming the callback and entering the ensure that publishes
        # its completion, would leave the aggregate resolved with
        # @callback_finished false forever and every later #await waiting on
        # it. Only the user callback itself runs with interrupts enabled
        # again, so user code stays interruptible; interrupting it still runs
        # the ensure, which publishes completion and wakes the awaiters.
        #
        # @return [void]
        def run_callback_after
          Thread.handle_interrupt(Object => :never) do
            yield
            callback = claim_callback
            break if callback.nil?

            begin
              # Called outside the mutex, since the callback may call back
              # into this object. @errors is stable here: the countdown
              # reached zero, so no future is left to append to it.
              Thread.handle_interrupt(Object => :immediate) { callback.call(@errors) }
            ensure
              finish_callback
            end
          end
        end

        # Claims the aggregate callback for the current thread, or returns nil
        # when there is nothing to run yet or another thread already claimed
        # it.
        #
        # @return [Proc, nil] The callback this thread must run.
        def claim_callback
          @mutex.synchronize do
            next nil if @callback.nil? || @remaining.nonzero? || callback_claimed?

            @callback_thread = Thread.current
            @callback
          end
        end

        # Whether some thread already claimed the callback, and so is either
        # running it now or has finished it. Called under the mutex.
        #
        # @return [bool] True when the callback must not be run again.
        def callback_claimed?
          !@callback_thread.nil? || @callback_finished
        end

        # Publishes the callback's completion and wakes the awaiters. Called
        # with interrupts deferred, so the two flags and the broadcast cannot
        # be torn apart.
        #
        # @return [void]
        def finish_callback
          @mutex.synchronize do
            @callback_thread = nil
            @callback_finished = true
            release_futures_if_done
            @cond.broadcast
          end
        end
      end

      # Returns an aggregate that resolves once all given futures completed.
      # It registers +on_complete+ on every future, so none of them may
      # already carry a callback.
      #
      # @param futures [Array[Ilios::Cassandra::Future]] The futures to aggregate.
      # @return [Ilios::Cassandra::Future::All] The aggregate.
      # @raise [ArgumentError] If the same future is given more than once.
      # @raise [Ilios::Cassandra::ExecutionError] If one of the futures already carries a callback.
      def self.all(futures)
        All.new(futures)
      end
    end
  end
end
