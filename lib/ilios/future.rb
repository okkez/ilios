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
        # @param futures [Array[Ilios::Cassandra::Future]] The futures to aggregate.
        # @raise [ArgumentError] If the same future is given more than once.
        # @raise [Ilios::Cassandra::ExecutionError] If one of the futures already carries a callback.
        def initialize(futures)
          futures = futures.to_a
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
          @callback_invoked = false
          @callback_finished = false
          # The thread currently running the aggregate callback, so #await
          # called from inside that callback returns instead of waiting for
          # the thread it is running on.
          @callback_thread = nil
          futures.each do |future|
            future.on_complete { |_value, error| future_completed(error) }
          end
        end

        # Runs block once every aggregated future completed. The block
        # receives the failures: an empty array when all of them succeeded,
        # otherwise the Ilios::Cassandra::ExecutionError of each failed future,
        # in completion order. It runs exactly once, inline when the aggregate
        # already resolved, otherwise on the dispatcher thread that delivers
        # the last completion.
        #
        # @yieldparam errors [Array[Ilios::Cassandra::ExecutionError]] The failures.
        # @return [Ilios::Cassandra::Future::All] self.
        # @raise [Ilios::Cassandra::ExecutionError] If this method will be called twice.
        # @raise [ArgumentError] If no block was given.
        def on_complete(&block)
          raise(ArgumentError, 'no block given') unless block

          @mutex.synchronize do
            raise(Ilios::Cassandra::ExecutionError, 'It should not call twice') if @callback

            @callback = block
          end
          invoke_callback_if_resolved
          self
        end

        # Waits until every aggregated future completed and, when an
        # on_complete callback is registered, until that callback returned.
        #
        # Safe to call from inside a future callback: the waiting is delegated
        # to the futures' own #await, which keeps delivering completions on the
        # dispatcher thread instead of blocking it. Blocking directly on the
        # condition variable there would stall the whole process, since the
        # dispatcher thread is the one that would have to signal it.
        #
        # @return [Ilios::Cassandra::Future::All] self.
        def await
          @futures.each(&:await)
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

        # Counts down one future. Called from that future's on_complete, on
        # the dispatcher thread.
        #
        # @param error [Ilios::Cassandra::ExecutionError, nil] The failure, or nil when the future succeeded.
        # @return [void]
        def future_completed(error)
          @mutex.synchronize do
            @errors << error if error
            @remaining -= 1
            @cond.broadcast if @remaining.zero?
          end
          invoke_callback_if_resolved
        end

        # Runs the aggregate callback exactly once, outside the mutex so that
        # the callback may call back into this object, and wakes the awaiters
        # when it returned.
        #
        # @return [void]
        def invoke_callback_if_resolved
          callback = @mutex.synchronize do
            break nil if @callback.nil? || !@remaining.zero? || @callback_invoked

            @callback_invoked = true
            @callback_thread = Thread.current
            @callback
          end
          return unless callback

          begin
            # @errors is stable here: the countdown reached zero, so no future
            # is left to append to it.
            callback.call(@errors)
          ensure
            @mutex.synchronize do
              @callback_thread = nil
              @callback_finished = true
              @cond.broadcast
            end
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
