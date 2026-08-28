# frozen_string_literal: true

module Ilios
  module Cassandra
    # Native asynchronous future returned by Session#prepare_async and
    # Session#execute_async. This file adds the Ruby-level aggregation API on
    # top of the C implementation.
    class Future
      # Aggregate over multiple futures that resolves once all of them
      # completed. Created via Ilios::Cassandra::Future.all, which registers
      # +on_complete+ on every future, so the futures must not carry any
      # callback of their own.
      class All
        # @param futures [Array[Ilios::Cassandra::Future]] The futures to aggregate.
        def initialize(futures)
          futures = futures.to_a
          @mutex = Mutex.new
          @cond = ConditionVariable.new
          @remaining = futures.size
          @errors = []
          @callback = nil
          @callback_invoked = false
          @callback_finished = false
          futures.each do |future|
            future.on_complete { |_value, error| future_completed(error) }
          end
        end

        # Runs block once every aggregated future completed. The block
        # receives the failures: an empty array when every future succeeded,
        # otherwise the ExecutionError of each failed future. Runs
        # immediately when everything already completed.
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
        # @return [Ilios::Cassandra::Future::All] self.
        def await
          @mutex.synchronize do
            @cond.wait(@mutex) until @remaining.zero? && (@callback.nil? || @callback_finished)
          end
          self
        end

        private

        # Called from each future's on_complete, on the callback-delivery
        # thread.
        def future_completed(error)
          @mutex.synchronize do
            @errors << error if error
            @remaining -= 1
            @cond.broadcast if @remaining.zero?
          end
          invoke_callback_if_resolved
        end

        # Runs the aggregate callback exactly once, outside the mutex, and
        # wakes the awaiters when it returned.
        def invoke_callback_if_resolved
          callback = @mutex.synchronize do
            break nil if @callback.nil? || !@remaining.zero? || @callback_invoked

            @callback_invoked = true
            @callback
          end
          return unless callback

          begin
            callback.call(@errors)
          ensure
            @mutex.synchronize do
              @callback_finished = true
              @cond.broadcast
            end
          end
        end
      end

      # Returns a Future::All that resolves once all given futures completed.
      # Registers +on_complete+ on every future, so they must not carry any
      # callback of their own.
      #
      # @param futures [Array[Ilios::Cassandra::Future]] The futures to aggregate.
      # @return [Ilios::Cassandra::Future::All] The aggregate future.
      # @raise [Ilios::Cassandra::ExecutionError] If a future already has a callback.
      def self.all(futures)
        All.new(futures)
      end
    end
  end
end
