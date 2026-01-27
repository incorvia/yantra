# lib/yantra/stats.rb

module Yantra
  class Stats
    # Calculates the failure rate for a given step class within a time window.
    #
    # @param klass_name [String] The class name of the step.
    # @param time_window [ActiveSupport::Duration] The time window to look back.
    # @return [Float] The failure rate (0.0 to 1.0).
    def self.failure_rate(klass_name, time_window = 1.hour)
      total = Yantra::Step.where(klass: klass_name)
                          .where('created_at > ?', Time.now - time_window)
                          .count

      return 0.0 if total == 0

      failed = Yantra::Step.where(klass: klass_name, state: 'failed')
                           .where('created_at > ?', Time.now - time_window)
                           .count

      failed.to_f / total
    end

    # Returns counts of steps by state for a given class within a time window.
    #
    # @param klass_name [String] The class name of the step.
    # @param time_window [ActiveSupport::Duration] The time window to look back.
    # @return [Hash] A hash mapping state to count.
    def self.count_by_state(klass_name, time_window = 1.hour)
      Yantra::Step.where(klass: klass_name)
                  .where('created_at > ?', Time.now - time_window)
                  .group(:state)
                  .count
    end
  end
end
