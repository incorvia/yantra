# lib/yantra/reporting.rb

module Yantra
  module Reporting
    # Compiles a report for a given subject (Workflow or Step).
    # Merges the subject's reporting_data with data from the configured reporting_adapter.
    #
    # @param subject [Yantra::Workflow, Yantra::Step] The subject to report on.
    # @return [Hash] The compiled report data.
    def self.compile_report(subject)
      base_data = subject.reporting_data || {}

      adapter_data = if Yantra.configuration.reporting_adapter.respond_to?(:call)
        Yantra.configuration.reporting_adapter.call(subject)
      else
        {}
      end

      # Ensure adapter_data is a Hash
      adapter_data = {} unless adapter_data.is_a?(Hash)

      base_data.merge(adapter_data)
    end
  end
end
