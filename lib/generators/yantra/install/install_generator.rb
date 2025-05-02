# lib/generators/yantra/install/install_generator.rb
require 'rails/generators/base'
require 'rails/generators/active_record' # Required for next_migration_number

module Yantra
  module Generators
    # Generator to install Yantra ActiveRecord migrations into a Rails application.
    class InstallGenerator < Rails::Generators::Base
      # Allow this generator to pull database-specific migration timestamps
      include Rails::Generators::Migration

      # Define where the generator should look for template files.
      source_root File.expand_path("templates", __dir__)

      # Implement the required method for ActiveRecord migration timestamping
      # Delegates to ActiveRecord's standard timestamp generator.
      def self.next_migration_number(dirname)
        ActiveRecord::Generators::Base.next_migration_number(dirname)
      end

      # The main method executed by the generator.
      def create_migration_files


        # Use migration_template to copy each migration file.
        # Rails handles timestamping and conflict checking (by class name).
        migration_template "create_yantra_workflows.rb",          "db/migrate/create_yantra_workflows.rb"
        migration_template "create_yantra_steps.rb",              "db/migrate/create_yantra_steps.rb"
        migration_template "create_yantra_step_dependencies.rb",  "db/migrate/create_yantra_step_dependencies.rb"
        migration_template "add_delay_to_yantra_steps.rb",        "db/migrate/add_delay_to_yantra_steps.rb"
        migration_template "add_performed_at_to_yantra_steps.rb", "db/migrate/add_performed_at_to_yantra_steps.rb"
      end
    end
  end
end
