# frozen_string_literal: true

require_relative "lib/yantra/version"

Gem::Specification.new do |spec|
  spec.name = "yantra"
  spec.version = Yantra::VERSION
  spec.authors = ["Kevin Incorvia"]
  spec.email = ["kincorvia@suvie.com"]

  spec.summary = "A robust, DAG-based workflow orchestration engine for Ruby with pluggable persistence."
  spec.description = "Yantra provides a powerful and flexible framework for defining, executing, and monitoring complex workflows composed of dependent jobs (as Directed Acyclic Graphs - DAGs) in Ruby applications.

Key features include:

* **Reliable Execution:** Orchestrates job execution via common background worker systems (Sidekiq, Resque, Active Job adapters planned).
* **Pluggable Persistence:** Utilizes a Repository pattern, allowing you to choose your persistence backend (e.g., Redis, PostgreSQL) without altering core workflow logic.
* **Core Observability:** Built-in lifecycle event emission (`workflow.started`, `job.failed`, etc.) enables easy integration with monitoring, logging, and alerting systems.
* **Clear State Management:** Explicit state machines for both workflows and jobs ensure predictable execution and status tracking.
* **Simplified API:** Clean interface using unique IDs (UUIDs) for unambiguous referencing of workflows and jobs.
* **Modern & Maintainable:** Designed with testability, extensibility, and modern Ruby practices in mind.

Yantra aims to be a robust, developer-friendly solution for managing background job dependencies and complex process flows."
  spec.homepage = "https://www.suvie.com"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 2.6.0"

  spec.metadata["allowed_push_host"] = "TODO: Set to your gem server 'https://example.com'"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(__dir__) do
    `git ls-files -z`.split("\x0").reject do |f|
      (File.expand_path(f) == __FILE__) || f.start_with?(*%w[bin/ test/ spec/ features/ .git .circleci appveyor])
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  # Uncomment to register a new dependency of your gem
  # spec.add_dependency "example-gem", "~> 1.0"

  # For more information and examples about making a new gem, check out our
  # guide at: https://bundler.io/guides/creating_gem.html

  spec.add_development_dependency 'activerecord', '~> 7.1'
  spec.add_development_dependency "rails", ">= 6.0"
  spec.add_development_dependency "sqlite3", ">= 2.1"     # Or compatible version
  spec.add_development_dependency "minitest", "~> 5.0" 
  spec.add_development_dependency "minitest-focus"
  spec.add_development_dependency "database_cleaner-active_record"
  spec.add_development_dependency "mocha"
  spec.add_development_dependency "pry"
  spec.add_development_dependency "pg"
  spec.add_development_dependency "sidekiq"

  # spec.add_development_dependency "database_cleaner-active_record" # If using DatabaseCleaner
end
