# Rakefile (example for Minitest)
require "bundler/gem_tasks"
require "rake/testtask"

Rake::TestTask.new(:test) do |t|
  t.libs << "test"
  t.libs << "lib"
  t.test_files = FileList["test/**/*_test.rb"] # This pattern finds the files
end

task default: :test # Makes `rake` run the tests
