require "logger"
require 'securerandom'

require "./lib/redis_migration.rb"

CONFIG    = ENV["file"]
@redismig = RedisMigration.new(CONFIG)

desc "watch"
task :watch do
  @redismig.watch
end
