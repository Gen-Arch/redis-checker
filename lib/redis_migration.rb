require "redis"
require "tomlrb"
require "logger"
require "colorize"

class RedisMigration
  UPDATE_COMMANDS       = /set|setex|getset/i
  UPDATE_MSEC_COMMANDS  = /psetex/i
  DELETE_COMMANDS       = /del/i

  def initialize(file)
    file        ||= 'redis.toml'
    @config     = Tomlrb.load_file(file, symbolize_keys: true)
    @loggers    = get_logger
    @redis      = con
  end

  def watch(count=100)
    key   = "test"
    value = "test"
    ttl   = rand(99999)

    count.times do |num|
      begin
        @redis[:src].setex(key, ttl, value)
        res = @redis[:src].get(key)

        @loggers.each do |logger|
          logger.info("#{num} : #{res}")
        end
        sleep 1
      rescue Redis::CannotConnectError => err
        recon
        next
      end
    end
  end


  def monitor
    @redis[:src].monitor do |data|
      data = parse(data)

      data.delete(:value) unless ENV["varbose"] && data[:value]

      @loggers.each do |logger|
        logger.info(data)
      end
    end
  end

  def add_test_data
    key   = SecureRandom.alphanumeric
    value = Array.new
    rand(100).times do
      value << SecureRandom.base64(1000)
    end

    @redis[:src].setex(key, rand(99999), value.join(" "))
    pp @redis[:src].get(key)
  end

  private
  def con
    types = [:src]
    redis = Hash.new
    types.each do |type|
      redis[type] = Redis.new(**@config[type])
    end

    redis
  end

  def recon
    begin
      sleep 1
      puts "reconnect..."
      @redis[:src] = Redis.new(**@config[:src])
    rescue => e
      retry
    end
  end

  def values(type)
    data = Hash.new
    keys(type).each do |key|
      data[key] = @redis[type].get(key)
    end

    data
  end

  def parse(data)
    data.force_encoding('UTF-8')
    data = data.encode("UTF-16BE", "UTF-8", :invalid => :replace, :undef => :replace, :replace => '?').encode("UTF-8")
    data = data.split("\s")

    if data.size == 1
      {type: data}
    elsif data.size == 4
      {
        id:     data[0],
        src:    "#{data[1]} #{data[2]}",
        type:   data[3].gsub(/^\"|\"$/, ""),
      }
    elsif data.size == 5
      {
        id:     data[0],
        src:    "#{data[1]} #{data[2]}",
        type:   data[3].gsub(/^\"|\"$/, ""),
        key:    data[4].gsub(/^\"|\"$/, ""),
      }
    elsif data.size >= 6
      {
        id:     data[0],
        src:    "#{data[1]} #{data[2]}",
        type:   data[3].gsub(/^\"|\"$/, ""),
        key:    data[4].gsub(/^\"|\"$/, ""),
        value:  data[5...-1].join(" ").gsub(/^\"|\"$/, "")
      }
    else
      Hash.new
    end
  end

  def get_logger
    loggers = Array.new
    loggers << Logger.new(STDOUT,     datetime_format: '%Y-%m-%d %H:%M:%S')
    loggers << Logger.new(ENV["log"], datetime_format: '%Y-%m-%d %H:%M:%S') if ENV["log"]

    loggers
  end
end
