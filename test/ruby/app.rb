require 'connection_pool'
require 'faktory'
require 'securerandom'

class SomeWorker
  include Faktory::Job

  def perform(*args)
    puts "Hello, I am #{jid} with args #{args}"
    sleep 1
  end
end

class Failer
  include Faktory::Job

  def perform(a,b,c,d)
    puts "I am failing, #{jid}"
    raise "oops"
  end
end

# This is the simplest possible Faktory worker process:
# a single threaded loop, processing one job at a time and reporting
# any errors back to the server.
# Notably it doesn't attempt to handle any network errors so
# everything will quickly die if there's a problem.

# push a dummy job to Faktory for us to immediately process
$pool = ConnectionPool.new { Faktory::Client.new(url: "tcp://localhost:7419", debug: true) }
$pool.with do |faktory|
  puts faktory.push({ queue: :bulk, jobtype: 'Failer', jid: SecureRandom.hex(8), args:[1,2,3,"\r\n"] })
  puts faktory.push({ queue: :critical, jobtype: 'SomeWorker', jid: SecureRandom.hex(8), args:[8,2,3,"\r\n"] })
  puts faktory.push({ jobtype: 'SomeWorker', jid: SecureRandom.hex(8), args:[1,2,3,"\r\n"], at: (Time.now.utc + 3600).iso8601 })
end

def inker
  loop do
    $pool.with do |faktory|
      puts faktory.push({ queue: :critical, jobtype: 'someworker', jid: SecureRandom.hex(8), args:[26,2,3,"\r\n"] })
    end
    sleep(1 + rand)
  end
end

#inker = safe_spawn(&method(:inker))
