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

#ENV["FAKTORY_PROVIDER"] = "FAKTORY_URL"
#ENV["FAKTORY_URL"] = "tcp://localhost.contribsys.com:7419"

# push a dummy job to Faktory for us to immediately process
$pool = ConnectionPool.new { Faktory::Client.new(debug: true) }
$pool.with do |faktory|
  puts faktory.push({ queue: :bulk, jobtype: 'Failer', jid: SecureRandom.hex(8), args:[1,2,3,"\r\n"], 'retry': 5 })
  puts faktory.push({ queue: :critical, jobtype: 'SomeWorker', jid: SecureRandom.hex(8), args:[8,2,3,"\r\n"] })
  puts faktory.push({ jobtype: 'SomeWorker', jid: SecureRandom.hex(8), args:[1,2,3,"\r\n"], at: (Time.now.utc + 3600).iso8601 })
end

def enqueuer
  loop do
    $pool.with do |faktory|
      puts faktory.push({ queue: :critical, jobtype: 'someworker', jid: SecureRandom.hex(8), args:[26,2,3,"\r\n"] })
    end
    sleep(1 + rand)
  end
end

#inker = safe_spawn(&method(:enqueuer))
