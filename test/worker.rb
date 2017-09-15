require 'faktory'
require 'securerandom'

=begin
 ruby -I~/src/faktory-ruby/lib worker.rb
=end

# This is the simplest possible Faktory worker process:
# a single threaded loop, processing one job at a time and reporting
# any errors back to the server.
# Notably it doesn't attempt to handle any network errors so
# everything will quickly die if there's a problem.

def execute(job)
  jid = job["jid"]
  type = job["jobtype"]
  case type
  when "failer"
    failer(jid, *job['args'])
  when "someworker"
    someworker(jid, *job['args'])
  else
    puts "Unknown job type #{type}"
  end
end

def failer(jid, *args)
  puts "I am failing, #{jid}"
  raise "oops"
end

def someworker(jid, *args)
  puts "Hello, I am #{jid} with args #{args}"
  sleep 1
end

# push a dummy job to Faktory for us to immediately process
$pool = ConnectionPool.new { Faktory::Client.new(url: "tcp://localhost:7419", debug: true) }
$pool.with do |faktory|
  puts faktory.push({ queue: :bulk, jobtype: 'failer', jid: SecureRandom.hex(8), args:[1,2,3,"\r\n"] })
  puts faktory.push({ queue: :critical, jobtype: 'someworker', jid: SecureRandom.hex(8), args:[8,2,3,"\r\n"] })
  puts faktory.push({ jobtype: 'someworker', jid: SecureRandom.hex(8), args:[1,2,3,"\r\n"], at: (Time.now.utc + 3600).iso8601 })
end

$done = false

%w(INT TERM).each do |sig|
  trap sig do
    puts "Shutting down!"
    $done = true
  end
end

def heartbeat
  loop do
    signal = $pool.with {|f| f.beat }

    if signal == "quiet"
      puts "No more jobs for me"
    elsif signal == "terminate"
      puts "Shutting down"
      $done = true
      return
    end
    sleep 5
  end
end

def safe_spawn
  Thread.new do
    begin
      yield
    rescue
      puts $!
      p $!.backtrace
    end
  end
end

def inker
  loop do
    $pool.with do |faktory|
      puts faktory.push({ queue: :critical, jobtype: 'someworker', jid: SecureRandom.hex(8), args:[26,2,3,"\r\n"] })
    end
    sleep(1 + rand)
  end
end

beater = safe_spawn(&method(:heartbeat))
inker = safe_spawn(&method(:inker))

while !$done
  job = $pool.with {|f| f.fetch(:critical, :default, :bulk) }
  if job
    jid = job["jid"]
    begin
      execute(job)
      $pool.with {|f| f.ack(jid) }
    rescue => ex
      $pool.with {|f| f.fail(jid, ex) }
    end
  else
    sleep 1
  end
end
