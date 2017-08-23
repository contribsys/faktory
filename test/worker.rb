require 'faktory'
require 'securerandom'

=begin
 ruby -I~/src/faktory-ruby/lib worker.rb
=end

# This is the simplest possible Faktory worker process:
# a single threaded loop, processing one job at a time and reporting
# any errors back to the server.

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
faktory = Faktory::Client.new
puts faktory.push({ jobtype: 'failer', jid: SecureRandom.hex(8), args:[1,2,3,"\r\n"] })
puts faktory.push({ jobtype: 'someworker', jid: SecureRandom.hex(8), args:[1,2,3,"\r\n"] })
puts faktory.push({ jobtype: 'someworker', jid: SecureRandom.hex(8), args:[1,2,3,"\r\n"], at: (Time.now.utc + 3600).iso8601 })

$done = false

%w(INT TERM).each do |sig|
  trap sig do
    puts "Shutting down!"
    $done = true
  end
end

while !$done
  job = faktory.pop("default")
  if job
    jid = job["jid"]
    begin
      execute(job)
      faktory.ack(jid)
    rescue => ex
      faktory.fail(jid, ex)
    end
  else
    puts "Nothing"
    sleep 1
  end
end

def heartbeat
  loop do
    faktory.beat
    sleep 5
  end
end
