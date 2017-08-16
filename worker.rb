require 'faktory'
require 'securerandom'

# This is the simplest possible Faktory worker process:
# a single threaded loop, processing one job at a time and reporting
# any errors back to the server.

def execute(job)
  jid = job["jid"]
  type = job["jobtype"]
  case type
  when "someworker"
    someworker(jid, *job['args'])
  else
    puts "Unknown job type #{type}"
  end
end

def someworker(jid, *args)
  puts "Executing #{jid} #{args}"
  sleep 1
end

# push a dummy job to Faktory for us to immediately process
faktory = Faktory::Client.new
faktory.push({ jobtype: 'someworker', jid: SecureRandom.hex(8), args:[1,2,3,"\r\n"] })

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
    sleep 1
  end
end
