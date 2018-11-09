require 'erb'
require 'shellwords'

# GitHub release notes auto-generator
# Use like `ruby notes.rb 0.9.0`
raise "invalid arguments" unless ARGV.size == 1
bigver = ARGV[0]
shortver = bigver.gsub(/[^0-9]/, "")
title = bigver
sums = {}

Dir["packaging/output/systemd/*"].each do |fullname|
  name = File.basename(fullname)

  output = `shasum -p -a 256 #{Shellwords.escape(fullname)}`
  if $?.exitstatus != 0
    raise output
  end
  sums[name] = output.split[0]
end

content = ERB.new(File.read("#{__dir__}/release-notes.md.erb"), nil, '-').result(binding)
File.open("/tmp/release-notes.md", "w") do |file|
  file.write(content)
end
