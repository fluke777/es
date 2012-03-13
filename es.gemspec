# Ensure we require the local version and not one we might have installed already
require File.join([File.dirname(__FILE__),'lib','es_version.rb'])
spec = Gem::Specification.new do |s| 
  s.name = 'es'
  s.version = Es::VERSION
  s.author = 'Your Name Here'
  s.email = 'your@email.address.com'
  s.homepage = 'http://your.website.com'
  s.platform = Gem::Platform::RUBY
  s.summary = 'A description of your project'
# Add your other files here if you make them
  s.files = ['bin/es'] + Dir['lib/**/*.*']
  s.require_paths << 'lib'
  s.has_rdoc = true
  s.extra_rdoc_files = ['README.rdoc','es.rdoc']
  s.rdoc_options << '--title' << 'es' << '--main' << 'README.rdoc' << '-ri'
  s.bindir = 'bin'
  s.executables << 'es'
  s.add_development_dependency('rake')
  s.add_development_dependency('rdoc')
  s.add_dependency('gli')
  s.add_dependency('gooddata', '>= 0.5.3')
  s.add_dependency('jsonify')
  s.add_dependency('chronic')
  s.add_dependency('rainbow')
  s.add_dependency('kwalify')
  s.add_dependency('pry')
  s.add_dependency('activesupport')
  s.add_dependency('i18n')
  s.add_dependency('terminal-table')
  s.add_dependency('fastercsv')
end
