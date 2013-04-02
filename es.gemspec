# Ensure we require the local version and not one we might have installed already
$:.push File.expand_path("../lib", __FILE__)
require "es_version"

spec = Gem::Specification.new do |s| 
  s.name = 'gd_es'
  s.version = Es::VERSION
  s.author = 'Tomas Svarovsky'
  s.email = 'svarovsky.tomas@gmail.com'
  s.homepage = 'https://github.com/fluke777/es'
  s.platform = Gem::Platform::RUBY
  s.summary = 'Project which simplifies interaction with GoodData Eventstore storage'
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
  s.add_dependency('gooddata', '>= 0.5.4')
  s.add_dependency('jsonify')
  s.add_dependency('chronic')
  s.add_dependency('rainbow')
  s.add_dependency('pry')
  s.add_dependency('activesupport')
  s.add_dependency('i18n')
  s.add_dependency('terminal-table')
  s.add_dependency('fastercsv')
  s.add_dependency('yajl-ruby')
end
