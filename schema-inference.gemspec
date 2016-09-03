# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'schema/inference/version'

Gem::Specification.new do |spec|
  spec.name          = 'schema-inference'
  spec.version       = Schema::Inference::VERSION
  spec.authors       = ['Eurico Doirado']
  spec.email         = ['eurico@phybbit.com']

  spec.summary       = %q{Supports inferring tabular schemas from deep nested structures.}
  spec.homepage      = 'https://github.com/Phybbit/schema-inference'
  spec.license       = 'MIT'

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bundler', '~> 1.12'
  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'rspec', '~> 3.0'
  spec.add_development_dependency 'pry-byebug'

  spec.add_dependency 'activesupport', '>= 4.0.0'
  spec.add_dependency 'parallel', '~>1.8'
  spec.add_dependency 'timeliness', '~>0.3'
end
