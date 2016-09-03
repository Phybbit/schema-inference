require 'active_support'
require 'active_support/core_ext/object/blank'
require 'active_support/core_ext/hash/reverse_merge'

require 'parallel'
require 'timeliness'

require 'extensions/boolean'
require 'schema/inference/version'
require 'schema/inference/schema_inferrer'


module Schema
  module Inference

    def Inference.schema(*args)
      SchemaInferrer.new.infer_schema(*args)
    end

  end
end
