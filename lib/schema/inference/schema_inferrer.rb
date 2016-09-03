module Schema
  module Inference
    class SchemaInferrer
      class << self

        SEPARATOR = '|' # if this change, update the regex that use the character directly in this mixin

        # Generate a schema based on this collection's records.
        # We evaluate the schema of each record and then merge all
        # the information together.
        # @param data [Array] of samples on which we will
        #        perform the schema analysis.
        # @param extended [Boolean] Set to true to keep each field as a basic type.
        #        Set to false to reduce the terminal arrays to a single key (under the type array).
        # @return [Hash] with one entry per 'column'/'field'. The values
        #         contains information about the type and usage.
        def infer_schema(dataset: [], batch_count: 0, extended: false)
          # support detecting schemas of single objects
          dataset = [dataset] if dataset.is_a?(Hash)
          raise ArgumentError, 'dataset must be an array or a hash' unless dataset.is_a?(Array)

          has_dataset = dataset.count > 0 || (block_given? && batch_count > 0)
          raise ArgumentError, 'a dataset or a block with a batch count must be passed' unless has_dataset

          if dataset.count > 0
            # divide in batches to process in parallel
            count_per_process = (dataset.count / Parallel.processor_count.to_f).ceil
            batch_count = (dataset.count / count_per_process.to_f).ceil
          end

          results = parallel_map(batch_count.times) do |i|
            if block_given?
              batch = yield
            else
              batch = dataset[i*count_per_process...(i+1)*count_per_process]
            end
            { partial_schema: data_schema(batch), count: batch.count }
          end

          partial_schemas = results.map { |r| r[:partial_schema] }
          total_count = results.map { |r| r[:count] }.reduce(:+)

          table_schema = process_schema_results(partial_schemas, total_count, extended)
          table_schema.sort_by { |k, v| -v[:usage] }.to_h
        end

        private

        def data_schema(data)
          table_schema = {}
          data.each do |record|
            # fetch the record schema & update the general schema
            rec_schema = record_schema(record)
            rec_schema.each do |field_schema|
              table_schema[field_schema[:field]] ||= {type: field_schema[:type], usage_count: 0}
              if table_schema[field_schema[:field]][:type] != field_schema[:type]
                if table_schema[field_schema[:field]][:type] == NilClass
                  table_schema[field_schema[:field]][:type] = field_schema[:type]
                elsif field_schema[:type] != nil
                  table_schema[field_schema[:field]][:type] = lowest_common_type(field_schema[:type], table_schema[field_schema[:field]][:type])
                end
              end
              table_schema[field_schema[:field]][:usage_count] += 1
              table_schema[field_schema[:field]][:types] ||= {}
              table_schema[field_schema[:field]][:types][field_schema[:type]] ||= 0
              table_schema[field_schema[:field]][:types][field_schema[:type]] += 1
            end
          end

          table_schema
        end

        def process_schema_results(results, total_count, extended)
          # aggregate the results
          table_schema = results[0]
          results[1..-1].each { |res|
            table_schema.each { |k, v|
              next if res[k].blank?

              # aggregate types count
              res[k][:types].each { |type, count|
                table_schema[k][:types][type] ||= 0
                table_schema[k][:types][type] += count
              }

              # aggregate other informations
              table_schema[k][:usage_count] += res[k][:usage_count].to_i
              if (table_schema[k][:type] != res[k][:type])
                if table_schema[k][:type] == NilClass
                  table_schema[k][:type] = res[k][:type]
                elsif res[k][:type] != NilClass
                  table_schema[k][:type] = lowest_common_type(res[k][:type], table_schema[k][:type])
                end
              end
            }

            # make sure keys that were not in table_schema are now added.
            table_schema.reverse_merge!(res)
          }

          # detect and remove nulls that are part of other schemas
          # e.g. { 'some_data': null } and { 'some_data': { 'hash': 1 } }
          # shouldn't be reported as different keys
          table_schema.each { |k, v|
            next unless v[:type] == NilClass
            # check if there is any key that match this one plus an hash/array extension
            full_key_exists = table_schema.find {|full_key, _| full_key =~ /^#{k}\|.*/}.present?
            table_schema.delete(k) if full_key_exists
          }

          # detect and process array information
          unless extended
            terminal_array_keys = {}
            table_schema.keys.each { |key|
                is_terminal_array = /(?<key_prefix>.*)\|[0-9]+$/ =~ key
                next unless is_terminal_array
                terminal_array_keys[key_prefix] ||= []
                terminal_array_keys[key_prefix] << key
            }

            terminal_array_keys.each do |key_prefix, keys|
              keys_usage_count = keys.map{ |x| table_schema[x][:usage_count] }
              usage_count = keys_usage_count.max
              # min size = how many keys have "always" been used
              # As the keys may not have been used at the same time,
              # this may not be valid depending on the array usage.
              min_size = keys_usage_count.count { |x| x == usage_count }
              max_size = keys.map { |x| x.split(SEPARATOR)[-1].to_i }.max + 1

              # delete keys that are part of they array
              keys.each { |key, _| table_schema.delete(key) }

              table_schema[key_prefix] = {
                type: Array,
                usage_count: usage_count,
                min_size: min_size,
                max_size: max_size
              }
            end
          end

          # add a percentage in terms of usage
          table_schema.each { |k, v|
            table_schema[k][:usage] = table_schema[k][:usage_count] / total_count.to_f
          }

          table_schema
        end

        NumericTypes = [Numeric, Integer].freeze
        def lowest_common_type(type1, type2)
          return type1 if type1 == type2
          return Numeric if NumericTypes.include?(type1) && NumericTypes.include?(type2)
          Object
        end

        # Recursively explore a record and return its schema
        def record_schema(record, name = "")
          if record.is_a? Hash
            record.flat_map { |k, v|
              field_name = "#{name}#{SEPARATOR}#{k}" if name.present?
              field_name ||= k
              record_schema(v, field_name)
            }
          elsif record.is_a? Array
            record.each_with_index.flat_map { |x, index|
              field_name = "#{name}#{SEPARATOR}#{index}" if name.present?
              field_name ||= k
              record_schema(x, field_name)
            }
          else
            { field: name, type: detect_type_of(record) }
          end
        end

        def detect_type_of(value)
          return Boolean  if value.is_a?(TrueClass) || value.is_a?(FalseClass)
          return Integer  if value.is_a? Integer
          return Numeric  if value.is_a? Numeric
          return Time     if value.is_a? Time
          return NilClass if value.is_a? NilClass

          if value.is_a? String
            return Integer if value =~ /^[-+]?[0-9]+$/
            return Numeric if value =~ /^[-+]?[0-9]*\.?[0-9]+$/
            return Boolean if %w(false true).include?(value.downcase)
            return Time if Timeliness.parse(value) != nil
            return String
          end

          Object
        end

        def key_access_tokens(key:)
          key.split(SEPARATOR).map { |token|
            # only parse integers for array indexing
            next token unless is_integer?(token)
            token.to_i
          }
        end

        def record_value(record:, key:)
          tokens = key_access_tokens(key: key)
          record.dig(*tokens)
        end

        def add_value_to_record(record:, key:, value:)
          tokens = key.split(SEPARATOR)
          current_ref = record
          previous_token = tokens[0]

          tokens[1..-1].each_with_index { |token|
            if is_integer?(token)
              current_ref[previous_token] ||= []
              current_ref = current_ref[previous_token]
              previous_token = token.to_i
            else
              current_ref[previous_token] ||= {}
              current_ref = current_ref[previous_token]
              previous_token = token
            end
          }

          current_ref[previous_token] = value
        end

        def is_integer?(value)
          (/^[+-]?[0-9]+$/ =~ value).present?
        end

        def parallel_map(itr, &block)
          # set to true to debug code in the iteration
          is_debugging_impl = ENV['DEBUG']
          if is_debugging_impl
            itr.map do |arg|
              block.call(arg)
            end
          else
            Parallel.map(itr) do |arg|
              block.call(arg)
            end
          end
        end

      end

    end
  end
end
