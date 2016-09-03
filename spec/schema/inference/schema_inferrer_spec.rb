require 'spec_helper'

describe Schema::Inference::SchemaInferrer do

  describe '#schema' do
    let (:inferrer) { Schema::Inference::SchemaInferrer.new }

    it 'recognizes numerics' do
      dataset = [
        { 'numeric' => 1 },
        { 'numeric' => 1.5 },
        { 'numeric' => '1' },
        { 'numeric' => '1.5' },
      ]
      expect(inferrer.infer_schema(dataset: dataset)['numeric'][:type]).to eq Numeric
    end

    it 'recognizes integers' do
      dataset = [
        { 'integer' => 1 },
        { 'integer' => '1' },
      ]
      expect(inferrer.infer_schema(dataset: dataset)['integer'][:type]).to eq Integer
    end

    it 'recognizes booleans' do
      dataset = [
        { 'bool' => true },
        { 'bool' => false },
        { 'bool' => 'true' },
        { 'bool' => 'false' },
        { 'bool' => 'TRUE' },
        { 'bool' => 'FALSE' },
      ]
      expect(inferrer.infer_schema(dataset: dataset)['bool'][:type]).to eq Boolean
    end

    it 'recognizes strings' do
      dataset = [
        { 'string' => 'a string' },
        { 'string' => 'and another string' },
      ]
      expect(inferrer.infer_schema(dataset: dataset)['string'][:type]).to eq String
    end

    it 'recognizes fields with multiple types as Object' do
      dataset = [
        { 'multiple' => 1 },
        { 'multiple' => 'was an int but became a string' },
      ]
      expect(inferrer.infer_schema(dataset: dataset)['multiple'][:type]).to eq Object
    end

    it 'recognizes (terminal) arrays' do
      dataset = [
        { 'array' => [0, 1, nil, 2] },
        { 'array' => [4] },
      ]
      expect(inferrer.infer_schema(dataset: dataset)['array'][:type]).to eq Array
      expect(inferrer.infer_schema(dataset: dataset)['array'][:min_size]).to eq 1
      expect(inferrer.infer_schema(dataset: dataset)['array'][:max_size]).to eq 4
    end

    it 'recognizes (terminal) arrays content (extended)' do
      dataset = [
        { 'array' => [1] },
      ]
      expect(inferrer.infer_schema(dataset: dataset, extended: true)['array.0'][:type]).to eq Integer
    end

    it 'recognizes fields in hashes' do
      dataset = [
        { 'in' => { 'hash' => 'some string' } }
      ]
      expect(inferrer.infer_schema(dataset: dataset)['in.hash'][:type]).to eq String
    end

    it 'recognizes fields in arrays' do
      dataset = [
        { 'array' => [{ 'deep' => 'structure' }] }
      ]
      expect(inferrer.infer_schema(dataset: dataset)['array.0.deep'][:type]).to eq String
    end

    it 'recognizes nils' do
      dataset = [
        { 'with_nulls' => nil },
      ]
      expect(inferrer.infer_schema(dataset: dataset)['with_nulls'][:type]).to eq NilClass
    end

    it 'removes null keys that have full keys with other objects' do
      dataset = [
        { 'with_nils' => nil },
        { 'with_nils' => [4] },
      ]
      expect(inferrer.infer_schema(dataset: dataset)['with_nils'][:type]).to eq Array
    end

    it 'supports a single document/hash' do
      data = { 'numeric' => 1.5 }
      expect(inferrer.infer_schema(dataset: data)['numeric'][:type]).to eq Numeric
    end

    it 'supports streaming' do
      datasets = [
        [{'numeric' => 1}],
        [{'numeric' => 1.5}]
      ]

      schema = inferrer.infer_schema(batch_count: 2) do |idx|
        # In a real use case with a lot of data, fetching/accessing the data
        # here would avoid the IPC cost of sending the data to the child process.
        datasets[idx]
      end

      expect(schema['numeric'][:type]).to eq Numeric
    end
  end
end
