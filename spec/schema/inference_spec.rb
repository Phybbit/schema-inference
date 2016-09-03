require 'spec_helper'

describe Schema::Inference do
  it 'has a version number' do
    expect(Schema::Inference::VERSION).not_to be nil
  end

  describe '#schema' do
    let (:ns) { Schema::Inference }

    it 'support a default interface' do
      dataset = [
        { 'numeric' => 1 },
        { 'hash' => { 'value' => 'string' }}
      ]
      expect(ns.schema(dataset: dataset)['numeric'][:type]).to eq Integer
      expect(ns.schema(dataset: dataset)['hash.value'][:type]).to eq String
    end
  end
end
