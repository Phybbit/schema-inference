# Schema::Inference

Supports inferring tabular schemas from deep nested data structures.
There 2 main uses for this gem:
- gives schema information on a nested data structure (useful when converting to a tabular format)
- recover types from data that has been serialized to string (e.g. JSON or CSV)

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'schema-inference'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install schema-inference

## Usage

1. Report information on nested data structure

```
schema = Schema::Inference.schema(dataset: [
  {
    'person' => {
      'name' => 'Bob',
      'age' => 30,
      'weight' => 60
    },
    'updated_at' => '2016-01-01T00:00:00Z'
  },
  {
    'person' => {
      'name' => 'Alice',
      # Alice does not want to show her age
      'weight' => 50.5
    },
    'updated_at' => '2016-01-01T00:00:00Z'
  },
])

schema['person.name'][:type]  # String
schema['person.name'][:usage] # 1.0 (100% of the entries have a name)

schema['person.age'][:type]  # Integer
schema['person.age'][:usage] # 0.5 (50% of the entries have an age)

schema['person.weight'][:type] # Numeric (inferred to be numeric, even though an integer was present)
schema['updated_at'][:type]    # Time
```

2. Recover types from string serialization

```
schema = Schema::Inference.schema(dataset: {
  'serialized_time' => '2016-01-01T00:00:00Z',
  'serialized_integer' => '100',
  'serialized_numeric' => '0.5',
  'serialized_boolean' => 'true',
})
schema['serialized_time'][:type]    # Time
schema['serialized_integer'][:type] # Integer
schema['serialized_numeric'][:type] # Numeric
schema['serialized_boolean'][:type] # Boolean
```

3. If you need to load a lot of data consider using the following pattern:
```
schema = Schema::Inference.schema(batch_count: 10) do |idx\
 # Pull and return some large amount of data.
 # Fetching/accessing the data here would avoid the IPC cost of
 # sending the data to the child process for parallel processing.
 # e.g.:
 MongoClient.find.limit(1000).offset(1000 * idx)
end
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. In certain cases, you maybe want to use the debug flag e.g. `DEBUG=true rake spec` to disable parallel schema processing. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/phybbit/schema-inference. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
