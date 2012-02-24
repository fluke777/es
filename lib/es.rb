require 'pry'
require 'chronic'
require 'jsonify'
require 'json'
require 'rainbow'
require 'kwalify'
require 'active_support/time'

module Es

  class InsufficientSpecificationError < RuntimeError
  end

  class IncorrectSpecificationError < RuntimeError
  end
  
  class UnableToMerge < RuntimeError
  end

  class Timeframe
    INTERVAL_UNITS = [:day, :week, :month, :year]
    DAY_WITHIN_PERIOD = [:first, :last]
    attr_accessor :to, :from, :interval_unit, :interval, :day_within_period

    def self.parse(spec)
      if spec == 'latest' then
        Timeframe.new({
          :to => 'today',
          :from => 'yesterday'
        })
      else
        Timeframe.new(spec)
      end
    end

    def initialize(spec)
      validate_spec(spec)
      @spec = spec
      @to = Chronic.parse(spec[:to])
      @from = spec[:from] ? Chronic.parse(spec[:from]) : to.advance(:days => -1)
      @interval_unit = spec[:interval_unit] || :day
      @interval = spec[:interval] || 1
      @day_within_period = spec[:day_within_period] || :last
    end

    def validate_spec(spec)
      fail IncorrectSpecificationError.new("Timeframe should have a specification") if spec.nil?
      fail InsufficientSpecificationError.new("To key was not specified during the Timeframe creation") unless spec.has_key?(:to)
      fail InsufficientSpecificationError.new("From key was not specified during the Timeframe creation") unless spec.has_key?(:from)
      fail IncorrectSpecificationError.new("Interval key should be a number") if spec[:interval] && !spec[:interval].is_a?(Fixnum)
      fail IncorrectSpecificationError.new("Interval_unit key should be one of :day, :week, :month, :year") if spec[:interval_unit] && !INTERVAL_UNITS.include?(spec[:interval_unit])
      fail IncorrectSpecificationError.new("Interval_unit key should be one of :day, :week, :month, :year") if spec[:day_within_period] && !DAY_WITHIN_PERIOD.include?(spec[:day_within_period])
    end

    def to_extract_fragment(pid, options = {})
      {
        :endDate            => to.strftime('%Y-%m-%d'),
        :startDate          => from.strftime('%Y-%m-%d'),
        :intervalUnit       => interval_unit,
        :dayWithinPeriod    => day_within_period.to_s.upcase,
        :interval           => interval
      }
    end

  end

  class Extract

    attr_accessor :entities, :timeframe, :timezone

    def self.parse(spec, a_load)
      global_timeframe = parse_timeframes(spec[:timeframes]) || parse_timeframes("latest")
      
      parsed_entities = spec[:entities].map do |entity_spec|
        entity_name = entity_spec[:entity]
        load_entity = a_load.get_merged_entity_for(entity_name)
        fields = entity_spec[:fields].map do |field|
          if load_entity.has_field?(field)
            load_entity.get_field(field)
          elsif field == "snapshot"
            Es::SnapshotField.new("snapshot", "snapshot")
          elsif field == "autoincrement"
            Es::AutoincrementField.new("generate", "autoincrement")
          elsif field.respond_to?(:keys) && field.keys.first == :hid
            Es::HIDField.new('hid', "historicid", {
              :entity => field[:hid][:from_entity],
              :fields => field[:hid][:from_fields],
              :through => field[:hid][:connected_through]
            })
          else
            fail InsufficientSpecificationError.new("The field #{field.to_s.bright} was not found in either the loading specification nor was recognized as a special column")
          end
        end
        parsed_timeframe = parse_timeframes(entity_spec[:timeframes])
        Entity.new(entity_name, {
          :fields => fields,
          :file   => entity_spec[:file],
          :timeframe => parsed_timeframe || global_timeframe || (raise "Timeframe has to be defined")
        })
      end

      Extract.new(parsed_entities)
    end

    def self.parse_timeframes(timeframe_spec)
      return nil if timeframe_spec.nil?
      return Timeframe.parse("latest") if timeframe_spec == "latest"
      if timeframe_spec.is_a?(Array) then
        timeframe_spec.map {|t_spec| Es::Timeframe.parse(t_spec)}
      else
        Es::Timeframe.parse(timeframe_spec)
      end
    end

    def initialize(entities, options = {})
      @entities = entities
      @timeframe = options[:timeframe]
      @timezone = options[:timezone] || 'UTC'
    end

    def get_entity(name)
      entities.detect {|e| e.name == name}
    end

    def to_extract_fragment(pid, options = {})
      entities.map do |entity|
        entity.to_extract_fragment(pid, options)
      end
    end

  end

  class Load
    attr_accessor :entities

    def self.parse(spec)
      
      begin
      Load.new(spec.map do |entity_spec|
          Entity.parse(entity_spec)
      end)
      rescue Es::IncorrectSpecificationError => e
        puts "Seems like there are multiple definitions for the same entity and there is a column which has same name".color(:red)
        fail
      end
      
    end

    def initialize(entities)
      @entities = entities
      validate
    end

    def get_merged_entity_for(name)
      entities_to_merge = entities.find_all {|e| e.name == name}
      fail UnableToMerge.new("There is no entity #{name.bright} in current load object.") if entities_to_merge.empty?
      merged_fields = entities_to_merge.inject([]) {|all, e| all.concat e.fields}
      Entity.new(name, {
        :file => "MERGED",
        :fields => merged_fields
      })
    end

    def validate
      names = entities.map {|e| e.name}.uniq
      names.each do |name|
        merged_entity = get_merged_entity_for(name)
      end
    end

  end

  class Entity
    attr_accessor :name, :fields, :file, :timeframes

    def self.parse(spec)
      begin
        entity = Entity.new(spec[:entity], {
          :file => spec[:file],
          :fields => spec[:fields] && spec[:fields].map {|field_spec| Field.parse(field_spec)}
        })
      rescue Es::IncorrectSpecificationError => e
        puts "Error during parsing entity #{spec[:entity]}".color(:red)
        puts e.message
      end
    end

    def initialize(name, options)
      raise Es::IncorrectSpecificationError.new("Entity name is not specified.") if name.nil?
      raise Es::IncorrectSpecificationError.new("Entity name should be a string.") unless name.is_a?(String)
      raise Es::IncorrectSpecificationError.new("Entity name should not be empty.") if name.strip.empty?
      raise Es::IncorrectSpecificationError.new("File is not specified.") if options[:file].nil?
      raise Es::IncorrectSpecificationError.new("File should be a string.") unless options[:file].is_a?(String)
      raise Es::IncorrectSpecificationError.new("Fields are not specified.") if options[:fields].nil?
      raise Es::IncorrectSpecificationError.new("Entity should contain at least one field.") if options[:fields].empty?
      @name = name
      @fields = options[:fields]
      @file = options[:file]
      if options[:timeframe] && !options[:timeframe].is_a?(Array)
        @timeframes = [options[:timeframe]]
      else
        @timeframes = options[:timeframe]
      end
      raise Es::IncorrectSpecificationError.new("Entity #{name} should not contain multiple fields with the same name.") if has_multiple_same_fields?
    end

    def has_multiple_same_fields?
      fields.uniq_by {|s| s.name}.count != fields.count
    end

    def to_extract_fragment(pid, options = {})
      pretty = options[:pretty].nil? ? true : options[:pretty]
      read_map = [{
        :file       => Es::Helpers.load_destination_dir(pid, self) + '/' + Es::Helpers.load_destination_file(self),
        :populates  => (fields.find {|f| f.is_hid?} || fields.find {|f| f.type == "recordid"}).name,
        :columns    => (fields.map do |field|
          field.to_extract_fragment(pid, options)
        end)
      }]
      
      {
        :readTask => {
          :entity => name,
          :timeFrames => (timeframes.map{|t| t.to_extract_fragment(pid, options)}),
          :readMap => (pretty ? read_map : read_map.to_json),
          :timezone => 'UTC',
          :computedStreams => '[{"type":"computed","ops":[]}]'
        }
      }
    end

    def to_load_fragment(pid)
      {
        :uploadTask => {
          :entity       => name,
          :file         => Es::Helpers.load_destination_dir(pid, self) + '/' + Es::Helpers.load_destination_file(self),
          :attributes   => fields.map {|f| f.to_load_fragment(pid)}
        }
      }
    end

    def has_field?(name)
      !!fields.detect {|f| f.name == name}
    end

    def get_field(name)
      fields.detect {|f| f.name == name}
    end
    

  end

# Fields

  class Field

    FIELD_TYPES = ["attribute", "recordid", "timeAttribute", "fact", "timestamp", "autoincrement", "snapshot", "hid", "historicid"]

    def self.parse(spec)
      raise InsufficientSpecificationError.new("Field specification is empty") if spec.nil?
      raise InsufficientSpecificationError.new("Field specification is should be an object") unless spec.is_a?(Hash)
      Field.new(spec[:name], spec[:type])
    end

    attr_accessor :type, :name

    def is_snapshot?
      false
    end

    def is_autoincrement?
      false
    end

    def is_hid?
      false
    end

    def initialize(name, type)
      raise Es::IncorrectSpecificationError.new("The field name \"#{name.bright}\" does not have type specified. Type should be one of [#{FIELD_TYPES.join(', ')}]") if type.nil?
      raise Es::IncorrectSpecificationError.new("The type of field name \"#{name.bright}\" should be a string.") unless type.is_a?(String)
      raise Es::IncorrectSpecificationError.new("The field name \"#{name.bright}\" does have wrong type specified. Specified \"#{type.bright}\" should be one of [#{FIELD_TYPES.join(', ')}]") unless FIELD_TYPES.include?(type)
      @name = name
      @type = type
    end

    def to_extract_fragment(pid, options = {})
      {
        :name => name,
        :preferred => name,
        :definition => {
          :ops => [{
            :type => Es::Helpers.type_to_type(type),
            :data => name
          }],
          :type => Es::Helpers.type_to_operation(type)
        }
      }
    end

    def to_load_fragment(pid)
      {
        :name => name,
        :type => type
      }
    end

    def ==(other)
      other.name == name
    end

  end

  class SnapshotField < Field

    attr_accessor :type, :name

    def is_snapshot?
      true
    end

    def to_extract_fragment(pid, options = {})
      {
        :name => name,
        :preferred => name,
        :definition => {
          :type => "snapshot",
          :data => "date"
        }
      }
    end

  end

  class HIDField < Field

    attr_accessor :type, :name, :entity, :fields, :through

    def is_hid?
      true
    end

    def initialize(name, type, options)
      super(name, type)
      @entity = options[:entity] || fail("Entity has to be scpecified for a HID Field")
      @fields = options[:fields] || fail("Fields has to be scpecified for a HID Field")
      @through = options[:through]
    end

    def to_extract_fragment(pid, options = {})
      {
        :type => "historicid",
        :ops  => [
          through.nil? ? {:type => "recordid"} : {:type => "stream", :data => through},
          {
            :type => "entity",
            :data => entity,
            :ops  => fields.map do |f|
              {
                :type => "stream",
                :data => f
              }
            end
          }
        ]
      }
    end

  end

  class AutoincrementField < Field

    attr_accessor :type, :name

    def is_autoincrement?
      true
    end

    def to_extract_fragment(pid, options = {})
      {
        :name => name,
        :preferred => name,
        :definition => {
          :type => "generate",
          :data => "autoincrement"
        }
      }
    end
  end

  module Helpers
    TEMPLATE_DIR = "./lib/templates"

    def self.load_config(filename, validate=true)
      if validate
        parser = Kwalify::Yaml::Parser.new
        document = parser.parse_file(filename)
        errors = parser.errors()
        if errors && !errors.empty?
          for e in errors
            puts "#{e.linenum}:#{e.column} [#{e.path}] #{e.message}"
          end
          exit
        end
      end
      JSON.parse(File.read(filename), :symbolize_names => true)
    end

    def self.load_destination_dir(pid, entity)
      "/uploads/#{pid}/#{entity.name}"
    end

    def self.load_destination_file(entity, options={})
      with_date = options[:with_date] || false
      source = entity.file
      filename = File.basename(source)
      base =  File.basename(source, '.*')
      ext = File.extname(filename)
      with_date ? base + '_' + DateTime.now.strftime("%Y-%M-%d_%H:%M:%S") + ext : base + ext
    end

    def self.type_to_type(type)
      case type
      when "recordid"
        "recordid"
      when "attribute"
        "stream"
      when "fact"
        "stream"
      when "timeAttribute"
        "stream"
      when "snapshot"
        "snapshot"
      end
    end
    # field.type == 'recordid' ? 'recordid' : 'stream'
    
    def self.type_to_operation(type)
      case type
      when "recordid"
        "value"
      when "attribute"
        "value"
      when "fact"
        "number"
      when "snapshot"
        "snapshot"
      when "timeAttribute"
        "date"
      end
    end
  end

end

# Hack for 1.8.7
# uniq on array does not take block
module Enumerable
  def uniq_by
    seen = Hash.new { |h,k| h[k] = true; false }
    reject { |v| seen[yield(v)] }
  end
end