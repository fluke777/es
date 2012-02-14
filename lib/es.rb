module Es

  TEMPLATE_DIR = "./lib/templates"

  def self.generate(template_name, params, pretty = true)
    mode = pretty ? :pretty : :plain
    template = File.read("#{TEMPLATE_DIR}/#{template_name}.jsonify")
    result = Jsonify::Builder.send(mode) do |json|
      eval(template)
    end
    result
  end

  class Field

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
      @name = name
      @type = type
    end
    
  end

  class SnapshotField < Field

    attr_accessor :type, :name

    def is_snapshot?
      true
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

  end
  

  class AutoincrementField < Field

    attr_accessor :type, :name

    def is_autoincrement?
      true
    end
  end


  class Entity
    attr_accessor :name, :file, :fields
    
    def initialize(name, file, fields = [])
      @name = name
      @file = file
      @fields = fields
    end
    
    def find_field_by_name(a_name)
      @fields.find {|f| f.name == a_name}
    end
  end

  module Helpers
    
    def self.parse_load_config(file)
      params = JSON.parse(File.read(file), :symbolize_names => true)
      entities = params.map do |f|
        fields = f[:fields].map do |field|
          Es::Field.new(field[:name], field[:type])
        end
        Es::Entity.new(f[:entity], f[:file], fields)
      end
      entities
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
        "key"
      when "snapshot"
        "snapshot"
      when "timeAttribute"
        "date"
      end
    end
  end

end