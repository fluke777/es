module Es
  module Commands

    def self.create(options)
      pid     = options[:pid]
      es_name = options[:es_name]

      begin
        GoodData.post "/gdc/projects/#{pid}/eventStore/stores", {:store => {:storeId => es_name}}
      rescue RestClient::BadRequest
        puts "Seems like eventstore with name #{es_name} already exists"
        exit 1
      end
    end

    def self.delete(options)
      pid     = options[:pid]
      es_name = options[:es_name]
      GoodData.delete "/gdc/projects/#{pid}/eventStore/stores/#{es_name}"
    end

    def self.get_types
      Es::Field::FIELD_TYPES
    end

    def self.truncate(options)
      pid             = options[:pid]
      es_name         = options[:es_name]
      entity_name     = options[:entity]
      timestamp       = options[:timestamp]
      filenames       = options[:load_filenames]
      basedir_pattern = options[:basedir_pattern] || "*.json"

      base_dir = options[:basedir]
      if base_dir.nil?
        # fail "You need to specify entity name" if entity_name.nil?
        fail "You need to specify timestamp" if timestamp.nil?
        fail "You need to specify base filename" if filenames.empty?
      else
        # puts "would grab files like this #{"#{base_dir}/gen_load*.json"}"
        filenames = Dir::glob("#{base_dir}/#{basedir_pattern}")
      end

      filenames.each do |base_filename|

        base_config_file = Es::Helpers.load_config(base_filename)
        base = Es::Load.parse(base_config_file)

        base.entities.each do |entity|
          next if !entity_name.nil? and entity_name != entity.name
          entity.truncate(pid, es_name)
        end
      end
    end

    def self.load(options)
      pid       = options[:pid]
      es_name   = options[:es_name]
      filenames = options[:filenames]
      base_dir  = options[:basedir]
      pattern   = options[:pattern] || "*.json"

      if base_dir.nil?
        fail "Provide path to the loading configuration as a first argument" if filenames.empty?
      else
        # puts "would grab files like this #{"#{base_dir}/gen_json*.json"}"
        filenames = Dir::glob("#{base_dir}/#{pattern}")
      end

      # for each config file
      filenames.each do |filename|
        fail "File #{filename} cannot be found" unless File.exist?(filename)
        load_config_file = Es::Helpers.load_config(filename)
        load = Es::Load.parse(load_config_file)

        load.entities.each do |entity|
          next if options[:only] && entity.name != options[:only]
          next unless Es::Helpers.has_more_lines?(entity.file)
          web_dav_file = Es::Helpers.load_destination_dir(pid, entity) + '/' + Es::Helpers.destination_file(entity)
          if options[:verbose]
            puts "Entity #{entity.name}".bright
            puts "Configuration from #{filename}"
            puts "Will load from #{entity.file} to #{web_dav_file}"
            puts JSON::pretty_generate(entity.to_load_fragment(pid))
          end
          if options[:j]
            puts "Entity #{entity.name}".bright unless options[:verbose]
            puts "load the file #{entity.file} to destination #{web_dav_file} and run the specified as the task"
            puts "======= Load JSON start"
            puts entity.to_load_fragment(pid).to_json.color(:blue)
            puts "======= Load JSON end"
            puts
          else
            entity.load(pid, es_name)
            puts "Done" if options[:verbose]
          end
        end
      end
    end

    def self.load_deleted(options)
      filenames = options[:filenames]
      base_dir  = options[:basedir]
      pattern   = options[:pattern] || "*.json"
      pid       = options[:pid]
      es_name   = options[:es_name]

      if base_dir.nil?
        fail "Provide path to the loading configuration as a first argument" if filenames.empty?
      else
        # puts "would grab files like this #{"#{base_dir}/gen_load*.json"}"
        filenames = Dir::glob("#{base_dir}/#{pattern}")
      end

      compatibility_mode = options[:compatibility] || false
      deleted_type = compatibility_mode ? "isDeleted" : "attribute"

      filenames.each do |load_config_file|
        load_config = Es::Helpers.load_config(load_config_file)
        load = Es::Load.parse(load_config)

        load.entities.each do |entity|
          source_dir = File.dirname(entity.file)
          deleted_filename = Es::Helpers.destination_file(entity, :deleted => true)
          deleted_source = "#{source_dir}/#{deleted_filename}"
          next unless File.exist? deleted_source
          next unless Es::Helpers.has_more_lines?(deleted_source)
          e = Es::Entity.new(entity.name, {
            :file   => deleted_source,
            :fields => [
              Es::Field.new('Id', 'recordid'),
              Es::Field.new('Timestamp', 'timestamp'),
              Es::Field.new('IsDeleted', deleted_type)
            ]
          })
          e.load(pid, es_name)

          if !compatibility_mode
            deleted_with_time = "#{source_dir}/#{deleted_filename}".gsub(/\.csv$/, '_del.csv')
            FasterCSV.open(deleted_with_time, 'w') do |csv|
              csv << ['Id', 'Timestamp', 'DeletedAt']
              FasterCSV.foreach("#{source_dir}/#{deleted_filename}", :headers => true, :return_headers => false) do |row|
                csv << row.values_at('Id', 'Timestamp', 'Timestamp')
              end
            end

            e1 = Es::Entity.new(entity.name, {
              :file   => deleted_with_time,
              :fields => [
                Es::Field.new('Id', 'recordid'),
                Es::Field.new('Timestamp', 'timestamp'),
                Es::Field.new('DeletedAt', 'time')
              ]
            })
            e1.load(pid, es_name)
          end
        end
      end
      
    end

    def self.extract(options)
      base_dir      = options[:base_dir]
      extract_dir   = options[:extract_dir]
      pid           = options[:pid]
      es_name       = options[:es_name]
      now           = options[:now]

      if base_dir.nil? && extract_dir.nil?
        fail "Provide path to the loading configuration as a first argument" if args.first.nil?
        load_config_files = [args.first]
        fail "Provide path to the extract configuration as a second argument" if args[1].nil?
        extract_config_files = [args[1]]
      else
        load_config_files = Dir::glob("#{base_dir}/gen_load*.json")
        extract_config_files = Dir::glob("#{extract_dir}/gen_extract*.json")
      end

      # build one giant load config
      load_entities = load_config_files.reduce([]) do |memo, filename|
        fail "File #{filename} cannot be found" unless File.exist?(filename)
        load_config = Es::Helpers.load_config(filename)
        load = Es::Load.parse(load_config)
        memo.concat(load.entities)
      end
      hyper_load = Es::Load.new(load_entities)

      extract_config_files.each do |extract_config_file|
        fail "File #{extract_config_file} cannot be found" unless File.exist?(extract_config_file)
        extract_config = Es::Helpers.load_config(extract_config_file)
        extract = Es::Extract.parse(extract_config, hyper_load, :now => now)

        extract.entities.each do |entity|
          next if options[:only] && entity.name != options[:only]
          # pp extract.to_extract_fragment(pid)

          if options[:verbose] || options[:json] || options[:debug] then
            puts "Entity #{entity.name.bright}" 
            puts "Config from #{load_config_files.join(', ')} and #{extract_config_file}"
          end

          puts JSON.pretty_generate(entity.to_extract_fragment(pid)) if options[:debug]

          if options[:json]
            # puts "load the file #{entity.file} to destination #{web_dav_file} and run the specified as the task"
            puts "======= Extract JSON start"
            puts entity.to_extract_fragment(pid, :pretty => false).to_json.color(:blue)
            puts "======= Extract JSON end"
            puts
          else
            entity.extract(pid, es_name)
          end
        end
      end
    end

  end
end