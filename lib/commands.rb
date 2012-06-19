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
    
    def self.init(options)
      pid             = options[:pid]
      es_name         = options[:es_name]
      base_dir        = options[:basedir]
      deleted         = options[:deleted]
      only            = options[:only]
      
      fail "Provide path to the loading configuration" if base_dir.nil?
      filenames = Dir::glob("#{base_dir}/gen_json*.json")
      
      # for each config file
      filenames.each do |filename|
        fail "File #{filename} cannot be found" unless File.exist?(filename)
        load_config_file = Es::Helpers.load_config(filename)
        load = Es::Load.parse(load_config_file)
        
        load.entities.each do |entity|
          next if only && entity.name != only
          begin
            tmp_file = Tempfile.new(entity.name)
            header_row = []
            content_row = []
            entity.fields.each do |field|
              header_row << field.name
              content_row << 2147483647 if field.is_timestamp?
              content_row << 1 if field.is_recordid?
              content_row << "" if !field.is_recordid? && !field.is_timestamp?
            end
            if deleted
              header_row << "IsDeleted" << "DeletedAt"
              content_row << "" << ""
            end
            tmp_file.puts(header_row.join(","))
            tmp_file.puts(content_row.join(","))
            entity.file = tmp_file.path
            # create temp file, link it to entity
            web_dav_file = Es::Helpers.load_destination_dir(pid, entity) + '/' + Es::Helpers.destination_file(entity)
            if options[:verbose]
              puts "Entity #{entity.name}".bright
              puts "Configuration from #{filename}"
              puts "Will load from #{entity.file} to #{web_dav_file}"
              puts JSON::pretty_generate(entity.to_load_fragment(pid))
            end
            entity.load(pid, es_name)
            puts "Done" if options[:verbose]
          ensure
            tmp_file.close
            tmp_file.unlink
          end
          truncate(:pid => options[:pid], :es_name => options[:es_name], :load_filenames => [filename], :timestamp => 2147483646, :entity => entity)
        end
      end
    end

    def self.truncate(options)
      pid             = options[:pid]
      es_name         = options[:es_name]
      entity_name     = options[:entity]
      timestamp       = options[:timestamp]
      filenames       = options[:load_filenames]
      basedir_pattern = options[:basedir_pattern] || "*.json"
      logger          = options[:logger]

      base_dir = options[:basedir]
      
      fail "You need to specify timestamp" if timestamp.nil?
      if base_dir.nil?
        # fail "You need to specify entity name" if entity_name.nil?
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
          logger.info "truncating entity \"#{entity.name}\"} at timestamp #{timestamp} that is #{Time.at(timestamp).to_s}" if logger
          entity.truncate(pid, es_name, timestamp)
        end
      end
    end

    def self.load(options)
      pid       = options[:pid]
      es_name   = options[:es_name]
      filenames = options[:filenames]
      base_dir  = options[:basedir]
      pattern   = options[:pattern] || "*.json"
      only      = options[:only]
      logger    = options[:logger]

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
          next if only && entity.name != only
          next unless Es::Helpers.has_more_lines?(entity.file)

          logger.info "Loading entity \"#{entity.name}\", fields #{entity.fields.map {|f| f.name}.join(', ')}" if logger
          logger.info "Using json => #{entity.to_load_fragment(pid).to_json}" if logger

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
      logger    = options[:logger]

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
          has_more_lines = Es::Helpers.has_more_lines?(deleted_source)
          logger.info "Deleted records for entity #{entity.name} is not loaded since the file #{deleted_source} is empty." if logger && !has_more_lines
          next unless has_more_lines

          logger.info "Loading deleted records for entity #{entity.name} in with compatibility mode set to #{compatibility_mode}." if logger

          e = Es::Entity.create_deleted_entity(entity.name, {:compatibility_mode => compatibility_mode, :file => deleted_source})
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
      base_dir      = options[:basedir]
      extract_dir   = options[:extractdir]
      pid           = options[:pid]
      es_name       = options[:es_name]
      now           = options[:now]
      args          = options[:args]
      logger        = options[:logger]

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

          logger.info "Extracting entity \"#{entity.name}\", fields #{entity.fields.map {|f| f.name}.join(', ')}" if logger
          logger.info "Using json => #{entity.to_extract_fragment(pid, :pretty => false).to_json}" if logger

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


    def self.generate_base(options)
      entity = options[:entity]
      input_filename = options[:input]
      input_dir = options[:inputdir]
      output_filename = options[:output]
      base_dir = options[:basedir]

      fail "You need to specify input file name or input dir" if input_filename.nil? && input_dir.nil?

      if base_dir.nil?
        input_filenames = [input_filename]
        else
        input_filenames = Dir::glob("#{input_dir}/*.csv")
      end
      
      input_filenames.each do |input_filename|
        
        headers = nil
        FasterCSV.foreach(input_filename, :headers => true, :return_headers => true) do |row|
          if row.header_row?
            headers = row.fields
            break
          end
        end
        
        entity_name = entity || File.basename(input_filename, ".csv")
        load = Es::Load.new([
                            Es::Entity.new(entity_name, {
                                           :file => input_filename,
                                           :fields => headers.map do |field_name|
                                           Es::Field.new(field_name, "none")
                                           end
                                           })
                            ])
        
        config = JSON.pretty_generate(load.to_config)
        
        if input_dir && base_dir
          File.open(base_dir+"/gen_load_"+entity_name+".json", 'w') do |f|
            f.write config
          end
          elsif input_filename && output_filename
          File.open(output_filename, 'w') do |f|
            f.write config
          end
          else 
          puts config
        end
      end
    end
    
    def self.generate_extract(options)
      base_dir = options[:basedir]
      fail "You need to specify base dir" if base_dir.nil?

      base_filenames = Dir::glob("#{base_dir}/gen_load_*.json")
      # build one giant load config
      base_entities = base_filenames.reduce([]) do |memo, filename|
        fail "File #{filename} cannot be found" unless File.exist?(filename)
        load_config = Es::Helpers.load_config(filename)
        load = Es::Load.parse(load_config)
        memo.concat(load.entities)
      end
      hyper_load = Es::Load.new(base_entities)
      entity_names = hyper_load.entities.map {|e| e.name}.uniq

      entity_names.each do |entity_name|
        entity = hyper_load.get_merged_entity_for(entity_name)
        
        File.open(base_dir+"/gen_extract_"+entity.name+".json", 'w') do |f|
          f.write JSON.pretty_generate(entity.to_extract_config)
        end
      end
    end
    
    def self.load_column(options)
      file = options[:input]
      name = options[:name]
      type = options[:type]
      entity = options[:entity]
      base_filename = options[:base]
      pid = options[:pid]
      es_name = options[:es_name]
      rid_name = options[:rid] || 'Id'

      fail "You need to specify column name" if name.nil?
      fail "You need to specify column type" if type.nil?
      fail "You need to specify entity name" if entity.nil?
      fail "You need to specify input file name" if file.nil?

      base_config_file = Es::Helpers.load_config(base_filename)
      base = Es::Load.parse(base_config_file)

      load = Es::Load.new([
        Es::Entity.new(entity, {
           :file => file,
           :fields => [
             Es::Field.new('Timestamp', 'timestamp'),
             Es::Field.new(rid_name, 'recordid'),
             Es::Field.new(name, type)
           ]
        })
      ])

      base.get_entity(entity).add_field(Es::Field.new(name, type))
      puts "Added field #{name}" if options[:verbose]
      base.to_config_file(base_filename)

      load.entities.first.load(pid, es_name)
    
    end
    
  end
end