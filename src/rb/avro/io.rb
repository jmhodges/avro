module Avro
  module IO
    # Raised when datum is not an example of schema
    class AvroTypeError < AvroError
      def initialize(expected_schema, datum)
        msg =
          "The datum #{datum} is not an example of schema #{expected_schema}"
        super(msg)
      end
    end

    # Raised when writer's and reader's schema do not match
    class SchemaMatchException < AvroError
      def initialize(writers_schema, readers_schema)
        msg = "Writer's schema #{writers_schema} and Reader's schema " +
          "#{readers_schema} do not match."
        super(msg)
      end
    end

    # FIXME(jmhodges) move validate to this module?

    class BinaryDecoder
      # Read leaf values

      # reader is an object on which we can call read, seek and tell.
      attr_reader :reader
      def initialize(reader)
        @reader = reader
      end

      def byte!
        @reader.read(1)[0]
      end

      def read_null
        # null is written as zero byte's
        nil
      end

      def read_boolean
        byte! == 1
      end

      def read_int; read_long; end

      def read_long
        # int and long values are written using variable-length,
        # zig-zag coding.
        b = byte!
        n = b & 0x7F
        shift = 7
        while (b & 0x80) != 0
          b = byte!
          n |= (b & 0x7F) << shift
          shift += 7
        end
        (n >> 1) ^ -(n & 1)
      end

      def read_float
        # A float is written as 4 bytes.
        # The float is converted into a 32-bit integer using a method
        # equivalent to Java's floatToIntBits and then encoded in
        # little-endian format.
        
        bits = (byte! & 0xFF) |
          ((byte! & 0xff) <<  8) |
          ((byte! & 0xff) << 16) |
          ((byte! & 0xff) << 24)
        [bits].pack('i').unpack('e')[0]
      end

      def read_double
        #  A double is written as 8 bytes.
        # The double is converted into a 64-bit integer using a method
        # equivalent to Java's doubleToLongBits and then encoded in
        # little-endian format.
        
        bits = (byte! & 0xFF) |
          ((byte! & 0xff) <<  8) |
          ((byte! & 0xff) << 16) |
          ((byte! & 0xff) << 24) |
          ((byte! & 0xff) << 32) |
          ((byte! & 0xff) << 40) |
          ((byte! & 0xff) << 48) |
          ((byte! & 0xff) << 56)
        [bits].pack('Q').unpack('d')[0]
      end

      def read_bytes
        # Bytes are encoded as a long followed by that many bytes of
        # data.
        read(read_long)
      end

      def read_string
        # A string is encoded as a long followed by that many bytes of
        # UTF-8 encoded character data.
        # FIXME utf-8 encode this in 1.9
        read_bytes
      end

      def read(len)
        # Read n bytes
        @reader.read(len)
      end
    end

    # Write leaf values
    class BinaryEncoder
      attr_reader :writer

      def initialize(writer)
        @writer = writer
      end

      # null is written as zero bytes
      def write_null(datum)
        nil
      end

      # a boolean is written as a single byte 
      # whose value is either 0 (false) or 1 (true).
      def write_boolean(datum)
        on_disk = datum ? 1.chr : 0.chr
        writer.write(on_disk)
      end

      # int and long values are written using variable-length,
      # zig-zag coding.
      def write_int(n)
        write_long(n)
      end

      # int and long values are written using variable-length,
      # zig-zag coding.
      def write_long(n)
        foo = n
        n = (n << 1) ^ (n >> 63)
        while (n & ~0x7F) != 0
          @writer.write(((n & 0x7f) | 0x80).chr)
          n >>= 7
        end
        @writer.write(n.chr)
      end

      # A float is written as 4 bytes.
      # The float is converted into a 32-bit integer using a method
      # equivalent to Java's floatToIntBits and then encoded in
      # little-endian format.
      def write_float(datum)
        bits = [datum].pack('e').unpack('i')[0]
        @writer.write(((bits      ) & 0xFF).chr)
        @writer.write(((bits >> 8 ) & 0xFF).chr)
        @writer.write(((bits >> 16) & 0xFF).chr)
        @writer.write(((bits >> 24) & 0xFF).chr)
      end

      # A double is written as 8 bytes.
      # The double is converted into a 64-bit integer using a method
      # equivalent to Java's doubleToLongBits and then encoded in
      # little-endian format.
      def write_double(datum)

        bits = [datum].pack('d').unpack('Q')[0]
        @writer.write(((bits      ) & 0xFF).chr)
        @writer.write(((bits >> 8 ) & 0xFF).chr)
        @writer.write(((bits >> 16) & 0xFF).chr)
        @writer.write(((bits >> 24) & 0xFF).chr)
        @writer.write(((bits >> 32) & 0xFF).chr)
        @writer.write(((bits >> 40) & 0xFF).chr)
        @writer.write(((bits >> 48) & 0xFF).chr)
        @writer.write(((bits >> 56) & 0xFF).chr)
      end

      # Bytes are encoded as a long followed by that many bytes of data.
      def write_bytes(datum)
        write_long(datum.size)
        @writer.write(datum)
      end

      # A string is encoded as a long followed by that many bytes of
      # UTF-8 encoded character data
      def write_string(datum)
        # FIXME utf-8 encode this in 1.9
        write_bytes(datum)
      end

      # Write an arbritary datum.
      def write(datum)
        writer.write(datum)
      end
    end

    
    class DatumReader
      def self.check_props(schema_one, schema_two, prop_list)
        prop_list.all? do |prop|
          schema_one.to_hash[prop] == schema_two.to_hash[prop]
        end
      end

      def self.match_schemas(writers_schema, readers_schema)
        w_type = writers_schema.type
        r_type = readers_schema.type

        if [w_type, r_type].include? 'union'
          return true

        elsif Schema::PRIMITIVE_TYPES.include?(w_type) &&
            Schema::PRIMITIVE_TYPES.include?(r_type) &&
            w_type == r_type
          # This conditional is begging for some OO love.
          return true
        elsif (w_type == r_type) && (r_type == 'record') &&
            check_props(writers_schema, readers_schema,
                                   ['fullname'])
          return true
        elsif (w_type == r_type) && (r_type == 'fixed') &&
            check_props(writers_schema, readers_schema,
                                   ['fullname', 'size'])
          return true
        elsif (w_type == r_type) && (r_type == 'enum') &&
            check_props(writers_schema, readers_schema,
                                   ['fullname'])
          return true
        elsif (w_type == r_type) && (r_type == 'map') &&
            check_props(writers_schema.values,
                                   readers_schema.values,
                                   ['type'])
          return true
        elsif (w_type == r_type) && (r_type == 'array') &&
            check_props(writers_schema.items,
                                   readers_schema.items,
                                   ['type'])
          return true
        end

        # Handle schema promotion
        if w_type == 'int' && ['long', 'float', 'double'].include?(r_type)
          return true
        elsif w_type == 'long' && ['float', 'double'].include?(r_type)
          return true
        elsif w_type == 'float' && r_type == 'double'
          return true
        end

        return false
      end

      attr_accessor :writers_schema, :readers_schema

      def initialize(writers_schema=nil, readers_schema=nil)
        @writers_schema = writers_schema
        @readers_schema = readers_schema
      end

      def read(decoder)
        self.readers_schema = writers_schema unless readers_schema
        read_data(writers_schema, readers_schema, decoder)
      end

      def read_data(writers_schema, readers_schema, decoder)
        # schema matching
        unless self.class.match_schemas(writers_schema, readers_schema)
          raise SchemaMatchException.new(writers_schema, readers_schema)
        end

        # schema resolution: reader's schema is a union, writer's
        # schema is not
        if writers_schema.type != 'union' && readers_schema.type == 'union'
          rs = readers_schema.schemas.find{|s|
            self.class.match_schemas(writers_schema, s)
          }
          return read_data(writers_schema, rs, decoder) if rs
          raise SchemaMatchException.new(writers_schema, readers_schema)
        end

        # function dispatch for reading data based on type of writer's
        # schema
        case writers_schema.type
        when 'null'; decoder.read_null
        when 'boolean'; decoder.read_boolean
        when 'string'; decoder.read_string
        when 'int'; decoder.read_int
        when 'long'; decoder.read_long
        when 'float'; decoder.read_float
        when 'double'; decoder.read_double
        when 'bytes'; decoder.read_bytes
        when 'fixed'
          read_fixed(writers_schema, readers_schema, decoder)
        when 'enum'
          read_enum(writers_schema, readers_schema, decoder)
        when 'array'
          read_array(writers_schema, readers_schema, decoder)
        when 'map'
          read_map(writers_schema, readers_schema, decoder)
        when 'union'
          read_union(writers_schema, readers_schema, decoder)
        when 'record'
          read_record(writers_schema, readers_schema, decoder)
        else
          msg = "Cannot read unknown schema type: #{writers_schema.type}"
          raise AvroError, msg
        end
      end

      def read_fixed(writers_schema, readers_schema, decoder)
        decoder.read(writers_schema.size)
      end

      def read_enum(writers_schema, readers_schema, decoder)
        index_of_symbol = decoder.read_int
        read_symbol = writers_schema.symbols[index_of_symbol]

        # TODO(jmhodges): figure out what unset means for resolution
        # schema resolution
        unless readers_schema.symbols.include?(read_symbol)
          # 'unset' here
        end

        read_symbol
      end

      def read_array(writers_schema, readers_schema, decoder)
        read_items = []
        block_count = decoder.read_long
        while block_count != 0
          if block_count < 0
            block_count = -block_count
            block_size = decoder.read_long
          end
          block_count.times do
            read_items << read_data(writers_schema.items,
                                    readers_schema.items,
                                    decoder)
          end
          block_count = decoder.read_long
        end

        read_items
      end

      def read_map(writers_schema, readers_schema, decoder)
        read_items = {}
        block_count = decoder.read_long
        while block_count != 0
          if block_count < 0
            block_count = -block_count
            block_size = decoder.read_long
          end
          block_count.times do
            key = decoder.read_string
            read_items[key] = read_data(writers_schema.values,
                                        readers_schema.values,
                                        decoder)
          end
          block_count = decoder.read_long
        end

        read_items
      end

      def read_union(writers_schema, readers_schema, decoder)
        index_of_schema = decoder.read_long
        selected_writers_schema = writers_schema.schemas[index_of_schema]

        read_data(selected_writers_schema, readers_schema, decoder)
      end

      def read_record(writers_schema, readers_schema, decoder)
        readers_fields_hash = readers_schema.fields_hash
        read_record = {}
        writers_schema.fields.each do |field|
          if readers_field = readers_fields_hash[field.name]
            field_val = read_data(field.type, readers_field.type, decoder)
            read_record[field.name] = field_val
          else
            skip_data(field.type, decoder)
          end
        end

        # fill in the default values
        if readers_fields_hash.size > read_record.size
          writers_fields_hash = writers_schema.fields_hash
          readers_fields_hash.each do |field_name, field|
            
            unless writers_fields_hash.has_key? field_name
              if !field.default.nil?
                field_val = read_default_value(field.type, field.default)
                read_record[field.name] = field_val
              else
                # FIXME(jmhodges) another 'unset' here
              end
            end
          end
        end

        read_record
      end

      def read_default_value(field_schema, default_value)
        # Basically a JSON Decoder?
        case field_schema.type
        when 'null'
          return nil
        when 'boolean'
          return default_value
        when 'int', 'long'
          return Integer(default_value)
        when 'float', 'double'
          return Float(default_value)
        when 'enum', 'fixed', 'string', 'bytes'
          return default_value
        when 'array'
          read_array = []
          default_value.each do |json_val|
            item_val = read_default_value(field_schema.items, json_val)
            read_array << item_val
          end
          return read_array
        when 'map'
          read_map = {}
          default_value.each do |key, json_val|
            map_val = read_default_value(field_schema.values, json_val)
            read_map[key] = map_val
          end
          return read_map
        when 'union'
          return read_default_value(field_schema.schemas[0], default_value)
        when 'record'
          read_record = {}
          field_schema.fields.each do |field|
            json_val = default_value[field.name]
            json_val = field.default unless json_val
            field_val = read_default_value(field.type, json_val)
            read_record[field.name] = field_val
          end
          return read_record
        else
          fail_msg = "Unknown type: #{field_schema.type}"
          raise AvroError(fail_msg)
        end
      end
    end # DatumReader

    # DatumWriter for generic ruby objects
    class DatumWriter
      attr_accessor :writers_schema
      def initialize(writers_schema=nil)
        @writers_schema = writers_schema
      end

      def write(datum, encoder)
        write_data(writers_schema, datum, encoder)
      end

      def write_data(writers_schema, datum, encoder)
        unless Schema.validate(writers_schema, datum)
          raise AvroTypeError.new(writers_schema, datum)
        end

        # function dispatch to write datum
        case writers_schema.type
        when 'null'
          encoder.write_null(datum)
        when 'boolean'
          encoder.write_boolean(datum)
        when 'string'
          encoder.write_string(datum)
        when 'int'
          encoder.write_int(datum)
        when 'long'
          encoder.write_long(datum)
        when 'float'
          encoder.write_float(datum)
        when 'double'
          encoder.write_double(datum)
        when 'bytes'
          encoder.write_bytes(datum)
        when 'fixed'
          write_fixed(writers_schema, datum, encoder)
        when 'enum'
          write_enum(writers_schema, datum, encoder)
        when 'array'
          write_array(writers_schema, datum, encoder)
        when 'map'
          write_map(writers_schema, datum, encoder)
        when 'union'
          write_union(writers_schema, datum, encoder)
        when 'record'
          write_record(writers_schema, datum, encoder)
        else
          fail_msg = "Unknown type: #{writers_schema.type}"
          raise AvroError.new(fail_msg)
        end
      end
      
      def write_fixed(writers_schema, datum, encoder)
        encoder.write(datum)
      end
      
      def write_enum(writers_schema, datum, encoder)
        index_of_datum = writers_schema.symbols.index(datum)
        encoder.write_int(index_of_datum)
      end

      def write_array(writers_schema, datum, encoder)
        if datum.size > 0
          encoder.write_long(datum.size)
          datum.each do |item|
            write_data(writers_schema.items, item, encoder)
          end
        end
        encoder.write_long(0)
      end

      def write_map(writers_schema, datum, encoder)
        if datum.size > 0
          encoder.write_long(datum.size)
          datum.each do |k,v|
            encoder.write_string(k)
            write_data(writers_schema.values, v, encoder)
          end
        end
        encoder.write_long(0)
      end

      def write_union(writers_schema, datum, encoder)
        index_of_schema = writers_schema.schemas.
          find_index{|e| Schema.validate(e, datum) }
        unless index_of_schema
          raise AvroTypeError.new(writers_schema, datum)
        end
        encoder.write_long(index_of_schema)
        write_data(writers_schema.schemas[index_of_schema], datum, encoder)
      end

      def write_record(writers_schema, datum, encoder)
        writers_schema.fields.each do |field|
          write_data(field.type, datum[field.name], encoder)
        end
      end
    end # DatumWriter
  end
end
