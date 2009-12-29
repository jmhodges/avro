module Avro
  module GenericIO
    class DatumReader
      def initialize(actual=nil, expected=nil)
        set_schema(actual)
        @expected = expected
        @readers = {
          Schema::BOOLEAN => lambda {|actual, expected, decoder| decoder.read_boolean },
          Schema::STRING => lambda {|actual, expected, decoder| decoder.read_string },
          Schema::INT => lambda {|actual, expected, decoder| decoder.read_int },
          Schema::LONG => lambda {|actual, expected, decoder| decoder.read_long },
          Schema::FLOAT => lambda {|actual, expected, decoder| decoder.read_float },
          Schema::DOUBLE => lambda {|actual, expected, decoder| decoder.read_double },
          Schema::BYTES => lambda {|actual, expected, decoder| decoder.read_bytes },
          Schema::FIXED => method(:read_fixed),
          Schema::ARRAY => method(:read_array),
          Schema::MAP  => method(:read_map),
          Schema::RECORD => method(:read_record),
          Schema::ENUM => method(:read_enum)
        }

        @skippers = {
          Schema::STRING => lambda {|actual, expected, decoder| decoder.skip_string },
          Schema::INT => lambda {|actual, expected, decoder| decoder.skip_int },
          Schema::LONG => lambda {|actual, expected, decoder| decoder.skip_long },
          Schema::FLOAT => lambda {|actual, expected, decoder| decoder.skip_float },
          Schema::DOUBLE => lambda {|actual, expected, decoder| decoder.skip_double },
          Schema::BYTES => lambda {|actual, expected, decoder| decoder.skip_bytes },
          Schema::FIXED => method(:skip_fixed),
          Schema::ARRAY => method(:skip_array),
          Schema::MAP  => method(:skip_map),
          Schema::RECORD => method(:skip_record),
          Schema::ENUM => method(:skip_enum)
        }
      end

      def set_schema(schm); @actual = schm; end

      def read(decoder)
        @expected = @actual unless @expected
        read_data(@actual, @expected, decoder)
      end

      def read_data(actual, expected, decoder)
        if actual.gettype == Schema::UNION
          actual = actual.getelementtypes[decoder.read_long.to_i]
        end
        if expected.gettype == Schema::UNION
          expected = resolve(actual, expected)
        end
        if actual.gettype == Schema::NULL
          return nil
        end

        fn = @readers[actual.gettype]
        if fn
          fn.call(actual, expected, decoder)
        else
          raise AvroError, "Unknown type: #{actual}"
        end
      end

      def read_fixed(actual, expected, decoder)
        check_name(actual, expected)
        if actual.getsize != expected.getsize
          raise_match_exception(actual, expected)
        end

        decoder.read(actual.getsize)
      end

      def read_array(actual, expected, decoder)
        if actual.getelementtype.gettype != expected.getelementtype.gettype
          raise_match_exception(actual, expected)
        end
        result = []
        size = decoder.read_long
        while size != 0
          if size < 0
            size = -size
            bytecount = decoder.read_long # ignore bytecount if this is a blocking array
          end

          size.times do
            result << read_data(actual.getelementtype,
                                expected.getelementtype,
                                decoder)
          end
          size = decoder.read_long
        end
        result
      end

      def read_map(actual, expected, decoder)
        if actual.getvaluetype.gettype != expected.getvaluetype.gettype
          raise_match_exception(actual, expected)
        end

        result = {}
        size = decoder.read_long
        while size != 0
          if size < 0
            size = -size
            decoder.read_long
          end
          size.times do
            key = decoder.read_string
            result[key] = read_data(actual.getvaluetype,
                                    expected.getvaluetype,
                                    decoder)
          end
          size = decoder.read_long
        end
        result
      end

      def read_record(actual, expected, decoder)
        check_name(actual, expected)
        expectedfields = expected.getfields
        record = create_record(actual)
        size = 0
        actual.getfields.each do |fieldname, field|
          if expected == actual
            expectedfield = field
          else
            expectedfield = expectedfields[fieldname]
          end
          if expectedfield.nil?
            skipdata(field.getschema, decoder)
            next
          end
          record[fieldname] = read_data(field.getschema,
                                        expectedfield.getschema,
                                        decoder)
          size += 1
        end
        if expectedfields.size > size # not all fields set
          actualfields = actual.getfields
          expectedfields.each do |fieldname, field|
            unless actualfields.has_key?(fieldname)
              defval = field.getdefaultvalue
              if !defval.nil?
                record[fieldname] = defaultfieldvalue(field.getschema, defval)
              end
            end
          end
        end
        return record
      end

      def read_enum(actual, expected, decoder)
        check_name(actual, expected)
        index = decoder.read_int
        actual.getenumsymbols[index]
      end

      def skip_fixed; end
      def skip_array; end
      def skip_map; end
      def skip_record; end
      def skip_enum; end

      def resolve(actual, expected)
        # scan for exact match
        r = expected.getelementtypes.find{|e| e.gettype == actual.gettype }
        return r if r

        # san for match via numeric promotion
        atype = actual.gettype
        expected.getelementtypes.each do |elem|
          etype = elem.gettype
          case atype
          when Schema::INT
            if [Schema::LONG, Schema::FLOAT, Schema::DOUBLE].include? etype
              return elem
            end
          when Schema::LONG
            return elem if [Schema::LONG, Schema::FLOAT].include? etype
          when Schema::FLOAT
            return elem if etype == Schema::DOUBLE
          else
            raise_match_exception(actual, expected)
          end
        end
      end

      def check_name(actual, expected)
        if actual.getname != expected.getname
          raise_match_exception(actual, expected)
        end
      end

      def create_record(schm); {}; end

      def defaultfieldvalue(schm, defaultnode)
        case schm.gettype
        when Schema::RECORD
          record = create_record(schm)
          schm.getfields.values.each do |field|
            v = defaultnode[field.getname] || field.getdefaultvalue
            if v
              record[field.getname] = defaultfieldvalue(field.getschema, v)
            end
          end
          record

        when Schema::ARRAY
          defaultnode.map{|n| defaultfieldvalue(schm.getelementtype, n) }

        when Schema::MAP
          map = {}
          defaultnode.each do |k,v|
            map[k] = defaultfieldvalue(schm.getvaluetype, v)
          end
          map

        when Schema::UNION
          defaultfieldvalue(schm.getelementtypes[0], defaultnode)
        when Schema::ENUM, Schema::FIXED, Schema::STRING, Schema::BYTES
          defaultnode

        when Schema::INT, Schema::LONG
          defaultnode.to_i
        when Schema::FLOAT, Schema::DOUBLE
          defaultnode.to_f
        when Schema::BOOLEAN
          defaultnode
        when Schema::NULL
          nil
        else
          raise AvroError, "Unknown type: #{actual.to_s}"
        end
      end

      def raise_match_exception(actual, expected)
        raise AvroException, "Expected "+ expected.to_s + ", found " + actual.to_s
      end
    end

    class DatumWriter
      def initialize(schm=nil)
        @schm = schm
        @writers = {
          Schema::BOOLEAN => lambda {|schm, datum, encoder| 
            encoder.write_boolean(datum)},
          Schema::STRING => lambda {|schm, datum, encoder|
            encoder.write_string(datum)},
          Schema::INT => lambda {|schm, datum, encoder|
            encoder.write_int(datum)},
          Schema::LONG => lambda {|schm, datum, encoder|
            encoder.write_long(datum)},
          Schema::FLOAT => lambda {|schm, datum, encoder|
            encoder.write_float(datum)},
          Schema::DOUBLE => lambda {|schm, datum, encoder|
            encoder.write_double(datum)},
          Schema::BYTES => lambda {|schm, datum, encoder|
            encoder.write_bytes(datum) },
          Schema::FIXED => lambda {|schm, datum, encoder| 
            encoder.write(datum) },
          Schema::ARRAY => lambda {|schm, datum, encoder| write_array(schm, datum, encoder) },
          Schema::MAP => lambda {|schm, datum, encoder| write_map(schm, datum, encoder) },
          Schema::RECORD => lambda {|schm, datum, encoder| write_record(schm, datum, encoder) },
          Schema::ENUM => lambda {|schm, datum, encoder| write_enum(schm, datum, encoder) },
          Schema::UNION => lambda {|schm, datum, encoder| write_union(schm, datum, encoder) }
        }
      end

      def write(datum, encoder)
        write_data(@schm, datum, encoder)
      end

      def write_data(schm, datum, encoder)
        if schm.gettype == Schema::NULL
          return if datum.nil?
          raise AvroTypeError.new(schm, datum)
        end

        if fn = @writers[schm.gettype]
          fn.call(schm, datum, encoder)
        else
          raise AvroTypeError.new(schm, datum)
        end
      end

      def write_map(schm, datum, encoder)
        raise AvroTypeError.new(schm, datum) unless datum.is_a?(Hash)
        if datum.size > 0
          encoder.write_long(datum.size)
          datum.each do |k,v|
            encoder.write_string(k)
            write_data(schm.getvaluetype, v, encoder)
          end
        end
        encoder.write_long(0)
      end

      def write_array(schm, datum, encoder)
        raise AvroTypeError.new(schm, datum) unless datum.is_a?(Array)
        if datum.size > 0
          encoder.write_long(datum.size)
          datum.each{|item| write_data(schm.getelementtype, item, encoder) }
        end
        encoder.write_long(0)
      end

      def write_record(schm, datum, encoder)
        raise AvroTypeError.new(schm, datum) unless datum.is_a?(Hash)
        schm.getfields.values.each do |field|
          write_data(field.getschema, datum[field.getname], encoder)
        end
      end

      def write_union(schm, datum, encoder)
        index = schm.getelementtypes.
          find_index{|e| Schema.validate(e, datum) }
        raise AvroTypeError.new(schm, datum) unless index
        encoder.write_long(index)
        write_data(schm.getelementtypes[index], datum, encoder)
      end

      def write_enum(schm, datum, encoder)
        index = schm.getenumordinal(datum)
        encoder.write_int(index)
      end
    end
  end
end
