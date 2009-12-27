module Avro
  module GenericIO
    class DatumReaderBase
      def set_schema(schema); end
      def read(decoder); end
    end

    class DatumReader < DatumReaderBase
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

      def read_array(actual, expected, decoder); raise NotImplementedError end
      def read_map(actual, expected, decoder); raise NotImplementedError end

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

      def read_enum(actual, expected, decoder); raise NotImplementedError end

      def skip_fixed; end
      def skip_array; end
      def skip_map; end
      def skip_record; end
      def skip_enum; end

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
  end
end
