module Avro
  class Schema
    STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL,
    ARRAY, MAP, UNION, FIXED, RECORD, ENUM = (0..13).to_a

    VALIDATIONS = {
      NULL => lambda { |schm, object| object.nil? },
      BOOLEAN => lambda {|schm, object| object == true || object == false },
      STRING => lambda {|schm, object| object.is_a? String },
      FLOAT => lambda {|schm, object| object.is_a?(Float) },
      DOUBLE => lambda {|schm, object| object.is_a?(Float) },
      BYTES => lambda {|schm, object| object.is_a?(String) },
      INT => lambda {|schm, object| (object.is_a?(Fixnum) || object.is_a?(Bignum)) && (IO::INT_MIN_VALUE <= object) && (object <= IO::INT_MAX_VALUE) },
      LONG => lambda {|schm, object| (object.is_a?(Fixnum) || object.is_a?(Bignum)) && (IO::LONG_MIN_VALUE <= object) && (object <= IO::LONG_MAX_VALUE) },
      ENUM => lambda {|schm, object| schm.getenumsymbols.include?(object) },
      FIXED => lambda {|schm, object|
        object.is_a?(String) && object.size == schm.getsize},

      ARRAY => lambda {|schm, object| self.validatearray(schm, object) },
      MAP => lambda {|schm, object| self.validatemap(schm, object) },
      RECORD => lambda {|schm, object| self.validaterecord(schm, object) },
      UNION => lambda {|schm, object| self.validateunion(schm, object) }
    }

    def self.parse(json_string)
      real_parse(Yajl::Parser.new.parse(json_string), Names.new)
    end

    # FIXME just a straight translation of crappy code, modulo the
    # case statement.
    def self.real_parse(obj, names)
      case obj
      when String
        schema = names[obj]
        unless schema
          raise SchemaParseError, "Undefined name: #{obj}"
        end
        return schema
      when Hash
        type = obj['type']
        unless type
          raise SchemaParseError, "No type: #{obj}"
        end
        if ['record', 'error', 'enum', 'fixed'].include?(type)
          name = obj['name']
          space = obj['namespace']
          unless name
            raise SchemaParseError, "No name in schema: #{obj}"
          end
          if ['error', 'record'].include?(type)
            fields = OrderedHash.new
            schema = RecordSchema.new(fields, name, space, type == 'error')
            names[name] = schema
            fieldsnode = obj['fields']
            unless fieldsnode
              raise SchemaParseException, "Record has no fields: #{obj}"
            end

            fieldsnode.each do |field|
              fieldname = field['name']
              unless fieldname
                raise SchemaParseException, "No field name: #{field}"
              end
              fieldtype = field['type']
              unless fieldtype
                raise SchemaParseException, "No field type: #{field}"
              end
              defaultval = field['default']
              fields[fieldname] = Field.new(fieldname,
                                            real_parse(fieldtype, names),
                                            defaultval)
            end
            return schema
          elsif type == 'enum'
            symbolsnode = obj['symbols']
            unless symbolsnode && symbolsnode.is_a?(Array)
              raise SchemaParseException, "Enum has no symbols: #{obj}"
            end
            symbols = symbolsnode.dup
            schema = EnumSchema.new(name, space, symbols)
            names[name] = schema
            return schema
          elsif type == 'fixed'
            schema = FixedSchema.new(name, space, obj['size'])
            names[name] = schema
            return schema
          end
        elsif type == 'array'
          return ArraySchema.new(real_parse(obj['items'], names))
        elsif type == 'map'
          return MapSchema.new(real_parse(obj['values'], names))
        else
          raise SchemaParseException, "Type not yet supported: #{type}"
        end
      when Array
        elemtypes = []
        obj.each do |elemtype|
          elemtypes << real_parse(elemtype, names)
        end
        return UnionSchema.new(elemtypes)
      else
        raise SchemaParseException, "Schema not yet support: #{obj}"
      end
    end

    def self.validate(schema, obj)
      validation = VALIDATIONS[schema.gettype]
      validation ? validation.call(schema, obj) : false
    end

    def self.validatearray(schm, object)
      return false unless object.is_a?(Array)
      object.all?{|elem| validate(schm.getelementtype, elem) }
    end

    def self.validatemap(schm, object)
      return false unless object.is_a?(Hash)
      object.all?{|k,v| validate(schm.getvaluetype, v) }
    end
    
    def self.validaterecord(schm, object)
      return false unless object.is_a?(Hash)
      schm.getfields.values.all? do |field|
        validate(field.getschema, object[field.getname])
      end
    end

    def self.validateunion(schm, object)
      schm.getelementtypes.any?{|e| validate(e, object) }
    end

    def initialize(type)
      @type = type
    end

    def gettype; @type; end

    def ==(other, seen=nil)
      other.is_a?(Schema) && @type == other.gettype
    end

    def hash(seen=nil)
      @type.hash
    end

    def to_s(names=Names.new)
      raise NotImplementedError
    end

    class StringSchema < Schema
      def initialize; super(STRING); end
      def to_s(names=Names.new); '"string"'; end
    end

    class BytesSchema < Schema
      def initialize; super(BYTES); end
      def to_s(names=Names.new); '"bytes"'; end
    end
    
    class IntSchema < Schema
      def initialize; super(INT); end
      def to_s(names=Names.new); '"int"'; end
    end

    class LongSchema < Schema
      def initialize; super(LONG); end
      def to_s(names=Names.new); '"long"'; end
    end

    class FloatSchema < Schema
      def initialize; super(FLOAT); end
      def to_s(names=Names.new); '"float"'; end
    end

    class DoubleSchema < Schema
      def initialize; super(DOUBLE); end
      def to_s(names=Names.new); '"double"'; end
    end

    class BooleanSchema < Schema
      def initialize; super(BOOLEAN); end
      def to_s(names=Names.new); '"boolean"'; end
    end

    class NullSchema < Schema
      def initialize; super(NULL); end
      def to_s(names=Names.new); '"null"'; end
    end

    class NamedSchema < Schema
      def initialize(type, name, space)
        super(type)
        @name = name
        @space = space
      end

      def getname; @name; end

      def namestring
        str = '"name": "' + @name + '", '
        if @space
          str << '"namespace": "' << @space << '", '
        end
        str
      end
    end

    class RecordSchema < NamedSchema
      def initialize(fields, name=nil, space=nil, is_error=false)
        super(RECORD, name, space)
        @fields = fields
        @is_error = is_error
      end

      def getfields; @fields; end
      def is_error; @is_error; end

      def to_s(names=Names.new)
        return '"'+getname+'"' if self == names[getname]
        names[getname] = self if getname

        str = '{"type" : "'
        if is_error
          str << 'error'
        else
          str << 'record'
        end
        str << '", ' << namestring << '"fields": ['
        count = 0
        @fields.values.each do |field|
          str << '{"name": "' << field.getname
          str << '", "type": ' << field.getschema.to_s(names)
          if !field.getdefaultvalue.nil?
            str << ', "default": ' << field.getdefaultvalue.to_s(names)
          end
          str << '}'
          count += 1
          if count < @fields.size
            str << ','
          end
        end
        str << ']}'
      end
    end

    class ArraySchema < Schema
      attr_reader :elemtype
      def initialize(elemtype)
        super(ARRAY)
        @elemtype = elemtype
      end

      def getelementtype; @elemtype; end

      def ==(other, seen={})
        other.is_a?(ArraySchema) && @elemtype == other.elemtype
      end

      def to_s(names=Names.new)
        '{"type": "array", "items": '+ @elemtype.to_s(names) + '}'
      end
    end

    class MapSchema < Schema
      def initialize(valuetype)
        super(MAP)
        @vtype = valuetype
      end

      def getvaluetype; @vtype; end

      def to_s(names=Names.new)
        '{"type": "map", "values": ' + @vtype.to_s(names) + '}'
      end
    end

    class UnionSchema < Schema
      def initialize(elemtypes)
        super(UNION)
        @elemtypes = elemtypes
      end

      def getelementtypes; @elemtypes; end

      def to_s(names=Names.new)
        str = '['
        count = 0
        @elemtypes.each do |elemtype|
          str << elemtype.to_s(names)
          count += 1
          if count < @elemtypes.size # FIXME Array#join
            str << ','
          end
        end
        str << ']'
        str
      end
    end

    class EnumSchema < NamedSchema
      def initialize(name, space, symbols)
        super(ENUM, name, space)
        @symbols = symbols
        @ordinals = {}
        @symbols.each_with_index do |sym, i|
          @ordinals[sym] = i
        end
      end

      def getenumsymbols; @symbols; end
      def getenumordinal(symbol); @ordinals[symbol]; end

      def to_s(names=Names.new)
        str = '{"type": "enum", ' + namestring
        str << '"symbols": ['
        count = 0
        unless @symbols.empty?
          str << '"' << @symbols.join('", "') << '"'
        end
        str << ']}'
        str
      end
    end

    class FixedSchema < NamedSchema
      def initialize(name, space, size)
        super(FIXED, name, space)
        @size = size
      end
      def getsize; @size; end
      
      def to_s(names=Names.new)
        return '"'+getname+'"' if names[getname] == self
        names[getname] = self
        str = '{"type": "fixed", ' + namestring
        str << '"size": ' + @size.to_s + '}'
        str
      end
    end
    
    class Field
      def initialize(name, schema, defaultvalue=nil)
        @name = name
        @schema = schema
        @defaultvalue = defaultvalue
      end

      def getschema; @schema; end
      def getdefaultvalue; @defaultvalue; end
      def getname; @name; end
    end
  end


  class AvroError < StandardError; end
  class SchemaParseError < AvroError; end

  PRIMITIVES = {
    'string' => Schema::StringSchema.new,
    'bytes' => Schema::BytesSchema.new,
    'int' => Schema::IntSchema.new,
    'long' => Schema::LongSchema.new,
    'float' => Schema::FloatSchema.new,
    'double' => Schema::DoubleSchema.new,
    'boolean' => Schema::BooleanSchema.new,
    'null' => Schema::NullSchema.new
  }

  class Names < OrderedHash
    def initialize(names=PRIMITIVES)
      super()
      @names = {}
      merge!(names)
      @names = names
    end

    def []=(key, value)
      if @names.has_key?(key)
        raise SchemaParseError, "Can't redefine: #{key}"
      end
      super(key, value)
    end
  end
end
