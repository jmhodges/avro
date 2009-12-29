module Avro
  module IO
    INT_MIN_VALUE = -(1 << 31)
    INT_MAX_VALUE = (1 << 31) - 1
    LONG_MIN_VALUE = -(1 << 63)
    LONG_MAX_VALUE = (1 << 63) - 1

    class Decoder
      def initialize(reader)
        @reader = reader
      end

      def byte
        @reader.read(1).unpack('c')[0]
      end
      
      def read_boolean
        @reader.read(1) == "1"
      end

      def read_int; read_long; end

      def read_long
        b = byte
        n = b & 0x7F
        shift = 7
        while (b & 0x80) != 0
          b = byte
          n |= (b & 0x7F) << shift
          shift += 7
        end
        (n >> 1) ^ -(n & 1)
      end

      def read_float
        bits = (byte & 0xFF) |
          ((byte & 0xff) <<  8) |
          ((byte & 0xff) << 16) |
          ((byte & 0xff) << 24)
        [bits].pack('i').unpack('e')[0]
      end

      def read_double
        bits = (byte & 0xFF) |
          ((byte & 0xff) <<  8) |
          ((byte & 0xff) << 16) |
          ((byte & 0xff) << 24) |
          ((byte & 0xff) << 32) |
          ((byte & 0xff) << 40) |
          ((byte & 0xff) << 48) |
          ((byte & 0xff) << 56)
        [bits].pack('Q').unpack('d')[0]
      end

      def read_bytes
        read(read_long)
      end

      def read_string
        # FIXME utf-8 encode this in 1.9
        read_bytes
      end

      def read(len)
        @reader.read(len)
      end
    end

    class Encoder
      def initialize(writer)
        @writer = writer
      end

      def write_boolean(datum)
        if datum != true && datum != false
          # FIXME passing Schema::BOOLEAN, which is a fixnum, might be a bug.
          raise AvroTypeError.new(Schema::BOOLEAN, datum)
        end
        @writer.write(datum ? 1 : 0)
      end

      def write_int(n)
        if n < INT_MIN_VALUE || n > INT_MAX_VALUE
          raise AvroTypeError, Schema::INT, n, "datum too big to fit into avro int"
        end
        write_long(n)
      end

      def write_long(n)
        if n < LONG_MIN_VALUE || n > LONG_MAX_VALUE
          raise AvroTypeError, Schema::LONG, n, "datum too big to fit into avro long"
        end
        n = (n << 1) ^ (n >> 63)
        while (n & ~0x7F) != 0
          @writer.write(((n & 0x7f) | 0x80).chr)
          n >>= 7
        end
        @writer.write(n.chr)
      end

      def write_float(datum)
        bits = [datum].pack('e').unpack('i')[0]
        @writer.write(((bits      ) & 0xFF).chr)
        @writer.write(((bits >> 8 ) & 0xFF).chr)
        @writer.write(((bits >> 16) & 0xFF).chr)
        @writer.write(((bits >> 24) & 0xFF).chr)
      end

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

      def write_bytes(datum)
        unless datum.is_a?(String)
          raise AvroTypeError.new(Schema::BYTES, datum, "avro bytes should be a String")
        end
        write_long(datum.size)
        @writer.write(datum)
      end

      def write_string(datum)
        # FIXME utf-8 encode this in 1.9
        unless datum.is_a?(String)
          raise AvroTypeError.new(Schema::BYTES, datum, "avro string should be a String")
        end
        write_bytes(datum)
      end

      def write(datum)
        @writer.write(datum)
      end
    end
  end
end
