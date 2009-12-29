require 'test/unit'
require 'avro'
require 'stringio'

class RandomData
  def initialize(schm, seed=nil)
    srand(seed) if seed
    @seed = seed
    @schm = schm
  end

  def next
    nextdata(@schm)
  end

  def nextdata(schm, d=0)
    case schm.gettype
    when Avro::Schema::BOOLEAN
      rand > 0.5
    when Avro::Schema::STRING
      randstr()
    when Avro::Schema::INT
      rand(Avro::IO::INT_MAX_VALUE - Avro::IO::INT_MIN_VALUE) + Avro::IO::INT_MIN_VALUE
    when Avro::Schema::LONG
      rand(Avro::IO::LONG_MAX_VALUE - Avro::IO::LONG_MIN_VALUE) + Avro::IO::LONG_MIN_VALUE
    when Avro::Schema::FLOAT
      (-1024 + 2048 * rand).round.to_f
    when Avro::Schema::DOUBLE
      Avro::IO::LONG_MIN_VALUE + (Avro::IO::LONG_MAX_VALUE - Avro::IO::LONG_MIN_VALUE) * rand
    when Avro::Schema::BYTES
      randstr(BYTEPOOL)
    when Avro::Schema::NULL
      nil
    when Avro::Schema::ARRAY
      arr = []
      len = rand(5) + 2 - d
      len = 0 if len < 0
      len.times{ arr << nextdata(schm.getelementtype, d+1) }
      arr
    when Avro::Schema::MAP
      map = {}
      len = rand(5) + 2 - d
      len = 0 if len < 0
      len.times do
        map[nextdata(Avro::Schema::StringSchema.new)] = nextdata(schm.getvaluetype, d+1)
      end
      map
    when Avro::Schema::RECORD
      m = {}
      schm.getfields.values.each do |field|
        m[field.getname] = nextdata(field.getschema, d+1)
      end
      m
    when Avro::Schema::UNION
      types = schm.getelementtypes
      nextdata(types[rand(types.size)], d)
    when Avro::Schema::ENUM
      symbols = schm.getenumsymbols
      len = symbols.size
      return nil if len == 0
      symbols[rand(len)]
    when Avro::Schema::FIXED
      BYTEPOOL[rand(BYTEPOOL.size), 1]
    end
  end

  CHARPOOL = 'abcdefghjkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789'
  BYTEPOOL = '12345abcd'

  def randstr(chars=CHARPOOL, length=20)
    str = ''
    rand(length+1).times { str << chars[rand(chars.size)] }
    str
  end
end
