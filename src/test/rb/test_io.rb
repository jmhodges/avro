require 'test_help'

class TestIO < Test::Unit::TestCase
  def test_null
    check_default('"null"', "null", nil)
  end

  def test_boolean
    check_default('"boolean"', "true", true)
    check_default('"boolean"', "false", false)
  end

  def test_string
    check_default('"string"', '"foo"', "foo")
  end

  def test_bytes
    check_default('"bytes"', '"foo"', "foo")
  end

  def test_int
    check_default('"int"', "5", 5)
  end

  def test_long
    check_default('"long"', "9", 9)
  end

  def test_float
    check_default('"float"', "1.2", 1.2)
  end

  def test_double
    check_default('"double"', "1.2", 1.2)
  end

  def test_array
    array_schema = '{"type": "array", "items": "long"}'
    check_default(array_schema, "[1]", [1])
  end

  def test_map
    map_schema = '{"type": "map", "values": "long"}'
    check_default(map_schema, '{"a": 1}', {"a" => 1})
  end

  def test_record
    record_schema = <<EOS
      {"type": "record",
       "name": "Test",
       "fields": [{"name": "f",
                   "type": "long"}]}
EOS
    check_default(record_schema, '{"f": 11}', {"f" => 11})
  end

  def test_enum
    enum_schema = '{"type": "enum", "name": "Test","symbols": ["A", "B"]}'
    check_default(enum_schema, '"B"', "B")
  end

  def test_recursive
    recursive_schema = <<EOS
      {"type": "record",
       "name": "Node",
       "fields": [{"name": "label", "type": "string"},
                  {"name": "children",
                   "type": {"type": "array", "items": "Node"}}]}
EOS
    check(recursive_schema)
  end

  def test_union
    union_schema = <<EOS
      ["string",
       "null",
       "long",
       {"type": "record",
        "name": "Cons",
        "fields": [{"name": "car", "type": "string"},
                   {"name": "cdr", "type": "string"}]}]
EOS
    check(union_schema)
    check_default('["double", "long"]', "1.1", 1.1)
  end

  def test_lisp
    lisp_schema = <<EOS
      {"type": "record",
       "name": "Lisp",
       "fields": [{"name": "value",
                   "type": ["null", "string",
                            {"type": "record",
                             "name": "Cons",
                             "fields": [{"name": "car", "type": "Lisp"},
                                        {"name": "cdr", "type": "Lisp"}]}]}]}
EOS
    check(lisp_schema)
  end

  def test_fixed
    fixed_schema = '{"type": "fixed", "name": "Test", "size": 1}'
    check_default(fixed_schema, '"a"', "a")
  end

  private

  def check_default(schemajson, defaultjson, defaultvalue)
    check(schemajson)
    actual_schema = '{"type": "record", "name": "Foo", "fields": []}'
    actual = Avro::Schema.parse(actual_schema)

    expected_schema = <<EOS
      {"type": "record",
       "name": "Foo",
       "fields": [{"name": "f", "type": #{schemajson}, "default": #{defaultjson}}]}
EOS
    expected = Avro::Schema.parse(expected_schema)

    reader = Avro::GenericIO::DatumReader.new(actual, expected)
    record = reader.read(Avro::IO::Decoder.new(StringIO.new))
    assert_equal defaultvalue, record["f"]
  end

  def check(str)
    # parse schema, then convert back to string
    schema = Avro::Schema.parse str

    parsed_string = schema.to_s

     # test that the round-trip didn't mess up anything
    # NB: I don't think we should do this. Why enforce ordering?
    assert_equal(str.gsub(/\s+/,''),
                  parsed_string.gsub(/\s+/, ''))

    # test __eq__
    assert_equal(schema, Avro::Schema.parse(str))

    # test hashcode doesn't generate infinite recursion
    schema.hash

    # test serialization of random data
    randomdata = RandomData.new(schema)
    9.times { checkser(schema, randomdata) }

    # test writing of data to file
    # checkdatafile(schema) # FIXME checkdatafile not written
  end

  def checkser(schm, randomdata)
    datum = randomdata.next
    assert validate(schm, datum)
    w = datumwriter(schm)
    writer = StringIO.new "", "w"
    w.write(datum, Avro::IO::Encoder.new(writer))
    r = datumreader(schm)
    reader = StringIO.new(writer.string)
    ob = r.read(Avro::IO::Decoder.new(reader))
    assert_equal(datum, ob) # FIXME check on assertdata conditional
  end

  def validate(schm, datum)
    Avro::Schema.validate(schm, datum)
  end

  def datumwriter(schm)
    Avro::GenericIO::DatumWriter.new(schm)
  end

  def datumreader(schm)
    Avro::GenericIO::DatumReader.new(schm)
  end
end
