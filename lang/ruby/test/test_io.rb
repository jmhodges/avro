# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'test_help'

class TestIO < Test::Unit::TestCase
  DATAFILE = 'tmp/test.rb.avro'
  Schema = Avro::Schema

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

  def check_default(schema_json, default_json, default_value)
    check(schema_json)
    actual_schema = '{"type": "record", "name": "Foo", "fields": []}'
    actual = Avro::Schema.parse(actual_schema)

    expected_schema = <<EOS
      {"type": "record",
       "name": "Foo",
       "fields": [{"name": "f", "type": #{schema_json}, "default": #{default_json}}]}
EOS
    expected = Avro::Schema.parse(expected_schema)

    reader = Avro::IO::DatumReader.new(actual, expected)
    record = reader.read(Avro::IO::BinaryDecoder.new(StringIO.new))
    assert_equal default_value, record["f"]
  end

  def check(str)
    # parse schema, then convert back to string
    schema = Avro::Schema.parse str

    parsed_string = schema.to_s

     # test that the round-trip didn't mess up anything
    # NB: I don't think we should do this. Why enforce ordering?
    assert_equal(Yajl.load(str),
                  Yajl.load(parsed_string))

    # test __eq__
    assert_equal(schema, Avro::Schema.parse(str))

    # test hashcode doesn't generate infinite recursion
    schema.hash

    # test serialization of random data
    randomdata = RandomData.new(schema)
    9.times { checkser(schema, randomdata) }

    # test writing of data to file
    check_datafile(schema)
  end

  def checkser(schm, randomdata)
    datum = randomdata.next
    assert validate(schm, datum)
    w = Avro::IO::DatumWriter.new(schm)
    writer = StringIO.new "", "w"
    w.write(datum, Avro::IO::BinaryEncoder.new(writer))
    r = datum_reader(schm)
    reader = StringIO.new(writer.string)
    ob = r.read(Avro::IO::BinaryDecoder.new(reader))
    assert_equal(datum, ob) # FIXME check on assertdata conditional
  end

  def check_datafile(schm)
    seed = 0
    count = 10
    random_data = RandomData.new(schm, seed)

   
    f = File.open(DATAFILE, 'wb')
    dw = Avro::DataFile::Writer.new(f, datum_writer(schm), schm)
    count.times{ dw << random_data.next }
    dw.close

    random_data = RandomData.new(schm, seed)


    f = File.open(DATAFILE, 'r+')
    dr = Avro::DataFile::Reader.new(f, datum_reader(schm))

    last_index = nil
    dr.each_with_index do |data, c|
      last_index = c
      # FIXME assertdata conditional
      assert_equal(random_data.next, data)
    end
    dr.close
    assert_equal count, last_index+1
  end

  def validate(schm, datum)
    Avro::Schema.validate(schm, datum)
  end

  def datum_writer(schm)
    Avro::IO::DatumWriter.new(schm)
  end

  def datum_reader(schm)
    Avro::IO::DatumReader.new(schm)
  end
end
