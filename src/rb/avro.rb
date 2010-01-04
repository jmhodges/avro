#!/usr/bin/env ruby
require 'yajl'
require 'openssl'
require 'set'
require 'avro/schema'
require 'avro/io'
require 'avro/data_file'

module Avro
  VERSION = "FIXME"
  class AvroTypeError < Avro::AvroError
    def initialize(schm=nil, datum=nil, msg=nil)
      msg ||= "Not a #{schm.to_s}: #{datum}"
      super(msg)
    end
  end
end
