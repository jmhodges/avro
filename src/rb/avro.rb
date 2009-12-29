#!/usr/bin/env ruby
require 'yajl'
require 'avro/ordered_hash'
require 'avro/schema'
require 'avro/generic_io'
require 'avro/io'

module Avro
  VERSION = "FIXME"
  class AvroTypeError < Avro::AvroError
    def initialize(schm=nil, datum=nil, msg=nil)
      msg ||= "Not a #{schm.to_s}: #{datum}"
      super(msg)
    end
  end
end
