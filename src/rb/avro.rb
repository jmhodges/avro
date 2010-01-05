#!/usr/bin/env ruby
require 'yajl'
require 'set'
require 'md5'

module Avro
  VERSION = "FIXME"

  class AvroError < StandardError; end

  class AvroTypeError < Avro::AvroError
    def initialize(schm=nil, datum=nil, msg=nil)
      msg ||= "Not a #{schm.to_s}: #{datum}"
      super(msg)
    end
  end
end

require 'avro/collect_hash'
require 'avro/schema'
require 'avro/io'
require 'avro/data_file'
require 'avro/protocol'
