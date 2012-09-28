#
#This script is slightly modified version of the script found within a great book
#'Seven Databases in Seven Weeks' by Eric Redmond and Jim R. Wilson
#
require 'rubygems'
require 'libxml'
require 'couchrest'

include LibXML

class JamendoCallbacks
  include XML::SaxParser::Callbacks
  def initialize()
    @db = CouchRest.database("http://localhost:5984/ruby_music")
    @startTime = Time.now
    @count = 0
    @max = nil
    @stack = []
    @artist = nil
    @album = nil
    @track = nil
    @tag = nil
    @buffer = nil
  end

  def on_start_element(element, attributes)
    case element
      when 'artist'
        @artist = { :albums => []}
        @stack.push @artist
      when 'album'
        @album = { :tracks => []}
        @artist[:albums].push @album
        @stack.push @album
      when 'track'
        @track = { :tags => []}
        @album[:tracks].push @track
        @stack.push @track
      when 'tag'
        @tag = {}
        @track[:tags].push @tag
        @stack.push @tag
      when 'Artists', 'Albums', 'Tracks', 'Tags'
        #ignore
      else
        @buffer = []
    end
  end

  def on_characters(chars)
    @buffer << chars unless @buffer.nil?
  end

  def on_end_element(element)
    case element
      when 'artist'
        @stack.pop
        @artist['_id'] = @artist['id']
        @artist[:random] = rand
        @db.save_doc(@artist, false, true)
        @count += 1
        if !@max.nil? && @count >= @max
          on_end_document
        end
      when 'album', 'track', 'tag'
        top = @stack.pop
        top[:random] = rand
      when 'Artists', 'Albums', 'Tracks', 'Tags'
        #ignore
      else
        if @stack[-1] && @buffer
          @stack[-1][element] = @buffer.join.force_encoding('utf-8')
          @buffer = nil
        end
    end
  end
  def on_end_document()
    puts "Time elapsed #{Time.now - @startTime} seconds"
    puts "TOTAL #{@count} records inserted!"
    exit(1)
  end
end

class String
  def force_encoding(enc)
    self
  end
end

parser = XML::SaxParser.io(ARGF)
parser.callbacks = JamendoCallbacks.new
parser.parse
