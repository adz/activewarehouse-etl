require 'digest/sha1'

module ETL
  module Processor

    # A row level processor to fill more values in the row from a source
    class FillRowProcessor < ETL::Processor::RowProcessor
      # The values hash which maps retrieved column names to row values to populate
      attr_accessor :values

      # The match hash which maps existing row values to column names for selection criteria
      attr_accessor :match

      # The target database (for lookup)
      attr_accessor :target

      # The target table (for lookup)
      attr_accessor :table

      # The database connection
      attr_accessor :connection

      # Initialize the row processor
      #
      # Configuration options:
      # <tt>:values</tt>: The values to overwrite or inject into the row
      # This is a Hash of :key => :column pairs that match the retrieved
      # columns to the row keys
      # <tt>:match</tt>: The row values to use to match the appropriate column
      # This is a Hash of :key => :column pairs to use to generate a where clause
      # <tt>:target</tt>: The target database
      # <tt>:table</tt>: The target table
      # <tt>:overwrite</tt>: Whether or not to overwrite values if they preexist
      # in the row (uses blank? rules).  Defaults to TRUE
      # <tt>:use_first</tt>: Whether or not to use the first returned value if
      # multiple rows are returned.  Defaults to FALSE.  Will raise error if 
      # FALSE and multiple rows are found
      # <tt>:use_cache</tt>: Whether or not to cache the results as they are found.  Defaults to true.
      # <tt>:preload_cache</tt>: Whether or not to preload the cache.  Defaults to true.
      #
      def initialize(control, configuration)
        super

        @values = configuration[:values] || raise(ETL::ControlError, ":values must be specified")
        @match  = configuration[:match] || raise(ETL::ControlError, ":match must be specified")
        @target = configuration[:target] || raise(ETL::ControlError, ":target must be specified")
        @table  = configuration[:table] || raise(ETL::ControlError, ":table must be specified")
        @use_first = configuration[:use_first] || false
        @overwrite = configuration[:overwrite] === false ? false : true
        @use_cache = configuration[:use_cache] === false ? false : true

        @connection = ETL::Engine.connection(target)

        preload_cache unless configuration[:preload_cache] === false
      end

      # The cache store
      def cache
        @cache ||= Hash.new
      end

      def cache_key(values, side = :left)
        Digest::SHA1.hexdigest(
          match.send(side == :left ? :keys : :values).collect { |i|
            # Db keys will come back as strings, whereas etl rows are keyed on symbols
            side == :left || i = i.to_s
            values[i]
          }.to_s
        )
      end

      # Whether or not to overwrite existing values in the row (uses blank? rules if false)
      def overwrite?(existing, update)
        case
        when @overwrite
          update.blank? ? false : true
        else
          existing.blank? ? true : false
        end
      end

      def preload_cache
        query = select_stmt
        connection.select_all(query).each do |v|
          key = cache_key(v, :right)
          if cache[key] and not use_first?
            raise TooManyResultsError, "Too many results found (and use_first not set) using the following value: #{v}"
          end

          next if cache[key] and use_first?
          cache[key] = v
        end
        ETL::Engine.logger.debug("Cache looks like: #{cache.inspect}")
      end

      # Whether or not to cache the results as they are found
      def use_cache?
        @use_cache
      end

      # Whether or not to use the first returned row if there are multiple values returned.
      # If not true, then any multiple row return will result in an error
      def use_first?
        @use_first
      end

      # Process the row and modify it as appropriate
      def process(row)
        value = cache[cache_key(row)]
        if not @preload_cache and not value
          query = select_stmt + conditions(row)
          ETL::Engine.logger.debug("Looking up row using query: #{query}")

          value = connection.select_all(query)
          if value.length > 1 and not use_first?
            raise TooManyResultsError, "Too many results found (and use_first not set) using the following query: #{q}"
          end
          value = value.first
          cache[cache_key(row)] = value
        end

        if value.nil? or value.empty?
          ETL::Engine.logger.debug("Unable to find FillRow match for row: #{row.inspect}")
        else
          value.each_pair do |key, col_value|
            row[key.to_sym] = col_value if overwrite?(row[key.to_sym], col_value)
          end
        end
        row
      end

      private

      def select_stmt
        @select_stmt ||= "SELECT #{values.to_a.collect { |a| a.each(&:to_s).join(' ') }.join(', ')}, #{match.values.collect { |a| a.to_s}.join(', ')} FROM #{table.to_s}"
        #@select_stmt ||= "SELECT #{values.to_a.collect { |a| a.each(&:to_s).join(' ') }.join(', ')} FROM #{table.to_s}"
      end

      def conditions(r)
        " WHERE " +
          match.each_pair.collect { |key, column|
            "#{column.to_s} = #{connection.quote(r[key])}"
          }.join(" AND ")
      end
    end
  end
end

class TooManyResultsError < StandardError; end
