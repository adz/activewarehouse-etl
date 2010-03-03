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
      #
      def initialize(control, configuration)
        super

        @values = configuration[:values] || raise(ETL::ControlError, ":values must be specified")
        @match  = configuration[:match] || raise(ETL::ControlError, ":match must be specified")
        @target = configuration[:target] || raise(ETL::ControlError, ":target must be specified")
        @table  = configuration[:table] || raise(ETL::ControlError, ":table must be specified")
        @use_first = ( configuration[:use_first] || false )
        @overwrite = ( configuration[:overwrite] || true )

        @connection = ETL::Engine.connection(target)
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

      # Whether or not to use the first returned row if there are multiple values returned.
      # If not true, then any multiple row return will result in an error
      def use_first?
        @use_first
      end

      # Process the row and modify it as appropriate
      def process(row)
        conditions = []
        match.each_pair do |key, column|
          conditions << "#{column.to_s} = #{connection.quote(row[key])}"
        end
        q = "SELECT #{values.to_a.collect { |a| a.each(&:to_s).join(' ') }.join(', ')} FROM #{table.to_s} WHERE "
        q << conditions.join(' AND ')

        ETL::Engine.logger.debug("Looking up row using query: #{q}")

        value = connection.select_all(q)
        if value.length > 1 and not use_first?
          raise TooManyResultsError, "Too many results found (and use_first not set) using the following query: #{q}"
        end

        if value.empty?
          ETL::Engine.logger.info("Unable to find FillRow match for row: #{row.inspect}")
        else
          value.first.each_pair do |key, col_value|
            ETL::Engine.logger.debug("Before update: #{row.inspect}")
            row[key.to_sym] = col_value if overwrite?(row[key.to_sym], col_value)
            ETL::Engine.logger.debug("After update: #{row.inspect}")
          end
        end
        row
      end
    end
  end
end

class TooManyResultsError < StandardError; end
