module ETL #:nodoc:
  module Transform #:nodoc:
    # Transform a String representation of a date to a Time instance
    class StringToTimeTransform < ETL::Transform::Transform
      # Transform the value using Time.parse
      def transform(name, value, row)
        begin
          Time.parse(value) unless value.nil?
        rescue
          puts "Invalid time found: #{value}"
          raise
        end
      end
    end
  end
end
