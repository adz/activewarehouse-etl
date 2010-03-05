module ETL
  module Processor
    # This processor is both a valid RowProcessor (called on each row with after_read) or a Processor (called once on pre_process or post_process)
    class BlockProcessor < ETL::Processor::RowProcessor
      def initialize(control, configuration)
        super
        @block = configuration[:block]
        @config = configuration[:config]
      end
      def process(row=nil)
        if @block.arity == 2
          @block.call(row, @config)
        else
          @block.call(row)
        end
      end
    end
  end
end
