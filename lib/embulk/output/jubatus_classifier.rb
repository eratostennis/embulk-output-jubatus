module Embulk
  class JubatusClassifierOutputPlugin < OutputPlugin
    require 'jubatus/classifier/client'

    Plugin.register_output('jubatus_classifier', self)

    def self.transaction(config, schema, processor_count, &control)
      task = { 
        'host' => config.param('host', :string, :default => 'localhost'),
        'port' => config.param('port', :integer, :default => 9199),
        'name' => config.param('name', :string, :default => 'test'),
      }   

      puts "Jubatus classfier output started."
      yield(task)
      puts "Jubatus classifier output finished."

      return {}
    end 

    def initialize(task, schema, index)
      super
      @juba = ::Jubatus::Classifier::Client::Classifier.new(task['host'], task['port'], task['name'])
    end 

    def close
    end 

    def add(page)
      train_data = Array.new
      page.each do |record|
        key = record.shift
        hash = Hash[record]
        train_data.push([key, ::Jubatus::Common::Datum.new(hash)])
      end 
      train_data.sort_by{rand}
      @juba.train(train_data)
    end 

    def finish
    end 

    def abort
    end 

    def commit
      {}  
    end 
  end 
end
