require 'search_config'

module HealthCheck
  class LocalSearchClient
    def initialize(options={})
      @index_name          = options[:index] || "mainstream"
      @index = SearchConfig.new.search_server.index(@index_name)
    end

    def search(term)
      extract_results(@index.search(term))
    end

    def to_s
      "Local search [index=#{@index_name}]"
    end

  private
    def extract_results(result_set)
      result_set.results.map { |r| r.link }
    end
  end
end