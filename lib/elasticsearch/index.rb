require "document"
require "logging"
require "cgi"
require "rest-client"
require "multi_json"
require "json"
require "elasticsearch/advanced_search_query_builder"
require "elasticsearch/client"
require "elasticsearch/escaping"
require "elasticsearch/result_set"
require "elasticsearch/scroll_enumerator"
require "elasticsearch/search_query_builder"

module Elasticsearch
  class Index
    include Elasticsearch::Escaping

    # An enumerator with a manually-specified size.
    # This means we can count the number of documents in an index without
    # having to load them all.
    class SizedEnumerator < Enumerator
      attr_reader :size

      def initialize(size, &block)
        super(&block)
        @size = size
      end
    end

    # The number of documents to insert at once when populating
    def self.populate_batch_size
      50
    end

    # The number of documents to retrieve at once when retrieving all documents
    # Gotcha: this is actually the number of documents per shard, so there will
    # be up to some multiple of this number per page.
    def self.scroll_batch_size
      50
    end

    # How long to hold a scroll cursor open between requests
    # We should be able to keep this low, since these are only for internal use
    SCROLL_TIMEOUT_MINUTES = 1

    # How long to wait between reads when streaming data from the elasticsearch server
    TIMEOUT_SECONDS = 5.0

    # How long to wait for a connection to the elasticsearch server
    OPEN_TIMEOUT_SECONDS = 5.0

    attr_reader :mappings, :index_name

    def initialize(base_uri, index_name, mappings)
      @client = Client.new(
        base_uri + "#{CGI.escape(index_name)}/",
        timeout: TIMEOUT_SECONDS,
        open_timeout: OPEN_TIMEOUT_SECONDS
      )
      @index_name = index_name
      raise ArgumentError, "Missing index_name parameter" unless @index_name
      @mappings = mappings
    end

    def field_names
      @mappings["edition"]["properties"].keys
    end

    def real_name
      alias_info = MultiJson.decode(@client.get("_aliases"))
      # If the index exists, it will return something of the form:
      # { real_name => { "aliases" => { alias => {} } } }
      # If not, it'll return:
      # {}
      alias_info.keys.first
    end

    def exists?
      ! real_name.nil?
    end

    def add(documents)
      if documents.size == 1
        logger.info "Adding #{documents.size} document to #{index_name}"
      else
        logger.info "Adding #{documents.size} documents to #{index_name}"
      end
      documents = documents.map(&:elasticsearch_export).map do |doc|
        index_action(doc).to_json + "\n" + doc.to_json
      end
      # Ensure the request payload ends with a newline
      @client.post("_bulk", documents.join("\n") + "\n", content_type: :json)
    end

    def populate_from(source_index)
      total_indexed = 0
      all_docs = source_index.all_documents
      all_docs.each_slice(self.class.populate_batch_size) do |documents|
        add documents
        total_indexed += documents.length
        logger.info do
          progress = "#{total_indexed}/#{all_docs.size}"
          source_name = source_index.index_name
          "Populated #{progress} from #{source_name} into #{index_name}"
        end
      end

      commit
    end

    def get(link)
      logger.info "Retrieving document with link '#{link}'"
      begin
        response = @client.get("_all/#{CGI.escape(link)}")
      rescue RestClient::ResourceNotFound
        return nil
      end

      document_from_hash(MultiJson.decode(response.body)["_source"])
    end

    def document_from_hash(hash)
      Document.from_hash(hash, @mappings)
    end

    def all_documents
      # Set off a scan query to get back a scroll ID and result count
      search_body = {query: {match_all: {}}}
      batch_size = self.class.scroll_batch_size
      ScrollEnumerator.new(@client, search_body, batch_size) do |hit|
        document_from_hash(hit["_source"])
      end
    end

    def all_document_links(exclude_formats = [])
      search_body = {
        "query" => {
          "bool" => {
            "must_not" => {
              "terms" => {
                "format" => exclude_formats
              }
            }
          }
        },
        "fields" => ["link"]
      }

      batch_size = self.class.scroll_batch_size
      ScrollEnumerator.new(@client, search_body, batch_size) do |hit|
        hit["fields"]["link"]
      end
    end

    # `options` can have the following keys:
    #   :fields - a list of field names to be included in the document, if not
    #             specified, the mappings are used.
    def documents_by_format(format, options = {})
      batch_size = 500
      search_body = {query: {term: {format: format}}}
      if options[:fields]
        search_body.merge!(fields: options[:fields])
        field_names = options[:fields]
        result_key = "fields"
      else
        # Use all field names from the mappings
        # TODO: remove duplication between this and Document.from_hash
        field_names = @mappings["edition"]["properties"].keys.map(&:to_s)
        result_key = "_source"
      end

      ScrollEnumerator.new(@client, search_body, batch_size) do |hit|
        Document.new(field_names, hit[result_key])
      end
    end

    def search(query, organisation=nil)
      builder = SearchQueryBuilder.new(query, organisation)
      # payload = builder.query_hash.to_json

      # Per-format boosting done as a filter, so the results get cached on the
      # server, as they are the same for each query

      boosted_formats = {
        # Mainstream formats
        "smart-answer"  => 1.5,
        "transaction"   => 1.5,
        # Inside Gov formats
        "topical_event" => 1.5,
        "minister"      => 2,
        "organisation"  => 2.5,
        "topic"         => 1.2,
        "document_series" => 1.5,
        "operational_field" => 1.5,
      }

      format_boosts = boosted_formats.map do |format, boost|
        {
          filter: { term: { format: format } },
          boost: boost
        }
      end

      # An implementation of http://wiki.apache.org/solr/FunctionQuery#recip
      # Curve for 2 months: http://www.wolframalpha.com/share/clip?f=d41d8cd98f00b204e9800998ecf8427e5qr62u0si
      #
      # Behaves as a freshness boost for newer documents with a public_timestamp and search_format_types announcement
      time_boost = {
        filter: { term: { search_format_types: "announcement" } },
        script: "((0.05 / ((3.16*pow(10,-11)) * abs(time() - doc['public_timestamp'].date.getMillis()) + 0.05)) + 0.12)"
      }

      query_analyzer = "query_default"

      number_of_words = query.split(' ').length

      fields = ["title", "description", "indexable_content"]
      exact_boost = 1

      payload = {
        from: 0,
        size: 50,
        query: {
          custom_filters_score: {
            query: {
              bool: {
                should: [
                  {
                    bool: {
                      should: [
                        match_phrase: {
                          title: {
                            query: escape(query),
                            analyzer: "query_default",
                          }
                        },
                        match_phrase: {
                          description: {
                            query: escape(query),
                            analyzer: "query_default",
                          }
                        },
                        match_phrase: {
                          indexable_content: {
                            query: escape(query),
                            analyzer: "query_default",
                          }
                        },
                      ],
                      minimum_number_should_match: 1
                    }
                  },
                  {
                    multi_match: {
                      query: escape(query),
                      operator: "and",
                      fields: fields,
                      analyzer: "query_default"
                    }
                  },
                  {
                    multi_match: {
                      query: escape(query),
                      operator: "or",
                      fields: fields,
                      analyzer: "shingled_query_analyzer"
                    }
                  },
                  {
                    match: {
                      _all: {
                        query: escape(query),
                        analyzer: "query_default",
                        # boost: 3.5,
                        minimum_should_match: "1<2 2<3 3<3"
                      }
                    }
                  }
                ],
                minimum_number_should_match: 1
              }
            },
            filters: format_boosts + [ time_boost ]
          }
        }
      }.to_json

      # puts "Here is the payload:\n\n#{payload}\n\n"

      response = @client.get_with_payload("_search", payload)
      ResultSet.new(@mappings, MultiJson.decode(response))
    end

    def advanced_search(params)
      logger.info "params:#{params.inspect}"
      raise "Pagination params are required." if params["per_page"].nil? || params["page"].nil?

      order     = params.delete("order")
      format    = params.delete("format")
      backend   = params.delete("backend")
      keywords  = params.delete("keywords")
      per_page  = params.delete("per_page").to_i
      page      = params.delete("page").to_i

      query_builder = AdvancedSearchQueryBuilder.new(keywords, params, order, @mappings)
      raise query_builder.error unless query_builder.valid?

      starting_index = page <= 1 ? 0 : (per_page * (page - 1))
      payload = {
        "from" => starting_index,
        "size" => per_page
      }

      payload.merge!(query_builder.query_hash)

      logger.debug "Request payload: #{payload.to_json}"
      result = @client.get_with_payload("_search", payload.to_json)
      ResultSet.new(@mappings, MultiJson.decode(result))
    end

    def delete(link)
      begin
        # Can't use a simple delete, because we don't know the type
        @client.delete "_query", params: {q: "link:#{escape(link)}"}
      rescue RestClient::ResourceNotFound
      end
      return true  # For consistency with the Solr API and simple_json_response
    end

    def delete_by_format(format)
      @client.delete_with_payload("_query", {term: {format: format}}.to_json)
    end

    def delete_all
      @client.delete_with_payload("_query", {match_all: {}}.to_json)
      commit
    end

    def commit
      @client.post "_refresh", nil
    end

    private
    def logger
      Logging.logger[self]
    end

    def index_action(doc)
      {"index" => {"_type" => doc["_type"], "_id" => doc["link"]}}
    end
  end
end
