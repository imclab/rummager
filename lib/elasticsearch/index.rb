require "document"
require "section"
require "logger"
require "cgi"
require "rest-client"
require "multi_json"
require "json"
require "elasticsearch/advanced_search_query_builder"

module Elasticsearch
  class Index

    POPULATE_BATCH_SIZE = 50

    # We need to provide a limit to queries: if we want everything, just use this
    # This number is big enough that it vastly exceeds the number of items we're
    # indexing, but not so big as to trigger strange behaviour (internal errors)
    # in elasticsearch
    MASSIVE_NUMBER = 200_000

    attr_reader :mappings, :index_name

    class Client

      attr_reader :index_name  # The admin wrapper needs to get to this

      # Sub-paths almost certainly shouldn't start with leading slashes,
      # since this will make the request relative to the server root
      SAFE_ABSOLUTE_PATHS = ["/_bulk", "/_status", "/_cluster/health"]

      def initialize(base_uri, index_name, logger = nil)
        @index_uri = base_uri + "#{CGI.escape(index_name)}/"
        @index_name = index_name

        @logger = logger || Logger.new("/dev/null")
      end

      def recording_elastic_error(&block)
        yield
      rescue Errno::ECONNREFUSED, Timeout::Error, SocketError
        Rummager.statsd.increment("elasticsearcherror")
        raise
      end

      def logging_exception_body(&block)
        yield
      rescue RestClient::InternalServerError => error
        @logger.error(
          "Internal server error in elasticsearch. " +
          "Response: #{error.http_body}"
        )
        raise
      end

      def request(method, sub_path, payload)
        recording_elastic_error do
          logging_exception_body do
            RestClient::Request.execute(
              method: method,
              url:  url_for(sub_path),
              payload: payload,
              headers: {content_type: "application/json"}
            )
          end
        end
      end

      # Forward on HTTP request methods, intercepting and resolving URLs
      [:get, :post, :put, :head, :delete].each do |method_name|
        define_method method_name do |sub_path, *args|
          full_url = url_for(sub_path)
          @logger.debug "Sending #{method_name.upcase} request to #{full_url}"
          args.each_with_index do |argument, index|
            @logger.debug "Argument #{index + 1}: #{argument.inspect}"
          end
          recording_elastic_error do
            logging_exception_body do
              RestClient.send(method_name, url_for(sub_path), *args)
            end
          end
        end
      end

    private
      def url_for(sub_path)
        if sub_path.start_with? "/"
          path_without_query = sub_path.split("?")[0]
          unless SAFE_ABSOLUTE_PATHS.include? path_without_query
            @logger.error "Request sub-path '#{sub_path}' has a leading slash"
            raise ArgumentError, "Only whitelisted absolute paths are allowed"
          end
        end

        # Addition on URLs does relative resolution
        (@index_uri + sub_path).to_s
      end
    end

    def initialize(base_uri, index_name, mappings, logger = nil)
      @client = Client.new(base_uri, index_name, logger)
      @index_name = index_name
      raise ArgumentError, "Missing index_name parameter" unless @index_name
      @mappings = mappings
      @logger = logger || Logger.new("/dev/null")
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
      @logger.info "Adding #{documents.size} document(s) to elasticsearch"
      documents = documents.map(&:elasticsearch_export).map do |doc|
        index_action(doc).to_json + "\n" + doc.to_json
      end
      # Ensure the request payload ends with a newline
      @client.post("_bulk", documents.join("\n") + "\n", content_type: :json)
    end

    def populate_from(source_index)
      total_indexed = 0
      # This will load the entire content of the search index into memory at
      # once, which isn't yet a big deal but may become a problem as the search
      # index grows. One alternative could be to use elasticsearch scan queries
      # <http://www.elasticsearch.org/guide/reference/api/search/search-type.html>
      all_docs = source_index.all_documents
      all_docs.each_slice(POPULATE_BATCH_SIZE) do |documents|
        add documents
        total_indexed += documents.length
        @logger.info "Populated #{total_indexed} of #{all_docs.size}"
      end

      commit
    end

    def get(link)
      @logger.info "Retrieving document with link '#{link}'"
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

    def all_documents(options={})
      limit = options.fetch(:limit, MASSIVE_NUMBER)
      search_body = {query: {match_all: {}}, size: limit}
      result = @client.request(:get, "_search", search_body.to_json)
      result = MultiJson.decode(result)
      result["hits"]["hits"].map { |hit|
        document_from_hash(hit["_source"])
      }
    end

    def search(query)
      # Per-format boosting done as a filter, so the results get cached on the
      # server, as they are the same for each query

      boosted_formats = {
        # Mainstream formats
        "smart-answer"  => 1.5,
        "transaction"   => 1.5,
        # Inside Gov formats
        "topical_event" => 1.5,
        "minister"      => 1.7,
        "organisation"  => 2.5,
        "topic"         => 1.5,
        "document_series" => 1.3,
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

      match_fields = {
        "title" => 5,
        "description" => 2,
        "indexable_content" => 1,
      }

      # "driving theory test" => ["driving theory", "theory test"]
      shingles = query.split.each_cons(2).map { |s| s.join(' ') }

      # These boosts will be different on each query, so there's no benefit to
      # caching them in a filter
      shingle_boosts = shingles.map do |shingle|
        match_fields.map do |field_name, _|
          {
            text: {
              field_name => {
                query: shingle,
                type: "phrase",
                boost: 2,
                analyzer: query_analyzer
              },
            }
          }
        end
      end

      query_boosts = shingle_boosts

      payload = {
        from: 0, size: 50,
        query: {
          custom_filters_score: {
            query: {
              bool: {
                must: {
                  query_string: {
                    fields: match_fields.map { |name, boost|
                      boost == 1 ? name : "#{name}^#{boost}"
                    },
                    query: escape(query),
                    analyzer: query_analyzer
                  }
                },
                should: query_boosts
              }
            },
            filters: format_boosts + [time_boost]
          }
        }
      }.to_json

      # RestClient does not allow a payload with a GET request
      # so we have to call @client.request directly.
      @logger.debug "Request payload: #{payload}"
      result = @client.request(:get, "_search", payload)
      result = MultiJson.decode(result)
      result["hits"]["hits"].map { |hit|
        document_from_hash(hit["_source"].merge("es_score" => hit["_score"]))
      }
    end

    def advanced_search(params)
      @logger.info "params:#{params.inspect}"
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

      @logger.info "Request payload: #{payload.to_json}"

      # RestClient does not allow a payload with a GET request
      # so we have to call @client.request directly.
      result = @client.request(:get, "_search", payload.to_json)
      result = MultiJson.decode(result)
      {
        total: result["hits"]["total"],
        results: result["hits"]["hits"].map { |hit|
          document_from_hash(hit["_source"].merge("es_score" => hit["_score"]))
        }
      }
    end

    LUCENE_SPECIAL_CHARACTERS = Regexp.new("(" + %w[
      + - && || ! ( ) { } [ ] ^ " ~ * ? : \\
    ].map { |s| Regexp.escape(s) }.join("|") + ")")

    LUCENE_BOOLEANS = /\b(AND|OR|NOT)\b/

    def escape(s)
      # 6 slashes =>
      #  ruby reads it as 3 backslashes =>
      #    the first 2 =>
      #      go into the regex engine which reads it as a single literal backslash
      #    the last one combined with the "1" to insert the first match group
      special_chars_escaped = s.gsub(LUCENE_SPECIAL_CHARACTERS, '\\\\\1')

      # Map something like 'fish AND chips' to 'fish "AND" chips', to avoid
      # Lucene trying to parse it as a query conjunction
      special_chars_escaped.gsub(LUCENE_BOOLEANS, '"\1"')
    end

    def facet(field_name)
      # Return a list of Section objects for each section with content
      unless field_name == "section"
        raise ArgumentError, "Faceting is only available on sections"
      end

      _facet(field_name).map { |term_info|
        Section.new(term_info["term"])
      }
    end

    def section(section_slug)
      # RestClient does not allow a payload with a GET request
      # so we have to call @client.request directly.
      payload = {
          from: 0, size: 50,
          query: {
            term: { section: section_slug }
          }
      }.to_json
      result = @client.request(:get, "_search", payload)
      result = MultiJson.decode(result)
      result["hits"]["hits"].map { |hit|
        document_from_hash(hit["_source"])
      }
    end

    def formats
      _facet "format"
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
      @client.request :delete, "_query", {term: {format: format}}.to_json
    end

    def delete_all
      @client.request :delete, "_query", {match_all: {}}.to_json
      commit
    end

    def commit
      @client.post "_refresh", nil
    end

    private
    def index_action(doc)
      {"index" => {"_type" => doc["_type"], "_id" => doc["link"]}}
    end

    def _facet(facet_name)
      # Each entry in the array returned is of the format:
      #
      #   { "term" => "mushroom", "count" => 57000 }
      payload = {
        query: {match_all: {}},
        size: 0,  # We only need facet information: no point returning results
        facets: {
          facet_name => {
            terms: {field: facet_name, size: 100, order: "term"},
            global: true
          }
        }
      }.to_json
      result = MultiJson.decode(@client.request(:get, "_search", payload))
      result["facets"][facet_name]["terms"]
    end
  end
end
