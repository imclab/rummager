require "elasticsearch/escaping"

module Elasticsearch
  class SearchQueryBuilder
    include Elasticsearch::Escaping

    QUERY_ANALYZER = "query_default"

    def initialize(query, organisation=nil)
      @query = query
      @organisation = organisation
    end

    def query_hash
      must_conditions = [
        {
          match: {
            _all: {
              query: escape(@query),
              analyzer: QUERY_ANALYZER,
              minimum_should_match: "3<3 7<50%"
            }
          }
        }
      ]
      if @organisation
        must_conditions << {
          term: {
            organisations: @organisation
          }
        }
      end
      {
        from: 0, size: 50,
        query: {
          custom_filters_score: {
            query: {
              bool: {
                must: must_conditions,
                should: exact_field_boosts + [ exact_match_boost, shingle_token_filter_boost ],
                minimum_number_should_match: 1
              }
            },
            filters: format_boosts + [time_boost]
          }
        }
      }
    end

  private
    def match_fields
      {
        "title" => 5,
        "description" => 2,
        "indexable_content" => 1,
      }
    end

    # "driving theory test" => ["driving theory", "theory test"]
    def shingles
      @query.split.each_cons(2).map { |s| s.join(' ') }
    end

    def shingle_boosts
      shingles.map do |shingle|
        match_fields.map do |field_name, _|
          {
            text: {
              field_name => {
                query: shingle,
                type: "phrase",
                boost: 2,
                analyzer: QUERY_ANALYZER
              },
            }
          }
        end
      end
    end

    def shingle_token_filter_boost
      {
        multi_match: {
          query: escape(@query),
          operator: "or",
          fields: match_fields.keys,
          analyzer: "shingled_query_analyzer"
        }
      }
    end

    def exact_field_boosts
      match_fields.map {|field_name, _|
        {
          match_phrase: {
            field_name => {
              query: escape(@query),
              analyzer: QUERY_ANALYZER,
            }
          }
        }
      }
    end

    def exact_match_boost
      {
        multi_match: {
          query: escape(@query),
          operator: "and",
          fields: match_fields.keys,
          analyzer: QUERY_ANALYZER
        }
      }
    end

    def boosted_formats
      {
        # Mainstream formats
        "smart-answer"      => 1.5,
        "transaction"       => 1.5,
        # Inside Gov formats
        "topical_event"     => 1.5,
        "minister"          => 1.7,
        "organisation"      => 2.5,
        "topic"             => 1.5,
        "document_series"   => 1.3,
        "operational_field" => 1.5,
      }
    end

    def format_boosts
      boosted_formats.map do |format, boost|
        {
          filter: { term: { format: format } },
          boost: boost
        }
      end
    end

    # An implementation of http://wiki.apache.org/solr/FunctionQuery#recip
    # Curve for 2 months: http://www.wolframalpha.com/share/clip?f=d41d8cd98f00b204e9800998ecf8427e5qr62u0si
    #
    # Behaves as a freshness boost for newer documents with a public_timestamp and search_format_types announcement
    def time_boost
      {
        filter: { term: { search_format_types: "announcement" } },
        script: "((0.05 / ((3.16*pow(10,-11)) * abs(time() - doc['public_timestamp'].date.getMillis()) + 0.05)) + 0.12)"
      }
    end
  end
end
