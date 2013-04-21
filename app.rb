%w[ lib ].each do |path|
  $:.unshift path unless $:.include?(path)
end

require "sinatra"
require "multi_json"
require "csv"
require "statsd"

require "document"
require "elasticsearch/index"
require "elasticsearch/search_server"

require_relative "config"
require_relative "helpers"

class Rummager < Sinatra::Application
  def self.statsd
    @@statsd ||= Statsd.new("localhost").tap do |c|
      c.namespace = ENV["GOVUK_STATSD_PREFIX"].to_s
    end
  end

  def search_server
    settings.search_config.search_server
  end

  def current_index
    index_name = params["index"] || settings.default_index_name
    search_server.index(index_name)
  rescue Elasticsearch::NoSuchIndex
    halt(404)
  end

  def indices_for_sitemap
    settings.search_config.index_names.map do |index_name|
      search_server.index(index_name)
    end
  end

  def text_error(content)
    halt 403, {"Content-Type" => "text/plain"}, content
  end

  def json_only
    unless [nil, "json"].include? params[:format]
      expires 86400, :public
      halt 404
    end
  end

  helpers do
    include Helpers
  end

  before do
    content_type :json
  end

  # /index_name/search?q=pie to search a named index
  # /search?q=pie to search the primary index
  get "/?:index?/search.?:format?" do
    json_only

    query = params["q"].to_s.gsub(/[\u{0}-\u{1f}]/, "").strip

    if query == ""
      expires 3600, :public
      halt 404
    end

    expires 3600, :public if query.length < 20

    results = current_index.search(query)

    MultiJson.encode(results.map { |r| r.to_hash.merge(
      highlight: r.highlight,
      presentation_format: r.presentation_format,
      humanized_format: r.humanized_format
    )})
  end

  get "/:index/advanced_search.?:format?" do
    json_only

    results = current_index.advanced_search(request.params)
    MultiJson.encode({
      total: results[:total],
      results: results[:results].map { |r| r.to_hash.merge(
        highlight: r.highlight,
        presentation_format: r.presentation_format,
        humanized_format: r.humanized_format
      )}
    })
  end

  get "/sitemap.xml" do
    content_type :xml
    expires 86400, :public
    # Site maps can have up to 50,000 links in them.
    # We use one for / so we can have up to 49,999 others.
    documents = indices_for_sitemap.flat_map do |index|
      index.all_documents.take(49_999)
    end

    builder do |xml|
      xml.instruct!
      xml.urlset(xmlns: "http://www.sitemaps.org/schemas/sitemap/0.9") do
        xml.url do
          xml.loc "#{base_url}#{"/"}"
        end
        documents.each do |document|
          unless [settings.inside_government_link, settings.recommended_format].include?(document.format)
            xml.url do
              url = document.link
              url = "#{base_url}#{url}" if url =~ /^\//
              xml.loc url
            end
          end
        end
      end
    end
  end

  post "/?:index?/documents" do
    request.body.rewind
    documents = [MultiJson.decode(request.body.read)].flatten.map { |hash|
      current_index.document_from_hash(hash)
    }

    simple_json_result(current_index.add(documents))
  end

  post "/?:index?/commit" do
    simple_json_result(current_index.commit)
  end

  get "/?:index?/documents/*" do
    document = current_index.get(params["splat"].first)
    halt 404 unless document

    MultiJson.encode document.to_hash
  end

  delete "/?:index?/documents/*" do
    simple_json_result(current_index.delete(params["splat"].first))
  end

  post "/?:index?/documents/*" do
    unless request.form_data?
      halt(
        415,
        {"Content-Type" => "text/plain"},
        "Amendments require application/x-www-form-urlencoded data"
      )
    end
    document = current_index.get(params["splat"].first)
    halt 404 unless document
    text_error "Cannot change document links" if request.POST.include? "link"

    # Note: this expects application/x-www-form-urlencoded data, not JSON
    request.POST.each_pair do |key, value|
      if document.has_field?(key)
        document.set key, value
      else
        text_error "Unrecognised field '#{key}'"
      end
    end
    simple_json_result(current_index.add([document]))
  end

  delete "/?:index?/documents" do
    if params["delete_all"]
      action = current_index.delete_all
    else
      action = current_index.delete(params["link"])
    end
    simple_json_result(action)
  end
end
