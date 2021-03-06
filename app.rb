%w[ lib ].each do |path|
  $LOAD_PATH.unshift(path) unless $LOAD_PATH.include?(path)
end

require "sinatra"
require "multi_json"
require "csv"

require "document"
require "result_set_presenter"
require "organisation_set_presenter"
require "document_series_registry"
require "document_collection_registry"
require "organisation_registry"
require "suggester"
require "topic_registry"
require "world_location_registry"
require "elasticsearch/index"
require "elasticsearch/search_server"
require "redis"
require "matcher_set"

require_relative "config"
require_relative "helpers"

class Rummager < Sinatra::Application
  def search_server
    settings.search_config.search_server
  end

  def current_index
    index_name = params["index"] || settings.default_index_name
    search_server.index(index_name)
  rescue Elasticsearch::NoSuchIndex
    halt(404)
  end

  def document_series_registry
    index_name = settings.search_config.document_series_registry_index
    @@document_series_registry ||= DocumentSeriesRegistry.new(search_server.index(index_name)) if index_name
  end

  def document_collection_registry
    index_name = settings.search_config.document_collection_registry_index
    @@document_collection_registry ||= DocumentCollectionRegistry.new(search_server.index(index_name)) if index_name
  end

  def organisation_registry
    index_name = settings.search_config.organisation_registry_index
    @@organisation_registry ||= OrganisationRegistry.new(search_server.index(index_name)) if index_name
  end

  def topic_registry
    index_name = settings.search_config.topic_registry_index
    @@topic_registry ||= TopicRegistry.new(search_server.index(index_name)) if index_name
  end

  def world_location_registry
    index_name = settings.search_config.world_location_registry_index
    @@world_location_registry ||= WorldLocationRegistry.new(search_server.index(index_name)) if index_name
  end

  def indices_for_sitemap
    settings.search_config.index_names.map do |index_name|
      search_server.index(index_name)
    end
  end

  def lines_from_a_file(filepath)
    path = File.expand_path(filepath, File.dirname(__FILE__))
    lines = File.open(path).map(&:chomp)
    lines.reject { |line| line.start_with?('#') || line.empty? }
  end

  def ignores_from_file
    @@_ignores_from_file ||= lines_from_a_file("config/suggest/ignore.txt")
  end

  def blacklist_from_file
    @@_blacklist_from_file ||= lines_from_a_file("config/suggest/blacklist.txt")
  end

  def suggester
    ignore = ignores_from_file
    if organisation_registry
      ignore = ignore + organisation_registry.all.map(&:acronym).reject(&:nil?)
    end
    digit_or_word_containing_a_digit = /\d/
    ignore = ignore + [digit_or_word_containing_a_digit]
    Suggester.new(ignore: MatcherSet.new(ignore),
                  blacklist: MatcherSet.new(blacklist_from_file))
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

  error RestClient::RequestTimeout do
    halt(503, "Elasticsearch timed out")
  end

  error Redis::TimeoutError do
    halt(503, "Redis queue timed out")
  end

  error Elasticsearch::InvalidQuery do
    halt(422, env['sinatra.error'].message)
  end

  error Elasticsearch::BulkIndexFailure do
    halt(500, env['sinatra.error'].message)
  end

  # To search a named index:
  #   /index_name/search?q=pie
  #
  # To search the primary index:
  #   /search?q=pie
  #
  # To scope a search to an organisation:
  #   /search?q=pie&organisation_slug=home-office
  #
  # To get the results in date order, rather than relevancy:
  #   /search?q=pie&sort=public_timestamp&order=desc
  #
  # The response looks like this:
  #
  #   {
  #     "total": 1,
  #     "results": [
  #       ...
  #     ],
  #     "spelling_suggestions": [
  #       ...
  #     ]
  #   }
  get "/?:index?/search.?:format?" do
    json_only

    query = params["q"].to_s.gsub(/[\u{0}-\u{1f}]/, "").strip

    if query == ""
      expires 3600, :public
      halt 404
    end

    expires 3600, :public if query.length < 20
    organisation = params["organisation_slug"].blank? ? nil : params["organisation_slug"]
    result_set = current_index.search(query,
      organisation: organisation,
      sort: params["sort"],
      order: params["order"])
    presenter_context = {
      organisation_registry: organisation_registry,
      topic_registry: topic_registry,
      document_series_registry: document_series_registry,
      document_collection_registry: document_collection_registry,
      world_location_registry: world_location_registry,
      spelling_suggestions: suggester.suggestions(params["q"])
    }
    presenter = ResultSetPresenter.new(result_set, presenter_context)
    presenter.present
  end

  # Perform an advanced search. Supports filters and pagination.
  #
  # Returns the first N results if no keywords or filters supplied
  #
  # Required parameters:
  #   per_page              - eg "40"
  #   page                  - eg "1"
  #
  # Optional parameters:
  #   order[fieldname]      - eg order[public_timestamp]=desc
  #   keywords              - eg "tax"
  #
  # Arbitrary "filter parameters", anything which is defined in the mappings
  # for the index is allowed. Examples:
  #   search_format_types[]        - eg "consultation"
  #   topics[]                     - eg "climate-change"
  #   organisations[]              - eg "cabinet-office"
  #   relevant_to_local_government - eg "1"
  #
  # If the field type is defined as "date", this is possible:
  #   fieldname[from]     - eg public_timestamp[from]="2013-04-30"
  #   fieldname[to]      - eg public_timestamp[to]="2013-04-30"
  #
  get "/:index/advanced_search.?:format?" do
    json_only

    # Using request.params because it is just the params from the request
    # rather than things added by Sinatra (eg splat, captures, index and format)
    result_set = current_index.advanced_search(request.params)
    ResultSetPresenter.new(result_set).present
  end

  get "/organisations.?:format?" do
    json_only

    organisations = organisation_registry.all
    OrganisationSetPresenter.new(organisations).present
  end

  # Insert (or overwrite) a document
  post "/?:index?/documents" do
    request.body.rewind
    documents = [MultiJson.decode(request.body.read)].flatten.map { |hash|
      current_index.document_from_hash(hash)
    }

    if settings.enable_queue
      current_index.add_queued(documents)
      json_result 202, "Queued"
    else
      simple_json_result(current_index.add(documents))
    end
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
    document_link = params["splat"].first
    if settings.enable_queue
      current_index.delete_queued(document_link)
      json_result 202, "Queued"
    else
      simple_json_result(current_index.delete(document_link))
    end
  end

  # Update an existing document
  post "/?:index?/documents/*" do
    unless request.form_data?
      halt(
        415,
        {"Content-Type" => "text/plain"},
        "Amendments require application/x-www-form-urlencoded data"
      )
    end

    begin
      if settings.enable_queue
        current_index.amend_queued(params["splat"].first, request.POST)
        json_result 202, "Queued"
      else
        current_index.amend(params["splat"].first, request.POST)
        json_result 200, "OK"
      end
    rescue ArgumentError => e
      text_error e.message
    rescue Elasticsearch::DocumentNotFound
      halt 404
    end
  end

  delete "/?:index?/documents" do
    # DEPRECATED: the preferred way to do this is now through the
    # `rummager:switch_to_empty_index` Rake command

    if params["delete_all"]
      action = current_index.delete_all
    else
      action = current_index.delete(params["link"])
    end
    simple_json_result(action)
  end

  get "/_status" do
    status = {}
    status["queues"] = {}

    retries = Sidekiq::RetrySet.new.group_by(&:queue)
    scheduled = Sidekiq::ScheduledSet.new.group_by(&:queue)

    Sidekiq::Stats.new.queues.each do |queue_name, queue_size|
      retry_count = retries.fetch(queue_name, []).size
      scheduled_count = scheduled.fetch(queue_name, []).size
      status["queues"][queue_name] = {
        "jobs" => queue_size,
        "retries" => retry_count,
        "scheduled" => scheduled_count
      }
    end
    MultiJson.encode(status)
  end
end
