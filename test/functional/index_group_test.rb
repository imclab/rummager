require "test_helper"
require "multi_json"
require "elasticsearch/search_server"
require "elasticsearch/index_group"

class IndexGroupTest < MiniTest::Unit::TestCase

  ELASTICSEARCH_OK = {
    status: 200,
    body: MultiJson.encode({"ok" => true, "acknowledged" => true})
  }

  def setup
    @schema = {
      "index" => {
        "settings" => "awesomeness"
      },
      "mappings" => {
        "default" => {
          "edition" => {
            "properties" => {
              "title" => { "type" => "string" }
            }
          }
        },
        "custom" => {
          "edition" => {
            "properties" => {
              "title" => { "type" => "string" },
              "description" => { "type" => "string" }
            }
          }
        }
      }
    }
    @server = Elasticsearch::SearchServer.new(
      "http://localhost:9200/",
      @schema,
      ["mainstream", "custom"]
    )
  end

  def test_create_index
    expected_body = MultiJson.encode({
      "settings" => @schema["index"]["settings"],
      "mappings" => @schema["mappings"]["default"]
    })
    stub = stub_request(:put, %r(http://localhost:9200/mainstream-.*/))
      .with(body: expected_body)
      .to_return(
        status: 200,
        body: '{"ok": true, "acknowledged": true}'
      )
    index = @server.index_group("mainstream").create_index

    assert_requested(stub)
    assert index.is_a? Elasticsearch::Index
    assert_match(/^mainstream-/, index.index_name)
    assert_equal ["title"], index.field_names
  end

  def test_create_index_with_custom_mappings
    expected_body = MultiJson.encode({
      "settings" => @schema["index"]["settings"],
      "mappings" => @schema["mappings"]["custom"]
    })
    stub = stub_request(:put, %r(http://localhost:9200/custom-.*/))
      .with(body: expected_body)
      .to_return(
        status: 200,
        body: '{"ok": true, "acknowledged": true}'
      )
    index = @server.index_group("custom").create_index

    assert_requested(stub)
    assert index.is_a? Elasticsearch::Index
    assert_match(/^custom-/, index.index_name)
    assert_equal ["title", "description"], index.field_names
  end

  def test_switch_index_with_no_existing_alias
    new_index = stub("New index", index_name: "test-new")
    get_stub = stub_request(:get, "http://localhost:9200/_aliases")
      .to_return(
        status: 200,
        body: MultiJson.encode({
          "test-new" => { "aliases" => {} }
        })
      )
    expected_body = MultiJson.encode({
      "actions" => [
        { "add" => { "index" => "test-new", "alias" => "test" } }
      ]
    })
    post_stub = stub_request(:post, "http://localhost:9200/_aliases")
      .with(body: expected_body)
      .to_return(ELASTICSEARCH_OK)

    @server.index_group("test").switch_to(new_index)

    assert_requested(get_stub)
    assert_requested(post_stub)
  end

  def test_switch_index_with_existing_alias
    new_index = stub("New index", index_name: "test-new")
    get_stub = stub_request(:get, "http://localhost:9200/_aliases")
      .to_return(
        status: 200,
        body: MultiJson.encode({
          "test-old" => { "aliases" => { "test" => {} } },
          "test-new" => { "aliases" => {} }
        })
      )

    expected_body = MultiJson.encode({
      "actions" => [
        { "remove" => { "index" => "test-old", "alias" => "test" } },
        { "add" => { "index" => "test-new", "alias" => "test" } }
      ]
    })
    post_stub = stub_request(:post, "http://localhost:9200/_aliases")
      .with(body: expected_body)
      .to_return(ELASTICSEARCH_OK)

    @server.index_group("test").switch_to(new_index)

    assert_requested(get_stub)
    assert_requested(post_stub)
  end

  def test_switch_index_with_multiple_existing_aliases
    # Not expecting the system to get into this state, but it should cope
    new_index = stub("New index", index_name: "test-new")
    get_stub = stub_request(:get, "http://localhost:9200/_aliases")
      .to_return(
        status: 200,
        body: MultiJson.encode({
          "test-old" => { "aliases" => { "test" => {} } },
          "test-old2" => { "aliases" => { "test" => {} } },
          "test-new" => { "aliases" => {} }
        })
      )

    expected_body = MultiJson.encode({
      "actions" => [
        { "remove" => { "index" => "test-old", "alias" => "test" } },
        { "remove" => { "index" => "test-old2", "alias" => "test" } },
        { "add" => { "index" => "test-new", "alias" => "test" } }
      ]
    })
    post_stub = stub_request(:post, "http://localhost:9200/_aliases")
      .with(body: expected_body)
      .to_return(ELASTICSEARCH_OK)

    @server.index_group("test").switch_to(new_index)

    assert_requested(get_stub)
    assert_requested(post_stub)
  end

  def test_switch_index_with_existing_real_index
    new_index = stub("New index", index_name: "test-new")
    get_stub = stub_request(:get, "http://localhost:9200/_aliases")
      .to_return(
        status: 200,
        body: MultiJson.encode({
          "test" => { "aliases" => {} }
        })
      )

    assert_raises RuntimeError do
      @server.index_group("test").switch_to(new_index)
    end
  end

  def test_index_names_with_no_indices
    stub_request(:get, "http://localhost:9200/_aliases")
      .to_return(
        status: 200,
        body: MultiJson.encode({
        })
      )

    assert_equal [], @server.index_group("test").index_names
  end

  def test_index_names_with_index
    index_name = "test-2012-03-01t12:00:00z-12345678-1234-1234-1234-123456789012"
    stub_request(:get, "http://localhost:9200/_aliases")
      .to_return(
        status: 200,
        body: MultiJson.encode({
          index_name => { "aliases" => { "test" => {} } }
        })
      )

    assert_equal [index_name], @server.index_group("test").index_names
  end

  def test_index_names_with_other_groups
    this_name = "test-2012-03-01t12:00:00z-12345678-1234-1234-1234-123456789012"
    other_name = "fish-2012-03-01t12:00:00z-87654321-4321-4321-4321-210987654321"

    stub_request(:get, "http://localhost:9200/_aliases")
      .to_return(
        status: 200,
        body: MultiJson.encode({
          this_name => { "aliases" => {} },
          other_name => { "aliases" => {} }
        })
      )

    assert_equal [this_name], @server.index_group("test").index_names
  end

  def test_clean_with_no_indices
    stub_request(:get, "http://localhost:9200/_aliases")
      .to_return(
        status: 200,
        body: MultiJson.encode({
        })
      )

    @server.index_group("test").clean
  end

  def test_clean_with_dead_index
    index_name = "test-2012-03-01t12:00:00z-12345678-1234-1234-1234-123456789012"
    stub_request(:get, "http://localhost:9200/_aliases")
      .to_return(
        status: 200,
        body: MultiJson.encode({
          index_name => { "aliases" => {} }
        })
      )

    delete_stub = stub_request(:delete, "http://localhost:9200/#{index_name}")
      .to_return(ELASTICSEARCH_OK)

    @server.index_group("test").clean

    assert_requested delete_stub
  end

  def test_clean_with_live_index
    index_name = "test-2012-03-01t12:00:00z-12345678-1234-1234-1234-123456789012"
    stub_request(:get, "http://localhost:9200/_aliases")
      .to_return(
        status: 200,
        body: MultiJson.encode({
          index_name => { "aliases" => { "test" => {} } }
        })
      )

    @server.index_group("test").clean
  end

  def test_clean_with_multiple_indices
    index_names = [
      "test-2012-03-01t12:00:00z-12345678-1234-1234-1234-123456789012",
      "test-2012-03-01t12:00:00z-abcdefab-abcd-abcd-abcd-abcdefabcdef"
    ]
    stub_request(:get, "http://localhost:9200/_aliases")
      .to_return(
        status: 200,
        body: MultiJson.encode({
          index_names[0] => { "aliases" => {} },
          index_names[1] => { "aliases" => {} }
        })
      )

    delete_stubs = index_names.map { |index_name|
      stub_request(:delete, "http://localhost:9200/#{index_name}")
        .to_return(ELASTICSEARCH_OK)
    }

    @server.index_group("test").clean

    delete_stubs.each do |delete_stub| assert_requested delete_stub end
  end

  def test_clean_with_live_and_dead_indices
    live_name = "test-2012-03-01t12:00:00z-12345678-1234-1234-1234-123456789012"
    dead_name = "test-2012-03-01t12:00:00z-87654321-4321-4321-4321-210987654321"

    stub_request(:get, "http://localhost:9200/_aliases")
      .to_return(
        status: 200,
        body: MultiJson.encode({
          live_name => { "aliases" => { "test" => {} } },
          dead_name => { "aliases" => {} }
        })
      )

    delete_stub = stub_request(:delete, "http://localhost:9200/#{dead_name}")
      .to_return(ELASTICSEARCH_OK)

    @server.index_group("test").clean

    assert_requested delete_stub
  end

  def test_clean_with_other_alias
    # If there's an alias we don't know about, that should save the index
    index_name = "test-2012-03-01t12:00:00z-12345678-1234-1234-1234-123456789012"
    stub_request(:get, "http://localhost:9200/_aliases")
      .to_return(
        status: 200,
        body: MultiJson.encode({
          index_name => { "aliases" => { "something_else" => {} } }
        })
      )

    @server.index_group("test").clean
  end

  def test_clean_with_other_groups
    # Check we don't go around deleting index from other groups
    this_name = "test-2012-03-01t12:00:00z-12345678-1234-1234-1234-123456789012"
    other_name = "fish-2012-03-01t12:00:00z-87654321-4321-4321-4321-210987654321"

    stub_request(:get, "http://localhost:9200/_aliases")
      .to_return(
        status: 200,
        body: MultiJson.encode({
          this_name => { "aliases" => {} },
          other_name => { "aliases" => {} }
        })
      )

    delete_stub = stub_request(:delete, "http://localhost:9200/#{this_name}")
      .to_return(ELASTICSEARCH_OK)

    @server.index_group("test").clean

    assert_requested delete_stub
  end

  def test_promoted_results_passed_to_index
    promoted_results = stub("promoted results")
    base_uri = "http://localhost"
    name = "my_index"
    index_settings = {"settings" => {}}
    mappings = {"default" => {}}
    index_group = Elasticsearch::IndexGroup.new(base_uri, name, index_settings, mappings, promoted_results)

    assert_equal promoted_results, index_group.current.promoted_results
  end
end
