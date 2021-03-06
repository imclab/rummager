module Fixtures
  module DefaultMappings
    def default_mappings
      {
        "edition" => {
          "_all" => { "enabled" => true} ,
          "properties" => {
            "title" => { "type" => "string", "index" => "analyzed" },
            "description" => { "type" => "string", "index" => "analyzed" },
            "format" => { "type" => "string", "index" => "not_analyzed", "include_in_all" => false },
            "section" => { "type" => "string", "index" => "not_analyzed", "include_in_all" => false },
            "subsection" => { "type" => "string", "index" => "not_analyzed", "include_in_all" => false },
            "subsubsection" => { "type" => "string", "index" => "not_analyzed", "include_in_all" => false },
            "link" => { "type" => "string", "index" => "not_analyzed", "include_in_all" => false },
            "indexable_content" => { "type" => "string", "index" => "analyzed"}
          }
        }
      }
    end
  end
end