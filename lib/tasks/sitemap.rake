require "elasticsearch/sitemap"

namespace :sitemap do
  desc "Generate new sitemap files and if all is ok switch symlink"
  task :generate_and_replace do
    # Individual site maps can have a maximum of 50,000 links in them.
    # Generate site maps and then an index site map to point to them

    sitemap_directory = File.join(PROJECT_ROOT, "public", "system")
    sitemap = Sitemap.new(sitemap_directory)
    sitemap_index_filename = sitemap.generate(search_server.all_indices)

    sitemap_index_path = File.join(sitemap_directory, sitemap_index_filename)
    sitemap_link_path = File.join(sitemap_directory, "sitemap.xml")

    `ln -sf #{sitemap_index_path} #{sitemap_link_path}`
    fail("Symlinking failed") unless $?.success?
  end
end