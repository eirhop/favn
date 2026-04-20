FavnTestSupport.Fixtures.compile_fixtures!([
  :pipeline_assets
])

Logger.configure(level: :warning)
ExUnit.start()
