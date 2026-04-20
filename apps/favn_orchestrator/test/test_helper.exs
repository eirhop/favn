FavnTestSupport.Fixtures.compile_fixtures!([
  :basic_assets,
  :graph_assets,
  :runner_assets,
  :pipeline_assets
])

Logger.configure(level: :warning)
ExUnit.start()
