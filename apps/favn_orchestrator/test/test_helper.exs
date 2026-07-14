FavnTestSupport.Fixtures.compile_fixtures!([
  :basic_assets,
  :graph_assets,
  :runner_assets,
  :pipeline_assets
])

Code.require_file("support/runtime.ex", __DIR__)

Logger.configure(level: :warning)
ExUnit.start(capture_log: true)
