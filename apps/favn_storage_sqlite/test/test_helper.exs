FavnTestSupport.Fixtures.compile_fixtures!([
  :pipeline_assets
])

Code.require_file("../../favn_legacy/test/support/favn_test_setup.ex", __DIR__)

Logger.configure(level: :warning)
ExUnit.start()
