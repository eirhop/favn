Code.require_file("support/fixtures/assets/basic_assets.ex", __DIR__)
Code.require_file("support/fixtures/assets/graph_assets.ex", __DIR__)
Code.require_file("support/fixtures/assets/runner_assets.ex", __DIR__)
Code.require_file("support/favn_test_setup.ex", __DIR__)

Logger.configure(level: :warning)
ExUnit.start()
