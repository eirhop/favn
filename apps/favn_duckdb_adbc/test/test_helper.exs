Code.require_file("support/test_support.exs", __DIR__)

unless System.get_env("FAVN_DUCKDB_ADBC_INTEGRATION") == "1" do
  ExUnit.configure(exclude: [adbc_integration: true])
end

ExUnit.start()
