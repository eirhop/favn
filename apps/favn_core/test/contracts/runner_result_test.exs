defmodule Favn.Contracts.RunnerResultTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerAssetResult
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult

  test "keeps manifest identity in execution results" do
    result =
      %RunnerResult{
        run_id: "run_1",
        manifest_version_id: "mv_1",
        manifest_content_hash: "abc",
        status: :ok,
        asset_results: [%RunnerAssetResult{ref: {MyApp.Asset, :asset}, status: :ok}],
        metadata: %{duration_ms: 10}
      }

    assert result.manifest_version_id == "mv_1"
    assert result.manifest_content_hash == "abc"
    assert result.status == :ok
    assert [%RunnerAssetResult{ref: {MyApp.Asset, :asset}, status: :ok}] = result.asset_results
  end

  test "runner errors carry explicit retryability" do
    error = RunnerError.normalize(:bad_config, type: :missing_runtime_config, retryable?: false)

    assert error.type == :missing_runtime_config
    assert error.retryable? == false
    assert is_binary(error.reason)
  end
end
