defmodule Favn.Contracts.RunnerResultTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerResult

  test "keeps manifest identity in execution results" do
    result =
      %RunnerResult{
        run_id: "run_1",
        manifest_version_id: "mv_1",
        manifest_content_hash: "abc",
        status: :ok,
        asset_results: [%{asset_ref: {MyApp.Asset, :asset}, status: :ok}],
        metadata: %{duration_ms: 10}
      }

    assert result.manifest_version_id == "mv_1"
    assert result.manifest_content_hash == "abc"
    assert result.status == :ok
    assert [%{asset_ref: {MyApp.Asset, :asset}, status: :ok}] = result.asset_results
  end
end
