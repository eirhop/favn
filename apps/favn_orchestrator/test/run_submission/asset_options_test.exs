defmodule FavnOrchestrator.RunSubmission.AssetOptionsTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Asset
  alias FavnOrchestrator.OperatorCommands.AssetRunRequest
  alias FavnOrchestrator.RunSubmission.AssetOptions

  @asset %Asset{ref: {MyApp.Assets.Orders, :asset}, module: MyApp.Assets.Orders, name: :asset}

  test "normalizes string-keyed manifest request options" do
    assert {:ok, opts} =
             AssetOptions.from_input(@asset, %{
               "config" => %{
                 "dependencies" => "none",
                 "refresh" => "force_selected",
                 "metadata" => %{"source" => "operator"}
               }
             })

    assert opts[:dependencies] == :none
    assert opts[:refresh] == {:force_assets, [@asset.ref]}
    assert opts[:metadata] == %{"source" => "operator"}
  end

  test "rejects malformed metadata before selection enrichment" do
    assert {:error, :invalid_run_metadata} =
             AssetOptions.from_input(@asset, %{
               config: %{metadata: :invalid},
               selection: %{source: :refresh_timeline, id: "refresh:day:2026-05-12"}
             })
  end

  test "operator upstream refresh requires dependency traversal" do
    request = %AssetRunRequest{
      dependency_mode: :none,
      refresh_mode: :force_selected_upstream
    }

    assert {:error, {:refresh_include_upstream_requires_dependencies, :all}} =
             AssetOptions.from_operator_request(@asset, request)
  end
end
