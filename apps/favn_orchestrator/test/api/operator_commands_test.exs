defmodule FavnOrchestrator.API.OperatorCommandsTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.API.OperatorCommands
  alias FavnOrchestrator.OperatorCommands.AssetRunRequest

  test "preserves an asset run context through HTTP input normalization" do
    params = %{
      "run_context_id" => "pipeline:scheduled",
      "dependencies" => "none",
      "window" => %{
        "kind" => "month",
        "value" => "2026-07",
        "timezone" => "Europe/Oslo"
      }
    }

    assert {:ok, input} = OperatorCommands.normalize_run_input(params, %{type: "asset"})
    assert input[:run_context_id] == "pipeline:scheduled"

    assert {:ok, %AssetRunRequest{run_context_id: "pipeline:scheduled"}} =
             AssetRunRequest.from_input(input)
  end
end
