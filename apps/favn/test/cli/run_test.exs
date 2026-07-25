defmodule Favn.CLIRunTest do
  use ExUnit.Case, async: true

  alias Favn.CLI.Run

  test "resolves user-facing module shorthand for default and named assets" do
    manifest = %{
      "targets" => %{
        "pipelines" => [],
        "assets" => [
          %{
            "asset_ref" => "Elixir.MyApp.Source.Accounts:asset",
            "target_id" => "asset:Elixir.MyApp.Source.Accounts:asset",
            "label" => "Elixir.MyApp.Source.Accounts:asset"
          },
          %{
            "asset_ref" => "Elixir.MyApp.Landing.Extracts:activities",
            "target_id" => "asset:Elixir.MyApp.Landing.Extracts:activities",
            "label" => "Elixir.MyApp.Landing.Extracts:activities"
          }
        ]
      }
    }

    assert {:ok, %{"asset_ref" => "Elixir.MyApp.Source.Accounts:asset"}} =
             Run.resolve_run_target(manifest, "MyApp.Source.Accounts")

    assert {:ok, %{"asset_ref" => "Elixir.MyApp.Landing.Extracts:activities"}} =
             Run.resolve_run_target(manifest, "MyApp.Landing.Extracts:activities")
  end
end
