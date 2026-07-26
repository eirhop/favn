defmodule Favn.CLIRunTest do
  use ExUnit.Case, async: true

  alias Favn.CLI.Run
  alias Favn.Window.Request

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

  test "builds target-specific exact-window payloads" do
    assert {:ok, request} = Request.parse("day:2026-07-23", timezone: "Europe/Oslo")

    pipeline =
      Run.submission_payload(
        %{"target_id" => "pipeline:daily", "target_type" => "pipeline"},
        request,
        []
      )

    assert pipeline.window == %{
             mode: "single",
             kind: "day",
             value: "2026-07-23",
             timezone: "Europe/Oslo"
           }

    asset =
      Run.submission_payload(
        %{"target_id" => "asset:events", "target_type" => "asset"},
        request,
        []
      )

    assert asset.selection == %{
             source: "data_coverage_timeline",
             id: "window:day:2026-07-23",
             kind: "day",
             value: "2026-07-23",
             timezone: "Europe/Oslo"
           }
  end
end
