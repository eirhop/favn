defmodule FavnOrchestrator.Operator.Catalogue.RunHistoryTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Pipeline
  alias FavnOrchestrator.Operator.Catalogue.RunHistory

  test "matches current pipeline identity and ignores malformed timestamps" do
    pipeline = %Pipeline{module: MyApp.Pipelines.Orders, name: :orders}

    run = %{
      id: "run-1",
      status: :ok,
      submit_kind: :pipeline,
      submit_ref: "Elixir.MyApp.Pipelines.Orders",
      target_refs: nil,
      finished_at: "invalid",
      inserted_at: ~U[2026-05-12 10:00:00Z]
    }

    assert [^run] = RunHistory.for_pipeline(pipeline, %{selected_assets: []}, [run])
    assert RunHistory.time_key(run) == ~U[2026-05-12 10:00:00Z]
  end

  test "projects unexpected legacy scope values without raising" do
    entry =
      RunHistory.entry(%{
        id: "run-1",
        status: :ok,
        metadata: %{backfill: {:legacy, :scope}}
      })

    assert entry.scope == %{type: :range, label: "{:legacy, :scope}"}
  end
end
