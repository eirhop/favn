defmodule FavnOrchestrator.WindowSummaryTest do
  use ExUnit.Case, async: true

  alias Favn.Window.Anchor
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.WindowSummary

  test "reads an anchor embedded in run metadata" do
    {:ok, anchor} =
      Anchor.new(:day, ~U[2026-05-01 00:00:00Z], ~U[2026-05-02 00:00:00Z])

    summary =
      %RunState{metadata: %{pipeline_context: %{anchor_window: anchor}}}
      |> WindowSummary.from_run()

    assert summary == %{
             key: "day:#{anchor.timezone}:2026-05-01T00:00:00.000000Z",
             label: "May 1",
             kind: :day,
             start_at: anchor.start_at,
             end_at: anchor.end_at,
             timezone: anchor.timezone
           }
  end

  test "normalizes string-keyed persisted window maps" do
    run = %RunState{
      metadata: %{
        "selected_window" => %{
          "key" => "day:UTC:2026-05-01",
          "kind" => :day,
          "start_at" => ~U[2026-05-01 00:00:00Z],
          "end_at" => ~U[2026-05-02 00:00:00Z],
          "timezone" => "UTC"
        }
      }
    }

    assert %{key: "day:UTC:2026-05-01", label: "May 1", kind: :day} =
             WindowSummary.from_run(run)
  end

  test "falls back to a string window parameter" do
    assert %{
             key: "manual-window",
             label: "manual-window",
             kind: nil,
             start_at: nil,
             end_at: nil,
             timezone: nil
           } = WindowSummary.from_run(%RunState{params: %{"window" => "manual-window"}})
  end

  test "reports an empty normalized window" do
    assert WindowSummary.empty?(WindowSummary.public(%{}))
    assert WindowSummary.empty?(WindowSummary.public(nil))
  end
end
