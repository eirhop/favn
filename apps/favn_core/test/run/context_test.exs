defmodule Favn.Run.ContextTest do
  use ExUnit.Case, async: true

  alias Favn.Run.Context

  test "context struct keeps runtime invocation fields" do
    now = ~U[2026-04-15 12:00:00Z]

    context = %Context{
      run_id: "run_1",
      target_refs: [{MyApp.Asset, :asset}],
      current_ref: {MyApp.Asset, :asset},
      asset: %{ref: {MyApp.Asset, :asset}, relation: nil, config: %{}},
      params: %{full_refresh: false},
      window: nil,
      pipeline: nil,
      run_started_at: now,
      stage: 0,
      attempt: 1,
      max_attempts: 1
    }

    assert context.run_id == "run_1"
    assert context.current_ref == {MyApp.Asset, :asset}
    assert context.params == %{full_refresh: false}
  end
end
