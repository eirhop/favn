defmodule Favn.Run.AssetResultTest do
  use ExUnit.Case, async: true

  alias Favn.Run.AssetResult

  test "asset result struct captures one completed execution" do
    started_at = ~U[2026-04-15 12:00:00Z]
    finished_at = ~U[2026-04-15 12:00:01Z]

    result = %AssetResult{
      ref: {MyApp.Asset, :asset},
      stage: 0,
      status: :ok,
      started_at: started_at,
      finished_at: finished_at,
      duration_ms: 1000,
      meta: %{rows_affected: 10},
      attempt_count: 1,
      max_attempts: 1,
      attempts: [
        %{
          attempt: 1,
          started_at: started_at,
          finished_at: finished_at,
          duration_ms: 1000,
          status: :ok,
          meta: %{rows_affected: 10},
          error: nil
        }
      ]
    }

    assert result.status == :ok
    assert result.ref == {MyApp.Asset, :asset}
    assert [%{attempt: 1, status: :ok}] = result.attempts
  end
end
