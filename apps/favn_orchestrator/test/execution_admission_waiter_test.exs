defmodule FavnOrchestrator.ExecutionAdmission.WaiterTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.ExecutionAdmission.Waiter

  @now ~U[2026-08-07 12:00:00Z]

  test "rejects a blocked scope that was not requested" do
    waiter =
      waiter_fixture()
      |> Map.put(:requested_scopes, [%{kind: :run, key: "run-1", limit: 1}])

    assert {:error, {:invalid_execution_admission_waiter_field, :blocked_scope}} =
             Waiter.normalize(waiter)
  end

  test "rejects a queue reason that does not match the blocked scope" do
    waiter =
      waiter_fixture()
      |> Map.put(:queue_reason, :pipeline_concurrency)

    assert {:error, {:invalid_execution_admission_waiter_field, :queue_reason}} =
             Waiter.normalize(waiter)
  end

  defp waiter_fixture do
    %{
      waiter_id: "waiter-1",
      run_id: "run-1",
      asset_step_id: "step-1",
      queue_reason: :global_concurrency,
      blocked_scope: %{kind: :global, key: "default", limit: 4},
      requested_scopes: [%{kind: :global, key: "default", limit: 4}],
      stage: 0,
      attempt: 1,
      inserted_at: @now,
      updated_at: @now,
      deadline_at: DateTime.add(@now, 30, :second),
      wake_generation: 0
    }
  end
end
