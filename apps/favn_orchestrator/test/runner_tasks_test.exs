Code.require_file("../../favn_test_support/fixtures/runner_task_persistence.exs", __DIR__)

defmodule FavnOrchestrator.RunnerTasksTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerTask
  alias FavnOrchestrator.RunnerTasks
  alias FavnTestSupport.RunnerTaskPersistence, as: Fixture

  test "completion tags deterministic persistence rejection without losing its reason" do
    version = Fixture.version()

    {:relation_inspection, _request, inspection} =
      Enum.find(Fixture.tasks(version), &(elem(&1, 0) == :relation_inspection))

    [column] = inspection.columns

    inspection = %{
      inspection
      | columns: [%{column | metadata: %{contract_nullability: :unexpected}}]
    }

    message = %RunnerTask.Result{
      workspace_id: "workspace",
      task_id: "task",
      task_kind: :relation_inspection,
      runner_instance_id: "runner",
      runner_session_generation: 1,
      assignment_generation: 1,
      outcome: :succeeded,
      retry_class: :terminal,
      result: inspection,
      finished_at: DateTime.utc_now()
    }

    assert :ok = RunnerTask.Result.validate(message)

    assert {:error, {:runner_task_result_persistence_rejected, :invalid_contract_nullability}} =
             RunnerTasks.complete(message)
  end
end
