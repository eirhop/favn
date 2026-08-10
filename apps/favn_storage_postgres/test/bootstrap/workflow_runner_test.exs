defmodule FavnStoragePostgres.Bootstrap.WorkflowRunnerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias FavnStoragePostgres.Bootstrap.WorkflowRunner

  test "worker exceptions retain bounded stage diagnostics without disclosing the reason" do
    handler = "workflow-runner-failure-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach_many(
      handler,
      [
        [:favn, :storage_postgres, :database_workflow, :stage],
        [:favn, :storage_postgres, :database_workflow, :worker_failure]
      ],
      fn event, measurements, metadata, _config ->
        send(parent, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    secret_message =
      "ecto://database-user-canary:database-password-canary@postgres.example/favn " <>
        "access-token-canary arbitrary-exception-canary"

    log =
      capture_log(fn ->
        result =
          WorkflowRunner.run(:bootstrap, fn ->
            :ok = WorkflowRunner.track_stage(:database, fn -> :ok end)
            :ok = WorkflowRunner.record_completed([:database])

            WorkflowRunner.track_stage(:identities, fn ->
              raise RuntimeError, secret_message
            end)
          end)

        send(parent, {:workflow_result, result})
      end)

    assert_receive {:workflow_result,
                    {:error,
                     %{
                       operation: :bootstrap,
                       outcome: :unknown,
                       state: :unknown_outcome,
                       code: :unexpected_worker_exit,
                       safe_to_retry: false,
                       completed_stages: [:database],
                       findings: [finding]
                     } = result}}

    assert finding.code == :unexpected_worker_exit
    assert finding.stage == :identities
    assert finding.details.failure_kind == :error
    assert finding.details.failure_class == "RuntimeError"
    assert is_binary(finding.details.failure_location)
    assert finding.details.failure_location =~ "WorkflowRunnerTest"
    assert finding.details.diagnostic_id =~ ~r/^diag_[0-9a-f]{16}$/

    assert log =~ "favn.database_workflow.worker_failed operation=bootstrap"
    assert log =~ "stage=identities"
    assert log =~ "failure_kind=error"
    assert log =~ "failure_class=RuntimeError"
    assert log =~ "completed_stages=database"
    assert log =~ "diagnostic_id=#{finding.details.diagnostic_id}"

    telemetry = collect_telemetry([])
    inspected = inspect([result, log, telemetry])

    for canary <- [
          "database-user-canary",
          "database-password-canary",
          "postgres.example",
          "access-token-canary",
          "arbitrary-exception-canary"
        ] do
      refute inspected =~ canary
    end

    assert Enum.any?(telemetry, fn
             {[:favn, :storage_postgres, :database_workflow, :stage], _measurements,
              %{operation: :bootstrap, stage: :identities, outcome: :started}} ->
               true

             _event ->
               false
           end)

    assert Enum.any?(telemetry, fn
             {[:favn, :storage_postgres, :database_workflow, :worker_failure], _measurements,
              %{diagnostic_id: diagnostic_id, stage: :identities}} ->
               diagnostic_id == finding.details.diagnostic_id

             _event ->
               false
           end)
  end

  test "a status exit outside identity inspection retains its safe class and schema stage" do
    assert {:error,
            %{
              operation: :status,
              state: :operation_failed,
              code: :unexpected_worker_exit,
              safe_to_retry: true,
              completed_stages: [],
              findings: [finding]
            }} =
             WorkflowRunner.run(:status, fn ->
               WorkflowRunner.track_stage(:schema, fn ->
                 Process.exit(self(), :kill)
               end)
             end)

    assert finding.stage == :schema
    assert finding.details.failure_kind == :exit
    assert finding.details.failure_class == "killed"
    assert finding.details.failure_location == "worker"
  end

  test "a status stage is retained across the classified connection worker boundary" do
    parent = self()

    log =
      capture_log(fn ->
        result =
          WorkflowRunner.run(:status, fn ->
            workflow_context = WorkflowRunner.current_context()
            workflow_reference = WorkflowRunner.context_reference(workflow_context)
            caller = self()

            {worker, monitor} =
              spawn_monitor(fn ->
                case WorkflowRunner.guarded_result(workflow_context, fn ->
                       WorkflowRunner.track_stage(:runtime, fn ->
                         raise "classified-connection-exception-canary"
                       end)
                     end) do
                  {:ok, result} -> send(caller, {:classified_result, result})
                  {:error, failure} -> send(caller, {:classified_failure, failure})
                end
              end)

            relay_classified_result(workflow_reference, worker, monitor)
          end)

        send(parent, {:classified_workflow_result, result})
      end)

    assert_receive {:classified_workflow_result, {:error, %{findings: [finding]}}}

    assert finding.stage == :runtime
    assert finding.details.failure_kind == :error
    assert finding.details.failure_class == "RuntimeError"
    refute log =~ "classified-connection-exception-canary"
  end

  defp collect_telemetry(events) do
    receive do
      {:telemetry, event, measurements, metadata} ->
        collect_telemetry([{event, measurements, metadata} | events])
    after
      0 -> Enum.reverse(events)
    end
  end

  defp relay_classified_result(workflow_reference, worker, monitor) do
    receive do
      {:favn_workflow_context, ^workflow_reference, event} ->
        WorkflowRunner.absorb_context_event(workflow_reference, event)
        relay_classified_result(workflow_reference, worker, monitor)

      {:classified_failure, failure} ->
        Process.demonitor(monitor, [:flush])
        WorkflowRunner.propagate_failure(failure)

      {:classified_result, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^worker, reason} ->
        exit(reason)
    end
  end
end
