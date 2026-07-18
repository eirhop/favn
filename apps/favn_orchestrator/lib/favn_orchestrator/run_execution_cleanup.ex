defmodule FavnOrchestrator.RunExecutionCleanup do
  @moduledoc false

  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunnerClientValidator
  alias FavnOrchestrator.RunServer.Cancellation
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.RuntimeConfig

  @confirmed_statuses [:cancel_acknowledged, :already_completed]

  @spec cancel_active(RunState.t(), term()) :: [map()]
  def cancel_active(%RunState{} = run, reason) do
    case RunExecutionOwnership.fetch_active(run) do
      {:ok, ownerships} -> cancel_ownerships(run, ownerships, reason)
      {:error, error} -> [unknown_status({:execution_ownership_read_failed, error})]
    end
  end

  @spec confirmed?([map()]) :: boolean()
  def confirmed?(statuses) when is_list(statuses) do
    Enum.all?(statuses, &(Map.get(&1, :status) in @confirmed_statuses))
  end

  @spec release_admission(RunState.t()) :: :ok
  def release_admission(%RunState{} = run) do
    release_admission(run, run.id)
  end

  defp release_admission(run_or_id, run_id) do
    case ExecutionAdmission.release_run(run_or_id) do
      :ok ->
        :ok

      {:error, reason} ->
        OperationalEvents.emit(
          :run_execution_admission_cleanup_failed,
          %{},
          %{run_id: run_id, reason: reason},
          level: :warning
        )

        :ok
    end
  end

  defp cancel_ownerships(run, ownerships, reason) do
    execution_ids =
      ownerships
      |> Enum.map(& &1.runner_execution_id)
      |> Enum.filter(&is_binary/1)
      |> Enum.uniq()

    missing_id_statuses =
      case RunExecutionOwnership.persist_unknown_without_execution_id(run, reason) do
        :ok -> []
        {:error, error} -> [unknown_status(error)]
      end

    if execution_ids == [] do
      missing_id_statuses
    else
      results = cancel_execution_ids(run, execution_ids, reason)
      persist_result = RunExecutionOwnership.persist_cancel_outcomes(run, results, reason)

      missing_id_statuses ++ result_statuses(results) ++ persist_statuses(persist_result)
    end
  end

  defp cancel_execution_ids(run, execution_ids, reason) do
    runtime_config = RuntimeConfig.current()

    if RunnerClientValidator.validate(runtime_config.runner_client) == :ok do
      Cancellation.dispatch_runner_work(
        run,
        execution_ids,
        reason,
        runtime_config.runner_client,
        runtime_config.runner_client_opts
      )
    else
      Enum.map(execution_ids, fn execution_id ->
        %{
          execution_id: execution_id,
          status: :unknown_runner_outcome,
          error: :runner_client_not_available
        }
      end)
    end
  end

  defp result_statuses(results) do
    Enum.map(results, fn result ->
      %{
        runner_execution_id: Map.get(result, :execution_id),
        status: RunExecutionOwnership.cancel_outcome_status(result),
        error: safe_error(Map.get(result, :error))
      }
    end)
  end

  defp persist_statuses(:ok), do: []
  defp persist_statuses({:error, error}), do: [unknown_status(error)]

  defp unknown_status(error) do
    %{runner_execution_id: nil, status: :unknown_runner_outcome, error: safe_error(error)}
  end

  defp safe_error(nil), do: nil

  defp safe_error(error) do
    case Redaction.redact_operational_bounded(%{error: error}) do
      %{error: safe} -> safe
      _other -> "[REDACTED]"
    end
  end
end
