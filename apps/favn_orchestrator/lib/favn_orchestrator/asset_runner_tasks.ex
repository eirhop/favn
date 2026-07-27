defmodule FavnOrchestrator.AssetRunnerTasks do
  @moduledoc false

  alias Favn.Contracts.RunnerTask.PersistenceCodec
  alias Favn.Contracts.RunnerWork
  alias FavnOrchestrator.Persistence.Commands.EnqueueRunnerTask
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.RunnerTasks

  @spec enqueue(
          RunState.t(),
          RunnerWork.t(),
          Favn.Plan.node_key(),
          non_neg_integer(),
          pos_integer(),
          map()
        ) ::
          {:ok, FavnOrchestrator.Persistence.Results.RunnerTask.t(), RunnerWork.t()}
          | {:error, term()}
  def enqueue(%RunState{} = run, %RunnerWork{} = work, node_key, _stage, attempt, context)
      when is_map(context) do
    task_id = task_id(run, work, node_key, attempt)
    work = prepare_payload(work, task_id)
    occurred_at = run.inserted_at || DateTime.utc_now()

    with {:ok, runner_pool} <- Favn.RunnerPool.encode(work.runner_pool),
         {:ok, payload, payload_hash} <- PersistenceCodec.encode_payload(:asset_attempt, work),
         {:ok, orchestration_context} <-
           PersistenceCodec.encode_orchestration_context(context),
         {:ok, task} <-
           RunnerTasks.enqueue(%EnqueueRunnerTask{
             workspace_context:
               SystemContext.workspace(run.workspace_id, :asset_runner_task_enqueue),
             command_id: "enqueue:#{task_id}",
             task_id: task_id,
             domain_identity: domain_identity(run, work, node_key, attempt),
             task_kind: :asset_attempt,
             runner_pool: runner_pool,
             required_runner_release_id: work.required_runner_release_id,
             retry_class: :unknown_do_not_retry,
             payload: payload,
             payload_hash: payload_hash,
             orchestration_context: orchestration_context,
             run_id: run.id,
             asset_step_id: work.asset_step_id,
             required_capability: "asset_execution",
             deadline_at: work.deadline_at,
             occurred_at: occurred_at
           }) do
      {:ok, task, work}
    end
  end

  defp prepare_payload(work, task_id) do
    metadata =
      work.metadata
      |> Map.drop([:ownership_id, :dispatch_id, :runtime_input_event, :runtime_input_lineage])
      |> Map.put(:runner_task_id, task_id)

    %{
      work
      | manifest_lease_id: nil,
        runtime_input_pin: nil,
        metadata: metadata
    }
  end

  @doc false
  def task_id(run, work, node_key, attempt) do
    digest =
      :crypto.hash(
        :sha256,
        :erlang.term_to_binary(
          {run.workspace_id, run.id, work.asset_step_id, node_key, attempt},
          [:deterministic]
        )
      )
      |> Base.encode16(case: :lower)

    "rt_" <> digest
  end

  defp domain_identity(run, work, node_key, attempt) do
    digest =
      :crypto.hash(
        :sha256,
        :erlang.term_to_binary(
          {run.workspace_id, run.id, work.asset_step_id, node_key, attempt},
          [:deterministic]
        )
      )
      |> Base.encode16(case: :lower)

    "asset-attempt:" <> digest
  end
end
