FavnTestSupport.Fixtures.compile_fixtures!([
  :basic_assets,
  :graph_assets,
  :runner_assets,
  :pipeline_assets
])

Code.require_file("support/runtime.ex", __DIR__)

defmodule FavnOrchestrator.TestRunnerTaskStore do
  @behaviour FavnOrchestrator.Persistence.RunnerTaskStore

  alias Favn.Contracts.GenerationCapabilitiesRequest
  alias Favn.Contracts.GenerationCapabilitiesResult
  alias Favn.Contracts.GenerationMarkerReadRequest
  alias Favn.Contracts.GenerationMarkerReadResult
  alias Favn.Contracts.RunnerTask.PersistenceCodec
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.RuntimeInput.Pin
  alias Favn.RuntimeInput.Resolution
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.RunnerCapacityDemand
  alias FavnOrchestrator.Persistence.Results.RunnerCapacityHealth
  alias FavnOrchestrator.Persistence.Results.RunnerReleaseDrain
  alias FavnOrchestrator.Persistence.Results.RunnerTask

  def enqueue(command) do
    if test_pid = Process.get(:rebuild_test_pid) do
      send(test_pid, {:runner_task_enqueued, command})
    end

    with runner when is_atom(runner) and not is_nil(runner) <-
           Application.get_env(:favn_orchestrator, :test_runner_executor),
         {:ok, payload} <- PersistenceCodec.decode_payload(command.task_kind, command.payload) do
      opts = Application.get_env(:favn_orchestrator, :test_runner_executor_opts, [])
      task = execute(command, runner, opts, payload)
      Process.put({__MODULE__, command.task_id}, task)
      {:ok, task}
    else
      _missing -> unavailable()
    end
  end

  def claim(_command), do: unavailable()
  def transition(_command), do: unavailable()
  def persist_runtime_inputs(_command), do: unavailable()
  def append_log_batch(_command), do: unavailable()
  def complete(_command), do: unavailable()
  def request_cancellation(_command), do: unavailable()
  def acknowledge_cancellation(_command), do: unavailable()
  def release(_command), do: unavailable()
  def retry(_command), do: unavailable()
  def recover_expired(_command), do: unavailable()
  def reconcile_demand(_command), do: unavailable()

  def ensure_demand(command) do
    {:ok,
     %RunnerCapacityDemand{
       runner_pool: command.runner_pool,
       required_runner_release_id: command.required_runner_release_id,
       outstanding_count: 0,
       queued_count: 0,
       active_count: 0,
       version: 0,
       updated_at: command.occurred_at,
       healthy?: true
     }}
  end

  def get(query) do
    case Process.get({__MODULE__, query.task_id}) do
      %RunnerTask{} = task -> {:ok, task}
      nil -> unavailable()
    end
  end

  def page_run(_query), do: unavailable()

  def demand(query) do
    case Application.get_env(:favn_orchestrator, :test_runner_capacity_demand) do
      %RunnerCapacityDemand{} = demand
      when demand.runner_pool == query.runner_pool and
             demand.required_runner_release_id == query.required_runner_release_id ->
        {:ok, demand}

      _other ->
        unavailable()
    end
  end

  def list_demands(_query) do
    case Application.get_env(:favn_orchestrator, :test_runner_capacity_demand) do
      %RunnerCapacityDemand{} = demand -> {:ok, [demand]}
      _other -> {:ok, []}
    end
  end

  def release_drain(query) do
    with {:ok, demand} <- demand(query) do
      {:ok, drain_result(demand)}
    end
  end

  def capacity_health(_query) do
    case Application.get_env(:favn_orchestrator, :test_runner_capacity_demand) do
      %RunnerCapacityDemand{healthy?: healthy?} ->
        {:ok,
         %RunnerCapacityHealth{
           partition_count: 1,
           unhealthy_partition_count: if(healthy?, do: 0, else: 1)
         }}

      _other ->
        {:ok, %RunnerCapacityHealth{partition_count: 0, unhealthy_partition_count: 0}}
    end
  end

  def list_release_drains(_query) do
    with {:ok, demands} <- list_demands(nil) do
      {:ok, Enum.map(demands, &drain_result/1)}
    end
  end

  def runtime_inputs(run_id, node_keys) do
    node_keys
    |> List.wrap()
    |> Enum.map(&Process.get({__MODULE__, :runtime_input, run_id, &1}))
    |> Enum.reject(&is_nil/1)
  end

  defp unavailable,
    do: {:error, Error.new(:unavailable, "runner task store is not used by this test")}

  defp drain_result(demand) do
    %RunnerReleaseDrain{
      runner_pool: demand.runner_pool,
      required_runner_release_id: demand.required_runner_release_id,
      outstanding_task_count: demand.outstanding_count,
      active_run_count: 0,
      pending_operation_count: 0,
      blocker_count: demand.outstanding_count,
      updated_at: demand.updated_at,
      healthy?: demand.healthy?,
      durable_drained?: demand.healthy? and demand.outstanding_count == 0
    }
  end

  defp execute(command, runner, opts, payload) do
    result =
      case {command.task_kind, payload} do
        {:asset_attempt, %RunnerWork{} = work} ->
          case runner.resolve_runtime_inputs(work, opts) do
            {:ok, %Resolution{} = resolution} ->
              pin = Pin.new(work.run_id, RunnerWork.node_key(work), resolution)

              Process.put(
                {__MODULE__, :runtime_input, work.run_id, RunnerWork.node_key(work)},
                pin
              )

              {:ok, runner_result(work)}

            {:ok, nil} ->
              {:ok, runner_result(work)}

            error ->
              error
          end

        {:relation_inspection, request} ->
          runner.inspect_relation(request, opts)

        {:generation_capabilities,
         %GenerationCapabilitiesRequest{manifest: version, asset_ref: asset_ref}} ->
          case runner.generation_capabilities(version, asset_ref, opts) do
            {:ok, capabilities} ->
              {:ok, %GenerationCapabilitiesResult{capabilities: capabilities}}

            error ->
              error
          end

        {:generation_marker_read,
         %GenerationMarkerReadRequest{
           manifest: version,
           asset_ref: asset_ref,
           require_relation_instance?: require_relation_instance?
         }} ->
          marker_opts =
            Keyword.put(opts, :require_relation_instance?, require_relation_instance?)

          case runner.generation_marker(version, asset_ref, marker_opts) do
            {:ok, marker} -> {:ok, %GenerationMarkerReadResult{marker: marker}}
            error -> error
          end

        {:generation_marker_initialize, request} ->
          runner.initialize_generation_marker(request, opts)

        {:generation_activate, request} ->
          runner.activate_generation(request, opts)

        {:generation_reconcile, request} ->
          runner.reconcile_generation(request, opts)

        {:generation_discard, request} ->
          runner.discard_generation(request, opts)

        _other ->
          {:error, :unsupported_test_runner_task}
      end

    base = %RunnerTask{
      workspace_id: command.workspace_context.workspace_id,
      task_id: command.task_id,
      domain_identity: command.domain_identity,
      task_kind: command.task_kind,
      runner_pool: command.runner_pool,
      required_runner_release_id: command.required_runner_release_id,
      required_capability: command.required_capability,
      retry_class: command.retry_class,
      payload: payload,
      payload_hash: command.payload_hash,
      orchestration_context: command.orchestration_context,
      assignment_generation: 0,
      inserted_at: command.occurred_at
    }

    case result do
      {:ok, value} ->
        %{base | status: :succeeded, result: value, retry_class: :terminal}

      {:error, reason} ->
        %{base | status: :failed, error: reason}
    end
  end

  defp runner_result(work) do
    %RunnerResult{
      run_id: work.run_id,
      manifest_version_id: work.manifest_version_id,
      manifest_content_hash: work.manifest_content_hash,
      required_runner_release_id: work.required_runner_release_id,
      status: :ok,
      asset_results: [],
      metadata: RunnerWork.lifecycle_metadata(work)
    }
  end
end

Logger.configure(level: :warning)
ExUnit.start(capture_log: true)
