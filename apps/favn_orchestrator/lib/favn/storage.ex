defmodule Favn.Storage do
  @moduledoc """
  Public storage facade for run and scheduler state persistence.
  """

  alias Favn.Run
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage, as: OrchestratorStorage
  alias FavnOrchestrator.Storage.Adapter.Memory, as: MemoryAdapter

  @default_adapter MemoryAdapter

  @type error :: :not_found | :invalid_opts | {:store_error, term()}

  @spec child_specs() :: {:ok, [Supervisor.child_spec()]} | {:error, error()}
  def child_specs do
    adapter = adapter_module()

    with :ok <- validate_adapter(adapter) do
      OrchestratorStorage.child_specs()
      |> normalize_result()
    end
  end

  @spec put_run(Run.t()) :: :ok | {:error, error()}
  def put_run(%Run{} = run) do
    with {:ok, run_state} <- to_run_state(run),
         :ok <- OrchestratorStorage.put_run(run_state) do
      :ok
    else
      {:error, reason} -> normalize_error(reason)
    end
  end

  @spec get_run(term()) :: {:ok, term()} | {:error, error()}
  def get_run(run_id) when is_binary(run_id) do
    case OrchestratorStorage.get_run(run_id) do
      {:ok, run_state} -> {:ok, Projector.project_run(run_state)}
      {:error, reason} -> normalize_error(reason)
    end
  end

  def get_run(_run_id), do: {:error, :invalid_opts}

  @spec list_runs(Favn.list_runs_opts()) :: {:ok, [term()]} | {:error, error()}
  def list_runs(opts \\ []) when is_list(opts) do
    with :ok <- validate_list_opts(opts),
         {:ok, run_states} <- OrchestratorStorage.list_runs(opts) do
      {:ok, Projector.project_runs(run_states)}
    else
      {:error, reason} -> normalize_error(reason)
    end
  end

  @spec adapter_module() :: module()
  def adapter_module do
    Application.get_env(:favn_orchestrator, :storage_adapter, @default_adapter)
  end

  @spec adapter_opts() :: keyword()
  def adapter_opts do
    Application.get_env(:favn_orchestrator, :storage_adapter_opts, [])
  end

  @spec validate_adapter(module()) :: :ok | {:error, error()}
  def validate_adapter(adapter) when is_atom(adapter) do
    case OrchestratorStorage.validate_adapter(adapter) do
      :ok -> :ok
      {:error, reason} -> {:error, {:store_error, reason}}
    end
  end

  def validate_adapter(_adapter), do: {:error, {:store_error, :invalid_storage_adapter}}

  defp to_run_state(%Run{} = run) do
    with :ok <- validate_run_identity(run.id, run.manifest_version_id, run.manifest_content_hash),
         :ok <- validate_asset_ref(run.asset_ref) do
      now = DateTime.utc_now()

      run_state =
        %RunState{
          id: run.id,
          manifest_version_id: run.manifest_version_id,
          manifest_content_hash: run.manifest_content_hash,
          asset_ref: run.asset_ref,
          target_refs: normalize_refs(run.target_refs),
          plan: run.plan,
          status: normalize_status(run.status),
          event_seq: normalize_event_seq(run.event_seq),
          params: normalize_map(run.params),
          trigger: normalize_map(run.trigger),
          metadata: normalize_map(run.metadata),
          submit_kind: normalize_submit_kind(run.submit_kind),
          rerun_of_run_id: normalize_optional_string(run.rerun_of_run_id),
          parent_run_id: normalize_optional_string(run.parent_run_id),
          root_run_id: normalize_optional_string(run.root_run_id),
          lineage_depth: normalize_non_neg_int(run.lineage_depth, 0),
          max_attempts: normalize_max_attempts(run.retry_policy),
          retry_backoff_ms: normalize_non_neg_int(run.retry_backoff_ms, 0),
          timeout_ms: normalize_positive_int(run.timeout_ms, 5_000),
          runner_execution_id: normalize_optional_string(run.runner_execution_id),
          result: run.result,
          error: run.error,
          inserted_at: run.started_at || now,
          updated_at: run.finished_at || now
        }
        |> RunState.with_snapshot_hash()

      {:ok, run_state}
    end
  end

  defp validate_run_identity(id, manifest_version_id, manifest_content_hash)
       when is_binary(id) and id != "" and is_binary(manifest_version_id) and
              manifest_version_id != "" and is_binary(manifest_content_hash) and
              manifest_content_hash != "" do
    :ok
  end

  defp validate_run_identity(_id, _manifest_version_id, _manifest_content_hash),
    do: {:error, :invalid_opts}

  defp validate_asset_ref({module, name}) when is_atom(module) and is_atom(name), do: :ok
  defp validate_asset_ref(_asset_ref), do: {:error, :invalid_opts}

  defp validate_list_opts(opts) do
    status = Keyword.get(opts, :status)
    limit = Keyword.get(opts, :limit)

    cond do
      not is_nil(status) and status not in [:running, :ok, :error, :cancelled, :timed_out] ->
        {:error, :invalid_opts}

      not is_nil(limit) and (not is_integer(limit) or limit <= 0) ->
        {:error, :invalid_opts}

      true ->
        :ok
    end
  end

  defp normalize_result({:ok, _value} = ok), do: ok
  defp normalize_result({:error, reason}), do: normalize_error(reason)

  defp normalize_error(:not_found), do: {:error, :not_found}
  defp normalize_error(:invalid_opts), do: {:error, :invalid_opts}
  defp normalize_error({:store_error, _reason} = error), do: {:error, error}
  defp normalize_error(reason), do: {:error, {:store_error, reason}}

  defp normalize_status(:ok), do: :ok
  defp normalize_status(:error), do: :error
  defp normalize_status(:cancelled), do: :cancelled
  defp normalize_status(:timed_out), do: :timed_out
  defp normalize_status(_status), do: :running

  defp normalize_event_seq(value) when is_integer(value) and value > 0, do: value
  defp normalize_event_seq(_value), do: 1

  defp normalize_submit_kind(:pipeline), do: :pipeline
  defp normalize_submit_kind(:rerun), do: :rerun
  defp normalize_submit_kind(_submit_kind), do: :manual

  defp normalize_map(value) when is_map(value), do: value
  defp normalize_map(_value), do: %{}

  defp normalize_refs(value) when is_list(value) do
    value
    |> Enum.filter(fn
      {module, name} when is_atom(module) and is_atom(name) -> true
      _other -> false
    end)
    |> Enum.uniq()
  end

  defp normalize_refs(_value), do: []

  defp normalize_optional_string(value) when is_binary(value) and value != "", do: value
  defp normalize_optional_string(_value), do: nil

  defp normalize_non_neg_int(value, _default) when is_integer(value) and value >= 0, do: value
  defp normalize_non_neg_int(_value, default), do: default

  defp normalize_positive_int(value, _default) when is_integer(value) and value > 0, do: value
  defp normalize_positive_int(_value, default), do: default

  defp normalize_max_attempts(retry_policy) when is_map(retry_policy) do
    case Map.get(retry_policy, :max_attempts) do
      value when is_integer(value) and value > 0 -> value
      _other -> 1
    end
  end

  defp normalize_max_attempts(_retry_policy), do: 1
end
