defmodule FavnOrchestrator.Storage.RunStateCodec do
  @moduledoc false

  alias Favn.Plan
  alias FavnOrchestrator.RunState

  @statuses [:pending, :running, :ok, :partial, :error, :cancelled, :timed_out]
  @submit_kinds [:manual, :rerun, :pipeline, :backfill_asset, :backfill_pipeline]

  @spec normalize(RunState.t()) :: {:ok, RunState.t()} | {:error, term()}
  def normalize(%RunState{} = run_state) do
    with :ok <- validate_identity(run_state),
         :ok <- validate_asset_ref(run_state.asset_ref),
         :ok <- validate_refs(run_state.target_refs, :target_refs),
         :ok <- validate_plan(run_state.plan),
         :ok <- validate_status(run_state.status),
         :ok <- validate_event_seq(run_state.event_seq),
         :ok <- validate_map(run_state.params, :params),
         :ok <- validate_map(run_state.trigger, :trigger),
         :ok <- validate_map(run_state.metadata, :metadata),
         :ok <- validate_submit_kind(run_state.submit_kind),
         :ok <- validate_optional_string(run_state.rerun_of_run_id, :rerun_of_run_id),
         :ok <- validate_optional_string(run_state.parent_run_id, :parent_run_id),
         :ok <- validate_optional_string(run_state.root_run_id, :root_run_id),
         :ok <- validate_non_neg_integer(run_state.lineage_depth, :lineage_depth),
         :ok <- validate_positive_integer(run_state.max_attempts, :max_attempts),
         :ok <- validate_non_neg_integer(run_state.retry_backoff_ms, :retry_backoff_ms),
         :ok <- validate_positive_integer(run_state.timeout_ms, :timeout_ms),
         :ok <- validate_optional_string(run_state.runner_execution_id, :runner_execution_id),
         :ok <- validate_result(run_state.result),
         :ok <- validate_datetime(run_state.inserted_at, :inserted_at),
         :ok <- validate_datetime(run_state.updated_at, :updated_at) do
      {:ok, run_state |> normalize_legacy_finalized_snapshot() |> RunState.with_snapshot_hash()}
    end
  end

  @spec to_record(RunState.t()) :: {:ok, map()} | {:error, term()}
  def to_record(%RunState{} = run_state) do
    with {:ok, normalized} <- normalize(run_state) do
      {:ok, Map.from_struct(normalized)}
    end
  end

  @spec from_record(map()) :: {:ok, RunState.t()} | {:error, term()}
  def from_record(record) when is_map(record) do
    run_state = struct(RunState, record)
    normalize(run_state)
  end

  defp validate_identity(%RunState{} = run_state) do
    cond do
      not (is_binary(run_state.id) and run_state.id != "") ->
        {:error, :invalid_run_id}

      not (is_binary(run_state.manifest_version_id) and run_state.manifest_version_id != "") ->
        {:error, :invalid_manifest_version_id}

      not (is_binary(run_state.manifest_content_hash) and run_state.manifest_content_hash != "") ->
        {:error, :invalid_manifest_content_hash}

      true ->
        :ok
    end
  end

  defp validate_asset_ref({module, name}) when is_atom(module) and is_atom(name), do: :ok
  defp validate_asset_ref(_value), do: {:error, :invalid_asset_ref}

  defp validate_refs(refs, _field) when is_list(refs) do
    if Enum.all?(refs, &(validate_asset_ref(&1) == :ok)),
      do: :ok,
      else: {:error, :invalid_target_refs}
  end

  defp validate_refs(_refs, :target_refs), do: {:error, :invalid_target_refs}

  defp validate_plan(nil), do: :ok
  defp validate_plan(%Plan{}), do: :ok
  defp validate_plan(_plan), do: {:error, :invalid_plan}

  defp validate_status(status) when status in @statuses, do: :ok
  defp validate_status(_status), do: {:error, :invalid_status}

  defp validate_event_seq(event_seq) when is_integer(event_seq) and event_seq >= 0, do: :ok
  defp validate_event_seq(_value), do: {:error, :invalid_event_seq}

  defp validate_map(value, _field) when is_map(value), do: :ok
  defp validate_map(_value, field), do: {:error, {:invalid_run_field, field}}

  defp validate_submit_kind(kind) when kind in @submit_kinds, do: :ok
  defp validate_submit_kind(_kind), do: {:error, {:invalid_run_field, :submit_kind}}

  defp validate_optional_string(nil, _field), do: :ok
  defp validate_optional_string(value, _field) when is_binary(value) and value != "", do: :ok
  defp validate_optional_string(_value, field), do: {:error, {:invalid_run_field, field}}

  defp validate_non_neg_integer(value, _field) when is_integer(value) and value >= 0, do: :ok
  defp validate_non_neg_integer(_value, field), do: {:error, {:invalid_run_field, field}}

  defp validate_positive_integer(value, _field) when is_integer(value) and value > 0, do: :ok
  defp validate_positive_integer(_value, field), do: {:error, {:invalid_run_field, field}}

  defp validate_result(nil), do: :ok
  defp validate_result(result) when is_map(result), do: :ok
  defp validate_result(_result), do: {:error, {:invalid_run_field, :result}}

  defp validate_datetime(nil, _field), do: :ok
  defp validate_datetime(%DateTime{}, _field), do: :ok
  defp validate_datetime(_value, field), do: {:error, {:invalid_run_field, field}}

  defp normalize_legacy_finalized_snapshot(%RunState{} = run_state) do
    if legacy_finalized_snapshot?(run_state) do
      %{
        run_state
        | metadata:
            Map.put(run_state.metadata, :terminal_event_type, terminal_event_type(run_state))
      }
    else
      run_state
    end
  end

  defp legacy_finalized_snapshot?(%RunState{metadata: metadata} = run_state)
       when is_map(metadata) do
    RunState.terminal_status?(run_state.status) and not RunState.finalized?(run_state) and
      terminal_result?(run_state.result, run_state.status)
  end

  defp legacy_finalized_snapshot?(%RunState{}), do: false

  defp terminal_result?(%{status: result_status, asset_results: asset_results}, status)
       when is_list(asset_results) do
    matching_status?(result_status, status)
  end

  defp terminal_result?(%{"status" => result_status, "asset_results" => asset_results}, status)
       when is_list(asset_results) do
    matching_status?(result_status, status)
  end

  defp terminal_result?(_result, _status), do: false

  defp matching_status?(left, right) when is_atom(left) and is_atom(right), do: left == right
  defp matching_status?(left, right) when is_binary(left) and is_binary(right), do: left == right

  defp matching_status?(left, right) when is_atom(left) and is_binary(right),
    do: Atom.to_string(left) == right

  defp matching_status?(left, right) when is_binary(left) and is_atom(right),
    do: left == Atom.to_string(right)

  defp matching_status?(_left, _right), do: false

  defp terminal_event_type(%RunState{status: status}), do: RunState.terminal_event_type(status)
end
