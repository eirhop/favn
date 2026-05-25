defmodule FavnOrchestrator.Storage.RunStateCodec do
  @moduledoc false

  alias FavnOrchestrator.RunState

  @spec normalize(RunState.t()) :: {:ok, RunState.t()} | {:error, term()}
  def normalize(%RunState{} = run_state) do
    with :ok <- validate_identity(run_state),
         :ok <- validate_asset_ref(run_state.asset_ref),
         :ok <- validate_event_seq(run_state.event_seq) do
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

  defp validate_event_seq(event_seq) when is_integer(event_seq) and event_seq >= 0, do: :ok
  defp validate_event_seq(_value), do: {:error, :invalid_event_seq}

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
