defmodule FavnOrchestrator.Storage.RunStateCodec do
  @moduledoc false

  alias FavnOrchestrator.RunState

  @spec normalize(RunState.t()) :: {:ok, RunState.t()} | {:error, term()}
  def normalize(%RunState{} = run_state) do
    with :ok <- validate_identity(run_state),
         :ok <- validate_asset_ref(run_state.asset_ref),
         :ok <- validate_event_seq(run_state.event_seq) do
      {:ok, RunState.with_snapshot_hash(run_state)}
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
end
