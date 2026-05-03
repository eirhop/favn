defmodule FavnOrchestrator.Storage.RunEventCodec do
  @moduledoc false

  @spec normalize(String.t(), map()) :: {:ok, map()} | {:error, term()}
  def normalize(run_id, event) when is_binary(run_id) and is_map(event) do
    with {:ok, sequence} <- validate_sequence(Map.get(event, :sequence)),
         :ok <- validate_run_id(run_id, Map.get(event, :run_id)),
         {:ok, event_type} <- validate_event_type(Map.get(event, :event_type)),
         {:ok, occurred_at} <- normalize_occurred_at(Map.get(event, :occurred_at)),
         {:ok, status} <- normalize_status(Map.get(event, :status)) do
      data = normalize_data(Map.get(event, :data))

      {:ok,
       %{
         schema_version: normalize_schema_version(Map.get(event, :schema_version)),
         run_id: run_id,
         sequence: sequence,
         event_type: event_type,
         entity: normalize_entity(Map.get(event, :entity), event_type),
         occurred_at: occurred_at,
         status: status,
         global_sequence: normalize_global_sequence(Map.get(event, :global_sequence)),
         manifest_version_id: normalize_optional_binary(Map.get(event, :manifest_version_id)),
         manifest_content_hash: normalize_optional_binary(Map.get(event, :manifest_content_hash)),
         asset_ref: normalize_asset_ref(Map.get(event, :asset_ref), data),
         stage: normalize_stage(Map.get(event, :stage), data),
         data: data
       }}
    end
  end

  defp normalize_schema_version(value) when is_integer(value) and value > 0, do: value
  defp normalize_schema_version(_value), do: 1

  defp normalize_global_sequence(value) when is_integer(value) and value > 0, do: value
  defp normalize_global_sequence(_value), do: nil

  defp validate_sequence(sequence) when is_integer(sequence) and sequence > 0, do: {:ok, sequence}
  defp validate_sequence(value), do: {:error, {:invalid_run_event_field, :sequence, value}}

  defp validate_run_id(_run_id, nil), do: :ok
  defp validate_run_id(run_id, run_id), do: :ok
  defp validate_run_id(_run_id, value), do: {:error, {:invalid_run_event_field, :run_id, value}}

  defp validate_event_type(value) when is_atom(value) and not is_nil(value), do: {:ok, value}
  defp validate_event_type(value) when is_binary(value) and value != "", do: {:ok, value}
  defp validate_event_type(value), do: {:error, {:invalid_run_event_field, :event_type, value}}

  defp normalize_occurred_at(nil), do: {:ok, DateTime.utc_now()}
  defp normalize_occurred_at(%DateTime{} = occurred_at), do: {:ok, occurred_at}

  defp normalize_occurred_at(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, occurred_at, _offset} -> {:ok, occurred_at}
      _ -> {:error, {:invalid_run_event_field, :occurred_at, value}}
    end
  end

  defp normalize_occurred_at(value), do: {:error, {:invalid_run_event_field, :occurred_at, value}}

  defp normalize_status(nil), do: {:ok, nil}
  defp normalize_status(status) when is_atom(status), do: {:ok, status}
  defp normalize_status(status) when is_binary(status), do: {:ok, status}
  defp normalize_status(value), do: {:error, {:invalid_run_event_field, :status, value}}

  defp normalize_entity(:run, _event_type), do: :run
  defp normalize_entity(:step, _event_type), do: :step

  defp normalize_entity(_value, event_type) when is_atom(event_type) do
    if String.starts_with?(Atom.to_string(event_type), "step_"), do: :step, else: :run
  end

  defp normalize_entity(_value, event_type) when is_binary(event_type) do
    if String.starts_with?(event_type, "step_"), do: :step, else: :run
  end

  defp normalize_optional_binary(nil), do: nil
  defp normalize_optional_binary(value) when is_binary(value) and value != "", do: value
  defp normalize_optional_binary(_value), do: nil

  defp normalize_asset_ref({module, name} = ref, _data) when is_atom(module) and is_atom(name),
    do: ref

  defp normalize_asset_ref(_value, data) when is_map(data) do
    case Map.get(data, :asset_ref) do
      {module, name} = ref when is_atom(module) and is_atom(name) -> ref
      _ -> nil
    end
  end

  defp normalize_stage(value, _data) when is_integer(value) and value >= 0, do: value

  defp normalize_stage(_value, data) when is_map(data) do
    case Map.get(data, :stage) do
      stage when is_integer(stage) and stage >= 0 -> stage
      _ -> nil
    end
  end

  defp normalize_data(data) when is_map(data), do: data
  defp normalize_data(_value), do: %{}
end
