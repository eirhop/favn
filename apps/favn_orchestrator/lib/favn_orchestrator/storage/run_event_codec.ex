defmodule FavnOrchestrator.Storage.RunEventCodec do
  @moduledoc false

  @spec normalize(String.t(), map()) :: {:ok, map()} | {:error, term()}
  def normalize(run_id, event) when is_binary(run_id) and is_map(event) do
    with {:ok, sequence} <- validate_sequence(Map.get(event, :sequence)),
         :ok <- validate_run_id(run_id, Map.get(event, :run_id)),
         {:ok, event_type} <- validate_event_type(Map.get(event, :event_type)),
         {:ok, occurred_at} <- normalize_occurred_at(Map.get(event, :occurred_at)),
         {:ok, status} <- normalize_status(Map.get(event, :status)) do
      {:ok,
       %{
         run_id: run_id,
         sequence: sequence,
         event_type: event_type,
         occurred_at: occurred_at,
         status: status,
         manifest_version_id: normalize_optional_binary(Map.get(event, :manifest_version_id)),
         manifest_content_hash: normalize_optional_binary(Map.get(event, :manifest_content_hash)),
         asset_ref: normalize_asset_ref(Map.get(event, :asset_ref)),
         data: normalize_data(Map.get(event, :data))
       }}
    end
  end

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

  defp normalize_optional_binary(nil), do: nil
  defp normalize_optional_binary(value) when is_binary(value) and value != "", do: value
  defp normalize_optional_binary(_value), do: nil

  defp normalize_asset_ref({module, name} = ref) when is_atom(module) and is_atom(name), do: ref
  defp normalize_asset_ref(_value), do: nil

  defp normalize_data(data) when is_map(data), do: data
  defp normalize_data(_value), do: %{}
end
