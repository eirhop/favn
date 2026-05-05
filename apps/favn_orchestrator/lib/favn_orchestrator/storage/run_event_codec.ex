defmodule FavnOrchestrator.Storage.RunEventCodec do
  @moduledoc false

  alias FavnOrchestrator.Storage.JsonSafe

  @format "favn.run_event.storage.v1"

  @spec encode(map()) :: {:ok, String.t()} | {:error, term()}
  def encode(event) when is_map(event) do
    {:ok, Jason.encode!(event_to_dto(event))}
  rescue
    error -> {:error, {:run_event_encode_failed, error}}
  end

  @spec decode(String.t()) :: {:ok, map()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    with {:ok, %{"format" => @format, "schema_version" => 1} = dto} <- Jason.decode(payload),
         {:ok, occurred_at} <- normalize_occurred_at(Map.get(dto, "occurred_at")) do
      {:ok,
       %{
         schema_version: 1,
         run_id: Map.get(dto, "run_id"),
         sequence: Map.get(dto, "sequence"),
         event_type: existing_atom_or_string(Map.get(dto, "event_type")),
         entity: entity_from_dto(Map.get(dto, "entity")),
         occurred_at: occurred_at,
         status: existing_atom_or_string(Map.get(dto, "status")),
         global_sequence: normalize_global_sequence(Map.get(dto, "global_sequence")),
         manifest_version_id: normalize_optional_binary(Map.get(dto, "manifest_version_id")),
         manifest_content_hash: normalize_optional_binary(Map.get(dto, "manifest_content_hash")),
         asset_ref: ref_from_dto(Map.get(dto, "asset_ref")),
         stage: normalize_stage(Map.get(dto, "stage"), %{}),
         data: data_from_dto(Map.get(dto, "data"))
       }}
    else
      {:ok, other} -> {:error, {:invalid_run_event_dto, other}}
      {:error, reason} -> {:error, {:invalid_run_event_json, reason}}
    end
  end

  @spec normalize(String.t(), map()) :: {:ok, map()} | {:error, term()}
  def normalize(run_id, event) when is_binary(run_id) and is_map(event) do
    with {:ok, sequence} <- validate_sequence(Map.get(event, :sequence)),
         :ok <- validate_run_id(run_id, Map.get(event, :run_id)),
         {:ok, event_type} <- validate_event_type(Map.get(event, :event_type)),
         {:ok, occurred_at} <- normalize_occurred_at(Map.get(event, :occurred_at)),
         {:ok, status} <- normalize_status(Map.get(event, :status)) do
      raw_data = Map.get(event, :data)
      data = normalize_data(raw_data)

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
         asset_ref: normalize_asset_ref(Map.get(event, :asset_ref), raw_data),
         stage: normalize_stage(Map.get(event, :stage), raw_data),
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

  defp normalize_asset_ref(_value, _data), do: nil

  defp normalize_stage(value, _data) when is_integer(value) and value >= 0, do: value

  defp normalize_stage(_value, data) when is_map(data) do
    case Map.get(data, :stage) do
      stage when is_integer(stage) and stage >= 0 -> stage
      _ -> nil
    end
  end

  defp normalize_stage(_value, _data), do: nil

  defp normalize_data(data) when is_map(data), do: JsonSafe.data(data)
  defp normalize_data(_value), do: %{}

  defp event_to_dto(event) when is_map(event) do
    data = JsonSafe.data(Map.get(event, :data, %{}))

    %{
      "format" => @format,
      "schema_version" => 1,
      "run_id" => Map.get(event, :run_id),
      "sequence" => Map.get(event, :sequence),
      "event_type" => stringify(Map.get(event, :event_type)),
      "entity" => stringify(Map.get(event, :entity)),
      "occurred_at" => datetime_to_dto(Map.get(event, :occurred_at)),
      "status" => stringify(Map.get(event, :status)),
      "global_sequence" => Map.get(event, :global_sequence),
      "manifest_version_id" => Map.get(event, :manifest_version_id),
      "manifest_content_hash" => Map.get(event, :manifest_content_hash),
      "asset_ref" => JsonSafe.ref(Map.get(event, :asset_ref)),
      "stage" => Map.get(event, :stage),
      "data" => data
    }
  end

  defp data_from_dto(%{} = value),
    do: Map.new(value, fn {key, val} -> {key, data_from_dto(val)} end)

  defp data_from_dto(values) when is_list(values), do: Enum.map(values, &data_from_dto/1)
  defp data_from_dto(value), do: value

  defp ref_from_dto(%{"module" => module, "name" => name})
       when is_binary(module) and is_binary(name) do
    %{"module" => module, "name" => name}
  end

  defp ref_from_dto(_value), do: nil

  defp existing_atom_or_string(nil), do: nil

  defp existing_atom_or_string(value) when is_binary(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError -> value
  end

  defp existing_atom_or_string(value), do: value

  defp entity_from_dto("step"), do: :step
  defp entity_from_dto(_value), do: :run

  defp stringify(nil), do: nil
  defp stringify(value) when is_atom(value), do: Atom.to_string(value)
  defp stringify(value) when is_binary(value), do: value
  defp stringify(value), do: inspect(value)

  defp datetime_to_dto(nil), do: nil
  defp datetime_to_dto(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp datetime_to_dto(value) when is_binary(value), do: value
  defp datetime_to_dto(_value), do: DateTime.utc_now() |> DateTime.to_iso8601()
end
