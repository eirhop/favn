defmodule FavnOrchestrator.Storage.RunEventCodec do
  @moduledoc false

  alias FavnOrchestrator.RunEvents.EventType
  alias FavnOrchestrator.Storage.JsonSafe

  @format "favn.run_event.storage.v1"
  @dto_fields ~w(format schema_version run_id sequence event_type entity occurred_at status
                 global_sequence manifest_version_id manifest_content_hash asset_ref stage data)

  @spec encode(map()) :: {:ok, String.t()} | {:error, term()}
  def encode(event) when is_map(event) do
    with {:ok, run_id} <- required_binary(:run_id, Map.get(event, :run_id)),
         {:ok, normalized} <- normalize(run_id, event) do
      {:ok, Jason.encode!(event_to_dto(normalized))}
    end
  rescue
    error -> {:error, {:run_event_encode_failed, error}}
  end

  def encode(event), do: {:error, {:invalid_run_event, event}}

  @spec decode(String.t()) :: {:ok, map()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    with {:ok, dto} <- decode_dto(payload),
         :ok <- reject_unknown_fields(dto),
         {:ok, run_id} <- required_binary(:run_id, Map.get(dto, "run_id")),
         {:ok, sequence} <- validate_sequence(Map.get(dto, "sequence")),
         {:ok, event_type} <- validate_event_type(atom_or_string(Map.get(dto, "event_type"))),
         {:ok, entity} <- entity_from_dto(Map.get(dto, "entity")),
         {:ok, occurred_at} <- decode_occurred_at(Map.get(dto, "occurred_at")),
         {:ok, status} <- normalize_status(atom_or_string(Map.get(dto, "status"))),
         {:ok, global_sequence} <- optional_positive_integer(:global_sequence, dto),
         {:ok, manifest_version_id} <- optional_binary(:manifest_version_id, dto),
         {:ok, manifest_content_hash} <- optional_binary(:manifest_content_hash, dto),
         {:ok, asset_ref} <- ref_from_dto(Map.get(dto, "asset_ref")),
         {:ok, stage} <- optional_non_negative_integer(:stage, dto),
         {:ok, data} <- decoded_data(Map.get(dto, "data")) do
      {:ok,
       %{
         schema_version: 1,
         run_id: run_id,
         sequence: sequence,
         event_type: event_type,
         entity: entity,
         occurred_at: occurred_at,
         status: status,
         global_sequence: global_sequence,
         manifest_version_id: manifest_version_id,
         manifest_content_hash: manifest_content_hash,
         asset_ref: asset_ref,
         stage: stage,
         data: data
       }}
    end
  end

  def decode(payload), do: {:error, {:invalid_run_event_payload, payload}}

  @spec normalize(String.t(), map()) :: {:ok, map()} | {:error, term()}
  def normalize(run_id, event) when is_binary(run_id) and is_map(event) do
    with {:ok, run_id} <- required_binary(:run_id, run_id),
         {:ok, sequence} <- validate_sequence(Map.get(event, :sequence)),
         :ok <- validate_run_id(run_id, Map.get(event, :run_id)),
         {:ok, event_type} <- validate_event_type(Map.get(event, :event_type)),
         {:ok, occurred_at} <- normalize_occurred_at(Map.get(event, :occurred_at)),
         {:ok, status} <- normalize_status(Map.get(event, :status)),
         {:ok, schema_version} <- normalize_schema_version(Map.get(event, :schema_version)),
         {:ok, global_sequence} <-
           normalize_optional_positive_integer(:global_sequence, Map.get(event, :global_sequence)),
         {:ok, manifest_version_id} <-
           normalize_optional_binary(:manifest_version_id, Map.get(event, :manifest_version_id)),
         {:ok, manifest_content_hash} <-
           normalize_optional_binary(
             :manifest_content_hash,
             Map.get(event, :manifest_content_hash)
           ),
         {:ok, asset_ref} <-
           normalize_asset_ref(Map.get(event, :asset_ref), Map.get(event, :data)),
         {:ok, stage} <- normalize_stage(Map.get(event, :stage), Map.get(event, :data)),
         {:ok, data} <- normalize_data(Map.get(event, :data)) do
      {:ok,
       %{
         schema_version: schema_version,
         run_id: run_id,
         sequence: sequence,
         event_type: event_type,
         entity: normalize_entity(Map.get(event, :entity), event_type),
         occurred_at: occurred_at,
         status: status,
         global_sequence: global_sequence,
         manifest_version_id: manifest_version_id,
         manifest_content_hash: manifest_content_hash,
         asset_ref: asset_ref,
         stage: stage,
         data: data
       }}
    end
  end

  def normalize(_run_id, event), do: {:error, {:invalid_run_event, event}}

  defp normalize_schema_version(nil), do: {:ok, 1}
  defp normalize_schema_version(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp normalize_schema_version(value),
    do: {:error, {:invalid_run_event_field, :schema_version, value}}

  defp validate_sequence(sequence) when is_integer(sequence) and sequence > 0, do: {:ok, sequence}
  defp validate_sequence(value), do: {:error, {:invalid_run_event_field, :sequence, value}}

  defp validate_run_id(_run_id, nil), do: :ok
  defp validate_run_id(run_id, run_id), do: :ok
  defp validate_run_id(_run_id, value), do: {:error, {:invalid_run_event_field, :run_id, value}}

  defp validate_event_type(value) when is_atom(value) and not is_nil(value) do
    if EventType.line_safe?(value) do
      {:ok, value}
    else
      {:error, {:invalid_run_event_field, :event_type, value}}
    end
  end

  defp validate_event_type(value) when is_binary(value) do
    if EventType.line_safe?(value) do
      {:ok, value}
    else
      {:error, {:invalid_run_event_field, :event_type, value}}
    end
  end

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
  defp normalize_status(status) when is_atom(status) and not is_nil(status), do: {:ok, status}

  defp normalize_status(status) when is_binary(status) and status != "", do: {:ok, status}

  defp normalize_status(value), do: {:error, {:invalid_run_event_field, :status, value}}

  defp normalize_entity(:run, _event_type), do: :run
  defp normalize_entity(:step, _event_type), do: :step

  defp normalize_entity(_value, event_type) when is_atom(event_type) do
    if String.starts_with?(Atom.to_string(event_type), "step_"), do: :step, else: :run
  end

  defp normalize_entity(_value, event_type) when is_binary(event_type) do
    if String.starts_with?(event_type, "step_"), do: :step, else: :run
  end

  defp normalize_asset_ref({module, name} = ref, _data) when is_atom(module) and is_atom(name),
    do: {:ok, ref}

  defp normalize_asset_ref(nil, data) when is_map(data) do
    case Map.get(data, :asset_ref) do
      {module, name} = ref when is_atom(module) and is_atom(name) -> {:ok, ref}
      nil -> {:ok, nil}
      value -> {:error, {:invalid_run_event_field, :asset_ref, value}}
    end
  end

  defp normalize_asset_ref(nil, _data), do: {:ok, nil}

  defp normalize_asset_ref(value, _data),
    do: {:error, {:invalid_run_event_field, :asset_ref, value}}

  defp normalize_stage(value, _data) when is_integer(value) and value >= 0, do: {:ok, value}

  defp normalize_stage(nil, data) when is_map(data) do
    case Map.get(data, :stage) do
      stage when is_integer(stage) and stage >= 0 -> {:ok, stage}
      nil -> {:ok, nil}
      value -> {:error, {:invalid_run_event_field, :stage, value}}
    end
  end

  defp normalize_stage(nil, _data), do: {:ok, nil}

  defp normalize_stage(value, _data),
    do: {:error, {:invalid_run_event_field, :stage, value}}

  defp normalize_data(nil), do: {:ok, %{}}
  defp normalize_data(data) when is_map(data), do: {:ok, JsonSafe.data(data)}

  defp normalize_data(value),
    do: {:error, {:invalid_run_event_field, :data, value}}

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

  defp ref_from_dto(nil), do: {:ok, nil}

  defp ref_from_dto(%{"module" => module, "name" => name})
       when is_binary(module) and is_binary(name) do
    {:ok, %{"module" => module, "name" => name}}
  end

  defp ref_from_dto(value), do: {:error, {:invalid_run_event_field, :asset_ref, value}}

  defp atom_or_string(nil), do: nil

  defp atom_or_string(value) when is_binary(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError -> value
  end

  defp atom_or_string(value), do: value

  defp entity_from_dto("step"), do: {:ok, :step}
  defp entity_from_dto("run"), do: {:ok, :run}

  defp entity_from_dto(value),
    do: {:error, {:invalid_run_event_field, :entity, value}}

  defp stringify(nil), do: nil
  defp stringify(value) when is_atom(value), do: Atom.to_string(value)
  defp stringify(value) when is_binary(value), do: value
  defp datetime_to_dto(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp decode_dto(payload) do
    case Jason.decode(payload) do
      {:ok, %{"format" => @format, "schema_version" => 1} = dto} -> {:ok, dto}
      {:ok, other} -> {:error, {:invalid_run_event_dto, other}}
      {:error, reason} -> {:error, {:invalid_run_event_json, reason}}
    end
  end

  defp reject_unknown_fields(dto) do
    case dto |> Map.keys() |> Kernel.--(@dto_fields) |> Enum.sort() do
      [] -> :ok
      fields -> {:error, {:unknown_run_event_fields, fields}}
    end
  end

  defp required_binary(_field, value) when is_binary(value) and value != "", do: {:ok, value}
  defp required_binary(field, value), do: {:error, {:invalid_run_event_field, field, value}}

  defp optional_binary(field, dto) do
    normalize_optional_binary(field, Map.get(dto, Atom.to_string(field)))
  end

  defp normalize_optional_binary(_field, nil), do: {:ok, nil}

  defp normalize_optional_binary(_field, value) when is_binary(value) and value != "",
    do: {:ok, value}

  defp normalize_optional_binary(field, value),
    do: {:error, {:invalid_run_event_field, field, value}}

  defp optional_positive_integer(field, dto) do
    normalize_optional_positive_integer(field, Map.get(dto, Atom.to_string(field)))
  end

  defp normalize_optional_positive_integer(_field, nil), do: {:ok, nil}

  defp normalize_optional_positive_integer(_field, value)
       when is_integer(value) and value > 0,
       do: {:ok, value}

  defp normalize_optional_positive_integer(field, value),
    do: {:error, {:invalid_run_event_field, field, value}}

  defp optional_non_negative_integer(field, dto) do
    case Map.get(dto, Atom.to_string(field)) do
      nil -> {:ok, nil}
      value when is_integer(value) and value >= 0 -> {:ok, value}
      value -> {:error, {:invalid_run_event_field, field, value}}
    end
  end

  defp decode_occurred_at(nil),
    do: {:error, {:invalid_run_event_field, :occurred_at, nil}}

  defp decode_occurred_at(value), do: normalize_occurred_at(value)

  defp decoded_data(%{} = value), do: {:ok, data_from_dto(value)}
  defp decoded_data(value), do: {:error, {:invalid_run_event_field, :data, value}}
end
