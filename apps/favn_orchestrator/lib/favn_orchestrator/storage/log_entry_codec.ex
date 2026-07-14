defmodule FavnOrchestrator.Storage.LogEntryCodec do
  @moduledoc false

  alias Favn.Log.Entry, as: LogEntry
  alias FavnOrchestrator.Storage.JsonSafe
  alias FavnOrchestrator.Storage.PayloadCodec

  @format "favn.log_entry.storage.v1"
  @dto_fields ~w(format schema_version id global_sequence run_id asset_step_id node_key
                 node_key_payload asset_ref asset_ref_payload runner_execution_id attempt
                 producer_id producer_sequence occurred_at level source stream message metadata
                 truncated)
  @fields [
    :schema_version,
    :id,
    :global_sequence,
    :run_id,
    :asset_step_id,
    :node_key,
    :asset_ref,
    :runner_execution_id,
    :attempt,
    :producer_id,
    :producer_sequence,
    :occurred_at,
    :level,
    :source,
    :stream,
    :message,
    :metadata,
    :truncated
  ]

  @spec normalize(term()) :: {:ok, LogEntry.t()} | {:error, term()}
  def normalize(entry) do
    attrs = entry |> entry_to_map() |> Map.take(@fields)

    with {:ok, occurred_at} <- normalize_datetime(Map.get(attrs, :occurred_at)),
         {:ok, message} <- normalize_message(Map.get(attrs, :message)),
         {:ok, schema_version} <- optional_positive(:schema_version, attrs, 1),
         {:ok, id} <- normalize_id(Map.get(attrs, :id)),
         {:ok, global_sequence} <- optional_positive(:global_sequence, attrs),
         {:ok, run_id} <- optional_binary(:run_id, attrs),
         {:ok, asset_step_id} <- optional_binary(:asset_step_id, attrs),
         {:ok, runner_execution_id} <- optional_binary(:runner_execution_id, attrs),
         {:ok, attempt} <- optional_positive(:attempt, attrs),
         {:ok, producer_id} <- optional_binary(:producer_id, attrs),
         {:ok, producer_sequence} <- optional_non_negative(:producer_sequence, attrs),
         {:ok, metadata} <- normalize_metadata(Map.get(attrs, :metadata)),
         {:ok, truncated} <- normalize_truncated(Map.get(attrs, :truncated)) do
      attrs =
        attrs
        |> Map.put(:schema_version, schema_version)
        |> Map.put(:id, id)
        |> Map.put(:global_sequence, global_sequence)
        |> Map.put(:run_id, run_id)
        |> Map.put(:asset_step_id, asset_step_id)
        |> Map.put(:runner_execution_id, runner_execution_id)
        |> Map.put(:attempt, attempt)
        |> Map.put(:producer_id, producer_id)
        |> Map.put(:producer_sequence, producer_sequence)
        |> Map.put(:occurred_at, occurred_at)
        |> Map.put(:message, message)
        |> Map.put(:metadata, metadata)
        |> Map.put(:truncated, truncated)

      {:ok, LogEntry.normalize(attrs)}
    end
  rescue
    error -> {:error, {:invalid_log_entry, error}}
  catch
    kind, reason -> {:error, {:invalid_log_entry, {kind, reason}}}
  end

  defp entry_to_map(%_{} = entry), do: Map.from_struct(entry)
  defp entry_to_map(entry) when is_list(entry), do: Map.new(entry)
  defp entry_to_map(entry) when is_map(entry), do: entry

  @spec assign_global_sequence(LogEntry.t(), pos_integer()) :: LogEntry.t()
  def assign_global_sequence(entry, global_sequence)
      when is_integer(global_sequence) and global_sequence > 0 do
    struct(entry, global_sequence: global_sequence)
  end

  @spec encode(LogEntry.t()) :: {:ok, String.t()} | {:error, term()}
  def encode(entry) do
    {:ok, Jason.encode!(to_dto(entry))}
  rescue
    error -> {:error, {:log_entry_encode_failed, error}}
  end

  @spec decode(String.t()) :: {:ok, LogEntry.t()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    with {:ok, dto} <- decode_dto(payload),
         :ok <- reject_unknown_fields(dto),
         {:ok, _id} <- required_binary(:id, Map.get(dto, "id")),
         {:ok, _global_sequence} <- required_positive(:global_sequence, dto),
         {:ok, _occurred_at} <- required_datetime(Map.get(dto, "occurred_at")),
         {:ok, _message} <- required_binary(:message, Map.get(dto, "message")),
         {:ok, node_key} <-
           decode_payload_or_value(Map.get(dto, "node_key_payload"), Map.get(dto, "node_key")),
         {:ok, asset_ref} <-
           decode_payload_or_value(Map.get(dto, "asset_ref_payload"), Map.get(dto, "asset_ref")) do
      normalize(%{
        schema_version: 1,
        id: Map.get(dto, "id"),
        global_sequence: Map.get(dto, "global_sequence"),
        run_id: Map.get(dto, "run_id"),
        asset_step_id: Map.get(dto, "asset_step_id"),
        node_key: node_key,
        asset_ref: asset_ref,
        runner_execution_id: Map.get(dto, "runner_execution_id"),
        attempt: Map.get(dto, "attempt"),
        producer_id: Map.get(dto, "producer_id"),
        producer_sequence: Map.get(dto, "producer_sequence"),
        occurred_at: Map.get(dto, "occurred_at"),
        level: atom_or_string(Map.get(dto, "level")),
        source: atom_or_string(Map.get(dto, "source")),
        stream: atom_or_string(Map.get(dto, "stream")),
        message: Map.get(dto, "message"),
        metadata: Map.get(dto, "metadata"),
        truncated: Map.get(dto, "truncated")
      })
    end
  end

  def decode(payload), do: {:error, {:invalid_log_entry_payload, payload}}

  @spec node_key_storage(term()) :: {String.t() | nil, String.t() | nil}
  def node_key_storage(nil), do: {nil, nil}

  def node_key_storage(node_key) do
    case PayloadCodec.encode(node_key) do
      {:ok, blob} ->
        {:crypto.hash(:sha256, blob) |> Base.encode16(case: :lower), blob}

      {:error, _reason} ->
        inspected = inspect(node_key)
        {:crypto.hash(:sha256, inspected) |> Base.encode16(case: :lower), nil}
    end
  end

  @spec asset_ref_storage(term()) :: {String.t() | nil, String.t() | nil}
  def asset_ref_storage(nil), do: {nil, nil}

  def asset_ref_storage(asset_ref) do
    case PayloadCodec.encode(asset_ref) do
      {:ok, blob} -> {asset_ref_key(asset_ref), blob}
      {:error, _reason} -> {asset_ref_key(asset_ref), nil}
    end
  end

  defp to_dto(entry) do
    attrs = Map.from_struct(entry)

    %{
      "format" => @format,
      "schema_version" => 1,
      "id" => Map.get(attrs, :id),
      "global_sequence" => Map.get(attrs, :global_sequence),
      "run_id" => Map.get(attrs, :run_id),
      "asset_step_id" => Map.get(attrs, :asset_step_id),
      "node_key" => JsonSafe.data(Map.get(attrs, :node_key)),
      "node_key_payload" => payload_to_dto(Map.get(attrs, :node_key)),
      "asset_ref" => JsonSafe.data(Map.get(attrs, :asset_ref)),
      "asset_ref_payload" => payload_to_dto(Map.get(attrs, :asset_ref)),
      "runner_execution_id" => Map.get(attrs, :runner_execution_id),
      "attempt" => Map.get(attrs, :attempt),
      "producer_id" => Map.get(attrs, :producer_id),
      "producer_sequence" => Map.get(attrs, :producer_sequence),
      "occurred_at" => datetime_to_dto(Map.get(attrs, :occurred_at)),
      "level" => stringify(Map.get(attrs, :level)),
      "source" => stringify(Map.get(attrs, :source)),
      "stream" => stringify(Map.get(attrs, :stream)),
      "message" => Map.get(attrs, :message) || "",
      "metadata" => JsonSafe.data(Map.get(attrs, :metadata) || %{}),
      "truncated" => Map.get(attrs, :truncated) == true
    }
  end

  defp normalize_id(nil), do: {:ok, random_uuid()}
  defp normalize_id(value) when is_binary(value) and value != "", do: {:ok, value}
  defp normalize_id(value), do: {:error, {:invalid_log_entry_field, :id, value}}

  defp random_uuid do
    <<a::32, b::16, c::16, d::16, e::48>> = :crypto.strong_rand_bytes(16)

    c = Bitwise.bor(Bitwise.band(c, 0x0FFF), 0x4000)
    d = Bitwise.bor(Bitwise.band(d, 0x3FFF), 0x8000)

    [
      Base.encode16(<<a::32>>, case: :lower),
      Base.encode16(<<b::16>>, case: :lower),
      Base.encode16(<<c::16>>, case: :lower),
      Base.encode16(<<d::16>>, case: :lower),
      Base.encode16(<<e::48>>, case: :lower)
    ]
    |> Enum.join("-")
  end

  defp normalize_message(value) when is_binary(value), do: {:ok, value}
  defp normalize_message(nil), do: {:ok, ""}
  defp normalize_message(value), do: {:error, {:invalid_log_entry_field, :message, value}}

  defp normalize_datetime(nil), do: {:ok, DateTime.utc_now()}
  defp normalize_datetime(%DateTime{} = value), do: {:ok, value}

  defp normalize_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      _ -> {:error, {:invalid_log_entry_field, :occurred_at, value}}
    end
  end

  defp normalize_datetime(value), do: {:error, {:invalid_log_entry_field, :occurred_at, value}}

  defp normalize_metadata(nil), do: {:ok, %{}}
  defp normalize_metadata(value) when is_map(value), do: {:ok, JsonSafe.data(value)}

  defp normalize_metadata(value),
    do: {:error, {:invalid_log_entry_field, :metadata, value}}

  defp normalize_truncated(nil), do: {:ok, false}
  defp normalize_truncated(value) when is_boolean(value), do: {:ok, value}

  defp normalize_truncated(value),
    do: {:error, {:invalid_log_entry_field, :truncated, value}}

  defp atom_or_string(nil), do: nil

  defp atom_or_string(value) when is_binary(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError -> value
  end

  defp atom_or_string(value), do: value

  defp stringify(nil), do: nil
  defp stringify(value) when is_atom(value), do: Atom.to_string(value)
  defp stringify(value) when is_binary(value), do: value
  defp stringify(value), do: inspect(value)

  defp datetime_to_dto(nil), do: nil
  defp datetime_to_dto(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp datetime_to_dto(value) when is_binary(value), do: value

  defp payload_to_dto(nil), do: nil

  defp payload_to_dto(value) do
    case PayloadCodec.encode(value) do
      {:ok, payload} -> payload
      {:error, _reason} -> nil
    end
  end

  defp decode_payload_or_value(payload, _fallback) when is_binary(payload) do
    case PayloadCodec.decode(payload) do
      {:ok, value} -> {:ok, value}
      {:error, reason} -> {:error, {:invalid_log_entry_payload_field, reason}}
    end
  end

  defp decode_payload_or_value(nil, fallback), do: {:ok, fallback}

  defp decode_payload_or_value(payload, _fallback),
    do: {:error, {:invalid_log_entry_payload_field, payload}}

  defp asset_ref_key({module, name}) when is_atom(module) and is_atom(name),
    do: Atom.to_string(module) <> "." <> Atom.to_string(name)

  defp asset_ref_key(%{"module" => module, "name" => name})
       when is_binary(module) and is_binary(name),
       do: module <> "." <> name

  defp asset_ref_key(value),
    do: :crypto.hash(:sha256, inspect(value)) |> Base.encode16(case: :lower)

  defp decode_dto(payload) do
    case Jason.decode(payload) do
      {:ok, %{"format" => @format, "schema_version" => 1} = dto} -> {:ok, dto}
      {:ok, other} -> {:error, {:invalid_log_entry_dto, other}}
      {:error, reason} -> {:error, {:invalid_log_entry_json, reason}}
    end
  end

  defp reject_unknown_fields(dto) do
    case dto |> Map.keys() |> Kernel.--(@dto_fields) |> Enum.sort() do
      [] -> :ok
      fields -> {:error, {:unknown_log_entry_fields, fields}}
    end
  end

  defp required_binary(_field, value) when is_binary(value) and value != "", do: {:ok, value}
  defp required_binary(field, value), do: {:error, {:invalid_log_entry_field, field, value}}

  defp required_positive(field, dto) do
    case Map.get(dto, Atom.to_string(field)) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      value -> {:error, {:invalid_log_entry_field, field, value}}
    end
  end

  defp required_datetime(nil), do: {:error, {:invalid_log_entry_field, :occurred_at, nil}}
  defp required_datetime(value), do: normalize_datetime(value)

  defp optional_binary(field, attrs) do
    case Map.get(attrs, field) do
      nil -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      value -> {:error, {:invalid_log_entry_field, field, value}}
    end
  end

  defp optional_positive(field, attrs, default \\ nil) do
    case Map.get(attrs, field) do
      nil -> {:ok, default}
      value when is_integer(value) and value > 0 -> {:ok, value}
      value -> {:error, {:invalid_log_entry_field, field, value}}
    end
  end

  defp optional_non_negative(field, attrs) do
    case Map.get(attrs, field) do
      nil -> {:ok, nil}
      value when is_integer(value) and value >= 0 -> {:ok, value}
      value -> {:error, {:invalid_log_entry_field, field, value}}
    end
  end
end
