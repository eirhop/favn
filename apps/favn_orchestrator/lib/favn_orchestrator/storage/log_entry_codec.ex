defmodule FavnOrchestrator.Storage.LogEntryCodec do
  @moduledoc false

  alias FavnOrchestrator.Storage.JsonSafe
  alias FavnOrchestrator.Storage.PayloadCodec

  @format "favn.log_entry.storage.v1"
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

  @spec normalize(term()) :: {:ok, Favn.Log.Entry.t()} | {:error, term()}
  def normalize(entry) do
    attrs = entry |> entry_to_map() |> Map.take(@fields)

    with {:ok, occurred_at} <- normalize_datetime(Map.get(attrs, :occurred_at)),
         {:ok, message} <- normalize_message(Map.get(attrs, :message)) do
      attrs =
        attrs
        |> Map.put(:schema_version, normalize_schema_version(Map.get(attrs, :schema_version)))
        |> Map.put(:id, normalize_id(Map.get(attrs, :id)))
        |> Map.put(:global_sequence, normalize_positive_integer(Map.get(attrs, :global_sequence)))
        |> Map.put(:occurred_at, occurred_at)
        |> Map.put(:level, normalize_atom_or_string(Map.get(attrs, :level)))
        |> Map.put(:source, normalize_atom_or_string(Map.get(attrs, :source)))
        |> Map.put(:stream, normalize_atom_or_string(Map.get(attrs, :stream)))
        |> Map.put(:message, message)
        |> Map.put(:metadata, normalize_metadata(Map.get(attrs, :metadata)))
        |> Map.put(:truncated, Map.get(attrs, :truncated) == true)

      {:ok, Favn.Log.Entry.normalize(attrs)}
    end
  end

  defp entry_to_map(%_{} = entry), do: Map.from_struct(entry)
  defp entry_to_map(entry) when is_list(entry), do: Map.new(entry)
  defp entry_to_map(entry) when is_map(entry), do: entry

  @spec assign_global_sequence(Favn.Log.Entry.t(), pos_integer()) :: Favn.Log.Entry.t()
  def assign_global_sequence(entry, global_sequence)
      when is_integer(global_sequence) and global_sequence > 0 do
    struct(entry, global_sequence: global_sequence)
  end

  @spec encode(Favn.Log.Entry.t()) :: {:ok, String.t()} | {:error, term()}
  def encode(entry) do
    {:ok, Jason.encode!(to_dto(entry))}
  rescue
    error -> {:error, {:log_entry_encode_failed, error}}
  end

  @spec decode(String.t()) :: {:ok, Favn.Log.Entry.t()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    with {:ok, %{"format" => @format, "schema_version" => 1} = dto} <- Jason.decode(payload),
         {:ok, occurred_at} <- normalize_datetime(Map.get(dto, "occurred_at")),
         {:ok, metadata} <- normalize_decoded_metadata(Map.get(dto, "metadata")) do
      attrs = %{
        schema_version: 1,
        id: Map.get(dto, "id"),
        global_sequence: normalize_positive_integer(Map.get(dto, "global_sequence")),
        run_id: normalize_optional_binary(Map.get(dto, "run_id")),
        asset_step_id: normalize_optional_binary(Map.get(dto, "asset_step_id")),
        node_key:
          decode_payload_or_value(Map.get(dto, "node_key_payload"), Map.get(dto, "node_key")),
        asset_ref:
          decode_payload_or_value(Map.get(dto, "asset_ref_payload"), Map.get(dto, "asset_ref")),
        runner_execution_id: normalize_optional_binary(Map.get(dto, "runner_execution_id")),
        attempt: normalize_non_negative_integer(Map.get(dto, "attempt")),
        producer_id: normalize_optional_binary(Map.get(dto, "producer_id")),
        producer_sequence: normalize_positive_integer(Map.get(dto, "producer_sequence")),
        occurred_at: occurred_at,
        level: existing_atom_or_string(Map.get(dto, "level")),
        source: existing_atom_or_string(Map.get(dto, "source")),
        stream: existing_atom_or_string(Map.get(dto, "stream")),
        message: Map.get(dto, "message") || "",
        metadata: metadata,
        truncated: Map.get(dto, "truncated") == true
      }

      {:ok, Favn.Log.Entry.normalize(attrs)}
    else
      {:ok, other} -> {:error, {:invalid_log_entry_dto, other}}
      {:error, reason} -> {:error, {:invalid_log_entry_json, reason}}
    end
  end

  @spec node_key_storage(term()) :: {String.t() | nil, String.t() | nil}
  def node_key_storage(nil), do: {nil, nil}

  def node_key_storage(node_key) do
    with {:ok, blob} <- PayloadCodec.encode(node_key) do
      {:crypto.hash(:sha256, blob) |> Base.encode16(case: :lower), blob}
    else
      _ ->
        inspected = inspect(node_key)
        {:crypto.hash(:sha256, inspected) |> Base.encode16(case: :lower), nil}
    end
  end

  @spec asset_ref_storage(term()) :: {String.t() | nil, String.t() | nil}
  def asset_ref_storage(nil), do: {nil, nil}

  def asset_ref_storage(asset_ref) do
    with {:ok, blob} <- PayloadCodec.encode(asset_ref) do
      {asset_ref_key(asset_ref), blob}
    else
      _ -> {asset_ref_key(asset_ref), nil}
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

  defp normalize_schema_version(value) when is_integer(value) and value > 0, do: value
  defp normalize_schema_version(_value), do: 1

  defp normalize_id(value) when is_binary(value) and value != "", do: value
  defp normalize_id(_value), do: random_uuid()

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

  defp normalize_positive_integer(value) when is_integer(value) and value > 0, do: value
  defp normalize_positive_integer(_value), do: nil

  defp normalize_non_negative_integer(value) when is_integer(value) and value >= 0, do: value
  defp normalize_non_negative_integer(_value), do: nil

  defp normalize_atom_or_string(nil), do: nil
  defp normalize_atom_or_string(value) when is_atom(value), do: value
  defp normalize_atom_or_string(value) when is_binary(value) and value != "", do: value
  defp normalize_atom_or_string(_value), do: nil

  defp normalize_optional_binary(value) when is_binary(value) and value != "", do: value
  defp normalize_optional_binary(_value), do: nil

  defp normalize_metadata(value) when is_map(value), do: JsonSafe.data(value)
  defp normalize_metadata(_value), do: %{}

  defp normalize_decoded_metadata(value) when is_map(value), do: {:ok, value}
  defp normalize_decoded_metadata(nil), do: {:ok, %{}}

  defp normalize_decoded_metadata(value),
    do: {:error, {:invalid_log_entry_field, :metadata, value}}

  defp existing_atom_or_string(nil), do: nil

  defp existing_atom_or_string(value) when is_binary(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError -> value
  end

  defp existing_atom_or_string(value), do: value

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

  defp decode_payload_or_value(payload, fallback) when is_binary(payload) do
    case PayloadCodec.decode(payload) do
      {:ok, value} -> value
      {:error, _reason} -> fallback
    end
  end

  defp decode_payload_or_value(_payload, fallback), do: fallback

  defp asset_ref_key({module, name}) when is_atom(module) and is_atom(name),
    do: Atom.to_string(module) <> "." <> Atom.to_string(name)

  defp asset_ref_key(%{"module" => module, "name" => name})
       when is_binary(module) and is_binary(name),
       do: module <> "." <> name

  defp asset_ref_key(value),
    do: :crypto.hash(:sha256, inspect(value)) |> Base.encode16(case: :lower)
end
