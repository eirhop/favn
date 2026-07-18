defmodule Favn.Log.Entry do
  @moduledoc """
  Shared log entry contract for backend log streaming and persistence.

  Runners provide producer identity and ordering for idempotency. Storage and
  orchestrator layers assign the authoritative commit-safe `global_sequence`.
  """

  alias Favn.Log.Identity

  @schema_version 1
  @levels [:debug, :info, :warning, :error]
  @sources [:orchestrator, :runner, :sql_runtime, :adapter, :user_code, :system]
  @streams [:stdout, :stderr, :system]
  @max_message_bytes 65_536

  @type level :: :debug | :info | :warning | :error
  @type source :: :orchestrator | :runner | :sql_runtime | :adapter | :user_code | :system
  @type stream :: :stdout | :stderr | :system

  @type t :: %__MODULE__{
          schema_version: pos_integer(),
          id: String.t() | nil,
          global_sequence: non_neg_integer() | nil,
          run_id: String.t() | nil,
          asset_step_id: String.t() | nil,
          node_key: String.t() | nil,
          asset_ref: String.t() | nil,
          runner_execution_id: String.t() | nil,
          attempt: pos_integer() | nil,
          producer_id: String.t() | nil,
          producer_sequence: non_neg_integer() | nil,
          occurred_at: DateTime.t() | nil,
          level: level(),
          source: source(),
          stream: stream() | nil,
          message: String.t(),
          metadata: map(),
          truncated: boolean()
        }

  defstruct schema_version: @schema_version,
            id: nil,
            global_sequence: nil,
            run_id: nil,
            asset_step_id: nil,
            node_key: nil,
            asset_ref: nil,
            runner_execution_id: nil,
            attempt: nil,
            producer_id: nil,
            producer_sequence: nil,
            occurred_at: nil,
            level: :info,
            source: :user_code,
            stream: :system,
            message: "",
            metadata: %{},
            truncated: false

  @doc """
  Normalizes a map or keyword list into a log entry struct.
  """
  @spec normalize(map() | keyword() | t()) :: t()
  def normalize(%__MODULE__{} = entry), do: normalize(Map.from_struct(entry))

  def normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize()

  def normalize(attrs) when is_map(attrs) do
    attrs = atomize_known_keys(attrs)

    {message, message_truncated?} = normalize_message(Map.get(attrs, :message, ""))

    struct!(__MODULE__, %{
      schema_version: Map.get(attrs, :schema_version, @schema_version),
      id: Map.get(attrs, :id),
      global_sequence: Map.get(attrs, :global_sequence),
      run_id: Map.get(attrs, :run_id),
      asset_step_id: Map.get(attrs, :asset_step_id),
      node_key: normalize_identity(Map.get(attrs, :node_key), &Identity.node_key/1),
      asset_ref: normalize_identity(Map.get(attrs, :asset_ref), &Identity.asset_ref/1),
      runner_execution_id: Map.get(attrs, :runner_execution_id),
      attempt: Map.get(attrs, :attempt),
      producer_id: Map.get(attrs, :producer_id),
      producer_sequence: Map.get(attrs, :producer_sequence),
      occurred_at: Map.get(attrs, :occurred_at),
      level: normalize_enum(Map.get(attrs, :level), @levels, :level, :info),
      source: normalize_enum(Map.get(attrs, :source), @sources, :source, :user_code),
      stream: normalize_enum(Map.get(attrs, :stream), @streams, :stream, :system),
      message: message,
      metadata: Map.get(attrs, :metadata, %{}) || %{},
      truncated: Map.get(attrs, :truncated, false) == true or message_truncated?
    })
  end

  @doc """
  Returns the maximum persisted message size in bytes before truncation.
  """
  @spec max_message_bytes() :: pos_integer()
  def max_message_bytes, do: @max_message_bytes

  @doc """
  Returns supported log levels.
  """
  @spec levels() :: [level()]
  def levels, do: @levels

  @doc """
  Returns supported log sources.
  """
  @spec sources() :: [source()]
  def sources, do: @sources

  @doc """
  Returns supported log streams.
  """
  @spec streams() :: [stream()]
  def streams, do: @streams

  defp atomize_known_keys(attrs) do
    known_keys = Map.keys(%__MODULE__{})

    Enum.reduce(attrs, %{}, fn {key, value}, acc ->
      normalized_key =
        if key in known_keys, do: key, else: normalize_known_string_key(key, known_keys)

      Map.put(acc, normalized_key, value)
    end)
  end

  defp normalize_known_string_key(key, known_keys) when is_binary(key) do
    Enum.find(known_keys, key, &(Atom.to_string(&1) == key))
  end

  defp normalize_known_string_key(key, _known_keys), do: key

  defp normalize_enum(nil, _allowed, _field, default), do: default

  defp normalize_enum(value, allowed, field, default) when is_binary(value) do
    value
    |> String.to_existing_atom()
    |> normalize_enum(allowed, field, default)
  rescue
    ArgumentError -> raise ArgumentError, "invalid #{field}: #{inspect(value)}"
  end

  defp normalize_enum(value, allowed, field, _default) do
    if value in allowed do
      value
    else
      raise ArgumentError, "invalid #{field}: #{inspect(value)}"
    end
  end

  defp normalize_message(value) when is_binary(value), do: truncate_message(value)
  defp normalize_message(value), do: value |> inspect() |> truncate_message()

  defp truncate_message(value) when byte_size(value) <= @max_message_bytes, do: {value, false}

  defp truncate_message(value) do
    suffix = "\n[TRUNCATED]"
    prefix_size = @max_message_bytes - byte_size(suffix)
    {valid_prefix(value, prefix_size) <> suffix, true}
  end

  defp valid_prefix(_value, size) when size <= 0, do: ""

  defp valid_prefix(value, size) do
    prefix = binary_part(value, 0, size)

    if String.valid?(prefix) do
      prefix
    else
      valid_prefix(value, size - 1)
    end
  end

  defp normalize_identity(nil, _normalizer), do: nil

  defp normalize_identity(value, normalizer) do
    case normalizer.(value) do
      {:ok, identity} -> identity
      {:error, reason} -> raise ArgumentError, "invalid log identity: #{inspect(reason)}"
    end
  end
end
