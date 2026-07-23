defmodule Favn.SQL.PartitionSpec do
  @moduledoc """
  Ordered physical partition keys for a SQL materialization.

  The contract is intentionally limited to DuckLake's structured partition
  expressions. It carries no raw SQL and makes no claim about historical data
  files, which may retain an older DuckLake partition specification.
  """

  @max_keys 32
  @max_identifier_bytes 255
  @max_bucket_count 2_147_483_647
  @transforms [:identity, :year, :month, :day, :hour, :bucket]
  @temporal_transforms [:year, :month, :day, :hour]

  @typedoc "One normalized ordered partition key."
  @type key :: %{
          required(:column) => String.t(),
          required(:transform) => transform(),
          required(:bucket_count) => pos_integer() | nil
        }

  @type transform :: :identity | :year | :month | :day | :hour | :bucket

  @enforce_keys [:keys]
  defstruct [:keys]

  @type t :: %__MODULE__{keys: [key()]}

  @doc """
  Normalizes a public `partitioned_by` declaration.

  Identity keys are atoms or strings. Transforms use `{:year, column}`,
  `{:month, column}`, `{:day, column}`, `{:hour, column}`, or
  `{:bucket, bucket_count, column}`.
  """
  @spec normalize!(t() | [term()]) :: t()
  def normalize!(%__MODULE__{keys: keys}), do: normalize_keys!(keys)
  def normalize!(keys) when is_list(keys), do: normalize_keys!(keys)

  def normalize!(value) do
    raise ArgumentError,
          "partitioned_by must be a non-empty list of columns or supported transforms, got: #{inspect(value)}"
  end

  @doc "Rehydrates a partition specification from a decoded manifest value."
  @spec from_value!(t() | map()) :: t()
  def from_value!(%__MODULE__{} = spec), do: normalize!(spec)

  def from_value!(value) when is_map(value) do
    case field(value, :keys) do
      keys when is_list(keys) ->
        keys
        |> Enum.map(&key_from_value!/1)
        |> normalize_keys!()

      keys ->
        raise ArgumentError,
              "partition specification keys must be a non-empty list, got: #{inspect(keys)}"
    end
  end

  def from_value!(value) do
    raise ArgumentError, "invalid partition specification: #{inspect(value)}"
  end

  @doc "Returns the supported structured partition transforms."
  @spec transforms() :: [transform()]
  def transforms, do: @transforms

  @doc "Returns whether a transform requires a date or timestamp source column."
  @spec temporal_transform?(transform()) :: boolean()
  def temporal_transform?(transform), do: transform in @temporal_transforms

  @doc "Returns the maximum supported number of partition keys."
  @spec max_keys() :: pos_integer()
  def max_keys, do: @max_keys

  defp normalize_keys!([]) do
    raise ArgumentError, "partitioned_by must contain at least one partition key"
  end

  defp normalize_keys!(keys) when is_list(keys) and length(keys) <= @max_keys do
    normalized = Enum.map(keys, &normalize_key!/1)

    if length(normalized) == length(Enum.uniq(normalized)) do
      %__MODULE__{keys: normalized}
    else
      raise ArgumentError, "partitioned_by contains duplicate partition keys"
    end
  end

  defp normalize_keys!(keys) when is_list(keys) do
    raise ArgumentError, "partitioned_by supports at most #{@max_keys} partition keys"
  end

  defp normalize_keys!(value) do
    raise ArgumentError,
          "partition specification keys must be a non-empty list, got: #{inspect(value)}"
  end

  defp normalize_key!(column) when is_atom(column) or is_binary(column) do
    key(:identity, column, nil)
  end

  defp normalize_key!({transform, column}) when transform in @temporal_transforms do
    key(transform, column, nil)
  end

  defp normalize_key!({:bucket, bucket_count, column})
       when is_integer(bucket_count) and bucket_count > 0 and
              bucket_count <= @max_bucket_count do
    key(:bucket, column, bucket_count)
  end

  defp normalize_key!({:bucket, bucket_count, _column}) do
    raise ArgumentError,
          "partitioned_by bucket count must be between 1 and #{@max_bucket_count}, got: #{inspect(bucket_count)}"
  end

  defp normalize_key!(value) when is_map(value) do
    value
    |> key_from_value!()
    |> normalize_key!()
  end

  defp normalize_key!(value) do
    raise ArgumentError,
          "invalid partition key #{inspect(value)}; expected a column, " <>
            "{:year | :month | :day | :hour, column}, or {:bucket, count, column}"
  end

  defp key(transform, column, bucket_count) do
    %{
      column: normalize_column!(column),
      transform: transform,
      bucket_count: bucket_count
    }
  end

  defp normalize_column!(column) when is_atom(column),
    do: normalize_column!(Atom.to_string(column))

  defp normalize_column!(column) when is_binary(column) do
    if column != "" and byte_size(column) <= @max_identifier_bytes and
         not String.contains?(column, <<0>>) do
      column
    else
      raise ArgumentError,
            "partitioned_by column must be a non-empty identifier of at most #{@max_identifier_bytes} bytes"
    end
  end

  defp normalize_column!(column) do
    raise ArgumentError,
          "partitioned_by column must be an atom or string, got: #{inspect(column)}"
  end

  defp key_from_value!(value) when is_map(value) do
    transform = value |> field(:transform) |> normalize_transform!()
    column = field(value, :column)
    bucket_count = field(value, :bucket_count)

    case transform do
      :identity -> column
      transform when transform in @temporal_transforms -> {transform, column}
      :bucket -> {:bucket, bucket_count, column}
    end
  end

  defp key_from_value!(value) do
    raise ArgumentError, "invalid partition key value: #{inspect(value)}"
  end

  defp normalize_transform!(transform) when transform in @transforms, do: transform

  defp normalize_transform!(transform) when is_binary(transform) do
    case transform do
      "identity" -> :identity
      "year" -> :year
      "month" -> :month
      "day" -> :day
      "hour" -> :hour
      "bucket" -> :bucket
      _other -> invalid_transform!(transform)
    end
  end

  defp normalize_transform!(transform), do: invalid_transform!(transform)

  defp invalid_transform!(transform) do
    raise ArgumentError,
          "unsupported partition transform #{inspect(transform)}; expected one of #{inspect(@transforms)}"
  end

  defp field(value, key) do
    Map.get(value, key, Map.get(value, Atom.to_string(key)))
  end
end
