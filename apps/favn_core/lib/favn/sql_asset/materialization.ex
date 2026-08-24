defmodule Favn.SQLAsset.Materialization do
  @moduledoc """
  Canonical SQL asset materialization metadata.
  """

  @type strategy :: :append | :replace | :delete_insert | :merge | :replace_groups

  @type incremental_opts ::
          [
            strategy: strategy(),
            unique_key: [atom()],
            window_column: atom() | String.t(),
            replacement_key: [atom()]
          ]
  @type t :: :view | :table | {:incremental, incremental_opts()}

  @spec normalize!(t()) :: t()
  def normalize!(:view), do: :view
  def normalize!(:table), do: :table

  def normalize!({:incremental, opts}) when is_list(opts) do
    if not Keyword.keyword?(opts) do
      raise ArgumentError,
            "incremental materialization options must be a keyword list, got: #{inspect(opts)}"
    end

    ensure_unique_keys!(opts)
    validate_incremental_opts!(opts)
    {:incremental, opts}
  end

  def normalize!(value) do
    raise ArgumentError,
          "materialization must be :view, :table, or {:incremental, keyword()}, got: #{inspect(value)}"
  end

  defp ensure_unique_keys!(opts) do
    keys = Keyword.keys(opts)

    if length(keys) == length(Enum.uniq(keys)) do
      :ok
    else
      raise ArgumentError, "incremental materialization options contain duplicate keys"
    end
  end

  defp validate_incremental_opts!(opts) do
    Enum.each(opts, fn
      {:strategy, strategy}
      when strategy in [:append, :replace, :delete_insert, :merge, :replace_groups] ->
        :ok

      {:strategy, value} ->
        raise ArgumentError,
              "incremental materialization strategy must be :append, :replace, :delete_insert, :merge, or :replace_groups, got: #{inspect(value)}"

      {:unique_key, keys} when is_list(keys) ->
        Enum.each(keys, fn
          key when is_atom(key) ->
            :ok

          key ->
            raise ArgumentError,
                  "incremental materialization unique_key entries must be atoms, got: #{inspect(key)}"
        end)

      {:unique_key, value} ->
        raise ArgumentError,
              "incremental materialization unique_key must be a list of atoms, got: #{inspect(value)}"

      {:window_column, column} when is_atom(column) or is_binary(column) ->
        :ok

      {:window_column, value} ->
        raise ArgumentError,
              "incremental materialization window_column must be an atom or string, got: #{inspect(value)}"

      {:replacement_key, keys} when is_list(keys) and keys != [] ->
        Enum.each(keys, fn
          key when is_atom(key) and not is_nil(key) ->
            :ok

          key ->
            raise ArgumentError,
                  "incremental materialization replacement_key entries must be non-nil atoms, got: #{inspect(key)}"
        end)

        if length(keys) != length(Enum.uniq(keys)) do
          raise ArgumentError,
                "incremental materialization replacement_key entries must be unique"
        end

      {:replacement_key, value} ->
        raise ArgumentError,
              "incremental materialization replacement_key must be a non-empty list of atoms, got: #{inspect(value)}"

      {key, _value} ->
        raise ArgumentError,
              "incremental materialization contains unsupported key #{inspect(key)}; allowed keys: [:strategy, :unique_key, :window_column, :replacement_key]"
    end)

    case Keyword.fetch(opts, :strategy) do
      {:ok, strategy} -> validate_strategy_opts!(strategy, opts)
      :error -> raise ArgumentError, "incremental materialization requires :strategy"
    end
  end

  defp validate_strategy_opts!(:replace_groups, opts) do
    if not Keyword.has_key?(opts, :replacement_key) do
      raise ArgumentError, "incremental :replace_groups requires :replacement_key"
    end

    for forbidden <- [:unique_key, :window_column], Keyword.has_key?(opts, forbidden) do
      raise ArgumentError, "incremental :replace_groups does not accept #{inspect(forbidden)}"
    end

    :ok
  end

  defp validate_strategy_opts!(_strategy, opts) do
    if Keyword.has_key?(opts, :replacement_key) do
      raise ArgumentError, "incremental replacement_key requires strategy :replace_groups"
    end

    :ok
  end
end
