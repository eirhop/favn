defmodule Favn.SQLAsset.Materialization do
  @moduledoc """
  Canonical SQL asset materialization metadata.
  """

  @type strategy :: :append | :replace | :delete_insert | :merge
  @type incremental_opts :: [strategy: strategy(), unique_key: [atom()]]
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
      {:strategy, strategy} when strategy in [:append, :replace, :delete_insert, :merge] ->
        :ok

      {:strategy, value} ->
        raise ArgumentError,
              "incremental materialization strategy must be :append, :replace, :delete_insert, or :merge, got: #{inspect(value)}"

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

      {key, _value} ->
        raise ArgumentError,
              "incremental materialization contains unsupported key #{inspect(key)}; allowed keys: [:strategy, :unique_key]"
    end)

    case Keyword.fetch(opts, :strategy) do
      {:ok, _strategy} -> :ok
      :error -> raise ArgumentError, "incremental materialization requires :strategy"
    end
  end
end
