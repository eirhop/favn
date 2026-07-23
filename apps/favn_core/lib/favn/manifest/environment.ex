defmodule Favn.Manifest.Environment do
  @moduledoc """
  Validated deployment inputs used while resolving a manifest.

  Runtime code consumes only the concrete values stored in the manifest. It
  does not re-read this configuration.
  """

  alias Favn.Window.Validate

  @type timezone_source :: :application_default | :utc_fallback

  @type t :: %__MODULE__{
          default_timezone: String.t(),
          default_timezone_source: timezone_source(),
          coverage_scope: %{from: Date.t()} | nil
        }

  @enforce_keys [:default_timezone, :default_timezone_source]
  defstruct [:default_timezone, :default_timezone_source, coverage_scope: nil]

  @doc "Builds a manifest environment from explicit normalized inputs."
  @spec new(keyword() | map()) :: {:ok, t()} | {:error, term()}
  def new(values \\ [])

  def new(values) when is_list(values) do
    cond do
      not Keyword.keyword?(values) ->
        {:error, {:invalid_manifest_environment, values}}

      duplicate_keys(values) != [] ->
        {:error, {:duplicate_manifest_environment_keys, duplicate_keys(values)}}

      true ->
        new(Map.new(values))
    end
  end

  def new(values) when is_map(values) do
    with {:ok, values} <- normalize_environment_keys(values),
         default <- Map.get(values, :default_timezone),
         source <- if(is_nil(default), do: :utc_fallback, else: :application_default),
         timezone <- default || "Etc/UTC",
         :ok <- Validate.timezone(timezone),
         {:ok, coverage_scope} <- normalize_scope(Map.get(values, :coverage_scope)) do
      {:ok,
       %__MODULE__{
         default_timezone: timezone,
         default_timezone_source: source,
         coverage_scope: coverage_scope
       }}
    end
  end

  def new(values), do: {:error, {:invalid_manifest_environment, values}}

  @doc "Builds a manifest environment and raises on invalid input."
  @spec new!(keyword() | map()) :: t()
  def new!(values \\ []) do
    case new(values) do
      {:ok, environment} -> environment
      {:error, reason} -> raise ArgumentError, "invalid manifest environment: #{inspect(reason)}"
    end
  end

  defp normalize_scope(nil), do: {:ok, nil}

  defp normalize_scope(scope) when is_list(scope) do
    if Keyword.keyword?(scope),
      do: normalize_scope(Map.new(scope)),
      else: {:error, {:invalid_coverage_scope, scope}}
  end

  defp normalize_scope(scope) when is_map(scope) do
    normalized = Map.new(scope, fn {key, value} -> {normalize_scope_key(key), value} end)

    case normalized do
      %{from: value} when map_size(normalized) == 1 ->
        normalize_scope_from(value)

      %{from: _value} ->
        {:error, {:unsupported_coverage_scope_keys, Map.keys(normalized) -- [:from]}}

      _other ->
        {:error, :coverage_scope_from_required}
    end
  rescue
    ArgumentError -> {:error, {:invalid_coverage_scope, scope}}
  end

  defp normalize_scope(scope), do: {:error, {:invalid_coverage_scope, scope}}

  defp normalize_environment_keys(values) do
    Enum.reduce_while(values, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      normalized_key = normalize_environment_key(key)

      cond do
        normalized_key not in [:default_timezone, :coverage_scope] ->
          {:halt, {:error, {:unsupported_manifest_environment_key, key}}}

        Map.has_key?(acc, normalized_key) ->
          {:halt, {:error, {:duplicate_manifest_environment_key, normalized_key}}}

        true ->
          {:cont, {:ok, Map.put(acc, normalized_key, value)}}
      end
    end)
  end

  defp normalize_environment_key(:default_timezone), do: :default_timezone
  defp normalize_environment_key("default_timezone"), do: :default_timezone
  defp normalize_environment_key(:coverage_scope), do: :coverage_scope
  defp normalize_environment_key("coverage_scope"), do: :coverage_scope
  defp normalize_environment_key(key), do: key

  defp duplicate_keys(values) do
    values
    |> Keyword.keys()
    |> Enum.frequencies()
    |> Enum.filter(fn {_key, count} -> count > 1 end)
    |> Enum.map(&elem(&1, 0))
  end

  defp normalize_scope_key(:from), do: :from
  defp normalize_scope_key("from"), do: :from
  defp normalize_scope_key(key), do: key

  defp normalize_scope_from(%Date{} = date), do: {:ok, %{from: date}}

  defp normalize_scope_from(value) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> {:ok, %{from: date}}
      {:error, reason} -> {:error, {:invalid_coverage_scope_from, value, reason}}
    end
  end

  defp normalize_scope_from(value), do: {:error, {:invalid_coverage_scope_from, value}}
end
