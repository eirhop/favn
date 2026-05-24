defmodule Favn.Log.Filter do
  @moduledoc """
  Query filter contract for backend logs.
  """

  alias Favn.Log.Entry
  alias Favn.Ref

  @type t :: %__MODULE__{
          run_id: String.t() | nil,
          asset_step_id: String.t() | nil,
          runner_execution_id: String.t() | nil,
          node_key: String.t() | nil,
          asset_ref: Ref.t() | nil,
          stream: Entry.stream() | nil,
          levels: [Entry.level()],
          sources: [Entry.source()],
          since: DateTime.t() | nil,
          until: DateTime.t() | nil
        }

  defstruct run_id: nil,
            asset_step_id: nil,
            runner_execution_id: nil,
            node_key: nil,
            asset_ref: nil,
            stream: nil,
            levels: [],
            sources: [],
            since: nil,
            until: nil

  @doc """
  Normalizes a map or keyword list into a log filter struct.
  """
  @spec normalize(map() | keyword() | t()) :: t()
  def normalize(%__MODULE__{} = filter), do: normalize(Map.from_struct(filter))
  def normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize()

  def normalize(attrs) when is_map(attrs) do
    attrs = atomize_known_keys(attrs)

    struct!(__MODULE__, %{
      run_id: Map.get(attrs, :run_id),
      asset_step_id: Map.get(attrs, :asset_step_id),
      runner_execution_id: Map.get(attrs, :runner_execution_id),
      node_key: Map.get(attrs, :node_key),
      asset_ref: Map.get(attrs, :asset_ref),
      stream: normalize_optional_enum(Map.get(attrs, :stream), Entry.streams(), :stream),
      levels: normalize_list(Map.get(attrs, :levels, []), Entry.levels(), :level),
      sources: normalize_list(Map.get(attrs, :sources, []), Entry.sources(), :source),
      since: Map.get(attrs, :since),
      until: Map.get(attrs, :until)
    })
  end

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

  defp normalize_list(nil, _allowed, _field), do: []

  defp normalize_list(value, allowed, field) when not is_list(value),
    do: normalize_list([value], allowed, field)

  defp normalize_list(values, allowed, field) do
    Enum.map(values, &normalize_enum(&1, allowed, field))
  end

  defp normalize_enum(value, allowed, field) when is_binary(value) do
    value
    |> String.to_existing_atom()
    |> normalize_enum(allowed, field)
  rescue
    ArgumentError -> raise ArgumentError, "invalid #{field}: #{inspect(value)}"
  end

  defp normalize_enum(value, allowed, field) do
    if value in allowed do
      value
    else
      raise ArgumentError, "invalid #{field}: #{inspect(value)}"
    end
  end

  defp normalize_optional_enum(nil, _allowed, _field), do: nil
  defp normalize_optional_enum(value, allowed, field), do: normalize_enum(value, allowed, field)
end
