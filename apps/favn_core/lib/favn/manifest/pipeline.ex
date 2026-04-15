defmodule Favn.Manifest.Pipeline do
  @moduledoc """
  Canonical persisted descriptor for one pipeline definition.
  """

  @type t :: %__MODULE__{
          module: module() | nil,
          name: atom() | nil,
          selectors: [term()],
          deps: :all | :none,
          schedule: term(),
          window: atom() | nil,
          source: atom() | nil,
          outputs: [atom()],
          config: map(),
          metadata: map()
        }

  defstruct module: nil,
            name: nil,
            selectors: [],
            deps: :all,
            schedule: nil,
            window: nil,
            source: nil,
            outputs: [],
            config: %{},
            metadata: %{}

  @spec from_definition(map()) :: t()
  def from_definition(definition) when is_map(definition) do
    %__MODULE__{
      module: Map.get(definition, :module),
      name: Map.get(definition, :name),
      selectors: normalize_list(Map.get(definition, :selectors, [])),
      deps: normalize_deps(Map.get(definition, :deps, :all)),
      schedule: Map.get(definition, :schedule),
      window: Map.get(definition, :window),
      source: Map.get(definition, :source),
      outputs: normalize_atom_list(Map.get(definition, :outputs, [])),
      config: normalize_map(Map.get(definition, :config, %{})),
      metadata: normalize_map(Map.get(definition, :meta, %{}))
    }
  end

  defp normalize_deps(:all), do: :all
  defp normalize_deps(:none), do: :none
  defp normalize_deps(_other), do: :all

  defp normalize_atom_list(list) when is_list(list) do
    list
    |> Enum.filter(&is_atom/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp normalize_atom_list(_other), do: []

  defp normalize_list(list) when is_list(list), do: list
  defp normalize_list(_other), do: []

  defp normalize_map(value) when is_map(value), do: value
  defp normalize_map(_other), do: %{}
end
