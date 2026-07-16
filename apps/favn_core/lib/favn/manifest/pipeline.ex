defmodule Favn.Manifest.Pipeline do
  @moduledoc """
  Canonical persisted descriptor for one pipeline definition.

  Tag and category selectors carry manifest labels. Labels are normalized to
  strings for stable selector behavior across JSON persistence.
  """

  alias Favn.Window.Policy

  @type t :: %__MODULE__{
          module: module() | nil,
          name: atom() | nil,
          selectors: [term()],
          deps: :all | :none,
          schedule: term(),
          window: Favn.Window.Policy.t() | nil,
          retry_policy: Favn.Retry.Policy.t() | nil,
          max_concurrency: pos_integer() | nil,
          execution_pool: atom() | nil,
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
            retry_policy: nil,
            max_concurrency: nil,
            execution_pool: nil,
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
      window: Policy.from_value!(Map.get(definition, :window)),
      retry_policy: normalize_retry_policy(Map.get(definition, :retry_policy)),
      max_concurrency: normalize_max_concurrency(Map.get(definition, :max_concurrency)),
      execution_pool: normalize_execution_pool(Map.get(definition, :execution_pool)),
      source: Map.get(definition, :source),
      outputs: normalize_atom_list(Map.get(definition, :outputs, [])),
      config: normalize_map(Map.get(definition, :config, %{})),
      metadata: normalize_map(Map.get(definition, :meta, %{}))
    }
  end

  defp normalize_deps(:all), do: :all
  defp normalize_deps(:none), do: :none
  defp normalize_deps(_other), do: :all

  defp normalize_retry_policy(nil), do: nil
  defp normalize_retry_policy(value), do: Favn.Retry.Policy.new!(value)

  defp normalize_max_concurrency(value) when is_integer(value) and value > 0, do: value
  defp normalize_max_concurrency(_other), do: nil

  defp normalize_execution_pool(value) when is_atom(value), do: value
  defp normalize_execution_pool(_other), do: nil

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
