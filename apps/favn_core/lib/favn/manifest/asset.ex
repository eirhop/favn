defmodule Favn.Manifest.Asset do
  @moduledoc """
  Canonical runtime descriptor for one compiled asset.

  This struct contains only runtime-required metadata. It intentionally excludes
  source locations and compiler diagnostics.
  """

  alias Favn.Manifest.SQLExecution
  alias Favn.RuntimeConfig.Requirements
  alias Favn.SQLAsset.Compiler

  @type t :: %__MODULE__{
          ref: {module(), atom()} | nil,
          module: module() | nil,
          name: atom() | nil,
          type: :elixir | :sql | :source,
          depends_on: [{module(), atom()}],
          execution: %{
            required(:entrypoint) => atom() | nil,
            required(:arity) => non_neg_integer() | nil
          },
          config: map(),
          relation: map() | struct() | nil,
          window: map() | struct() | nil,
          materialization: map() | struct() | nil,
          relation_inputs: [map() | struct()],
          runtime_config: Requirements.declarations(),
          sql_execution: SQLExecution.t() | nil,
          metadata: map()
        }

  defstruct [
    :ref,
    :module,
    :name,
    type: :elixir,
    depends_on: [],
    execution: %{entrypoint: nil, arity: nil},
    config: %{},
    relation: nil,
    window: nil,
    materialization: nil,
    relation_inputs: [],
    runtime_config: %{},
    sql_execution: nil,
    metadata: %{}
  ]

  @spec from_asset(map()) :: t()
  def from_asset(asset) when is_map(asset) do
    %__MODULE__{
      ref: Map.get(asset, :ref),
      module: Map.get(asset, :module),
      name: Map.get(asset, :name),
      type: Map.get(asset, :type, :elixir),
      depends_on: normalize_depends_on(Map.get(asset, :depends_on, [])),
      execution: %{entrypoint: Map.get(asset, :entrypoint), arity: Map.get(asset, :arity)},
      config: normalize_map(Map.get(asset, :config, %{})),
      relation: Map.get(asset, :relation),
      window: Map.get(asset, :window_spec),
      materialization: Map.get(asset, :materialization),
      relation_inputs: normalize_list(Map.get(asset, :relation_inputs, [])),
      runtime_config: normalize_runtime_config(Map.get(asset, :runtime_config, %{})),
      sql_execution: build_sql_execution(asset),
      metadata: normalize_map(Map.get(asset, :meta, %{}))
    }
  end

  defp build_sql_execution(%{type: :sql, module: module}) when is_atom(module) do
    case Compiler.fetch_definition(module) do
      {:ok, definition} -> SQLExecution.from_definition(definition)
      {:error, _reason} -> nil
    end
  end

  defp build_sql_execution(_asset), do: nil

  defp normalize_depends_on(depends_on) when is_list(depends_on) do
    depends_on
    |> Enum.uniq()
    |> Enum.sort(&compare_refs/2)
  end

  defp normalize_depends_on(_other), do: []

  defp normalize_list(value) when is_list(value), do: value
  defp normalize_list(_other), do: []

  defp normalize_map(value) when is_map(value), do: value
  defp normalize_map(_other), do: %{}

  defp normalize_runtime_config(value) when is_map(value), do: Requirements.normalize!(value)
  defp normalize_runtime_config(_other), do: %{}

  defp compare_refs({left_module, left_name}, {right_module, right_name}) do
    left = {Atom.to_string(left_module), Atom.to_string(left_name)}
    right = {Atom.to_string(right_module), Atom.to_string(right_name)}
    left <= right
  end
end
