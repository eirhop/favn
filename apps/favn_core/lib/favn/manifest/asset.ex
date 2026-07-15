defmodule Favn.Manifest.Asset do
  @moduledoc """
  Canonical runtime descriptor for one compiled asset.

  This struct contains only runtime-required metadata. It intentionally excludes
  source locations and compiler diagnostics.

  Asset freshness policies declared with `@freshness` are stored in `:freshness`
  as normalized `Favn.Freshness.Policy` values. Runtime code uses this manifest
  field, not authoring modules, when deciding whether a planned node should run,
  skip as fresh, or dirty downstream nodes.

  Metadata `:category` and `:tags` values are selector labels. They are
  normalized to strings at manifest boundaries so persisted manifests do not need
  to create atoms for user-facing labels.
  """

  alias Favn.Freshness.Policy, as: FreshnessPolicy
  alias Favn.Manifest.SQLExecution
  alias Favn.RuntimeConfig.Requirements
  alias Favn.SQL.SessionRequirements
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
          freshness: FreshnessPolicy.t() | nil,
          materialization: map() | struct() | nil,
          relation_inputs: [map() | struct()],
          runtime_config: Requirements.declarations(),
          session_requirements: SessionRequirements.t(),
          sql_execution: SQLExecution.t() | nil,
          execution_pool: atom() | nil,
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
    freshness: nil,
    materialization: nil,
    relation_inputs: [],
    runtime_config: %{},
    session_requirements: %SessionRequirements{version: 1},
    sql_execution: nil,
    execution_pool: nil,
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
      freshness: normalize_freshness(Map.get(asset, :freshness)),
      materialization: Map.get(asset, :materialization),
      relation_inputs: normalize_list(Map.get(asset, :relation_inputs, [])),
      runtime_config: normalize_runtime_config(Map.get(asset, :runtime_config, %{})),
      session_requirements: normalize_session_requirements(Map.get(asset, :session_requirements)),
      sql_execution: build_sql_execution(asset),
      execution_pool: normalize_execution_pool(Map.get(asset, :execution_pool)),
      metadata: normalize_map(Map.get(asset, :meta, %{}))
    }
  end

  defp normalize_execution_pool(value) when is_atom(value), do: value
  defp normalize_execution_pool(_other), do: nil

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

  defp normalize_session_requirements(nil), do: SessionRequirements.empty()

  defp normalize_session_requirements(value), do: SessionRequirements.validate!(value)

  defp normalize_freshness(value) do
    case FreshnessPolicy.from_value(value) do
      {:ok, policy} -> policy
      {:error, reason} -> raise ArgumentError, "invalid freshness policy: #{inspect(reason)}"
    end
  end

  defp compare_refs({left_module, left_name}, {right_module, right_name}) do
    left = {Atom.to_string(left_module), Atom.to_string(left_name)}
    right = {Atom.to_string(right_module), Atom.to_string(right_name)}
    left <= right
  end
end
