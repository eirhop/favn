defmodule Favn.Manifest.Asset do
  @moduledoc """
  Canonical runtime descriptor for one compiled asset.

  This struct contains only runtime-required metadata. It intentionally excludes
  source locations and compiler diagnostics.

  Asset freshness policies declared with `freshness/1` are stored in `:freshness`
  as normalized `Favn.Freshness.Policy` values. Runtime code uses this manifest
  field, not authoring modules, when deciding whether a planned node should run,
  skip as fresh, or dirty downstream nodes.

  Metadata `:category` and `:tags` values are selector labels. They are
  normalized to strings at manifest boundaries so persisted manifests do not need
  to create atoms for user-facing labels.
  """

  alias Favn.Coverage.Effective, as: EffectiveCoverage
  alias Favn.Coverage.Spec, as: CoverageSpec
  alias Favn.Freshness.Policy, as: FreshnessPolicy
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Environment
  alias Favn.Manifest.Compatibility
  alias Favn.Manifest.TargetDescriptor
  alias Favn.RuntimeConfig.Requirements
  alias Favn.SQL.SessionRequirements
  alias Favn.Window.Spec, as: WindowSpec

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
          settings: Favn.Settings.t(),
          description: String.t() | nil,
          relation: map() | struct() | nil,
          window: map() | struct() | nil,
          coverage: EffectiveCoverage.t() | nil,
          freshness: FreshnessPolicy.t() | nil,
          retry_policy: Favn.Retry.Policy.t() | nil,
          materialization: map() | struct() | nil,
          relation_inputs: [map() | struct()],
          runtime_config: Requirements.declarations(),
          session_requirements: SessionRequirements.t(),
          execution_package_hash: String.t() | nil,
          assurance: map() | nil,
          target_descriptor: TargetDescriptor.t() | nil,
          semantic_generation_id: String.t() | nil,
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
    settings: %{},
    description: nil,
    relation: nil,
    window: nil,
    coverage: nil,
    freshness: nil,
    retry_policy: nil,
    materialization: nil,
    relation_inputs: [],
    runtime_config: %{},
    session_requirements: %SessionRequirements{version: 1},
    execution_package_hash: nil,
    assurance: nil,
    target_descriptor: nil,
    semantic_generation_id: nil,
    execution_pool: nil,
    metadata: %{}
  ]

  @spec from_asset(map(), keyword()) :: t()
  def from_asset(asset, opts \\ []) when is_map(asset) and is_list(opts) do
    package = Keyword.get_lazy(opts, :execution_package, fn -> execution_package(asset) end)
    environment = Keyword.get(opts, :environment, Environment.new!())
    window = resolve_window(Map.get(asset, :window_spec), environment)
    coverage = resolve_coverage(Map.get(asset, :coverage_spec), window, environment)
    freshness = resolve_freshness(Map.get(asset, :freshness), window, environment)

    manifest_asset = %__MODULE__{
      ref: Map.get(asset, :ref),
      module: Map.get(asset, :module),
      name: Map.get(asset, :name),
      type: Map.get(asset, :type, :elixir),
      depends_on: normalize_depends_on(Map.get(asset, :depends_on, [])),
      execution: %{entrypoint: Map.get(asset, :entrypoint), arity: Map.get(asset, :arity)},
      settings: Favn.Settings.normalize!(Map.get(asset, :settings, %{})),
      description: Map.get(asset, :doc),
      relation: Map.get(asset, :relation),
      window: window,
      coverage: coverage,
      freshness: freshness,
      retry_policy: normalize_retry_policy(Map.get(asset, :retry_policy)),
      materialization: Map.get(asset, :materialization),
      relation_inputs: normalize_list(Map.get(asset, :relation_inputs, [])),
      runtime_config: normalize_runtime_config(Map.get(asset, :runtime_config, %{})),
      session_requirements: normalize_session_requirements(Map.get(asset, :session_requirements)),
      execution_package_hash: package_hash(package),
      assurance: package_assurance(package),
      execution_pool: normalize_execution_pool(Map.get(asset, :execution_pool)),
      metadata: normalize_map(Map.get(asset, :meta, %{}))
    }

    descriptor =
      TargetDescriptor.from_asset(Map.from_struct(manifest_asset),
        connection_definitions: Keyword.get(opts, :connection_definitions, %{}),
        manifest_schema_version:
          Keyword.get(opts, :manifest_schema_version, Compatibility.current_schema_version()),
        runner_contract_version:
          Keyword.get(
            opts,
            :runner_contract_version,
            Compatibility.current_runner_contract_version()
          )
      )

    semantic_generation_id =
      if descriptor,
        do: nil,
        else:
          TargetDescriptor.semantic_generation_id(
            Map.from_struct(manifest_asset),
            Keyword.get(opts, :runner_release_id)
          )

    %{
      manifest_asset
      | target_descriptor: descriptor,
        semantic_generation_id: semantic_generation_id
    }
  end

  defp normalize_execution_pool(value) when is_atom(value), do: value
  defp normalize_execution_pool(_other), do: nil

  defp execution_package(%{type: :sql, ref: ref, module: module})
       when is_tuple(ref) and is_atom(module) do
    case ExecutionPackage.from_asset(%{type: :sql, ref: ref, module: module}) do
      {:ok, package} -> package
      {:error, _reason} -> nil
    end
  end

  defp execution_package(_asset), do: nil

  defp package_hash(%ExecutionPackage{content_hash: content_hash}), do: content_hash
  defp package_hash(_package), do: nil

  defp package_assurance(%ExecutionPackage{sql_execution: execution}) do
    if is_nil(execution.contract) and execution.checks == [] do
      nil
    else
      %{
        contract: execution.contract,
        checks:
          Enum.map(execution.checks, fn check ->
            Map.take(check, [
              :name,
              :origin,
              :claim_id,
              :at,
              :when,
              :on_violation,
              :message
            ])
          end)
      }
    end
  end

  defp package_assurance(_package), do: nil

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

  defp resolve_window(value, %Environment{} = environment) do
    with {:ok, window} <- WindowSpec.from_value(value),
         {:ok, window} <-
           resolve_window_timezone(
             window,
             environment.default_timezone,
             environment.default_timezone_source
           ) do
      window
    else
      {:error, reason} -> raise ArgumentError, "invalid manifest asset window: #{inspect(reason)}"
    end
  end

  defp resolve_window_timezone(nil, _timezone, _source), do: {:ok, nil}

  defp resolve_window_timezone(%WindowSpec{} = window, timezone, source),
    do: WindowSpec.resolve_timezone(window, timezone, source)

  defp resolve_coverage(value, window, %Environment{} = environment) do
    scope_from = if environment.coverage_scope, do: environment.coverage_scope.from

    with {:ok, spec} <- CoverageSpec.from_value(value),
         {:ok, coverage} <- EffectiveCoverage.resolve(spec, window, scope_from) do
      coverage
    else
      {:error, reason} ->
        raise ArgumentError, "invalid manifest asset coverage: #{inspect(reason)}"
    end
  end

  defp resolve_freshness(value, window, %Environment{} = environment) do
    with freshness <- normalize_freshness(value),
         {:ok, freshness} <-
           FreshnessPolicy.resolve_timezone(
             freshness,
             window,
             environment.default_timezone,
             environment.default_timezone_source
           ) do
      freshness
    else
      {:error, reason} ->
        raise ArgumentError, "invalid manifest asset freshness: #{inspect(reason)}"
    end
  end

  defp normalize_retry_policy(nil), do: nil
  defp normalize_retry_policy(value), do: Favn.Retry.Policy.new!(value)

  defp compare_refs({left_module, left_name}, {right_module, right_name}) do
    left = {Atom.to_string(left_module), Atom.to_string(left_name)}
    right = {Atom.to_string(right_module), Atom.to_string(right_name)}
    left <= right
  end
end
