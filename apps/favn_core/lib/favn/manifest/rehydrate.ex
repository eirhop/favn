defmodule Favn.Manifest.Rehydrate do
  @moduledoc """
  Rehydrates decoded manifest payloads into canonical runtime structs.
  """

  alias Favn.Asset.RelationInput
  alias Favn.Freshness.Policy, as: FreshnessPolicy
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Build
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Labels
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.SQLExecution
  alias Favn.RelationRef
  alias Favn.RuntimeInputResolver.Ref, as: RuntimeInputResolverRef
  alias Favn.Retry.Policy, as: RetryPolicy
  alias Favn.RuntimeConfig.Ref, as: RuntimeConfigRef
  alias Favn.SQL.Definition, as: SQLDefinition
  alias Favn.SQL.Check
  alias Favn.SQL.SessionRequirements
  alias Favn.SQL.Contract
  alias Favn.SQL.Contract.{Column, Composition, Grain, Lineage, Param, RowCount, UniqueKey}
  alias Favn.SQL.Template
  alias Favn.SQLAsset.RelationUsage

  alias Favn.SQL.Template.{
    AssetRef,
    Call,
    DefinitionRef,
    Fragment,
    Placeholder,
    Relation,
    Requirements,
    RuntimeRelation,
    Text
  }

  alias Favn.SQLAsset.Materialization
  alias Favn.Window.{Policy, Spec}

  @max_manifest_atom_length 255
  @max_manifest_module_length 255
  @max_manifest_atom_refs 100_000
  @min_manifest_atom_headroom 100_000

  @type error ::
          {:invalid_manifest_input, term()}
          | {:invalid_manifest_payload, term()}
          | {:manifest_atom_limit_exceeded, non_neg_integer(), pos_integer()}
          | {:manifest_atom_headroom_exceeded, non_neg_integer(), non_neg_integer(),
             pos_integer()}

  @spec manifest(map() | struct() | Build.t()) :: {:ok, Manifest.t()} | {:error, error()}
  def manifest(%Build{manifest: manifest}), do: manifest(manifest)
  def manifest(%Manifest{} = manifest), do: {:ok, manifest}

  def manifest(value) when is_map(value) do
    with :ok <- validate_manifest_atom_budget(value) do
      {:ok, build_manifest(value)}
    end
  rescue
    error -> {:error, {:invalid_manifest_payload, error}}
  end

  def manifest(other), do: {:error, {:invalid_manifest_input, other}}

  defp build_manifest(value) do
    assets = value |> field_value(:assets, []) |> build_assets()
    graph = value |> field_value(:graph, %Graph{}) |> build_graph() |> validate_graph!(assets)

    %Manifest{
      schema_version: field_value(value, :schema_version),
      runner_contract_version: field_value(value, :runner_contract_version),
      assets: assets,
      pipelines: value |> field_value(:pipelines, []) |> build_pipelines(),
      schedules: value |> field_value(:schedules, []) |> build_schedules(),
      graph: graph,
      metadata: value |> field_value(:metadata, %{}) |> plain_map()
    }
  end

  defp build_assets(values) when is_list(values), do: Enum.map(values, &build_asset/1)
  defp build_assets(_other), do: []

  defp build_asset(value) when is_map(value) do
    %Asset{
      ref: value |> field_value(:ref) |> decode_ref(),
      module: value |> field_value(:module) |> decode_module(),
      name: value |> field_value(:name) |> decode_atom_optional(),
      type: value |> field_value(:type, :elixir) |> decode_known_atom([:elixir, :sql, :source]),
      depends_on: value |> field_value(:depends_on, []) |> build_refs(),
      execution: value |> field_value(:execution, %{}) |> build_execution(),
      settings: value |> field_value(:settings, %{}) |> build_settings(),
      description: field_value(value, :description),
      relation: value |> field_value(:relation) |> build_relation(),
      window: value |> field_value(:window) |> build_window_spec(),
      freshness: value |> field_value(:freshness) |> build_freshness(),
      retry_policy: value |> field_value(:retry_policy) |> build_retry_policy(),
      materialization: value |> field_value(:materialization) |> decode_materialization(),
      relation_inputs: value |> field_value(:relation_inputs, []) |> build_relation_inputs(),
      runtime_config: value |> field_value(:runtime_config, %{}) |> build_runtime_config(),
      session_requirements:
        value
        |> field_value(:session_requirements, %{})
        |> build_session_requirements(),
      execution_package_hash: field_value(value, :execution_package_hash),
      assurance: value |> field_value(:assurance) |> build_assurance(),
      execution_pool: value |> field_value(:execution_pool) |> decode_atom_optional(),
      metadata: value |> field_value(:metadata, %{}) |> build_metadata()
    }
  end

  defp build_asset(other), do: other

  defp build_assurance(nil), do: nil

  defp build_assurance(value) when is_map(value) do
    contract = value |> field_value(:contract) |> build_sql_contract()

    checks =
      value
      |> field_value(:checks, [])
      |> Enum.map(&build_assurance_check/1)

    %{contract: contract, checks: checks}
  end

  defp build_assurance(other), do: other

  defp build_assurance_check(value) when is_map(value) do
    %{
      name: value |> field_value(:name) |> decode_atom_optional(),
      origin: value |> field_value(:origin) |> decode_known_atom([:authored, :contract]),
      claim_id: field_value(value, :claim_id),
      at:
        value
        |> field_value(:at)
        |> decode_known_atom([:before_materialize, :after_materialize]),
      when: value |> field_value(:when) |> decode_known_atom_optional([:target_exists]),
      on_violation:
        value
        |> field_value(:on_violation)
        |> decode_known_atom([:fail, :warn, :skip_materialization]),
      message: field_value(value, :message)
    }
  end

  defp build_assurance_check(other), do: other

  defp build_session_requirements(value) when value in [nil, %{}],
    do: SessionRequirements.empty()

  defp build_session_requirements(value), do: SessionRequirements.validate!(value)

  defp build_execution(value) when is_map(value) do
    %{
      entrypoint: value |> field_value(:entrypoint) |> decode_atom_optional(),
      arity: field_value(value, :arity)
    }
  end

  defp build_execution(_other), do: %{entrypoint: nil, arity: nil}

  defp build_relation(nil), do: nil
  defp build_relation(%RelationRef{} = value), do: value

  defp build_relation(value) when is_map(value) or is_list(value) do
    RelationRef.new!(%{
      connection: value |> field_value(:connection) |> decode_atom_optional(),
      catalog: field_value(value, :catalog),
      schema: field_value(value, :schema),
      name: field_value(value, :name)
    })
  rescue
    _error -> plain_map(value)
  end

  defp build_relation(other), do: other

  defp build_window_spec(nil), do: nil
  defp build_window_spec(%Spec{} = value), do: value

  defp build_window_spec(value) when is_map(value) do
    case Spec.from_value(value) do
      {:ok, %Spec{} = spec} -> spec
      {:ok, nil} -> nil
      {:error, _reason} -> plain_map(value)
    end
  end

  defp build_window_spec(other), do: other

  defp build_freshness(nil), do: nil
  defp build_freshness(%FreshnessPolicy{} = value), do: FreshnessPolicy.from_value!(value)

  defp build_freshness(value) when is_map(value) do
    value
    |> normalize_freshness_map()
    |> FreshnessPolicy.from_value!()
  end

  defp build_freshness(value) when is_binary(value) do
    value
    |> decode_known_atom([:daily, :day, :always])
    |> FreshnessPolicy.from_value!()
  end

  defp build_freshness(value), do: FreshnessPolicy.from_value!(value)

  defp build_retry_policy(nil), do: nil
  defp build_retry_policy(%RetryPolicy{} = value), do: RetryPolicy.new!(value)
  defp build_retry_policy(value), do: RetryPolicy.new!(plain_map(value))

  defp normalize_freshness_map(value) do
    mode =
      value
      |> field_value(:mode)
      |> decode_known_atom([:calendar_period, :max_age, :window_success, :always])

    case mode do
      :calendar_period ->
        %{
          mode: mode,
          kind: value |> field_value(:kind) |> decode_known_atom([:day]),
          timezone: field_value(value, :timezone)
        }

      :max_age ->
        %{
          mode: mode,
          amount: field_value(value, :amount),
          unit: value |> field_value(:unit) |> decode_known_atom([:second, :minute, :hour, :day])
        }

      :window_success ->
        %{mode: mode}

      :always ->
        %{mode: mode}
    end
  end

  defp decode_materialization(nil), do: nil
  defp decode_materialization(:view), do: :view
  defp decode_materialization(:table), do: :table
  defp decode_materialization("view"), do: :view
  defp decode_materialization("table"), do: :table

  defp decode_materialization({:incremental, opts}) when is_list(opts) do
    Materialization.normalize!({:incremental, decode_materialization_opts(opts)})
  rescue
    _error -> {:incremental, decode_materialization_opts(opts)}
  end

  defp decode_materialization([kind, opts]) when kind in [:incremental, "incremental"] do
    decode_materialization({:incremental, opts})
  end

  defp decode_materialization(other), do: other

  defp decode_materialization_opts(values) when is_list(values) do
    Enum.map(values, fn
      {key, value} ->
        {decode_materialization_opt_key(key), decode_materialization_opt_value(key, value)}

      [key, value] ->
        {decode_materialization_opt_key(key), decode_materialization_opt_value(key, value)}

      other ->
        other
    end)
  end

  defp decode_materialization_opt_key(key),
    do: decode_known_atom(key, [:strategy, :unique_key, :window_column])

  defp decode_materialization_opt_value(key, value) do
    case decode_materialization_opt_key(key) do
      :strategy -> decode_known_atom(value, [:append, :replace, :delete_insert, :merge])
      :unique_key when is_list(value) -> Enum.map(value, &decode_atom_optional/1)
      :window_column when is_binary(value) -> value
      :window_column -> decode_atom_optional(value)
      _ -> value
    end
  end

  defp build_relation_inputs(values) when is_list(values),
    do: Enum.map(values, &build_relation_input/1)

  defp build_relation_inputs(_other), do: []

  defp build_relation_input(value) when is_map(value) do
    %RelationInput{
      kind:
        value
        |> field_value(:kind)
        |> decode_known_atom([:plain_relation, :direct_asset_ref]),
      relation_ref: value |> field_value(:relation_ref) |> build_relation(),
      raw: field_value(value, :raw),
      asset_ref: value |> field_value(:asset_ref) |> decode_ref(),
      resolution:
        value |> field_value(:resolution) |> decode_known_atom_optional([:resolved, :deferred]),
      span: value |> field_value(:span) |> plain_map_or_nil()
    }
  end

  defp build_relation_input(other), do: other

  defp build_runtime_config(values) when is_map(values) do
    Map.new(values, fn {scope, fields} ->
      {decode_atom_optional(scope), build_runtime_config_fields(fields)}
    end)
  end

  defp build_runtime_config(_other), do: %{}

  defp build_runtime_config_fields(fields) when is_map(fields) do
    Map.new(fields, fn {field, value} ->
      {decode_atom_optional(field), build_runtime_config_ref(value)}
    end)
  end

  defp build_runtime_config_fields(_other), do: %{}

  defp build_runtime_config_ref(%RuntimeConfigRef{} = ref), do: ref

  defp build_runtime_config_ref(value) when is_map(value) do
    %RuntimeConfigRef{
      provider: value |> field_value(:provider) |> decode_known_atom([:env]),
      key: field_value(value, :key),
      secret?: field_value(value, :secret?, field_value(value, :secret, false)),
      required?: field_value(value, :required?, field_value(value, :required, true))
    }
  end

  defp build_runtime_config_ref(other), do: other

  @doc "Rehydrates one immutable SQL execution package."
  @spec execution_package(map() | ExecutionPackage.t()) ::
          {:ok, ExecutionPackage.t()} | {:error, error()}
  def execution_package(%ExecutionPackage{} = package) do
    execution_package(Map.from_struct(package))
  end

  def execution_package(value) when is_map(value) do
    with :ok <- validate_manifest_atom_budget(value) do
      {:ok,
       %ExecutionPackage{
         schema_version: field_value(value, :schema_version),
         content_hash: field_value(value, :content_hash),
         asset_ref: value |> field_value(:asset_ref) |> decode_ref(),
         sql_execution: value |> field_value(:sql_execution) |> build_sql_execution()
       }}
    end
  rescue
    error -> {:error, {:invalid_manifest_payload, error}}
  end

  def execution_package(other), do: {:error, {:invalid_manifest_input, other}}

  defp build_sql_execution(nil), do: nil

  defp build_sql_execution(value) when is_map(value) do
    contract = value |> field_value(:contract) |> build_sql_contract()

    checks =
      value
      |> field_value(:checks, [])
      |> build_sql_checks()
      |> then(&Contract.validate_generated_checks!(contract, &1))

    execution = %SQLExecution{
      sql: field_value(value, :sql),
      template: value |> field_value(:template) |> build_template(),
      runtime_inputs: value |> field_value(:runtime_inputs) |> build_runtime_input_ref(),
      contract: contract,
      sql_definitions: value |> field_value(:sql_definitions, []) |> build_sql_definitions(),
      checks: checks
    }

    validate_query_runtime_relations!(execution)
    validate_check_runtime_relations!(execution)
    execution
  end

  defp build_sql_execution(other), do: other

  defp build_runtime_input_ref(nil), do: nil

  defp build_runtime_input_ref(%RuntimeInputResolverRef{} = ref) do
    case RuntimeInputResolverRef.validate(ref) do
      :ok -> ref
      {:error, :invalid_module} -> raise ArgumentError, "invalid runtime input resolver reference"
    end
  end

  defp build_runtime_input_ref(value) when is_map(value) do
    allowed_keys = MapSet.new([:module, "module"])

    if map_size(value) != 1 or Enum.any?(Map.keys(value), &(!MapSet.member?(allowed_keys, &1))) do
      raise ArgumentError,
            "invalid runtime input resolver reference; expected %{module: MyApp.Inputs}"
    end

    value
    |> field_value(:module)
    |> decode_module()
    |> RuntimeInputResolverRef.new!()
  end

  defp build_runtime_input_ref(_other) do
    raise ArgumentError,
          "invalid runtime input resolver reference; expected %{module: MyApp.Inputs}"
  end

  defp validate_query_runtime_relations!(%SQLExecution{
         template: %Template{} = template,
         sql_definitions: definitions
       }) do
    case RelationUsage.runtime_relations(template, definitions) |> MapSet.to_list() do
      [] ->
        :ok

      relations ->
        names = relations |> Enum.sort() |> Enum.map_join(", ", &"#{&1}()")
        raise ArgumentError, "#{names} may only be used inside SQL check bodies"
    end
  end

  defp validate_query_runtime_relations!(_execution), do: :ok

  defp validate_check_runtime_relations!(%SQLExecution{
         checks: checks,
         sql_definitions: definitions
       }) do
    Enum.each(checks, fn %Check{} = check ->
      relations = RelationUsage.runtime_relations(check.template, definitions)
      uses_query? = MapSet.member?(relations, :query)
      uses_target? = MapSet.member?(relations, :target)

      if check.uses_query? != uses_query? or check.uses_target? != uses_target? do
        raise ArgumentError,
              "SQL check #{inspect(check.name)} runtime relation flags do not match its template"
      end
    end)
  end

  defp build_sql_checks(values) when is_list(values), do: Enum.map(values, &build_sql_check/1)

  defp build_sql_checks(other),
    do: raise(ArgumentError, "SQL execution checks must be a list, got: #{inspect(other)}")

  defp build_sql_check(%Check{} = check), do: Check.validate!(check)

  defp build_sql_check(value) when is_map(value) do
    Check.new!(%{
      name: value |> field_value(:name) |> decode_atom_optional(),
      at:
        value
        |> field_value(:at)
        |> decode_known_atom([:before_materialize, :after_materialize]),
      on_violation:
        value
        |> field_value(:on_violation)
        |> decode_known_atom([:fail, :warn, :skip_materialization]),
      when:
        value
        |> field_value(:when)
        |> decode_known_atom_optional([:target_exists]),
      message: field_value(value, :message),
      sql: field_value(value, :sql),
      template: value |> field_value(:template) |> build_template(),
      file: field_value(value, :file),
      line: field_value(value, :line),
      origin:
        value
        |> field_value(:origin, :authored)
        |> decode_known_atom([:authored, :contract]),
      claim_id: field_value(value, :claim_id),
      uses_query?: field_value(value, :uses_query?, false),
      uses_target?: field_value(value, :uses_target?, false)
    })
  end

  defp build_sql_check(other), do: raise(ArgumentError, "invalid SQL check #{inspect(other)}")

  defp build_sql_contract(nil), do: nil
  defp build_sql_contract(%Contract{} = contract), do: Contract.validate!(contract)

  defp build_sql_contract(value) when is_map(value) do
    Contract.new!(%{
      grain: value |> field_value(:grain) |> build_contract_grain(),
      columns: value |> field_value(:columns, []) |> Enum.map(&build_contract_column/1),
      compositions:
        value
        |> field_value(:compositions, [])
        |> Enum.map(&build_contract_composition/1),
      unique_keys:
        value |> field_value(:unique_keys, []) |> Enum.map(&build_contract_unique_key/1),
      row_count: value |> field_value(:row_count) |> build_contract_row_count()
    })
  end

  defp build_sql_contract(other),
    do: raise(ArgumentError, "invalid SQL output contract #{inspect(other)}")

  defp build_contract_grain(nil), do: nil
  defp build_contract_grain(%Grain{} = grain), do: Grain.validate!(grain)

  defp build_contract_grain(value) when is_map(value) do
    Grain.new!(%{
      by: value |> field_value(:by, []) |> build_atom_list(),
      description: field_value(value, :description)
    })
  end

  defp build_contract_column(%Column{} = column), do: Column.validate!(column)

  defp build_contract_column(value) when is_map(value) do
    %Column{
      name: value |> field_value(:name) |> decode_atom_optional(),
      type:
        value
        |> field_value(:type)
        |> decode_known_atom(Column.supported_types()),
      nullable?: field_value(value, :nullable?, true),
      description: field_value(value, :description),
      renamed_from: value |> field_value(:renamed_from) |> decode_atom_optional(),
      tags: value |> field_value(:tags, []) |> build_string_list(),
      sources: value |> field_value(:sources, []) |> Enum.map(&build_contract_lineage/1),
      via:
        value
        |> field_value(:via)
        |> decode_known_atom_optional([:identity, :transformation, :aggregation])
    }
    |> Column.validate!()
  end

  defp build_contract_column(other),
    do: raise(ArgumentError, "invalid SQL contract column #{inspect(other)}")

  defp build_contract_composition(%Composition{} = composition),
    do: Composition.validate!(composition)

  defp build_contract_composition(value) when is_map(value) do
    allowed = MapSet.new([:module, "module", :start_index, "start_index", :columns, "columns"])

    if Enum.any?(Map.keys(value), &(!MapSet.member?(allowed, &1))) do
      raise ArgumentError, "invalid SQL contract composition fields"
    end

    Composition.new!(
      value |> field_value(:module) |> decode_module(),
      field_value(value, :start_index),
      value |> field_value(:columns, []) |> build_atom_list()
    )
  end

  defp build_contract_composition(other),
    do: raise(ArgumentError, "invalid SQL contract composition #{inspect(other)}")

  defp build_contract_lineage(%Lineage{} = lineage), do: Lineage.validate!(lineage)

  defp build_contract_lineage(value) when is_map(value) do
    kind = value |> field_value(:kind) |> decode_known_atom([:asset, :external])

    %Lineage{
      kind: kind,
      asset_ref: value |> field_value(:asset_ref) |> decode_ref(),
      dataset: field_value(value, :dataset),
      column:
        case kind do
          :asset -> value |> field_value(:column) |> decode_atom_optional()
          :external -> field_value(value, :column)
        end
    }
    |> Lineage.validate!()
  end

  defp build_contract_lineage(other),
    do: raise(ArgumentError, "invalid SQL contract lineage #{inspect(other)}")

  defp build_contract_unique_key(%UniqueKey{} = key), do: UniqueKey.validate!(key)

  defp build_contract_unique_key(value) when is_map(value) do
    value |> field_value(:columns, []) |> build_atom_list() |> UniqueKey.new!()
  end

  defp build_contract_unique_key(other),
    do: raise(ArgumentError, "invalid SQL contract unique key #{inspect(other)}")

  defp build_contract_row_count(nil), do: nil
  defp build_contract_row_count(%RowCount{} = row_count), do: RowCount.validate!(row_count)

  defp build_contract_row_count(value) when is_map(value) do
    allowed =
      MapSet.new([
        :equals,
        "equals",
        :min,
        "min",
        :max,
        "max",
        :when,
        "when",
        :on_violation,
        "on_violation"
      ])

    if Enum.any?(Map.keys(value), &(!MapSet.member?(allowed, &1))) do
      raise ArgumentError, "invalid SQL contract row_count fields"
    end

    RowCount.new!(%{
      equals: value |> field_value(:equals) |> build_contract_row_count_equals(),
      min: field_value(value, :min),
      max: field_value(value, :max),
      when:
        value
        |> field_value(:when)
        |> decode_known_atom_optional([:target_exists]),
      on_violation:
        value
        |> field_value(:on_violation)
        |> decode_known_atom([:fail, :warn, :skip_materialization])
    })
  end

  defp build_contract_row_count(other),
    do: raise(ArgumentError, "invalid SQL contract row_count #{inspect(other)}")

  defp build_contract_row_count_equals(nil), do: nil

  defp build_contract_row_count_equals(value) when is_integer(value), do: value

  defp build_contract_row_count_equals(%Param{} = param), do: Param.validate!(param)

  defp build_contract_row_count_equals(value) when is_map(value) do
    allowed = MapSet.new([:name, "name"])

    if map_size(value) != 1 or Enum.any?(Map.keys(value), &(!MapSet.member?(allowed, &1))) do
      raise ArgumentError, "invalid SQL contract row_count param"
    end

    value |> field_value(:name) |> decode_atom_optional() |> Param.new!()
  end

  defp build_contract_row_count_equals(other),
    do: raise(ArgumentError, "invalid SQL contract row_count equals #{inspect(other)}")

  defp build_sql_definitions(values) when is_list(values),
    do: Enum.map(values, &build_sql_definition/1)

  defp build_sql_definitions(_other), do: []

  defp build_sql_definition(value) when is_map(value) do
    %SQLDefinition{
      module: value |> field_value(:module) |> decode_module(),
      name: value |> field_value(:name) |> decode_atom_optional(),
      arity: field_value(value, :arity),
      params: value |> field_value(:params, []) |> build_sql_params(),
      shape: value |> field_value(:shape) |> decode_known_atom([:expression, :relation]),
      sql: field_value(value, :sql),
      template: value |> field_value(:template) |> build_template(),
      file: field_value(value, :file),
      line: field_value(value, :line),
      declared_file: field_value(value, :declared_file),
      declared_line: field_value(value, :declared_line),
      relation_defaults: build_relation_defaults(field_value(value, :relation_defaults, %{}))
    }
  end

  defp build_sql_definition(other), do: other

  defp build_relation_defaults(value) when is_map(value) do
    value
    |> Enum.reduce(%{}, fn {key, child}, acc ->
      case relation_default_key(key) do
        nil -> acc
        canonical -> Map.put(acc, canonical, relation_default_value(child))
      end
    end)
  end

  defp build_relation_defaults(_other), do: %{}

  defp relation_default_key(:connection), do: :connection
  defp relation_default_key("connection"), do: :connection
  defp relation_default_key(:catalog), do: :catalog
  defp relation_default_key("catalog"), do: :catalog
  defp relation_default_key(:schema), do: :schema
  defp relation_default_key("schema"), do: :schema
  defp relation_default_key(_key), do: nil

  defp relation_default_value(value) when is_binary(value), do: value
  defp relation_default_value(value) when is_atom(value), do: value
  defp relation_default_value(value), do: value

  defp build_sql_params(values) when is_list(values), do: Enum.map(values, &build_sql_param/1)
  defp build_sql_params(_other), do: []

  defp build_sql_param(value) when is_map(value) do
    %SQLDefinition.Param{
      name: value |> field_value(:name) |> decode_atom_optional(),
      index: field_value(value, :index)
    }
  end

  defp build_sql_param(other), do: other

  defp build_template(%Template{} = value), do: value

  defp build_template(value) when is_map(value) do
    %Template{
      source: field_value(value, :source),
      root_kind: value |> field_value(:root_kind) |> decode_known_atom([:query, :expression]),
      nodes: value |> field_value(:nodes, []) |> build_template_nodes(),
      span: value |> field_value(:span) |> build_template_span(),
      requires: value |> field_value(:requires) |> build_requirements()
    }
  end

  defp build_template(other), do: other

  defp build_template_nodes(values) when is_list(values),
    do: Enum.map(values, &build_template_node/1)

  defp build_template_nodes(_other), do: []

  defp build_template_node(value) when is_map(value) do
    cond do
      template_relation?(value) ->
        %Relation{
          raw: field_value(value, :raw),
          segments: value |> field_value(:segments, []) |> build_string_list(),
          span: value |> field_value(:span) |> build_template_span()
        }

      template_placeholder?(value) ->
        source = value |> field_value(:source) |> decode_placeholder_source()

        %Placeholder{
          name: value |> field_value(:name) |> decode_placeholder_name(source),
          source: source,
          span: value |> field_value(:span) |> build_template_span()
        }

      template_call?(value) ->
        %Call{
          definition: value |> field_value(:definition) |> build_definition_ref(),
          args: value |> field_value(:args, []) |> build_template_fragments(),
          context: value |> field_value(:context) |> decode_known_atom([:expression, :relation]),
          span: value |> field_value(:span) |> build_template_span()
        }

      template_asset_ref?(value) ->
        %AssetRef{
          module: value |> field_value(:module) |> decode_module(),
          asset_ref: value |> field_value(:asset_ref) |> decode_ref(),
          relation: value |> field_value(:relation) |> build_relation(),
          resolution:
            value |> field_value(:resolution) |> decode_known_atom([:resolved, :deferred]),
          span: value |> field_value(:span) |> build_template_span()
        }

      template_fragment?(value) ->
        %Fragment{
          nodes: value |> field_value(:nodes, []) |> build_template_nodes(),
          span: value |> field_value(:span) |> build_template_span()
        }

      template_runtime_relation?(value) ->
        %RuntimeRelation{
          kind: value |> field_value(:kind) |> decode_known_atom([:query, :target]),
          span: value |> field_value(:span) |> build_template_span()
        }

      template_text?(value) ->
        %Text{
          sql: field_value(value, :sql),
          span: value |> field_value(:span) |> build_template_span()
        }

      true ->
        plain_map(value)
    end
  end

  defp build_template_node(other), do: other

  defp build_template_fragments(values) when is_list(values),
    do: Enum.map(values, &build_template_fragment/1)

  defp build_template_fragments(_other), do: []

  defp build_template_fragment(value) when is_map(value) do
    %Fragment{
      nodes: value |> field_value(:nodes, []) |> build_template_nodes(),
      span: value |> field_value(:span) |> build_template_span()
    }
  end

  defp build_template_fragment(other), do: other

  defp build_definition_ref(value) when is_map(value) do
    %DefinitionRef{
      provider: value |> field_value(:provider) |> decode_module(),
      name: value |> field_value(:name) |> decode_atom_optional(),
      arity: field_value(value, :arity),
      kind: value |> field_value(:kind) |> decode_known_atom([:expression, :relation])
    }
  end

  defp build_definition_ref(other), do: other

  defp decode_placeholder_source(:runtime), do: :runtime
  defp decode_placeholder_source(:query_param), do: :query_param
  defp decode_placeholder_source("runtime"), do: :runtime
  defp decode_placeholder_source("query_param"), do: :query_param
  defp decode_placeholder_source({:local_arg, index}), do: {:local_arg, index}

  defp decode_placeholder_source([kind, index]) when kind in [:local_arg, "local_arg"],
    do: {:local_arg, index}

  defp decode_placeholder_source(other), do: other

  defp decode_placeholder_name(value, :query_param) when is_binary(value), do: value
  defp decode_placeholder_name(value, _source), do: decode_atom_optional(value)

  defp build_requirements(%Requirements{} = value), do: value

  defp build_requirements(value) when is_map(value) do
    %Requirements{
      runtime_inputs: value |> field_value(:runtime_inputs) |> build_atom_mapset(),
      query_params: value |> field_value(:query_params) |> build_string_mapset()
    }
  end

  defp build_requirements(_other),
    do: %Requirements{runtime_inputs: MapSet.new(), query_params: MapSet.new()}

  defp build_atom_mapset(%MapSet{} = value), do: value

  defp build_atom_mapset(value) when is_map(value) do
    entries =
      case field_value(value, :map) do
        nested when is_map(nested) -> Map.keys(nested)
        _other -> Map.keys(value)
      end

    entries
    |> Enum.map(&decode_atom_optional/1)
    |> MapSet.new()
  end

  defp build_atom_mapset(value) when is_list(value) do
    value
    |> Enum.map(&decode_atom_optional/1)
    |> MapSet.new()
  end

  defp build_atom_mapset(_other), do: MapSet.new()

  defp build_string_mapset(%MapSet{} = value), do: MapSet.new(value, &to_string/1)

  defp build_string_mapset(value) when is_map(value) do
    entries =
      case field_value(value, :map) do
        nested when is_map(nested) -> Map.keys(nested)
        _other -> Map.keys(value)
      end

    entries
    |> Enum.map(&to_string/1)
    |> MapSet.new()
  end

  defp build_string_mapset(value) when is_list(value) do
    value
    |> Enum.map(&to_string/1)
    |> MapSet.new()
  end

  defp build_string_mapset(_other), do: MapSet.new()

  defp build_pipelines(values) when is_list(values), do: Enum.map(values, &build_pipeline/1)
  defp build_pipelines(_other), do: []

  defp build_pipeline(value) when is_map(value) do
    %Pipeline{
      module: value |> field_value(:module) |> decode_module(),
      name: value |> field_value(:name) |> decode_atom_optional(),
      selectors: value |> field_value(:selectors, []) |> build_selectors(),
      deps: value |> field_value(:deps, :all) |> decode_known_atom([:all, :none]),
      schedule: value |> field_value(:schedule) |> decode_pipeline_schedule(),
      window: value |> field_value(:window) |> build_window_policy(),
      retry_policy: value |> field_value(:retry_policy) |> build_retry_policy(),
      max_concurrency: value |> field_value(:max_concurrency) |> decode_max_concurrency(),
      execution_pool: value |> field_value(:execution_pool) |> decode_atom_optional(),
      resource_recovery:
        value |> field_value(:resource_recovery) |> Favn.ResourceRecovery.Policy.from_value!(),
      source: value |> field_value(:source) |> decode_atom_optional(),
      outputs: value |> field_value(:outputs, []) |> build_atom_list(),
      settings: value |> field_value(:settings, %{}) |> build_settings(),
      metadata: value |> field_value(:metadata, %{}) |> build_metadata()
    }
  end

  defp build_pipeline(other), do: other

  defp build_settings(value) when is_map(value) do
    value
    |> Map.new(fn
      {key, setting} when is_atom(key) ->
        {key, setting}

      {key, setting} when is_binary(key) ->
        unless Favn.Settings.valid_key_string?(key) do
          raise ArgumentError, "invalid settings key #{inspect(key)}"
        end

        {decode_manifest_atom!(key), setting}

      {key, _setting} ->
        raise ArgumentError, "invalid settings key #{inspect(key)}"
    end)
    |> Favn.Settings.normalize!()
  end

  defp build_settings(value), do: Favn.Settings.normalize!(value)

  defp build_window_policy(nil), do: nil
  defp build_window_policy(%Policy{} = policy), do: policy
  defp build_window_policy(value), do: Policy.from_value!(value)

  defp decode_max_concurrency(value) when is_integer(value) and value > 0, do: value
  defp decode_max_concurrency(_value), do: nil

  defp build_selectors(values) when is_list(values), do: Enum.map(values, &decode_selector/1)
  defp build_selectors(_other), do: []

  defp decode_selector({kind, value}) when is_atom(kind), do: decode_selector([kind, value])

  defp decode_selector([kind, value]) when kind in [:asset, "asset"],
    do: {:asset, decode_selector_asset(value)}

  defp decode_selector([kind, value]) when kind in [:module, "module"],
    do: {:module, decode_module(value)}

  defp decode_selector([kind, value]) when kind in [:tag, "tag"],
    do: {:tag, Labels.normalize_label!(value)}

  defp decode_selector([kind, value]) when kind in [:category, "category"],
    do: {:category, Labels.normalize_label!(value)}

  defp decode_selector([module, name]), do: {:asset, decode_ref([module, name])}

  defp decode_selector(value) when is_map(value) do
    case field_value(value, :module) do
      kind when kind in [:asset, "asset"] ->
        {:asset, value |> field_value(:name) |> decode_selector_asset()}

      kind when kind in [:module, "module"] ->
        {:module, value |> field_value(:name) |> decode_module()}

      kind when kind in [:tag, "tag"] ->
        {:tag, value |> field_value(:name) |> Labels.normalize_label!()}

      kind when kind in [:category, "category"] ->
        {:category, value |> field_value(:name) |> Labels.normalize_label!()}

      _asset_ref ->
        {:asset, decode_ref(value)}
    end
  end

  defp decode_selector(other), do: other

  defp decode_selector_asset(value) do
    case decode_ref(value) do
      {module, name} = ref when is_atom(module) and is_atom(name) -> ref
      other -> decode_module(other)
    end
  end

  defp decode_pipeline_schedule(nil), do: nil
  defp decode_pipeline_schedule(%Schedule{} = value), do: value
  defp decode_pipeline_schedule({:ref, ref}), do: {:ref, decode_ref(ref)}
  defp decode_pipeline_schedule({:inline, schedule}), do: {:inline, build_schedule(schedule)}

  defp decode_pipeline_schedule([kind, value]) when kind in [:ref, "ref"],
    do: {:ref, decode_ref(value)}

  defp decode_pipeline_schedule([kind, value]) when kind in [:inline, "inline"],
    do: {:inline, build_schedule(value)}

  defp decode_pipeline_schedule(value) when is_map(value), do: build_schedule(value)
  defp decode_pipeline_schedule(other), do: other

  defp build_schedules(values) when is_list(values), do: Enum.map(values, &build_schedule/1)
  defp build_schedules(_other), do: []

  defp build_schedule(value) when is_map(value) do
    %Schedule{
      module: value |> field_value(:module) |> decode_module(),
      name: value |> field_value(:name) |> decode_atom_optional(),
      ref: value |> field_value(:ref) |> decode_ref(),
      kind: value |> field_value(:kind, :cron) |> decode_known_atom([:cron]),
      cron: field_value(value, :cron),
      timezone: field_value(value, :timezone),
      missed: value |> field_value(:missed, :skip) |> decode_known_atom([:skip, :one, :all]),
      overlap:
        value
        |> field_value(:overlap, :forbid)
        |> decode_known_atom([:forbid, :allow, :queue_one]),
      active: field_value(value, :active, true),
      origin: value |> field_value(:origin, :named) |> decode_known_atom([:inline, :named])
    }
  end

  defp build_schedule(other), do: other

  defp build_graph(%Graph{} = value), do: value

  defp build_graph(value) when is_map(value) do
    %Graph{
      nodes: value |> field_value(:nodes, []) |> build_refs(),
      edges: value |> field_value(:edges, []) |> build_edges(),
      topo_order: value |> field_value(:topo_order, []) |> build_refs()
    }
  end

  defp build_graph(_other), do: %Graph{}

  defp validate_graph!(%Graph{nodes: [], edges: [], topo_order: []}, assets) when assets != [] do
    raise ArgumentError, "manifest graph is required for non-empty assets"
  end

  defp validate_graph!(%Graph{} = graph, _assets), do: graph

  defp build_edges(values) when is_list(values) do
    Enum.map(values, fn
      edge when is_map(edge) ->
        %{
          from: edge |> field_value(:from) |> decode_ref(),
          to: edge |> field_value(:to) |> decode_ref()
        }

      other ->
        plain_map(other)
    end)
  end

  defp build_edges(_other), do: []

  defp build_refs(values) when is_list(values), do: Enum.map(values, &decode_ref/1)
  defp build_refs(_other), do: []

  defp decode_ref(nil), do: nil
  defp decode_ref({module, name}) when is_atom(module) and is_atom(name), do: {module, name}
  defp decode_ref([module, name]), do: {decode_module(module), decode_atom_optional(name)}

  defp decode_ref(value) when is_map(value) do
    {value |> field_value(:module) |> decode_module(),
     value |> field_value(:name) |> decode_atom_optional()}
  end

  defp decode_ref(other), do: other

  defp build_atom_list(values) when is_list(values), do: Enum.map(values, &decode_atom_optional/1)
  defp build_atom_list(_other), do: []

  defp build_string_list(values) when is_list(values), do: Enum.map(values, &to_string/1)
  defp build_string_list(_other), do: []

  defp build_metadata(value) when is_map(value) do
    plain = plain_map(value)

    plain
    |> Map.delete("category")
    |> Map.delete("tags")
    |> maybe_put(:category, field_value(value, :category) |> normalize_label_optional())
    |> maybe_put(:tags, field_value(value, :tags) |> build_metadata_tags())
  end

  defp build_metadata(_other), do: %{}

  defp build_metadata_tags(nil), do: nil

  defp build_metadata_tags(values), do: Labels.normalize_labels!(values)

  defp build_template_span(nil), do: nil

  defp build_template_span(%Template.Span{} = value), do: value

  defp build_template_span(value) when is_map(value) do
    %Template.Span{
      start_offset: field_value(value, :start_offset),
      end_offset: field_value(value, :end_offset),
      start_line: field_value(value, :start_line),
      start_column: field_value(value, :start_column),
      end_line: field_value(value, :end_line),
      end_column: field_value(value, :end_column)
    }
  end

  defp build_template_span(other), do: other

  defp template_text?(value), do: map_has_key?(value, :sql) and map_has_key?(value, :span)

  defp template_relation?(value),
    do:
      map_has_key?(value, :raw) and map_has_key?(value, :segments) and map_has_key?(value, :span)

  defp template_placeholder?(value),
    do: map_has_key?(value, :name) and map_has_key?(value, :source) and map_has_key?(value, :span)

  defp template_call?(value),
    do:
      map_has_key?(value, :definition) and map_has_key?(value, :args) and
        map_has_key?(value, :context) and map_has_key?(value, :span)

  defp template_asset_ref?(value),
    do:
      map_has_key?(value, :module) and map_has_key?(value, :asset_ref) and
        map_has_key?(value, :resolution) and map_has_key?(value, :span)

  defp template_fragment?(value), do: map_has_key?(value, :nodes) and map_has_key?(value, :span)

  defp template_runtime_relation?(value),
    do: map_has_key?(value, :kind) and map_has_key?(value, :span)

  defp plain_map_or_nil(nil), do: nil
  defp plain_map_or_nil(value), do: plain_map(value)

  defp plain_map(%_{} = value), do: value |> Map.from_struct() |> plain_map()

  defp plain_map(value) when is_map(value) do
    Enum.reduce(value, %{}, fn {key, item}, acc ->
      Map.put(acc, key, plain_value(item))
    end)
  end

  defp plain_map(value) when is_list(value), do: Enum.map(value, &plain_value/1)
  defp plain_map(other), do: other

  defp plain_value(%_{} = value), do: value |> Map.from_struct() |> plain_map()
  defp plain_value(value) when is_map(value), do: plain_map(value)
  defp plain_value(value) when is_list(value), do: Enum.map(value, &plain_value/1)
  defp plain_value(other), do: other

  defp map_has_key?(value, key),
    do: Map.has_key?(value, key) or Map.has_key?(value, Atom.to_string(key))

  defp field_value(value, field, default \\ nil)

  defp field_value(value, field, default) when is_map(value) do
    atom_key = field
    string_key = Atom.to_string(field)

    cond do
      Map.has_key?(value, atom_key) -> Map.get(value, atom_key)
      Map.has_key?(value, string_key) -> Map.get(value, string_key)
      true -> default
    end
  end

  defp field_value(_value, _field, default), do: default

  defp decode_module(nil), do: nil
  defp decode_module(value) when is_atom(value), do: value

  defp decode_module("Elixir." <> _rest = value) when is_binary(value),
    do: decode_manifest_module!(value)

  defp decode_module(value) when is_binary(value), do: decode_existing_atom!(value)
  defp decode_module(other), do: other

  defp decode_atom_optional(nil), do: nil
  defp decode_atom_optional(value) when is_atom(value), do: value
  defp decode_atom_optional(value) when is_binary(value), do: decode_manifest_atom!(value)
  defp decode_atom_optional(other), do: other

  defp decode_known_atom(value, allowed) do
    allowed_strings = Enum.map(allowed, &Atom.to_string/1)

    cond do
      is_atom(value) and value in allowed ->
        value

      is_binary(value) and value in allowed_strings ->
        String.to_existing_atom(value)

      true ->
        raise ArgumentError,
              "invalid enum value #{inspect(value)} expected one of #{inspect(allowed)}"
    end
  end

  defp decode_known_atom_optional(nil, _allowed), do: nil
  defp decode_known_atom_optional(value, allowed), do: decode_known_atom(value, allowed)

  defp normalize_label_optional(nil), do: nil
  defp normalize_label_optional(value), do: Labels.normalize_label!(value)

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp decode_existing_atom!(value) do
    case maybe_existing_atom(value) do
      {:ok, atom} -> atom
      :error -> raise ArgumentError, "unknown atom #{inspect(value)}"
    end
  end

  defp decode_manifest_module!(value) do
    if valid_manifest_module?(value) do
      # Manifest atoms are open-world identifiers emitted from compiled user
      # code and transported as JSON strings. We trust that source for
      # local/project manifests, but validate shape and budget them before
      # creating atoms so malformed manifests cannot exhaust the BEAM atom table.
      String.to_atom(value)
    else
      raise ArgumentError, "invalid module reference #{inspect(value)}"
    end
  end

  defp validate_manifest_atom_budget(value) do
    atom_refs = collect_manifest_atom_refs(value, MapSet.new())
    atom_ref_count = MapSet.size(atom_refs)

    if atom_ref_count > @max_manifest_atom_refs do
      {:error, {:manifest_atom_limit_exceeded, atom_ref_count, @max_manifest_atom_refs}}
    else
      validate_manifest_atom_headroom(atom_refs)
    end
  end

  defp validate_manifest_atom_headroom(atom_refs) do
    new_atom_count = Enum.count(atom_refs, &(maybe_existing_atom(&1) == :error))
    atom_count = :erlang.system_info(:atom_count)
    atom_limit = :erlang.system_info(:atom_limit)

    if atom_limit - atom_count - new_atom_count >= @min_manifest_atom_headroom do
      :ok
    else
      {:error, {:manifest_atom_headroom_exceeded, atom_count, atom_limit, new_atom_count}}
    end
  end

  defp collect_manifest_atom_refs(%_{} = struct, refs) do
    struct
    |> Map.from_struct()
    |> collect_manifest_atom_refs(refs)
  end

  defp collect_manifest_atom_refs(value, refs), do: collect_manifest_atom_refs(value, refs, [])

  defp collect_manifest_atom_refs(%_{} = struct, refs, path) do
    struct
    |> Map.from_struct()
    |> collect_manifest_atom_refs(refs, path)
  end

  defp collect_manifest_atom_refs(map, refs, path) when is_map(map) do
    Enum.reduce(map, refs, fn {key, value}, acc ->
      key_name = collect_key_name(key)

      cond do
        key_name == "selectors" ->
          collect_manifest_selectors(value, acc, [key | path])

        key_name == "settings" ->
          collect_settings_atom_refs(value, acc)

        metadata_label_path?(path, key_name) ->
          acc

        true ->
          refs = collect_manifest_atom_refs(key, acc, [key | path])
          collect_manifest_atom_refs(value, refs, [key | path])
      end
    end)
  end

  defp collect_manifest_atom_refs(list, refs, path) when is_list(list) do
    Enum.reduce(list, refs, &collect_manifest_atom_refs(&1, &2, path))
  end

  defp collect_manifest_atom_refs(value, refs, _path) when is_binary(value) do
    if valid_manifest_atom_ref?(value), do: MapSet.put(refs, value), else: refs
  end

  defp collect_manifest_atom_refs(_other, refs, _path), do: refs

  defp collect_manifest_selectors(values, refs, path) when is_list(values) do
    Enum.reduce(values, refs, &collect_manifest_selector(&1, &2, path))
  end

  defp collect_manifest_selectors(value, refs, path),
    do: collect_manifest_atom_refs(value, refs, path)

  defp collect_settings_atom_refs(settings, refs) when is_map(settings) do
    Enum.reduce(Map.keys(settings), refs, fn
      key, acc when is_atom(key) ->
        MapSet.put(acc, Atom.to_string(key))

      key, acc when is_binary(key) ->
        if(Favn.Settings.valid_key_string?(key), do: MapSet.put(acc, key), else: acc)

      _key, acc ->
        acc
    end)
  end

  defp collect_settings_atom_refs(_settings, refs), do: refs

  defp collect_manifest_selector([kind, _label], refs, path) when kind in [:tag, "tag"] do
    collect_manifest_atom_refs(kind, refs, path)
  end

  defp collect_manifest_selector([kind, _label], refs, path)
       when kind in [:category, "category"] do
    collect_manifest_atom_refs(kind, refs, path)
  end

  defp collect_manifest_selector(value, refs, path) when is_map(value) do
    case field_value(value, :module) do
      kind when kind in [:tag, "tag", :category, "category"] ->
        value
        |> Map.delete(:name)
        |> Map.delete("name")
        |> collect_manifest_atom_refs(refs, path)

      _other ->
        collect_manifest_atom_refs(value, refs, path)
    end
  end

  defp collect_manifest_selector(value, refs, path),
    do: collect_manifest_atom_refs(value, refs, path)

  defp metadata_label_path?(path, key_name) when key_name in ["category", "tags"] do
    collect_key_name(List.first(path)) in ["metadata", "meta"]
  end

  defp metadata_label_path?(_path, _key_name), do: false

  defp collect_key_name(key) when is_atom(key), do: Atom.to_string(key)
  defp collect_key_name(key) when is_binary(key), do: key
  defp collect_key_name(_key), do: nil

  defp decode_manifest_atom!(value) do
    if valid_manifest_atom?(value) do
      String.to_atom(value)
    else
      raise ArgumentError, "invalid atom reference #{inspect(value)}"
    end
  end

  defp valid_manifest_atom_ref?(value) do
    valid_manifest_atom?(value) or valid_manifest_module?(value)
  end

  defp valid_manifest_module?(value) when is_binary(value) do
    byte_size(value) <= @max_manifest_module_length and
      Regex.match?(~r/^Elixir\.[A-Z][A-Za-z0-9_]*(\.[A-Z][A-Za-z0-9_]*)*$/, value)
  end

  defp valid_manifest_atom?(value) when is_binary(value) do
    byte_size(value) in 1..@max_manifest_atom_length and
      Regex.match?(~r/^[A-Za-z_][A-Za-z0-9_]*[!?=]?$/, value)
  end

  defp maybe_existing_atom(value) when is_binary(value) do
    {:ok, String.to_existing_atom(value)}
  rescue
    ArgumentError -> :error
  end
end
