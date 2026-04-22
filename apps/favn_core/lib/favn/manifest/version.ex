defmodule Favn.Manifest.Version do
  @moduledoc """
  Immutable pinned manifest version envelope.
  """

  alias Favn.Asset.RelationInput
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Build
  alias Favn.Manifest.Compatibility
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Identity
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.SQLExecution
  alias Favn.RelationRef
  alias Favn.SQL.Definition, as: SQLDefinition
  alias Favn.SQL.Template

  alias Favn.SQL.Template.{
    AssetRef,
    Call,
    DefinitionRef,
    Fragment,
    Placeholder,
    Relation,
    Requirements,
    Text
  }

  alias Favn.SQLAsset.Materialization
  alias Favn.Window.Spec

  @type t :: %__MODULE__{
          manifest_version_id: String.t(),
          content_hash: String.t(),
          schema_version: pos_integer(),
          runner_contract_version: pos_integer(),
          serialization_format: String.t(),
          manifest: Manifest.t(),
          inserted_at: DateTime.t() | nil
        }

  defstruct [
    :manifest_version_id,
    :content_hash,
    :schema_version,
    :runner_contract_version,
    :manifest,
    inserted_at: nil,
    serialization_format: "json-v1"
  ]

  @type opt ::
          {:manifest_version_id, String.t()}
          | {:serialization_format, String.t()}
          | {:inserted_at, DateTime.t()}
          | {:hash_algorithm, :sha256}

  @type error ::
          {:invalid_manifest_version_id, term()}
          | {:invalid_serialization_format, term()}
          | {:unknown_opt, atom()}
          | Favn.Manifest.Compatibility.error()
          | Favn.Manifest.Identity.error()

  @spec new(map() | struct(), [opt()]) :: {:ok, t()} | {:error, error()}
  def new(manifest, opts \\ []) when is_list(opts) do
    canonical_manifest = canonical_manifest(manifest)
    manifest_version_id = Keyword.get(opts, :manifest_version_id, default_manifest_version_id())
    serialization_format = Keyword.get(opts, :serialization_format, "json-v1")

    with :ok <- validate_opts(opts),
         :ok <- Compatibility.validate_manifest(canonical_manifest),
         {:ok, schema_version} <- read_field(canonical_manifest, :schema_version),
         {:ok, runner_contract_version} <-
           read_field(canonical_manifest, :runner_contract_version),
         :ok <- validate_manifest_version_id(manifest_version_id),
         :ok <- validate_serialization_format(serialization_format),
         {:ok, content_hash} <-
           Identity.hash_manifest(canonical_manifest,
             algorithm: Keyword.get(opts, :hash_algorithm, :sha256)
           ) do
      {:ok,
       %__MODULE__{
         manifest_version_id: manifest_version_id,
         content_hash: content_hash,
         schema_version: schema_version,
         runner_contract_version: runner_contract_version,
         serialization_format: serialization_format,
         manifest: canonical_manifest,
         inserted_at: Keyword.get(opts, :inserted_at)
       }}
    end
  end

  defp canonical_manifest(%Build{manifest: manifest}), do: canonical_manifest(manifest)

  defp canonical_manifest(%Manifest{} = manifest), do: manifest

  defp canonical_manifest(manifest) when is_map(manifest) do
    if Enum.any?(Map.keys(manifest), &is_binary/1) do
      rehydrate_manifest(manifest)
    else
      manifest
    end
  end

  defp canonical_manifest(manifest), do: manifest

  defp rehydrate_manifest(manifest) do
    %Manifest{
      schema_version: field_value(manifest, :schema_version),
      runner_contract_version: field_value(manifest, :runner_contract_version),
      assets: manifest |> field_value(:assets, []) |> canonical_assets(),
      pipelines: manifest |> field_value(:pipelines, []) |> canonical_pipelines(),
      schedules: manifest |> field_value(:schedules, []) |> canonical_schedules(),
      graph: manifest |> field_value(:graph, %Graph{}) |> canonical_graph(),
      metadata: manifest |> field_value(:metadata, %{}) |> canonical_plain_map()
    }
  end

  defp canonical_assets(assets) when is_list(assets), do: Enum.map(assets, &canonical_asset/1)
  defp canonical_assets(_other), do: []

  defp canonical_asset(%Asset{} = asset) do
    %Asset{
      ref: asset |> field_value(:ref) |> canonical_ref(),
      module: asset |> field_value(:module) |> canonical_module(),
      name: asset |> field_value(:name) |> canonical_atom(),
      type: asset |> field_value(:type, :elixir) |> canonical_atom(),
      depends_on: asset |> field_value(:depends_on, []) |> canonical_refs(),
      execution: asset |> field_value(:execution, %{}) |> canonical_execution(),
      config: asset |> field_value(:config, %{}) |> canonical_plain_map(),
      relation: asset |> field_value(:relation) |> canonical_relation(),
      window: asset |> field_value(:window) |> canonical_window_spec(),
      materialization: asset |> field_value(:materialization) |> canonical_materialization(),
      relation_inputs: asset |> field_value(:relation_inputs, []) |> canonical_relation_inputs(),
      sql_execution: asset |> field_value(:sql_execution) |> canonical_sql_execution(),
      metadata: asset |> field_value(:metadata, %{}) |> canonical_metadata()
    }
  end

  defp canonical_asset(asset) when is_map(asset) do
    %Asset{
      ref: asset |> field_value(:ref) |> canonical_ref(),
      module: asset |> field_value(:module) |> canonical_module(),
      name: asset |> field_value(:name) |> canonical_atom(),
      type: asset |> field_value(:type, :elixir) |> canonical_atom(),
      depends_on: asset |> field_value(:depends_on, []) |> canonical_refs(),
      execution: asset |> field_value(:execution, %{}) |> canonical_execution(),
      config: asset |> field_value(:config, %{}) |> canonical_plain_map(),
      relation: asset |> field_value(:relation) |> canonical_relation(),
      window: asset |> field_value(:window) |> canonical_window_spec(),
      materialization: asset |> field_value(:materialization) |> canonical_materialization(),
      relation_inputs: asset |> field_value(:relation_inputs, []) |> canonical_relation_inputs(),
      sql_execution: asset |> field_value(:sql_execution) |> canonical_sql_execution(),
      metadata: asset |> field_value(:metadata, %{}) |> canonical_metadata()
    }
  end

  defp canonical_asset(other), do: other

  defp canonical_execution(execution) when is_map(execution) do
    %{
      entrypoint: execution |> field_value(:entrypoint) |> canonical_atom(),
      arity: field_value(execution, :arity)
    }
  end

  defp canonical_execution(_other), do: %{entrypoint: nil, arity: nil}

  defp canonical_relation(nil), do: nil
  defp canonical_relation(%RelationRef{} = relation), do: relation

  defp canonical_relation(relation) when is_map(relation) or is_list(relation) do
    RelationRef.new!(%{
      connection: relation |> field_value(:connection) |> canonical_atom(),
      catalog: field_value(relation, :catalog),
      schema: field_value(relation, :schema),
      name: field_value(relation, :name)
    })
  rescue
    _error -> canonical_plain_map(relation)
  end

  defp canonical_relation(other), do: other

  defp canonical_window_spec(nil), do: nil
  defp canonical_window_spec(%Spec{} = spec), do: spec

  defp canonical_window_spec(spec) when is_map(spec) do
    kind = spec |> field_value(:kind) |> canonical_atom()

    opts = [
      lookback: field_value(spec, :lookback, 0),
      timezone: field_value(spec, :timezone, "Etc/UTC")
    ]

    opts =
      case field_value(spec, :refresh_from) do
        nil -> opts
        refresh_from -> Keyword.put(opts, :refresh_from, canonical_atom(refresh_from))
      end

    try do
      Spec.new!(kind, opts)
    rescue
      _error -> canonical_plain_map(spec)
    end
  end

  defp canonical_window_spec(other), do: other

  defp canonical_materialization(nil), do: nil
  defp canonical_materialization(:view), do: :view
  defp canonical_materialization(:table), do: :table
  defp canonical_materialization("view"), do: :view
  defp canonical_materialization("table"), do: :table

  defp canonical_materialization({:incremental, opts}) when is_list(opts) do
    Materialization.normalize!({:incremental, canonical_materialization_opts(opts)})
  rescue
    _error -> {:incremental, canonical_materialization_opts(opts)}
  end

  defp canonical_materialization([kind, opts]) when kind in [:incremental, "incremental"] do
    canonical_materialization({:incremental, canonical_materialization_opts(opts)})
  end

  defp canonical_materialization(other), do: other

  defp canonical_materialization_opts(opts) when is_list(opts) do
    Enum.map(opts, fn
      {key, value} ->
        {canonical_atom(key), canonical_materialization_opt_value(canonical_atom(key), value)}

      [key, value] ->
        {canonical_atom(key), canonical_materialization_opt_value(canonical_atom(key), value)}

      other ->
        other
    end)
  end

  defp canonical_materialization_opts(opts), do: opts

  defp canonical_materialization_opt_value(:strategy, value), do: canonical_atom(value)

  defp canonical_materialization_opt_value(:unique_key, value) when is_list(value) do
    Enum.map(value, &canonical_atom/1)
  end

  defp canonical_materialization_opt_value(:window_column, value) when is_binary(value), do: value
  defp canonical_materialization_opt_value(:window_column, value), do: canonical_atom(value)
  defp canonical_materialization_opt_value(_key, value), do: value

  defp canonical_relation_inputs(inputs) when is_list(inputs),
    do: Enum.map(inputs, &canonical_relation_input/1)

  defp canonical_relation_inputs(_other), do: []

  defp canonical_relation_input(%RelationInput{} = input) do
    %RelationInput{
      kind: input |> field_value(:kind) |> canonical_atom(),
      relation_ref: input |> field_value(:relation_ref) |> canonical_relation(),
      raw: field_value(input, :raw),
      asset_ref: input |> field_value(:asset_ref) |> canonical_ref(),
      resolution: input |> field_value(:resolution) |> canonical_atom(),
      span: input |> field_value(:span) |> canonical_plain_map_or_nil()
    }
  end

  defp canonical_relation_input(input) when is_map(input) do
    %RelationInput{
      kind: input |> field_value(:kind) |> canonical_atom(),
      relation_ref: input |> field_value(:relation_ref) |> canonical_relation(),
      raw: field_value(input, :raw),
      asset_ref: input |> field_value(:asset_ref) |> canonical_ref(),
      resolution: input |> field_value(:resolution) |> canonical_atom(),
      span: input |> field_value(:span) |> canonical_plain_map_or_nil()
    }
  end

  defp canonical_relation_input(other), do: other

  defp canonical_sql_execution(nil), do: nil

  defp canonical_sql_execution(%SQLExecution{} = payload) do
    %SQLExecution{
      sql: field_value(payload, :sql),
      template: payload |> field_value(:template) |> canonical_template(),
      sql_definitions: payload |> field_value(:sql_definitions, []) |> canonical_sql_definitions()
    }
  end

  defp canonical_sql_execution(payload) when is_map(payload) do
    %SQLExecution{
      sql: field_value(payload, :sql),
      template: payload |> field_value(:template) |> canonical_template(),
      sql_definitions: payload |> field_value(:sql_definitions, []) |> canonical_sql_definitions()
    }
  end

  defp canonical_sql_execution(other), do: other

  defp canonical_sql_definitions(definitions) when is_list(definitions),
    do: Enum.map(definitions, &canonical_sql_definition/1)

  defp canonical_sql_definitions(_other), do: []

  defp canonical_sql_definition(%SQLDefinition{} = definition) do
    %SQLDefinition{
      module: definition |> field_value(:module) |> canonical_module(),
      name: definition |> field_value(:name) |> canonical_atom(),
      arity: field_value(definition, :arity),
      params: definition |> field_value(:params, []) |> canonical_sql_params(),
      shape: definition |> field_value(:shape) |> canonical_atom(),
      sql: field_value(definition, :sql),
      template: definition |> field_value(:template) |> canonical_template(),
      file: field_value(definition, :file),
      line: field_value(definition, :line),
      declared_file: field_value(definition, :declared_file),
      declared_line: field_value(definition, :declared_line)
    }
  end

  defp canonical_sql_definition(definition) when is_map(definition) do
    %SQLDefinition{
      module: definition |> field_value(:module) |> canonical_module(),
      name: definition |> field_value(:name) |> canonical_atom(),
      arity: field_value(definition, :arity),
      params: definition |> field_value(:params, []) |> canonical_sql_params(),
      shape: definition |> field_value(:shape) |> canonical_atom(),
      sql: field_value(definition, :sql),
      template: definition |> field_value(:template) |> canonical_template(),
      file: field_value(definition, :file),
      line: field_value(definition, :line),
      declared_file: field_value(definition, :declared_file),
      declared_line: field_value(definition, :declared_line)
    }
  end

  defp canonical_sql_definition(other), do: other

  defp canonical_sql_params(params) when is_list(params),
    do: Enum.map(params, &canonical_sql_param/1)

  defp canonical_sql_params(_other), do: []

  defp canonical_sql_param(%SQLDefinition.Param{} = param) do
    %SQLDefinition.Param{
      name: param |> field_value(:name) |> canonical_atom(),
      index: field_value(param, :index)
    }
  end

  defp canonical_sql_param(param) when is_map(param) do
    %SQLDefinition.Param{
      name: param |> field_value(:name) |> canonical_atom(),
      index: field_value(param, :index)
    }
  end

  defp canonical_sql_param(other), do: other

  defp canonical_template(%Template{} = template) do
    %Template{
      source: field_value(template, :source),
      root_kind: template |> field_value(:root_kind) |> canonical_atom(),
      nodes: template |> field_value(:nodes, []) |> canonical_template_nodes(),
      span: template |> field_value(:span) |> canonical_plain_map_or_nil(),
      requires: template |> field_value(:requires) |> canonical_requirements()
    }
  end

  defp canonical_template(template) when is_map(template) do
    %Template{
      source: field_value(template, :source),
      root_kind: template |> field_value(:root_kind) |> canonical_atom(),
      nodes: template |> field_value(:nodes, []) |> canonical_template_nodes(),
      span: template |> field_value(:span) |> canonical_plain_map_or_nil(),
      requires: template |> field_value(:requires) |> canonical_requirements()
    }
  end

  defp canonical_template(other), do: other

  defp canonical_template_nodes(nodes) when is_list(nodes),
    do: Enum.map(nodes, &canonical_template_node/1)

  defp canonical_template_nodes(_other), do: []

  defp canonical_template_node(%Text{} = node),
    do: %Text{
      sql: field_value(node, :sql),
      span: node |> field_value(:span) |> canonical_plain_map_or_nil()
    }

  defp canonical_template_node(%Relation{} = node) do
    %Relation{
      raw: field_value(node, :raw),
      segments: node |> field_value(:segments, []) |> canonical_string_list(),
      span: node |> field_value(:span) |> canonical_plain_map_or_nil()
    }
  end

  defp canonical_template_node(%Placeholder{} = node) do
    %Placeholder{
      name: node |> field_value(:name) |> canonical_atom(),
      source: node |> field_value(:source) |> canonical_placeholder_source(),
      span: node |> field_value(:span) |> canonical_plain_map_or_nil()
    }
  end

  defp canonical_template_node(%Call{} = node) do
    %Call{
      definition: node |> field_value(:definition) |> canonical_definition_ref(),
      args: node |> field_value(:args, []) |> canonical_template_fragments(),
      context: node |> field_value(:context) |> canonical_atom(),
      span: node |> field_value(:span) |> canonical_plain_map_or_nil()
    }
  end

  defp canonical_template_node(%AssetRef{} = node) do
    %AssetRef{
      module: node |> field_value(:module) |> canonical_module(),
      asset_ref: node |> field_value(:asset_ref) |> canonical_ref(),
      relation: node |> field_value(:relation) |> canonical_relation(),
      resolution: node |> field_value(:resolution) |> canonical_atom(),
      span: node |> field_value(:span) |> canonical_plain_map_or_nil()
    }
  end

  defp canonical_template_node(%Fragment{} = node) do
    %Fragment{
      nodes: node |> field_value(:nodes, []) |> canonical_template_nodes(),
      span: node |> field_value(:span) |> canonical_plain_map_or_nil()
    }
  end

  defp canonical_template_node(node) when is_map(node) do
    cond do
      template_text?(node) ->
        %Text{
          sql: field_value(node, :sql),
          span: node |> field_value(:span) |> canonical_plain_map_or_nil()
        }

      template_relation?(node) ->
        %Relation{
          raw: field_value(node, :raw),
          segments: node |> field_value(:segments, []) |> canonical_string_list(),
          span: node |> field_value(:span) |> canonical_plain_map_or_nil()
        }

      template_placeholder?(node) ->
        %Placeholder{
          name: node |> field_value(:name) |> canonical_atom(),
          source: node |> field_value(:source) |> canonical_placeholder_source(),
          span: node |> field_value(:span) |> canonical_plain_map_or_nil()
        }

      template_call?(node) ->
        %Call{
          definition: node |> field_value(:definition) |> canonical_definition_ref(),
          args: node |> field_value(:args, []) |> canonical_template_fragments(),
          context: node |> field_value(:context) |> canonical_atom(),
          span: node |> field_value(:span) |> canonical_plain_map_or_nil()
        }

      template_asset_ref?(node) ->
        %AssetRef{
          module: node |> field_value(:module) |> canonical_module(),
          asset_ref: node |> field_value(:asset_ref) |> canonical_ref(),
          relation: node |> field_value(:relation) |> canonical_relation(),
          resolution: node |> field_value(:resolution) |> canonical_atom(),
          span: node |> field_value(:span) |> canonical_plain_map_or_nil()
        }

      template_fragment?(node) ->
        %Fragment{
          nodes: node |> field_value(:nodes, []) |> canonical_template_nodes(),
          span: node |> field_value(:span) |> canonical_plain_map_or_nil()
        }

      true ->
        canonical_plain_map(node)
    end
  end

  defp canonical_template_node(other), do: other

  defp canonical_template_fragments(fragments) when is_list(fragments),
    do: Enum.map(fragments, &canonical_template_fragment/1)

  defp canonical_template_fragments(_other), do: []

  defp canonical_template_fragment(%Fragment{} = fragment), do: canonical_template_node(fragment)

  defp canonical_template_fragment(fragment) when is_map(fragment) do
    %Fragment{
      nodes: fragment |> field_value(:nodes, []) |> canonical_template_nodes(),
      span: fragment |> field_value(:span) |> canonical_plain_map_or_nil()
    }
  end

  defp canonical_template_fragment(other), do: other

  defp canonical_definition_ref(%DefinitionRef{} = definition) do
    %DefinitionRef{
      provider: definition |> field_value(:provider) |> canonical_module(),
      name: definition |> field_value(:name) |> canonical_atom(),
      arity: field_value(definition, :arity),
      kind: definition |> field_value(:kind) |> canonical_atom()
    }
  end

  defp canonical_definition_ref(definition) when is_map(definition) do
    %DefinitionRef{
      provider: definition |> field_value(:provider) |> canonical_module(),
      name: definition |> field_value(:name) |> canonical_atom(),
      arity: field_value(definition, :arity),
      kind: definition |> field_value(:kind) |> canonical_atom()
    }
  end

  defp canonical_definition_ref(other), do: other

  defp canonical_placeholder_source(:runtime), do: :runtime
  defp canonical_placeholder_source(:query_param), do: :query_param
  defp canonical_placeholder_source("runtime"), do: :runtime
  defp canonical_placeholder_source("query_param"), do: :query_param
  defp canonical_placeholder_source({:local_arg, index}), do: {:local_arg, index}

  defp canonical_placeholder_source([kind, index]) when kind in [:local_arg, "local_arg"],
    do: {:local_arg, index}

  defp canonical_placeholder_source(other), do: other

  defp canonical_requirements(%Requirements{} = requirements) do
    %Requirements{
      runtime_inputs: requirements |> field_value(:runtime_inputs) |> canonical_mapset(),
      query_params: requirements |> field_value(:query_params) |> canonical_mapset()
    }
  end

  defp canonical_requirements(requirements) when is_map(requirements) do
    %Requirements{
      runtime_inputs: requirements |> field_value(:runtime_inputs) |> canonical_mapset(),
      query_params: requirements |> field_value(:query_params) |> canonical_mapset()
    }
  end

  defp canonical_requirements(_other) do
    %Requirements{runtime_inputs: MapSet.new(), query_params: MapSet.new()}
  end

  defp canonical_mapset(%MapSet{} = value), do: value

  defp canonical_mapset(value) when is_map(value) do
    entries =
      case field_value(value, :map) do
        nested when is_map(nested) -> Map.keys(nested)
        _other -> Map.keys(value)
      end

    entries
    |> Enum.map(&canonical_atom/1)
    |> MapSet.new()
  end

  defp canonical_mapset(value) when is_list(value) do
    value
    |> Enum.map(&canonical_atom/1)
    |> MapSet.new()
  end

  defp canonical_mapset(_other), do: MapSet.new()

  defp canonical_pipelines(pipelines) when is_list(pipelines),
    do: Enum.map(pipelines, &canonical_pipeline/1)

  defp canonical_pipelines(_other), do: []

  defp canonical_pipeline(%Pipeline{} = pipeline) do
    %Pipeline{
      module: pipeline |> field_value(:module) |> canonical_module(),
      name: pipeline |> field_value(:name) |> canonical_atom(),
      selectors: pipeline |> field_value(:selectors, []) |> canonical_selectors(),
      deps: pipeline |> field_value(:deps, :all) |> canonical_atom(),
      schedule: pipeline |> field_value(:schedule) |> canonical_pipeline_schedule(),
      window: pipeline |> field_value(:window) |> canonical_atom(),
      source: pipeline |> field_value(:source) |> canonical_atom(),
      outputs: pipeline |> field_value(:outputs, []) |> canonical_atom_list(),
      config: pipeline |> field_value(:config, %{}) |> canonical_plain_map(),
      metadata: pipeline |> field_value(:metadata, %{}) |> canonical_metadata()
    }
  end

  defp canonical_pipeline(pipeline) when is_map(pipeline) do
    %Pipeline{
      module: pipeline |> field_value(:module) |> canonical_module(),
      name: pipeline |> field_value(:name) |> canonical_atom(),
      selectors: pipeline |> field_value(:selectors, []) |> canonical_selectors(),
      deps: pipeline |> field_value(:deps, :all) |> canonical_atom(),
      schedule: pipeline |> field_value(:schedule) |> canonical_pipeline_schedule(),
      window: pipeline |> field_value(:window) |> canonical_atom(),
      source: pipeline |> field_value(:source) |> canonical_atom(),
      outputs: pipeline |> field_value(:outputs, []) |> canonical_atom_list(),
      config: pipeline |> field_value(:config, %{}) |> canonical_plain_map(),
      metadata: pipeline |> field_value(:metadata, %{}) |> canonical_metadata()
    }
  end

  defp canonical_pipeline(other), do: other

  defp canonical_selectors(selectors) when is_list(selectors),
    do: Enum.map(selectors, &canonical_selector/1)

  defp canonical_selectors(_other), do: []

  defp canonical_selector({kind, value}) when is_atom(kind), do: canonical_selector([kind, value])

  defp canonical_selector([kind, value]) when kind in [:asset, "asset"] do
    {:asset, canonical_selector_asset(value)}
  end

  defp canonical_selector([kind, value]) when kind in [:module, "module"] do
    {:module, canonical_module(value)}
  end

  defp canonical_selector([kind, value]) when kind in [:tag, "tag"] do
    {:tag, canonical_atom_or_binary(value)}
  end

  defp canonical_selector([kind, value]) when kind in [:category, "category"] do
    {:category, canonical_atom_or_binary(value)}
  end

  defp canonical_selector(selector) when is_map(selector) do
    case selector |> field_value(:module) |> canonical_atom() do
      :asset -> {:asset, selector |> field_value(:name) |> canonical_selector_asset()}
      :module -> {:module, selector |> field_value(:name) |> canonical_module()}
      :tag -> {:tag, selector |> field_value(:name) |> canonical_atom_or_binary()}
      :category -> {:category, selector |> field_value(:name) |> canonical_atom_or_binary()}
      _other -> selector
    end
  end

  defp canonical_selector(other), do: other

  defp canonical_selector_asset(value) do
    case canonical_ref(value) do
      {module, name} = ref when is_atom(module) and is_atom(name) -> ref
      other -> canonical_module(other)
    end
  end

  defp canonical_pipeline_schedule(nil), do: nil
  defp canonical_pipeline_schedule(%Schedule{} = schedule), do: schedule
  defp canonical_pipeline_schedule({:ref, ref}), do: {:ref, canonical_ref(ref)}

  defp canonical_pipeline_schedule({:inline, schedule}),
    do: {:inline, canonical_schedule(schedule)}

  defp canonical_pipeline_schedule([kind, value]) when kind in [:ref, "ref"],
    do: {:ref, canonical_ref(value)}

  defp canonical_pipeline_schedule([kind, value]) when kind in [:inline, "inline"] do
    {:inline, canonical_schedule(value)}
  end

  defp canonical_pipeline_schedule(schedule) when is_map(schedule),
    do: canonical_schedule(schedule)

  defp canonical_pipeline_schedule(other), do: other

  defp canonical_schedules(schedules) when is_list(schedules),
    do: Enum.map(schedules, &canonical_schedule/1)

  defp canonical_schedules(_other), do: []

  defp canonical_schedule(%Schedule{} = schedule) do
    %Schedule{
      module: schedule |> field_value(:module) |> canonical_module(),
      name: schedule |> field_value(:name) |> canonical_atom(),
      ref: schedule |> field_value(:ref) |> canonical_ref(),
      kind: schedule |> field_value(:kind, :cron) |> canonical_atom(),
      cron: field_value(schedule, :cron),
      timezone: field_value(schedule, :timezone),
      missed: schedule |> field_value(:missed, :skip) |> canonical_atom(),
      overlap: schedule |> field_value(:overlap, :forbid) |> canonical_atom(),
      active: field_value(schedule, :active, true),
      origin: schedule |> field_value(:origin, :named) |> canonical_atom()
    }
  end

  defp canonical_schedule(schedule) when is_map(schedule) do
    %Schedule{
      module: schedule |> field_value(:module) |> canonical_module(),
      name: schedule |> field_value(:name) |> canonical_atom(),
      ref: schedule |> field_value(:ref) |> canonical_ref(),
      kind: schedule |> field_value(:kind, :cron) |> canonical_atom(),
      cron: field_value(schedule, :cron),
      timezone: field_value(schedule, :timezone),
      missed: schedule |> field_value(:missed, :skip) |> canonical_atom(),
      overlap: schedule |> field_value(:overlap, :forbid) |> canonical_atom(),
      active: field_value(schedule, :active, true),
      origin: schedule |> field_value(:origin, :named) |> canonical_atom()
    }
  end

  defp canonical_schedule(other), do: other

  defp canonical_graph(%Graph{} = graph) do
    %Graph{
      nodes: graph |> field_value(:nodes, []) |> canonical_refs(),
      edges: graph |> field_value(:edges, []) |> canonical_edges(),
      topo_order: graph |> field_value(:topo_order, []) |> canonical_refs()
    }
  end

  defp canonical_graph(graph) when is_map(graph) do
    %Graph{
      nodes: graph |> field_value(:nodes, []) |> canonical_refs(),
      edges: graph |> field_value(:edges, []) |> canonical_edges(),
      topo_order: graph |> field_value(:topo_order, []) |> canonical_refs()
    }
  end

  defp canonical_graph(_other), do: %Graph{}

  defp canonical_edges(edges) when is_list(edges) do
    Enum.map(edges, fn
      %{from: from, to: to} -> %{from: canonical_ref(from), to: canonical_ref(to)}
      %{"from" => from, "to" => to} -> %{from: canonical_ref(from), to: canonical_ref(to)}
      other -> canonical_plain_map(other)
    end)
  end

  defp canonical_edges(_other), do: []

  defp canonical_refs(refs) when is_list(refs), do: Enum.map(refs, &canonical_ref/1)
  defp canonical_refs(_other), do: []

  defp canonical_ref({module, name}) when is_atom(module) and is_atom(name), do: {module, name}
  defp canonical_ref([module, name]), do: {canonical_module(module), canonical_atom(name)}

  defp canonical_ref(value) when is_map(value) do
    {value |> field_value(:module) |> canonical_module(),
     value |> field_value(:name) |> canonical_atom()}
  end

  defp canonical_ref(other), do: other

  defp canonical_module(nil), do: nil
  defp canonical_module(module) when is_atom(module), do: module
  defp canonical_module(module) when is_binary(module), do: String.to_atom(module)
  defp canonical_module(other), do: other

  defp canonical_atom(nil), do: nil
  defp canonical_atom(value) when is_atom(value), do: value
  defp canonical_atom(value) when is_binary(value), do: String.to_atom(value)
  defp canonical_atom(other), do: other

  defp canonical_atom_list(values) when is_list(values), do: Enum.map(values, &canonical_atom/1)
  defp canonical_atom_list(_other), do: []

  defp canonical_string_list(values) when is_list(values), do: Enum.map(values, &to_string/1)
  defp canonical_string_list(_other), do: []

  defp canonical_atom_or_binary(value) when is_atom(value), do: value
  defp canonical_atom_or_binary(value) when is_binary(value), do: canonical_atom(value)
  defp canonical_atom_or_binary(other), do: other

  defp canonical_metadata(metadata) when is_map(metadata) do
    metadata = canonical_plain_map(metadata)

    metadata =
      if Map.has_key?(metadata, :category) do
        Map.update!(metadata, :category, &canonical_atom/1)
      else
        metadata
      end

    if Map.has_key?(metadata, :tags) do
      Map.update!(metadata, :tags, fn
        values when is_list(values) -> Enum.map(values, &canonical_atom_or_binary/1)
        other -> other
      end)
    else
      metadata
    end
  end

  defp canonical_metadata(_other), do: %{}

  defp canonical_plain_map_or_nil(nil), do: nil
  defp canonical_plain_map_or_nil(value), do: canonical_plain_map(value)

  defp canonical_plain_map(%_{} = value), do: value |> Map.from_struct() |> canonical_plain_map()

  defp canonical_plain_map(value) when is_map(value) do
    Enum.reduce(value, %{}, fn {key, item}, acc ->
      Map.put(acc, canonical_plain_key(key), canonical_plain_value(item))
    end)
  end

  defp canonical_plain_map(value) when is_list(value), do: canonical_plain_value(value)
  defp canonical_plain_map(other), do: other

  defp canonical_plain_value(value) when is_map(value), do: canonical_plain_map(value)

  defp canonical_plain_value(value) when is_list(value),
    do: Enum.map(value, &canonical_plain_value/1)

  defp canonical_plain_value(other), do: other

  defp canonical_plain_key(key) when is_atom(key), do: key
  defp canonical_plain_key(key) when is_binary(key), do: String.to_atom(key)
  defp canonical_plain_key(key), do: key

  defp template_text?(node), do: map_has_key?(node, :sql) and map_has_key?(node, :span)

  defp template_relation?(node),
    do: map_has_key?(node, :raw) and map_has_key?(node, :segments) and map_has_key?(node, :span)

  defp template_placeholder?(node),
    do: map_has_key?(node, :name) and map_has_key?(node, :source) and map_has_key?(node, :span)

  defp template_call?(node),
    do:
      map_has_key?(node, :definition) and map_has_key?(node, :args) and
        map_has_key?(node, :context) and
        map_has_key?(node, :span)

  defp template_asset_ref?(node),
    do:
      map_has_key?(node, :module) and map_has_key?(node, :asset_ref) and
        map_has_key?(node, :relation) and
        map_has_key?(node, :resolution) and map_has_key?(node, :span)

  defp template_fragment?(node), do: map_has_key?(node, :nodes) and map_has_key?(node, :span)

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

  defp read_field(value, field) do
    atom_key = field
    string_key = Atom.to_string(field)

    cond do
      Map.has_key?(value, atom_key) -> {:ok, Map.get(value, atom_key)}
      Map.has_key?(value, string_key) -> {:ok, Map.get(value, string_key)}
      true -> {:error, {:missing_manifest_field, field}}
    end
  end

  defp validate_opts(opts) do
    allowed = [:manifest_version_id, :serialization_format, :inserted_at, :hash_algorithm]

    case Enum.find(opts, fn {key, _value} -> key not in allowed end) do
      nil -> :ok
      {key, _value} -> {:error, {:unknown_opt, key}}
    end
  end

  defp validate_manifest_version_id(value) when is_binary(value) and value != "", do: :ok
  defp validate_manifest_version_id(value), do: {:error, {:invalid_manifest_version_id, value}}

  defp validate_serialization_format(value) when is_binary(value) and value != "", do: :ok
  defp validate_serialization_format(value), do: {:error, {:invalid_serialization_format, value}}

  defp default_manifest_version_id do
    "mv_" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  end
end
